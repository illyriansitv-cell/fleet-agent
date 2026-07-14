import asyncio
import json
import os
import logging
import socket

import httpx
import redis.asyncio as aioredis

from . import metrics, llm, bus, reporter
from .executor import _dispatch

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("fleet-agent")

NODE_LABEL = os.environ["NODE_LABEL"]
REDIS_URL = os.environ.get("REDIS_URL", "redis://dokploy-redis:6379")
BACKEND_URL = os.environ.get("FLEETDEPLOY_BACKEND_URL", "")
AGENT_SECRET = os.environ.get("AGENT_SECRET", "")
SCALE_CPU_THRESHOLD = float(os.environ.get("SCALE_CPU_THRESHOLD", "80"))
SCALE_CONSECUTIVE = int(os.environ.get("SCALE_CONSECUTIVE", "3"))
LOOP_INTERVAL = int(os.environ.get("LOOP_INTERVAL", "60"))
PEER_FETCH_INTERVAL = int(os.environ.get("PEER_FETCH_INTERVAL", "300"))
FD_HEALTH_CHECK_INTERVAL = int(os.environ.get("FD_HEALTH_CHECK_INTERVAL", "300"))


async def _prune_dead_swarm_nodes() -> str | None:
    """
    Manager-only: remove Down swarm nodes to prevent overlay IP pool exhaustion
    and global-service scheduling chaos. Runs every 6 hours on the manager.
    """
    raw = await metrics._run("docker node ls --format '{{.ID}} {{.Status}}' 2>/dev/null")
    dead = [line.split()[0] for line in raw.strip().splitlines()
            if len(line.split()) >= 2 and line.split()[1].lower() == "down"]
    if not dead:
        return None
    for node_id in dead:
        await metrics._run(f"docker node rm {node_id} 2>/dev/null || true")
    return f"pruned {len(dead)} dead Swarm node(s): {', '.join(dead[:5])}{'…' if len(dead) > 5 else ''}"


async def _ensure_fd_health() -> str | None:
    """
    Check that the fd-health (whoami) container is running on this node.
    If it's stopped or missing, recreate it. Returns a log message if action
    was taken, None if everything was already fine.
    """
    status = await metrics._run(
        "docker inspect fd-health --format '{{.State.Running}}' 2>/dev/null"
    )
    if status.strip() == "true":
        return None
    # Container is stopped or missing — recreate it.
    await metrics._run(
        "docker rm -f fd-health 2>/dev/null || true && "
        "docker run -d --name fd-health --network dokploy-network "
        "--restart unless-stopped traefik/whoami:latest 2>/dev/null"
    )
    return f"fd-health was {'stopped' if status.strip() == 'false' else 'missing'} — recreated standalone container"


async def main():
    log.info(f"Fleet agent starting — node={NODE_LABEL}")
    redis = aioredis.from_url(REDIS_URL, decode_responses=True)
    headers = {"X-Agent-Secret": AGENT_SECRET}
    high_cpu_streak = 0
    last_peer_fetch = 0.0
    last_fd_health_check = 0.0
    last_model_fetch = 0.0
    last_swarm_prune = 0.0
    cached_models: list = []
    MODEL_FETCH_INTERVAL = 300
    SWARM_PRUNE_INTERVAL = 21600  # 6 hours — manager only
    # mutable thresholds — updated live from heartbeat agent_config
    cpu_threshold = SCALE_CPU_THRESHOLD
    scale_consecutive = SCALE_CONSECUTIVE

    async with httpx.AsyncClient(base_url=BACKEND_URL, headers=headers, timeout=15) as client:
        while True:
            try:
                m = await metrics.collect()

                now = asyncio.get_event_loop().time()
                if now - last_peer_fetch >= PEER_FETCH_INTERVAL:
                    await bus.fetch_peers_from_backend(client)
                    last_peer_fetch = now
                await bus.get_all_peers(redis)  # also try local Redis, updates cache silently
                peers = bus.get_cached_peers()

                if m["cpu_pct"] > 90 or m["mem_pct"] > 90:
                    status = "critical"
                elif m["cpu_pct"] > 70 or m["mem_pct"] > 80 or m["error_count_5m"] > 10:
                    status = "degraded"
                else:
                    status = "healthy"

                task = "monitoring"

                if m["error_count_5m"] > 5 and m["recent_errors"]:
                    task = "classifying"
                    summary = await llm.classify_errors(m["recent_errors"])
                    await reporter.log_event(
                        client, NODE_LABEL, "warning",
                        f"Error spike ({m['error_count_5m']} errors/5m): {summary}",
                        {"error_count": m["error_count_5m"]},
                    )
                    task = "monitoring"

                if m["cpu_pct"] > cpu_threshold:
                    high_cpu_streak += 1
                    if high_cpu_streak >= scale_consecutive:
                        task = "scaling"
                        should, reason = await llm.should_scale_out(m, peers)
                        if should:
                            log.info(f"Scale-out triggered: {reason}")
                            await reporter.trigger_scale_out(client, NODE_LABEL, reason)
                            await reporter.log_event(
                                client, NODE_LABEL, "scale_out",
                                f"Scale-out triggered: {reason}", m,
                            )
                            high_cpu_streak = 0
                        task = "monitoring"
                else:
                    high_cpu_streak = max(0, high_cpu_streak - 1)

                now = asyncio.get_event_loop().time()
                if NODE_LABEL == "de" and now - last_swarm_prune >= SWARM_PRUNE_INTERVAL:
                    prune_msg = await _prune_dead_swarm_nodes()
                    last_swarm_prune = now
                    if prune_msg:
                        log.warning(f"[self-heal] {prune_msg}")
                        await reporter.log_event(
                            client, NODE_LABEL, "action",
                            f"[self-heal] {prune_msg}", {},
                        )

                now = asyncio.get_event_loop().time()
                if now - last_fd_health_check >= FD_HEALTH_CHECK_INTERVAL:
                    heal_msg = await _ensure_fd_health()
                    last_fd_health_check = now
                    if heal_msg:
                        log.warning(f"[self-heal] {heal_msg}")
                        await reporter.log_event(
                            client, NODE_LABEL, "action",
                            f"[self-heal] {heal_msg}", {},
                        )

                # Refresh Ollama models every 5 min
                now = asyncio.get_event_loop().time()
                if now - last_model_fetch >= MODEL_FETCH_INTERVAL:
                    cached_models = await llm.list_ollama_models()
                    last_model_fetch = now

                # Collect running containers for admin visibility
                containers = await metrics.collect_containers()

                ip = socket.gethostbyname(socket.gethostname())
                state = {**m, "status": status, "task": task, "node_label": NODE_LABEL, "ip": ip}
                await bus.publish_state(redis, NODE_LABEL, state)

                gossip = {"peers": peers, "containers": containers, "ollama_models": cached_models}
                hb = await reporter.heartbeat(client, NODE_LABEL, m, status, task, gossip)
                directive    = hb.get("directive")   if hb else None
                directive_id = hb.get("directive_id") if hb else None
                # Update LLM config + prompt overrides if backend sent new settings
                llm_config = hb.get("llm_config") if hb else None
                if llm_config:
                    llm.update_config(llm_config)
                agent_prompts = hb.get("agent_prompts") if hb else None
                if agent_prompts:
                    llm.update_prompts(agent_prompts)
                agent_config = hb.get("agent_config") if hb else None
                if agent_config:
                    if "scale_cpu_threshold" in agent_config:
                        cpu_threshold = float(agent_config["scale_cpu_threshold"])
                    if "scale_consecutive" in agent_config:
                        scale_consecutive = int(agent_config["scale_consecutive"])

                if directive:
                    log.info(f"Directive received: {directive}")
                    task = "executing"
                    # Structured JSON directive → bypass LLM, dispatch directly
                    try:
                        action = json.loads(directive)
                        if isinstance(action, dict) and "action_type" in action:
                            res = await _dispatch(action)
                            result = {
                                "action_type": action["action_type"],
                                "reasoning": action.get("reasoning", "structured action"),
                                "ok": res["ok"],
                                "output": res["output"],
                                "raw_plan": "",
                            }
                        else:
                            raise ValueError("not a structured action")
                    except (json.JSONDecodeError, ValueError):
                        result = await llm.handle_directive(directive, m)
                    await reporter.log_event(
                        client, NODE_LABEL,
                        "action" if result.get("ok") else "error",
                        f"[{result.get('action_type','?')}] {directive[:60]} → "
                        f"{'OK' if result.get('ok') else 'FAILED'}: {result.get('output','')[:120]}",
                        {
                            "directive": directive,
                            "action_type": result.get("action_type"),
                            "ok": result.get("ok"),
                            "output": result.get("output", "")[:500],
                            "reasoning": result.get("reasoning", ""),
                        },
                    )
                    if directive_id:
                        await reporter.complete_directive(client, directive_id, result)
                    task = "monitoring"

            except Exception as e:
                log.error(f"Agent loop error: {e}", exc_info=True)

            await asyncio.sleep(LOOP_INTERVAL)


if __name__ == "__main__":
    asyncio.run(main())
