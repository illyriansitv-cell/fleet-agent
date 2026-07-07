import asyncio
import os
import logging
import socket

import httpx
import redis.asyncio as aioredis

from . import metrics, llm, bus, reporter

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("fleet-agent")

NODE_LABEL = os.environ["NODE_LABEL"]
REDIS_URL = os.environ.get("REDIS_URL", "redis://dokploy-redis:6379")
BACKEND_URL = os.environ.get("FLEETDEPLOY_BACKEND_URL", "")
AGENT_SECRET = os.environ.get("AGENT_SECRET", "")
SCALE_CPU_THRESHOLD = float(os.environ.get("SCALE_CPU_THRESHOLD", "80"))
SCALE_CONSECUTIVE = int(os.environ.get("SCALE_CONSECUTIVE", "3"))
LOOP_INTERVAL = int(os.environ.get("LOOP_INTERVAL", "30"))


async def main():
    log.info(f"Fleet agent starting — node={NODE_LABEL}")
    redis = aioredis.from_url(REDIS_URL, decode_responses=True)
    headers = {"X-Agent-Secret": AGENT_SECRET}
    high_cpu_streak = 0

    async with httpx.AsyncClient(base_url=BACKEND_URL, headers=headers, timeout=15) as client:
        while True:
            try:
                m = await metrics.collect()
                peers = await bus.fetch_peers_from_backend(client)
                await bus.get_all_peers(redis)  # also try local Redis, updates cache silently

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

                if m["cpu_pct"] > SCALE_CPU_THRESHOLD:
                    high_cpu_streak += 1
                    if high_cpu_streak >= SCALE_CONSECUTIVE:
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

                ip = socket.gethostbyname(socket.gethostname())
                state = {**m, "status": status, "task": task, "node_label": NODE_LABEL, "ip": ip}
                await bus.publish_state(redis, NODE_LABEL, state)

                hb = await reporter.heartbeat(client, NODE_LABEL, m, status, task, peers)
                directive    = hb.get("directive")   if hb else None
                directive_id = hb.get("directive_id") if hb else None
                if directive:
                    log.info(f"Directive received: {directive}")
                    task = "executing"
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
