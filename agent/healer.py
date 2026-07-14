"""
Local Qwen healer — runs every 5 min to proactively detect and fix VPS health issues.
Always calls Ollama directly, completely independent of the configured LLM provider.
"""
import json
import logging
import os
import re
import shlex

import httpx

log = logging.getLogger("fleet-agent.healer")

OLLAMA_URL = os.environ.get("OLLAMA_URL", "http://ollama:11434")
QWEN_HEAL_MODEL = os.environ.get("QWEN_HEAL_MODEL", "qwen2.5-coder:1.5b")
NODE_LABEL = os.environ.get("NODE_LABEL", "?")

DEFAULT_HEAL_PROMPT = (
    "You are an autonomous VPS health monitor on node {node_label}.\n"
    "Analyze the system state and decide if a fix is needed.\n"
    "Respond ONLY with valid JSON — no markdown, no text outside the JSON.\n\n"
    'If everything looks OK: {"ok": true}\n'
    'If a fix is needed: {"ok": false, "reason": "<what is wrong>", '
    '"action": {"action_type": "<type>", "<param>": "<value>"}}\n\n'
    "Available action_types:\n"
    '- container_restart: restart a stopped/crashed container. Add "container_name": "<name>"\n'
    '- service_restart: restart a Docker Swarm service. Add "service_name": "<name>"\n'
    "- disk_cleanup: run docker system prune -f  (use only when disk > 85%)\n"
    '- shell_safe: run a safe targeted shell command. Add "command": "<cmd>"\n'
    "  Examples: \"docker exec mongo mongosh --eval 'rs.initiate()'\", "
    '"docker network prune -f", "docker volume prune -f"\n\n'
    "NEVER suggest: rm -rf, dd, mkfs, format, drop database, curl|bash, wget|bash\n"
    'When in doubt: respond {"ok": true}\n'
    "Only act on clear, obvious problems you are confident about."
)


async def _run(cmd: str, timeout: int = 10) -> str:
    import asyncio
    try:
        proc = await asyncio.create_subprocess_shell(
            cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=timeout)
        return stdout.decode().strip() or stderr.decode().strip()
    except Exception:
        return ""


async def _get_stopped_containers() -> str:
    out = await _run(
        "docker ps -a --filter status=exited --filter status=dead "
        "--format '{{.Names}} [{{.Status}}]' 2>/dev/null | head -10"
    )
    return out or "none"


async def _get_mongo_status() -> str:
    ctr = await _run(
        "docker ps --format '{{.Names}}' 2>/dev/null | grep -iE 'mongo' | head -1"
    )
    ctr = ctr.strip()
    if not ctr:
        return "not on this node"
    out = await _run(
        f"docker exec {shlex.quote(ctr)} mongosh --quiet --norc "
        "--eval \"try{var s=rs.status();print(s.myState+' '+s.members.map(m=>m.stateStr).join(','))}catch(e){print('err:'+e.message)}\" 2>/dev/null",
        timeout=8,
    )
    result = (out or "query failed").strip()
    # Auth errors mean MongoDB is running and secured — not a problem to fix
    if "requires authentication" in result or "Authentication failed" in result:
        return "running (auth-protected — normal)"
    return result[:200]


async def _get_disk_info() -> str:
    out = await _run("df -h / 2>/dev/null | tail -1")
    return out or "unknown"


async def _get_swarm_down() -> str:
    out = await _run(
        "docker node ls --format '{{.Hostname}} {{.Status}}' 2>/dev/null "
        "| grep -i down | head -5"
    )
    return out or "none"


def _fmt_peers(peers: dict) -> str:
    if not peers:
        return "none"
    parts = []
    for k, v in peers.items():
        if k == NODE_LABEL:
            continue
        parts.append(
            f"{k.upper()}: cpu={v.get('cpu_pct', 0):.0f}% mem={v.get('mem_pct', 0):.0f}% "
            f"status={v.get('status', '?')}"
        )
    return "\n".join(parts) or "none"


async def run_heal_check(
    metrics: dict,
    peers: dict,
    containers: list,
    heal_prompt: str | None = None,
) -> dict | None:
    """
    Run a Qwen health analysis. Returns event-ready dict or None if healthy / Ollama offline.
    """
    stopped = await _get_stopped_containers()
    mongo = await _get_mongo_status()
    disk = await _get_disk_info()
    swarm_down = await _get_swarm_down()

    context = (
        f"NODE:{NODE_LABEL} CPU:{metrics.get('cpu_pct', 0):.0f}% "
        f"MEM:{metrics.get('mem_pct', 0):.0f}% DISK:{metrics.get('disk_pct', 0):.0f}%\n"
        f"ERRORS_5m:{metrics.get('error_count_5m', 0)}\n"
        f"STOPPED:\n{stopped}\n"
        f"SWARM_DOWN:{swarm_down}\n"
        f"MONGO:{mongo}\n"
        f"DISK:{disk}\n"
        f"PEERS:\n{_fmt_peers(peers)}"
    )

    system = (heal_prompt or DEFAULT_HEAL_PROMPT).replace("{node_label}", NODE_LABEL)
    prompt = f"{system}\n\nSYSTEM STATE:\n{context}"

    try:
        async with httpx.AsyncClient(timeout=45) as c:
            resp = await c.post(
                f"{OLLAMA_URL}/api/generate",
                json={"model": QWEN_HEAL_MODEL, "prompt": prompt, "stream": False},
            )
            resp.raise_for_status()
            raw = resp.json().get("response", "").strip()
    except Exception as e:
        log.debug(f"Qwen heal: Ollama unavailable — {e}")
        return None

    # Parse JSON (strip markdown fences if model wraps response)
    try:
        text = raw
        if "```" in text:
            m = re.search(r"```(?:json)?\s*([\s\S]*?)```", text)
            text = m.group(1).strip() if m else text
        m2 = re.search(r"\{[\s\S]*\}", text)
        if m2:
            text = m2.group(0)
        result = json.loads(text)
    except Exception as e:
        log.warning(f"Qwen heal JSON parse failed: {e} | raw={raw[:150]}")
        return None

    if result.get("ok", True):
        log.debug("Qwen heal: all healthy")
        return None

    reason = result.get("reason", "unknown issue")[:120]
    action = result.get("action")

    if not action or not isinstance(action, dict):
        log.info(f"Qwen heal: flagged '{reason}' but returned no action")
        return {
            "acted": False,
            "reason": reason,
            "action_type": "none",
            "output": "no action suggested",
            "ok": False,
        }

    log.info(f"Qwen heal: {reason} → action={action}")

    from agent.executor import _dispatch
    res = await _dispatch(action)

    return {
        "acted": True,
        "reason": reason,
        "action_type": action.get("action_type", "?"),
        "output": res.get("output", "")[:500],
        "ok": res.get("ok", False),
    }
