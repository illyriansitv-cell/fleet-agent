"""
Local Qwen healer — runs every 5 min to proactively detect and fix VPS health issues.
Always calls Ollama directly, completely independent of the configured LLM provider.
"""
import asyncio
import json
import logging
import os
import re
import shlex
import ssl
import socket
from datetime import datetime

import httpx

log = logging.getLogger("fleet-agent.healer")

OLLAMA_URL = os.environ.get("OLLAMA_URL", "http://ollama:11434")
QWEN_HEAL_MODEL = os.environ.get("QWEN_HEAL_MODEL", "qwen2.5-coder:1.5b")
NODE_LABEL = os.environ.get("NODE_LABEL", "?")

# Domains to monitor for SSL expiry and HTTP reachability
MONITOR_DOMAINS = os.environ.get("MONITOR_DOMAINS", "ulitar.com").split(",")

DEFAULT_HEAL_PROMPT = (
    "You are an autonomous VPS health monitor on node {node_label}.\n"
    "Analyze the system state and decide if a fix is needed.\n"
    "Respond ONLY with valid JSON — no markdown, no text outside the JSON.\n\n"
    'If everything looks OK: {"ok": true}\n'
    'If a fix is needed: {"ok": false, "reason": "<what is wrong>", '
    '"action": {"action_type": "<type>", "<param>": "<value>"}}\n\n'
    "Available action_types:\n"
    '- container_restart: restart a stopped/OOM-killed container. Add "container_name": "<name>"\n'
    '- service_restart: restart a Docker Swarm service. Add "service_name": "<name>"\n'
    "- disk_cleanup: run docker system prune -f  (only when disk > 85%)\n"
    '- shell_safe: run a safe targeted command. Add "command": "<cmd>"\n'
    "  Examples: \"docker network prune -f\", \"docker volume prune -f\"\n\n"
    "INFO-ONLY (do not try to fix — just log if bad):\n"
    "- SSL expiry warnings: these are external, no local fix possible\n"
    "- HTTP reachability failures: network/upstream issue, no local fix\n"
    "  For these: respond {\"ok\": false, \"reason\": \"<issue>\", \"action\": null}\n\n"
    "NEVER suggest: rm -rf, dd, mkfs, format, drop database, curl|bash, wget|bash\n"
    'When in doubt: respond {"ok": true}\n'
    "Only act on clear, obvious problems you are confident about."
)


async def _run(cmd: str, timeout: int = 10) -> str:
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


async def _get_oom_killed() -> str:
    """Find containers killed by OOM — exit code 137 or OOMKilled=true."""
    out = await _run(
        "docker ps -a -q 2>/dev/null | xargs -r docker inspect "
        "--format '{{.Name}} restarts={{.RestartCount}} oom={{.State.OOMKilled}}' 2>/dev/null "
        "| grep 'oom=true' | sed 's|/||' | head -5"
    )
    return out.strip() or "none"


async def _get_mongo_status() -> str:
    ctr = (await _run(
        "docker ps --format '{{.Names}}' 2>/dev/null | grep -iE 'mongo' | head -1"
    )).strip()
    if not ctr:
        return "not on this node"
    out = await _run(
        f"docker exec {shlex.quote(ctr)} mongosh --quiet --norc "
        "--eval \"try{var s=rs.status();print(s.myState+' '+s.members.map(m=>m.stateStr).join(','))}catch(e){print('err:'+e.message)}\" 2>/dev/null",
        timeout=8,
    )
    result = (out or "query failed").strip()
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


async def _check_ssl(domains: list[str]) -> str:
    """Check SSL cert expiry using Python ssl — no openssl CLI needed."""
    results = []
    for domain in domains:
        try:
            ctx = ssl.create_default_context()
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(domain, 443, ssl=ctx, server_hostname=domain),
                timeout=8,
            )
            cert = writer.get_extra_info("ssl_object").getpeercert()
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:
                pass
            exp = datetime.strptime(cert["notAfter"], "%b %d %H:%M:%S %Y %Z")
            days = (exp - datetime.utcnow()).days
            results.append(f"{domain}:{'OK(' + str(days) + 'd)' if days >= 14 else 'EXPIRING IN ' + str(days) + 'd!'}")
        except Exception:
            results.append(f"{domain}:check-failed")
    return " ".join(results) or "skipped"


async def _check_http(domains: list[str]) -> str:
    """HTTP reachability check — report failures only."""
    results = []
    async with httpx.AsyncClient(timeout=10, follow_redirects=True) as c:
        for domain in domains:
            try:
                r = await c.get(f"https://{domain}")
                if r.status_code >= 500:
                    results.append(f"{domain}:{r.status_code}")
            except Exception:
                results.append(f"{domain}:UNREACHABLE")
    return " ".join(results) if results else "all reachable"


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
    stopped, oom, mongo, disk, swarm_down, ssl_status, http_status = await asyncio.gather(
        _get_stopped_containers(),
        _get_oom_killed(),
        _get_mongo_status(),
        _get_disk_info(),
        _get_swarm_down(),
        _check_ssl(MONITOR_DOMAINS),
        _check_http(MONITOR_DOMAINS),
    )

    context = (
        f"NODE:{NODE_LABEL} CPU:{metrics.get('cpu_pct', 0):.0f}% "
        f"MEM:{metrics.get('mem_pct', 0):.0f}% DISK:{metrics.get('disk_pct', 0):.0f}%\n"
        f"ERRORS_5m:{metrics.get('error_count_5m', 0)}\n"
        f"STOPPED:\n{stopped}\n"
        f"OOM_KILLED:{oom}\n"
        f"SWARM_DOWN:{swarm_down}\n"
        f"MONGO:{mongo}\n"
        f"DISK:{disk}\n"
        f"SSL:{ssl_status}\n"
        f"HTTP:{http_status}\n"
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
        # Info-only event (SSL expiry, HTTP issue, etc.)
        log.info(f"Qwen heal: flagged '{reason}' — no action")
        return {
            "acted": False,
            "reason": reason,
            "action_type": "none",
            "output": "info only — no local fix possible",
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


async def backup_mongodb() -> str | None:
    """
    Daily MongoDB backup to /var/backups/mongodb/ — keep last 7 days.
    Called from main.py on DE node only.
    """
    ctr = (await _run(
        "docker ps --format '{{.Names}}' 2>/dev/null | grep -i mongo | head -1"
    )).strip()
    if not ctr:
        return None

    date_str = datetime.utcnow().strftime("%Y%m%d")
    backup_dir = "/var/backups/mongodb"
    backup_file = f"{backup_dir}/backup-{date_str}.gz"

    await _run(f"mkdir -p {backup_dir}")

    # Try to get credentials from container environment
    env_raw = await _run(
        f"docker inspect {shlex.quote(ctr)} --format '{{{{json .Config.Env}}}}' 2>/dev/null"
    )
    username = password = ""
    try:
        for ev in json.loads(env_raw):
            if ev.startswith("MONGO_INITDB_ROOT_USERNAME="):
                username = ev.split("=", 1)[1]
            elif ev.startswith("MONGO_INITDB_ROOT_PASSWORD="):
                password = ev.split("=", 1)[1]
    except Exception:
        pass

    auth = (
        f"--username {shlex.quote(username)} --password {shlex.quote(password)} "
        "--authenticationDatabase admin"
        if username and password else ""
    )

    try:
        proc = await asyncio.create_subprocess_shell(
            f"docker exec {shlex.quote(ctr)} mongodump {auth} --archive --gzip 2>/dev/null "
            f"> {backup_file}",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        await asyncio.wait_for(proc.communicate(), timeout=300)
    except asyncio.TimeoutError:
        return "MongoDB backup timed out after 5 min"
    except Exception as e:
        return f"MongoDB backup error: {e}"

    # Keep last 7 backups
    await _run(
        f"ls -t {backup_dir}/backup-*.gz 2>/dev/null | tail -n +8 | xargs rm -f 2>/dev/null || true"
    )

    size = await _run(f"du -sh {backup_file} 2>/dev/null | cut -f1")
    return f"MongoDB backup: {size or '?'} → {backup_file}"
