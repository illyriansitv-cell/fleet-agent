"""
Execution engine for fleet-agent directives.

Phase 1 — Claude classifies the directive into a structured JSON action.
Phase 2 — The action is validated against an allowlist and executed
           against the local Docker daemon via the mounted socket.

All shell arguments are passed through shlex.quote(). Only pre-defined
action types can run. Unknown or ambiguous directives become a noop.
"""
import asyncio
import json
import logging
import os
import re
import shlex

log = logging.getLogger("fleet-agent.executor")

NODE_LABEL = os.environ.get("NODE_LABEL", "?")

# Hard timeout per Docker command (seconds)
CMD_TIMEOUT = 60

# Allowed action types — anything not in this set is rejected at runtime
ALLOWED_ACTIONS = {
    "service_restart",    # docker service update --force <svc>
    "service_scale",      # docker service scale <svc>=<n>
    "service_rollback",   # docker service rollback <svc>
    "service_logs",       # docker service logs <svc> --tail N
    "service_list",       # docker service ls
    "service_inspect",    # docker service inspect <svc> --pretty
    "container_restart",  # docker restart <container>
    "container_list",     # docker ps
    "image_pull",         # docker pull <image>  (allowlisted registries only)
    "stack_list",         # docker stack ls
    "disk_cleanup",       # docker system prune -f
    "node_status",        # docker node ls
    "noop",               # acknowledge, take no action
}

# Registry allowlist for image_pull — prevents arbitrary pulls
_SAFE_REGISTRIES = {
    "ghcr.io/imgproxy/imgproxy",
    "ollama/ollama",
    "mongo",
    "redis",
    "nginx",
    "13.140.141.3:5000",  # local Swarm registry
}

_SAFE_NAME = re.compile(r"^[a-zA-Z0-9_\-\.:/]+$")
_SAFE_INT  = re.compile(r"^\d+$")


def _safe_name(s: str) -> bool:
    return bool(s) and bool(_SAFE_NAME.match(s))


def _safe_int(s: str) -> bool:
    return bool(s) and bool(_SAFE_INT.match(s))


async def _exec(cmd: str, timeout: int = CMD_TIMEOUT) -> tuple[int, str, str]:
    try:
        proc = await asyncio.create_subprocess_shell(
            cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=timeout)
        return (proc.returncode or 0), stdout.decode().strip(), stderr.decode().strip()
    except asyncio.TimeoutError:
        return 1, "", f"Command timed out after {timeout}s"
    except Exception as e:
        return 1, "", str(e)


async def _dispatch(action: dict) -> dict:
    """Execute a validated action dict. Returns {ok, output}."""
    atype    = action.get("action_type", "noop")
    svc      = str(action.get("service_name", "")).strip()
    ctr      = str(action.get("container_name", "")).strip()
    image    = str(action.get("image", "")).strip()
    replicas = str(action.get("replicas") or "").strip()
    tail     = str(action.get("tail") or "50").strip()

    if atype not in ALLOWED_ACTIONS:
        return {"ok": False, "output": f"Action '{atype}' not in allowlist — blocked."}

    if atype == "noop":
        return {"ok": True, "output": "Acknowledged — no action required."}

    if atype == "service_restart":
        if not _safe_name(svc):
            return {"ok": False, "output": f"Unsafe service name: {svc!r}"}
        rc, out, err = await _exec(f"docker service update --force {shlex.quote(svc)}")
        return {"ok": rc == 0, "output": out or err}

    if atype == "service_scale":
        if not _safe_name(svc) or not _safe_int(replicas):
            return {"ok": False, "output": f"Invalid: svc={svc!r} replicas={replicas!r}"}
        rc, out, err = await _exec(f"docker service scale {shlex.quote(svc)}={replicas}")
        return {"ok": rc == 0, "output": out or err}

    if atype == "service_rollback":
        if not _safe_name(svc):
            return {"ok": False, "output": f"Unsafe service name: {svc!r}"}
        rc, out, err = await _exec(f"docker service rollback {shlex.quote(svc)}")
        return {"ok": rc == 0, "output": out or err}

    if atype == "service_logs":
        if not _safe_name(svc):
            return {"ok": False, "output": f"Unsafe service name: {svc!r}"}
        n = tail if _safe_int(tail) else "50"
        rc, out, err = await _exec(
            f"docker service logs {shlex.quote(svc)} --tail {n} --no-trunc 2>&1",
            timeout=20,
        )
        return {"ok": True, "output": (out or err)[:3000]}

    if atype == "service_inspect":
        if not _safe_name(svc):
            return {"ok": False, "output": f"Unsafe service name: {svc!r}"}
        rc, out, err = await _exec(f"docker service inspect {shlex.quote(svc)} --pretty 2>&1")
        return {"ok": rc == 0, "output": (out or err)[:2000]}

    if atype == "service_list":
        rc, out, err = await _exec("docker service ls 2>&1")
        return {"ok": rc == 0, "output": out or err}

    if atype == "container_restart":
        if not _safe_name(ctr):
            return {"ok": False, "output": f"Unsafe container name: {ctr!r}"}
        rc, out, err = await _exec(f"docker restart {shlex.quote(ctr)}")
        return {"ok": rc == 0, "output": out or err or "restarted"}

    if atype == "container_list":
        rc, out, err = await _exec(
            "docker ps --format 'table {{.Names}}\t{{.Status}}\t{{.Image}}' 2>&1"
        )
        return {"ok": rc == 0, "output": out or err}

    if atype == "image_pull":
        if not _safe_name(image):
            return {"ok": False, "output": f"Unsafe image name: {image!r}"}
        if not any(image.startswith(r) for r in _SAFE_REGISTRIES):
            return {"ok": False, "output": f"Image registry not in allowlist: {image!r}"}
        rc, out, err = await _exec(f"docker pull {shlex.quote(image)}", timeout=120)
        return {"ok": rc == 0, "output": out or err}

    if atype == "stack_list":
        rc, out, err = await _exec("docker stack ls 2>&1")
        return {"ok": rc == 0, "output": out or err}

    if atype == "disk_cleanup":
        rc, out, err = await _exec("docker system prune -f 2>&1", timeout=120)
        return {"ok": rc == 0, "output": out or err}

    if atype == "node_status":
        rc, out, err = await _exec("docker node ls 2>&1")
        return {"ok": rc == 0, "output": out or err}

    return {"ok": False, "output": f"Unhandled action type: {atype}"}


_CLASSIFY_SYSTEM = """You are the execution engine for a Docker Swarm fleet agent.
Parse the operator's directive and return ONLY a single valid JSON object — no markdown, no explanation.

Allowed action_types: """ + ", ".join(sorted(ALLOWED_ACTIONS)) + """

JSON schema:
{
  "action_type": "<one of the allowed types>",
  "service_name": "<full Swarm service name e.g. global-services_imgproxy, or empty>",
  "container_name": "<standalone container name, or empty>",
  "image": "<full image:tag for image_pull, or empty>",
  "replicas": <integer for service_scale, else null>,
  "tail": <integer lines for service_logs, default 50>,
  "reasoning": "<one sentence explanation>"
}

Rules:
- Swarm services → service_* actions (names like global-services_imgproxy, aura-continental-backend)
- Standalone containers → container_restart
- Vague / informational directives → noop
- disk_cleanup only if operator explicitly says clean / prune Docker
- Never invent image names for image_pull
- service_restart = rolling restart (--force update), use this for "fix", "restart", "bounce"
"""


async def execute_directive(directive: str, metrics: dict, claude) -> dict:
    """
    Two-phase execution:
      1. Claude classifies the directive → structured JSON action
      2. _dispatch() validates + runs the action

    Returns:
      action_type, reasoning, ok, output, raw_plan
    """
    user_msg = (
        f"Node: {NODE_LABEL}  "
        f"CPU={metrics.get('cpu_pct')}%  MEM={metrics.get('mem_pct')}%  "
        f"disk={metrics.get('disk_pct')}%  containers={metrics.get('containers_running')}\n\n"
        f"Directive: {directive}"
    )

    raw_plan = ""
    action: dict = {"action_type": "noop"}

    try:
        msg = await claude.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=300,
            system=_CLASSIFY_SYSTEM,
            messages=[{"role": "user", "content": user_msg}],
        )
        raw_plan = msg.content[0].text.strip()
        # Strip markdown code fences if Claude adds them
        if raw_plan.startswith("```"):
            raw_plan = raw_plan.split("```")[1]
            if raw_plan.startswith("json\n"):
                raw_plan = raw_plan[5:]
        action = json.loads(raw_plan)
        log.info(
            "Directive classified → action=%s service=%s container=%s reason=%s",
            action.get("action_type"), action.get("service_name"),
            action.get("container_name"), action.get("reasoning"),
        )
    except json.JSONDecodeError as e:
        log.warning("Claude returned non-JSON: %s | raw=%s", e, raw_plan[:200])
        return {
            "action_type": "noop", "reasoning": "parse error",
            "ok": False,
            "output": f"Could not parse Claude response: {raw_plan[:300]}",
            "raw_plan": raw_plan,
        }
    except Exception as e:
        log.warning("Claude classify error: %s", e)
        return {
            "action_type": "noop", "reasoning": "llm unavailable",
            "ok": False, "output": f"LLM error: {e}", "raw_plan": "",
        }

    result = await _dispatch(action)

    if result["ok"]:
        log.info("Executed %s OK: %s", action.get("action_type"), result["output"][:120])
    else:
        log.warning("Execution FAILED %s: %s", action.get("action_type"), result["output"][:120])

    return {
        "action_type": action.get("action_type", "noop"),
        "reasoning":   action.get("reasoning", ""),
        "ok":          result["ok"],
        "output":      result["output"],
        "raw_plan":    raw_plan,
    }
