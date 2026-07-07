import os
import json
import logging
import httpx
import anthropic

log = logging.getLogger("fleet-agent.llm")

NODE_LABEL = os.environ.get("NODE_LABEL", "unknown")
OLLAMA_URL = os.environ.get("OLLAMA_URL", "http://ollama:11434")
_claude = None


def _get_claude() -> anthropic.AsyncAnthropic:
    global _claude
    if _claude is None:
        _claude = anthropic.AsyncAnthropic(api_key=os.environ["CLAUDE_API_KEY"])
    return _claude


async def classify_errors(errors: list[str]) -> str:
    try:
        prompt = (
            "You are a server monitoring assistant. "
            "Classify these errors in 1-2 sentences:\n"
            + "\n".join(errors[:20])
        )
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.post(
                f"{OLLAMA_URL}/api/generate",
                json={"model": "qwen2.5:0.5b", "prompt": prompt, "stream": False},
            )
            resp.raise_for_status()
            return resp.json().get("response", "").strip() or "classification unavailable"
    except Exception as e:
        log.warning(f"Ollama classify failed: {e}")
        return "classification unavailable"


async def should_scale_out(metrics: dict, peer_states: dict) -> tuple[bool, str]:
    try:
        peers_summary = {
            k: {"cpu": v.get("cpu_pct"), "mem": v.get("mem_pct"), "status": v.get("status")}
            for k, v in peer_states.items()
            if k != NODE_LABEL
        }
        prompt = (
            f"Fleet node {NODE_LABEL} metrics: CPU={metrics['cpu_pct']}%, "
            f"MEM={metrics['mem_pct']}%, containers={metrics['containers_running']}, "
            f"errors_5m={metrics['error_count_5m']}.\n"
            f"Peer nodes: {json.dumps(peers_summary)}.\n"
            "Should we scale out to a new Hetzner VPS to handle this load? "
            "Answer YES or NO and one sentence why."
        )
        claude = _get_claude()
        msg = await claude.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=100,
            messages=[{"role": "user", "content": prompt}],
        )
        text = msg.content[0].text.strip()
        should = text.upper().startswith("YES")
        reason = text.split("\n")[0] if "\n" in text else text
        return should, reason
    except Exception as e:
        log.warning(f"Claude scale-out check failed: {e}")
        return False, f"LLM unavailable: {e}"


async def handle_directive(directive: str, metrics: dict) -> dict:
    """Execute a directive. Returns structured result dict."""
    try:
        from agent.executor import execute_directive
        claude = _get_claude()
        result = await execute_directive(directive, metrics, claude)
        return result
    except Exception as e:
        log.warning(f"Directive execution failed: {e}")
        return {
            "action_type": "noop", "reasoning": str(e),
            "ok": False, "output": f"Execution error: {e}", "raw_plan": "",
        }
