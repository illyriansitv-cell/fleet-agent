import os
import json
import logging
import httpx

log = logging.getLogger("fleet-agent.llm")

NODE_LABEL = os.environ.get("NODE_LABEL", "unknown")
OLLAMA_URL = os.environ.get("OLLAMA_URL", "http://ollama:11434")

# Active config — updated dynamically from heartbeat response
_cfg: dict = {
    "provider": os.environ.get("LLM_PROVIDER", "anthropic"),
    "model":    os.environ.get("LLM_MODEL", "claude-haiku-4-5-20251001"),
    "base_url": os.environ.get("LLM_BASE_URL", ""),
    "api_key":  os.environ.get("LLM_API_KEY") or os.environ.get("CLAUDE_API_KEY", ""),
}


def update_config(cfg: dict) -> None:
    """Called from main.py when heartbeat returns llm_config. Takes effect next LLM call."""
    for k in ("provider", "model", "base_url", "api_key"):
        if cfg.get(k):
            _cfg[k] = cfg[k]
    log.info("LLM config updated: provider=%s model=%s", _cfg["provider"], _cfg["model"])


_prompt_overrides: dict = {}

def update_prompts(prompts: dict) -> None:
    """Called from main.py when heartbeat returns agent_prompts. Overrides hardcoded system prompts."""
    global _prompt_overrides
    _prompt_overrides = prompts or {}

def _get_prompt(key: str, default: str) -> str:
    """Return admin-configured prompt override, or fall back to hardcoded default."""
    return _prompt_overrides.get(key) or default


async def _chat(system_prompt: str, user_msg: str, max_tokens: int = 300) -> str:
    """Provider-agnostic chat completion. Returns text or raises."""
    provider = _cfg["provider"]
    model    = _cfg["model"]
    api_key  = _cfg["api_key"]

    if provider == "anthropic":
        import anthropic
        client = anthropic.AsyncAnthropic(api_key=api_key)
        msg = await client.messages.create(
            model=model, max_tokens=max_tokens,
            system=system_prompt,
            messages=[{"role": "user", "content": user_msg}],
        )
        return msg.content[0].text.strip()

    if provider == "ollama":
        base_url = _cfg.get("base_url") or OLLAMA_URL
        async with httpx.AsyncClient(timeout=30) as c:
            resp = await c.post(
                f"{base_url}/api/generate",
                json={"model": model, "prompt": f"{system_prompt}\n\n{user_msg}", "stream": False},
            )
            resp.raise_for_status()
            return resp.json().get("response", "").strip()

    if provider == "gemini":
        base_url = _cfg.get("base_url") or "https://generativelanguage.googleapis.com/v1beta/openai"
        async with httpx.AsyncClient(timeout=30) as c:
            resp = await c.post(
                f"{base_url}/chat/completions",
                headers={"Authorization": f"Bearer {api_key}"},
                json={
                    "model": model,
                    "messages": [
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": user_msg},
                    ],
                    "max_tokens": max_tokens,
                },
            )
            resp.raise_for_status()
            return resp.json()["choices"][0]["message"]["content"].strip()

    raise ValueError(f"Unknown LLM provider: {provider!r}")


async def list_ollama_models() -> list[dict]:
    """Query local Ollama for installed models. Returns [] if Ollama unreachable."""
    try:
        base_url = _cfg.get("base_url") or OLLAMA_URL
        async with httpx.AsyncClient(timeout=5) as c:
            resp = await c.get(f"{base_url}/api/tags")
            resp.raise_for_status()
            models = resp.json().get("models", [])
            return [
                {"name": m.get("name", ""), "size_gb": round(m.get("size", 0) / 1e9, 1)}
                for m in models
            ]
    except Exception:
        return []


_DEFAULT_CLASSIFY_ERRORS = "You are a server monitoring assistant. Classify these errors in 1-2 sentences."
_DEFAULT_SHOULD_SCALE = "You are a fleet scaling assistant. Answer YES or NO and one sentence why."


async def classify_errors(errors: list[str]) -> str:
    try:
        system = _get_prompt("classify_errors", _DEFAULT_CLASSIFY_ERRORS)
        user   = "\n".join(errors[:20])
        return await _chat(system, user, max_tokens=100)
    except Exception as e:
        log.warning("classify_errors failed: %s", e)
        return "classification unavailable"


async def should_scale_out(metrics: dict, peer_states: dict) -> tuple[bool, str]:
    try:
        peers_summary = {
            k: {"cpu": v.get("cpu_pct"), "mem": v.get("mem_pct"), "status": v.get("status")}
            for k, v in peer_states.items()
            if k != NODE_LABEL
        }
        system = _get_prompt("should_scale_out", _DEFAULT_SHOULD_SCALE)
        user = (
            f"Fleet node {NODE_LABEL}: CPU={metrics['cpu_pct']}%, "
            f"MEM={metrics['mem_pct']}%, containers={metrics['containers_running']}, "
            f"errors_5m={metrics['error_count_5m']}.\n"
            f"Peers: {json.dumps(peers_summary)}.\n"
            "Should we scale out to a new VPS to handle this load?"
        )
        text = await _chat(system, user, max_tokens=100)
        return text.upper().startswith("YES"), text.split("\n")[0]
    except Exception as e:
        log.warning("should_scale_out failed: %s", e)
        return False, f"LLM unavailable: {e}"


async def handle_directive(directive: str, metrics: dict) -> dict:
    """Execute a free-text directive via LLM classification. Returns structured result dict."""
    try:
        from agent.executor import execute_directive
        result = await execute_directive(directive, metrics)
        return result
    except Exception as e:
        log.warning("Directive execution failed: %s", e)
        return {
            "action_type": "noop", "reasoning": str(e),
            "ok": False, "output": f"Execution error: {e}", "raw_plan": "",
        }
