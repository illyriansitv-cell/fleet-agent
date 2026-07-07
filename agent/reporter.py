import logging

log = logging.getLogger("fleet-agent.reporter")


async def heartbeat(client, node_label: str, metrics: dict, status: str, task: str, gossip: dict) -> dict:
    try:
        import socket
        ip = socket.gethostbyname(socket.gethostname())
        resp = await client.post("/agents/heartbeat", json={
            "node_label": node_label,
            "ip": ip,
            "cpu_pct": metrics["cpu_pct"],
            "mem_pct": metrics["mem_pct"],
            "disk_pct": metrics["disk_pct"],
            "containers_running": metrics["containers_running"],
            "current_task": task,
            "status": status,
            "node_gossip": gossip,
        })
        return resp.json()
    except Exception as e:
        log.warning(f"Heartbeat failed: {type(e).__name__}: {e!r}")
        return {}


async def log_event(client, node_label: str, event_type: str, message: str, metadata: dict = {}) -> None:
    try:
        await client.post("/agents/events", json={
            "node_label": node_label,
            "event_type": event_type,
            "message": message,
            "metadata": metadata,
        })
    except Exception as e:
        log.warning(f"Event log failed: {type(e).__name__}: {e!r}")


async def complete_directive(client, directive_id: str, result: dict) -> None:
    try:
        await client.post("/agents/directive/complete", json={
            "directive_id": directive_id,
            "action_type": result.get("action_type", ""),
            "ok": bool(result.get("ok")),
            "output": result.get("output", "")[:2000],
        })
    except Exception as e:
        log.warning(f"Directive complete report failed: {e}")


async def trigger_scale_out(client, node_label: str, reason: str) -> None:
    try:
        await client.post("/agents/scale-out", json={
            "node_label": node_label,
            "reason": reason,
        })
    except Exception as e:
        log.warning(f"Scale-out trigger failed: {e}")
