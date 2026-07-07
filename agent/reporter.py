import logging

log = logging.getLogger("fleet-agent.reporter")


async def heartbeat(client, node_label: str, metrics: dict, status: str, task: str, gossip: dict):
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
        data = resp.json()
        return data.get("directive") or None
    except Exception as e:
        log.warning(f"Heartbeat failed: {e}")
        return None


async def log_event(client, node_label: str, event_type: str, message: str, metadata: dict = {}) -> None:
    try:
        await client.post("/agents/events", json={
            "node_label": node_label,
            "event_type": event_type,
            "message": message,
            "metadata": metadata,
        })
    except Exception as e:
        log.warning(f"Event log failed: {e}")


async def trigger_scale_out(client, node_label: str, reason: str) -> None:
    try:
        await client.post("/agents/scale-out", json={
            "node_label": node_label,
            "reason": reason,
        })
    except Exception as e:
        log.warning(f"Scale-out trigger failed: {e}")
