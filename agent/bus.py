import json
import logging

log = logging.getLogger("fleet-agent.bus")

GOSSIP_KEY = "fleet:gossip"
GOSSIP_TTL = 180

# In-memory peer cache — populated from backend /admin/agents each cycle
_peer_cache: dict = {}


async def publish_state(r, node_label: str, state: dict) -> None:
    """Try local Redis first; silently skip if unavailable (cross-node Redis not reachable)."""
    try:
        await r.hset(GOSSIP_KEY, node_label, json.dumps(state))
        await r.expire(GOSSIP_KEY, GOSSIP_TTL)
    except Exception:
        pass  # Redis unavailable on this node — backend is source of truth


def get_cached_peers() -> dict:
    """Return in-memory peer cache without any I/O."""
    return dict(_peer_cache)


async def get_all_peers(r) -> dict:
    """Return peer cache (populated by fetch_peers_from_backend)."""
    try:
        raw = await r.hgetall(GOSSIP_KEY)
        peers = {}
        for k, v in raw.items():
            try:
                peers[k] = json.loads(v)
            except Exception:
                pass
        if peers:
            _peer_cache.update(peers)
    except Exception:
        pass
    return dict(_peer_cache)


_GOSSIP_KEEP = {"node_label", "ip", "cpu_pct", "mem_pct", "disk_pct",
                "containers_running", "status", "current_task", "last_seen"}


async def fetch_peers_from_backend(client) -> dict:
    """Fetch all agent states from FleetDeploy backend and cache them."""
    global _peer_cache
    try:
        resp = await client.get("/admin/agents",
                                headers={"X-Admin-Secret": "Apple22244"})
        data = resp.json()
        _peer_cache = {
            a["node_label"]: {k: v for k, v in a.items() if k in _GOSSIP_KEEP}
            for a in (data.get("agents") or [])
        }
    except Exception as e:
        log.debug(f"Peer fetch failed: {e}")
    return dict(_peer_cache)
