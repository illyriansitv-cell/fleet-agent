import json
import logging

log = logging.getLogger("fleet-agent.bus")

GOSSIP_KEY = "fleet:gossip"
GOSSIP_TTL = 180


async def publish_state(r, node_label: str, state: dict) -> None:
    try:
        await r.hset(GOSSIP_KEY, node_label, json.dumps(state))
        await r.expire(GOSSIP_KEY, GOSSIP_TTL)
    except Exception as e:
        log.warning(f"Redis publish failed: {e}")


async def get_all_peers(r) -> dict:
    try:
        raw = await r.hgetall(GOSSIP_KEY)
        peers = {}
        for k, v in raw.items():
            try:
                peers[k] = json.loads(v)
            except Exception:
                pass
        return peers
    except Exception as e:
        log.warning(f"Redis getall failed: {e}")
        return {}
