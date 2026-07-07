import asyncio
import psutil


async def _run(cmd: str) -> str:
    proc = await asyncio.create_subprocess_shell(
        cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, _ = await proc.communicate()
    return stdout.decode().strip()


async def collect() -> dict:
    cpu_pct = psutil.cpu_percent(interval=1)
    mem = psutil.virtual_memory()
    disk = psutil.disk_usage("/")

    containers_str = await _run("docker ps -q 2>/dev/null | wc -l")
    try:
        containers_running = int(containers_str)
    except ValueError:
        containers_running = 0

    error_count_str = await _run(
        "docker ps -q 2>/dev/null | xargs -r docker logs --since 5m 2>&1 | grep -ci error 2>/dev/null || echo 0"
    )
    try:
        error_count_5m = int(error_count_str)
    except ValueError:
        error_count_5m = 0

    errors_raw = await _run(
        "docker ps -q 2>/dev/null | xargs -I{} docker logs --since 5m {} 2>&1 | grep -i error | tail -30"
    )
    recent_errors = [l for l in errors_raw.splitlines() if l.strip()] if errors_raw else []

    return {
        "cpu_pct": round(cpu_pct, 1),
        "mem_pct": round(mem.percent, 1),
        "disk_pct": round(disk.percent, 1),
        "containers_running": containers_running,
        "error_count_5m": error_count_5m,
        "recent_errors": recent_errors[:30],
    }
