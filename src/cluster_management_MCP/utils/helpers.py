''' Helper functions to facilitate processes within the application. '''

import sys
import time
import subprocess
from subprocess import Popen
from pathlib import Path

def _check_paths(in_dir: Path) -> None:
    """Make sure that there is a Frontend directory present"""
    if not in_dir.is_dir():
        print(f"[launch-cluster] ERROR:  dir not found at {in_dir}", file=sys.stderr)
        sys.exit(1)


def _terminate(procs: list[Popen], time_limit:int) -> None:
    """
    Sets a timer for subprocesses to finish in 5 seconds. 
    Terminates them if they have completed, and 
    or if they take over 5 seconds.
    """

    for p in procs:
        if p.poll() is None: 
            p.terminate()
            
    # Processes should finish in 'time_limit' seconds at most
    deadline = time.monotonic() + time_limit
    for p in procs:
        remaining = deadline - time.monotonic()
        try:
            p.wait(timeout=max(remaining, 0.1))
        except subprocess.TimeoutExpired:
            p.kill()
            p.wait()


def open_ssh_tunnel(host: str, user: str, key_path: str, port: int,
                    local_port: int | None = None) -> Popen:
    """
    Forward localhost:`local_port` (default `port`) to `port` on `host`'s loopback
    interface (ssh -L local_port:localhost:port). Used for the MLflow server, which only listens
    on 127.0.0.1. Fails fast instead of prompting (BatchMode) or running without
    the forward (ExitOnForwardFailure). Caller terminates the returned process.
    """
    return Popen(
        [
            "ssh", "-N",
            "-L", f"{local_port or port}:localhost:{port}",
            "-i", str(Path(key_path).expanduser()),
            "-o", "BatchMode=yes",
            "-o", "ExitOnForwardFailure=yes",
            "-o", "ServerAliveInterval=30",
            "-o", "ConnectTimeout=10",
            f"{user}@{host}",
        ],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        text=True,
    )


async def preload_ollama_model(base_url: str | None, model: str,
                               keep_alive: int | str, num_ctx: int) -> None:
    """
    Load `model` into Ollama memory ahead of the first query.
    An /api/generate request with no prompt only loads the model. num_ctx must
    match the chat client's, otherwise Ollama reloads the model on the first query.
    """
    import httpx

    base = (base_url or "http://localhost:11434").rstrip("/")
    if "://" not in base:  # OLLAMA_HOST may be given as host:port
        base = f"http://{base}"
    url = f"{base}/api/generate"
    payload = {"model": model, "keep_alive": keep_alive, "options": {"num_ctx": num_ctx}}
    async with httpx.AsyncClient(timeout=300) as client:
        r = await client.post(url, json=payload)
        r.raise_for_status()
