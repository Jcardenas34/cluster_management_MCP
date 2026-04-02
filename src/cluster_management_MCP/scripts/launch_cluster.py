"""launch-cluster entry point — starts uvicorn backend + vite frontend concurrently."""

from subprocess import Popen
import sys
import time
from pathlib import Path
from cluster_management_MCP.utils.helpers import _check_paths, _terminate 

# Definining correct places to run the front and backend
_SCRIPTS_DIR = Path(__file__).resolve().parent           # .../src/cluster_management_MCP/scripts
_REPO_ROOT   = _SCRIPTS_DIR.parent.parent.parent         # repo root
FRONTEND_DIR = _REPO_ROOT / "frontend"                   


def main() -> None:
    # Check if the frontend is present
    _check_paths(FRONTEND_DIR)

    # Define commands to initialize front and backend servers
    backend_cmd = [sys.executable, "-m", "uvicorn",
                   "cluster_management_MCP.scripts.api_server:app",
                   "--reload", "--port", "8000"]
    frontend_cmd = "npm run dev -- --host"


    procs = []
    try:
        print("[launch-cluster] Starting backend  (uvicorn :8000) ...", flush=True)
        backend  = Popen(backend_cmd,  cwd=str(_REPO_ROOT))

        print("[launch-cluster] Starting frontend (vite   :5173) ...",  flush=True)
        frontend = Popen(frontend_cmd, cwd=str(FRONTEND_DIR), shell=True)

        print("[launch-cluster] Press Ctrl+C to stop both.\n",          flush=True)
        procs = [backend, frontend]

        while True:
            for p in procs:
                complete_status = p.poll() # Will other than None if completed/terminated
                if complete_status is not None:
                    name = "backend" if p is backend else "frontend"
                    print(f"\n[launch-cluster] {name} exited ({complete_status}). Stopping other.", file=sys.stderr)
                    _terminate([q for q in procs if q is not p], time_limit=5)
                    sys.exit(complete_status)
            time.sleep(0.5)

    except KeyboardInterrupt:
        print("\n[launch-cluster] Ctrl+C — shutting down ...", file=sys.stderr)
        _terminate(procs, time_limit=5)
        sys.exit(0)


if __name__ == "__main__":
    main()
