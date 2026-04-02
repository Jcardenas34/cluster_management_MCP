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
