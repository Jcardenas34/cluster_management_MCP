import subprocess
import pathlib
from cluster_management_MCP.utils.tool_helpers import _ssh_run
from cluster_management_MCP.core.mcp_instance import mcp

from cluster_management_MCP.utils.config import (
    MASTER_HOST, WORKER_HOSTS, ADDITIONAL_HOSTS, KAFKA_HOST,
    SSH_USER, SSH_KEY_PATH, PROMETHEUS_URL, SPARK_HOME,
    CONDA_PATH, SPARK_JOB_DIR, _DATASTREAM_PID_FILE, _DATASTREAM_LOG_FILE,
)


_ALL_NODES = "all"


# File tree exploration tool
# @mcp.tool()
# def ls_filetree(in_dir:str, recursive:bool = False) -> list[dict]:
#     '''
#     Used to list the contents of the directory on the local machine, provided in the input. 
#     returns a string with the content of the directory.

#     Args:
#         in_dir: the directory path  
#         recursive: If true, lists all contents recursively
    
#     Returns:
#         A list of dicts with name, type for each entry, and full file path to the entry.
        
#     Useful home directory:
#     - /mnt/c/Users/Carde/

#     '''
#     result = pathlib.Path(in_dir)
#     if not result.exists():
#         raise FileNotFoundError()
#     if not result.is_dir():
#         raise NotADirectoryError()   

#     glob_pattern = "**/*" if recursive else "*"
#     entries = []
#     for entry in result.glob(glob_pattern):
#         entries.append({
#             "name": entry.name,
#             "type": "directory" if entry.is_dir() else "file",
#             "path": str(entry.resolve()),
#         })
#     return entries

@mcp.tool()
def copy_file_from_remote_host(remote_file_target:str, target_host:str, username:str="chiralpair", destination_path:str="/mnt/c/Users/Carde/Desktop") -> dict:
    """
    Executes an `scp` bash command on this PC, to download a file or directory from a remote host.
    Assume the user's remote_file_target is correct, despite mispellings. But alert the user if 
    of potential typos.
    
    Args:
        remote_file_target: The target file that will be downloaded from the remote host.
        target_host:        The IP address of the remote host we will download the file from.
        username:           The username used to download the file.  (default: chiralpair)
        destination_path:   The location on this PC where the file will be stored. (default '/mnt/c/Users/Carde/Desktop')

    Examples:
        copy_file_from_remote_host(~/fraud_detection/*, 192.168.1.250)
        copy_file_from_remote_host(~/scripts/cv_stream_sample.py, 192.168.1.100, "chiralpair","/mnt/c/Users/Carde/Downloads")

    """
    dest_path = pathlib.Path(destination_path)
    if not dest_path.exists():
        return {"status":"failure","message":f"Destination path {destination_path} on PC does not exist"}

    command = ["scp", "-r",
               f"{username}@{target_host}:{remote_file_target}",
               f"{destination_path}"]
    try:
        result = subprocess.run(command, capture_output=True, timeout=60, text=True, check=True)
        return {"status":"success","message":f"File successfully downloaded to {destination_path}"}
    except subprocess.TimeoutExpired as e:
        return {"status":"failure","message":"scp command timed out."}
    except FileNotFoundError as e:
        return {"status":"failure","message":f"File not present {e}"}
    except subprocess.CalledProcessError as e:
        if "could not resolve" in e.stderr.lower():
            return {"status":"failure","message":"Host is not reachable"}
    except Exception as e:
        return {"status":"error","message": e}
    
    

@mcp.tool()
def run_command(command: str, target: str = _ALL_NODES) -> dict:
    """
    Execute an arbitrary shell command on one or all cluster nodes via SSH.
    Use this for file tree exploration, remote file downloads, RAM usage of a particular node,
    available disk space of a node, etc.

    Args:
        command: The shell command to run (e.g. 'df -h', 'uptime', 'ls /opt/spark').
        target:  Which node(s) to run on. Options:
                   - "all"            — master + all workers (default)
                   - "master"         — master node only
                   - "workers"        — all worker nodes only
                   - "<ip-or-host>"   — a specific node by hostname or IP address

    Returns a dict of {node: output_string} for each targeted host.

    Examples:
        run_command("df -h")                              # disk usage on all nodes
        run_command("uptime", target="master")            # load average on master only
        run_command("free -m", target="workers")          # RAM on workers only
        run_command("ls /opt/spark", target="192.168.1.50")  # specific node
        run_command("ls ~", target="192.168.1.9")         # Explore the contents of the user's home directory
    """
    if target == "all":
        hosts = [MASTER_HOST] + WORKER_HOSTS
    elif target == "master":
        hosts = [MASTER_HOST]
    elif target == "workers":
        hosts = list(WORKER_HOSTS)
    else:
        hosts = [target]

    return {host: _ssh_run(host, command) for host in hosts}


@mcp.tool()
def shutdown_cluster(delay_minutes: int = 0) -> str:
    """
    Power off all cluster nodes (workers first, then master).

    This is a hard OS-level shutdown — not just stopping Spark services.
    The machines will fully power down and require a physical or remote
    wake event to come back online.

    Args:       
        delay_minutes: Minutes to wait before powering off (default 0 = immediately).
                       Use a non-zero value to give running jobs time to finish.

    Returns a status string summarising the shutdown command sent to each host.

    WARNING: Call stop_spark_cluster first if you want a graceful Spark shutdown
    before the OS powers off.
    """
    timing = "now" if delay_minutes == 0 else f"+{delay_minutes}"
    cmd = f"sudo shutdown -h {timing}"
    results = []

    for worker in WORKER_HOSTS:
        out = _ssh_run(worker, cmd)
        results.append(f"[worker {worker}]: shutdown scheduled ({timing}) — {out}")

    for node in ADDITIONAL_HOSTS:
        out = _ssh_run(node, cmd)
        results.append(f"[node {node}]: shutdown scheduled ({timing}) — {out}")
    

    out = _ssh_run(MASTER_HOST, cmd)
    results.append(f"[master {MASTER_HOST}]: shutdown scheduled ({timing}) — {out}")

    return "\n".join(results)

@mcp.tool()
def read_remote_file(path: str, target: str = "master") -> dict:
    """
    Read the contents of a file on a remote cluster node.

    Useful for inspecting files, logs, or any text file on a node.
    

    Args:
        path:   Absolute path to the file on the remote host
                (e.g. '/home/chiralpair/spark_scripts/risk_results.csv').
        target: Which node to read from. Options:
                   - "master"         — master node (default)
                   - "workers"        — read the same path from all worker nodes
                   - "all"            — master + all workers
                   - "<ip-or-host>"   — a specific node by hostname or IP address

    Returns a dict of {node: file_contents_string} for each targeted host.
    If the file does not exist on a node, the value will contain the error message.

    Examples:
        read_remote_file("/home/chiralpair/spark_scripts/risk_results.csv")
        read_remote_file("/var/log/syslog", target="workers")
        read_remote_file("/opt/spark/logs/spark-master.out", target="master")
    """
    if target == "all":
        hosts = [MASTER_HOST] + WORKER_HOSTS
    elif target == "master":
        hosts = [MASTER_HOST]
    elif target == "workers":
        hosts = list(WORKER_HOSTS)
    else:
        hosts = [target]

    return {host: _ssh_run(host, f"cat {path}") for host in hosts}

