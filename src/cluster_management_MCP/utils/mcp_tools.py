import pathlib
import paramiko
import subprocess
import pandas as pd
from cluster_management_MCP.utils.tool_helpers import _ssh_run, _prometheus_query
from cluster_management_MCP.core.mcp_instance import mcp

from cluster_management_MCP.utils.config import (
    MASTER_HOST, WORKER_HOSTS, ADDITIONAL_HOSTS, KAFKA_HOST,
    SSH_USER, SSH_KEY_PATH, PROMETHEUS_URL, SPARK_HOME,
    CONDA_PATH, SPARK_JOB_DIR, _DATASTREAM_PID_FILE, _DATASTREAM_LOG_FILE,
)




# File tree exploration tool
@mcp.tool()
def ls_filetree(in_dir:str, recursive:bool = False) -> list[dict]:
    '''
    Used to list the contents of the directory on the local machine, provided in the input. 
    returns a string with the content of the directory.

    Args:
        in_dir: the directory path  
        recursive: If true, lists all contents recursively
    
    Returns:
        A list of dicts with name, type for each entry, and full file path to the entry.
        
    Useful home directory:
    - /mnt/c/Users/Carde/

    '''
    result = pathlib.Path(in_dir)
    if not result.exists():
        raise FileNotFoundError()
    if not result.is_dir():
        raise NotADirectoryError()   

    glob_pattern = "**/*" if recursive else "*"
    entries = []
    for entry in result.glob(glob_pattern):
        entries.append({
            "name": entry.name,
            "type": "directory" if entry.is_dir() else "file",
            "path": str(entry.resolve()),
        })
    return entries




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




# ---------------------------------------------------------------------------
# Health check tools
# ---------------------------------------------------------------------------

@mcp.tool()
def get_node_cpu_load() -> dict:
    """
    Query Prometheus for the current CPU load percentage on each cluster node.
    Returns a dict of {instance_address: cpu_load_percent} sorted highest first.
    A value of 85.3 means that node is 85.3% utilised.
    """
    promql = (
        "100 - (avg by(instance) "
        "(rate(node_cpu_seconds_total{mode='idle'}[2m])) * 100)"
    )
    results = _prometheus_query(promql)

    load_map = {}
    for item in results:
        instance = item["metric"].get("instance", "unknown")
        value = round(float(item["value"][1]), 2)
        load_map[instance] = value

    # Sort highest load first so the model can answer "which node is busiest" trivially
    return dict(sorted(load_map.items(), key=lambda x: x[1], reverse=True))


@mcp.tool()
def get_node_memory_usage() -> dict:
    """
    Query Prometheus for available memory as a percentage of total on each node.
    Returns a dict of {instance_address: percent_used}.
    A value of 72.1 means that node is using 72.1% of its RAM.
    """
    promql = (
        "100 - (node_memory_MemAvailable_bytes / node_memory_MemTotal_bytes * 100)"
    )
    results = _prometheus_query(promql)

    memory_map = {}
    for item in results:
        instance = item["metric"].get("instance", "unknown")
        value = round(float(item["value"][1]), 2)
        memory_map[instance] = value

    return dict(sorted(memory_map.items(), key=lambda x: x[1], reverse=True))


@mcp.tool()
def get_nodes_up() -> dict:
    """
    Query Prometheus to determine which cluster nodes are currently reachable.
    Returns a dict of {nodename: {"instance": address, "up": bool}} where up=True
    means the node is being scraped by Prometheus successfully.
    """
    promql = (
        "up{job='prometheus', instance!~'localhost:.*|127.0.0.1:.*'}"
        " * on(instance) group_left(nodename) node_uname_info"
    )
    results = _prometheus_query(promql)

    status_map = {}
    for item in results:
        # Get the IP Address and node name
        instance = item["metric"].get("instance", "unknown")
        nodename = item["metric"].get("nodename", instance)
        is_up = item["value"][1] == "1"
        status_map[nodename] = {"instance": instance, "up": is_up}

    return status_map


@mcp.tool()
def get_cluster_summary() -> dict:
    """
    Return a high-level summary of the entire cluster in a single call.
    Includes: how many nodes are up, CPU load per node, memory usage per node.
    Use this as the first tool to call when answering general health questions.
    """
    nodes_up = get_nodes_up()
    cpu = get_node_cpu_load()
    memory = get_node_memory_usage()

    return {
        "nodes_up": sum(1 for v in nodes_up.values() if v["up"]),
        "nodes_total": len(nodes_up),
        "node_status": nodes_up,
        "cpu_load_percent": cpu,
        "memory_used_percent": memory,
    }


# ---------------------------------------------------------------------------
# Raw PromQL escape hatch — for questions not covered above
# ---------------------------------------------------------------------------

@mcp.tool()
def query_prometheus(promql: str) -> list:
    """
    Execute an arbitrary PromQL instant query against the local Prometheus instance.
    Use this ONLY for monitoring metrics regarding the nodes within the cluster not covered by the other tools.

    Common useful queries:
      - Disk usage:  (1 - node_filesystem_avail_bytes / node_filesystem_size_bytes) * 100
      - Network in:  rate(node_network_receive_bytes_total[2m])
      - System load: node_load1

    Returns the raw Prometheus result list. Each item has a 'metric' label dict
    and a 'value' pair of [timestamp, value_string].
    """
    return _prometheus_query(promql)




# ---------------------------------------------------------------------------
# Ad-hoc command execution and file reading
# ---------------------------------------------------------------------------

_ALL_NODES = "all"


@mcp.tool()
def run_command(command: str, target: str = _ALL_NODES) -> dict:
    """
    Execute an arbitrary shell command on one or all cluster nodes via SSH.

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


@mcp.tool()
def lookup_win_rate(
    attackers: int,
    defenders: int,
    section: str = "raw",
) -> str:
    """
    Look up the attacker win rate for a specific attacker/defender army size from
    the RISK simulation results CSV files on the master node.

    Files are pivot tables saved with DataFrame.to_csv(), index included:
      - "raw"      → risk_results_with_cease_fires.csv  (all battles included)
      - "excluded" → risk_results_resolved.csv           (cease-fire battles excluded)

    Args:
        attackers: Number of attacking armies.
        defenders: Number of defending armies.
        section:   Which file to query: "raw" (default) or "excluded".

    Returns a plain-English string with the win rate and the file it came from.
    """
    if section.lower() == "excluded":
        resolved_path = f"{SPARK_JOB_DIR}/risk_results_resolved.csv"
        section_label = "excluded"
    else:
        resolved_path = f"{SPARK_JOB_DIR}/risk_results_with_cease_fires.csv"
        section_label = "raw"

    # Copy the remote file locally via SFTP, then parse with pandas.
    local_tmp = pathlib.Path(f"/tmp/risk_lookup_{section_label}.csv")
    if not local_tmp.exists():
        try:
            client = paramiko.SSHClient()
            client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            client.connect(MASTER_HOST, username=SSH_USER, key_filename=SSH_KEY_PATH, timeout=10)
            sftp = client.open_sftp()
            sftp.get(resolved_path, str(local_tmp))
            sftp.close()
            client.close()
        except Exception as e:
            return f"Could not copy file from master node: {e}"

    try:
        df = pd.read_csv(local_tmp, index_col=0)
        df.index = df.index.astype(int)
        df.columns = df.columns.astype(int)
    except Exception as e:
        return f'Failed to parse "{section_label}": {e}'

    try:
        value = df.loc[defenders, attackers]
    except KeyError:
        return (
            f'No data for attackers={attackers}, defenders={defenders} '
            f'in "{section_label}". '
            f'Attacker range: {df.columns.min()}–{df.columns.max()}, '
            f'Defender range: {df.index.min()}–{df.index.max()}.'
        )

    return (
        f'Win rate for {attackers} attackers vs {defenders} defenders: {value}% '
        f'(section: "{section_label}")'
    )




@mcp.tool()
def get_kafka_status() -> dict:
    """
    Check whether the Kafka stream is currently running on the Kafka node.
    Returns the systemd service status and current memory usage of the
    Kafka process. Call this before starting or stopping Kafka to confirm
    its current state.
    """
    status = _ssh_run(KAFKA_HOST, "sudo systemctl is-active kafka")
    
    # Get Kafka process memory if running
    mem = _ssh_run(
        KAFKA_HOST,
        "ps aux | grep -E '[k]afka' | awk '{sum += $6} END {printf \"%.0f\", sum/1024}'"
    )

    return {
        "status": status.strip(),          # "active" or "inactive"
        "memory_mb": mem.strip() or "0",
        "running": status.strip() == "active"
    }


@mcp.tool()
def start_kafka(confirm: bool = False) -> str:
    """
    Start the Kafka stream service on the Kafka node.
    Only start Kafka when events are expected — it consumes significant
    RAM on the Raspberry Pi even when idle.

    Args:
        confirm: Must be True to proceed. Prevents accidental starts.
    """
    if not confirm:
        return (
            "Kafka start aborted — pass confirm=True to proceed. "
            "Check current state first with get_kafka_status()."
        )

    out = _ssh_run(KAFKA_HOST, "sudo systemctl start kafka")
    
    # Give it a moment then confirm it came up
    import time
    time.sleep(3)
    status = _ssh_run(KAFKA_HOST, "sudo systemctl is-active kafka")

    return (
        f"Start command issued. Current status: {status.strip()}\n"
        f"Output: {out if out else 'none'}"
    )


@mcp.tool()
def stop_kafka(confirm: bool = False) -> str:
    """
    Stop the Kafka stream service on the Kafka node to free RAM.
    The Spark streaming job that consumes from Kafka should be stopped
    first, otherwise it will log connection errors until Kafka restarts.

    Args:
        confirm: Must be True to proceed. Prevents accidental stops.
    """
    if not confirm:
        return (
            "Kafka stop aborted — pass confirm=True to proceed. "
            "Ensure the Spark streaming job is stopped first."
        )

    out = _ssh_run(KAFKA_HOST, "sudo systemctl stop kafka")
    status = _ssh_run(KAFKA_HOST, "sudo systemctl is-active kafka")

    return (
        f"Stop command issued. Current status: {status.strip()}\n"
        f"Output: {out if out else 'none'}"
    )


@mcp.tool()  
def restart_kafka(confirm: bool = False) -> str:
    """
    Restart the Kafka service on the Kafka node.
    Useful when Kafka is running but consumers are experiencing
    connection issues or lag is building up.

    Args:
        confirm: Must be True to proceed.
    """
    if not confirm:
        return "Kafka restart aborted — pass confirm=True to proceed."

    out = _ssh_run(KAFKA_HOST, "sudo systemctl restart kafka")

    import time
    time.sleep(3)
    status = _ssh_run(KAFKA_HOST, "sudo systemctl is-active kafka")

    return f"Restart complete. Status: {status.strip()}"