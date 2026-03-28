import os
import httpx
import pathlib
import paramiko
import subprocess
from dotenv import load_dotenv
from cluster_management_MCP.core.mcp_instance import mcp


def get_windows_host_ip():
    '''
    Retrieve ipaddress of local machine via cmd line 
    '''
    result = subprocess.run(
        ["ip", "route", "show", "default"],
        capture_output=True, text=True
    )
    return result.stdout.split()[2]



# Load cluster environment 
load_dotenv() 

MASTER_HOST    = os.getenv("SPARK_MASTER_HOST", "")
WORKER_HOSTS   = os.getenv("SPARK_WORKER_HOSTS", "").split(",")
SSH_USER       = os.getenv("SSH_USER", "")
SSH_KEY_PATH   = os.getenv("SSH_KEY_PATH", "~/.ssh/id_rsa")
PROMETHEUS_URL = f"http://{get_windows_host_ip()}:9090"
SPARK_HOME     = os.getenv("SPARK_HOME", "/opt/spark")
CONDA_PATH     = os.getenv("CONDA_PATH", "")
SPARK_JOB_DIR  = os.getenv("SPARK_JOB_DIR", "")


def _ssh_run(host: str, command: str) -> str:
    """
    Open an SSH connection to `host`, run `command`, return combined stdout/stderr.
    Raises on connection failure so the tool returns a clean error string.
    """
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    try:
        client.connect(host, username=SSH_USER, key_filename=SSH_KEY_PATH, timeout=10)
        _, stdout, stderr = client.exec_command(command)
        out = stdout.read().decode().strip()
        err = stderr.read().decode().strip()
        return out if out else err
    except Exception as e:
        return f"SSH error on {host}: {e}"
    finally:
        client.close()


def _prometheus_query(promql: str) -> list:
    """
    Execute an instant PromQL query. Returns the 'result' list from Prometheus,
    or raises with a descriptive message on HTTP/parse failure.
    """
    try:
        print(f"{PROMETHEUS_URL}/api/v1/query")
        response = httpx.get(
            f"{PROMETHEUS_URL}/api/v1/query",
            params={"query": promql},
            timeout=5,
        )
        response.raise_for_status()
        data = response.json()
        if data["status"] != "success":
            raise ValueError(f"Prometheus returned status: {data['status']}")
        return data["data"]["result"]
    except Exception as e:
        raise RuntimeError(f"Prometheus query failed: {e}")


# File tree exploration tool

# @mcp.tool()
# def ls_filetree(in_dir:str, recursive:bool = False) -> list[dict]:
#     '''
#     Used to list the contents of the directory on the local machine,provided in the input. 
#     returns a string with the content of the directory.

#     Args:
#         in_dir: the directory path  
#         recursive: If true, lists allcontents recursively
    
#     Returns:
#         A list of dicts with name, and type for each entry
        
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
#             "type": "dirctory" if entry.is_dir() else "file",
#             "path": str(entry.resolve()),
#         })
#     return entries

# ---------------------------------------------------------------------------
# Cluster lifecycle tools
# ---------------------------------------------------------------------------

@mcp.tool()
def start_spark_cluster() -> str:
    """
    Start the Spark cluster. Starts the master node first, then all worker nodes.
    Returns a status string summarising what happened on each host.
    """
    results = []

    # Start master
    out = _ssh_run(MASTER_HOST, f"{SPARK_HOME}/sbin/start-master.sh")
    results.append(f"[master {MASTER_HOST}]: {out}")

    # Start each worker — workers connect back to the master automatically
    for worker in WORKER_HOSTS:
        out = _ssh_run(worker, f"{SPARK_HOME}/sbin/start-worker.sh spark://{MASTER_HOST}:7077")
        results.append(f"[worker {worker}]: {out}")

    return "\n".join(results)


@mcp.tool()
def stop_spark_cluster() -> str:
    """
    Gracefully stop all Spark workers first, then the master node.
    Returns a status string summarising what happened on each host.
    """
    results = []

    for worker in WORKER_HOSTS:
        out = _ssh_run(worker, f"{SPARK_HOME}/sbin/stop-worker.sh")
        results.append(f"[worker {worker}]: {out}")

    out = _ssh_run(MASTER_HOST, f"{SPARK_HOME}/sbin/stop-master.sh")
    results.append(f"[master {MASTER_HOST}]: {out}")

    return "\n".join(results)


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
    Use this for metrics not covered by the other tools.

    Common useful queries:
      - Disk usage:  (1 - node_filesystem_avail_bytes / node_filesystem_size_bytes) * 100
      - Network in:  rate(node_network_receive_bytes_total[2m])
      - System load: node_load1

    Returns the raw Prometheus result list. Each item has a 'metric' label dict
    and a 'value' pair of [timestamp, value_string].
    """
    return _prometheus_query(promql)


# OLD SUBMIT JOB KEPT FOR REFERENCE
# @mcp.tool()
# def submit_spark_job(
#     script: str = "pyspark_roll_simulator.py",
#     archives: str = "spark_env.tar.gz",
#     extra_args: str = "",
# ) -> str:
#     """
#     Submit a PySpark job to the Spark cluster via spark-submit on the master node.
#     The job runs inside the 'spark_env' conda environment.

#     Args:
#         script:     The Python script to run (default: 'pyspark_roll_simulator.py')
#         archives:   Archive to ship with the job (default: 'spark_env.tar.gz')
#         extra_args: Any additional spark-submit flags e.g. '--executor-memory 2g'

#     Returns stdout/stderr from spark-submit.
#     """
#     submit_cmd = (
#         f"spark-submit "
#         f"--master spark://{MASTER_HOST}:7077 "
#         # f"--archives {SPARK_JOB_DIR}/{archives} "
#         f"{extra_args} "
#         f"{SPARK_JOB_DIR}/{script}"
#     ).strip()

#     # conda run -n <env> executes a command inside the environment without
#     # needing an interactive shell or sourcing conda init scripts first.
#     # This is the correct approach for non-interactive SSH sessions.
#     full_command = f"{CONDA_PATH}/bin/conda run -n spark_env {submit_cmd}"

#     return _ssh_run(MASTER_HOST, full_command)


@mcp.tool()
def submit_spark_job(
    script: str = "pyspark_roll_simulator.py",
    archives: str = "spark_env.tar.gz",
    extra_args: str = "",
    min_att: int = 2,
    max_att: int = 24,
    min_def: int = 2,
    max_def: int = 24,
    trials: int = 100,
    batches: int = 100,
    slices: int = 100,
    output: str = "risk_results.txt",
) -> str:
    """
    Submit a PySpark RISK simulation job to the Spark cluster via spark-submit.
    The job runs inside the 'spark_env' conda environment.

    Args:
        script:     The Python script to run (default: 'pyspark_roll_simulator.py')
        archives:   Archive to ship with the job (default: 'spark_env.tar.gz')
        extra_args: Additional spark-submit flags e.g. '--executor-memory 2g'
        min_att:    Minimum attacker army size (default: 2)
        max_att:    Maximum attacker army size (default: 24)
        min_def:    Minimum defender army size (default: 2)
        max_def:    Maximum defender army size (default: 24)
        trials:     Trials per batch (default: 100)
        batches:    Batches per scenario (default: 100)
        slices:     Spark partition count (default: 100)
        output:     Output filename (default: 'risk_results.txt')

    Returns stdout/stderr from spark-submit.
    """
    # Build the simulation argument string to pass after the script path
    sim_args = (
        f"--min_att {min_att} "
        f"--max_att {max_att} "
        f"--min_def {min_def} "
        f"--max_def {max_def} "
        f"--trials {trials} "
        f"--batches {batches} "
        f"--slices {slices} "
        f"--output {SPARK_JOB_DIR}/{output}"  # absolute path so the master writes to a known location
    ).strip()

    submit_cmd = (
        f"spark-submit "
        f"--master spark://{MASTER_HOST}:7077 "
        f"{extra_args} "
        f"{SPARK_JOB_DIR}/{script} "
        f"{sim_args}"           # script args must come AFTER the script path
    ).strip()

    full_command = f"{CONDA_PATH}/bin/conda run -n spark_env {submit_cmd}"

    job_output = _ssh_run(MASTER_HOST, full_command)

    # Read the results file the simulation wrote so the agent can report them directly
    results_path = f"{SPARK_JOB_DIR}/{output}"
    results_content = _ssh_run(MASTER_HOST, f"cat {results_path}")

    return f"=== Job Output ===\n{job_output}\n\n=== Simulation Results ({output}) ===\n{results_content}"


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

    Useful for inspecting Spark job output files, logs, or any text file on a node.

    Args:
        path:   Absolute path to the file on the remote host
                (e.g. '/home/pi/spark_jobs/risk_results.txt').
        target: Which node to read from. Options:
                   - "master"         — master node (default)
                   - "workers"        — read the same path from all worker nodes
                   - "all"            — master + all workers
                   - "<ip-or-host>"   — a specific node by hostname or IP address

    Returns a dict of {node: file_contents_string} for each targeted host.
    If the file does not exist on a node, the value will contain the error message.

    Examples:
        read_remote_file("/home/pi/spark_jobs/risk_results.txt")
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


# ---------------------------------------------------------------------------
# Streaming job tools
# ---------------------------------------------------------------------------

_DATASTREAM_PID_FILE = "/tmp/datastream.pid"
_DATASTREAM_LOG_FILE = "/tmp/datastream.log"


@mcp.tool()
def start_datastream() -> str:
    """
    Start the Spark structured streaming job (scale_datastream.py) on the master node
    in the background. Requires the Kafka broker (192.168.1.61) to be running, as the
    job consumes from Kafka to scale synthetic or real transaction data for the fraud
    detection pipeline. It runs continuously, feeding downstream fraud detection models.

    The PID is written to /tmp/datastream.pid so the job can be stopped later
    with stop_datastream(). Logs are written to /tmp/datastream.log.

    Returns a status string with the PID if the job started successfully.
    """
    script_path = f"{SPARK_JOB_DIR}/scale_datastream.py"

    # Check whether a datastream is already running
    check = _ssh_run(
        MASTER_HOST,
        f"[ -f {_DATASTREAM_PID_FILE} ] && ps -p $(cat {_DATASTREAM_PID_FILE}) > /dev/null 2>&1 && echo RUNNING || echo NOT_RUNNING",
    )
    if check.strip() == "RUNNING":
        pid = _ssh_run(MASTER_HOST, f"cat {_DATASTREAM_PID_FILE}").strip()
        return f"Datastream is already running (PID {pid}). Call stop_datastream() first if you want to restart it."

    submit_cmd = (
        f"{SPARK_HOME}/bin/spark-submit"
        f" --master spark://{MASTER_HOST}:7077"
        f" --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"
        f" --driver-memory 2G"
        f" --executor-memory 500M"
        f" {script_path}"
    )

    # Launch detached; capture PID into the pid file
    launch_cmd = (
        f"nohup {submit_cmd}"
        f" > {_DATASTREAM_LOG_FILE} 2>&1 &"
        f" echo $! > {_DATASTREAM_PID_FILE}"
        f" && cat {_DATASTREAM_PID_FILE}"
    )

    out = _ssh_run(MASTER_HOST, launch_cmd)
    pid = out.strip()
    if pid.isdigit():
        return f"Datastream started on {MASTER_HOST} (PID {pid}). Logs: {_DATASTREAM_LOG_FILE}"
    return f"Datastream launch may have failed. Raw output: {out}"


@mcp.tool()
def stop_datastream() -> str:
    """
    Stop the running Spark structured streaming job (scale_datastream.py) on the
    master node that was started with start_datastream().

    This halts the fraud detection data pipeline — no further synthetic or real
    transaction data will be scaled and forwarded to downstream consumers until
    the stream is restarted with start_datastream().

    Sends SIGTERM to the driver process by PID. Falls back to pkill on
    'scale_datastream.py' if the PID file is missing.

    Returns a status string indicating whether the process was stopped.
    """
    stop_cmd = (
        f"if [ -f {_DATASTREAM_PID_FILE} ]; then"
        f"  PID=$(cat {_DATASTREAM_PID_FILE});"
        f"  if ps -p $PID > /dev/null 2>&1; then"
        f"    kill $PID && rm -f {_DATASTREAM_PID_FILE} && echo \"Stopped PID $PID\";"
        f"  else"
        f"    rm -f {_DATASTREAM_PID_FILE} && echo \"Process $PID was not running (PID file cleaned up)\";"
        f"  fi;"
        f"else"
        f"  pkill -f scale_datastream.py && echo \"Stopped via pkill\" || echo \"No datastream process found\";"
        f"fi"
    )

    out = _ssh_run(MASTER_HOST, stop_cmd)
    return out.strip()