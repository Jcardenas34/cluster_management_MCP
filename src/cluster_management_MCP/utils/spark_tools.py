'''
Spark specific MCP tools which include node monitoring, 
spark job submission.
'''

from cluster_management_MCP.utils.tool_helpers import _ssh_run, _fetch_spark_master_json, _is_worker_running
from cluster_management_MCP.core.mcp_instance import mcp
from cluster_management_MCP.utils.config import (
    MASTER_HOST, WORKER_HOSTS, ADDITIONAL_HOSTS, KAFKA_HOST,
    SSH_USER, SSH_KEY_PATH, PROMETHEUS_URL, SPARK_HOME,
    CONDA_PATH, SPARK_JOB_DIR, _DATASTREAM_PID_FILE, _DATASTREAM_LOG_FILE,
)


@mcp.tool()
def start_spark_cluster() -> str:
    """
    Starts all nodes in the Spark cluster. Starts the master node first, then all worker nodes.
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
def start_spark_node(node_IP: str) -> dict:
    """
    Starts an individual Spark worker by IP address.
    Useful for when an individual node needs to be started for additional computational power.

    Args:
        node_IP: The IP address of the node that will be started as a spark worker.
    
    returns a dictionary with fields describing success or failure of stoppage.
    {
    "status": "node status",
    "error_type": "specific kind of failure, empty is successful",
    "message": "message from node command was executed on"
    }

    """
    if node_IP not in WORKER_HOSTS:
        return {"status":"error" , "error_type":"node not registered", 
                "message":f"node with IP address {node_IP} not a registered worker on this cluster."}
    
    if _is_worker_running(node_IP):
        return {"status":"error", "error_type":"node on", 
                "message":f"node with IP address {node_IP} has already been started."}
    
    try:
        out = _ssh_run(node_IP, f"{SPARK_HOME}/sbin/start-worker.sh spark://{MASTER_HOST}:7077")
    except Exception as e:
        return {"status":"error", "error_type":"ssh error", 
                "message":f"Returned Error: {e}"}

    
    return {"status":"success", "node":f"{node_IP}", 
            "message": f"[worker {node_IP} response]: {out}"}

@mcp.tool()
def stop_spark_node(node_IP: str) -> dict:
    """
    Stops an individual Spark worker by IP address.
    Useful for when an individual node needs to be taken down for maintainence, 
    for reallocation of resources or if node is under too high a load.

    Args:
        node_IP: The IP address of the node that will be removed as a spark worker.
    
    returns a dictionary with fields describing success or failure of stoppage.
    {
    "status": "node status",
    "error_type": "specific kind of failure, empty is successful",
    "message": "message from node command was executed on"
    }

    """
    if node_IP not in WORKER_HOSTS:
        return {"status":"error" , "error_type":"node not registered", 
                "message":f"node with IP address {node_IP} not a registered worker on this cluster."}
    
    if _is_worker_running(node_IP):
        return {"status":"error", "error_type":"node stopped", 
                "message":f"node with IP address {node_IP} has already been stopped."}
    
    try:
        out = _ssh_run(node_IP, f"{SPARK_HOME}/sbin/stop-worker.sh")
    except Exception as e:
        return {"status":"error", "error_type":"ssh error", 
                "message":f"Returned Error {e}"}

    
    return {"status":"stopped", "node":f"{node_IP}", 
            "message": f"[worker {node_IP} response]: {out}"}



@mcp.tool()
def get_spark_master_summary() -> dict:
    """
    Return a top-level summary of the Spark Standalone Master.
    Includes cluster status, total/used/free cores and memory, and worker counts.

    Returns a dict with keys:
      status          - "ALIVE", "STANDBY", or "RECOVERING"
      spark_url       - e.g. "spark://host:7077"
      cores_total     - total cores across all alive workers
      cores_used      - cores currently allocated to running apps
      cores_free      - cores_total - cores_used
      memory_total_mb - total worker memory in MB
      memory_used_mb  - allocated worker memory in MB
      memory_free_mb  - memory_total_mb - memory_used_mb
      workers_alive   - number of workers in ALIVE state
      workers_dead    - number of workers in DEAD state
      active_apps     - number of currently running applications
      completed_apps  - number of completed applications recorded by the master
    """
    try:
        data = _fetch_spark_master_json()
    except Exception as e:
        return {"status":"error", "message":f"Exception Raised: {e}"}
    
    workers = data.get("workers", [])
    return {
        "status": data.get("status", "UNKNOWN"),
        "spark_url": data.get("url", f"spark://{MASTER_HOST}:7077"),
        "cores_total": data.get("cores", 0),
        "cores_used": data.get("coresused", 0),
        "cores_free": data.get("cores", 0) - data.get("coresused", 0),
        "memory_total_mb": data.get("memory", 0),
        "memory_used_mb": data.get("memoryused", 0),
        "memory_free_mb": data.get("memory", 0) - data.get("memoryused", 0),
        "workers_alive": sum(1 for w in workers if w.get("state") == "ALIVE"),
        "workers_dead": sum(1 for w in workers if w.get("state") != "ALIVE"),
        "active_apps": len(data.get("activeapps", [])),
        "completed_apps": len(data.get("completedapps", [])),
    }


@mcp.tool()
def get_spark_workers() -> list:
    """
    Return detailed information for every worker registered with the Spark master,
    including both alive and dead workers.

    Each entry in the returned list contains:
      id              - worker ID string
      host            - worker hostname or IP
      state           - "ALIVE" or "DEAD"
      cores_total     - total cores on this worker
      cores_used      - cores currently allocated
      cores_free      - cores_total - cores_used
      memory_total_mb - total memory on this worker in MB
      memory_used_mb  - allocated memory in MB
      memory_free_mb  - memory_total_mb - memory_used_mb
      last_heartbeat  - ISO 8601 timestamp of last heartbeat (or "unknown")
      webui           - URL of this worker's web UI
    """
    import datetime
    data = _fetch_spark_master_json()
    workers = []
    for w in data.get("workers", []):
        # lastheartbeat is in milliseconds since epoch
        hb_ms = w.get("lastheartbeat")
        if hb_ms is not None:
            hb_iso = datetime.datetime.fromtimestamp(hb_ms / 1000, tz=datetime.timezone.utc).isoformat()
        else:
            hb_iso = "unknown"
        workers.append({
            "id": w.get("id", ""),
            "host": w.get("host", ""),
            "state": w.get("state", "UNKNOWN"),
            "cores_total": w.get("cores", 0),
            "cores_used": w.get("coresused", 0),
            "cores_free": w.get("coresfree", 0),
            "memory_total_mb": w.get("memory", 0),
            "memory_used_mb": w.get("memoryused", 0),
            "memory_free_mb": w.get("memoryfree", 0),
            "last_heartbeat": hb_iso,
            "webui": w.get("webuiaddress", ""),
        })
    return workers


def _format_duration(seconds: float) -> str:
    seconds = int(seconds)
    h, rem = divmod(seconds, 3600)
    m, s = divmod(rem, 60)
    return f"{h}h {m:02d}m {s:02d}s" if h else f"{m}m {s:02d}s"


def _app_progress(app_id: str) -> dict | None:
    """
    Task progress for a running app, from its driver UI REST API. Drivers run on
    the master (client mode) and take UI ports 4040, 4041, ... in launch order.
    Returns None if no driver UI answers for this app.
    """
    import httpx
    for port in range(4040, 4044):
        base = f"http://{MASTER_HOST}:{port}/api/v1/applications/{app_id}"
        try:
            r = httpx.get(f"{base}/jobs", timeout=2)
        except httpx.HTTPError:
            break  # no UI on this port, so none on later ones either
        if r.status_code != 200:
            continue  # another app's UI
        jobs = r.json()
        tasks_total = sum(j.get("numTasks", 0) for j in jobs)
        tasks_done = sum(j.get("numCompletedTasks", 0) for j in jobs)
        return {
            "tasks_completed": tasks_done,
            "tasks_total": tasks_total,
            "tasks_failed": sum(j.get("numFailedTasks", 0) for j in jobs),
            "percent_complete": round(100 * tasks_done / tasks_total, 1) if tasks_total else None,
        }
    return None


@mcp.tool()
def get_spark_running_jobs() -> dict:
    """
    List the Spark jobs currently running on the cluster, with how long each has
    been running and its progress.
    Use when the user asks which Spark jobs are running, or how a running job is doing.
    For jobs that have already ended, use get_spark_completed_jobs.

    Each job contains:
      id                      - Spark application ID
      name                    - application name (RISK simulations are "RiskParallelSim")
      state                   - e.g. "RUNNING" or "WAITING" (waiting for free cores)
      started_at              - local start time, "YYYY-MM-DD HH:MM:SS"
      running_for             - elapsed time, e.g. "4m 12s"
      cores                   - cores allocated to the app
      memory_per_executor_mb  - memory per executor in MB
      progress                - task counts and percent_complete, for tasks
                                submitted so far (later stages may add more);
                                null if unavailable
    """
    import datetime
    try:
        data = _fetch_spark_master_json()
    except Exception as e:
        return {"status": "error", "message": f"Could not reach the Spark master: {e}"}
    if "activeapps" not in data:
        return {"status": "error", "message": data.get("message", "Unexpected response from the Spark master.")}

    now = datetime.datetime.now()
    jobs = []
    for app in data["activeapps"]:
        started = datetime.datetime.fromtimestamp(app.get("starttime", 0) / 1000)
        jobs.append({
            "id": app.get("id", ""),
            "name": app.get("name", ""),
            "state": app.get("state", ""),
            "started_at": started.strftime("%Y-%m-%d %H:%M:%S"),
            "running_for": _format_duration((now - started).total_seconds()),
            "cores": app.get("cores", 0),
            "memory_per_executor_mb": app.get("memoryperexecutor", app.get("memoryperslave", 0)),
            "progress": _app_progress(app.get("id", "")),
        })

    if not jobs:
        return {"status": "success", "running_jobs": 0, "message": "No Spark jobs are currently running."}
    return {"status": "success", "running_jobs": len(jobs), "jobs": jobs}


@mcp.tool()
def get_spark_completed_jobs(limit: int = 5) -> dict:
    """
    Show the history of Spark jobs that have finished, most recent first.
    Use when the user asks which jobs ran, past or previous jobs, or whether a job
    finished or failed. For jobs still running, use get_spark_running_jobs.
    History only covers jobs since the Spark master last restarted.

    Args:
      limit: maximum number of jobs to return (default 5)

    Each job contains:
      id                      - Spark application ID
      name                    - application name (RISK simulations are "RiskParallelSim")
      state                   - "FINISHED", "FAILED", or "KILLED"
      started_at              - local start time, "YYYY-MM-DD HH:MM:SS"
      finished_at             - local end time, "YYYY-MM-DD HH:MM:SS"
      duration                - total runtime, e.g. "2m 05s"
      cores                   - cores that were allocated
    """
    import datetime
    try:
        data = _fetch_spark_master_json()
    except Exception as e:
        return {"status": "error", "message": f"Could not reach the Spark master: {e}"}
    if "completedapps" not in data:
        return {"status": "error", "message": data.get("message", "Unexpected response from the Spark master.")}

    completed = sorted(data["completedapps"], key=lambda a: a.get("starttime", 0), reverse=True)
    fmt = lambda ms: datetime.datetime.fromtimestamp(ms / 1000).strftime("%Y-%m-%d %H:%M:%S")
    jobs = []
    for app in completed[:max(limit, 1)]:
        start_ms, duration_ms = app.get("starttime", 0), app.get("duration", 0)
        jobs.append({
            "id": app.get("id", ""),
            "name": app.get("name", ""),
            "state": app.get("state", ""),
            "started_at": fmt(start_ms),
            "finished_at": fmt(start_ms + duration_ms),
            "duration": _format_duration(duration_ms / 1000),
            "cores": app.get("cores", 0),
        })

    if not jobs:
        return {"status": "success", "completed_jobs": 0,
                "message": "No completed Spark jobs recorded since the Spark master started."}
    return {"status": "success", "completed_jobs": len(completed), "showing": len(jobs), "jobs": jobs}

# ---------------------------------------------------------------------------
# Spark job submission
# ---------------------------------------------------------------------------




# ---------------------------------------------------------------------------
# Spark Streaming job tools
# ---------------------------------------------------------------------------


# @mcp.tool()
# def start_datastream(executor_cores:int = 10) -> str:
#     """
#     Start the Spark structured streaming job (scale_datastream.py) on the master node
#     in the background. Requires the Kafka broker (192.168.1.61) to be running, as the
#     job consumes from Kafka to scale synthetic or real transaction data for the fraud
#     detection pipeline. It runs continuously, feeding downstream fraud detection models.

#     The PID is written to /tmp/datastream.pid so the job can be stopped later
#     with stop_datastream(). Logs are written to /tmp/datastream.log.

#     Args:
#         executor_cores: The total number of executor cores to use for data processing. (default: 10)

#     Returns a status string with the PID if the job started successfully.
#     """
#     script_path = f"{SPARK_JOB_DIR}/scale_datastream.py"

#     # Check whether a datastream is already running
#     check = _ssh_run(
#         MASTER_HOST,
#         f"[ -f {_DATASTREAM_PID_FILE} ] && ps -p $(cat {_DATASTREAM_PID_FILE}) > /dev/null 2>&1 && echo RUNNING || echo NOT_RUNNING",
#     )
#     if check.strip() == "RUNNING":
#         pid = _ssh_run(MASTER_HOST, f"cat {_DATASTREAM_PID_FILE}").strip()
#         return f"Datastream is already running (PID {pid}). Call stop_datastream() first if you want to restart it."

#     submit_cmd = (
#         f"{SPARK_HOME}/bin/spark-submit"
#         f" --master spark://{MASTER_HOST}:7077"
#         f" --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"
#         f" --driver-memory 2G"
#         f" --executor-memory 500M"
#         f" --total-executor-cores {executor_cores}"
#         f" {script_path}"
#     )

#     # Launch detached; capture PID into the pid file
#     launch_cmd = (
#         f"nohup {submit_cmd}"
#         f" > {_DATASTREAM_LOG_FILE} 2>&1 &"
#         f" echo $! > {_DATASTREAM_PID_FILE}"
#         f" && cat {_DATASTREAM_PID_FILE}"
#     )

#     out = _ssh_run(MASTER_HOST, launch_cmd)
#     pid = out.strip()
#     if pid.isdigit():
#         return f"Datastream started on {MASTER_HOST} (PID {pid}). Logs: {_DATASTREAM_LOG_FILE}"
#     return f"Datastream launch may have failed. Raw output: {out}"

# @mcp.tool()
# def stop_datastream() -> str:
#     """
#     Stop the running Spark structured streaming job (scale_datastream.py) on the
#     master node that was started with start_datastream().

#     This halts the fraud detection data pipeline — no further synthetic or real
#     transaction data will be scaled and forwarded to downstream consumers until
#     the stream is restarted with start_datastream().

#     Sends SIGTERM to the driver process by PID. Falls back to pkill on
#     'scale_datastream.py' if the PID file is missing.

#     Returns a status string indicating whether the process was stopped.
#     """
#     stop_cmd = (
#         f"if [ -f {_DATASTREAM_PID_FILE} ]; then"
#         f"  PID=$(cat {_DATASTREAM_PID_FILE});"
#         f"  if ps -p $PID > /dev/null 2>&1; then"
#         f"    kill $PID && rm -f {_DATASTREAM_PID_FILE} && echo \"Stopped PID $PID\";"
#         f"  else"
#         f"    rm -f {_DATASTREAM_PID_FILE} && echo \"Process $PID was not running (PID file cleaned up)\";"
#         f"  fi;"
#         f"else"
#         f"  pkill -f scale_datastream.py && echo \"Stopped via pkill\" || echo \"No datastream process found\";"
#         f"fi"
#     )

#     out = _ssh_run(MASTER_HOST, stop_cmd)
#     return out.strip()
