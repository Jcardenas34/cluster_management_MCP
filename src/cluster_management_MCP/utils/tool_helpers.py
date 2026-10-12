'''helper functions to be used specifically in tool calls'''

import httpx
import paramiko
import subprocess
import platform
import os


from cluster_management_MCP.utils.config import (
    MASTER_HOST, WORKER_HOSTS, ADDITIONAL_HOSTS, KAFKA_HOST,
    SSH_USER, SSH_KEY_PATH, PROMETHEUS_URL, SPARK_HOME,
    CONDA_PATH, SPARK_JOB_DIR, _DATASTREAM_PID_FILE, _DATASTREAM_LOG_FILE,
)


# Host Name : IP Address
node_mappings = {
                "node-00":"192.168.1.178",
                "node-01":"192.168.1.228",
                "node-02":"192.168.1.9",
                "node-03":"192.168.1.61",
                "node-06":"192.168.1.100",
                "node-kitsune":"192.168.1.250",
                }

def _is_node_accessible(target_host:str) -> bool:
    """
    Checks weather a node is accessible through `ping`
    
    returns:
        True if node returns ping request, 
        and False if node is unreachable.
    """

    command = f"ping -c 1 {target_host} > /dev/null 2>&1"
    try:
        result = subprocess.run(command, shell=True, capture_output=True, 
                                text=True, check=True, timeout=5)
        return True
    except subprocess.TimeoutExpired as e:
        raise TimeoutError("Connection to host timed out after 5 seconds.")
    except subprocess.CalledProcessError as e:
        return False
    except Exception as e:
        return False
    

def _ssh_run(host: str, command: str) -> tuple[bool,str]:
    """
    Open an SSH connection to `host`, run `command`, return combined stdout/stderr.
    Raises on connection failure so the tool returns a clean error string.
    """
    character_limit = 2000 # Prevents an overwhelimg amount of context from being used
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    try:
        client.connect(host, username=SSH_USER, key_filename=SSH_KEY_PATH, timeout=10)
        _, stdout, stderr = client.exec_command(command)
        out = stdout.read().decode().strip()
        err = stderr.read().decode().strip()
        return True, out[:character_limit] if out else err[:character_limit]
    except Exception as e:
        return False, f"SSH error on {host}: {e}"
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

def _is_worker_running(node_IP:str) -> bool:
    """
    Checks weather the IP address corresponds to a running spark worker.

    Args:
        node_IP: The IP address of the host to check using the Spark API

    returns a boolean indicating weather the worker is on (1) or off (0) 

    """

    try:
        data = _fetch_spark_master_json()
        workers = data.get("workers", {})
        if node_IP in workers:
            if workers[node_IP].get("state") == "ALIVE":
                return True
    except:
        return False
    
    # Executes when there is any other status, including DEAD 
    return False

def _fetch_spark_master_json() -> dict:
    """Fetch the Spark Standalone Master's /json/ endpoint and return the parsed dict."""
    url = f"http://{MASTER_HOST}:8080/json/"

    try:
        r = httpx.get(url, timeout=5)
        r.raise_for_status()
    except httpx.HTTPStatusError as e:
        return {"status":"error", "message":f"{e}"}
    
    return r.json()