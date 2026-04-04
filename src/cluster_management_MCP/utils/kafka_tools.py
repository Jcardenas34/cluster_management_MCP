'''Tools for managing kafka consumer on worker node.'''


import httpx
from cluster_management_MCP.utils.tool_helpers import _ssh_run
from cluster_management_MCP.core.mcp_instance import mcp
from cluster_management_MCP.utils.config import (
    MASTER_HOST, WORKER_HOSTS, ADDITIONAL_HOSTS, KAFKA_HOST,
    SSH_USER, SSH_KEY_PATH, PROMETHEUS_URL, SPARK_HOME,
    CONDA_PATH, SPARK_JOB_DIR, _DATASTREAM_PID_FILE, _DATASTREAM_LOG_FILE,
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