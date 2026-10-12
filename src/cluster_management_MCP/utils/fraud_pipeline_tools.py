#!/usr/bin/env python
import subprocess
import pathlib
from cluster_management_MCP.utils.tool_helpers import _ssh_run
from cluster_management_MCP.core.mcp_instance import mcp

from cluster_management_MCP.utils.config import (
    KAFKA_HOST, NANO_GPU_HOST

)


_ALL_NODES = "all"


@mcp.tool()
def get_fraud_inference_status() -> dict:
    """
    Check whether the fraud inference server is running on the Jetson nano.
    Returns the systemd service status and current memory usage of the
    Fraud inference process. Call this before starting or stopping Fraud inference server to confirm
    its current state.
    """
    status, message = _ssh_run(NANO_GPU_HOST, "sudo systemctl is-active fraud_inference.service")
    
    # Get Fraud Inference process memory if running
    memory_status, mem = _ssh_run(
        NANO_GPU_HOST,
        "ps aux | grep -E '[f]raud' | awk '{sum += $6} END {printf \"%.0f\", sum/1024}'"
    )

    return {
        "status": message.strip(),          # "active" or "inactive"
        "memory_mb": mem.strip() or "0",
        "running": message.strip() == "active"
    }



@mcp.tool()
def stop_fraud_inference() -> dict:
    """
    Will take the fraud inference pipeline running on node-06 offline to save on resources.
    Use this when asked to stop the fraud server or fraud infernce server.

    returns a dictionary of the form {"status":"", "message":""}
    """
    # Fraud server running as a service that starts automatically on node-06 (Jetson Nano)
    target_node = "192.168.1.100" 

    command = ["sudo","systemctl", "stop", "fraud_inference.service"]
    try:
        success_status, result = _ssh_run(target_node, " ".join(command))
        return {"status":"success",
                "message":f"fraud_inference.service successfully stopped."}
    except Exception as e:
        return {"status":"error",
                "message":f"Request failed: {e}"}


@mcp.tool()
def start_fraud_inference() -> dict:
    """
    Will take the fraud inference pipeline running on node-06 offline to save on resources.
    Use this when asked to stop the fraud server or fraud infernce server.

    returns a dictionary of the form {"status":"", "message":""}
    """
    # Fraud server running as a service that starts automatically on node-06 (Jetson Nano)
    target_node = "192.168.1.100" 

    command = ["sudo","systemctl", "start", "fraud_inference.service"]
    try:
        result = _ssh_run(target_node, " ".join(command))
        return {"status":"success",
                "message":f"fraud_inference.service successfully started."}
    except Exception as e:
        return {"status":"error",
                "message":f"Request failed: {e}"}