from cluster_management_MCP.utils.tool_helpers import _ssh_run, _prometheus_query
from cluster_management_MCP.core.mcp_instance import mcp

from cluster_management_MCP.utils.config import (
    MASTER_HOST, WORKER_HOSTS, ADDITIONAL_HOSTS, KAFKA_HOST,
    SSH_USER, SSH_KEY_PATH, PROMETHEUS_URL, SPARK_HOME,
    CONDA_PATH, SPARK_JOB_DIR, _DATASTREAM_PID_FILE, _DATASTREAM_LOG_FILE,
)


_ALL_NODES = "all"

# ---------------------------------------------------------------------------
# Health check tools
# ---------------------------------------------------------------------------

@mcp.tool()
def get_ip_nodename_mappings() -> dict:
    """
    Query prometheus for node names and IP address mappings.
    returns a dictionary of {nodename_1:nodename1_ip_addr, ...} 
    or {'error': '...'} on failure

    Use this before any tool call that needs to resolve a hostname to an IP address.
    Use this to determine the IP addresses of nodes, or hostnames of nodes given an IP address.
    """

    promql = "count by (nodename, instance) (node_uname_info)"
    try:
        results = _prometheus_query(promql)

        node_mappings = {}
        for res in results:
            ip_addr = res["metric"].get("instance")
            node_name = res["metric"].get("nodename")
            if ip_addr is None or node_name is None:
                continue
            node_mappings[node_name] = ip_addr.split(":")[0]

        if not node_mappings:
            return {"error":"No nodes in promethus, is it running? "}

        # Return in alphabetical order
        node_mappings = dict(sorted(node_mappings.items()))

        return node_mappings
    
    except Exception as e:
        return {"error":f"Could not retrieve node mappings: {e}"}


        


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


