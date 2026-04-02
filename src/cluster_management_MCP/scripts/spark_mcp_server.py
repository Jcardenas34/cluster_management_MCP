"""
A basic MCP server for managing a Raspberry Pi Spark cluster, submitting jobs and
querying Prometheus metrics.
"""

# ---------------------------------------------------------------------------
# MCP Server initialisation
# ---------------------------------------------------------------------------
from cluster_management_MCP.core.mcp_instance import mcp
import cluster_management_MCP.utils.mcp_tools  
import cluster_management_MCP.utils.spark_tools


if __name__ == "__main__":
    mcp.run()