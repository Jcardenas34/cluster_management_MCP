"""
An MCP server for managing a Raspberry Pi Spark cluster, submitting jobs, 
executing bash commangs and querying Prometheus metrics.
"""

# ---------------------------------------------------------------------------
# MCP Server initialisation
# ---------------------------------------------------------------------------
from cluster_management_MCP.core.mcp_instance import mcp
import cluster_management_MCP.utils.cluster_health_tools  
import cluster_management_MCP.utils.spark_tools
import cluster_management_MCP.utils.cli_tools
import cluster_management_MCP.utils.kafka_tools
# import cluster_management_MCP.utils.risk_tools


if __name__ == "__main__":
    mcp.run()