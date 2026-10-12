import pytest
from cluster_management_MCP.utils.tool_helpers import _is_node_accessible, _ping_server



class TestNodeAccessibility:

    def test_call_success(self):
        """Tests weather call to a known accessible node succeeds"""
        target_host="192.168.1.250"
        assert _is_node_accessible(target_host) == True

    def test_call_failure(self):
        target_host="192.168.1.678"
        assert _is_node_accessible(target_host) == False


    # def test_ping_success(self):
    #     target_host="192.168.1.250"
    #     assert _ping_server(target_host) == 1


    # def test_ping_failure(self):
    #     target_host="192.168.1.678"
    #     assert _ping_server(target_host) == 0
