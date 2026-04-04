
'''
Testing cluster health tool calls for SLM.
Dedicated test module for modularity and isolation.
Usage:
    pytest -m health_checks
'''
import re
import pytest
from cluster_management_MCP.utils.cluster_health_tools import ( get_node_cpu_load,
    get_node_memory_usage,
    get_nodes_up,
    get_cluster_summary,
    get_ip_nodename_mappings
    )



class TestIpNodeNameMappings:

    def test_returns_dict(self):
        '''Makes sure dict return type is correct'''
        assert isinstance(get_ip_nodename_mappings(), dict)

    @pytest.mark.parametrize("nodename",["node-00","node-01","node-02",
                                         "node-03","node-06",
                                         "node-kitsune"])
    def test_node_names(self, nodename: str):
        '''Makes sure the keys returned by promethus are unchanged'''
        payload = get_ip_nodename_mappings()
        assert nodename in payload.keys()
        
    def test_ip_addr_format(self):
        '''Makes sure the port number has been removed from the ip address'''
        payload = get_ip_nodename_mappings()
        pattern = r'^((25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$'
        validate_arr = [bool(re.match(pattern, ip)) for ip in payload.values()]
        assert all(validate_arr)
    



@pytest.mark.health_checks
class TestGetNodesUp:

    def test_returns_dict(self):
        """Should return a dictionary."""
        result = get_nodes_up()
        assert isinstance(result, dict)

    def test_up_values_are_booleans(self):
        """All values in the result must be booleans."""
        result = get_nodes_up()
        for item in result.values():
            assert isinstance(item["up"], bool), f"Expected bool for {item['instance']}, got {type(item['up'])}"

    def test_master_node_is_up(self):
        """The master node should appear in Prometheus and be reported as up."""
        result = get_nodes_up()
        assert len(result) > 0, "No nodes returned — is Prometheus reachable?"
        # At least one node should be up
        assert any(result.values()), "No nodes reported as up"

    def test_no_localhost_entries(self):
        """Prometheus localhost entries should be filtered out."""
        result = get_nodes_up()
        for instance in result:
            assert not instance.startswith("localhost"), f"localhost entry leaked through: {instance}"
            assert not instance.startswith("127.0.0.1"), f"127.0.0.1 entry leaked through: {instance}"

@pytest.mark.health_checks
class TestGetNodeCpuLoad:

    def test_returns_dict(self):
        result = get_node_cpu_load()
        assert isinstance(result, dict)

    def test_values_are_floats_in_valid_range(self):
        """CPU load values must be floats between 0 and 100."""
        result = get_node_cpu_load()
        for instance, load in result.items():
            assert isinstance(load, float), f"Expected float for {instance}, got {type(load)}"
            assert 0.0 <= load <= 100.0, f"CPU load out of range for {instance}: {load}"

    def test_sorted_descending(self):
        """Result should be sorted from highest to lowest CPU load."""
        result = get_node_cpu_load()
        values = list(result.values())
        assert values == sorted(values, reverse=True), "CPU load dict is not sorted descending"

    def test_returns_at_least_one_node(self):
        result = get_node_cpu_load()
        assert len(result) > 0, "No CPU data returned — is Prometheus reachable and node_exporter running?"


@pytest.mark.health_checks
class TestGetNodeMemoryUsage:

    def test_returns_dict(self):
        result = get_node_memory_usage()
        assert isinstance(result, dict)

    def test_values_are_floats_in_valid_range(self):
        """Memory usage values must be floats between 0 and 100."""
        result = get_node_memory_usage()
        for instance, usage in result.items():
            assert isinstance(usage, float), f"Expected float for {instance}, got {type(usage)}"
            assert 0.0 <= usage <= 100.0, f"Memory usage out of range for {instance}: {usage}"

    def test_sorted_descending(self):
        """Result should be sorted from highest to lowest memory usage."""
        result = get_node_memory_usage()
        values = list(result.values())
        assert values == sorted(values, reverse=True), "Memory usage dict is not sorted descending"

    def test_returns_at_least_one_node(self):
        result = get_node_memory_usage()
        assert len(result) > 0, "No memory data returned — is Prometheus reachable and node_exporter running?"


@pytest.mark.health_checks
class TestGetClusterSummary:

    def test_returns_dict_with_expected_keys(self):
        """Summary must contain all five top-level keys."""
        result = get_cluster_summary()
        expected_keys = {"nodes_up", "nodes_total", "node_status", "cpu_load_percent", "memory_used_percent"}
        assert expected_keys == set(result.keys())

    def test_node_counts_are_consistent(self):
        """nodes_up must be <= nodes_total."""
        result = get_cluster_summary()
        assert result["nodes_up"] <= result["nodes_total"]

    def test_node_counts_are_non_negative_integers(self):
        result = get_cluster_summary()
        assert isinstance(result["nodes_up"], int) and result["nodes_up"] >= 0
        assert isinstance(result["nodes_total"], int) and result["nodes_total"] >= 0

    def test_nested_dicts_are_populated(self):
        """Sub-dicts for status, CPU, and memory should each have at least one entry."""
        result = get_cluster_summary()
        assert len(result["node_status"]) > 0
        assert len(result["cpu_load_percent"]) > 0
        assert len(result["memory_used_percent"]) > 0

    def test_nodes_up_count_matches_node_status(self):
        """nodes_up count should equal the number of True values in node_status."""
        result = get_cluster_summary()
        true_count = sum(1 for v in result["node_status"].values() if v)
        assert result["nodes_up"] == true_count
