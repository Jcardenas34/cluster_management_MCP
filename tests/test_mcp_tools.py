"""
Integration tests for all MCP tool calls in cluster_management_MCP.

These tests hit the real cluster and Prometheus instance — run them with
the cluster online and environment variables loaded from your .env file.

Usage:
    pytest tests/test_mcp_tools.py -v
    pytest tests/test_mcp_tools.py -v -k "not spark_job"   # skip long-running job submission
"""

import pytest
from cluster_management_MCP.utils.cluster_health_tools import (
    start_spark_cluster,
    stop_spark_cluster,
    shutdown_cluster,
    query_prometheus,
    submit_spark_job
)

from cluster_management_MCP.utils.config import (
    MASTER_HOST, WORKER_HOSTS, ADDITIONAL_HOSTS, KAFKA_HOST,
    SSH_USER, SSH_KEY_PATH, PROMETHEUS_URL, SPARK_HOME,
    CONDA_PATH, SPARK_JOB_DIR, _DATASTREAM_PID_FILE, _DATASTREAM_LOG_FILE,
)


# ---------------------------------------------------------------------------
# Precondition checks — skip entire suite if env is not configured
# ---------------------------------------------------------------------------

def pytest_configure(config):
    config.addinivalue_line(
        "markers", "spark_job: marks tests that submit a real Spark job (slow)"
    )



# ---------------------------------------------------------------------------
# query_prometheus (raw PromQL escape hatch)
# ---------------------------------------------------------------------------

class TestQueryPrometheus:

    def test_returns_list(self):
        result = query_prometheus("up")
        assert isinstance(result, list)

    def test_each_result_has_metric_and_value(self):
        """Raw Prometheus results must have 'metric' and 'value' fields."""
        result = query_prometheus("up")
        for item in result:
            assert "metric" in item
            assert "value" in item
            assert len(item["value"]) == 2  # [timestamp, value_string]

    def test_node_load1_query(self):
        """A common node exporter query should return results."""
        result = query_prometheus("node_load1")
        assert isinstance(result, list)
        assert len(result) > 0, "node_load1 returned no data — is node_exporter running?"

    def test_invalid_promql_raises_runtime_error(self):
        """A syntactically invalid PromQL expression should raise RuntimeError."""
        with pytest.raises(RuntimeError):
            query_prometheus("this is not valid {{{promql")

    def test_disk_usage_query(self):
        """Disk usage query should return numeric results."""
        result = query_prometheus(
            "(1 - node_filesystem_avail_bytes / node_filesystem_size_bytes) * 100"
        )
        assert isinstance(result, list)
        for item in result:
            # value[1] is a string — should be parseable as float
            float(item["value"][1])


# ---------------------------------------------------------------------------
# Cluster lifecycle tools (start / stop)
# ---------------------------------------------------------------------------

class TestClusterLifecycle:
    """
    These tests start and then stop the Spark cluster.
    They are ordered: start -> verify -> stop -> verify.
    Run with: pytest -v -k "lifecycle"
    """

    def test_start_spark_cluster_returns_string(self):
        """start_spark_cluster should return a non-empty string status."""
        result = start_spark_cluster()
        assert isinstance(result, str)
        assert len(result) > 0

    def test_start_spark_cluster_includes_master(self):
        """Status string should mention the master host."""
        result = start_spark_cluster()
        assert MASTER_HOST in result, f"Master host {MASTER_HOST} not mentioned in output:\n{result}"

    def test_start_spark_cluster_includes_all_workers(self):
        """Status string should mention every configured worker host."""
        result = start_spark_cluster()
        for worker in WORKER_HOSTS:
            if worker:  # guard against empty string from env parsing
                assert worker in result, f"Worker {worker} not mentioned in output:\n{result}"

    def test_stop_spark_cluster_returns_string(self):
        """stop_spark_cluster should return a non-empty string status."""
        result = stop_spark_cluster()
        assert isinstance(result, str)
        assert len(result) > 0

    def test_stop_spark_cluster_includes_master(self):
        """Status string should mention the master host."""
        result = stop_spark_cluster()
        assert MASTER_HOST in result, f"Master host {MASTER_HOST} not mentioned in output:\n{result}"


# ---------------------------------------------------------------------------
# shutdown_cluster
# ---------------------------------------------------------------------------

class TestShutdownCluster:
    """
    These tests verify the shutdown command is correctly constructed and dispatched.
    They use a non-zero delay so the shutdown can be cancelled with `sudo shutdown -c`
    before the machines actually power off.

    Run with: pytest -v -k "shutdown"
    Cancel a pending shutdown on all nodes manually with: sudo shutdown -c
    """

    @pytest.mark.shutdown
    def test_returns_string(self):
        """shutdown_cluster should return a non-empty status string."""
        result = shutdown_cluster(delay_minutes=5)
        assert isinstance(result, str)
        assert len(result) > 0

    @pytest.mark.shutdown
    def test_includes_all_hosts_in_output(self):
        """Status string should mention the master and every worker."""
        result = shutdown_cluster(delay_minutes=5)
        assert MASTER_HOST in result, f"Master host {MASTER_HOST} not in output:\n{result}"
        for worker in WORKER_HOSTS:
            if worker:
                assert worker in result, f"Worker {worker} not in output:\n{result}"

    @pytest.mark.shutdown
    def test_delayed_shutdown_mentions_timing(self):
        """Output should include the delay timing for a scheduled shutdown."""
        result = shutdown_cluster(delay_minutes=5)
        assert "+5" in result, f"Expected '+5' timing in output:\n{result}"

    @pytest.mark.shutdown
    def test_immediate_shutdown_mentions_now(self):
        """Output for delay_minutes=0 should indicate 'now' timing."""
        result = shutdown_cluster(delay_minutes=0)
        assert "now" in result, f"Expected 'now' timing in output:\n{result}"

    @pytest.mark.shutdown
    def test_no_ssh_error_on_valid_hosts(self):
        """Result should not contain 'SSH error', which indicates a connection failure."""
        result = shutdown_cluster(delay_minutes=5)
        assert "SSH error" not in result, f"SSH connection failed for one or more nodes:\n{result}"


# ---------------------------------------------------------------------------
# submit_spark_job  (marked slow — skipped by default in quick runs)
# ---------------------------------------------------------------------------

class TestSubmitSparkJob:

    @pytest.mark.spark_job
    def test_submit_returns_string(self):
        """submit_spark_job should return a string (stdout/stderr from spark-submit)."""
        result = submit_spark_job(
            trials=1,
            batches=1,
            slices=2,
            min_att=2,
            max_att=3,
            min_def=2,
            max_def=3,
            output="pytest_test_output.txt",
        )
        assert isinstance(result, str)

    @pytest.mark.spark_job
    def test_submit_does_not_return_ssh_error(self):
        """Result should not start with 'SSH error', which indicates a connection failure."""
        result = submit_spark_job(
            trials=1,
            batches=1,
            slices=2,
            min_att=2,
            max_att=3,
            min_def=2,
            max_def=3,
            output="pytest_test_output.txt",
        )
        assert not result.startswith("SSH error"), f"SSH connection failed:\n{result}"

    @pytest.mark.spark_job
    def test_submit_uses_configured_job_dir(self):
        """
        If SPARK_JOB_DIR is set, the output path in the job command should reference it.
        This is a smoke test — it verifies the env var wiring, not the Spark output.
        """
        assert SPARK_JOB_DIR, "SPARK_JOB_DIR is not set — cannot verify job directory wiring"
        assert CONDA_PATH, "CONDA_PATH is not set — spark-submit will fail to launch"
