"""
pytest configuration for cluster_management_MCP integration tests.
Loads .env automatically so tests can be run without manually exporting vars.
"""
import pytest
from dotenv import load_dotenv


load_dotenv()

from cluster_management_MCP.utils.cluster_health_tools import (
    MASTER_HOST,
    WORKER_HOSTS,
    SSH_USER,
    SSH_KEY_PATH,
    PROMETHEUS_URL,
    SPARK_JOB_DIR,
    CONDA_PATH,
)

@pytest.fixture(scope="session", autouse=True)
def require_env():
    """Fail fast if critical environment variables are missing."""
    missing = []
    if not MASTER_HOST:
        missing.append("SPARK_MASTER_HOST")
    if not SSH_USER:
        missing.append("SSH_USER")
    if not SSH_KEY_PATH:
        missing.append("SSH_KEY_PATH")
    if missing:
        pytest.skip(f"Missing required environment variables: {', '.join(missing)}")

@pytest.fixture()
def mock_filetree(tmp_path):
    '''
    Creating a temporary file tree needed to test the ls_filetree MCP tool.
    Mock file tree will have both files and directories
    '''
    tmp_subdir = tmp_path/"pyspark_jobs"
    tmp_subdir.mkdir()

    # Making sample files to make sure second assert within ls_filetree is tested
    (tmp_path/"risk_results_with_cease_fires.txt").write_text("Attackers     2      3      4 \n")
    (tmp_path/"risk_results_with_cease_fires.txt").write_text("2          10.28  36.40  65.41  78.54 \n")
    (tmp_subdir/"spark_config.txt").write_text("SPARK_HOME=''")
    (tmp_subdir/"risk_battle_results.txt").write_text("Attackers   10")

    return tmp_path
