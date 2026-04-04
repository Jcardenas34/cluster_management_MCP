import pathlib
import paramiko
import pandas as pd
from cluster_management_MCP.core.mcp_instance import mcp
from cluster_management_MCP.utils.tool_helpers import _ssh_run
from cluster_management_MCP.utils.config import (
    MASTER_HOST, WORKER_HOSTS, ADDITIONAL_HOSTS, KAFKA_HOST,
    SSH_USER, SSH_KEY_PATH, PROMETHEUS_URL, SPARK_HOME,
    CONDA_PATH, SPARK_JOB_DIR, _DATASTREAM_PID_FILE, _DATASTREAM_LOG_FILE,
)



@mcp.tool()
def submit_pyspark_risk_simulation(
    script: str = "pyspark_roll_simulator.py",
    archives: str = "spark_env.tar.gz",
    extra_args: str = "",
    min_att: int = 2,
    max_att: int = 24,
    min_def: int = 2,
    max_def: int = 24,
    trials: int = 100,
    batches: int = 100,
    slices: int = 100,
    output: str = "risk_results.csv",
    n_cores: int = 16,
    executor_memory: str = "512M",
    executor_cores: int = 2,
) -> str:
    """
    Submit a PySpark RISK simulation job to the Spark cluster via spark-submit.
    The job runs inside the 'spark_env' conda environment.

    Before submitting a spark job to determine the win rate of a given army size,
    use the read_remote_file to check if a risk_results*.csv file exists on the MASTER_HOST node, and contains
    the result requested. The results file is not written to any other node on the cluster, if it does not
    exist on MASTER_HOST, it does not exist. Run the simulation job.

    If a user asks for a specific battle simulation between armies of spefic sizes
    the input parameters should be

    min_att=n, max_att=n, min_def=m, max_def=m

    Where n is the number of specified attackers, and m is the number of specified defenders.

    Args:
        script:          The Python script to run (default: 'pyspark_roll_simulator.py')
        archives:        Archive to ship with the job (default: 'spark_env.tar.gz')
        extra_args:      Additional spark-submit flags e.g. '--conf spark.some.option=value'
        min_att:         Minimum attacker army size (default: 2)
        max_att:         Maximum attacker army size (default: 24)
        min_def:         Minimum defender army size (default: 2)
        max_def:         Maximum defender army size (default: 24)
        trials:          Trials per batch (default: 100)
        batches:         Batches per scenario (default: 100)
        slices:          Spark partition count (default: 100)
        output:          Output filename (default: 'risk_results.csv')
        n_cores:         Total executor cores across the cluster (default: 10)
        executor_memory: Memory per executor (default: '512M', sized to fit Pi 3B+ nodes)
        executor_cores:  Cores per executor (default: 4, one executor per Pi worker node)

    Returns stdout/stderr from spark-submit.
    """
    # Build the simulation argument string to pass after the script path
    sim_args = (
        f"--min_att {min_att} "
        f"--max_att {max_att} "
        f"--min_def {min_def} "
        f"--max_def {max_def} "
        f"--trials {trials} "
        f"--batches {batches} "
        f"--slices {slices} "
        f"--output {SPARK_JOB_DIR}/{output}"  # absolute path so the master writes to a known location
    ).strip()

    submit_cmd = (
        f"spark-submit "
        f"--master spark://{MASTER_HOST}:7077 "
        f"--total-executor-cores {n_cores} "
        f"--executor-memory {executor_memory} "
        f"--executor-cores {executor_cores} "
        f"{SPARK_JOB_DIR}/{script} "
        f"{sim_args}"           # script args must come AFTER the script path
    ).strip()

    full_command = f"{CONDA_PATH}/bin/conda run -n spark_env {submit_cmd}"

    job_output = _ssh_run(MASTER_HOST, full_command)

    # Read the results file the simulation wrote so the agent can report them directly
    results_path = f"{SPARK_JOB_DIR}/{output}"
    # results_content = _ssh_run(MASTER_HOST, f"cat {results_path}")

    return f"=== Job Output ===\n{job_output}\n"


@mcp.tool()
def lookup_win_rate(
    attackers: int,
    defenders: int,
    section: str = "raw",
) -> str:
    """
    Look up the attacker win rate for a specific attacker/defender army size from
    the RISK simulation results CSV files on the master node.

    Files are pivot tables saved with DataFrame.to_csv(), index included:
      - "raw"      → risk_results_with_cease_fires.csv  (all battles included)
      - "excluded" → risk_results_resolved.csv           (cease-fire battles excluded)

    Args:
        attackers: Number of attacking armies.
        defenders: Number of defending armies.
        section:   Which file to query: "raw" (default) or "excluded".

    Returns a plain-English string with the win rate and the file it came from.
    """
    if section.lower() == "excluded":
        resolved_path = f"{SPARK_JOB_DIR}/risk_results_resolved.csv"
        section_label = "excluded"
    else:
        resolved_path = f"{SPARK_JOB_DIR}/risk_results_with_cease_fires.csv"
        section_label = "raw"

    # Copy the remote file locally via SFTP, then parse with pandas.
    local_tmp = pathlib.Path(f"/tmp/risk_lookup_{section_label}.csv")
    if not local_tmp.exists():
        try:
            client = paramiko.SSHClient()
            client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            client.connect(MASTER_HOST, username=SSH_USER, key_filename=SSH_KEY_PATH, timeout=10)
            sftp = client.open_sftp()
            sftp.get(resolved_path, str(local_tmp))
            sftp.close()
            client.close()
        except Exception as e:
            return f"Could not copy file from master node: {e}"

    try:
        df = pd.read_csv(local_tmp, index_col=0)
        df.index = df.index.astype(int)
        df.columns = df.columns.astype(int)
    except Exception as e:
        return f'Failed to parse "{section_label}": {e}'

    try:
        value = df.loc[defenders, attackers]
    except KeyError:
        return (
            f'No data for attackers={attackers}, defenders={defenders} '
            f'in "{section_label}". '
            f'Attacker range: {df.columns.min()}–{df.columns.max()}, '
            f'Defender range: {df.index.min()}–{df.index.max()}.'
        )

    return (
        f'Win rate for {attackers} attackers vs {defenders} defenders: {value}% '
        f'(section: "{section_label}")'
    )




