import io
import shlex
import paramiko
from datetime import datetime
from cluster_management_MCP.core.mcp_instance import mcp
from cluster_management_MCP.utils.tool_helpers import _ssh_run
from cluster_management_MCP.utils.config import (
    MASTER_HOST, WORKER_HOSTS, ADDITIONAL_HOSTS, KAFKA_HOST,
    SSH_USER, SSH_KEY_PATH, PROMETHEUS_URL, SPARK_HOME,
    CONDA_PATH, SPARK_JOB_DIR, _DATASTREAM_PID_FILE, _DATASTREAM_LOG_FILE,
)



# Background job bookkeeping on the master node. The PID is the wrapper shell, which
# lives until spark-submit exits and then appends an exit-code marker to the log.
_RISK_JOB_PID_FILE = f"{SPARK_JOB_DIR}/risk_job.pid"
_RISK_JOB_LOG_FILE = f"{SPARK_JOB_DIR}/risk_job.log"
_RISK_JOB_EXIT_MARKER = "__RISK_JOB_EXIT__"
_RISK_JOB_RUNNING_MARKER = "__RISK_JOB_RUNNING__"
_RISK_JOB_DONE_MARKER = "Done. Results written to"  # printed by pyspark_roll_simulator.py on success
_RISK_JOB_DONE_FOUND = "__RISK_JOB_DONE_FOUND__"
_RISK_JOB_ALIVE = f"[ -f {_RISK_JOB_PID_FILE} ] && kill -0 $(cat {_RISK_JOB_PID_FILE}) 2>/dev/null"


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
) -> dict:
    """
    Launch a PySpark RISK simulation job on the Spark cluster via spark-submit.
    The job runs inside the 'spark_env' conda environment.

    Before submitting a spark job to determine the win rate of a given army size,
    use lookup_win_rate to check whether the latest results already contain it. Only run the
    simulation job if they do not. Each run overwrites the previous results.

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

    Returns immediately once the job is launched; the job keeps running in the background
    for several minutes. Tell the user it was launched. Do not wait for it or check on it:
    the user is notified automatically, with a heatmap, when it finishes.
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

    # --no-capture-output streams job output to the log as it runs instead of at exit
    full_command = f"{CONDA_PATH}/bin/conda run --no-capture-output -n spark_env {submit_cmd}"
    wrapped = shlex.quote(f'{full_command}; echo "{_RISK_JOB_EXIT_MARKER} $?"')

    # Detach fully (nohup, all fds redirected) so the SSH session returns at once.
    launch_cmd = (
        f"if {_RISK_JOB_ALIVE}; then echo ALREADY_RUNNING; else "
        f"nohup bash -c {wrapped} > {_RISK_JOB_LOG_FILE} 2>&1 < /dev/null & "
        f"echo $! > {_RISK_JOB_PID_FILE}; echo LAUNCHED; fi"
    )

    ok, out = _ssh_run(MASTER_HOST, launch_cmd)
    if not ok or ("LAUNCHED" not in out and "ALREADY_RUNNING" not in out):
        return {"status": "error", "message": f"Could not launch simulation job:\n{out[-1000:]}"}
    if "ALREADY_RUNNING" in out:
        return {
            "status": "error",
            "message": "A RISK simulation is already running. Wait for it to finish before launching another.",
        }

    label = f"RISK simulation (attackers {min_att}-{max_att} vs defenders {min_def}-{max_def})"
    return {
        "status": "launched",
        "message": (
            f"{label} launched in the background. It takes several minutes; the user will be "
            "notified automatically when it finishes."
        ),
        "job": {"type": "risk_simulation", "label": label},
    }


def risk_job_status() -> dict:
    """
    Check the background RISK simulation job on the master node. Not an MCP tool;
    polled by the API server's job watcher.

    Returns {"state": "running" | "succeeded" | "failed" | "none" | "unreachable", "detail": str}.
    """
    # The log interleaves job stdout with Spark's stderr logging, and Spark keeps logging
    # after the script prints its done line, so search the whole file rather than its tail.
    # Failure detail skips Spark INFO lines, which would otherwise crowd out the error.
    ok, out = _ssh_run(
        MASTER_HOST,
        f"if {_RISK_JOB_ALIVE}; then echo {_RISK_JOB_RUNNING_MARKER}; "
        f"elif [ -f {_RISK_JOB_LOG_FILE} ]; then "
        f"grep -q {shlex.quote(_RISK_JOB_DONE_MARKER)} {_RISK_JOB_LOG_FILE} && echo {_RISK_JOB_DONE_FOUND}; "
        f"grep -o '{_RISK_JOB_EXIT_MARKER} [0-9]*' {_RISK_JOB_LOG_FILE} | tail -n 1; "
        f"grep -v ' INFO ' {_RISK_JOB_LOG_FILE} | tail -c 800; fi",
    )
    if not ok:
        return {"state": "unreachable", "detail": out}
    if out.startswith(_RISK_JOB_RUNNING_MARKER):
        return {"state": "running", "detail": ""}
    if not out:
        return {"state": "none", "detail": ""}
    if _RISK_JOB_DONE_FOUND in out and f"{_RISK_JOB_EXIT_MARKER} 0" in out:
        return {"state": "succeeded", "detail": ""}
    lines = [l for l in out.splitlines() if _RISK_JOB_DONE_FOUND not in l]
    exit_code = next((l.split()[-1] for l in lines if l.startswith(_RISK_JOB_EXIT_MARKER)), "unknown (job was killed)")
    log_tail = "\n".join(l for l in lines if not l.startswith(_RISK_JOB_EXIT_MARKER))
    return {"state": "failed", "detail": f"exit code {exit_code}\n{log_tail}"[-800:]}


# Result files written by pyspark_roll_simulator.py (pivot tables: rows=defenders, columns=attackers)
_RISK_RESULT_FILES = {
    "raw":      "risk_results_with_cease_fires.csv",  # cease-fires counted as trials
    "excluded": "risk_results_resolved.csv",          # cease-fires excluded
}


def _normalise_section(section: str) -> str:
    return "excluded" if section.lower() == "excluded" else "raw"


def load_risk_results(section: str = "raw"):
    """
    Read a RISK results pivot table fresh from the master node over SFTP.
    Returns (DataFrame indexed by defenders with attacker columns, last-modified datetime).
    """
    import pandas as pd  # deferred: pandas adds ~1.5s to MCP server start-up

    remote_path = f"{SPARK_JOB_DIR}/{_RISK_RESULT_FILES[_normalise_section(section)]}"
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    try:
        client.connect(MASTER_HOST, username=SSH_USER, key_filename=SSH_KEY_PATH, timeout=10)
        with client.open_sftp() as sftp:
            modified = datetime.fromtimestamp(sftp.stat(remote_path).st_mtime)
            with sftp.open(remote_path) as f:
                df = pd.read_csv(io.BytesIO(f.read()), index_col=0)
    finally:
        client.close()

    df.index = df.index.astype(int)
    df.columns = df.columns.astype(int)
    return df, modified


def risk_results_payload(section: str = "raw") -> dict:
    """JSON-serialisable form of a results table, used by the chat UI heatmap."""
    df, modified = load_risk_results(section)
    return {
        "section": _normalise_section(section),
        "attackers": df.columns.tolist(),
        "defenders": df.index.tolist(),
        "win_rates": df.values.tolist(),  # win_rates[i][j] = defenders[i] vs attackers[j]
        "updated_at": modified.isoformat(timespec="seconds"),
    }


@mcp.tool()
def show_risk_simulation_results(section: str = "raw") -> dict:
    """
    Show the user a heatmap of the latest RISK simulation results (attacker win rate
    for every attacker/defender army size in the last run).
    Use when the user asks to see, view, plot or visualise simulation results.
    The heatmap only appears when this tool is called, so call it again on every
    such request, even if it was called earlier in the conversation.

    Args:
        section: "raw" (default, cease-fires counted as losses) or "excluded" (cease-fires excluded).
    """
    try:
        df, modified = load_risk_results(section)
    except Exception as e:
        return {"status": "error", "message": f"Could not read results from master node: {e}"}

    return {
        "status": "success",
        "message": (
            f"Heatmap shown to the user. Results cover attackers {df.columns.min()}-{df.columns.max()} "
            f"vs defenders {df.index.min()}-{df.index.max()}, last updated {modified:%Y-%m-%d %H:%M}."
        ),
        "display": {"type": "risk_heatmap", "section": _normalise_section(section)},
    }


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
    section_label = _normalise_section(section)
    try:
        df, _ = load_risk_results(section_label)
    except Exception as e:
        return f'Could not read "{section_label}" results from master node: {e}'

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




