# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

An MCP (Model Context Protocol) server that lets a small local LLM (via Ollama) manage a physical Raspberry Pi / x86 Apache Spark cluster through natural language. The LLM drives SSH commands, Spark master REST calls, and Prometheus queries through MCP tools. A React chat frontend talks to a FastAPI backend that hosts the agent.

**Hardware:** Lenovo PC (Spark master), Raspberry Pi 3B+ workers, Raspberry Pi 3 (Kafka broker), NVIDIA Jetson Nano (edge fraud-inference node). Hostnames → IPs are hardcoded in `utils/tool_helpers.py::node_mappings`.

## Environment Setup

```bash
conda env create -f environment.yml
conda activate cluster_manager
pip install -e .
cd frontend && npm install && cd ..
cp .env.example .env  # then fill in real values
```

`.env` vars: `SPARK_MASTER_HOST`, `SPARK_WORKER_HOSTS` (comma-separated), `ADDITIONAL_HOSTS`, `KAFKA_HOST`, `NANO_GPU_HOST`, `SSH_USER`, `SSH_KEY_PATH`, `SPARK_HOME`, `CONDA_PATH`, `SPARK_JOB_DIR`, `OLLAMA_HOST`, `MLFLOW_SSH_HOST` (optional).

`config.py` loads `.env` with `override=True`, so `.env` beats shell exports (the MCP server subprocess gets a minimal env and only sees `.env`; `~/.bash_profile` exports a stale `SPARK_MASTER_HOST`).

Note: `PROMETHEUS_URL` is **not** read from `.env` — `utils/config.py` builds it at import time from the WSL default gateway (`ip route show default`) on port 9090.

## Running

```bash
launch-cluster   # starts uvicorn backend (:8000, --reload) + vite frontend (:5173, --host)
```

- Backend alone: `uvicorn cluster_management_MCP.scripts.api_server:app --reload --port 8000`
- Legacy CLI REPL (no entry point; partially stubbed — prints raw stream chunks, doesn't persist history): `python src/cluster_management_MCP/scripts/cluster_manager.py`
- The repo lives on `/mnt/c` (Windows drive under WSL, 9p mount), where file-change events are unreliable. `vite.config.ts` uses `watch.usePolling` so HMR sees edits; if the UI ever seems stale, check `curl localhost:5173/src/<file>` against disk and restart Vite.
- Optional MLflow tracing: the MLflow server runs as `mlflow.service` on `MLFLOW_SSH_HOST` (default `SPARK_MASTER_HOST`), bound to its `127.0.0.1:5000`. At startup the backend reuses `localhost:5000` if it already answers (e.g. a hand-made tunnel). Otherwise it opens `ssh -N -L 5000:localhost:5000` itself (`helpers.open_ssh_tunnel`) and closes it on shutdown. If MLflow is still unreachable, tracing is skipped.

The MCP server (`spark_mcp_server.py`) is spawned automatically as a stdio subprocess by the backend/CLI. Because it uses stdio transport, **never `print()` to stdout from tool code** — it corrupts the JSON-RPC stream.

## Architecture

```
frontend/ (React 19 + Vite, ChatWindowForge is the active UI)
  ↓ vite proxy: /chat, /nodes → :8000
scripts/api_server.py      — FastAPI; lifespan builds the agent (ChatOllama + langchain create_agent)
utils/api_server_helpers.py — _run_chat: streams agent, logs tool calls/timings, MLflow metrics, global message history
  ↓ MultiServerMCPClient (stdio)
scripts/spark_mcp_server.py — imports tool modules (triggering @mcp.tool registration), then mcp.run()
core/mcp_instance.py        — singleton FastMCP("spark-cluster-manager")
  ↓
utils/*_tools.py            — tool implementations (SSH via Paramiko, Prometheus/Spark via httpx)
```

**Tool modules** (enabled = imported in `spark_mcp_server.py`):

| Module | Enabled | Tools |
|---|---|---|
| `cluster_health_tools.py` | yes | `get_ip_nodename_mappings`, `get_node_cpu_load`, `get_node_memory_usage`, `get_nodes_up`, `get_cluster_summary`, `query_prometheus` |
| `spark_tools.py` | yes | `start/stop_spark_cluster`, `start/stop_spark_node`, `get_spark_master_summary`, `get_spark_workers`, `get_spark_running_jobs`, `get_spark_completed_jobs` (datastream tools commented out) |
| `cli_tools.py` | yes | `run_command`, `copy_file_from_remote_host`, `powerdown_node`, `shutdown_cluster` (`ls_filetree`, `read_remote_file` commented out) |
| `risk_tools.py` | yes | `submit_pyspark_risk_simulation`, `lookup_win_rate`, `show_risk_simulation_results` |
| `kafka_tools.py` | no | `get_kafka_status`, `start/stop/restart_kafka` |
| `fraud_pipeline_tools.py` | no | `get_fraud_inference_status`, `start/stop_fraud_inference` (Jetson) |

**Key design points:**
- Small models degrade as tool count grows (~12 tools is where qwen3.5:2b starts confusing them), so modules are toggled by commenting imports in `spark_mcp_server.py`. Tool docstrings are the model's only guidance — keep them precise.
- One MCP stdio session is held open for the app's lifetime (`AsyncExitStack` in the `api_server.py` lifespan; `async with mcp_client.session(...)` in the CLI) and tools come from `load_mcp_tools(session)`. Don't switch back to `mcp_client.get_tools()` — with no session, langchain-mcp-adapters spawns a new server process per tool call (~3s each vs ~0.03s).
- The Ollama model is preloaded at startup (`utils/helpers.py::preload_ollama_model`) and kept resident with `KEEP_ALIVE = -1`. The preload `num_ctx` must match the `ChatOllama` `num_ctx` (`NUM_CTX`), or Ollama reloads the model on the first query. Free VRAM manually with `ollama stop <model>`.
- **UI displays:** a tool may return a dict with a `"display": {"type": ...}` hint. `api_server_helpers._run_chat` collects hints from tool results, loads the data via `_load_display` (kept out of the LLM context to save tokens), and returns `{"reply", "displays"}`. The frontend renders each display under the message (`risk_heatmap` → `frontend/src/components/RiskHeatmap.tsx`). Add new display types in both places.
- **Background jobs:** `submit_pyspark_risk_simulation` launches spark-submit detached on master (`nohup`, PID/log in `SPARK_JOB_DIR/risk_job.{pid,log}`, exit code appended as `__RISK_JOB_EXIT__ N`) and returns a `"job"` hint at once. `_run_chat` sees the hint and `start_job_watcher` polls `risk_tools.risk_job_status()` every 15s in an asyncio task. On finish it queues a UI alert (with the heatmap) and a note that is prepended to the model's history on the next turn. The frontend polls `GET /jobs` every 5s for active jobs and alerts. Watchers live in the backend process; `resume_job_watchers()` re-attaches at startup (e.g. after `--reload`). Only one RISK job can run at a time. The CLI REPL does not watch jobs.
- RISK results: the Spark job (`pyspark_roll_simulator.py` in `SPARK_JOB_DIR` on master) writes only CSV/TXT pivot tables (rows=defenders, cols=attackers), overwritten each run. `risk_tools.load_risk_results` reads them fresh over SFTP.
- Model choice and context settings live at the top of `api_server.py` (`DEFAULT_MODEL`, `NUM_CTX`, `num_predict`); system prompts live in `utils/system_prompts.py` (backend uses `CLUSTER_MANAGER_1`).
- `/chat` is serialized by `_chat_lock` (409 if busy). `/nodes` calls `get_nodes_up` directly, bypassing the LLM. `/jobs` returns running background jobs and drains finished-job alerts.
- `tool_helpers._ssh_run(host, cmd)` returns `(ok: bool, output: str)` truncated to 2000 chars. Callers must unpack the tuple.
- SSH uses `AutoAddPolicy` (no host key verification — acceptable for a closed LAN).
- Logs are written daily to `logs/YYYY-MM-DD.log` (`utils/observability.py`) with `QUERY`/`TOOL_START`/`TOOL_END`/`RESPONSE`/`TIMING`/`JOB_START`/`JOB_END` lines — useful for debugging model tool-selection behavior.

## Testing

```bash
pytest -v -m filetree                 # offline tests (what CI runs)
pytest -v -k "not spark_job and not shutdown"  # live-cluster tests minus slow/dangerous ones
```

Markers (`pytest.ini`): `spark_job` (slow, real job submission), `shutdown` (real power-off; cancel with `sudo shutdown -c`), `filetree`, `health_checks`, `unit` (offline, mocked).

Most tests are integration tests against the live cluster. `conftest.py` skips the suite if `SPARK_MASTER_HOST`/`SSH_USER`/`SSH_KEY_PATH` are missing. CI (`.github/workflows/test.yml`) runs only `-m filetree` on Python 3.11.

## Known Issues

- CI is currently broken: `test_filetree_tools.py` imports `ls_filetree`, which is commented out.
- `test_mcp_tools.py` imports tools from the wrong module (and a nonexistent `submit_spark_job`); `test_tool_helpers.py` imports nonexistent `_ping_server`.
- `_prometheus_query` contains a debug `print()` (stdout corruption risk under stdio MCP).
- Several tools (`start/stop_spark_cluster`, `shutdown_cluster`, `powerdown_node`) interpolate the raw `_ssh_run` tuple instead of unpacking it; `powerdown_node` reports success even on SSH failure.
- `copy_file_from_remote_host` rejects `node-XX` hostnames (checks the unresolved name against `WORKER_HOSTS`) and can return `None` on some scp errors.
- `requires-python` in `pyproject.toml` is `<=3.13` (missing lower bound; intended `>=3.11,<=3.13`).
