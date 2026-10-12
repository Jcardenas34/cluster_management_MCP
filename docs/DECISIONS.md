# Decision log

Append-only. Newest entries at the bottom. Past entries are never edited — a decision that
reverses an earlier one references it with `supersedes:` in its own header.

## D-0001 — Tackle response latency before new features
*2026-10-08 · type: question*

**Prompt**
> I want to work on a latentcy problem first. Right now it takes on the order of 10s of seconds to get results from the LLm interface. ... I have considered switching to a smaller weight model, or maybe loading only the tools calls that are necessary. What is your opinion on this given the state of the code.

**Considered** — Smaller model; loading only needed tools; persistent MCP session; keeping the Ollama model resident; parallelizing SSH fan-out; backgrounding long jobs. Feature work (job results to LLM, log diagnosis, Kafka/Jetson tools, bug/CI cleanup) was put on hold.

**Decision** — Latency first. Smaller model rejected for now; per-domain tool loading deferred.

**Why** — Log analysis of 98 queries (median 25.8s, p90 168.5s) showed the LLM picks a tool within ~1–2s, so model size is not the bottleneck. Every tool call had a ~5s floor, even trivial Prometheus queries, and one first query waited 212s before its first tool call (model cold-load). Tool slimming mainly improves selection accuracy, and Ollama's prefix cache already absorbs most of its prompt cost.

**Effect** — Work proceeds on the MCP session and model warm-keeping (D-0002). SSH parallelization and backgrounding long jobs stay queued.

## D-0002 — One persistent MCP server session and a warm Ollama model
*2026-10-08 · type: ask*

**Prompt**
> Lets make sure that only 1 MCP is started and kept warm for requests. As well as keep the model warm.

**Considered** — langchain-mcp-adapters 0.2.1 `get_tools()` without a session opens a new stdio session per tool call, spawning `spark_mcp_server.py` each time (~3.5s of imports, pandas alone ~1.5s, plus MCP init). Alternatives: hold a session via `MultiServerMCPClient.session()` + `load_mcp_tools(session)`, or run the server over HTTP as a separate process.

**Decision** — Open one stdio session in the FastAPI lifespan (and CLI) and keep it open for the app's life. Set Ollama `keep_alive` and send a warm-up request at startup.

**Why** — Removes the ~5s per-tool-call spawn and the multi-minute cold-load, with no new process to manage. Stdio session is the smallest change to the existing code.

**Effect** — `api_server.py` and `cluster_manager.py` build tools from a held session; ChatOllama gets `keep_alive`; startup warms the model.

## D-0003 — View RISK simulation results as an interactive heatmap in the chat UI
*2026-10-08 · type: choice*

**Prompt**
> I want to be able to view the resutls of the simulation. I beleive that there is a plot prduced once the job i launched. Is this true>

> lets go with option 1

**Considered** — Inspection showed the Spark job (`~/spark_scripts/pyspark_roll_simulator.py` on master) writes only CSV/TXT pivot tables; the plots the user remembered come from the separate, non-Spark `risk_analysis_in_risk` scripts. Options: (A) interactive heatmap rendered in the React chat from the CSV data, (B) matplotlib PNG generated on the backend, (C) plot inside the Spark job (needs matplotlib in the shipped `spark_env.tar.gz`).

**Decision** — Option A. Per-run result history was not chosen; runs still overwrite the same files.

**Why** — The results are a small pivot table, so rendering client-side is cheap, needs nothing installed on the Pis, and allows hovering for exact values.

**Effect** — Tools can return a `display` hint; the API attaches the result data to the chat reply (kept out of the LLM's context to save tokens); the frontend renders a heatmap under the message. Also fixes `lookup_win_rate`'s never-refreshed `/tmp` cache and the unpacked `_ssh_run` tuple in `submit_pyspark_risk_simulation`.

## D-0004 — Make `.env` authoritative over shell exports
*2026-10-08 · type: choice*

**Considered** — While testing the heatmap, SFTP from the backend timed out while the same call inside the MCP server worked. `~/.bash_profile` exports `SPARK_MASTER_HOST=192.168.0.221` (stale; `.env` has `192.168.1.250`), and `load_dotenv()` does not override existing variables. The MCP stdio subprocess gets a minimal environment, so it only ever saw `.env`. Options: edit the user's shell profile, or have `config.py` prefer `.env`.

**Decision** — `load_dotenv(override=True)` in `utils/config.py`. The shell profile is left untouched for the user to clean up.

**Why** — Both processes then resolve the same hosts regardless of the launching shell; CI has no `.env`, so its env vars are unaffected.

**Effect** — Backend-side cluster access (heatmap data, `/nodes`) uses the `.env` hosts.

## D-0005 — Run RISK simulations in the background and alert when they finish
*2026-10-08 · type: ask*

**Prompt**
> I want to be able to continue making queries after the job is launched, and have the SLM tell me that the job was launched, then continue to let me do work. Is there a way to run the submission and notification that the job has completed asynchronously so that I get an alert when the results are done?

**Considered** — (a) Detach spark-submit on master (`nohup`, PID/log file, exit-code marker), have a backend asyncio watcher poll it over SSH, and have the frontend poll a `/jobs` endpoint. (b) Run the blocking call as a background task inside the MCP server. That server can't push to the UI, and the stdio session is shared. (c) Poll the Spark master's `/json/` for app completion. This misses spark-submit/conda failures that happen before the app registers, and it gives no exit status. (d) Push alerts over SSE/websocket instead of polling. The UI already polls `/nodes` every 5s, so polling is simpler.

**Decision** — Option (a). Only one RISK job can run at a time. No new MCP tool was added.

**Why** — The job state lives on the master (PID, log, exit marker), so it survives backend restarts (`--reload`), and the backend re-attaches at startup. Runs overwrite the same result files, so concurrent jobs would clobber each other. Keeping the tool count unchanged avoids confusing the 2B model. Verified over real SSH with a `sleep` stand-in: the launch returned in ~4s (SSH handshake), and the process outlived the session and was detected as finished.

**Effect** — `submit_pyspark_risk_simulation` returns at once with a `"job"` hint. `api_server_helpers` watches it (15s poll) and queues a UI alert with the heatmap. A note is prepended to the model's history on the next turn. New `GET /jobs` endpoint. The Forge UI shows a JOB RUNNING badge, posts a JOB alert message, and shows a browser notification when the tab is hidden.

## D-0006 — Backend opens the MLflow SSH tunnel itself
*2026-10-09 · type: ask*

**Prompt**
> Right now, I have MLflow running on a separate db server, and each time I want to connect to it to allow trace tracking, I have to create a reverse tunnel to it from this computer. Is there a way for us to connect to the sesrver automatically instead of connecting this way?

**Considered** — MLflow runs as `mlflow.service` on 192.168.1.250 with `--host 127.0.0.1`, so it is only reachable through `ssh -L 5000:localhost:5000` (the `mlflow_tunnel` alias). Options: (a) the backend opens that SSH forward at startup with the existing cluster SSH key; (b) rebind the service to `0.0.0.0` and point the tracking URI at the LAN address. Option (b) needs sudo on the server and exposes an unauthenticated MLflow to the LAN.

**Decision** — Option (a). Spawn the system `ssh` with `-N -L`, `BatchMode` and `ExitOnForwardFailure`, rather than adding a Python tunnelling library. An already-reachable `localhost:5000` is reused, and the tunnel is closed on shutdown.

**Why** — No server change, MLflow stays loopback-only, and there are no new dependencies. Verified live: the tunnel opens and `/health` returns OK; a bad host fails in ~3s and startup continues without tracing.

**Effect** — New `helpers.open_ssh_tunnel`, `_open_mlflow_tunnel` in the `api_server.py` lifespan, and optional `MLFLOW_SSH_HOST` in config (defaults to `SPARK_MASTER_HOST`). The Ollama reverse tunnel (`mlflow_reverse_tunnel`) is not automated.
