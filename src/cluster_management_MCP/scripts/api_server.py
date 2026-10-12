'''Instance of the backend server launched for the cluster manager'''
import os
import sys
import asyncio
import uuid
import mlflow
from contextlib import asynccontextmanager, AsyncExitStack

from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware


from langchain_ollama import ChatOllama
from langchain.agents import create_agent
from langchain_mcp_adapters.client import MultiServerMCPClient
from langchain_mcp_adapters.tools import load_mcp_tools
from dotenv import load_dotenv
from cluster_management_MCP.utils.system_prompts import CLUSTER_MANAGER_1
from cluster_management_MCP.utils.api_server_helpers import (
    _run_chat, ChatRequest, active_jobs, drain_job_notifications, resume_job_watchers,
)
from cluster_management_MCP.utils.observability import setup_logger
from cluster_management_MCP.utils.cluster_health_tools import get_nodes_up
from cluster_management_MCP.utils.helpers import preload_ollama_model, open_ssh_tunnel, _terminate
from cluster_management_MCP.utils.config import MLFLOW_SSH_HOST, SSH_USER, SSH_KEY_PATH

load_dotenv()

DEFAULT_MODEL = "qwen3.5:2b" # Adequate tool usage, but tool confusion around 12 tools. 
# DEFAULT_MODEL = "qwen3:4b" # Prone to overthinking..leading to extremely long wait times
# DEFAULT_MODEL = "mashriram/gemma3nTools:e2b" # Does this really support tools?
# DEFAULT_MODEL = "qwen2.5:0.5b" # Very small experimental model, shockingly performs VERY well at single issue commands, very quickly.

OLLAMA_URL = os.environ.get("OLLAMA_HOST")
SERVER_SCRIPT = Path(__file__).parent / "spark_mcp_server.py"
NUM_CTX = 5280
KEEP_ALIVE = -1  # Keep the model resident in Ollama indefinitely (Ollama default unloads after 5 min idle)

agent = None
logger, log_file = setup_logger()
_chat_lock = asyncio.Lock()
_mlflow_run = None
session_id: str = ""

os.environ.setdefault("MLFLOW_HTTP_REQUEST_TIMEOUT", "5")
os.environ.setdefault("MLFLOW_HTTP_REQUEST_MAX_RETRIES", "0")

MLFLOW_PORT = 5000
tracking_uri = f"http://localhost:{MLFLOW_PORT}"


def _mlflow_reachable(uri: str, timeout: float = 3.0) -> bool:
    import httpx
    try:
        return httpx.get(f"{uri}/health", timeout=timeout).is_success
    except Exception:
        return False


async def _open_mlflow_tunnel():
    """
    MLflow listens only on the server's loopback, so forward localhost:5000 over SSH.
    Returns the ssh process, or None if MLflow was already reachable (e.g. a tunnel
    opened by hand) or the tunnel could not be opened.
    """
    if await asyncio.to_thread(_mlflow_reachable, tracking_uri, 1.0):
        return None
    if not MLFLOW_SSH_HOST:
        return None
    proc = open_ssh_tunnel(MLFLOW_SSH_HOST, SSH_USER, SSH_KEY_PATH, MLFLOW_PORT)
    for _ in range(20):  # up to ~10s for the SSH handshake
        await asyncio.sleep(0.5)
        if proc.poll() is not None:
            logger.warning(f"MLflow SSH tunnel to {MLFLOW_SSH_HOST} failed: {proc.stderr.read().strip()}")
            return None
        if await asyncio.to_thread(_mlflow_reachable, tracking_uri, 1.0):
            logger.info(f"MLflow SSH tunnel open: localhost:{MLFLOW_PORT} -> {MLFLOW_SSH_HOST}")
            return proc
    logger.warning(f"MLflow not reachable through SSH tunnel to {MLFLOW_SSH_HOST}; closing it")
    await asyncio.to_thread(_terminate, [proc], 3)
    return None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global agent, _mlflow_run, session_id

    # ── MLflow setup (checked fresh each startup) ─────────────────────────────
    mlflow_tunnel = await _open_mlflow_tunnel()
    experiment = None
    if _mlflow_reachable(tracking_uri):
        try:
            mlflow.set_tracking_uri(tracking_uri)
            experiment = mlflow.set_experiment("kitsune_cluster_manager")
        except Exception as e:
            logger.error(f"[ERROR] MLflow setup failed: {e}")
    else:
        logger.warning("MLflow server not reachable — observability disabled")

    # ── Model warm-up (runs in the background while the MCP server starts) ───
    async def _warm_model():
        try:
            await preload_ollama_model(OLLAMA_URL, DEFAULT_MODEL, KEEP_ALIVE, NUM_CTX)
            logger.info(f"Model {DEFAULT_MODEL} loaded and kept warm (keep_alive={KEEP_ALIVE}).")
        except Exception as e:
            logger.warning(f"Model warm-up failed, first query will load it: {e}")
    warmup_task = asyncio.create_task(_warm_model())

    # ── MCP client ────────────────────────────────────────────────────────────
    # A single stdio session is held open for the app's lifetime. Without it,
    # langchain-mcp-adapters spawns a fresh spark_mcp_server.py for every tool call.
    mcp_client = MultiServerMCPClient(
        {
            "spark": {
                "command": sys.executable,
                "args": [str(SERVER_SCRIPT)],
                "transport": "stdio",
            }
        }
    )
    mcp_stack = AsyncExitStack()
    mcp_session = await mcp_stack.enter_async_context(mcp_client.session("spark"))
    tools = await load_mcp_tools(mcp_session)
    if experiment:
        mlflow.langchain.autolog(log_traces=True)
    llm = ChatOllama(model=DEFAULT_MODEL, base_url=OLLAMA_URL, temperature=0,
                     num_ctx=NUM_CTX, reasoning=False, verbose=True,
                     num_predict=1200, keep_alive=KEEP_ALIVE)
    agent = create_agent(model=llm, tools=tools, system_prompt=CLUSTER_MANAGER_1)
    tool_names = [t.name for t in tools]
    logger.info(f"Agent ready. {len(tools)} tools loaded.")
    logger.info(f"Logging to: {log_file}")
    logger.info(f"SESSION_START | model={DEFAULT_MODEL} tools={tool_names}")

    session_id = str(uuid.uuid4())
    if experiment:
        try:
            _mlflow_run = mlflow.start_run(
                run_name=f"session-{session_id[:8]}",
                tags={"session_id": session_id},
            )
            mlflow.log_params({
                "model": DEFAULT_MODEL,
                "tool_count": len(tools),
                "tool_names": ",".join(tool_names),
            })
            logger.info(f"MLflow session run started | session_id={session_id} | run_id={_mlflow_run.info.run_id}")
        except Exception as e:
            logger.warning(f"MLflow run could not start (metrics skipped): {e}")
            _mlflow_run = None

    resume_task = asyncio.create_task(resume_job_watchers())

    yield  # ← app is live

    # ── Shutdown ──────────────────────────────────────────────────────────────
    warmup_task.cancel()
    resume_task.cancel()
    await mcp_stack.aclose()

    if _mlflow_run is not None:
        try:
            # By run id: mlflow.end_run() in a worker thread sees no active run and does nothing.
            await asyncio.wait_for(
                asyncio.to_thread(mlflow.MlflowClient().set_terminated, _mlflow_run.info.run_id),
                timeout=5.0,
            )
            logger.info("MLflow run ended cleanly.")
        except (asyncio.TimeoutError, Exception) as e:
            logger.warning(f"MLflow end_run failed: {e}")

    if mlflow_tunnel is not None:
        await asyncio.to_thread(_terminate, [mlflow_tunnel], 3)


app = FastAPI(lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173"],
    allow_methods=["POST", "GET"],
    allow_headers=["*"],
)


@app.get("/nodes")
async def node_status():
    '''Returns live node up/down status directly from Prometheus, bypassing the LLM.'''
    try:
        result = await asyncio.wait_for(asyncio.to_thread(get_nodes_up), timeout=10.0)
        return result
    except asyncio.TimeoutError:
        raise HTTPException(status_code=504, detail="Node status check timed out")
    except Exception as e:
        raise HTTPException(status_code=503, detail=str(e))


@app.get("/jobs")
async def jobs():
    '''Background jobs still running, plus alerts for jobs that finished since the last call.'''
    return {"active": list(active_jobs.values()), "notifications": drain_job_notifications()}


@app.post("/chat")
async def chat(req: ChatRequest):
    '''Handles the actual message passing to the SLM/LLM when message has already been passed'''
    global message_history

    if _chat_lock.locked():
        raise HTTPException(status_code=409, detail="A request is already in progress.")

    async with _chat_lock:
        return await _run_chat(req, agent, session_id=session_id)


