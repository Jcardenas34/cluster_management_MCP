'''Instance of the backend server launched for the cluster manager'''
import os
import asyncio
import time
from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from langchain_ollama import ChatOllama
from langchain.agents import create_agent
from langchain_core.messages import AIMessage
from langchain_mcp_adapters.client import MultiServerMCPClient
from dotenv import load_dotenv
from cluster_management_MCP.utils.system_prompts import CLUSTER_MANAGER_1
from cluster_management_MCP.utils.observability import (
    setup_logger,
    format_args,
    extract_thinking,
)

load_dotenv()

DEFAULT_MODEL = "qwen3.5:2b" # Adequate tool usage, but tool confusion around 12 tools. 
# DEFAULT_MODEL = "qwen3:4b" # Prone to overthinking..leading to extremely long wait times
# DEFAULT_MODEL = "mashriram/gemma3nTools:e2b" # Does this really support tools?

OLLAMA_URL = os.environ.get("OLLAMA_HOST")
SERVER_SCRIPT = Path(__file__).parent / "spark_mcp_server.py"

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173"],
    allow_methods=["POST"],
    allow_headers=["*"],
)

agent = None
message_history = []
logger, log_file = setup_logger()
_chat_lock = asyncio.Lock()


@app.on_event("startup")
async def startup():
    global agent
    mcp_client = MultiServerMCPClient(
        {
            "spark": {
                "command": "python",
                "args": [str(SERVER_SCRIPT)],
                "transport": "stdio",
            }
        }
    )
    tools = await mcp_client.get_tools()
    llm = ChatOllama(model=DEFAULT_MODEL, base_url=OLLAMA_URL, temperature=0, 
                     num_ctx=5280, reasoning=False, verbose=True,
                     num_predict=1200)
    agent = create_agent(model=llm, tools=tools, system_prompt=CLUSTER_MANAGER_1)
    tool_names = [t.name for t in tools]
    print(f"Agent ready. {len(tools)} tools loaded.")
    print(f"Logging to: {log_file}")
    logger.info(f"SESSION_START | model={DEFAULT_MODEL} tools={tool_names}")


class ChatRequest(BaseModel):
    message: str


@app.post("/chat")
async def chat(req: ChatRequest):
    global message_history

    if _chat_lock.locked():
        raise HTTPException(status_code=409, detail="A request is already in progress.")

    async with _chat_lock:
        return await _run_chat(req)


async def _run_chat(req: ChatRequest):
    global message_history

    message_history.append({"role": "user", "content": req.message})
    logger.info(f"QUERY | {req.message}")

    query_start = time.perf_counter()
    all_new_messages: list = []
    tool_timers: dict[str, tuple[str, float]] = {}
    final_response = ""

    last_final_ai_msg = None  # last AIMessage that is NOT a tool call

    async for chunk in agent.astream(
        {"messages": message_history},
        config={"recursion_limit": 20},
    ):
        if not isinstance(chunk, dict):
            continue

        node_items: list[tuple[str, list]] = []
        for key, val in chunk.items():
            if isinstance(val, dict) and "messages" in val:
                node_items.append((key, val["messages"]))
            elif key == "messages" and isinstance(val, list):
                node_items.append(("agent", val))

        for _node, messages in node_items:
            for msg in messages:
                all_new_messages.append(msg)
                thinking, text = extract_thinking(msg.content, getattr(msg, "additional_kwargs", None))

                if thinking:
                    logger.debug(f"THINKING | {thinking}")

                if hasattr(msg, "tool_calls") and msg.tool_calls:
                    for tc in msg.tool_calls:
                        elapsed = time.perf_counter() - query_start
                        args_str = format_args(tc["args"])
                        logger.info(
                            f"TOOL_START | {tc['name']} | elapsed={elapsed:.2f}s | args={args_str}"
                        )
                        tool_timers[tc["id"]] = (tc["name"], time.perf_counter())

                elif hasattr(msg, "tool_call_id"):
                    tool_id: str = getattr(msg, "tool_call_id", "") or ""
                    fallback_name: str = getattr(msg, "name", "?") or "?"
                    tool_name, t_start = tool_timers.pop(tool_id, (fallback_name, query_start))
                    tool_duration = time.perf_counter() - t_start
                    logger.info(
                        f"TOOL_END | {tool_name} | duration={tool_duration:.2f}s"
                        f" | result={str(msg.content)[:1000]}"
                    )

                elif isinstance(msg, AIMessage):
                    last_final_ai_msg = msg  # track for fallback
                    if text and "<tool_call>" not in text:
                        final_response = text

    # Fallback: model used reasoning=True and put everything in reasoning_content,
    # leaving content empty. Extract from the final AIMessage's thinking block.
    if not final_response and last_final_ai_msg is not None:
        thinking, text = extract_thinking(
            last_final_ai_msg.content,
            getattr(last_final_ai_msg, "additional_kwargs", None),
        )
        if text and "<tool_call>" not in text:
            final_response = text
        elif thinking and "<tool_call>" not in thinking:
            final_response = thinking

    # If the captured response is raw tool-call XML (model outputted text-mode tool calls
    # instead of structured calls), it is not a real answer — discard it.
    if "<tool_call>" in final_response:
        logger.warning("RESPONSE contained raw <tool_call> XML — model stopped mid-task")
        final_response = (
            "The model stopped mid-task while generating tool calls. "
            "The task may be too long for a single context window. "
            "Try breaking the request into smaller steps."
        )

    query_duration = time.perf_counter() - query_start
    logger.info(f"RESPONSE | {final_response}")
    logger.info(f"TIMING | query_duration={query_duration:.2f}s")

    message_history = message_history + all_new_messages
    return {"reply": final_response}
