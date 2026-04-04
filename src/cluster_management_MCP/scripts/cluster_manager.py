"""
A barebones CLI for interacting with the Spark cluster MCP server.
Uses LangChain + LangGraph as the agent loop, Ollama as the local SLM,
and the MCP client to discover and call tools from spark_mcp_server.py.

Architecture recap:
    You (CLI input)
        -> LangGraph ReAct agent
            -> ChatOllama (qwen3:8b via Ollama HTTP API)
                <-> tool calls
            -> MCP Client (stdio subprocess)
                -> spark_mcp_server.py
                    -> Prometheus HTTP API / SSH

"""

import os
import sys
import time
import argparse
import asyncio
from pathlib import Path

from langchain_ollama import ChatOllama
from langchain.agents import create_agent
from langgraph.prebuilt import create_react_agent
from langchain_mcp_adapters.client import MultiServerMCPClient

from dotenv import load_dotenv
from cluster_management_MCP.utils.system_prompts import SYSTEM_PROMPT
from cluster_management_MCP.utils.observability import (
    setup_logger,
    format_args,
    truncate,
    extract_thinking,
    dim, yellow, green, cyan, red,
)

# ---------------------------------------------------------------------------
# Configuration defaults
# ---------------------------------------------------------------------------

# DEFAULT_MODEL = "qwen3:4b"
# DEFAULT_MODEL = "gemma3:1b"
# DEFAULT_MODEL = "qwen2.5:1.5b" # Small model with good tool calls
DEFAULT_MODEL = "qwen3.5:2b" # Larger model with good tool calls

DEFAULT_OLLAMA_BASE_URL = os.environ.get("OLLAMA_HOST")
DEFAULT_SERVER_SCRIPT = Path(__file__).parent / "spark_mcp_server.py"

# Max lines of thinking to show in the console (full text always logged)
THINKING_PREVIEW_LINES = 6


async def run_cli(model: str, ollama_url: str, server_script: str) -> None:
    print(f"\n  Spark Cluster Manager CLI")
    print(f"  Model  : {model}")
    print(f"  Server : {server_script}")
    print(f"  Ollama : {ollama_url}")
    print(f"\n  Connecting to MCP server...", end="", flush=True)

    # ------------------------------------------------------------------
    # 1. MCP Client
    # ------------------------------------------------------------------
    mcp_client = MultiServerMCPClient(
        {
            "spark": {
                "command": "python",
                "args": [str(server_script)],
                "transport": "stdio",
            }
        }
    )

    tools = await mcp_client.get_tools()
    print(f" done. ({len(tools)} tools loaded)\n")

    print("  Available tools:")
    for t in tools:
        print(f"    - {t.name}")
    print()

    
    # Log is created on a daily basis
    logger, log_file = setup_logger()
    print(f"  Logging to: {log_file}\n")
    logger.info(f"SESSION_START | model={model} tools={[t.name for t in tools]}")


    llm = ChatOllama(
        model=model,
        base_url=ollama_url,
        num_ctx=4096,
        temperature=0,
    )


    agent = create_agent(
        model=llm,
        tools=tools,
        system_prompt=SYSTEM_PROMPT,
    )

    # ------------------------------------------------------------------
    # 5. REPL
    # ------------------------------------------------------------------
    print("  Type your question and press Enter. Type 'exit' to quit.\n")
    print("-" * 60)

    message_history = []

    while True:
        try:
            user_input = input("\nYou: ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\n\n  Exiting.")
            logger.info("SESSION_END | reason=interrupt")
            break

        if not user_input:
            continue

        if user_input.lower() in {"exit", "quit", "q"}:
            print("\n  Exiting.")
            logger.info("SESSION_END | reason=user_exit")
            break

        message_history.append({"role": "user", "content": user_input})
        logger.info(f"QUERY | {user_input}")

        query_start = time.perf_counter()
        all_new_messages: list = []
        # Maps tool_call_id -> (tool_name, start_time)
        tool_timers: dict[str, tuple[str, float]] = {}
        final_response = ""

        print(f"\nAgent:", flush=True)

        try:
            async for chunk in agent.astream({"messages": message_history}):

                # Normalise chunk: collect named-node dicts that carry messages.
                # LangGraph react agent uses {"agent": {...}, "tools": {...}};
                # other wrappers may use different keys or a flat {"messages": [...]}.
                node_items: list[tuple[str, list]] = []
                if isinstance(chunk, dict):
                    for key, val in chunk.items():
                        if isinstance(val, dict) and "messages" in val:
                            node_items.append((key, val["messages"]))
                        elif key == "messages" and isinstance(val, list):
                            node_items.append(("agent", val))

                for node_name, messages in node_items:
                    for msg in messages:
                        all_new_messages.append(msg)
                        thinking, text = extract_thinking(msg.content)

                        # Show thinking preview (truncated) and log in full
                        if thinking:
                            lines = thinking.splitlines()
                            preview = lines[:THINKING_PREVIEW_LINES]
                            hidden = len(lines) - THINKING_PREVIEW_LINES
                            print(f"\n  {dim('[Thinking]')}")
                            for line in preview:
                                print(f"  {dim(line)}")
                            if hidden > 0:
                                print(f"  {dim(f'… ({hidden} more lines in log)')}")
                            logger.debug(f"THINKING | {thinking}")

                        # Tool calls — show name + args, start timer
                        if hasattr(msg, "tool_calls") and msg.tool_calls:
                            for tc in msg.tool_calls:
                                elapsed = time.perf_counter() - query_start
                                args_str = format_args(tc["args"])
                                tc_name = tc["name"]
                                label = f"→ [{elapsed:5.1f}s] {tc_name}({args_str})"
                                print(f"\n  {yellow(label)}", flush=True)
                                logger.info(f"TOOL_START | {tc['name']} | args={args_str}")
                                tool_timers[tc["id"]] = (tc["name"], time.perf_counter())

                        # Tool result (ToolMessage carries tool_call_id)
                        elif hasattr(msg, "tool_call_id"):
                            tool_id: str = getattr(msg, "tool_call_id", "") or ""
                            fallback_name: str = getattr(msg, "name", "?") or "?"
                            tool_name, t_start = tool_timers.pop(tool_id, (fallback_name, query_start))
                            tool_duration = time.perf_counter() - t_start
                            elapsed = time.perf_counter() - query_start
                            result_preview = truncate(str(msg.content))
                            label = f"← [{elapsed:5.1f}s] {tool_name} ({tool_duration:.1f}s)"
                            print(f"  {green(label)} {dim(result_preview)}", flush=True)
                            logger.info(
                                f"TOOL_END | {tool_name} | duration={tool_duration:.2f}s"
                                f" | result={str(msg.content)[:800]}"
                            )

                        # Plain text with no tool calls = final (or intermediate) response
                        elif text:
                            final_response = text

            # Fallback: if the loop finished without capturing a response, pull the
            # last AIMessage with text content from the accumulated message list.
            if not final_response:
                from langchain_core.messages import AIMessage
                for msg in reversed(all_new_messages):
                    if isinstance(msg, AIMessage) and msg.content:
                        if not (hasattr(msg, "tool_calls") and msg.tool_calls):
                            _, text = extract_thinking(msg.content)
                            final_response = text or str(msg.content)
                            break

            query_duration = time.perf_counter() - query_start
            print(f"\n{final_response}")
            print(f"\n  {dim(f'[{query_duration:.1f}s]')}")
            logger.info(f"RESPONSE | {final_response}")
            logger.info(f"TIMING | query_duration={query_duration:.2f}s")

            # Persist full history for follow-up questions
            message_history = message_history + all_new_messages

        except Exception as e:
            print(f"\n  {red(f'[Error] {e}')}")
            print("  Check: is `ollama serve` running? Is the model pulled?")
            print("  Check: is spark_mcp_server.py free of import errors?")
            logger.error(f"ERROR | {e}", exc_info=True)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="CLI interface for the " \
        " cluster MCP server"
    )
    parser.add_argument(
        "--model",
        default=DEFAULT_MODEL,
        help=f"Ollama model name (default: {DEFAULT_MODEL})",
    )
    parser.add_argument(
        "--ollama-url",
        default=DEFAULT_OLLAMA_BASE_URL,
        help=f"Ollama base URL (default: {DEFAULT_OLLAMA_BASE_URL})",
    )
    parser.add_argument(
        "--server",
        default=str(DEFAULT_SERVER_SCRIPT),
        help=f"Path to spark_mcp_server.py (default: {DEFAULT_SERVER_SCRIPT})",
    )
    args = parser.parse_args()

    if not Path(args.server).exists():
        print(f"\n  [Error] MCP server script not found: {args.server}")
        print("  Pass the correct path with --server /path/to/spark_mcp_server.py")
        sys.exit(1)

    asyncio.run(run_cli(args.model, args.ollama_url, args.server))


if __name__ == "__main__":
    main()
