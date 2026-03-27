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
import argparse
import asyncio
from pathlib import Path

from langchain_ollama import ChatOllama
from langchain.agents import create_agent
from langgraph.prebuilt import create_react_agent
from langchain_mcp_adapters.client import MultiServerMCPClient

from dotenv import load_dotenv
from cluster_management_MCP.utils.system_prompts import SYSTEM_PROMPT

# ---------------------------------------------------------------------------
# Configuration defaults
# ---------------------------------------------------------------------------

# DEFAULT_MODEL = "qwen3:4b"
# DEFAULT_MODEL = "gemma3:1b"
# DEFAULT_MODEL = "qwen2.5:1.5b" # Small model with good tool calls
DEFAULT_MODEL = "qwen3.5:2b" # Larger model with good tool calls

DEFAULT_OLLAMA_BASE_URL = os.environ.get("OLLAMA_HOST")
DEFAULT_SERVER_SCRIPT = Path(__file__).parent / "spark_mcp_server.py"



async def run_cli(model: str, ollama_url: str, server_script: str) -> None:
    print(f"\n  Spark Cluster Manager CLI")
    print(f"  Model  : {model}")
    print(f"  Server : {server_script}")
    print(f"  Ollama : {ollama_url}")
    print(f"\n  Connecting to MCP server...", end="", flush=True)

    # ------------------------------------------------------------------
    # 1. MCP Client
    #
    # As of langchain-mcp-adapters 0.1.0, MultiServerMCPClient is NOT
    # a context manager. Instantiate directly, then call get_tools().
    #
    # Under the hood get_tools() spawns spark_mcp_server.py as a
    # subprocess and performs the tools/list handshake over stdio.
    # Any @mcp.tool() you add to the server is discovered automatically
    # the next time this runs - no manual registration here.
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

    # ------------------------------------------------------------------
    # 2. LLM
    #
    # ChatOllama talks to Ollama's OpenAI-compatible HTTP endpoint.
    # temperature=0 gives deterministic tool routing (no randomness).
    # num_ctx=32768 is recommended by Ollama for reliable tool calling.
    # ------------------------------------------------------------------
    llm = ChatOllama(
        model=model,
        base_url=ollama_url,
        # num_ctx=32768,
        temperature=0,
    )

    # ------------------------------------------------------------------
    # 3. Agent
    #
    # create_react_agent builds a LangGraph graph implementing ReAct:
    #   LLM reasons -> emits tool_use block -> tool executes via MCP
    #   -> result appended to messages -> LLM called again -> repeat
    #   until LLM emits a plain text response with no tool calls.
    #
    # This is the loop you would write manually without LangGraph.
    # ------------------------------------------------------------------
    agent = create_agent(
        model=llm,
        tools=tools,
        system_prompt=SYSTEM_PROMPT,
    )

    # ------------------------------------------------------------------
    # 4. REPL
    # ------------------------------------------------------------------
    print("  Type your question and press Enter. Type 'exit' to quit.\n")
    print("-" * 60)

    # Full message history accumulated across turns.
    # The LLM is stateless - context only exists because we re-send
    # the entire history on every call.
    message_history = []

    while True:
        try:
            user_input = input("\nYou: ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\n\n  Exiting.")
            break

        if not user_input:
            continue

        if user_input.lower() in {"exit", "quit", "q"}:
            print("\n  Exiting.")
            break

        message_history.append({"role": "user", "content": user_input})

        print("\nAgent: ", end="", flush=True)
        try:
            result = await agent.ainvoke({"messages": message_history})

            # The agent returns the full updated message list.
            # The last message is always the final assistant response.
            response_text = result["messages"][-1].content
            print(response_text)

            # Persist the updated history so follow-up questions have context.
            message_history = result["messages"]

        except Exception as e:
            print(f"\n  [Error] {e}")
            print("  Check: is `ollama serve` running? Is the model pulled?")
            print("  Check: is spark_mcp_server.py free of import errors?")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="CLI interface for the Spark cluster MCP server"
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