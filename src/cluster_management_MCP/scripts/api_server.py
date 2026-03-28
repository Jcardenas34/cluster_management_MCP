import os
import asyncio
from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from langchain_ollama import ChatOllama
from langchain.agents import create_agent
from langchain_mcp_adapters.client import MultiServerMCPClient
from dotenv import load_dotenv
from cluster_management_MCP.utils.system_prompts import SYSTEM_PROMPT

load_dotenv()

DEFAULT_MODEL = "qwen3.5:2b"
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
    llm = ChatOllama(model=DEFAULT_MODEL, base_url=OLLAMA_URL, temperature=0)
    agent = create_agent(model=llm, tools=tools, system_prompt=SYSTEM_PROMPT)
    print(f"Agent ready. {len(tools)} tools loaded.")



class ChatRequest(BaseModel):
    message: str

@app.post("/chat")
async def chat(req: ChatRequest):
    global message_history
    message_history.append({"role": "user", "content": req.message})
    result = await agent.ainvoke({"messages": message_history})
    message_history = result["messages"]
    reply = result["messages"][-1].content
    return {"reply": reply}
