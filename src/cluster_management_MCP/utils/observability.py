"""
Observability utilities for the Spark Cluster Manager CLI.

Provides:
  - setup_logger()       : rotating daily file logger
  - format_args()        : compact JSON serialisation of tool args
  - truncate()           : cap long strings for console display
  - extract_thinking()   : split qwen3 <think> blocks from response text
"""

import json
import logging
import re
from datetime import datetime
from pathlib import Path


# ---------------------------------------------------------------------------
# ANSI colour helpers (Windows Terminal / VSCode terminal support these)
# ---------------------------------------------------------------------------

RESET  = "\033[0m"
DIM    = "\033[2m"
YELLOW = "\033[33m"
GREEN  = "\033[32m"
CYAN   = "\033[36m"
RED    = "\033[31m"


def _c(code: str, text: str) -> str:
    return f"{code}{text}{RESET}"


def dim(text: str)    -> str: return _c(DIM,    text)
def yellow(text: str) -> str: return _c(YELLOW, text)
def green(text: str)  -> str: return _c(GREEN,  text)
def cyan(text: str)   -> str: return _c(CYAN,   text)
def red(text: str)    -> str: return _c(RED,    text)


# ---------------------------------------------------------------------------
# Logger
# ---------------------------------------------------------------------------

def setup_logger(log_dir: Path | None = None) -> tuple[logging.Logger, Path]:
    """
    Configure a file logger that writes one log file per day under log_dir.
    Defaults to <repo_root>/logs/YYYY-MM-DD.log.

    Returns (logger, log_file_path).
    """
    if log_dir is None:
        # observability.py lives at src/cluster_management_MCP/utils/
        # so three .parent calls reach the repo root
        log_dir = Path(__file__).parents[3] / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)

    log_file = log_dir / f"{datetime.now().strftime('%Y-%m-%d')}.log"

    logger = logging.getLogger("cluster_manager")
    logger.setLevel(logging.DEBUG)

    # Avoid duplicate handlers if run_cli is called more than once in a session
    if not logger.handlers:
        handler = logging.FileHandler(log_file, encoding="utf-8")
        handler.setFormatter(
            logging.Formatter("%(asctime)s [%(levelname)-10s] %(message)s")
        )
        logger.addHandler(handler)

    return logger, log_file


# ---------------------------------------------------------------------------
# Formatting helpers
# ---------------------------------------------------------------------------

def format_args(args: dict, max_len: int = 120) -> str:
    """Compact JSON representation of tool arguments, truncated to max_len."""
    try:
        s = json.dumps(args, ensure_ascii=False, separators=(",", ":"))
    except Exception:
        s = str(args)
    return s[:max_len] + "…" if len(s) > max_len else s


def truncate(s: str, max_len: int = 400) -> str:
    """Truncate a string and append a length note when cut."""
    s = str(s)
    if len(s) > max_len:
        return s[:max_len] + dim(f" … [{len(s) - max_len} chars hidden]")
    return s


# ---------------------------------------------------------------------------
# Thinking-block extraction (qwen3 emits <think>…</think> in content)
# ---------------------------------------------------------------------------

def extract_thinking(content, additional_kwargs: dict | None = None) -> tuple[str, str]:
    """
    Split an AIMessage's content into (thinking_text, response_text).

    Handles:
      - additional_kwargs["reasoning_content"] (langchain-ollama reasoning=True)
      - List of content blocks: {"type": "thinking", "thinking": "…"}
      - Plain string containing <think>…</think> tags
    """
    if additional_kwargs and additional_kwargs.get("reasoning_content"):
        return additional_kwargs["reasoning_content"].strip(), str(content).strip()

    if isinstance(content, list):
        thinking_parts, text_parts = [], []
        for block in content:
            if not isinstance(block, dict):
                continue
            if block.get("type") == "thinking":
                thinking_parts.append(block.get("thinking", ""))
            elif block.get("type") == "text":
                text_parts.append(block.get("text", ""))
        return "\n".join(thinking_parts).strip(), "\n".join(text_parts).strip()

    if isinstance(content, str):
        match = re.search(r"<think>(.*?)</think>", content, re.DOTALL)
        if match:
            thinking = match.group(1).strip()
            text = re.sub(r"<think>.*?</think>", "", content, flags=re.DOTALL).strip()
            return thinking, text

    return "", str(content).strip() if content else ""
