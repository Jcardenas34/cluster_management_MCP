#!/usr/bin/env python
import asyncio
import json
import time
import uuid
import mlflow
from pydantic import BaseModel
import logging
from langchain_core.messages import AIMessage
from cluster_management_MCP.utils.observability import (
    format_args,
    extract_thinking,
)
from cluster_management_MCP.utils.risk_tools import risk_results_payload, risk_job_status

message_history = []
turn_count: int = 0
logger = logging.getLogger("cluster_manager")

# ── Background jobs ───────────────────────────────────────────────────────────
# A tool that launches a long-running job returns a "job" hint; the backend then
# polls the job here, outside the chat turn, and queues an alert when it ends.
JOB_POLL_INTERVAL_S = 15
JOB_MAX_UNREACHABLE = 20  # consecutive failed polls (~5 min) before giving up on a job

# job type → status check (blocking, returns {"state", "detail"}) and display shown on success
_JOB_TYPES = {
    "risk_simulation": {
        "status": risk_job_status,
        "display": {"type": "risk_heatmap", "section": "raw"},
    },
}

active_jobs: dict[str, dict] = {}     # job id → {"id", "type", "label", "started_at"}
job_notifications: list[dict] = []    # finished-job alerts waiting for the UI to collect
_pending_history: list[dict] = []     # finished-job notes for the model, added on the next turn
_job_tasks: set[asyncio.Task] = set() # strong refs so watcher tasks aren't garbage-collected

class ChatRequest(BaseModel):
    message: str


def _result_field(tool_content, key: str) -> dict | None:
    """
    Return a dict-valued hint (e.g. "display", "job") from a tool result, if any.
    Tools return a dict with that key; MCP delivers it as JSON text blocks.
    """
    blocks = tool_content if isinstance(tool_content, list) else [tool_content]
    for block in blocks:
        text = block.get("text") if isinstance(block, dict) else block
        if not isinstance(text, str):
            continue
        try:
            parsed = json.loads(text)
        except ValueError:
            continue
        if isinstance(parsed, dict) and isinstance(parsed.get(key), dict):
            return parsed[key]
    return None


def _load_display(hint: dict) -> dict | None:
    """
    Fetch the data a display needs. Done here rather than in the tool so large
    tables reach the UI without filling the model's context window.
    """
    if hint.get("type") == "risk_heatmap":
        return {
            "type": "risk_heatmap",
            "section": hint.get("section", "raw"),
            "data": {s: risk_results_payload(s) for s in ("raw", "excluded")},
        }
    return None


def start_job_watcher(job: dict, *, started_at: float | None = None) -> None:
    """Track a launched background job and poll it until it finishes."""
    if job.get("type") not in _JOB_TYPES:
        logger.warning(f"JOB_UNKNOWN_TYPE | {job}")
        return
    job = {**job, "id": uuid.uuid4().hex[:8], "started_at": started_at}
    active_jobs[job["id"]] = job
    logger.info(f"JOB_START | {job['id']} | {job.get('label', job['type'])}")
    task = asyncio.create_task(_watch_job(job))
    _job_tasks.add(task)
    task.add_done_callback(_job_tasks.discard)


async def resume_job_watchers() -> None:
    """Re-attach to jobs still running after a backend restart (e.g. uvicorn --reload)."""
    if any(j["type"] == "risk_simulation" for j in active_jobs.values()):
        return
    try:
        status = await asyncio.to_thread(risk_job_status)
    except Exception as e:
        logger.warning(f"JOB_RESUME_FAILED | {e}")
        return
    if status["state"] == "running":
        start_job_watcher({"type": "risk_simulation", "label": "RISK simulation"})


async def _watch_job(job: dict) -> None:
    check = _JOB_TYPES[job["type"]]["status"]
    unreachable = 0
    try:
        while True:
            await asyncio.sleep(JOB_POLL_INTERVAL_S)
            try:
                status = await asyncio.to_thread(check)
            except Exception as e:
                status = {"state": "unreachable", "detail": str(e)}
            if status["state"] == "running":
                unreachable = 0
                continue
            if status["state"] == "unreachable":
                unreachable += 1
                if unreachable < JOB_MAX_UNREACHABLE:
                    continue
            break
        await _notify_job_finished(job, status)
    except Exception:
        logger.error(f"JOB_WATCH_FAILED | {job['id']}", exc_info=True)
    finally:
        active_jobs.pop(job["id"], None)


async def _notify_job_finished(job: dict, status: dict) -> None:
    label = job.get("label", job["type"])
    took = ""
    if job.get("started_at"):
        took = f" after {(time.time() - job['started_at']) / 60:.1f} min"
    state, detail = status["state"], status.get("detail", "")

    displays = []
    if state == "succeeded":
        outcome = "succeeded"
        content = f"**{label}** finished{took}."
        model_note = f"[Job notification] {label} finished successfully. The user was shown a heatmap; use lookup_win_rate for specific values."
        try:
            display = await asyncio.to_thread(_load_display, _JOB_TYPES[job["type"]]["display"])
            if display:
                displays.append(display)
        except Exception as e:
            logger.warning(f"DISPLAY_FAILED | job {job['id']} | {e}")
            content += f"\n\nCould not load the results: {e}"
    elif state == "unreachable":
        outcome = "unknown"
        content = f"Lost contact with the master node while watching **{label}**; its outcome is unknown.\n\n```\n{detail}\n```"
        model_note = f"[Job notification] Lost contact with the master node while watching {label}; outcome unknown."
    else:
        outcome = "failed"
        content = f"**{label}** failed{took}.\n\n```\n{detail or 'No job log found.'}\n```"
        model_note = f"[Job notification] {label} failed. Last log output:\n{detail[-300:]}"

    logger.info(f"JOB_END | {job['id']} | {outcome}{took} | {detail[-300:]}")
    job_notifications.append({"id": job["id"], "status": outcome, "content": content, "displays": displays})
    _pending_history.append({"role": "assistant", "content": model_note})


def drain_job_notifications() -> list[dict]:
    notes = job_notifications[:]
    job_notifications.clear()
    return notes


async def _run_chat(req: ChatRequest, agent, *, session_id: str = ""):
    '''Pass queries to model'''
    global message_history, turn_count
    turn_count += 1
    current_turn = turn_count

    # Let the model know about jobs that finished since the last turn
    message_history.extend(_pending_history)
    _pending_history.clear()
    message_history.append({"role": "user", "content": req.message})
    logger.info(f"QUERY | {req.message}")

    query_start = time.perf_counter()
    all_new_messages: list = []
    tool_timers: dict[str, tuple[str, float]] = {}
    final_response = ""
    _session_tagged = False  # tag trace on first chunk, when autolog trace is active

    last_final_ai_msg = None  # last AIMessage that is NOT a tool call
    display_hints: dict[str, dict] = {}  # one per display type, latest wins

    try:
        async for chunk in agent.astream(
            {"messages": message_history},
        config={"recursion_limit": 20}
        ):
            if not _session_tagged and session_id:
                try:
                    mlflow.update_current_trace(
                        metadata={"mlflow.trace.session": session_id}
                    )
                except Exception:
                    pass
                _session_tagged = True

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
                        hint = _result_field(msg.content, "display")
                        if hint:
                            display_hints[hint.get("type", "")] = hint
                        job = _result_field(msg.content, "job")
                        if job:
                            start_job_watcher(job, started_at=time.time())

                    elif isinstance(msg, AIMessage):
                        last_final_ai_msg = msg  # track for fallback
                        if text and "<tool_call>" not in text:
                            final_response = text
    except Exception as exc:
        logger.error(f"AGENT_ERROR | {exc}", exc_info=True)
        try:
            span = mlflow.get_current_active_span()
            if span:
                span.set_status("ERROR")
                span.set_attribute("error.message", str(exc))
        except Exception:
            pass
        raise

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

    def _log_mlflow(root_span, duration, history_len, usage, active_run, turn):
        if root_span is not None:
            root_span.set_attribute("query.duration_s", round(duration, 4))
            root_span.set_attribute("conversation.turn_count", history_len)
            root_span.set_attribute("llm.input_tokens", usage.get("input_tokens", 0))
            root_span.set_attribute("llm.output_tokens", usage.get("output_tokens", 0))
            root_span.set_attribute("llm.total_tokens", usage.get("total_tokens", 0))
        if active_run:
            # Log by run id: this runs in a worker thread, where the fluent API has no
            # active run and mlflow.log_metric would silently start a new one.
            client = mlflow.MlflowClient()
            run_id = active_run.info.run_id
            client.log_metric(run_id, "query_duration_s", round(duration, 4), step=turn)
            client.log_metric(run_id, "input_tokens",  usage.get("input_tokens", 0),  step=turn)
            client.log_metric(run_id, "output_tokens", usage.get("output_tokens", 0), step=turn)
            client.log_metric(run_id, "total_tokens",  usage.get("total_tokens", 0),  step=turn)

    try:
        root_span = mlflow.get_current_active_span()
        usage = getattr(last_final_ai_msg, "usage_metadata", None) or {}
        active_run = mlflow.active_run()
        await asyncio.wait_for(
            asyncio.to_thread(
                _log_mlflow, root_span, query_duration, len(message_history),
                usage, active_run, current_turn,
            ),
            timeout=5.0,
        )
    except (asyncio.TimeoutError, Exception):
        logger.debug("MLflow span annotation skipped (timeout or error)", exc_info=True)

    displays = []
    for hint in display_hints.values():
        try:
            display = await asyncio.to_thread(_load_display, hint)
            if display:
                displays.append(display)
        except Exception as e:
            logger.warning(f"DISPLAY_FAILED | {hint} | {e}")

    message_history = message_history + all_new_messages
    return {"reply": final_response, "displays": displays}
