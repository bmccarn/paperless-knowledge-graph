import asyncio
import json
import logging
import uuid
import time
import os
from collections import deque
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional

from app.embeddings import embeddings_store
from app.graph import graph_store
from app.pipeline import sync_documents, reindex_all, reindex_document
from app.paperless import paperless_client
from app.query import query_engine
from app.entity_resolver import entity_resolver
from app.cache import get_all_cache_stats, invalidate_on_sync
from app import conversations
from starlette.responses import StreamingResponse

# --- In-memory log buffer & SSE ---
_log_buffer: deque = deque(maxlen=500)
_log_listeners: list[asyncio.Queue] = []

LOG_LEVELS = {"DEBUG": 10, "INFO": 20, "WARNING": 30, "ERROR": 40}


class BufferLogHandler(logging.Handler):
    """Captures log records into the in-memory buffer and notifies SSE listeners."""

    def emit(self, record: logging.LogRecord):
        entry = {
            "timestamp": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": self.format(record),
        }
        _log_buffer.append(entry)
        # Notify SSE listeners (fire-and-forget into each queue)
        for q in list(_log_listeners):
            try:
                q.put_nowait(entry)
            except Exception:
                pass


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(levelname)s %(message)s",
)

# Install the buffer handler on the root logger (after basicConfig)
_buffer_handler = BufferLogHandler()
_buffer_handler.setLevel(logging.DEBUG)
_buffer_handler.setFormatter(logging.Formatter("%(message)s"))
logging.getLogger().addHandler(_buffer_handler)
logger = logging.getLogger(__name__)

# Track background tasks
_tasks: dict[str, dict] = {}
_cancel_events: dict[str, asyncio.Event] = {}  # task_id -> cancel event
_last_failed_extraction: dict | None = None
_auto_sync_task: asyncio.Task | None = None


def _schedule_task_cleanup(task_id: str, delay: int = 300):
    """Remove a completed task from _tasks after `delay` seconds."""
    async def _cleanup():
        await asyncio.sleep(delay)
        _tasks.pop(task_id, None)
        _cancel_events.pop(task_id, None)
    asyncio.create_task(_cleanup())


def _make_progress_callback(task_id: str):
    """Create a progress callback that updates _tasks[task_id] in place."""
    def callback(event: str, data: dict):
        task = _tasks.get(task_id)
        if not task:
            return

        if event == "init":
            task["total_docs"] = data["total_docs"]

        elif event == "current":
            task["current_doc"] = data["title"]

        elif event == "result":
            result = data
            # Update counters
            if result.get("status") == "processed":
                task["processed"] = task.get("processed", 0) + 1
            elif result.get("status") == "skipped":
                task["skipped"] = task.get("skipped", 0) + 1
            elif result.get("status") == "error":
                task["errors"] = task.get("errors", 0) + 1

            # Timing
            started = task.get("_start_time", time.time())
            elapsed = time.time() - started
            task["elapsed_seconds"] = round(elapsed, 1)
            done = task.get("processed", 0) + task.get("skipped", 0) + task.get("errors", 0)
            total = task.get("total_docs", 0)
            if done > 0 and elapsed > 0:
                task["docs_per_minute"] = round(done / (elapsed / 60), 1)
                remaining = total - done
                secs_per_doc = elapsed / done
                task["estimated_remaining_seconds"] = round(remaining * secs_per_doc, 1)
            else:
                task["docs_per_minute"] = 0
                task["estimated_remaining_seconds"] = 0

            # Recent results (last 10)
            recent_entry = {"doc_id": result.get("doc_id"), "title": task.get("current_doc", ""), "status": result.get("status")}
            if result.get("status") == "processed":
                recent_entry["entities"] = result.get("entities_extracted", 0)
                recent_entry["relationships"] = result.get("chunks", 0)
            elif result.get("status") == "error":
                recent_entry["error"] = result.get("error", "")
                global _last_failed_extraction
                _last_failed_extraction = {
                    "doc_id": result.get("doc_id"),
                    "title": task.get("current_doc", ""),
                    "error": result.get("error", ""),
                    "task_id": task_id,
                    "at": datetime.now(timezone.utc).isoformat(),
                }
            recent = task.get("recent_results", [])
            recent.append(recent_entry)
            task["recent_results"] = recent[-10:]

    return callback


async def _freshness_snapshot() -> dict:
    """Compare Paperless source state with the indexed knowledge graph."""
    paperless = await paperless_client.get_document_summary()
    counts = await graph_store.get_counts()
    last_sync = await embeddings_store.get_last_sync()
    indexed_docs = counts.get("documents", 0)
    paperless_docs = paperless.get("count", 0)
    missing_docs = max(paperless_docs - indexed_docs, 0)

    latest_modified = paperless.get("latest_modified")
    stale = bool(missing_docs)
    if latest_modified and last_sync:
        try:
            latest_dt = datetime.fromisoformat(latest_modified.replace("Z", "+00:00"))
            stale = stale or latest_dt > last_sync
        except ValueError:
            stale = True
    elif latest_modified and not last_sync:
        stale = True

    return {
        "stale": stale,
        "paperless_documents": paperless_docs,
        "indexed_documents": indexed_docs,
        "missing_documents": missing_docs,
        "last_sync": last_sync.isoformat() if last_sync else None,
        "latest_paperless_modified": latest_modified,
        "latest_paperless_id": paperless.get("latest_id"),
        "latest_paperless_title": paperless.get("latest_title"),
        "last_failed_extraction": _last_failed_extraction,
    }


async def _run_sync_task(task_type: str = "sync") -> str:
    running = [t for t in _tasks.values() if t["status"] == "running"]
    if running:
        raise HTTPException(status_code=409, detail="A task is already running. Cancel it first or wait for it to finish.")

    task_id = str(uuid.uuid4())
    now = datetime.now(timezone.utc)
    _tasks[task_id] = {
        "status": "running",
        "type": task_type,
        "started": now.isoformat(),
        "_start_time": time.time(),
        "total_docs": 0,
        "processed": 0,
        "skipped": 0,
        "errors": 0,
        "current_doc": "",
        "elapsed_seconds": 0,
        "docs_per_minute": 0,
        "estimated_remaining_seconds": 0,
        "recent_results": [],
    }

    cancel_event = asyncio.Event()
    _cancel_events[task_id] = cancel_event
    progress_cb = _make_progress_callback(task_id)

    async def _run():
        try:
            invalidate_on_sync()
            result = await sync_documents(progress_callback=progress_cb, cancel_event=cancel_event)
            _tasks[task_id]["status"] = "completed"
            _tasks[task_id]["result"] = result
            _tasks[task_id]["current_doc"] = ""
            elapsed = time.time() - _tasks[task_id]["_start_time"]
            _tasks[task_id]["elapsed_seconds"] = round(elapsed, 1)
            _tasks[task_id]["estimated_remaining_seconds"] = 0
        except Exception as e:
            logger.error(f"Sync task {task_id} failed: {e}", exc_info=True)
            _tasks[task_id]["status"] = "failed"
            _tasks[task_id]["error"] = str(e)
            _tasks[task_id]["current_doc"] = ""
        finally:
            _schedule_task_cleanup(task_id)

    asyncio.create_task(_run())
    return task_id


async def _auto_sync_loop():
    from app.config import settings

    interval = max(settings.auto_sync_interval_minutes, 0)
    if interval <= 0:
        return

    logger.info("Auto sync enabled: every %s minutes", interval)
    while True:
        await asyncio.sleep(interval * 60)
        if any(t["status"] == "running" for t in _tasks.values()):
            logger.info("Auto sync skipped because a task is already running")
            continue
        try:
            await _run_sync_task(task_type="scheduled-sync")
        except Exception as e:
            logger.error("Auto sync failed to start: %s", e)


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _auto_sync_task
    logger.info("Starting up...")
    await graph_store.init()
    await embeddings_store.init()
    await conversations.init()
    _auto_sync_task = asyncio.create_task(_auto_sync_loop())
    logger.info("Startup complete")
    yield
    logger.info("Shutting down...")
    if _auto_sync_task:
        _auto_sync_task.cancel()
    await graph_store.close()
    await embeddings_store.close()
    await conversations.close()


app = FastAPI(
    title="Paperless Knowledge Graph",
    description="Knowledge graph extraction and query system for Paperless-ngx documents",
    version="2.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class QueryRequest(BaseModel):
    question: str
    conversation_id: Optional[str] = None
    model: Optional[str] = None
    mode: str = "deep"


class TaskResponse(BaseModel):
    task_id: str
    status: str
    message: str


class DocumentFeedbackRequest(BaseModel):
    reason: str = "extraction_wrong"
    note: str = ""


class EntityDecisionRequest(BaseModel):
    left_uuid: str
    right_uuid: str
    note: str = ""


class EntityMergeRequest(BaseModel):
    primary_uuid: str
    duplicate_uuid: str


# --- Paperless URL ---

def _get_paperless_url() -> str:
    from app.config import settings
    return settings.effective_paperless_external_url


@app.get("/config")
async def get_config():
    return {"paperless_url": _get_paperless_url()}


# --- Health & Status ---

@app.get("/status")
async def status():
    try:
        counts = await graph_store.get_counts()
        last_sync = await embeddings_store.get_last_sync()
        doc_embed_count = await embeddings_store.get_embedding_count()
        ent_embed_count = await embeddings_store.get_entity_embedding_count()
        docs_w_embeds = await embeddings_store.get_docs_with_embeddings_count()
        cache_stats = get_all_cache_stats()
        return {
            "status": "healthy",
            "graph": counts,
            "embeddings": {
                "document_chunks": doc_embed_count,
                "entity_embeddings": ent_embed_count,
                "docs_with_embeddings": docs_w_embeds,
            },
            "last_sync": last_sync.isoformat() if last_sync else None,
            "paperless_url": _get_paperless_url(),
            "active_tasks": {tid: {"status": t["status"], "type": t.get("type", "unknown")} for tid, t in _tasks.items()},
            "cache": cache_stats,
            "freshness": await _freshness_snapshot(),
        }
    except Exception as e:
        return {"status": "degraded", "error": str(e)}


@app.get("/freshness")
async def freshness():
    return await _freshness_snapshot()


@app.get("/health")
async def health():
    """Detailed component health check."""
    components = {}

    # Neo4j
    try:
        components["neo4j"] = await graph_store.check_health()
    except Exception as e:
        components["neo4j"] = {"status": "unhealthy", "error": str(e)}

    # pgvector
    try:
        count = await embeddings_store.get_embedding_count()
        components["pgvector"] = {"status": "healthy", "embedding_count": count}
    except Exception as e:
        components["pgvector"] = {"status": "unhealthy", "error": str(e)}

    # LiteLLM (LLM + embeddings gateway)
    try:
        from openai import AsyncOpenAI
        from app.config import settings
        client = AsyncOpenAI(base_url=settings.litellm_url, api_key=settings.litellm_api_key)
        response = await client.chat.completions.create(
            model=settings.gemini_model,
            messages=[{"role": "user", "content": "Say 'ok'"}],
            max_tokens=5,
        )
        components["litellm"] = {"status": "healthy"} if response.choices else {"status": "degraded"}
    except Exception as e:
        components["litellm"] = {"status": "unhealthy", "error": str(e)}

    # Cache
    components["cache"] = get_all_cache_stats()

    overall = "healthy"
    for key, comp in components.items():
        if key == "cache":
            continue
        if isinstance(comp, dict) and comp.get("status") == "unhealthy":
            overall = "unhealthy"
            break
        elif isinstance(comp, dict) and comp.get("status") == "degraded":
            overall = "degraded"

    return {"status": overall, "components": components}


@app.get("/ops/guardrails")
async def ops_guardrails(max_sync_age_hours: int = 24, allowed_doc_drift: int = 0):
    """Machine-readable guardrail checks for monitoring."""
    alerts = []
    counts = await graph_store.get_counts()
    last_sync = await embeddings_store.get_last_sync()
    health_status = await health()

    if last_sync:
        age_hours = (datetime.now(timezone.utc) - last_sync.astimezone(timezone.utc)).total_seconds() / 3600
    else:
        age_hours = None
        alerts.append({"type": "sync_never_ran", "severity": "warning", "message": "Knowledge graph has never synced"})

    if age_hours is not None and age_hours > max_sync_age_hours:
        alerts.append({
            "type": "sync_stale",
            "severity": "warning",
            "message": f"Last sync is {age_hours:.1f} hours old",
        })

    try:
        source = await paperless_client.get_document_summary()
        drift = int(source.get("count", 0)) - int(counts.get("documents", 0))
        if drift > allowed_doc_drift:
            alerts.append({
                "type": "doc_count_drift",
                "severity": "warning",
                "message": f"Paperless has {drift} more documents than the graph",
            })
    except Exception as e:
        source = {"error": str(e)}
        drift = None
        alerts.append({"type": "paperless_unreachable", "severity": "critical", "message": str(e)})

    litellm_status = health_status.get("components", {}).get("litellm", {}).get("status")
    if litellm_status != "healthy":
        alerts.append({
            "type": "model_route_health",
            "severity": "critical",
            "message": f"LiteLLM health is {litellm_status or 'unknown'}",
        })

    error_logs = [
        line for line in list(_log_buffer)[-200:]
        if line.get("level") == "ERROR" or "confidence=0." in line.get("message", "")
    ]
    if error_logs:
        alerts.append({
            "type": "recent_extraction_or_runtime_errors",
            "severity": "warning",
            "message": f"{len(error_logs)} recent error/low-confidence log lines",
        })

    return {
        "status": "ok" if not alerts else "alerting",
        "alerts": alerts,
        "last_sync": last_sync.isoformat() if last_sync else None,
        "sync_age_hours": round(age_hours, 2) if age_hours is not None else None,
        "graph_documents": counts.get("documents", 0),
        "paperless": source,
        "document_drift": drift,
    }


# --- Sync & Reindex ---

@app.post("/sync", response_model=TaskResponse)
async def sync():
    task_id = await _run_sync_task()
    return TaskResponse(task_id=task_id, status="started", message="Sync started in background")


@app.post("/reindex", response_model=TaskResponse)
async def reindex():
    # Prevent concurrent reindex/sync
    running = [t for t in _tasks.values() if t["status"] == "running"]
    if running:
        raise HTTPException(status_code=409, detail="A task is already running. Cancel it first or wait for it to finish.")
    task_id = str(uuid.uuid4())
    now = datetime.now(timezone.utc)
    _tasks[task_id] = {
        "status": "running",
        "type": "reindex",
        "started": now.isoformat(),
        "_start_time": time.time(),
        "total_docs": 0,
        "processed": 0,
        "skipped": 0,
        "errors": 0,
        "current_doc": "",
        "elapsed_seconds": 0,
        "docs_per_minute": 0,
        "estimated_remaining_seconds": 0,
        "recent_results": [],
    }

    cancel_event = asyncio.Event()
    _cancel_events[task_id] = cancel_event
    progress_cb = _make_progress_callback(task_id)

    async def _run():
        try:
            invalidate_on_sync()
            result = await reindex_all(progress_callback=progress_cb, cancel_event=cancel_event)
            _tasks[task_id]["status"] = "completed"
            _tasks[task_id]["result"] = result
            _tasks[task_id]["current_doc"] = ""
            elapsed = time.time() - _tasks[task_id]["_start_time"]
            _tasks[task_id]["elapsed_seconds"] = round(elapsed, 1)
            _tasks[task_id]["estimated_remaining_seconds"] = 0
        except Exception as e:
            logger.error(f"Reindex task {task_id} failed: {e}", exc_info=True)
            _tasks[task_id]["status"] = "failed"
            _tasks[task_id]["error"] = str(e)
            _tasks[task_id]["current_doc"] = ""
        finally:
            _schedule_task_cleanup(task_id)

    asyncio.create_task(_run())
    return TaskResponse(task_id=task_id, status="started", message="Full reindex started in background")


@app.post("/reindex/{doc_id}")
async def reindex_single(doc_id: int):
    try:
        result = await reindex_document(doc_id)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/document/{doc_id}/detail")
async def document_detail(doc_id: int):
    """Full inspection payload for one Paperless document."""
    try:
        paperless_doc = await paperless_client.get_document(doc_id)
        graph_detail = await graph_store.get_document_detail_graph(doc_id)
        chunks = await embeddings_store.get_document_chunks(doc_id)
        processing = await embeddings_store.get_document_processing_status(doc_id)
        return {
            "paperless": paperless_doc,
            "graph": graph_detail,
            "chunks": chunks,
            "processing": processing,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/document/{doc_id}/feedback")
async def document_feedback(doc_id: int, req: DocumentFeedbackRequest):
    """Record that a document extraction needs human review."""
    try:
        result = await embeddings_store.add_document_feedback(doc_id, req.reason, req.note)
        logger.warning("Document %s marked for extraction review: %s", doc_id, req.reason)
        return {"status": "recorded", "feedback": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/document/{doc_id}")
async def delete_document(doc_id: int):
    """Remove a document and all its entities/relationships from the knowledge graph."""
    try:
        await graph_store.delete_document_graph(doc_id)
        await embeddings_store.delete_document_embeddings(doc_id)
        await embeddings_store.delete_doc_hash(doc_id)
        invalidate_on_sync()
        logger.info(f"Deleted document {doc_id} from knowledge graph")
        return {"status": "deleted", "doc_id": doc_id}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/task/{task_id}")
async def get_task(task_id: str):
    task = _tasks.get(task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    # Return a copy without internal fields
    return {k: v for k, v in task.items() if not k.startswith("_")}




# --- Conversations ---

class ConversationCreate(BaseModel):
    title: str = "New conversation"

class ConversationRename(BaseModel):
    title: str

@app.get("/conversations")
async def list_conversations(limit: int = 50, offset: int = 0):
    return await conversations.list_conversations(limit=limit, offset=offset)

@app.post("/conversations")
async def create_conversation(req: ConversationCreate):
    return await conversations.create_conversation(title=req.title)

@app.get("/conversations/{conv_id}")
async def get_conversation(conv_id: str):
    conv = await conversations.get_conversation(conv_id)
    if not conv:
        raise HTTPException(status_code=404, detail="Conversation not found")
    return conv

@app.patch("/conversations/{conv_id}")
async def rename_conversation(conv_id: str, req: ConversationRename):
    result = await conversations.rename_conversation(conv_id, req.title)
    if not result:
        raise HTTPException(status_code=404, detail="Conversation not found")
    return result

@app.delete("/conversations/{conv_id}")
async def delete_conversation(conv_id: str):
    ok = await conversations.delete_conversation(conv_id)
    if not ok:
        raise HTTPException(status_code=404, detail="Conversation not found")
    return {"status": "deleted"}


class GenerateTitleRequest(BaseModel):
    message: str


@app.post("/generate-title")
async def generate_title(req: GenerateTitleRequest):
    """Generate a short, descriptive chat title from the user's message."""
    title = await conversations._generate_title(req.message, "")
    return {"title": title}

# --- Query ---

@app.post("/task/{task_id}/cancel")
async def cancel_task(task_id: str):
    """Cancel a running task."""
    cancel_event = _cancel_events.get(task_id)
    if not cancel_event:
        raise HTTPException(status_code=404, detail="Task not found or already completed")
    cancel_event.set()
    task = _tasks.get(task_id)
    if task:
        task["status"] = "cancelled"
    return {"status": "cancelled", "task_id": task_id}



@app.get("/models")
async def list_models():
    """List available chat models from LiteLLM."""
    from app.config import settings
    import httpx

    base_url = settings.litellm_url.rstrip("/")
    headers = {"Authorization": f"Bearer {settings.litellm_api_key}"}

    try:
        async with httpx.AsyncClient() as client:
            resp = await client.get(
                f"{base_url}/model/info",
                headers=headers,
                timeout=10,
            )
            resp.raise_for_status()
            models = _models_from_litellm_info(resp.json())
            if not models:
                raise ValueError("LiteLLM /model/info returned no chat models")
            return {"models": models, "default": settings.gemini_model}
    except Exception as e:
        logger.warning(f"Failed to list models from LiteLLM /model/info: {e}")

    try:
        async with httpx.AsyncClient() as client:
            resp = await client.get(
                f"{base_url}/v1/models",
                headers=headers,
                timeout=10,
            )
            resp.raise_for_status()
            models = _models_from_openai_models(resp.json())
            if not models:
                raise ValueError("LiteLLM /v1/models returned no chat models")
            return {"models": models, "default": settings.gemini_model}
    except Exception as e:
        logger.error(f"Failed to list models from LiteLLM fallback: {e}")
        return {"models": [_model_option(settings.gemini_model)], "default": settings.gemini_model}


def _models_from_litellm_info(data: dict) -> list[dict]:
    models = []
    for item in data.get("data", []):
        model_id = item.get("model_name") or item.get("id") or item.get("model")
        if not model_id or _is_embedding_model(model_id, item):
            continue
        models.append(_model_option(model_id))
    return _unique_sorted_models(models)


def _models_from_openai_models(data: dict) -> list[dict]:
    models = []
    for item in data.get("data", []):
        model_id = item.get("id") or item.get("model_name") or item.get("model")
        if not model_id or _is_embedding_model(model_id, item):
            continue
        models.append(_model_option(model_id))
    return _unique_sorted_models(models)


def _is_embedding_model(model_id: str, item: dict) -> bool:
    mode = (item.get("mode") or item.get("model_info", {}).get("mode") or "").lower()
    if mode in {"embedding", "embeddings"}:
        return True
    model_text = f"{model_id} {item.get('litellm_params', {}).get('model', '')}".lower()
    return any(keyword in model_text for keyword in ("embed", "embedding", "titan-embed"))


def _model_option(model_id: str) -> dict:
    # Display the LiteLLM route ID exactly as returned so provider prefixes,
    # decimals, and version strings are not mangled in the chat selector.
    return {"id": model_id, "name": model_id}


def _unique_sorted_models(models: list[dict]) -> list[dict]:
    unique = {model["id"]: model for model in models}
    return sorted(unique.values(), key=lambda model: model["name"].lower())

@app.post("/query")
async def query(req: QueryRequest):
    try:
        # Get conversation history if conversation_id provided
        conv_history = None
        if req.conversation_id:
            conv_history = await conversations.get_conversation_history(req.conversation_id)

        result = await query_engine.query(req.question, conversation_history=conv_history, model_override=req.model, mode=req.mode)
        paperless_base = _get_paperless_url()
        for source in result.get("sources", []):
            if source.get("document_id"):
                source["paperless_url"] = f"{paperless_base}/documents/{source['document_id']}/details"

        # Save to conversation if conversation_id provided
        if req.conversation_id:
            await conversations.add_message(req.conversation_id, "user", req.question)
            await conversations.add_message(
                req.conversation_id, "assistant", result["answer"],
                sources=result.get("sources"),
                entities=result.get("entities_found"),
                confidence=result.get("confidence"),
                follow_ups=result.get("follow_up_suggestions"),
            )

        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/query/stream")
async def query_stream(req: QueryRequest):
    """Streaming query endpoint with Server-Sent Events.
    
    Uses background task + queue so the query completes and saves
    even if the client disconnects (e.g. mobile tab backgrounded).
    """
    import asyncio

    conv_history = None
    if req.conversation_id:
        conv_history = await conversations.get_conversation_history(req.conversation_id)
        await conversations.add_message(req.conversation_id, "user", req.question)

    queue: asyncio.Queue = asyncio.Queue()

    async def _run_query():
        """Background task: runs query to completion, saves result regardless of client."""
        try:
            paperless_base = _get_paperless_url()
            full_answer_chunks = []
            final_sources = None
            final_entities = None
            final_confidence = None
            final_follow_ups = None

            async for event in query_engine.query_stream(req.question, conversation_history=conv_history, model_override=req.model, mode=req.mode):
                if event.get("type") == "answer_chunk":
                    full_answer_chunks.append(event.get("content", ""))
                if event.get("type") == "complete" and event.get("sources"):
                    for source in event["sources"]:
                        if source.get("document_id"):
                            source["paperless_url"] = f"{paperless_base}/documents/{source['document_id']}/details"
                if event.get("type") == "complete":
                    final_sources = event.get("sources")
                    final_entities = event.get("entities_found")
                    final_confidence = event.get("confidence")
                    final_follow_ups = event.get("follow_up_suggestions")

                await queue.put(event)

                if event.get("type") == "complete" and req.conversation_id:
                    full_answer = "".join(full_answer_chunks)
                    await conversations.add_message(
                        req.conversation_id, "assistant", full_answer,
                        sources=final_sources,
                        entities=final_entities,
                        confidence=final_confidence,
                        follow_ups=final_follow_ups,
                    )
        except Exception as e:
            await queue.put({"type": "error", "message": str(e)})
        finally:
            await queue.put(None)  # sentinel

    # Start query as background task — runs to completion even if client disconnects
    asyncio.create_task(_run_query())

    async def event_generator():
        try:
            while True:
                try:
                    event = await asyncio.wait_for(queue.get(), timeout=15.0)
                except asyncio.TimeoutError:
                    # Send SSE comment as keepalive to prevent connection timeout
                    yield ": keepalive\n\n"
                    continue
                if event is None:
                    break
                yield f"data: {json.dumps(event)}" + "\n\n"
        except asyncio.CancelledError:
            pass  # Client disconnected — query task still running in background

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache, no-store, no-transform", "Connection": "keep-alive", "X-Accel-Buffering": "no"},
    )

# --- Graph Browsing ---

@app.get("/graph/search")
async def graph_search(q: str, type: str = None, limit: int = 20):
    results = await graph_store.search_nodes(q, node_type=type, limit=limit)
    return {"query": q, "type": type, "results": results}


@app.get("/graph/node/{node_uuid}")
async def graph_node(node_uuid: str):
    node = await graph_store.get_node(node_uuid)
    if not node:
        raise HTTPException(status_code=404, detail="Node not found")
    return node


@app.get("/graph/neighbors/{node_uuid}")
async def graph_neighbors(node_uuid: str, depth: int = 2):
    result = await graph_store.get_neighbors(node_uuid, depth=min(depth, 4))
    return result


# --- Graph Initial Load ---

@app.get("/graph/initial")
async def graph_initial(limit: int = 300):
    """Return an initial set of nodes for graph visualization.
    Loads Person and Organization nodes with their connections."""
    result = await graph_store.get_initial_graph(limit=limit)
    return result


# --- Entity Resolution ---

@app.post("/resolve-entities")
async def resolve_entities():
    """Scan all entities and merge duplicates."""
    try:
        report = await entity_resolver.resolve_all_entities()
        return report
    except Exception as e:
        logger.error(f"Entity resolution failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/entity-review/candidates")
async def entity_review_candidates(limit: int = 50):
    decisions = await embeddings_store.get_entity_review_decisions()
    ignored_pairs = {
        tuple(sorted([d["left_uuid"], d["right_uuid"]]))
        for d in decisions
        if d["decision"] in {"ignore", "split", "merged"}
    }
    candidates = await graph_store.get_entity_review_candidates(ignored_pairs, limit=limit)
    return {"candidates": candidates, "ignored_count": len(ignored_pairs)}


@app.post("/entity-review/ignore")
async def entity_review_ignore(req: EntityDecisionRequest):
    decision = await embeddings_store.add_entity_review_decision(
        req.left_uuid, req.right_uuid, "ignore", req.note
    )
    return {"status": "ignored", "decision": decision}


@app.post("/entity-review/split")
async def entity_review_split(req: EntityDecisionRequest):
    decision = await embeddings_store.add_entity_review_decision(
        req.left_uuid, req.right_uuid, "split", req.note
    )
    return {"status": "split_requested", "decision": decision}


@app.post("/entity-review/merge")
async def entity_review_merge(req: EntityMergeRequest):
    try:
        merged = await graph_store.merge_entities(req.primary_uuid, req.duplicate_uuid)
        await embeddings_store.add_entity_review_decision(
            req.primary_uuid, req.duplicate_uuid, "merged", ""
        )
        return {"status": "merged", "entity": merged}
    except Exception as e:
        logger.error("Entity merge failed: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))




# --- Log Endpoints ---

@app.get("/logs")
async def get_logs(limit: int = 100, level: str = None, since: str = None):
    """Return buffered log lines with optional filtering."""
    lines = list(_log_buffer)
    if level and level.upper() in LOG_LEVELS:
        min_level = LOG_LEVELS[level.upper()]
        lines = [l for l in lines if LOG_LEVELS.get(l["level"], 0) >= min_level]
    if since:
        lines = [l for l in lines if l["timestamp"] > since]
    return {"lines": lines[-limit:], "total": len(_log_buffer)}


@app.get("/logs/stream")
async def stream_logs():
    """SSE endpoint for real-time log streaming."""
    queue: asyncio.Queue = asyncio.Queue()
    _log_listeners.append(queue)

    async def event_generator():
        try:
            while True:
                line = await queue.get()
                yield f"data: {json.dumps(line)}\n\n"
        except asyncio.CancelledError:
            pass
        finally:
            if queue in _log_listeners:
                _log_listeners.remove(queue)

    return StreamingResponse(event_generator(), media_type="text/event-stream")


@app.post("/create-indexes")
async def create_indexes():
    """Create vector indexes after data is loaded."""
    try:
        await embeddings_store.create_vector_indexes()
        return {"status": "ok", "message": "Vector indexes created"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
