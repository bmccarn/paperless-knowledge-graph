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
            recent = task.get("recent_results", [])
            recent.append(recent_entry)
            task["recent_results"] = recent[-10:]

    return callback


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting up...")
    await graph_store.init()
    await embeddings_store.init()
    await conversations.init()
    logger.info("Startup complete")
    yield
    logger.info("Shutting down...")
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


class TaskResponse(BaseModel):
    task_id: str
    status: str
    message: str



# --- Paperless URL ---

def _get_paperless_url() -> str:
    return os.getenv("PAPERLESS_EXTERNAL_URL", "http://your-paperless-host:8000")


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
        }
    except Exception as e:
        return {"status": "degraded", "error": str(e)}


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


# --- Sync & Reindex ---

@app.post("/sync", response_model=TaskResponse)
async def sync():
    # Prevent concurrent reindex/sync
    running = [t for t in _tasks.values() if t["status"] == "running"]
    if running:
        raise HTTPException(status_code=409, detail="A task is already running. Cancel it first or wait for it to finish.")
    task_id = str(uuid.uuid4())
    now = datetime.now(timezone.utc)
    _tasks[task_id] = {
        "status": "running",
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

    asyncio.create_task(_run())
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

    asyncio.create_task(_run())
    return TaskResponse(task_id=task_id, status="started", message="Full reindex started in background")


@app.post("/reindex/{doc_id}")
async def reindex_single(doc_id: int):
    try:
        result = await reindex_document(doc_id)
        return result
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


@app.post("/query")
async def query(req: QueryRequest):
    try:
        # Get conversation history if conversation_id provided
        conv_history = None
        if req.conversation_id:
            conv_history = await conversations.get_conversation_history(req.conversation_id)

        result = await query_engine.query(req.question, conversation_history=conv_history)
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
    """Streaming query endpoint with Server-Sent Events."""
    # Get conversation history before entering generator
    conv_history = None
    if req.conversation_id:
        conv_history = await conversations.get_conversation_history(req.conversation_id)
        # Save user message immediately
        await conversations.add_message(req.conversation_id, "user", req.question)

    async def event_generator():
        try:
            paperless_base = _get_paperless_url()
            full_answer_chunks = []
            final_sources = None
            final_entities = None
            final_confidence = None
            final_follow_ups = None

            async for event in query_engine.query_stream(req.question, conversation_history=conv_history):
                # Collect answer for saving
                if event.get("type") == "answer_chunk":
                    full_answer_chunks.append(event.get("content", ""))
                if event.get("type") == "complete" and event.get("sources"):
                    for source in event["sources"]:
                        if source.get("document_id"):
                            source["paperless_url"] = f"{paperless_base}/documents/{source['document_id']}/details"
                # Capture metadata from complete event
                if event.get("type") == "complete":
                    final_sources = event.get("sources")
                    final_entities = event.get("entities_found")
                    final_confidence = event.get("confidence")
                    final_follow_ups = event.get("follow_up_suggestions")

                yield f"data: {json.dumps(event)}" + "\n\n"

                # Save assistant message after complete
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
            err = {"type": "error", "message": str(e)}
            yield f"data: {json.dumps(err)}" + "\n\n"

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "Connection": "keep-alive", "X-Accel-Buffering": "no"},
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
