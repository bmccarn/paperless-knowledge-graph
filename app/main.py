import asyncio
import logging
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from app.embeddings import embeddings_store
from app.graph import graph_store
from app.pipeline import sync_documents, reindex_all, reindex_document
from app.query import query_engine
from app.summarizer import summarize_all_entities, summarize_entity
from app.entity_resolver import entity_resolver

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)

# Track background tasks
_tasks: dict[str, dict] = {}


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting up...")
    await graph_store.init()
    await embeddings_store.init()
    logger.info("Startup complete")
    yield
    logger.info("Shutting down...")
    await graph_store.close()
    await embeddings_store.close()


app = FastAPI(
    title="Paperless Knowledge Graph",
    description="Knowledge graph extraction and query system for Paperless-ngx documents",
    version="1.0.0",
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


class TaskResponse(BaseModel):
    task_id: str
    status: str
    message: str


# --- Health & Status ---

@app.get("/status")
async def status():
    try:
        counts = await graph_store.get_counts()
        last_sync = await embeddings_store.get_last_sync()
        embedding_count = await embeddings_store.get_embedding_count()
        return {
            "status": "healthy",
            "graph": counts,
            "embeddings": embedding_count,
            "last_sync": last_sync.isoformat() if last_sync else None,
            "active_tasks": {tid: t["status"] for tid, t in _tasks.items()},
        }
    except Exception as e:
        return {"status": "degraded", "error": str(e)}


# --- Sync & Reindex ---

@app.post("/sync", response_model=TaskResponse)
async def sync():
    task_id = str(uuid.uuid4())
    _tasks[task_id] = {"status": "running", "started": datetime.now(timezone.utc).isoformat()}

    async def _run():
        try:
            result = await sync_documents()
            # Auto-summarize entities affected by sync
            if result.get("processed", 0) > 0:
                logger.info("Auto-triggering entity summarization after sync")
                try:
                    summary_result = await summarize_all_entities(force=False)
                    result["summarization"] = summary_result
                except Exception as se:
                    logger.error(f"Post-sync summarization failed: {se}")
                    result["summarization_error"] = str(se)
            _tasks[task_id] = {"status": "completed", "result": result}
        except Exception as e:
            logger.error(f"Sync task {task_id} failed: {e}", exc_info=True)
            _tasks[task_id] = {"status": "failed", "error": str(e)}

    asyncio.create_task(_run())
    return TaskResponse(task_id=task_id, status="started", message="Sync started in background")


@app.post("/reindex", response_model=TaskResponse)
async def reindex():
    task_id = str(uuid.uuid4())
    _tasks[task_id] = {"status": "running", "started": datetime.now(timezone.utc).isoformat()}

    async def _run():
        try:
            result = await reindex_all()
            # Auto-summarize all entities after full reindex
            logger.info("Auto-triggering entity summarization after reindex")
            try:
                summary_result = await summarize_all_entities(force=True)
                result["summarization"] = summary_result
            except Exception as se:
                logger.error(f"Post-reindex summarization failed: {se}")
                result["summarization_error"] = str(se)
            _tasks[task_id] = {"status": "completed", "result": result}
        except Exception as e:
            logger.error(f"Reindex task {task_id} failed: {e}", exc_info=True)
            _tasks[task_id] = {"status": "failed", "error": str(e)}

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
    return task


# --- Query ---

@app.post("/query")
async def query(req: QueryRequest):
    try:
        result = await query_engine.query(req.question)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))




# --- Summarization ---

@app.post("/summarize-entities", response_model=TaskResponse)
async def summarize_entities_endpoint(force: bool = False):
    """Generate descriptions for all entities needing summarization."""
    task_id = str(uuid.uuid4())
    _tasks[task_id] = {"status": "running", "started": datetime.now(timezone.utc).isoformat()}

    async def _run():
        try:
            result = await summarize_all_entities(force=force)
            _tasks[task_id] = {"status": "completed", "result": result}
        except Exception as e:
            logger.error(f"Summarization task {task_id} failed: {e}", exc_info=True)
            _tasks[task_id] = {"status": "failed", "error": str(e)}

    asyncio.create_task(_run())
    return TaskResponse(task_id=task_id, status="started", message="Entity summarization started in background")


@app.post("/summarize-entity/{entity_uuid}")
async def summarize_single_entity(entity_uuid: str):
    """Summarize or re-summarize a single entity."""
    try:
        result = await summarize_entity(entity_uuid)
        if result.get("status") == "not_found":
            raise HTTPException(status_code=404, detail="Entity not found")
        return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


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

