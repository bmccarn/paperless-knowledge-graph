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
from app.entity_resolver import entity_resolver
from app.cache import get_all_cache_stats, invalidate_on_sync

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
        cache_stats = get_all_cache_stats()
        return {
            "status": "healthy",
            "graph": counts,
            "embeddings": embedding_count,
            "last_sync": last_sync.isoformat() if last_sync else None,
            "active_tasks": {tid: t["status"] for tid, t in _tasks.items()},
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

    # Gemini API
    try:
        from google import genai
        from app.config import settings
        client = genai.Client(api_key=settings.gemini_api_key)
        response = client.models.generate_content(
            model=settings.gemini_model,
            contents="Say 'ok'",
        )
        components["gemini"] = {"status": "healthy"} if response.text else {"status": "degraded"}
    except Exception as e:
        components["gemini"] = {"status": "unhealthy", "error": str(e)}

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
    task_id = str(uuid.uuid4())
    _tasks[task_id] = {"status": "running", "started": datetime.now(timezone.utc).isoformat()}

    async def _run():
        try:
            invalidate_on_sync()
            result = await sync_documents()
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
            invalidate_on_sync()
            result = await reindex_all()
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
