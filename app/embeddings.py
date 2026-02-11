import logging
from typing import Optional

import asyncpg
from openai import AsyncOpenAI

from app.config import settings

logger = logging.getLogger(__name__)

INIT_SQL = """
CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE IF NOT EXISTS document_embeddings (
    id SERIAL PRIMARY KEY,
    document_id INTEGER NOT NULL,
    chunk_index INTEGER NOT NULL DEFAULT 0,
    content TEXT NOT NULL,
    embedding vector(1536),
    created_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(document_id, chunk_index)
);

CREATE TABLE IF NOT EXISTS sync_state (
    id INTEGER PRIMARY KEY DEFAULT 1,
    last_sync_at TIMESTAMP,
    updated_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS document_hashes (
    document_id INTEGER PRIMARY KEY,
    content_hash VARCHAR(64) NOT NULL,
    processed_at TIMESTAMP DEFAULT NOW()
);

INSERT INTO sync_state (id, last_sync_at) VALUES (1, NULL)
ON CONFLICT (id) DO NOTHING;

CREATE INDEX IF NOT EXISTS idx_doc_embeddings_doc_id ON document_embeddings(document_id);
"""


class EmbeddingsStore:
    def __init__(self):
        self.pool: Optional[asyncpg.Pool] = None
        self.openai = AsyncOpenAI(
            base_url=settings.litellm_url,
            api_key=settings.litellm_api_key,
        )
        self.model = settings.embedding_model

    async def init(self):
        self.pool = await asyncpg.create_pool(
            host=settings.postgres_host,
            port=settings.postgres_port,
            database=settings.postgres_db,
            user=settings.postgres_user,
            password=settings.postgres_password,
            min_size=2,
            max_size=10,
        )
        async with self.pool.acquire() as conn:
            await conn.execute(INIT_SQL)
        logger.info("Embeddings store initialized")

    async def close(self):
        if self.pool:
            await self.pool.close()

    async def generate_embedding(self, text: str) -> list[float]:
        """Generate embedding via LiteLLM proxy."""
        try:
            resp = await self.openai.embeddings.create(
                model=self.model,
                input=text[:8000],
            )
            return resp.data[0].embedding
        except Exception as e:
            logger.error(f"Embedding generation failed: {e}")
            return []

    async def store_document_embedding(self, doc_id: int, content: str, chunk_index: int = 0):
        """Store document content and its embedding."""
        embedding = await self.generate_embedding(content)
        if not embedding:
            return
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO document_embeddings (document_id, chunk_index, content, embedding)
                VALUES ($1, $2, $3, $4::vector)
                ON CONFLICT (document_id, chunk_index) DO UPDATE
                SET content = $3, embedding = $4::vector, created_at = NOW()
                """,
                doc_id, chunk_index, content[:10000], str(embedding),
            )

    async def delete_document_embeddings(self, doc_id: int):
        async with self.pool.acquire() as conn:
            await conn.execute("DELETE FROM document_embeddings WHERE document_id = $1", doc_id)

    async def vector_search(self, query: str, limit: int = 10) -> list[dict]:
        """Search for similar documents using vector similarity."""
        embedding = await self.generate_embedding(query)
        if not embedding:
            return []
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT document_id, chunk_index, content,
                       1 - (embedding <=> $1::vector) as similarity
                FROM document_embeddings
                ORDER BY embedding <=> $1::vector
                LIMIT $2
                """,
                str(embedding), limit,
            )
            return [dict(r) for r in rows]

    async def get_last_sync(self):
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow("SELECT last_sync_at FROM sync_state WHERE id = 1")
            return row["last_sync_at"] if row else None

    async def set_last_sync(self, ts):
        async with self.pool.acquire() as conn:
            await conn.execute(
                "UPDATE sync_state SET last_sync_at = $1, updated_at = NOW() WHERE id = 1", ts
            )

    async def get_doc_hash(self, doc_id: int) -> Optional[str]:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT content_hash FROM document_hashes WHERE document_id = $1", doc_id
            )
            return row["content_hash"] if row else None

    async def set_doc_hash(self, doc_id: int, content_hash: str):
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO document_hashes (document_id, content_hash, processed_at)
                VALUES ($1, $2, NOW())
                ON CONFLICT (document_id) DO UPDATE
                SET content_hash = $2, processed_at = NOW()
                """,
                doc_id, content_hash,
            )

    async def delete_doc_hash(self, doc_id: int):
        async with self.pool.acquire() as conn:
            await conn.execute("DELETE FROM document_hashes WHERE document_id = $1", doc_id)

    async def clear_all(self):
        async with self.pool.acquire() as conn:
            await conn.execute("DELETE FROM document_embeddings")
            await conn.execute("DELETE FROM document_hashes")
            await conn.execute("UPDATE sync_state SET last_sync_at = NULL, updated_at = NOW() WHERE id = 1")

    async def get_embedding_count(self) -> int:
        async with self.pool.acquire() as conn:
            return await conn.fetchval("SELECT COUNT(*) FROM document_embeddings")


embeddings_store = EmbeddingsStore()
