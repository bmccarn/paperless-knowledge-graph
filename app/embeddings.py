import logging
from typing import Optional

import asyncpg
from openai import AsyncOpenAI

from app.config import settings

logger = logging.getLogger(__name__)

EMBEDDING_DIMENSIONS = 3072  # text-embedding-3-large

INIT_SQL = f"""
CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE IF NOT EXISTS document_embeddings (
    id SERIAL PRIMARY KEY,
    document_id INTEGER NOT NULL,
    chunk_index INTEGER NOT NULL DEFAULT 0,
    content TEXT NOT NULL,
    embedding vector({EMBEDDING_DIMENSIONS}),
    created_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(document_id, chunk_index)
);

CREATE TABLE IF NOT EXISTS sync_state (
    id INTEGER PRIMARY KEY DEFAULT 1,
    last_sync_at TIMESTAMPTZ,
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

TRGM_SQL = """
CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE INDEX IF NOT EXISTS idx_content_trgm ON document_embeddings USING GIN (content gin_trgm_ops);
"""


class EmbeddingsStore:
    def __init__(self):
        self.pool: Optional[asyncpg.Pool] = None
        self.openai = AsyncOpenAI(
            base_url=settings.litellm_url,
            api_key=settings.litellm_api_key,
        )
        self.model = settings.embedding_model
        self._trgm_initialized = False

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
        # Check if dimension migration is needed
        await self._migrate_dimensions()

        async with self.pool.acquire() as conn:
            await conn.execute(INIT_SQL)
            # Try to create pg_trgm extension and GIN index
            try:
                await conn.execute(TRGM_SQL)
                self._trgm_initialized = True
                logger.info("pg_trgm extension and GIN index initialized")
            except Exception as e:
                logger.warning(f"pg_trgm init failed (keyword search will be unavailable): {e}")
        logger.info("Embeddings store initialized")

    async def _migrate_dimensions(self):
        """Check embedding column dimension and recreate table if it doesn't match."""
        async with self.pool.acquire() as conn:
            # Check if table exists
            exists = await conn.fetchval(
                "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'document_embeddings')"
            )
            if not exists:
                return  # Table will be created by INIT_SQL

            # Check current dimension by inspecting column type
            try:
                col_type = await conn.fetchval("""
                    SELECT format_type(atttypid, atttypmod)
                    FROM pg_attribute
                    WHERE attrelid = 'document_embeddings'::regclass
                    AND attname = 'embedding'
                """)
                if col_type and f"vector({EMBEDDING_DIMENSIONS})" not in col_type:
                    logger.warning(
                        f"Embedding dimension mismatch: current={col_type}, expected=vector({EMBEDDING_DIMENSIONS}). "
                        f"Dropping and recreating embeddings table."
                    )
                    await conn.execute("DROP TABLE IF EXISTS document_embeddings CASCADE")
                    await conn.execute("DELETE FROM document_hashes")
                    await conn.execute("UPDATE sync_state SET last_sync_at = NULL, updated_at = NOW() WHERE id = 1")
                    logger.info("Embeddings table dropped for dimension migration. A full reindex will be needed.")
            except Exception as e:
                logger.warning(f"Dimension check failed (will proceed): {e}")

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

    async def generate_rich_embedding(self, name: str, entity_type: str = "",
                                       description: str = "", connected_names: list[str] = None) -> list[float]:
        """Generate a richer embedding for entities: name + type + description + connections."""
        parts = [name]
        if entity_type:
            parts.append(f"type: {entity_type}")
        if description:
            parts.append(description)
        if connected_names:
            parts.append("connected to: " + ", ".join(connected_names[:10]))
        text = " | ".join(parts)
        return await self.generate_embedding(text)

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

    async def keyword_search(self, query: str, limit: int = 10) -> list[dict]:
        """Full-text keyword search using pg_trgm similarity."""
        if not self._trgm_initialized:
            return []
        try:
            async with self.pool.acquire() as conn:
                rows = await conn.fetch(
                    """
                    SELECT document_id, chunk_index, content,
                           similarity(content, $1) AS rank_score
                    FROM document_embeddings
                    WHERE content % $1 OR content ILIKE '%' || $1 || '%'
                    ORDER BY similarity(content, $1) DESC
                    LIMIT $2
                    """,
                    query, limit,
                )
                return [dict(r) for r in rows]
        except Exception as e:
            logger.warning(f"Keyword search failed: {e}")
            return []

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
