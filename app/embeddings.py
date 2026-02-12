import logging
from typing import Optional

import asyncpg
from openai import AsyncOpenAI

from app.config import settings
from app.retry import retry_db, retry_with_backoff

logger = logging.getLogger(__name__)

EMBEDDING_DIMENSIONS = 3072  # text-embedding-3-large

INIT_SQL = f"""
CREATE EXTENSION IF NOT EXISTS vector;
CREATE EXTENSION IF NOT EXISTS pg_trgm;

CREATE TABLE IF NOT EXISTS document_embeddings (
    id SERIAL PRIMARY KEY,
    document_id INTEGER NOT NULL,
    chunk_index INTEGER NOT NULL DEFAULT 0,
    content TEXT NOT NULL,
    title TEXT,
    doc_type TEXT,
    embedding vector({EMBEDDING_DIMENSIONS}),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(document_id, chunk_index)
);

CREATE TABLE IF NOT EXISTS entity_embeddings (
    id SERIAL PRIMARY KEY,
    entity_uuid TEXT NOT NULL UNIQUE,
    entity_name TEXT NOT NULL,
    entity_type TEXT,
    content TEXT NOT NULL,
    embedding vector({EMBEDDING_DIMENSIONS}),
    created_at TIMESTAMPTZ DEFAULT NOW()
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

CREATE INDEX IF NOT EXISTS idx_content_trgm
ON document_embeddings USING GIN (content gin_trgm_ops);

CREATE INDEX IF NOT EXISTS idx_entity_content_trgm
ON entity_embeddings USING GIN (content gin_trgm_ops);
"""


def chunk_text(content: str, chunk_size: int = 4000, overlap: int = 800) -> list[str]:
    """Split text into chunks using paragraph/sentence boundaries with overlap."""
    if not content or not content.strip():
        return []
    if len(content) <= chunk_size:
        return [content]

    chunks = []
    start = 0
    while start < len(content):
        end = start + chunk_size
        if end >= len(content):
            chunk = content[start:]
            if chunk.strip():
                chunks.append(chunk)
            break

        # Try to find a good break point
        segment = content[start:end]
        # Try paragraph boundary
        break_pos = segment.rfind('\n\n')
        if break_pos > chunk_size // 2:
            end = start + break_pos + 2
        else:
            # Try line boundary
            break_pos = segment.rfind('\n')
            if break_pos > chunk_size // 2:
                end = start + break_pos + 1
            else:
                # Try sentence boundary
                break_pos = segment.rfind('. ')
                if break_pos > chunk_size // 2:
                    end = start + break_pos + 2

        chunk = content[start:end]
        if chunk.strip():
            chunks.append(chunk)

        # Move start with overlap
        start = end - overlap
        if start <= (end - chunk_size):
            start = end  # Prevent infinite loop

    return chunks if chunks else [content[:chunk_size]]


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
        await self._migrate_dimensions()

        async with self.pool.acquire() as conn:
            await conn.execute(INIT_SQL)
        logger.info("Embeddings store initialized (with HNSW indexes, entity table, pg_trgm)")

    async def _migrate_dimensions(self):
        """Check embedding column dimension and recreate table if it doesn't match."""
        async with self.pool.acquire() as conn:
            exists = await conn.fetchval(
                "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'document_embeddings')"
            )
            if not exists:
                return
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
                    await conn.execute("DROP TABLE IF EXISTS entity_embeddings CASCADE")
                    await conn.execute("DELETE FROM document_hashes")
                    await conn.execute("UPDATE sync_state SET last_sync_at = NULL, updated_at = NOW() WHERE id = 1")
                    logger.info("Embeddings tables dropped for dimension migration.")
            except Exception as e:
                logger.warning(f"Dimension check failed (will proceed): {e}")


    async def create_vector_indexes(self):
        """Create HNSW vector indexes. Call after data is loaded (reindex)."""
        async with self.pool.acquire() as conn:
            # Drop old indexes if they exist
            await conn.execute("DROP INDEX IF EXISTS idx_embeddings_hnsw")
            await conn.execute("DROP INDEX IF EXISTS idx_entity_embeddings_hnsw")
            
            # Count rows to determine lists parameter
            doc_count = await conn.fetchval("SELECT COUNT(*) FROM document_embeddings")
            entity_count = await conn.fetchval("SELECT COUNT(*) FROM entity_embeddings")
            
            if doc_count > 0:
                try:
                    await conn.execute("""
                        CREATE INDEX idx_embeddings_hnsw
                        ON document_embeddings
                        USING hnsw (embedding vector_cosine_ops)
                        WITH (m = 16, ef_construction = 64)
                    """)
                    logger.info(f"Created HNSW index on document_embeddings ({doc_count} rows)")
                except Exception as e:
                    if "2000 dimensions" in str(e):
                        logger.warning(f"Skipping document embeddings index — pgvector version doesn't support >2000 dims. Upgrade to pgvector 0.9+ for HNSW indexing of 3072-dim vectors. Sequential scan will be used.")
                    else:
                        raise
            
            if entity_count > 0:
                try:
                    await conn.execute("""
                        CREATE INDEX idx_entity_embeddings_hnsw
                        ON entity_embeddings
                        USING hnsw (embedding vector_cosine_ops)
                        WITH (m = 16, ef_construction = 64)
                    """)
                    logger.info(f"Created HNSW index on entity_embeddings ({entity_count} rows)")
                except Exception as e:
                    if "2000 dimensions" in str(e):
                        logger.warning(f"Skipping entity embeddings index — pgvector version doesn't support >2000 dims. Sequential scan will be used.")
                    else:
                        raise

    async def close(self):
        if self.pool:
            await self.pool.close()

    async def generate_embedding(self, text: str) -> list[float]:
        """Generate embedding via LiteLLM proxy."""
        try:
            async def _call():
                resp = await self.openai.embeddings.create(
                    model=self.model,
                    input=text[:24000],
                )
                return resp.data[0].embedding
            return await retry_with_backoff(_call, operation='generate_embedding')
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

    async def store_entity_embedding(self, entity_uuid: str, entity_name: str,
                                      entity_type: str = "", content: str = "",
                                      connected_names: list[str] = None):
        """Store entity embedding in the entity_embeddings table."""
        if not content:
            content = entity_name
        embedding = await self.generate_rich_embedding(
            entity_name, entity_type=entity_type,
            description=content, connected_names=connected_names
        )
        if not embedding:
            return
        async def _op():
            async with self.pool.acquire() as conn:
                await conn.execute(
                    """
                    INSERT INTO entity_embeddings (entity_uuid, entity_name, entity_type, content, embedding)
                    VALUES ($1, $2, $3, $4, $5::vector)
                    ON CONFLICT (entity_uuid) DO UPDATE
                    SET entity_name = $2, entity_type = $3, content = $4,
                        embedding = $5::vector, created_at = NOW()
                    """,
                    entity_uuid, entity_name, entity_type, content[:50000], str(embedding),
                )
        await retry_db(_op, operation='store_entity_embedding')

    async def store_document_embedding(self, doc_id: int, content: str, chunk_index: int = 0,
                                        title: str = None, doc_type: str = None):
        """Store document content and its embedding."""
        embedding = await self.generate_embedding(content)
        if not embedding:
            return
        async def _op():
            async with self.pool.acquire() as conn:
                await conn.execute(
                    """
                    INSERT INTO document_embeddings (document_id, chunk_index, content, title, doc_type, embedding)
                    VALUES ($1, $2, $3, $4, $5, $6::vector)
                    ON CONFLICT (document_id, chunk_index) DO UPDATE
                    SET content = $3, title = $4, doc_type = $5, embedding = $6::vector, created_at = NOW()
                    """,
                    doc_id, chunk_index, content[:50000], title, doc_type, str(embedding),
                )
        await retry_db(_op, operation='store_document_embedding')

    async def delete_document_embeddings(self, doc_id: int):
        async with self.pool.acquire() as conn:
            await conn.execute("DELETE FROM document_embeddings WHERE document_id = $1", doc_id)

    async def vector_search(self, query: str, limit: int = 10) -> list[dict]:
        """Search for similar documents using vector similarity."""
        embedding = await self.generate_embedding(query)
        if not embedding:
            return []
        async with self.pool.acquire() as conn:
            await conn.execute("SET hnsw.ef_search = 40")
            rows = await conn.fetch(
                """
                SELECT document_id, chunk_index, content, title, doc_type,
                       1 - (embedding <=> $1::vector) as similarity
                FROM document_embeddings
                ORDER BY embedding <=> $1::vector
                LIMIT $2
                """,
                str(embedding), limit,
            )
            return [dict(r) for r in rows]

    async def filtered_vector_search(self, query: str, doc_type: str = None, limit: int = 10) -> list[dict]:
        """Search with optional doc_type filter."""
        embedding = await self.generate_embedding(query)
        if not embedding:
            return []
        async with self.pool.acquire() as conn:
            await conn.execute("SET hnsw.ef_search = 40")
            if doc_type:
                rows = await conn.fetch(
                    """
                    SELECT document_id, chunk_index, content, title, doc_type,
                           1 - (embedding <=> $1::vector) as similarity
                    FROM document_embeddings
                    WHERE doc_type = $3
                    ORDER BY embedding <=> $1::vector
                    LIMIT $2
                    """,
                    str(embedding), limit, doc_type,
                )
            else:
                rows = await conn.fetch(
                    """
                    SELECT document_id, chunk_index, content, title, doc_type,
                           1 - (embedding <=> $1::vector) as similarity
                    FROM document_embeddings
                    ORDER BY embedding <=> $1::vector
                    LIMIT $2
                    """,
                    str(embedding), limit,
                )
            return [dict(r) for r in rows]

    async def entity_vector_search(self, query: str, limit: int = 10) -> list[dict]:
        """Search entity embeddings by vector similarity."""
        embedding = await self.generate_embedding(query)
        if not embedding:
            return []
        async with self.pool.acquire() as conn:
            await conn.execute("SET hnsw.ef_search = 40")
            rows = await conn.fetch(
                """
                SELECT entity_uuid, entity_name, entity_type, content,
                       1 - (embedding <=> $1::vector) as similarity
                FROM entity_embeddings
                ORDER BY embedding <=> $1::vector
                LIMIT $2
                """,
                str(embedding), limit,
            )
            return [dict(r) for r in rows]

    async def entity_keyword_search(self, query: str, limit: int = 10) -> list[dict]:
        """Search entity embeddings by keyword (trigram similarity + exact match)."""
        try:
            async with self.pool.acquire() as conn:
                await conn.execute("SET pg_trgm.similarity_threshold = 0.1")
                # Trigram similarity
                trgm_rows = await conn.fetch(
                    """
                    SELECT entity_uuid, entity_name, entity_type, content,
                           similarity(content, $1) AS rank_score
                    FROM entity_embeddings
                    WHERE content % $1
                    ORDER BY rank_score DESC
                    LIMIT $2
                    """,
                    query, limit,
                )
                # Exact substring match
                exact_rows = await conn.fetch(
                    """
                    SELECT entity_uuid, entity_name, entity_type, content,
                           1.0::float AS rank_score
                    FROM entity_embeddings
                    WHERE content ILIKE '%' || $1 || '%'
                    LIMIT $2
                    """,
                    query, limit,
                )
                # Deduplicate
                seen = set()
                results = []
                for r in list(exact_rows) + list(trgm_rows):
                    if r["entity_uuid"] not in seen:
                        seen.add(r["entity_uuid"])
                        results.append(dict(r))
                return results[:limit]
        except Exception as e:
            logger.warning(f"Entity keyword search failed: {e}")
            return []

    async def keyword_search(self, query: str, limit: int = 10) -> list[dict]:
        """Keyword search using trigram similarity + exact substring match."""
        try:
            async with self.pool.acquire() as conn:
                await conn.execute("SET pg_trgm.similarity_threshold = 0.1")
                # Trigram similarity (uses GIN index)
                trgm_rows = await conn.fetch(
                    """
                    SELECT document_id, chunk_index, content, title, doc_type,
                           similarity(content, $1) AS rank_score
                    FROM document_embeddings
                    WHERE content % $1
                    ORDER BY rank_score DESC
                    LIMIT $2
                    """,
                    query, limit,
                )
                # Exact substring match (for IDs, account numbers, etc.)
                exact_rows = await conn.fetch(
                    """
                    SELECT document_id, chunk_index, content, title, doc_type,
                           1.0::float AS rank_score
                    FROM document_embeddings
                    WHERE content ILIKE '%' || $1 || '%'
                    LIMIT $2
                    """,
                    query, limit,
                )
                # Deduplicate by (document_id, chunk_index)
                seen = set()
                results = []
                for r in list(exact_rows) + list(trgm_rows):
                    key = (r["document_id"], r["chunk_index"])
                    if key not in seen:
                        seen.add(key)
                        results.append(dict(r))
                return results[:limit]
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
            await conn.execute("DELETE FROM entity_embeddings")
            await conn.execute("DELETE FROM document_hashes")
            await conn.execute("UPDATE sync_state SET last_sync_at = NULL, updated_at = NOW() WHERE id = 1")

    async def get_embedding_count(self) -> int:
        async with self.pool.acquire() as conn:
            return await conn.fetchval("SELECT COUNT(*) FROM document_embeddings")

    async def get_entity_embedding_count(self) -> int:
        async with self.pool.acquire() as conn:
            return await conn.fetchval("SELECT COUNT(*) FROM entity_embeddings")

    async def get_docs_with_embeddings_count(self) -> int:
        """Count distinct documents that have at least one embedding."""
        async with self.pool.acquire() as conn:
            return await conn.fetchval("SELECT COUNT(DISTINCT document_id) FROM document_embeddings")

    async def get_docs_with_embeddings_count(self) -> int:
        """Count distinct documents that have at least one embedding."""
        async with self.pool.acquire() as conn:
            return await conn.fetchval("SELECT COUNT(DISTINCT document_id) FROM document_embeddings")


embeddings_store = EmbeddingsStore()
