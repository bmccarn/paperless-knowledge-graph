import logging
import json
import re
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

ALTER TABLE document_embeddings ADD COLUMN IF NOT EXISTS source_kind TEXT NOT NULL DEFAULT 'legacy';
ALTER TABLE document_embeddings ADD COLUMN IF NOT EXISTS source_content TEXT;

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

ALTER TABLE document_hashes ADD COLUMN IF NOT EXISTS ingestion_fingerprint VARCHAR(64);

CREATE TABLE IF NOT EXISTS document_feedback (
    id SERIAL PRIMARY KEY,
    document_id INTEGER NOT NULL,
    reason TEXT NOT NULL,
    note TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

ALTER TABLE document_feedback ADD COLUMN IF NOT EXISTS status TEXT NOT NULL DEFAULT 'open';
ALTER TABLE document_feedback ADD COLUMN IF NOT EXISTS resolution TEXT;
ALTER TABLE document_feedback ADD COLUMN IF NOT EXISTS resolution_note TEXT;
ALTER TABLE document_feedback ADD COLUMN IF NOT EXISTS resolved_at TIMESTAMPTZ;
ALTER TABLE document_feedback ADD COLUMN IF NOT EXISTS resolved_content_hash TEXT;
CREATE INDEX IF NOT EXISTS idx_document_feedback_open
ON document_feedback(document_id) WHERE status = 'open';

CREATE TABLE IF NOT EXISTS entity_review_decisions (
    id SERIAL PRIMARY KEY,
    left_uuid TEXT NOT NULL,
    right_uuid TEXT NOT NULL,
    decision TEXT NOT NULL,
    note TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(left_uuid, right_uuid, decision)
);

ALTER TABLE entity_review_decisions ADD COLUMN IF NOT EXISTS left_identity JSONB;
ALTER TABLE entity_review_decisions ADD COLUMN IF NOT EXISTS right_identity JSONB;
ALTER TABLE entity_review_decisions ADD COLUMN IF NOT EXISTS provenance TEXT NOT NULL DEFAULT 'legacy_unknown';
ALTER TABLE entity_review_decisions ADD COLUMN IF NOT EXISTS identity_status TEXT NOT NULL DEFAULT 'unassessed';
ALTER TABLE entity_review_decisions ADD COLUMN IF NOT EXISTS review_id TEXT;
ALTER TABLE entity_review_decisions ADD COLUMN IF NOT EXISTS review_method TEXT NOT NULL DEFAULT 'legacy_unknown';

INSERT INTO sync_state (id, last_sync_at) VALUES (1, NULL)
ON CONFLICT (id) DO NOTHING;

CREATE INDEX IF NOT EXISTS idx_doc_embeddings_doc_id ON document_embeddings(document_id);

CREATE INDEX IF NOT EXISTS idx_content_trgm
ON document_embeddings USING GIN (content gin_trgm_ops);

CREATE INDEX IF NOT EXISTS idx_entity_content_trgm
ON entity_embeddings USING GIN (content gin_trgm_ops);
"""


def _find_table_regions(content: str) -> list:
    """Find all markdown table regions in content.
    Returns list of (start_pos, end_pos, header_text) tuples.
    A table region spans from the header row through all contiguous data rows.
    The header_text includes the header row + separator row for prepending."""
    lines = content.split('\n')
    regions = []
    i = 0
    while i < len(lines):
        line = lines[i].strip()
        # Look for separator rows (| :--- | or | --- |) to identify tables
        if re.match(r'^\|[\s:]*-{2,}', line):
            if i > 0 and lines[i - 1].strip().startswith('|'):
                header = lines[i - 1].rstrip() + '\n' + lines[i].rstrip() + '\n'
                # Table starts at the header row
                table_start = sum(len(lines[j]) + 1 for j in range(i - 1))
                # Scan forward to find end of table
                j = i + 1
                while j < len(lines):
                    jline = lines[j].strip()
                    if jline.startswith('|'):
                        j += 1
                        continue
                    if not jline:
                        # Empty line — check if table continues after it
                        if j + 1 < len(lines) and lines[j + 1].strip().startswith('|'):
                            j += 1
                            continue
                        break
                    break  # Non-table, non-empty line
                table_end = sum(len(lines[k]) + 1 for k in range(j))
                regions.append((table_start, table_end, header))
                i = j
                continue
        i += 1
    return regions


def chunk_text(content: str, chunk_size: int = 4000, overlap: int = 800, *, include_table_headers: bool = True) -> list[str]:
    """Split text into chunks using paragraph/sentence boundaries with overlap.
    Table-aware: when a chunk starts inside a markdown table, the table's header
    row and separator row are prepended so the chunk is self-contained and the
    LLM can interpret column values correctly. Source certification disables
    inserted headers to retain the original contiguous intervals and indices."""
    if not content or not content.strip():
        return []
    if len(content) <= chunk_size:
        return [content]

    # Pre-compute table regions for header prepending
    table_regions = _find_table_regions(content)

    # Phase 1: Normal boundary-aware chunking
    raw_chunks = []
    chunk_starts = []
    start = 0
    while start < len(content):
        end = start + chunk_size
        if end >= len(content):
            chunk = content[start:]
            if chunk.strip():
                raw_chunks.append(chunk)
                chunk_starts.append(start)
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
            raw_chunks.append(chunk)
            chunk_starts.append(start)

        # Move start with overlap
        start = end - overlap
        if start <= (end - chunk_size):
            start = end  # Prevent infinite loop

    if not raw_chunks:
        return [content[:chunk_size]]
    if not include_table_headers:
        return raw_chunks

    # Phase 2: Prepend table headers to continuation chunks that start within a table
    result = [raw_chunks[0]]
    for i in range(1, len(raw_chunks)):
        chunk = raw_chunks[i]
        pos = chunk_starts[i]

        for tstart, tend, header in table_regions:
            if tstart < pos < tend:
                # This chunk starts inside this table — prepend header if not already present
                header_first_line = header.split('\n')[0].strip()
                if header_first_line not in chunk:
                    chunk = header + chunk
                break

        result.append(chunk)

    return result


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
        logger.info("Embeddings store initialized (exact search default, entity table, pg_trgm)")

    async def _migrate_dimensions(self):
        """Refuse incompatible schemas; startup must never erase existing data."""
        async with self.pool.acquire() as conn:
            for table in ("document_embeddings", "entity_embeddings"):
                col_type = await conn.fetchval("""
                    SELECT format_type(atttypid, atttypmod) FROM pg_attribute
                    WHERE attrelid = to_regclass($1) AND attname = 'embedding'
                    """, table)
                if col_type and col_type != f"vector({EMBEDDING_DIMENSIONS})":
                    raise RuntimeError(f"{table}.embedding is {col_type}, expected vector({EMBEDDING_DIMENSIONS}); "
                                       "prepare an explicit backed-up migration before starting this version")

    async def create_vector_indexes(self):
        """3072-dimension halfvec candidate indexes; normal retrieval remains exact.

        Approximate document search is an explicit method option and reranks
        candidates using the original full-precision vectors. See vector spec.
        """
        async with self.pool.acquire() as conn:
            for table, index in (("document_embeddings", "idx_embeddings_halfvec_hnsw"),
                                 ("entity_embeddings", "idx_entity_halfvec_hnsw")):
                await conn.execute(f"""
                    CREATE INDEX IF NOT EXISTS {index} ON {table}
                    USING hnsw ((embedding::halfvec({EMBEDDING_DIMENSIONS})) halfvec_cosine_ops)
                    WITH (m = 16, ef_construction = 64)
                    """)
        return {"dimensions": EMBEDDING_DIMENSIONS, "default_search": "exact",
                "optional_candidate_index": "halfvec_hnsw"}

    async def close(self):
        if self.pool:
            await self.pool.close()
            self.pool = None
        await self.openai.close()

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
            raise ValueError("Entity embedding generation returned no vector")
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
                                        title: str = None, doc_type: str = None,
                                        embedding: list[float] | None = None, *,
                                        source_kind: str = "ocr", source_content: str | None = None):
        """Store document content and its embedding."""
        if source_kind not in {"ocr", "generated", "metadata"}:
            raise ValueError("Unknown document source origin")
        if source_kind == "ocr" and source_content is None:
            source_content = content
        if embedding is None:
            embedding = await self.generate_embedding(content)
        if not embedding:
            raise ValueError("Document embedding generation returned no vector")
        async def _op():
            async with self.pool.acquire() as conn:
                await conn.execute(
                    """
                    INSERT INTO document_embeddings (document_id, chunk_index, content, title, doc_type, embedding, source_kind, source_content)
                    VALUES ($1, $2, $3, $4, $5, $6::vector, $7, $8)
                    ON CONFLICT (document_id, chunk_index) DO UPDATE
                    SET content = $3, title = $4, doc_type = $5, embedding = $6::vector,
                        source_kind = $7, source_content = $8, created_at = NOW()
                    """,
                    doc_id, chunk_index, content[:50000], title, doc_type, str(embedding), source_kind, source_content,
                )
        await retry_db(_op, operation='store_document_embedding')


    async def get_chunks_for_document(self, doc_id: int, limit: int = 3) -> list[dict]:
        """Retrieve stored chunks for a specific document by ID."""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT document_id, chunk_index, content, title, doc_type, source_kind, source_content
                FROM document_embeddings
                WHERE document_id = $1
                ORDER BY chunk_index ASC
                LIMIT $2
                """,
                doc_id, limit,
            )
            return [
                {
                    'document_id': r['document_id'],
                    'chunk_index': r['chunk_index'],
                    'content': r['content'],
                    'title': r['title'],
                    'doc_type': r['doc_type'],
                    'source_kind': r['source_kind'],
                    'source_content': r['source_content'],
                    'similarity': 0.5,  # neutral score for graph-driven results
                }
                for r in rows
            ]

    async def historical_document_candidates(self, terms: list[str], limit: int = 500) -> dict:
        """Bounded per-document index discovery independent of vector top-k.

        Only certifying, completed, feedback-free records participate. Preview
        text is a retrieval signal; the later chunk fetch supplies exact OCR.
        No embeddings or models are invoked by this metadata query.
        """
        if not terms:
            return {"documents": [], "candidate_count": 0, "truncated": False}
        pattern = r"\m(?:" + "|".join(re.escape(t) for t in terms[:24]) + r")\M"
        async with self.pool.acquire() as conn:
            rows = await conn.fetch("""
                WITH matching AS (
                    SELECT e.document_id, e.chunk_index, e.title, e.doc_type,
                           left(coalesce(e.source_content, e.content), 4000) AS preview,
                           row_number() OVER (PARTITION BY e.document_id ORDER BY e.chunk_index) AS rn
                    FROM document_embeddings e
                    JOIN document_hashes h ON h.document_id = e.document_id
                    WHERE e.source_kind IN ('ocr', 'legacy') AND e.chunk_index <> 9999
                      AND coalesce(e.doc_type, '') <> 'no_content'
                      AND NOT EXISTS (SELECT 1 FROM document_feedback f
                                      WHERE f.document_id = e.document_id AND f.status = 'open')
                      AND (coalesce(e.title, '') || ' ' || coalesce(e.doc_type, '') || ' ' ||
                           left(coalesce(e.source_content, e.content), 4000)) ~* $1
                )
                SELECT document_id, title, doc_type, preview, count(*) OVER () AS candidate_count
                FROM matching WHERE rn = 1 ORDER BY document_id LIMIT $2
                """, pattern, limit)
        count = int(rows[0]["candidate_count"]) if rows else 0
        return {"documents": [dict(row) for row in rows], "candidate_count": count, "truncated": count > limit}

    async def get_chunks_for_documents(self, doc_ids: list[int], chunks_per_doc: int = 2, *, relevance_terms: list[str] | None = None, include_opening: bool = False) -> list[dict]:
        """Retrieve stored chunks for multiple documents in a single query.
        Returns up to `chunks_per_doc` chunks per document. Optional query terms
        prioritize matching OCR chunks before chunk order, without model calls.
        include_opening retains chunk zero before relevant later chunks.
        """
        if not doc_ids:
            return []
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT document_id, chunk_index, content, title, doc_type, source_kind, source_content
                FROM (
                    SELECT document_id, chunk_index, content, title, doc_type, source_kind, source_content,
                           ROW_NUMBER() OVER (PARTITION BY document_id ORDER BY
                               CASE WHEN $4::boolean AND chunk_index = 0 THEN 0 ELSE 1 END,
                               CASE WHEN $3::text IS NOT NULL AND coalesce(source_content, content) ~* $3 THEN 0 ELSE 1 END,
                               chunk_index) AS rn
                    FROM document_embeddings
                    WHERE document_id = ANY($1::int[])
                ) sub
                WHERE rn <= $2
                ORDER BY document_id, chunk_index
                """,
                doc_ids, chunks_per_doc,
                r"\m(?:" + "|".join(re.escape(t) for t in relevance_terms[:24]) + r")\M" if relevance_terms else None, include_opening,
            )
            return [
                {
                    'document_id': r['document_id'],
                    'chunk_index': r['chunk_index'],
                    'content': r['content'],
                    'title': r['title'],
                    'doc_type': r['doc_type'],
                    'source_kind': r['source_kind'],
                    'source_content': r['source_content'],
                    'similarity': 0.4,  # lower base score for graph-driven results
                }
                for r in rows
            ]

    async def delete_document_embeddings(self, doc_id: int):
        async with self.pool.acquire() as conn:
            await conn.execute("DELETE FROM document_embeddings WHERE document_id = $1", doc_id)

    async def vector_search(self, query: str, limit: int = 10, *, approximate: bool = False) -> list[dict]:
        """Exact by default; optionally rerank approximate halfvec candidates."""
        embedding = await self.generate_embedding(query)
        if not embedding:
            return []
        async with self.pool.acquire() as conn:
            if approximate:
                async with conn.transaction():
                    await conn.execute("SET LOCAL hnsw.ef_search = 200")
                    rows = await conn.fetch(f"""
                        WITH candidates AS MATERIALIZED (
                            SELECT * FROM document_embeddings
                            ORDER BY embedding::halfvec({EMBEDDING_DIMENSIONS}) <=> $1::halfvec({EMBEDDING_DIMENSIONS})
                            LIMIT $3
                        )
                        SELECT document_id, chunk_index, content, title, doc_type, source_kind, source_content,
                               1 - (embedding <=> $1::vector) AS similarity
                        FROM candidates ORDER BY embedding <=> $1::vector, document_id, chunk_index LIMIT $2
                        """, str(embedding), limit, max(100, limit * 5))
            else:
                rows = await conn.fetch("""
                    SELECT document_id, chunk_index, content, title, doc_type, source_kind, source_content,
                           1 - (embedding <=> $1::vector) AS similarity
                    FROM document_embeddings
                    ORDER BY (embedding <=> $1::vector) + 0, document_id, chunk_index LIMIT $2
                    """, str(embedding), limit)
            return [dict(r) for r in rows]

    async def filtered_vector_search(self, query: str, doc_type: str = None, limit: int = 10) -> list[dict]:
        """Search with optional doc_type filter."""
        embedding = await self.generate_embedding(query)
        if not embedding:
            return []
        async with self.pool.acquire() as conn:
            if doc_type:
                rows = await conn.fetch(
                    """
                    SELECT document_id, chunk_index, content, title, doc_type, source_kind, source_content,
                           1 - (embedding <=> $1::vector) as similarity
                    FROM document_embeddings
                    WHERE doc_type = $3
                    ORDER BY (embedding <=> $1::vector) + 0
                    LIMIT $2
                    """,
                    str(embedding), limit, doc_type,
                )
            else:
                rows = await conn.fetch(
                    """
                    SELECT document_id, chunk_index, content, title, doc_type, source_kind, source_content,
                           1 - (embedding <=> $1::vector) as similarity
                    FROM document_embeddings
                    ORDER BY (embedding <=> $1::vector) + 0
                    LIMIT $2
                    """,
                    str(embedding), limit,
                )
            return [dict(r) for r in rows]

    async def vector_search_by_doc_ids(self, query: str, doc_ids: list[int], limit: int = 40) -> list[dict]:
        """Vector similarity search scoped to a specific set of document IDs.
        Used by graph-driven retrieval: the graph identifies candidate docs,
        then this method finds which of those docs are most semantically relevant
        to the query. Returns chunks sorted by similarity score."""
        if not doc_ids:
            return []
        embedding = await self.generate_embedding(query)
        if not embedding:
            return []
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT document_id, chunk_index, content, title, doc_type, source_kind, source_content,
                       1 - (embedding <=> $1::vector) as similarity
                FROM document_embeddings
                WHERE document_id = ANY($3::int[])
                ORDER BY (embedding <=> $1::vector) + 0
                LIMIT $2
                """,
                str(embedding), limit, doc_ids,
            )
            return [dict(r) for r in rows]

    async def entity_vector_search(self, query: str, limit: int = 10) -> list[dict]:
        """Search entity embeddings by vector similarity."""
        embedding = await self.generate_embedding(query)
        if not embedding:
            return []
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT entity_uuid, entity_name, entity_type, content,
                       1 - (embedding <=> $1::vector) as similarity
                FROM entity_embeddings
                ORDER BY (embedding <=> $1::vector) + 0
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
                    SELECT document_id, chunk_index, content, title, doc_type, source_kind, source_content,
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
                    SELECT document_id, chunk_index, content, title, doc_type, source_kind, source_content,
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

    async def get_ingestion_fingerprints(self, doc_ids: list[int] | None = None) -> dict[int, str | None]:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT document_id, ingestion_fingerprint FROM document_hashes "
                "WHERE $1::int[] IS NULL OR document_id = ANY($1::int[])", doc_ids)
            return {row["document_id"]: row["ingestion_fingerprint"] for row in rows}

    async def set_doc_hash(self, doc_id: int, content_hash: str, *, ingestion_fingerprint: str | None = None):
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO document_hashes (document_id, content_hash, ingestion_fingerprint, processed_at)
                VALUES ($1, $2, $3, NOW())
                ON CONFLICT (document_id) DO UPDATE
                SET content_hash = $2, ingestion_fingerprint = $3, processed_at = NOW()
                """,
                doc_id, content_hash, ingestion_fingerprint,
            )

    async def delete_doc_hash(self, doc_id: int):
        async with self.pool.acquire() as conn:
            await conn.execute("DELETE FROM document_hashes WHERE document_id = $1", doc_id)

    async def get_document_chunks(self, doc_id: int) -> list[dict]:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT chunk_index, title, doc_type, content, created_at
                FROM document_embeddings
                WHERE document_id = $1
                ORDER BY chunk_index
                """,
                doc_id,
            )
            return [dict(r) for r in rows]

    async def get_document_processing_status(self, doc_id: int) -> dict:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT content_hash, processed_at FROM document_hashes WHERE document_id = $1",
                doc_id,
            )
            chunk_count = await conn.fetchval(
                "SELECT COUNT(*) FROM document_embeddings WHERE document_id = $1",
                doc_id,
            )
            feedback_count = await conn.fetchval(
                "SELECT COUNT(*) FROM document_feedback WHERE document_id = $1",
                doc_id,
            )
            open_feedback_count = await conn.fetchval(
                "SELECT COUNT(*) FROM document_feedback WHERE document_id = $1 AND status = 'open'",
                doc_id,
            )
            return {
                "processed": bool(row),
                "content_hash": row["content_hash"] if row else None,
                "processed_at": row["processed_at"].isoformat() if row else None,
                "chunk_count": chunk_count,
                "feedback_count": feedback_count,
                "open_feedback_count": open_feedback_count,
            }

    async def add_document_feedback(self, doc_id: int, reason: str, note: str = "") -> dict:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                INSERT INTO document_feedback (document_id, reason, note)
                VALUES ($1, $2, $3)
                RETURNING *
                """,
                doc_id, reason, note,
            )
            return dict(row)

    async def get_document_feedback(self, doc_id: int) -> list[dict]:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT * FROM document_feedback WHERE document_id = $1 ORDER BY created_at DESC, id DESC",
                doc_id,
            )
            return [dict(row) for row in rows]

    async def get_open_feedback_document_ids(self, doc_ids: list[int]) -> set[int]:
        if not doc_ids:
            return set()
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT DISTINCT document_id FROM document_feedback WHERE status = 'open' AND document_id = ANY($1::int[])",
                list(set(doc_ids)),
            )
            return {row["document_id"] for row in rows}

    async def resolve_document_feedback(self, doc_id: int, feedback_id: int, resolution: str,
                                        note: str, content_hash: str | None = None) -> dict | None:
        if resolution not in {"reindexed_and_reviewed", "dismissed_after_review"} or not note.strip():
            raise ValueError("A resolution type and review note are required")
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                """UPDATE document_feedback
                   SET status = 'resolved', resolution = $3, resolution_note = $4,
                       resolved_at = NOW(), resolved_content_hash = $5
                   WHERE document_id = $1 AND id = $2 AND status = 'open'
                     AND ($3 != 'reindexed_and_reviewed' OR EXISTS (
                         SELECT 1 FROM document_hashes h
                         WHERE h.document_id = $1 AND h.content_hash = $5
                           AND h.processed_at > document_feedback.created_at
                     ))
                   RETURNING *""",
                doc_id, feedback_id, resolution, note.strip(), content_hash,
            )
            return dict(row) if row else None

    async def add_entity_review_decision(self, left_uuid: str, right_uuid: str, decision: str, note: str = "",
                                         *, left_identity: dict | None = None,
                                         right_identity: dict | None = None,
                                         provenance: str = "legacy_unknown",
                                         identity_status: str = "unassessed", review_id: str | None = None,
                                         review_method: str = "legacy_unknown") -> dict:
        from app.entity_policy import EXPLICIT_REVIEW_METHOD
        if provenance == "human_review" and (review_method != EXPLICIT_REVIEW_METHOD or not review_id):
            raise ValueError("Human decision provenance requires explicit review origin and correlation ID")
        ordered = sorted([left_uuid, right_uuid])
        if ordered[0] != left_uuid:
            left_identity, right_identity = right_identity, left_identity
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                INSERT INTO entity_review_decisions (left_uuid, right_uuid, decision, note, left_identity, right_identity, provenance, identity_status, review_id, review_method)
                VALUES ($1, $2, $3, $4, $5::jsonb, $6::jsonb, $7, $8, $9, $10)
                ON CONFLICT (left_uuid, right_uuid, decision) DO UPDATE
                SET note = EXCLUDED.note, created_at = NOW(),
                    left_identity = COALESCE(EXCLUDED.left_identity, entity_review_decisions.left_identity),
                    right_identity = COALESCE(EXCLUDED.right_identity, entity_review_decisions.right_identity),
                    provenance = EXCLUDED.provenance, identity_status = EXCLUDED.identity_status,
                    review_id = COALESCE(EXCLUDED.review_id, entity_review_decisions.review_id),
                    review_method = EXCLUDED.review_method
                RETURNING *
                """,
                ordered[0], ordered[1], decision, note,
                json.dumps(left_identity) if left_identity else None,
                json.dumps(right_identity) if right_identity else None, provenance, identity_status, review_id, review_method,
            )
            return self._decode_review_decision(row)

    async def hydrate_entity_review_identities(self, row: dict, status: str):
        """Update exact historic row in place; do not reorder UUIDs or rewrite history."""
        if status not in {"active", "unresolved_legacy"}:
            raise ValueError("Unsupported identity assessment status")
        async with self.pool.acquire() as conn:
            result = await conn.execute("""UPDATE entity_review_decisions
                SET left_identity=COALESCE(left_identity,$4::jsonb),
                    right_identity=COALESCE(right_identity,$5::jsonb), identity_status=$6
                WHERE left_uuid=$1 AND right_uuid=$2 AND decision=$3""",
                row["left_uuid"], row["right_uuid"], row["decision"],
                json.dumps(row.get("left_identity")) if row.get("left_identity") else None,
                json.dumps(row.get("right_identity")) if row.get("right_identity") else None, status)
            if result == "UPDATE 0":
                raise ValueError("Legacy decision disappeared during identity assessment")

    async def set_entity_decision_identity_status(self, left_uuid: str, right_uuid: str,
                                                  decision: str, status: str):
        await self.hydrate_entity_review_identities(
            {"left_uuid": left_uuid, "right_uuid": right_uuid, "decision": decision}, status)

    @staticmethod
    def _decode_review_decision(row) -> dict:
        result = dict(row)
        for key in ("left_identity", "right_identity"):
            if isinstance(result.get(key), str):
                result[key] = json.loads(result[key])
        return result

    async def get_entity_review_decisions(self) -> list[dict]:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch("SELECT * FROM entity_review_decisions")
            return [self._decode_review_decision(r) for r in rows]

    async def preserve_alias_revocations(self, rows: list[dict]):
        """Append idempotent revocations without rewriting historical decisions.

        Ordinary positive review, reprocessing and policy changes cannot revoke
        these records. Any future reauthorization API must explicitly target the
        revocation and preserve its history; no implicit last-write-wins exists.
        """
        if not rows:
            return
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                for row in rows:
                    if row.get("decision") != "alias_revoked":
                        raise ValueError("Expected alias revocation")
                    await conn.execute("""INSERT INTO entity_review_decisions
                        (left_uuid,right_uuid,decision,note,left_identity,right_identity,
                         provenance,identity_status,review_id,review_method)
                        VALUES($1,$2,'alias_revoked',$3,$4::jsonb,$5::jsonb,'alias_quarantine','active',$6,'alias_quarantine')
                        ON CONFLICT (left_uuid,right_uuid,decision) DO NOTHING""",
                        row["left_uuid"], row["right_uuid"], row["note"], json.dumps(row["left_identity"]),
                        json.dumps(row["right_identity"]), row["review_id"])

    async def get_incomplete_document_ids(self, doc_ids: list[int]) -> set[int]:
        """Missing completion markers mean a replacement is pending or failed."""
        if not doc_ids:
            return set()
        async with self.pool.acquire() as conn:
            completed = await conn.fetch("SELECT document_id FROM document_hashes WHERE document_id = ANY($1::int[])", doc_ids)
        return set(doc_ids) - {r["document_id"] for r in completed}

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

    async def get_document_embedding_ids(self) -> set[int]:
        """Return all document IDs with at least one embedding chunk."""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch("SELECT DISTINCT document_id FROM document_embeddings")
            return {int(r["document_id"]) for r in rows}

    async def get_document_hash_ids(self) -> set[int]:
        """Return all document IDs with processing hashes."""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch("SELECT document_id FROM document_hashes")
            return {int(r["document_id"]) for r in rows}


embeddings_store = EmbeddingsStore()
