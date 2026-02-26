"""Persistent conversation storage using pgvector Postgres."""

import asyncio
import logging
import uuid
import json
from datetime import datetime, timezone
from typing import Optional

import httpx
from app.config import settings

logger = logging.getLogger(__name__)


async def _generate_title(message: str, _answer: str = "") -> str:
    """Generate a short, descriptive conversation title using the LLM."""
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.post(
                f"{settings.litellm_url}/chat/completions",
                headers={
                    "Authorization": f"Bearer {settings.litellm_api_key or 'unused'}",
                    "Content-Type": "application/json",
                },
                json={
                    "model": settings.gemini_model,
                    "messages": [
                        {
                            "role": "system",
                            "content": (
                                "Generate a very short, descriptive title (3-6 words max) for a chat "
                                "conversation based on the user's message. Return ONLY the title, "
                                "no quotes, no punctuation at the end, no explanation."
                            ),
                        },
                        {
                            "role": "user",
                            "content": message,
                        },
                    ],
                    "max_tokens": 256,
                    "temperature": 0.7,
                },
            )

            if response.status_code == 200:
                data = response.json()
                title = data["choices"][0]["message"]["content"].strip()
                title = title.strip('"\'').strip('.')
                if len(title) > 60:
                    title = title[:57] + "..."
                logger.info(f"Generated title: {title}")
                return title
            else:
                logger.warning(f"LLM title generation failed: {response.status_code} {response.text[:200]}")
    except Exception as e:
        logger.warning(f"Failed to generate conversation title: {e}")

    return message[:40].strip() + ("..." if len(message) > 40 else "")

# We share the same Postgres as embeddings (knowledge_graph DB)
_pool = None


async def init():
    """Initialize connection pool and create tables."""
    global _pool
    import asyncpg
    _pool = await asyncpg.create_pool(settings.postgres_dsn, min_size=1, max_size=5)

    async with _pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS conversations (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                title TEXT NOT NULL DEFAULT 'New conversation',
                created_at TIMESTAMPTZ DEFAULT NOW(),
                updated_at TIMESTAMPTZ DEFAULT NOW()
            );

            CREATE TABLE IF NOT EXISTS conversation_messages (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                conversation_id UUID REFERENCES conversations(id) ON DELETE CASCADE,
                role TEXT NOT NULL,
                content TEXT NOT NULL,
                sources JSONB,
                entities JSONB,
                confidence FLOAT,
                query_time_ms INTEGER,
                cached BOOLEAN DEFAULT FALSE,
                follow_ups JSONB,
                created_at TIMESTAMPTZ DEFAULT NOW()
            );

            CREATE INDEX IF NOT EXISTS idx_messages_conv
                ON conversation_messages(conversation_id, created_at);
        """)
    logger.info("Conversation tables initialized")


async def close():
    global _pool
    if _pool:
        await _pool.close()


async def list_conversations(limit: int = 50, offset: int = 0) -> list[dict]:
    """List conversations with message count, most recent first."""
    async with _pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT c.id, c.title, c.created_at, c.updated_at,
                   COUNT(m.id) as message_count,
                   MAX(m.created_at) as last_message_at
            FROM conversations c
            LEFT JOIN conversation_messages m ON m.conversation_id = c.id
            GROUP BY c.id
            ORDER BY COALESCE(MAX(m.created_at), c.created_at) DESC
            LIMIT $1 OFFSET $2
        """, limit, offset)
        return [
            {
                "id": str(r["id"]),
                "title": r["title"],
                "created_at": r["created_at"].isoformat(),
                "updated_at": r["updated_at"].isoformat(),
                "message_count": r["message_count"],
                "last_message_at": r["last_message_at"].isoformat() if r["last_message_at"] else None,
            }
            for r in rows
        ]


async def create_conversation(title: str = "New conversation") -> dict:
    """Create a new conversation."""
    async with _pool.acquire() as conn:
        row = await conn.fetchrow("""
            INSERT INTO conversations (title) VALUES ($1)
            RETURNING id, title, created_at, updated_at
        """, title)
        return {
            "id": str(row["id"]),
            "title": row["title"],
            "created_at": row["created_at"].isoformat(),
            "updated_at": row["updated_at"].isoformat(),
            "messages": [],
        }


async def get_conversation(conv_id: str) -> Optional[dict]:
    """Get conversation with all messages."""
    async with _pool.acquire() as conn:
        conv = await conn.fetchrow(
            "SELECT id, title, created_at, updated_at FROM conversations WHERE id = $1",
            uuid.UUID(conv_id),
        )
        if not conv:
            return None

        msgs = await conn.fetch("""
            SELECT id, role, content, sources, entities, confidence,
                   query_time_ms, cached, follow_ups, created_at
            FROM conversation_messages
            WHERE conversation_id = $1
            ORDER BY created_at ASC
        """, uuid.UUID(conv_id))

        return {
            "id": str(conv["id"]),
            "title": conv["title"],
            "created_at": conv["created_at"].isoformat(),
            "updated_at": conv["updated_at"].isoformat(),
            "messages": [
                {
                    "id": str(m["id"]),
                    "role": m["role"],
                    "content": m["content"],
                    "sources": json.loads(m["sources"]) if m["sources"] else None,
                    "entities": json.loads(m["entities"]) if m["entities"] else None,
                    "confidence": m["confidence"],
                    "query_time_ms": m["query_time_ms"],
                    "cached": m["cached"],
                    "follow_ups": json.loads(m["follow_ups"]) if m["follow_ups"] else None,
                    "created_at": m["created_at"].isoformat(),
                }
                for m in msgs
            ],
        }


async def rename_conversation(conv_id: str, title: str) -> Optional[dict]:
    """Rename a conversation."""
    async with _pool.acquire() as conn:
        row = await conn.fetchrow("""
            UPDATE conversations SET title = $2, updated_at = NOW()
            WHERE id = $1
            RETURNING id, title, updated_at
        """, uuid.UUID(conv_id), title)
        if not row:
            return None
        return {"id": str(row["id"]), "title": row["title"], "updated_at": row["updated_at"].isoformat()}


async def delete_conversation(conv_id: str) -> bool:
    """Delete a conversation and all its messages."""
    async with _pool.acquire() as conn:
        result = await conn.execute(
            "DELETE FROM conversations WHERE id = $1", uuid.UUID(conv_id)
        )
        return result == "DELETE 1"


async def add_message(
    conv_id: str,
    role: str,
    content: str,
    sources: list = None,
    entities: list = None,
    confidence: float = None,
    query_time_ms: int = None,
    cached: bool = False,
    follow_ups: list = None,
) -> dict:
    """Add a message to a conversation."""
    async with _pool.acquire() as conn:
        row = await conn.fetchrow("""
            INSERT INTO conversation_messages
                (conversation_id, role, content, sources, entities, confidence,
                 query_time_ms, cached, follow_ups)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            RETURNING id, created_at
        """,
            uuid.UUID(conv_id),
            role,
            content,
            json.dumps(sources) if sources else None,
            json.dumps(entities) if entities else None,
            confidence,
            query_time_ms,
            cached,
            json.dumps(follow_ups) if follow_ups else None,
        )

        # Set placeholder title from first user message (will be replaced by AI title from frontend)
        if role == "user":
            count = await conn.fetchval(
                "SELECT COUNT(*) FROM conversation_messages WHERE conversation_id = $1 AND role = 'user'",
                uuid.UUID(conv_id),
            )
            if count == 1:  # First user message — set placeholder
                title = content[:80].strip()
                if len(content) > 80:
                    title += "..."
                await conn.execute(
                    "UPDATE conversations SET title = $2, updated_at = NOW() WHERE id = $1",
                    uuid.UUID(conv_id), title,
                )

        # Update conversation timestamp
        await conn.execute(
            "UPDATE conversations SET updated_at = NOW() WHERE id = $1",
            uuid.UUID(conv_id),
        )

        return {
            "id": str(row["id"]),
            "role": role,
            "content": content,
            "created_at": row["created_at"].isoformat(),
        }


async def get_conversation_history(conv_id: str, limit: int = 10) -> list[dict]:
    """Get recent messages for context (used by query engine)."""
    async with _pool.acquire() as conn:
        msgs = await conn.fetch("""
            SELECT role, content FROM conversation_messages
            WHERE conversation_id = $1
            ORDER BY created_at DESC
            LIMIT $2
        """, uuid.UUID(conv_id), limit)
        # Return in chronological order
        return [{"role": m["role"], "content": m["content"]} for m in reversed(msgs)]
