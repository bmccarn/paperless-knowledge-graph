#!/usr/bin/env python3
"""Report named entity nodes without vector rows; --apply inserts only missing rows.

Run from the repository root with python -m scripts.backfill_entity_embeddings.
Dry run by default. Read PGPASSWORD and NEO4J_PASSWORD from the environment.
Embedding requests use LITELLM_URL, LITELLM_API_KEY and EMBEDDING_MODEL.
"""
from __future__ import annotations

import argparse
import asyncio
from collections import Counter
import os
from types import SimpleNamespace

import asyncpg
from neo4j import AsyncGraphDatabase



async def backfill_entity_embeddings(store, driver, *, apply: bool = False, batch_size: int = 100) -> dict:
    if batch_size < 1:
        raise ValueError("batch_size must be positive")
    counts = Counter()
    missing = inserted = 0
    after = ""
    while True:
        async with driver.session() as session:
            result = await session.run("""
                MATCH (n) WHERE NOT n:Document AND n.uuid > $after
                RETURN n.uuid AS uuid, n.name AS name, labels(n) AS labels,
                       coalesce(n.entity_type, labels(n)[0]) AS entity_type,
                       coalesce(toStringOrNull(n.description), '') AS description
                ORDER BY n.uuid LIMIT $limit
                """, after=after, limit=batch_size)
            nodes = await result.data()
        if not nodes:
            break
        after = nodes[-1]["uuid"]
        nodes = [node for node in nodes if isinstance(node["name"], str) and node["name"].strip()]
        async with store.pool.acquire() as conn:
            existing = {row["entity_uuid"] for row in await conn.fetch(
                "SELECT entity_uuid FROM entity_embeddings WHERE entity_uuid=ANY($1::text[])",
                [node["uuid"] for node in nodes])}
        for node in nodes:
            if node["uuid"] in existing:
                continue
            missing += 1
            counts.update(node["labels"])
            if apply:
                inserted += await store.store_entity_embedding(
                    node["uuid"], node["name"], node["entity_type"], node["description"], only_missing=True)
    return {"missing": missing, "by_label": dict(sorted(counts.items())), "inserted": inserted}


async def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--postgres-dsn", required=True)
    parser.add_argument("--neo4j-uri", required=True)
    parser.add_argument("--neo4j-user", default="neo4j")
    parser.add_argument("--batch-size", type=int, default=100)
    parser.add_argument("--apply", action="store_true", help="generate and insert missing vectors")
    args = parser.parse_args()
    if args.batch_size < 1:
        parser.error("--batch-size must be positive")
    password = os.environ.get("NEO4J_PASSWORD")
    store = pool = None
    driver = AsyncGraphDatabase.driver(args.neo4j_uri, auth=(args.neo4j_user, password) if password else None)
    try:
        # Do not initialize or migrate schemas, including during a dry run.
        pool = await asyncpg.create_pool(args.postgres_dsn, min_size=1, max_size=2)
        if args.apply:
            from app.embeddings import embeddings_store as store
            store.pool = pool
        else:
            store = SimpleNamespace(pool=pool)
        report = await backfill_entity_embeddings(store, driver, apply=args.apply, batch_size=args.batch_size)
    finally:
        await driver.close()
        if pool is not None:
            await pool.close()
        if args.apply and store is not None:
            await store.openai.close()
    for label, count in report["by_label"].items():
        print(f"{label}\t{count}")
    print(f"Missing entity_embeddings rows: {report['missing']}")
    print(f"Inserted {report['inserted']} rows" if args.apply else "Dry run: no writes or embedding requests")


if __name__ == "__main__":
    asyncio.run(main())
