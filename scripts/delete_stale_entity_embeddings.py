#!/usr/bin/env python3
"""List, and with --apply delete, entity_embeddings rows whose UUID is no longer in Neo4j.

Dry run by default. Pass PGPASSWORD and NEO4J_PASSWORD in the environment,
not on the command line.
"""

from __future__ import annotations

import argparse
import asyncio
import os

import asyncpg
from neo4j import AsyncGraphDatabase


async def stale_entity_embeddings(conn, driver, *, apply: bool = False) -> list[dict]:
    # Read PostgreSQL first: a row is written after its node, so an ingest
    # running at the same time cannot make a live entity look stale.
    rows = [dict(row) for row in await conn.fetch(
        "SELECT entity_uuid, entity_type, entity_name FROM entity_embeddings ORDER BY entity_uuid")]
    async with driver.session() as session:
        result = await session.run("MATCH (n) WHERE n.uuid IN $uuids RETURN n.uuid AS uuid",
                                   uuids=[row["entity_uuid"] for row in rows])
        live = {row["uuid"] async for row in result}
    stale = [row for row in rows if row["entity_uuid"] not in live]
    if apply and stale:
        await conn.execute("DELETE FROM entity_embeddings WHERE entity_uuid = ANY($1::text[])",
                           [row["entity_uuid"] for row in stale])
    return stale


async def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--postgres-dsn", required=True, help="for example postgresql://kguser@localhost:5432/knowledge_graph")
    parser.add_argument("--neo4j-uri", required=True, help="for example bolt://localhost:7687")
    parser.add_argument("--neo4j-user", default="neo4j")
    parser.add_argument("--apply", action="store_true", help="delete the listed rows")
    args = parser.parse_args()
    password = os.environ.get("NEO4J_PASSWORD")
    conn = await asyncpg.connect(args.postgres_dsn)
    driver = AsyncGraphDatabase.driver(args.neo4j_uri, auth=(args.neo4j_user, password) if password else None)
    try:
        stale = await stale_entity_embeddings(conn, driver, apply=args.apply)
    finally:
        await driver.close()
        await conn.close()
    for row in stale:
        print(f"{row['entity_uuid']}\t{row['entity_type']}\t{row['entity_name']}")
    print(f"{'Deleted' if args.apply else 'Would delete'} {len(stale)} stale entity_embeddings rows")


if __name__ == "__main__":
    asyncio.run(main())
