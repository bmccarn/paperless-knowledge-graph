"""Real storage contracts against explicitly selected disposable local services."""
import json
import os
import time
import unittest
from urllib.parse import urlparse
from unittest.mock import AsyncMock

import asyncpg
import numpy as np
from neo4j import AsyncGraphDatabase

from tests.runtime import configure_test_environment
configure_test_environment()
from app.embeddings import EmbeddingsStore, INIT_SQL
from app.graph import GraphStore


def local_url(name):
    value = os.environ[name]
    if urlparse(value).hostname not in {"127.0.0.1", "localhost", "::1"}:
        raise ValueError("Storage tests require a disposable localhost service")
    return value


@unittest.skipUnless(os.environ.get("STORAGE_TEST_DSN"), "Set STORAGE_TEST_DSN for disposable PostgreSQL")
class PostgresIntegrityTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.store = EmbeddingsStore()
        self.store.pool = await asyncpg.create_pool(local_url("STORAGE_TEST_DSN"), min_size=1, max_size=2)
        async with self.store.pool.acquire() as conn:
            await conn.execute("CREATE EXTENSION IF NOT EXISTS vector WITH SCHEMA public; CREATE EXTENSION IF NOT EXISTS pg_trgm WITH SCHEMA public")
            await conn.execute("CREATE SCHEMA IF NOT EXISTS storage_test")
        await self.store.pool.close()
        self.store.pool = await asyncpg.create_pool(local_url("STORAGE_TEST_DSN"), min_size=1, max_size=2,
                                                   server_settings={"search_path": "storage_test,public"})
        async with self.store.pool.acquire() as conn:
            await conn.execute(INIT_SQL)
            await conn.execute("TRUNCATE document_embeddings, entity_embeddings, entity_review_decisions, document_hashes")
            await conn.execute("DROP INDEX IF EXISTS idx_embeddings_halfvec_hnsw")
            await conn.execute("ALTER TABLE document_embeddings ALTER COLUMN embedding TYPE vector(3072)")

    async def asyncTearDown(self):
        await self.store.close()

    async def test_review_snapshot_reorders_with_ids_and_roundtrips(self):
        left = {"type": "Person", "names": ["zed"], "source_doc_ids": [11]}
        right = {"type": "Person", "names": ["amy"], "source_doc_ids": [12]}
        row = await self.store.add_entity_review_decision("z", "a", "split", left_identity=left, right_identity=right)
        self.assertEqual(row["left_identity"], right)
        self.assertEqual(row["right_identity"], left)
        row = (await self.store.get_entity_review_decisions())[0]
        self.assertEqual(row["left_uuid"], "a")
        self.assertEqual(row["left_identity"]["source_doc_ids"], [12])

    async def test_legacy_veto_migration_preserves_nineteen_rows_without_trusting_aliases(self):
        async with self.store.pool.acquire() as conn:
            await conn.execute("ALTER TABLE entity_review_decisions DROP COLUMN provenance, DROP COLUMN identity_status")
            await conn.executemany("INSERT INTO entity_review_decisions (left_uuid,right_uuid,decision,note) VALUES($1,$2,'never_merge','preserve')",
                                  [(f"left-{i}", f"right-{i}") for i in range(19)])
            original = await conn.fetch("SELECT id,created_at FROM entity_review_decisions ORDER BY id")
            await conn.execute(INIT_SQL)
            await conn.execute(INIT_SQL)
        for row in await self.store.get_entity_review_decisions():
            self.assertEqual(row["provenance"], "legacy_unknown")
            self.assertEqual(row["identity_status"], "unassessed")
            self.assertIsNone(row["left_identity"])
            await self.store.set_entity_decision_identity_status(row["left_uuid"], row["right_uuid"], row["decision"], "unresolved_legacy")
        rows = await self.store.get_entity_review_decisions()
        self.assertEqual(len(rows), 19)
        self.assertTrue(all(row["identity_status"] == "unresolved_legacy" for row in rows))
        async with self.store.pool.acquire() as conn:
            self.assertEqual(await conn.fetch("SELECT id,created_at FROM entity_review_decisions ORDER BY id"), original)
        reviewed = await self.store.add_entity_review_decision("human-a", "human-b", "merged",
            left_identity={"canonical_name": "Example One", "type": "Organization"},
            right_identity={"canonical_name": "Example Two", "type": "Organization"},
            provenance="human_review", identity_status="active")
        self.assertEqual(reviewed["provenance"], "human_review")
        self.assertEqual(reviewed["identity_status"], "active")

    async def test_ocr_origin_and_ingestion_fingerprint_survive_storage_reads(self):
        from app.evidence import build_evidence_pack
        vector = [1.0] + [0.0] * 3071
        await self.store.store_document_embedding(101, "Date: 2099-01-01\n\nPremium $321.",
            embedding=vector, source_kind="ocr", source_content="Premium $321.")
        await self.store.store_document_embedding(101, "Invented premium $999.", chunk_index=9999,
            embedding=vector, source_kind="generated")
        await self.store.set_doc_hash(101, "original-ocr-hash", ingestion_fingerprint="metadata-fingerprint")
        self.assertEqual(await self.store.get_doc_hash(101), "original-ocr-hash")
        self.assertEqual(await self.store.get_ingestion_fingerprints([101]), {101: "metadata-fingerprint"})
        self.store.generate_embedding = AsyncMock(return_value=vector)
        for rows in (await self.store.get_chunks_for_document(101),
                     await self.store.get_chunks_for_documents([101]),
                     await self.store.vector_search("premium"),
                     await self.store.keyword_search("premium")):
            pack = build_evidence_pack("premium", {}, rows, [])
            self.assertEqual([item["content"] for item in pack["items"]], ["Premium $321."])
        await self.store.delete_doc_hash(101)
        self.assertEqual(await self.store.get_ingestion_fingerprints([101]), {})

    async def test_legacy_origin_migration_preserves_ocr_and_requires_reconciliation(self):
        from app.evidence import build_evidence_pack
        async with self.store.pool.acquire() as conn:
            await conn.execute("ALTER TABLE document_embeddings DROP COLUMN source_kind, DROP COLUMN source_content")
            await conn.execute("ALTER TABLE document_hashes DROP COLUMN ingestion_fingerprint")
            await conn.execute("INSERT INTO document_embeddings (document_id, chunk_index, content, title, doc_type) "
                               "VALUES (101, 0, $1, 'Statement', 'invoice'), (101, 9999, 'Invented premium $999.', 'Statement', 'invoice')",
                               "Document: Statement\nType: invoice\nDate: 2099-01-01\n\nPremium $321.")
            await conn.execute("INSERT INTO document_hashes (document_id, content_hash) VALUES (101, 'legacy-ocr-hash')")
            await conn.execute(INIT_SQL)
            await conn.execute(INIT_SQL)
        self.assertEqual(await self.store.get_doc_hash(101), "legacy-ocr-hash")
        self.assertEqual(await self.store.get_ingestion_fingerprints([101]), {101: None})
        rows = await self.store.get_chunks_for_document(101)
        self.assertEqual(len(rows), 2)
        self.assertEqual([item["content"] for item in build_evidence_pack("premium", {}, rows, [])["items"]], ["Premium $321."])

    async def test_prepared_embedding_writes_and_empty_generation_fails(self):
        self.store.generate_embedding = AsyncMock(side_effect=AssertionError("must not regenerate prepared vector"))
        vector = [1.0] + [0.0] * 3071
        await self.store.store_document_embedding(11, "Prepared text", embedding=vector)
        rows = await self.store.get_chunks_for_document(11)
        self.assertEqual(rows[0]["content"], "Prepared text")
        self.store.generate_embedding = AsyncMock(return_value=[])
        with self.assertRaises(ValueError):
            await self.store.store_document_embedding(12, "No vector")

    async def test_dimension_mismatch_fails_without_erasing_existing_data(self):
        async with self.store.pool.acquire() as conn:
            await conn.execute("DROP INDEX IF EXISTS idx_embeddings_halfvec_hnsw")
            await conn.execute("ALTER TABLE document_embeddings ALTER COLUMN embedding TYPE vector(3)")
            await conn.execute("INSERT INTO document_embeddings(document_id,content,embedding) VALUES(1,'keep me','[1,0,0]')")
        try:
            with self.assertRaisesRegex(RuntimeError, "explicit backed-up migration"):
                await self.store._migrate_dimensions()
            async with self.store.pool.acquire() as conn:
                self.assertEqual(await conn.fetchval("SELECT content FROM document_embeddings WHERE document_id=1"), "keep me")
        finally:
            async with self.store.pool.acquire() as conn:
                await conn.execute("TRUNCATE document_embeddings")
                await conn.execute("ALTER TABLE document_embeddings ALTER COLUMN embedding TYPE vector(3072)")

    async def test_3072_index_exact_baseline_and_optional_candidate_recall(self):
        rng = np.random.default_rng(17)
        vectors = rng.normal(size=(320, 3072)).astype(np.float32)
        async with self.store.pool.acquire() as conn:
            await conn.executemany("INSERT INTO document_embeddings(document_id,content,embedding) VALUES($1,$2,$3::vector)",
                                   [(i + 1, f"Synthetic {i + 1}", str(v.tolist())) for i, v in enumerate(vectors)])
        config = await self.store.create_vector_indexes()
        self.assertEqual(config["default_search"], "exact")
        timings, recalls = [], []
        for i in (2, 27, 101, 211):
            query = vectors[i] + rng.normal(scale=0.05, size=3072)
            expected = (np.argsort(-(vectors @ query) / (np.linalg.norm(vectors, axis=1) * np.linalg.norm(query)))[:10] + 1).tolist()
            self.store.generate_embedding = AsyncMock(return_value=query.tolist())
            start = time.perf_counter()
            exact = await self.store.vector_search("synthetic", 10)
            exact_ms = (time.perf_counter() - start) * 1000
            self.assertEqual([r["document_id"] for r in exact], expected)
            start = time.perf_counter()
            approximate = await self.store.vector_search("synthetic", 10, approximate=True)
            approximate_ms = (time.perf_counter() - start) * 1000
            recalls.append(len(set(expected) & {r["document_id"] for r in approximate}) / 10)
            timings.append({"exact_ms": round(exact_ms, 2), "approximate_ms": round(approximate_ms, 2)})
        async with self.store.pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute("SET LOCAL enable_seqscan=off")
                plan = await conn.fetch("EXPLAIN SELECT * FROM document_embeddings ORDER BY embedding::halfvec(3072) <=> $1::halfvec(3072) LIMIT 10", str(vectors[2].tolist()))
                self.assertIn("idx_embeddings_halfvec_hnsw", "\n".join(r[0] for r in plan))
        print("VECTOR_BASELINE " + json.dumps({"rows": 320, "dimensions": 3072, "recall_at_10": recalls, "timings": timings}))
        self.assertGreaterEqual(min(recalls), 0.9)


@unittest.skipUnless(os.environ.get("STORAGE_TEST_NEO4J"), "Set STORAGE_TEST_NEO4J for disposable Neo4j")
class RelationshipIntegrityTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.store = GraphStore()
        self.store.driver = AsyncGraphDatabase.driver(local_url("STORAGE_TEST_NEO4J"), auth=None)
        async with self.store.driver.session() as session:
            await session.run("MATCH (n) WHERE n.uuid STARTS WITH 'relationship-test-' OR n.paperless_id IN [990101,990102] DETACH DELETE n")
            await session.run("CREATE (:Person {uuid:'relationship-test-a',name:'Alice'}), (:Organization {uuid:'relationship-test-b',name:'Example'}), (:Asset {uuid:'relationship-test-unrelated',name:'Unrelated orphan'})")
        for doc_id in (990101, 990102):
            await self.store.create_document_node(doc_id, "Synthetic", "test", "2026-09-04", "test")

    async def asyncTearDown(self):
        async with self.store.driver.session() as session:
            await session.run("MATCH (n) WHERE n.uuid STARTS WITH 'relationship-test-' OR n.paperless_id IN [990101,990102] DETACH DELETE n")
        await self.store.driver.close()

    async def edge(self):
        async with self.store.driver.session() as session:
            row = await (await session.run("MATCH (:Person {uuid:'relationship-test-a'})-[r:WORKS_FOR]->() RETURN properties(r) AS props")).single()
            return row["props"] if row else None

    async def test_shared_support_is_idempotent_and_either_deletion_preserves_other(self):
        for first, second in ((990101, 990102), (990102, 990101)):
            for doc_id in (first, second, first):
                await self.store.create_relationship("relationship-test-a", "Person", "relationship-test-b", "Organization", "WORKS_FOR",
                    {"source_doc": doc_id, "confidence": 0.9, "inferred": False,
                     "evidence_spans": [{"quote": f"Source {doc_id} confirms employment", "start": 0}]})
            edge = await self.edge()
            self.assertEqual(edge["source_doc_ids"], [990101, 990102])
            self.assertEqual(edge["weight"], 2)
            self.assertEqual(len(edge["support_records"]), 2)
            await self.store.delete_document_graph(first)
            edge = await self.edge()
            self.assertEqual(edge["source_doc_ids"], [second])
            self.assertEqual(edge["weight"], 1)
            self.assertIn(str(second), edge["support_records"][0])
        await self.store.delete_document_graph(990101)
        self.assertIsNone(await self.edge())
        async with self.store.driver.session() as session:
            row = await (await session.run("MATCH (n {uuid:'relationship-test-unrelated'}) RETURN count(n) AS count")).single()
            self.assertEqual(row["count"], 1)


if __name__ == "__main__":
    unittest.main()
