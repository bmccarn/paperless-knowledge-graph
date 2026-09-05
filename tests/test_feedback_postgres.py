"""Actual feedback SQL against a disposable PostgreSQL schema (opt in)."""

import os
import unittest

import asyncpg

from tests.runtime import configure_test_environment

configure_test_environment()

from app.embeddings import EmbeddingsStore, INIT_SQL


@unittest.skipUnless(os.environ.get("FEEDBACK_TEST_DSN"), "Set FEEDBACK_TEST_DSN to a disposable PostgreSQL instance")
class FeedbackPostgresTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        dsn = os.environ["FEEDBACK_TEST_DSN"]
        connection = await asyncpg.connect(dsn)
        try:
            await connection.execute("CREATE SCHEMA IF NOT EXISTS feedback_test")
            await connection.execute("CREATE EXTENSION IF NOT EXISTS vector WITH SCHEMA public")
            await connection.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm WITH SCHEMA public")
            extension_schemas = await connection.fetch("SELECT DISTINCT n.nspname FROM pg_extension e JOIN pg_namespace n ON n.oid=e.extnamespace WHERE e.extname IN ('vector','pg_trgm')")
            search_path = ",".join(['feedback_test', *['"' + row["nspname"].replace('"', '""') + '"' for row in extension_schemas], 'public'])
        finally:
            await connection.close()
        self.store = EmbeddingsStore()
        self.store.pool = await asyncpg.create_pool(dsn, min_size=1, max_size=2, server_settings={"search_path": search_path})
        async with self.store.pool.acquire() as connection:
            await connection.execute(INIT_SQL)
            await connection.execute("TRUNCATE document_feedback RESTART IDENTITY")
            await connection.execute("TRUNCATE document_hashes")

    async def asyncTearDown(self):
        await self.store.close()

    async def test_open_resolved_lifecycle_survives_reads_and_scopes_lookup(self):
        first = await self.store.add_document_feedback(9101, "extraction_wrong", "Wrong amount")
        second = await self.store.add_document_feedback(9102, "extraction_wrong", "Wrong vehicle")
        self.assertEqual(first["status"], "open")
        self.assertEqual(await self.store.get_open_feedback_document_ids([9101]), {9101})
        self.assertEqual(await self.store.get_open_feedback_document_ids([]), set())
        status = await self.store.get_document_processing_status(9101)
        self.assertEqual(status["open_feedback_count"], 1)
        self.assertIsNone(await self.store.resolve_document_feedback(9102, first["id"], "dismissed_after_review", "Wrong document"))
        resolved = await self.store.resolve_document_feedback(9101, first["id"], "dismissed_after_review", "Checked against OCR", "reviewed-hash")
        self.assertEqual(resolved["status"], "resolved")
        self.assertIsNotNone(resolved["resolved_at"])
        self.assertEqual(resolved["resolved_content_hash"], "reviewed-hash")
        self.assertEqual(await self.store.get_open_feedback_document_ids([9101, 9102]), {9102})
        self.assertIsNone(await self.store.resolve_document_feedback(9101, first["id"], "dismissed_after_review", "Repeated resolution"))
        status = await self.store.get_document_processing_status(9101)
        self.assertEqual(status["feedback_count"], 1)
        self.assertEqual(status["open_feedback_count"], 0)
        self.assertEqual((await self.store.get_document_feedback(9102))[0]["id"], second["id"])

    async def test_existing_rows_migrate_to_open_and_schema_is_idempotent(self):
        async with self.store.pool.acquire() as connection:
            await connection.execute("DROP TABLE document_feedback")
            await connection.execute("CREATE TABLE document_feedback(id SERIAL PRIMARY KEY, document_id INTEGER NOT NULL, reason TEXT NOT NULL, note TEXT, created_at TIMESTAMPTZ DEFAULT NOW())")
            await connection.execute("INSERT INTO document_feedback(document_id,reason,note) VALUES(9101,'extraction_wrong','Legacy report')")
            await connection.execute(INIT_SQL)
            await connection.execute(INIT_SQL)
        report = (await self.store.get_document_feedback(9101))[0]
        self.assertEqual(report["status"], "open")
        self.assertEqual(await self.store.get_open_feedback_document_ids([9101]), {9101})

    async def test_reindex_resolution_requires_same_successful_hash_at_mutation_time(self):
        report = await self.store.add_document_feedback(9101, "extraction_wrong", "Wrong amount")
        self.assertIsNone(await self.store.resolve_document_feedback(9101, report["id"], "reindexed_and_reviewed", "Checked correction", "fresh-hash"))
        await self.store.set_doc_hash(9101, "fresh-hash")
        self.assertIsNone(await self.store.resolve_document_feedback(9101, report["id"], "reindexed_and_reviewed", "Checked correction", "stale-hash"))
        resolved = await self.store.resolve_document_feedback(9101, report["id"], "reindexed_and_reviewed", "Checked correction", "fresh-hash")
        self.assertEqual(resolved["status"], "resolved")


if __name__ == "__main__":
    unittest.main()
