"""Acquisition uses production SQL against an isolated disposable schema."""
import os
import unittest
import uuid
import asyncpg
from tests.runtime import configure_test_environment
configure_test_environment()
from tests.test_storage_integrity import local_url
from app.embeddings import EmbeddingsStore, INIT_SQL
from app.source_acquisition import SourceAcquisition, Execution
from tests.test_source_acquisition import Originals, docs, request
from app.paperless import PaperlessClient


@unittest.skipUnless(os.environ.get('STORAGE_TEST_DSN'), 'Requires disposable localhost PostgreSQL')
class AcquisitionDatastoreTests(unittest.IsolatedAsyncioTestCase):
    async def test_paged_full_originals_include_late_matches_and_incomplete_leads(self):
        schema = 'acquisition_' + uuid.uuid4().hex
        dsn = local_url('STORAGE_TEST_DSN')
        conn = await asyncpg.connect(dsn)
        store = EmbeddingsStore()
        try:
            await conn.execute('CREATE EXTENSION IF NOT EXISTS vector WITH SCHEMA public')
            await conn.execute('CREATE EXTENSION IF NOT EXISTS pg_trgm WITH SCHEMA public')
            await conn.execute(f'CREATE SCHEMA {schema}')
            store.pool = await asyncpg.create_pool(dsn, min_size=1, max_size=4,
                server_settings={'search_path': f'{schema},public'})
            originals = docs(521)
            async with store.pool.acquire() as db:
                await db.execute(INIT_SQL)
                for i, doc in originals.items():
                    doc['title'] = 'Unclassified record'
                    doc['content'] = 'Unrelated introduction. ' * 220 + f'Station pressure record {i}.\n'
                await db.executemany('INSERT INTO document_embeddings(document_id,content,source_content,source_kind) VALUES($1,$2,$2,\'ocr\')',
                    [(i, d['content']) for i, d in originals.items()])
                await db.executemany('INSERT INTO document_hashes(document_id,content_hash,ingestion_fingerprint) VALUES($1,$2,$3)',
                    [(i, PaperlessClient.content_hash(d['content']), PaperlessClient.ingestion_fingerprint(d))
                     for i, d in originals.items() if i != 521])
                await db.execute("INSERT INTO document_feedback(document_id,reason,status) VALUES(520,'synthetic','open')")
            page = await store.acquisition_document_page(['station'], after=0, limit=67)
            self.assertEqual(page['candidate_count'], 521)
            self.assertEqual(page['document_ids'], list(range(1, 68)))
            terminal = await store.acquisition_document_page(['station'], after=521, limit=67)
            self.assertEqual(terminal, {'document_ids': [], 'candidate_count': 521, 'next_after': None})
            import asyncio
            collector = SourceAcquisition(store, Originals(originals), lambda: asyncio.sleep(0, result='g1'))
            bundle = await collector.collect(request(), [{'id': 'search', 'query': 'station',
                'status': 'complete', 'sampling': 'sampled', 'document_ids': []}], Execution(page_size=67))
            self.assertEqual(len(bundle.receipt['documents']), 521)
            self.assertEqual(bundle.receipt['documents'][-2]['state'], 'feedback_blocked')
            self.assertEqual(bundle.receipt['documents'][-1]['state'], 'unindexed')
            self.assertFalse(bundle.receipt['complete'])
            self.assertEqual(len(bundle.evidence_pack['items']), 519)
        finally:
            await store.close()
            await conn.execute(f'DROP SCHEMA IF EXISTS {schema} CASCADE')
            await conn.close()
