"""Observed source and derived identities must not silently conceal drift."""
import asyncio
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock

from tests.runtime import configure_test_environment
configure_test_environment()
from app.paperless import PaperlessClient
from scripts.live_query_corpus import current_corpus


class LiveCorpusTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.docs = [{'id': i, 'content': f'Original {i}', 'modified': '2026-09-09',
                      'tags': [9] if i == 3 else [], 'title': f'Record {i}'} for i in (1, 2, 3)]
        self.paperless = PaperlessClient()
        self.paperless._configured_skip_tag_names = lambda: {'hold'}
        self.paperless.get_document_summary = AsyncMock(return_value={
            'count': 3, 'latest_id': 2, 'latest_modified': '2026-09-09'})
        self.paperless.get_all_tags = AsyncMock(return_value=[{'id': 9, 'name': 'hold'}])
        self.paperless.get_all_documents = AsyncMock(return_value=self.docs)
        self.graph = SimpleNamespace(get_all_document_ids=AsyncMock(return_value={1}))
        self.vectors = SimpleNamespace(
            get_document_embedding_ids=AsyncMock(return_value={1}),
            get_document_hash_ids=AsyncMock(return_value={1}),
            get_ingestion_fingerprints=AsyncMock(return_value={1: PaperlessClient.ingestion_fingerprint(self.docs[0])}),
            get_open_feedback_document_ids=AsyncMock(return_value=set()))
        self.generation = AsyncMock(return_value='redis:7')

    async def capture(self):
        return await current_corpus(self.paperless, self.graph, self.vectors, self.generation)

    async def test_missing_eligible_and_held_documents_remain_distinct(self):
        snapshot = await self.capture()
        self.assertEqual(snapshot['eligible_ids'], [1, 2])
        self.assertEqual(snapshot['missing_ids'], [2])
        self.assertEqual(snapshot['stale_ids'], [])
        self.assertEqual(snapshot['extra_ids'], [])
        self.assertEqual(snapshot, await self.capture())

    async def test_changed_generation_or_source_inventory_rejects(self):
        self.generation.side_effect = ['redis:7', 'redis:8']
        with self.assertRaises(ValueError): await self.capture()
        self.generation.side_effect = None
        original = self.paperless.get_document_summary.return_value
        self.paperless.get_document_summary.side_effect = [original, {**original, 'latest_id': 1}]
        with self.assertRaises(ValueError): await self.capture()
        self.paperless.get_document_summary.side_effect = None
        self.paperless.get_all_documents.return_value = [self.docs[0], self.docs[0], self.docs[2]]
        with self.assertRaises(ValueError): await self.capture()

    async def test_local_fallback_rejects_before_any_document_read(self):
        self.generation.return_value = 'local:unshared:0'
        with self.assertRaises(ValueError): await self.capture()
        self.paperless.get_all_documents.assert_not_called()
        self.graph.get_all_document_ids.assert_not_called()

    async def test_same_counts_do_not_hide_replaced_ids_changed_originals_or_stale_processing(self):
        original = await self.capture()
        self.docs[0]['content'] = 'Changed original'
        changed = await self.capture()
        self.assertEqual(changed['stale_ids'], [1])
        self.assertNotEqual(original['source_inventory_sha256'], changed['source_inventory_sha256'])
        self.graph.get_all_document_ids.return_value = {2}
        replacement = await self.capture()
        self.assertEqual(replacement['missing_ids'], [1, 2])
        self.assertNotEqual(changed, replacement)

    async def test_tags_feedback_and_processing_markers_are_bound(self):
        original = await self.capture()
        self.vectors.get_open_feedback_document_ids.return_value = {1}
        self.assertNotEqual(original, await self.capture())
        self.vectors.get_document_hash_ids.return_value = {1, 2}
        with self.assertRaises(ValueError): await self.capture()
        self.vectors.get_document_hash_ids.return_value = {1}
        self.paperless.get_all_tags.side_effect = [[{'id': 9, 'name': 'hold'}], []]
        with self.assertRaises(ValueError): await self.capture()

    async def test_failed_index_read_joins_sibling_reads_before_returning(self):
        started, cleaned = asyncio.Event(), asyncio.Event()
        async def blocked():
            started.set()
            try:
                await asyncio.Future()
            finally:
                await asyncio.sleep(0)
                cleaned.set()
        async def failed():
            await started.wait()
            raise OSError('Read failed')
        self.graph.get_all_document_ids.side_effect = blocked
        self.vectors.get_document_embedding_ids.side_effect = failed
        with self.assertRaises(ExceptionGroup): await self.capture()
        self.assertTrue(cleaned.is_set())
