"""Read-only attachment and teardown with local synthetic datastore clients."""
import asyncio
import json
from contextlib import asynccontextmanager
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, Mock, patch

from tests.runtime import configure_test_environment
configure_test_environment()
from app import query
from scripts.live_query_capture import ModelCapture
from scripts.live_query_readers import attach_readers, CapturedDocuments


class ReaderIsolationTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.capture = ModelCapture(self.root, max_calls=5, seconds=10)
        self.client = SimpleNamespace(max_retries=2, close=AsyncMock())
        self.graph = SimpleNamespace(driver=None, search_nodes=AsyncMock(return_value=[]))
        self.vector = SimpleNamespace(pool=None, openai=self.client, keyword_search=AsyncMock(return_value=[]))
        self.connection = SimpleNamespace(fetchval=AsyncMock(return_value='on'))
        @asynccontextmanager
        async def acquire(): yield self.connection
        self.pool = SimpleNamespace(acquire=acquire, close=AsyncMock())
        self.driver = SimpleNamespace(verify_connectivity=AsyncMock(), close=AsyncMock(), session=Mock())
        self.patches = [patch.object(query, 'graph_store', self.graph),
                        patch.object(query, 'embeddings_store', self.vector),
                        patch('asyncpg.create_pool', AsyncMock(return_value=self.pool)),
                        patch('neo4j.AsyncGraphDatabase.driver', return_value=self.driver)]
        self.started = [p.start() for p in self.patches]
        for p in reversed(self.patches): self.addCleanup(p.stop)

    async def test_only_read_methods_and_private_caches_are_attached_then_restored(self):
        production_cache = query.query_cache
        async with attach_readers(self.capture, self.root / 'originals') as readers:
            self.assertIsNot(query.query_cache, production_cache)
            await query.graph_store.search_nodes('name')
            await query.embeddings_store.keyword_search('text')
            for store, method in [(query.graph_store, 'create_relationship'),
                                  (query.embeddings_store, 'store_document')]:
                with self.assertRaises(AttributeError): getattr(store, method)
            self.graph.driver.session()
            self.driver.session.assert_called_once_with(default_access_mode='READ')
            self.assertEqual(readers['graph_reads'].denied, ['create_relationship'])
        self.assertIs(query.query_cache, production_cache)
        self.assertIs(query.graph_store, self.graph)
        self.assertIsNone(self.graph.driver)
        self.assertIsNone(self.vector.pool)
        self.assertEqual(self.client.max_retries, 2)
        self.pool.close.assert_awaited_once(); self.driver.close.assert_awaited_once()
        self.client.close.assert_awaited_once()
        self.assertEqual(self.started[2].await_args.kwargs['server_settings'],
                         {'default_transaction_read_only': 'on'})

    async def test_bad_read_only_session_fails_before_graph_connection(self):
        self.connection.fetchval.return_value = 'off'
        with self.assertRaisesRegex(ValueError, 'not read-only'):
            async with attach_readers(self.capture, self.root / 'originals'):
                self.fail('Must not attach readers')
        self.started[3].assert_not_called()
        self.pool.close.assert_awaited_once(); self.client.close.assert_awaited_once()
        self.assertIsNone(self.vector.pool)

    async def test_initialized_serving_store_is_rejected_before_any_attachment(self):
        self.graph.driver = object()
        with self.assertRaisesRegex(ValueError, 'isolated'):
            async with attach_readers(self.capture, self.root / 'originals'):
                self.fail('Serving process must not be reused')
        self.started[2].assert_not_awaited()
        self.client.close.assert_not_awaited()

    async def test_cancellation_closes_owned_clients_and_restores_globals(self):
        with self.assertRaises(asyncio.CancelledError):
            async with attach_readers(self.capture, self.root / 'originals'):
                raise asyncio.CancelledError
        self.pool.close.assert_awaited_once(); self.driver.close.assert_awaited_once()
        self.client.close.assert_awaited_once()
        self.assertIs(query.graph_store, self.graph)

    async def test_originals_are_retrieved_normally_and_changes_retained_as_failure(self):
        original = {'id': 42, 'content': 'Original text', 'modified': '2026-01-01'}
        changed = {**original, 'content': 'Changed text'}
        client = SimpleNamespace(get_document=AsyncMock(side_effect=[original, original, changed]))
        documents = CapturedDocuments(client, self.root)
        returned = await documents.get_document(42)
        returned['content'] = 'Caller edit'
        self.assertEqual((await documents.get_document(42))['content'], 'Original text')
        self.assertEqual(len(documents.hashes), 1)
        with self.assertRaisesRegex(ValueError, 'snapshot changed'):
            await documents.get_document(42)
        self.assertTrue(documents.changed)
        self.assertEqual(len(documents.hashes), 2)
        self.assertEqual(client.get_document.await_count, 3)
        self.assertEqual(json.loads((self.root / 'document-0001.json').read_bytes())['content'], 'Changed text')

    async def test_boolean_and_float_document_ids_cannot_alias_a_source(self):
        for index, invalid in enumerate((True, 1.0, 0, -1, '1', None)):
            with self.subTest(invalid=invalid):
                directory = self.root / str(index); directory.mkdir()
                client = SimpleNamespace(get_document=AsyncMock(return_value={'id': invalid, 'content': 'Text'}))
                documents = CapturedDocuments(client, directory)
                with self.assertRaisesRegex(ValueError, 'identity'):
                    await documents.get_document(invalid)
                client.get_document.assert_not_awaited()
                with self.assertRaisesRegex(ValueError, 'identity'):
                    await documents.get_document(1)
                self.assertTrue(documents.changed)
                self.assertFalse(documents.documents)
                self.assertEqual(len(documents.hashes), 1)
