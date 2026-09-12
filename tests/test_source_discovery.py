"""Real production search failures cannot become completed empty discovery."""
import unittest
from unittest.mock import AsyncMock, patch
from tests.runtime import configure_test_environment
configure_test_environment()
from app.embeddings import EmbeddingsStore
from app.source_discovery import SourceDiscovery, graph_document_ids


class FailingPool:
    def acquire(self): raise OSError('synthetic datastore outage')


class DiscoveryTests(unittest.IsolatedAsyncioTestCase):
    async def test_real_store_transport_and_embedding_failures_stay_failed(self):
        store = EmbeddingsStore(); store.pool = FailingPool()
        graph = type('Graph', (), {'get_node': AsyncMock()})()
        try:
            with patch.object(store.openai.embeddings, 'create', AsyncMock(side_effect=OSError('synthetic model outage'))):
                found = await SourceDiscovery(store, graph).search('station', 'search')
            rows = found['_discovery'][1:]
            self.assertEqual(len(rows), 4)
            self.assertTrue(all(o['status'] == 'failed' for o in rows), rows)
            self.assertTrue(all(o['document_ids'] == [] for o in rows))
        finally:
            store.pool = None; await store.close()

    async def test_partial_keyword_results_survive_second_query_failure(self):
        store = EmbeddingsStore()
        conn = type('Connection', (), {'execute': AsyncMock(), 'fetch': AsyncMock(
            side_effect=[[{'document_id': 42}], OSError('second query failed')])})()
        class Pool:
            def acquire(self): return self
            async def __aenter__(self): return conn
            async def __aexit__(self, *args): pass
        store.pool = Pool()
        try:
            with patch.object(store, 'vector_search', AsyncMock(return_value=[])), patch.object(
                    store, 'entity_vector_search', AsyncMock(return_value=[])), patch.object(
                    store, 'entity_keyword_search', AsyncMock(return_value=[])):
                found = await SourceDiscovery(store, object()).search('station', 'search')
            op = next(o for o in found['_discovery'] if o['id'].endswith(':keyword_results'))
            self.assertEqual(op['status'], 'failed')
            self.assertEqual(op['document_ids'], [42])
        finally:
            store.pool = None; await store.close()

    def test_graph_provenance_never_scrapes_prose_or_coerces_ids(self):
        value = {'properties': {'source_doc_ids': [1, 2, True, '3'], 'description': 'document 800'},
                 'relationships': [{'rel_props': {'source_doc': 4}, 'neighbor_props': {'paperless_id': 5}}]}
        self.assertEqual(graph_document_ids(value), [1, 2, 4, 5])

    async def test_graph_only_sources_are_retained_before_chunk_or_display_limits(self):
        index = type('Index', (), {
            'vector_search': AsyncMock(return_value=[{'document_id': 1}]),
            'keyword_search': AsyncMock(return_value=[]),
            'entity_vector_search': AsyncMock(return_value=[]),
            'entity_keyword_search': AsyncMock(return_value=[])})()
        graph = type('Graph', (), {
            'get_document_entities': AsyncMock(return_value=[{'uuid': 'person', 'source_doc_ids': list(range(2, 41))}]),
            'get_node': AsyncMock(return_value={'properties': {'uuid': 'person', 'source_doc_ids': [41]}}),
            'get_subgraph': AsyncMock(return_value={'nodes': [{'properties': {'source_doc_ids': [42]}}]})})()
        found = await SourceDiscovery(index, graph).search('station', 'search')
        ids = {i for op in found['_discovery'] for i in op['document_ids']}
        self.assertEqual(ids, set(range(1, 43)))
        self.assertTrue(all(o['status'] == 'complete' for o in found['_discovery']))

    async def test_disjoint_successful_keyword_samples_preserve_all_observed_leads(self):
        store = EmbeddingsStore()
        try:
            for entity in (False, True):
                key = 'entity_uuid' if entity else 'document_id'
                left = [{key: str(i) if entity else i, 'chunk_index': 0} for i in range(1, 16)]
                right = [{key: str(i) if entity else i, 'chunk_index': 0} for i in range(16, 31)]
                conn = type('Connection', (), {'execute': AsyncMock(), 'fetch': AsyncMock(side_effect=[left, right])})()
                class Pool:
                    def acquire(self): return self
                    async def __aenter__(self): return conn
                    async def __aexit__(self, *args): pass
                store.pool = Pool()
                method = store.entity_keyword_search if entity else store.keyword_search
                rows = await method('station', limit=15, strict=True)
                self.assertEqual({r[key] for r in rows}, {r[key] for r in left + right})
        finally:
            store.pool = None; await store.close()
