"""Full-module browsing contracts; opt-in Neo4j tests use dedicated fixtures."""
import asyncio
import os
from unittest.mock import patch
import unittest
from urllib.parse import urlparse

from tests.runtime import configure_test_environment
configure_test_environment()
from app.graph import GraphStore
from neo4j import AsyncGraphDatabase

TEST_URI = os.environ.get('NEO4J_TEST_URI', '')
PREFIX = 'pagination-test-'
FIRST_ID = 930000000


class ParameterTests(unittest.IsolatedAsyncioTestCase):
    async def test_invalid_inputs_fail_before_database_access(self):
        store = GraphStore()
        for kwargs in ({'limit': 0}, {'limit': 201}, {'offset': -1},
                       {'sort': 'n.title DELETE n'}, {'direction': 'desc DELETE n'},
                       {'node_type': 'Document) DELETE n'}):
            with self.subTest(kwargs=kwargs), self.assertRaises(ValueError):
                await store.search_nodes('query', **kwargs)

    async def test_empty_global_search_retains_compatible_empty_result(self):
        store = GraphStore()
        self.assertEqual(await store.search_nodes(''), [])
        page = await store.search_nodes('', include_page=True)
        self.assertEqual(page['total'], 0)
        self.assertFalse(page['has_more'])


@unittest.skipUnless(TEST_URI, 'Set NEO4J_TEST_URI to a disposable localhost Bolt URI')
class Neo4jBrowsingTests(unittest.IsolatedAsyncioTestCase):
    @classmethod
    def setUpClass(cls):
        if urlparse(TEST_URI).hostname not in {'localhost', '127.0.0.1'}:
            raise ValueError('Browsing integration tests require a disposable localhost database')
        async def seed():
            async with AsyncGraphDatabase.driver(TEST_URI, auth=None) as driver:
                async with driver.session() as session:
                    await session.run('MATCH (n:PaginationAudit) WHERE n.uuid STARTS WITH $prefix DETACH DELETE n', prefix=PREFIX)
                    result = await session.run('''
                        UNWIND range(0, 5104) AS i
                        CREATE (n:Document:PaginationAudit {
                            uuid: $prefix + toString(i), paperless_id: $first + i,
                            title: 'paginationfixture ' + right('00000' + toString(i), 5) +
                                   CASE WHEN i = 5104 THEN ' NeedleUnique923' ELSE '' END,
                            doc_type: CASE WHEN i % 2 = 0 THEN 'invoice' ELSE 'medical' END,
                            date: '2026-01-01', aliases: CASE WHEN i = 5104 THEN ['LateAlias923'] ELSE [] END
                        })
                    ''', prefix=PREFIX, first=FIRST_ID)
                    await result.consume()
        asyncio.run(seed())

    @classmethod
    def tearDownClass(cls):
        async def clean():
            async with AsyncGraphDatabase.driver(TEST_URI, auth=None) as driver:
                async with driver.session() as session:
                    result = await session.run('MATCH (n:PaginationAudit) WHERE n.uuid STARTS WITH $prefix DETACH DELETE n', prefix=PREFIX)
                    await result.consume()
        asyncio.run(clean())

    async def asyncSetUp(self):
        self.store = GraphStore()
        self.store.driver = AsyncGraphDatabase.driver(TEST_URI, auth=None)

    async def asyncTearDown(self):
        await self.store.close()

    async def test_finds_title_and_alias_beyond_old_5000_node_cap(self):
        for query in ('NeedleUnique923', 'LateAlias923'):
            with self.subTest(query=query):
                rows = await self.store.search_nodes(query, node_type='PaginationAudit')
                self.assertEqual([row['properties']['paperless_id'] for row in rows], [FIRST_ID + 5104])

    async def test_pages_reach_beyond_old_200_document_subset(self):
        first = await self.store.search_nodes('paginationfixture', node_type='Document', limit=25,
                                             sort='title', direction='asc', include_page=True)
        late = await self.store.search_nodes('paginationfixture', node_type='Document', limit=25, offset=250,
                                            sort='title', direction='asc', include_page=True)
        self.assertEqual(first['total'], 5105)
        self.assertEqual(first['results'][0]['properties']['paperless_id'], FIRST_ID)
        self.assertEqual(late['results'][0]['properties']['paperless_id'], FIRST_ID + 250)
        self.assertTrue(late['has_more'])
        self.assertEqual(len(late['results']), 25)

    async def test_type_filter_counts_and_order_are_global(self):
        page = await self.store.search_nodes('paginationfixture', node_type='Document', limit=25,
                                            doc_type='medical', sort='title', direction='desc', include_page=True)
        self.assertEqual(page['total'], 2552)
        self.assertEqual(page['doc_types'], {'invoice': 2553, 'medical': 2552})
        self.assertTrue(all(row['properties']['doc_type'] == 'medical' for row in page['results']))
        self.assertEqual(page['results'][0]['properties']['paperless_id'], FIRST_ID + 5103)

    async def test_final_and_empty_pages_have_truthful_metadata(self):
        page = await self.store.search_nodes('paginationfixture', node_type='Document', limit=25,
                                            offset=5100, include_page=True)
        self.assertEqual(len(page['results']), 5)
        self.assertFalse(page['has_more'])
        beyond = await self.store.search_nodes('paginationfixture', node_type='Document', offset=9999, include_page=True)
        self.assertEqual(beyond['results'], [])
        self.assertEqual(beyond['total'], 5105)
        missing = await self.store.search_nodes('AbsolutelyMissingNeedle923', node_type='Document', include_page=True)
        self.assertEqual(missing['total'], 0)
        self.assertFalse(missing['has_more'])

    async def test_document_and_graph_routes_serialize_complete_pages(self):
        from app import main
        from httpx import ASGITransport, AsyncClient
        with patch.object(main, 'graph_store', self.store):
            async with AsyncClient(transport=ASGITransport(app=main.app), base_url='http://test') as client:
                response = await client.get('/documents', params={'q': 'paginationfixture', 'offset': 250, 'limit': 25})
                self.assertEqual(response.status_code, 200)
                body = response.json()
                self.assertEqual(body['scope'], 'indexed_documents')
                self.assertEqual(body['total'], 5105)
                self.assertEqual(body['results'][0]['properties']['paperless_id'], FIRST_ID + 250)
                response = await client.get('/graph/search', params={'q': 'NeedleUnique923', 'offset': 0})
                self.assertEqual(response.json()['total'], 1)
                invalid = await client.get('/documents', params={'limit': 201})
                self.assertEqual(invalid.status_code, 400)
                invalid = await client.get('/graph/search', params={'type': 'Document) DELETE n', 'q': 'query'})
                self.assertEqual(invalid.status_code, 400)


if __name__ == '__main__':
    unittest.main()
