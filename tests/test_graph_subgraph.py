"""Links-among route contracts, with opt-in disposable Neo4j coverage."""
import asyncio
import json
import os
import time
import unittest
from unittest.mock import AsyncMock, patch
from urllib.parse import urlparse

import httpx
from neo4j import AsyncGraphDatabase

from tests.runtime import configure_test_environment
configure_test_environment()
from app import main
from app.graph import GraphStore

TEST_URI = os.environ.get('NEO4J_TEST_URI', '')
PREFIX = 'e1980000-0000-4000-8000-'
FIRST_ID = 958000000


async def get(store, ids):
    with patch.object(main, 'graph_store', store):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=main.app), base_url='http://test') as client:
            return await client.get('/graph/subgraph', params={'ids': ids})


class SubgraphRouteTests(unittest.IsolatedAsyncioTestCase):
    async def test_route_passes_entity_ids_to_store(self):
        fixture = {'links': [], 'documents': {}, 'total': 0, 'truncated': False}
        with patch.object(main.graph_store, 'get_subgraph', AsyncMock(return_value=fixture), create=True) as store:
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=main.app), base_url='http://test') as client:
                response = await client.get('/graph/subgraph', params={'ids': 'a,b'})
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), fixture)
        store.assert_awaited_once_with(['a', 'b'])

    async def test_id_bounds_and_empty_entries(self):
        for ids in ('', 'a', ',b', 'a,', 'a,,b', ','.join(str(i) for i in range(151))):
            with self.subTest(ids=ids):
                self.assertEqual((await get(None, ids)).status_code, 400)
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=main.app), base_url='http://test') as client:
            self.assertEqual((await client.get('/graph/subgraph')).status_code, 422)

    async def test_ids_are_trimmed_and_deduplicated_after_the_request_cap(self):
        store = AsyncMock()
        store.get_subgraph.return_value = {}
        await get(store, ' a, b,a ')
        store.get_subgraph.assert_awaited_once_with(['a', 'b'])
        self.assertEqual((await get(None, ','.join(['a'] * 151))).status_code, 400)


@unittest.skipUnless(TEST_URI, 'Set NEO4J_TEST_URI to a disposable localhost Bolt URI')
class Neo4jSubgraphTests(unittest.IsolatedAsyncioTestCase):
    @classmethod
    def setUpClass(cls):
        if urlparse(TEST_URI).hostname not in {'localhost', '127.0.0.1'}:
            raise ValueError('Subgraph tests require a disposable localhost database')

        async def seed():
            async with AsyncGraphDatabase.driver(TEST_URI, auth=None) as driver:
                async with driver.session() as session:
                    await (await session.run('''
                        CREATE (a:Person {uuid: $p + 'a'}), (b:Organization {uuid: $p + 'b'}),
                               (c:InsurancePolicy {uuid: $p + 'c'}), (x:Person {uuid: $p + 'outside'}),
                               (d:Document {uuid: $p + 'doc', paperless_id: $first, title: 'Subgraph statement',
                                            date: '2026-04-01', doc_type: 'work'}),
                               (:Document {paperless_id: $first + 1, title: 'Undated subgraph note'}),
                               (:Document {paperless_id: $first + 2, title: 'Capped source'}),
                               (a)-[:EMPLOYED_BY {support_records: $mixed, weight: 1}]->(b),
                               (b)-[:EMPLOYED_BY {source_doc: $first}]->(a),
                               (b)-[:ISSUED_BY {source_doc: $first, implied: true, weight: 999}]->(c),
                               (c)-[:RELATED_TO {inferred: true}]->(c),
                               (a)-[:MENTIONS]->(b), (b)-[:CONTAINS_RESULT]->(c),
                               (a)-[:SIGNED_BY]->(d), (d)-[:ISSUED_BY]->(c),
                               (x)-[:RELATED_TO]->(a)
                    ''', p=PREFIX, first=FIRST_ID, mixed=[json.dumps(record) for record in [
                        {'source_doc': FIRST_ID + 1, 'inferred': True, 'rationale': 'Excluded rationale'},
                        {'source_doc': FIRST_ID, 'inferred': False, 'confidence': 0.9,
                         'evidence_spans': ['Excluded quote ' * 300]}]])).consume()
                    await (await session.run('''
                        UNWIND range(0, 149) AS i
                        CREATE (:Organization {uuid: $p + right('000000000000' + toString(i), 12)})
                    ''', p=PREFIX)).consume()
                    await (await session.run('''
                        UNWIND range(0, 599) AS i
                        MATCH (a {uuid: $p + right('000000000000' + toString(toInteger(i / 149)), 12)}),
                              (b {uuid: $p + right('000000000000' + toString(i % 149 + 1), 12)})
                        CREATE (a)-[:RELATED_TO {source_doc: $first, evidence_spans: [$large]}]->(b)
                    ''', p=PREFIX, first=FIRST_ID, large='Excluded quote ' * 300)).consume()
        asyncio.run(seed())

    @classmethod
    def tearDownClass(cls):
        async def clean():
            async with AsyncGraphDatabase.driver(TEST_URI, auth=None) as driver:
                async with driver.session() as session:
                    await (await session.run('''
                        MATCH (n) WHERE n.uuid STARTS WITH $p OR n.paperless_id IN [$first, $first + 1, $first + 2]
                        DETACH DELETE n
                    ''', p=PREFIX, first=FIRST_ID)).consume()
        asyncio.run(clean())

    async def asyncSetUp(self):
        self.store = GraphStore()
        self.store.driver = AsyncGraphDatabase.driver(TEST_URI, auth=None)

    async def asyncTearDown(self):
        await self.store.close()

    async def test_only_links_among_entities_with_summary_fields_and_stable_order(self):
        ids = ','.join(PREFIX + suffix for suffix in ('c', 'b', 'a', 'doc', 'missing'))
        before = await self.store.get_node(PREFIX + 'a')
        response = await get(self.store, ids)
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertEqual(data['total'], 4)
        self.assertFalse(data['truncated'])
        keys = [link['key'] for link in data['links']]
        self.assertEqual(keys, [PREFIX + 'a~EMPLOYED_BY~' + PREFIX + 'b',
                                PREFIX + 'b~EMPLOYED_BY~' + PREFIX + 'a',
                                PREFIX + 'b~ISSUED_BY~' + PREFIX + 'c',
                                PREFIX + 'c~RELATED_TO~' + PREFIX + 'c'])
        self.assertEqual(data['links'][0], {
            'key': keys[0], 'source': PREFIX + 'a', 'target': PREFIX + 'b',
            'type': 'EMPLOYED_BY', 'family': 'parties', 'support_state': 'mixed',
            'stated_count': 1, 'inferred_count': 1,
            'records': [{'doc_id': FIRST_ID, 'inferred': False, 'confidence': 0.9},
                        {'doc_id': FIRST_ID + 1, 'inferred': True, 'confidence': None}]})
        self.assertEqual(data['links'][2]['support_state'], 'inferred')
        self.assertEqual(data['links'][2]['records'], [{'doc_id': FIRST_ID, 'inferred': True, 'confidence': None}])
        self.assertEqual(data['links'][3]['support_state'], 'inferred')
        self.assertEqual(data['links'][3]['records'], [])
        self.assertEqual(data['documents'], {
            str(FIRST_ID): {'title': 'Subgraph statement', 'date': '2026-04-01', 'doc_type': 'work'},
            str(FIRST_ID + 1): {'title': 'Undated subgraph note', 'date': None, 'doc_type': None}})
        self.assertNotIn('Excluded', response.text)
        self.assertEqual((await get(self.store, ','.join(reversed(ids.split(','))))).content, response.content)
        self.assertEqual(await self.store.get_node(PREFIX + 'a'), before)
        summary = await self.store.get_node_summary(PREFIX + 'a')
        self.assertIn(keys[0], [link['key'] for link in summary['links']])

    async def test_documents_and_unknown_ids_are_ignored(self):
        response = await get(self.store, PREFIX + 'doc,' + PREFIX + 'missing')
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), {'links': [], 'documents': {}, 'total': 0, 'truncated': False})

    async def test_150_entities_600_links_size_and_601_link_cap(self):
        ids = [PREFIX + f'{i:012d}' for i in range(150)]
        exact = await get(self.store, ','.join(ids))
        self.assertEqual(exact.status_code, 200)
        self.assertEqual((len(exact.json()['links']), exact.json()['total'], exact.json()['truncated']), (600, 600, False))
        self.assertLess(len(exact.content), 250_000)
        async with self.store.driver.session() as session:
            await (await session.run('''
                MATCH (a {uuid: $p + '000000000004'}), (b {uuid: $p + '000000000005'})
                CREATE (a)-[:RELATED_TO {source_doc: $doc}]->(b)
            ''', p=PREFIX, doc=FIRST_ID + 2)).consume()
        response = await get(self.store, ','.join(ids))
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertEqual((len(data['links']), data['total'], data['truncated']), (600, 601, True))
        self.assertEqual([link['key'] for link in data['links']], sorted(link['key'] for link in data['links']))
        self.assertEqual(set(data['documents']), {str(FIRST_ID)})
        self.assertNotIn('Excluded', response.text)
        self.assertLess(len(response.content), 250_000)
        started = time.perf_counter()
        self.assertEqual((await get(self.store, ','.join(ids))).content, response.content)
        print(f'150-entity capped subgraph: {len(response.content)} bytes, {time.perf_counter() - started:.3f}s warm request')
