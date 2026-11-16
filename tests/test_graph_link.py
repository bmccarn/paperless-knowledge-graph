"""Link evidence route contracts, with opt-in disposable Neo4j coverage."""
import asyncio
import json
import os
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
PREFIX = 'e1390000-0000-4000-8000-'
FIRST_ID = 913900000


async def get(store, source, rel_type, target):
    with patch.object(main, 'graph_store', store):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=main.app), base_url='http://test') as client:
            return await client.get('/graph/link', params={'source': source, 'type': rel_type, 'target': target})


class LinkRouteTests(unittest.IsolatedAsyncioTestCase):
    async def test_route_returns_the_store_records_and_404_for_no_link(self):
        store = AsyncMock()
        store.get_link.return_value = {'records': []}
        response = await get(store, 'a', 'COVERS', 'b')
        self.assertEqual((response.status_code, response.json()), (200, {'records': []}))
        store.get_link.assert_awaited_once_with('a', 'COVERS', 'b')
        store.get_link.return_value = None
        response = await get(store, 'b', 'COVERS', 'a')
        self.assertEqual((response.status_code, response.json()), (404, {'detail': 'Link not found'}))

    async def test_all_three_parameters_are_required(self):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=main.app), base_url='http://test') as client:
            for params in ({'source': 'a', 'type': 'COVERS'}, {'type': 'COVERS', 'target': 'b'}, {'source': 'a', 'target': 'b'}):
                with self.subTest(params=params):
                    self.assertEqual((await client.get('/graph/link', params=params)).status_code, 422)


@unittest.skipUnless(TEST_URI, 'Set NEO4J_TEST_URI to a disposable localhost Bolt URI')
class Neo4jLinkTests(unittest.IsolatedAsyncioTestCase):
    @classmethod
    def setUpClass(cls):
        if urlparse(TEST_URI).hostname not in {'localhost', '127.0.0.1'}:
            raise ValueError('Link tests require a disposable localhost database')

        async def seed():
            async with AsyncGraphDatabase.driver(TEST_URI, auth=None) as driver:
                async with driver.session() as session:
                    await (await session.run('''
                        CREATE (a:Contract {uuid: $p + 'a'}), (b:Product {uuid: $p + 'b'}),
                               (d:Document {paperless_id: $first, title: 'Link evidence note'}),
                               (a)-[:COVERS {support_records: $mixed, weight: 2}]->(b),
                               (b)-[:RELATED_TO {inferred: true}]->(a),
                               (a)-[:ISSUED_BY {source_doc: $first, confidence: 0.85, implied: false,
                                                rationale: 'Legacy rationale', evidence_spans: ['Legacy quote']}]->(d)
                    ''', p=PREFIX, first=FIRST_ID, mixed=[json.dumps(record) for record in [
                        {'source_doc': FIRST_ID + 1, 'inferred': True, 'confidence': 0.62,
                         'rationale': ['Names the policy.', 'Lists the car.'],
                         'evidence_json': json.dumps([{'start': 0, 'end': 9, 'quote': 'Renewal quote'}])},
                        {'source_doc': FIRST_ID, 'inferred': False, 'confidence': 0.94,
                         'evidence_spans': [{'start': 3, 'end': 9, 'quote': 'Stated quote'}, 'Second quote']}]])).consume()
        asyncio.run(seed())

    @classmethod
    def tearDownClass(cls):
        async def clean():
            async with AsyncGraphDatabase.driver(TEST_URI, auth=None) as driver:
                async with driver.session() as session:
                    await (await session.run('''
                        MATCH (n) WHERE n.uuid STARTS WITH $p OR n.paperless_id = $first DETACH DELETE n
                    ''', p=PREFIX, first=FIRST_ID)).consume()
        asyncio.run(clean())

    async def asyncSetUp(self):
        self.store = GraphStore()
        self.store.driver = AsyncGraphDatabase.driver(TEST_URI, auth=None)

    async def asyncTearDown(self):
        await self.store.close()

    async def test_records_with_quotes_and_rationale_in_document_order(self):
        response = await get(self.store, PREFIX + 'a', 'COVERS', PREFIX + 'b')
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), {'records': [
            {'doc_id': FIRST_ID, 'inferred': False, 'confidence': 0.94,
             'quotes': ['Stated quote', 'Second quote'], 'rationale': None},
            {'doc_id': FIRST_ID + 1, 'inferred': True, 'confidence': 0.62,
             'quotes': ['Renewal quote'], 'rationale': 'Names the policy. Lists the car.'}]})

    async def test_a_link_without_records_and_a_document_end(self):
        response = await get(self.store, PREFIX + 'b', 'RELATED_TO', PREFIX + 'a')
        self.assertEqual((response.status_code, response.json()), (200, {'records': []}))
        response = await get(self.store, PREFIX + 'a', 'ISSUED_BY', f'doc-{FIRST_ID}')
        self.assertEqual(response.json(), {'records': [
            {'doc_id': FIRST_ID, 'inferred': False, 'confidence': 0.85, 'quotes': ['Legacy quote'], 'rationale': 'Legacy rationale'}]})

    async def test_unknown_links_are_404(self):
        for source, rel_type, target in [(PREFIX + 'b', 'COVERS', PREFIX + 'a'), (PREFIX + 'a', 'EMPLOYED_BY', PREFIX + 'b'),
                                         (PREFIX + 'a', 'COVERS', PREFIX + 'missing'), (PREFIX + 'a', 'COVERS`]->() //', PREFIX + 'b')]:
            with self.subTest(key=(source, rel_type, target)):
                self.assertEqual((await get(self.store, source, rel_type, target)).status_code, 404)
