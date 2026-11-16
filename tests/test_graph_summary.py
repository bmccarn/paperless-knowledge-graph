"""Node summary contracts, with opt-in disposable Neo4j coverage."""
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
PREFIX = 'e1d8b0c1-209e-4a35-917c-'
FIRST_ID = 950000000


class SummaryRouteTests(unittest.IsolatedAsyncioTestCase):
    async def test_summary_selects_the_additive_mode(self):
        with patch.object(main.graph_store, 'get_node_summary', AsyncMock(return_value={'focus': {}}), create=True) as summary, \
             patch.object(main.graph_store, 'get_node', AsyncMock()) as full:
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=main.app), base_url='http://test') as client:
                response = await client.get('/graph/node/example?summary=1')
        self.assertEqual(response.json(), {'focus': {}})
        summary.assert_awaited_once_with('example')
        full.assert_not_awaited()

    async def test_default_response_bytes_are_unchanged(self):
        fixture = {'labels': ['Person'], 'properties': {'uuid': 'example', 'name': 'Alex Example'},
                   'relationships': [{'rel_type': 'RELATED_TO', 'direction': 'out',
                                      'rel_props': {'source_doc': 42, 'evidence_spans': ['Original quote']},
                                      'neighbor_labels': ['Organization'],
                                      'neighbor_props': {'uuid': 'other', 'name': 'Example Co'}}]}
        expected = json.dumps(fixture, ensure_ascii=False, separators=(',', ':')).encode()
        with patch.object(main.graph_store, 'get_node', AsyncMock(return_value=fixture)):
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=main.app), base_url='http://test') as client:
                for suffix in ('', '?summary=0'):
                    response = await client.get('/graph/node/example' + suffix)
                    self.assertEqual(response.content, expected)

    async def test_unknown_id_is_404_in_both_modes(self):
        with patch.object(main.graph_store, 'get_node', AsyncMock(return_value=None)), \
             patch.object(main.graph_store, 'get_node_summary', AsyncMock(return_value=None), create=True):
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=main.app), base_url='http://test') as client:
                for suffix in ('', '?summary=1'):
                    response = await client.get('/graph/node/unknown' + suffix)
                    self.assertEqual(response.status_code, 404)


@unittest.skipUnless(TEST_URI, 'Set NEO4J_TEST_URI to a disposable localhost Bolt URI')
class Neo4jSummaryTests(unittest.IsolatedAsyncioTestCase):
    @classmethod
    def setUpClass(cls):
        if urlparse(TEST_URI).hostname not in {'localhost', '127.0.0.1'}:
            raise ValueError('Summary tests require a disposable localhost database')

        async def seed():
            async with AsyncGraphDatabase.driver(TEST_URI, auth=None) as driver:
                async with driver.session() as session:
                    await (await session.run('''
                        CREATE (f:Organization {uuid: $prefix + '000000000660', name: 'Summary Co',
                                aliases: ['Example alias'], source_doc_ids: [$first, $first + 1]}),
                               (:Person {uuid: $prefix + '000000000661', name: 'Leaf Example'}),
                               (d:Document {paperless_id: $first, title: 'Summary statement',
                                                        date: '2026-04-01', doc_type: 'financial_invoice'}),
                               (:Document {paperless_id: $first + 1, title: 'Summary note',
                                                        date: '2026-05-01', doc_type: 'other'}),
                               (d)-[:MENTIONS]->(f)
                        WITH f
                        UNWIND range(0, 659) AS i
                        CREATE (p:Person {uuid: $prefix + right('000000000000' + toString(i), 12), name: 'Person ' + toString(i),
                                                       source_doc_ids: [$first], private_field: $large}),
                               (f)-[:EMPLOYS {support_records: CASE WHEN i = 0 THEN $records ELSE [$records[0]] END, rationale: $large}]->(p)
                    ''', prefix=PREFIX, first=FIRST_ID, large='Excluded content ' * 300,
                        records=[json.dumps({'source_doc': FIRST_ID, 'inferred': False, 'confidence': 0.9,
                                             'evidence_spans': ['Excluded quote ' * 300]}),
                                 json.dumps({'source_doc': FIRST_ID + 1, 'inferred': True,
                                             'rationale': 'Excluded rationale ' * 300})])).consume()
        asyncio.run(seed())

    @classmethod
    def tearDownClass(cls):
        async def clean():
            async with AsyncGraphDatabase.driver(TEST_URI, auth=None) as driver:
                async with driver.session() as session:
                    await (await session.run('MATCH (n) WHERE n.uuid STARTS WITH $prefix OR n.paperless_id IN [$first, $first + 1] DETACH DELETE n', prefix=PREFIX, first=FIRST_ID)).consume()
        asyncio.run(clean())

    async def test_parsed_dates_keep_raw_values_and_reject_unparseable_values(self):
        async with AsyncGraphDatabase.driver(TEST_URI, auth=None) as driver:
            async with driver.session() as session:
                try:
                    for raw, expected in [('April 1, 2026', '2026-04-01'), ('1 APR 26', '2026-04-01'),
                                          ('2026-04-01', '2026-04-01'), ('2026-04-01T09:30:00', '2026-04-01'),
                                          ('2026-04', '2026-04'), ('2026-02-30', None),
                                          ('undated', None), (None, None)]:
                        await (await session.run('MATCH (d:Document {paperless_id: $id}) SET d.date = $raw',
                                                id=FIRST_ID, raw=raw)).consume()
                        summary = await self.store.get_node_summary(PREFIX + '000000000660')
                        self.assertEqual(summary['documents'][str(FIRST_ID)]['date'], raw)
                        self.assertEqual(summary['documents'][str(FIRST_ID)]['parsed_date'], expected)
                finally:
                    await (await session.run("MATCH (d:Document {paperless_id: $id}) SET d.date = '2026-04-01'",
                                            id=FIRST_ID)).consume()

    async def asyncSetUp(self):
        self.store = GraphStore()
        self.store.driver = AsyncGraphDatabase.driver(TEST_URI, auth=None)
        self.patches = [patch.object(main, 'graph_store', self.store),
                        patch('app.graph.settings.owner_name', 'Person 0')]
        for item in self.patches:
            item.start()
        self.client = httpx.AsyncClient(transport=httpx.ASGITransport(app=main.app), base_url='http://test')

    async def asyncTearDown(self):
        await self.client.aclose()
        for item in reversed(self.patches):
            item.stop()
        await self.store.close()

    async def test_hub_summary_is_small_complete_stable_and_read_only(self):
        url = '/graph/node/' + PREFIX + '000000000660'
        before = await self.client.get(url)
        response = await self.client.get(url + '?summary=1')
        self.assertEqual(response.status_code, 200)
        summary = response.json()
        self.assertTrue('focus' in summary, 'summary focus is missing')
        self.assertLess(len(response.content), 300_000)
        links = summary['links']
        self.assertEqual(len(links), 661)  # 660 semantic links and one document mention.
        keys = [link['key'] for link in links]
        self.assertEqual(keys, sorted(set(keys)))
        self.assertEqual((await self.client.get(url + '?summary=1')).content, response.content)
        self.assertEqual((await self.client.get(url)).content, before.content)
        self.assertEqual(summary['focus'], {'id': PREFIX + '000000000660', 'label': 'Organization', 'name': 'Summary Co',
                         'aliases': ['Example alias'], 'evidence_doc_count': 2, 'link_count': 660,
                         'is_hub': True, 'is_owner': False})
        self.assertEqual(summary['owner_status'], 'found')
        self.assertEqual(summary['owner_match_count'], 1)
        self.assertEqual(summary['owner_duplicates'], [])
        employs = next(link for link in links if link['neighbor']['id'] == PREFIX + '000000000000')
        self.assertEqual(employs, {
            'key': PREFIX + '000000000660~EMPLOYS~' + PREFIX + '000000000000', 'type': 'EMPLOYS', 'family': 'parties',
            'direction': 'out', 'support_state': 'mixed', 'stated_count': 1, 'inferred_count': 1,
            'records': [{'doc_id': FIRST_ID, 'inferred': False, 'confidence': 0.9},
                        {'doc_id': FIRST_ID + 1, 'inferred': True, 'confidence': None}],
            'neighbor': {'id': PREFIX + '000000000000', 'label': 'Person', 'name': 'Person 0',
                         'evidence_doc_count': 1, 'link_count': 1, 'is_owner': True}})
        self.assertEqual(summary['documents'], {
            str(FIRST_ID): {'title': 'Summary statement', 'date': '2026-04-01', 'parsed_date': '2026-04-01', 'doc_type': 'financial_invoice'},
            str(FIRST_ID + 1): {'title': 'Summary note', 'date': '2026-05-01', 'parsed_date': '2026-05-01', 'doc_type': 'other'}})
        self.assertNotIn('Excluded', response.text)
        print(f'660-link summary plus one mention: {len(response.content)} bytes')

    async def test_incoming_key_matches_outgoing_and_document_neighbor_has_fields(self):
        incoming = (await self.client.get('/graph/node/' + PREFIX + '000000000000?summary=1')).json()
        outgoing = (await self.client.get('/graph/node/' + PREFIX + '000000000660?summary=1')).json()
        self.assertTrue('links' in incoming, 'summary links are missing')
        self.assertEqual(incoming['links'][0]['key'], next(link['key'] for link in outgoing['links']
                         if link['neighbor']['id'] == PREFIX + '000000000000'))
        self.assertEqual(incoming['links'][0]['direction'], 'in')
        mention = next(link for link in outgoing['links'] if link['type'] == 'MENTIONS')
        self.assertEqual(mention['neighbor'], {'id': f'doc-{FIRST_ID}', 'label': 'Document',
                         'name': 'Summary statement', 'evidence_doc_count': 0, 'link_count': 0, 'is_owner': False})
        self.assertEqual(mention['direction'], 'in')
        document = (await self.client.get(f'/graph/node/doc-{FIRST_ID}?summary=1')).json()
        self.assertEqual(document['focus']['id'], f'doc-{FIRST_ID}')
        self.assertEqual(document['focus']['link_count'], 0)
        self.assertIn(str(FIRST_ID), document['documents'])

    async def test_self_loop_appears_once_with_legacy_support(self):
        async with self.store.driver.session() as session:
            await (await session.run('''
                MATCH (n {uuid: $uuid}) CREATE (n)-[:RELATED_TO {source_doc: $doc, implied: true}]->(n)
            ''', uuid=PREFIX + '000000000661', doc=FIRST_ID)).consume()
        try:
            response = await self.client.get('/graph/node/' + PREFIX + '000000000661?summary=1')
            links = response.json()['links']
            self.assertEqual(len(links), 1)
            self.assertEqual(links[0]['support_state'], 'inferred')
            self.assertEqual(links[0]['records'], [{'doc_id': FIRST_ID, 'inferred': True, 'confidence': None}])
        finally:
            async with self.store.driver.session() as session:
                await (await session.run('''
                    MATCH (n {uuid: $uuid})-[r:RELATED_TO]->(n) DELETE r
                ''', uuid=PREFIX + '000000000661')).consume()

    async def test_owner_status_and_count_for_unset_and_missing_settings(self):
        for setting, status in [('', 'unset'), ('No matching person', 'not_found')]:
            with self.subTest(status=status), patch('app.graph.settings.owner_name', setting):
                response = await self.client.get('/graph/node/' + PREFIX + '000000000660?summary=1')
                self.assertEqual(response.status_code, 200)
                summary = response.json()
                self.assertEqual(summary['owner_status'], status)
                self.assertEqual(summary['owner_match_count'], 0)
                self.assertFalse(summary['focus']['is_owner'])
                self.assertFalse(any(link['neighbor']['is_owner'] for link in summary['links']))

    async def test_leaf_and_unknown_id(self):
        response = await self.client.get('/graph/node/' + PREFIX + '000000000661?summary=1')
        self.assertEqual(response.status_code, 200)
        summary = response.json()
        self.assertEqual(summary.get('links'), [])
        self.assertEqual(summary['documents'], {})
        response = await self.client.get('/graph/node/' + PREFIX + 'missing?summary=1')
        self.assertEqual(response.status_code, 404)
