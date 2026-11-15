"""Compact archive overview contracts, using synthetic graph data only."""
import json
import os
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

import httpx

from tests.runtime import configure_test_environment
configure_test_environment()
from app import main
from app.graph import GraphStore


def node(identifier, links=1, label='Organization', name='Example entity', documents=1, document_links=0):
    return dict(id=identifier, label=label, name=name, evidence_doc_count=documents,
                link_count=links, entity_links=links, document_links=document_links)


def link(source='a', target='b', kind='EMPLOYED_BY', **props):
    return dict(source=source, target=target, type=kind, props=props)


class OverviewTests(unittest.IsolatedAsyncioTestCase):
    async def request(self, nodes, links, owner=None):
        store = GraphStore()
        result_nodes, result_links = MagicMock(), MagicMock()
        result_nodes.__aiter__.return_value = nodes
        result_links.__aiter__.return_value = links
        session = AsyncMock()
        session.run.side_effect = [result_nodes, result_links]
        store.driver = MagicMock()
        store.driver.session.return_value.__aenter__.return_value = session
        with patch.object(main, 'graph_store', store), \
             patch.object(store, 'get_owner_matches', AsyncMock(return_value=('found', owner, []))):
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=main.app), base_url='http://test') as client:
                response = await client.get('/graph/overview')
        self.assertEqual(response.status_code, 200, response.text)
        return response, session

    async def test_merges_types_directions_documents_and_families(self):
        nodes = [node('b'), node('a', links=2, name='X' * 80)]
        links = [link(source='b', target='a', source_doc_ids=[1, 2]),
                 link(kind='COVERS', source_doc_ids=[2, 3], inferred=True,
                      support_records=[json.dumps({'source_doc': 2, 'inferred': True}),
                                       json.dumps({'source_doc': 3, 'inferred': True})])]
        response, _ = await self.request(nodes, links, owner='b')
        body = response.json()
        mask = (1 << 0) | (1 << 2)
        self.assertEqual(body['links'], [[0, 1, 3, 1, mask]])
        self.assertEqual(body['nodes'], [['a', 'Organization', 'X' * 60, 1, 2],
                                         ['b', 'Organization', 'Example entity', 1, 1]])
        self.assertEqual(body['owner'], 1)
        self.assertEqual(body['columns'], ['id', 'label', 'name', 'evidence_doc_count', 'link_count'])
        self.assertEqual(body['link_columns'], ['source', 'target', 'weight', 'support', 'families'])
        self.assertFalse(body['truncated'])
        repeated, _ = await self.request(list(reversed(nodes)), list(reversed(links)), owner='b')
        self.assertEqual(response.content, repeated.content)
        changed, _ = await self.request(nodes, links + [link(kind='PAID_TO', source_doc=4)])
        self.assertNotEqual(body['version'], changed.json()['version'])

    async def test_unlinked_owner_empty_and_support_codes(self):
        nodes = [node('a'), node('b'), node('c'), node('d', links=0, document_links=1),
                 node('isolated', links=0)]
        links = [link(source_doc=1), link(kind='RELATED_TO', inferred=True),
                 link('b', 'c', 'PAID_TO', source_doc=2, implied=True),
                 link('c', 'a', 'RELATED_TO', inferred=True)]
        body = (await self.request(nodes, links, owner='d'))[0].json()
        self.assertEqual(body['unlinked'], 1)
        self.assertIsNone(body['owner'])
        self.assertEqual([row[3] for row in body['links']], [1, 2, 2])
        self.assertEqual(body['links'][1], [0, 2, 0, 2, 1 << 8])
        other = (await self.request(nodes, [link(kind='ATTENDED', source_doc=1)]))[0].json()
        self.assertEqual(other['links'][0][4], 1 << 9)
        empty = (await self.request([], []))[0].json()
        self.assertEqual(empty['nodes'], [])
        self.assertEqual(empty['links'], [])
        self.assertEqual(empty['unlinked'], 0)
        self.assertIsNone(empty['owner'])

    async def test_cap_keeps_most_linked_then_id(self):
        nodes = [node(f'{i:05}', links=1) for i in range(8001)]
        nodes[-1]['link_count'] = 10
        body = (await self.request(nodes, [link('00000', '08000', source_doc=1)]))[0].json()
        self.assertTrue(body['truncated'])
        self.assertEqual(len(body['nodes']), 8000)
        ids = [row[0] for row in body['nodes']]
        self.assertEqual(ids, sorted(ids))
        self.assertNotIn('07999', ids)
        self.assertIn('08000', ids)
        self.assertEqual(len(body['links']), 1)

    async def test_archive_size_budget(self):
        nodes = [node(f'00000000-0000-0000-0000-{i:012}', name='Synthetic archive entity ' + 'x' * 60)
                 for i in range(6250)]
        links = [link(nodes[i]['id'], nodes[i + 1]['id'], source_doc=i + 1)
                 for i in range(0, 6250, 2)]
        response, _ = await self.request(nodes, links)
        self.assertLess(len(response.content), 1_500_000)
        self.assertEqual(len(response.json()['links']), 3125)
        print(f'6,250-node overview: {len(response.content)} bytes')


TEST_URI = os.environ.get('NEO4J_TEST_URI', '')
PREFIX = 'overview-202-'


@unittest.skipUnless(TEST_URI, 'Set NEO4J_TEST_URI to a disposable localhost Bolt URI')
class Neo4jOverviewTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        from urllib.parse import urlparse
        from neo4j import AsyncGraphDatabase
        if urlparse(TEST_URI).hostname not in {'localhost', '127.0.0.1'}:
            raise ValueError('Overview tests require a disposable localhost database')
        self.store = GraphStore()
        self.store.driver = AsyncGraphDatabase.driver(TEST_URI, auth=None)
        async with self.store.driver.session() as session:
            await (await session.run('''
                UNWIND range(0, 6249) AS i
                CREATE (:Organization {uuid: $prefix + right('00000' + toString(i), 5),
                        name: $name, source_doc_ids: [i + 1]})
            ''', prefix=PREFIX, name='Synthetic entity ' + 'x' * 80)).consume()
            await (await session.run('''
                UNWIND range(0, 3124) AS i
                MATCH (a {uuid: $prefix + right('00000' + toString(2*i), 5)}),
                      (b {uuid: $prefix + right('00000' + toString(2*i+1), 5)})
                CREATE (a)-[:EMPLOYED_BY {source_doc: i + 1}]->(b)
            ''', prefix=PREFIX)).consume()
            await (await session.run('''
                MATCH (a {uuid: $prefix + '00000'}), (b {uuid: $prefix + '00001'}),
                      (c {uuid: $prefix + '00002'})
                SET a:Person, a.name = 'Synthetic linked owner'
                CREATE (b)-[:COVERS {support_records: $records}]->(a),
                       (a)-[:MENTIONS {source_doc: 99}]->(c),
                       (a)-[:CONTAINS_RESULT {source_doc: 99}]->(c),
                       (d:Document {uuid: $prefix + 'doc', paperless_id: 952020001, title: 'Synthetic document'}),
                       (d)-[:RELATED_TO {source_doc: 99}]->(a),
                       (d)-[:MENTIONS]->(a),
                       (u:Person {uuid: $prefix + 'unlinked', name: 'Synthetic owner'}),
                       (d)-[:MENTIONS]->(u),
                       (:Person {uuid: $prefix + 'isolated', name: 'Isolated entity'}),
                       (s:Person {uuid: $prefix + 'self-only', name: 'Self-linked entity'}),
                       (s)-[:RELATED_TO {inferred: true}]->(s),
                       (a)-[:RELATED_TO {inferred: true}]->(a)
            ''', prefix=PREFIX, records=[json.dumps({'source_doc': 1, 'inferred': True}),
                                        json.dumps({'source_doc': 2, 'inferred': True})])).consume()
        self.client = httpx.AsyncClient(transport=httpx.ASGITransport(app=main.app), base_url='http://test')
        self.patches = [patch.object(main, 'graph_store', self.store), patch('app.graph.settings.owner_name', '')]
        for item in self.patches:
            item.start()

    async def asyncTearDown(self):
        await self.client.aclose()
        for item in reversed(self.patches):
            item.stop()
        async with self.store.driver.session() as session:
            await (await session.run('MATCH (n) WHERE n.uuid STARTS WITH $prefix DETACH DELETE n', prefix=PREFIX)).consume()
        await self.store.close()

    async def snapshot(self):
        async with self.store.driver.session() as session:
            nodes = await (await session.run('MATCH (n) WHERE n.uuid STARTS WITH $prefix RETURN n.uuid, labels(n), properties(n) ORDER BY n.uuid', prefix=PREFIX)).data()
            links = await (await session.run('MATCH (a)-[r]->(b) WHERE a.uuid STARTS WITH $prefix RETURN a.uuid, type(r), b.uuid, properties(r) ORDER BY a.uuid, type(r), b.uuid', prefix=PREFIX)).data()
        return nodes, links

    async def test_real_queries_size_exclusion_stability_and_read_only(self):
        import time
        before = await self.snapshot()
        started = time.monotonic()
        response = await self.client.get('/graph/overview')
        elapsed = time.monotonic() - started
        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertEqual(len(body['nodes']), 6250)
        self.assertEqual(len(body['links']), 3125)
        self.assertTrue(all(row[1] != 'Document' for row in body['nodes']))
        self.assertTrue(all(len(row[2]) <= 60 for row in body['nodes']))
        self.assertEqual(body['links'][0], [0, 1, 2, 1, 5])
        self.assertEqual(body['unlinked'], 1)
        self.assertIsNone(body['owner'])
        self.assertFalse(body['truncated'])
        self.assertLess(len(response.content), 1_500_000)
        self.assertEqual((await self.client.get('/graph/overview')).content, response.content)
        self.assertEqual(await self.snapshot(), before)
        with patch('app.graph.settings.owner_name', 'Synthetic linked owner'):
            self.assertEqual((await self.client.get('/graph/overview')).json()['owner'], 0)
        with patch('app.graph.settings.owner_name', 'Synthetic owner'):
            self.assertIsNone((await self.client.get('/graph/overview')).json()['owner'])
        print(f'Real Neo4j 6,250-node overview: {len(response.content)} bytes, {elapsed:.3f}s (first request)')

    async def test_real_cap_omits_edges_to_cut_nodes(self):
        async with self.store.driver.session() as session:
            await (await session.run("""
                MATCH (a {uuid: $prefix + '00000'})
                UNWIND range(0, 1751) AS i
                CREATE (n:Organization {uuid: $prefix + 'cap-' + right('0000' + toString(i), 4), name: 'Cap entity'}),
                       (a)-[:EMPLOYED_BY {source_doc: 1}]->(n)
            """, prefix=PREFIX)).consume()
        body = (await self.client.get('/graph/overview')).json()
        self.assertTrue(body['truncated'])
        self.assertEqual(len(body['nodes']), 8000)
        self.assertEqual(len(body['links']), 3125 + 1750)
        ids = [row[0] for row in body['nodes']]
        self.assertEqual(ids, sorted(ids))
        self.assertEqual(ids[-1], PREFIX + 'cap-1749')
        self.assertTrue(all(row[0] != row[1] for row in body['links']))
        self.assertTrue(all(0 <= row[0] < 8000 and 0 <= row[1] < 8000 for row in body['links']))
