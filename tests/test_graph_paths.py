"""The paths endpoint: semantic links only, owner excluded in the search, ranking and the time budget."""
import asyncio
import json
import os
import time
import unittest
from unittest.mock import patch
from urllib.parse import urlparse

from tests.runtime import configure_test_environment
configure_test_environment()
from httpx import ASGITransport, AsyncClient
from neo4j import AsyncGraphDatabase

from app import main
from app.graph import GraphStore

TEST_URI = os.environ.get('NEO4J_TEST_URI', '')
P = 'graph-paths-test-'
OWNER = 'Paths Owner Example'


def records(*inferred):
    return [json.dumps({'inferred': flag, 'source_doc': 950000000 + i}, sort_keys=True)
            for i, flag in enumerate(inferred)]


async def get(store, owner=OWNER, **params):
    with patch.object(main, 'graph_store', store), patch('app.graph.settings.owner_name', owner):
        async with AsyncClient(transport=ASGITransport(app=main.app), base_url='http://test') as client:
            return await client.get('/graph/paths', params=params)


class PathRouteTests(unittest.IsolatedAsyncioTestCase):
    async def test_forced_timeout_returns_503(self):
        class SlowStore:
            async def get_owner_matches(self):
                return 'unset', None, []

            async def find_paths(self, *args, **kwargs):
                await asyncio.sleep(1)

        with patch.object(main, 'PATH_SEARCH_SECONDS', 0.01):
            response = await get(SlowStore(), a='x', b='y')
        self.assertEqual(response.status_code, 503)
        self.assertEqual(response.json(), {'detail': 'path_search_timeout'})

    async def test_parameters_are_bounded(self):
        for params in ({'a': 'x'}, {'a': 'x', 'b': 'y', 'max_len': 1}, {'a': 'x', 'b': 'y', 'max_len': 5},
                       {'a': 'x', 'b': 'y', 'k': 6}, {'a': 'x', 'b': 'y', 'k': 0}):
            with self.subTest(params=params):
                self.assertEqual((await get(None, **params)).status_code, 422)


@unittest.skipUnless(TEST_URI, 'Set NEO4J_TEST_URI to a disposable localhost Bolt URI')
class Neo4jPathTests(unittest.IsolatedAsyncioTestCase):
    @classmethod
    def setUpClass(cls):
        if urlparse(TEST_URI).hostname not in {'localhost', '127.0.0.1'}:
            raise ValueError('Path integration tests require a disposable localhost database')

        async def seed():
            async with AsyncGraphDatabase.driver(TEST_URI, auth=None) as driver:
                async with driver.session() as session:
                    await (await session.run('MATCH (n) WHERE n.uuid STARTS WITH $p DETACH DELETE n', p=P)).consume()
                    await (await session.run('''
                        CREATE (a:Person {uuid: $p + 'a', name: 'Path Start'}),
                               (b:Organization {uuid: $p + 'b', name: 'Path End'}),
                               (o:Person {uuid: $p + 'owner', name: $owner, source_doc_ids: [1, 2, 3]}),
                               (:Person {uuid: $p + 'owner-small', name: $owner, source_doc_ids: [1]}),
                               (om:Person {uuid: $p + 'owner-mid', aliases: [$owner], source_doc_ids: [1, 2]}),
                               (m:Organization {uuid: $p + 'm', name: 'Middle Bank'}),
                               (n:Organization {uuid: $p + 'n', name: 'Inferred Middle'}),
                               (x:InsurancePolicy {uuid: $p + 'x', name: 'Mixed Policy'}),
                               (d:Document {uuid: $p + 'doc', paperless_id: 950000001, title: 'Paths doc'}),
                               // Strongest route, but through the owner.
                               (a)-[:SIBLING_OF {weight: 9}]->(o)-[:CUSTOMER_OF {weight: 9}]->(b),
                               // Next strongest, through a Person that also matches the owner setting.
                               (a)-[:SIBLING_OF {weight: 8}]->(om)-[:CUSTOMER_OF {weight: 8}]->(b),
                               // Through a Document node, and over an evidence link: never returned.
                               (a)-[:SIGNED_BY {weight: 9}]->(d)-[:ISSUED_BY {weight: 9}]->(b),
                               (d)-[:MENTIONS {weight: 9}]->(a),
                               (a)-[:CONTAINS_RESULT {weight: 9}]->(b),
                               // Weakest link 2, all stated.
                               (a)-[:WORKS_AT {weight: 3, support_records: $stated2}]->(m)-[:PAID_TO {weight: 2, support_records: $stated2}]->(b),
                               // Weakest link 2, one link mixed: stated in one document, inferred in another.
                               (a)-[:INSURES {weight: 2, inferred: true, support_records: $mixed}]->(x)-[:COVERS {weight: 2, support_records: $stated2}]->(b),
                               // Weakest link 4, one link fully inferred.
                               (a)-[:RELATED_TO {weight: 4, inferred: true, support_records: $inferred4}]->(n)-[:ASSOCIATED_WITH {weight: 4}]->(b)
                    ''', p=P, owner=OWNER, stated2=records(False, False), mixed=records(False, True),
                        inferred4=records(True, True, True, True))).consume()
                    # A hub of 660 links on the way from hub-a to hub-b: hub-a - hub - leaf - shared - hub-b.
                    await (await session.run('''
                        CREATE (ha:Person {uuid: $p + 'hub-a', name: 'Hub Start'}),
                               (hub:Organization {uuid: $p + 'hub', name: 'Busy Hub'}),
                               (s:Address {uuid: $p + 'shared', name: 'Shared Place'}),
                               (hb:Organization {uuid: $p + 'hub-b', name: 'Hub End'}),
                               (ha)-[:MEMBER_OF {weight: 1}]->(hub), (s)-[:LOCATED_IN {weight: 1}]->(hb)
                        WITH hub, s
                        UNWIND range(1, 659) AS i
                        CREATE (hub)-[:EMPLOYS {weight: 1}]->(leaf:Person {uuid: $p + 'leaf-' + toString(i)})
                        CREATE (leaf)-[:LIVES_IN {weight: 1}]->(s)
                        CREATE (leaf)-[:WORKS_AT {weight: 1}]->(:Organization {uuid: $p + 'side-' + toString(i)})
                    ''', p=P)).consume()
        asyncio.run(seed())

    @classmethod
    def tearDownClass(cls):
        async def clean():
            async with AsyncGraphDatabase.driver(TEST_URI, auth=None) as driver:
                async with driver.session() as session:
                    await (await session.run('MATCH (n) WHERE n.uuid STARTS WITH $p DETACH DELETE n', p=P)).consume()
        asyncio.run(clean())

    async def asyncSetUp(self):
        self.store = GraphStore()
        self.store.driver = AsyncGraphDatabase.driver(TEST_URI, auth=None)

    async def asyncTearDown(self):
        await self.store.close()

    def node_paths(self, body):
        return [[node.removeprefix(P) for node in path['nodes']] for path in body['paths']]

    async def test_semantic_paths_ranked_by_weakest_link(self):
        response = await get(self.store, a=P + 'a', b=P + 'b')
        self.assertEqual(response.status_code, 200)
        body = response.json()
        # Weakest 4 first; the two weakest-2 paths tie on length and inferred count, so the link keys order them.
        self.assertEqual(self.node_paths(body), [['a', 'n', 'b'], ['a', 'x', 'b'], ['a', 'm', 'b']])
        self.assertEqual([(p['length'], p['weakest_weight'], p['has_inferred']) for p in body['paths']],
                         [(2, 4, True), (2, 2, False), (2, 2, False)])
        self.assertTrue(body['exhausted'])
        self.assertEqual(body['paths'][2]['links'], [f'{P}a~WORKS_AT~{P}m', f'{P}m~PAID_TO~{P}b'])
        self.assertEqual(body['links_detail'][f'{P}a~INSURES~{P}x'], {
            'source': P + 'a', 'type': 'INSURES', 'target': P + 'x', 'family': 'coverage', 'weight': 2,
            'support_state': 'mixed', 'stated_count': 1, 'inferred_count': 1})
        self.assertEqual(body['nodes'][P + 'm'], {
            'id': P + 'm', 'label': 'Organization', 'name': 'Middle Bank', 'evidence_doc_count': 0,
            'link_count': 2, 'is_hub': False, 'is_owner': False})
        self.assertEqual(set(body['nodes']), {P + n for n in ('a', 'b', 'm', 'n', 'x')})
        self.assertEqual(set(body['links_detail']), {key for path in body['paths'] for key in path['links']})

    async def test_no_document_node_or_evidence_link(self):
        body = (await get(self.store, a=P + 'a', b=P + 'b', include_owner=1)).json()
        self.assertTrue(body['paths'])
        self.assertTrue(all('doc' not in path for path in self.node_paths(body)))
        self.assertTrue(all(link['type'] not in {'MENTIONS', 'CONTAINS_RESULT'} for link in body['links_detail'].values()))

    async def test_owner_excluded_unless_included(self):
        with patch('app.graph.settings.owner_name', OWNER):
            status, owner, matches = await self.store.get_owner_matches()
            self.assertEqual((status, owner, len(matches)), ('chosen', P + 'owner', 3))
        default = (await get(self.store, a=P + 'a', b=P + 'b')).json()
        # The other Persons that match the owner setting are left out with it.
        self.assertTrue(all(not node.startswith('owner') for path in self.node_paths(default) for node in path))
        included = (await get(self.store, a=P + 'a', b=P + 'b', include_owner=1)).json()
        self.assertEqual(self.node_paths(included)[:2], [['a', 'owner', 'b'], ['a', 'owner-mid', 'b']])
        self.assertTrue(included['nodes'][P + 'owner']['is_owner'])
        # A match that is an endpoint is searched from; the owner is still left out.
        endpoint = (await get(self.store, a=P + 'owner-mid', b=P + 'b')).json()
        self.assertEqual(self.node_paths(endpoint)[0], ['owner-mid', 'b'])
        self.assertTrue(all('owner' not in path for path in self.node_paths(endpoint)))
        # With no owner configured, nothing is excluded.
        unset = (await get(self.store, owner='', a=P + 'a', b=P + 'b')).json()
        self.assertEqual(self.node_paths(unset)[0], ['a', 'owner', 'b'])

    async def test_no_path_still_names_both_ends(self):
        body = (await get(self.store, a=P + 'a', b=P + 'hub-b', max_len=2)).json()
        self.assertEqual(body['paths'], [])
        self.assertTrue(body['exhausted'])
        self.assertEqual({key: (node['label'], node['name']) for key, node in body['nodes'].items()},
                         {P + 'a': ('Person', 'Path Start'), P + 'hub-b': ('Organization', 'Hub End')})
        self.assertEqual(body['nodes'][P + 'a']['link_count'], 6)
        self.assertEqual(body['links_detail'], {})

    async def test_inferred_links_can_be_left_out(self):
        body = (await get(self.store, a=P + 'a', b=P + 'b', allow_inferred=0)).json()
        # The fully inferred link goes; the mixed link has a stated record and stays.
        self.assertEqual(self.node_paths(body), [['a', 'x', 'b'], ['a', 'm', 'b']])

    async def test_k_and_max_len_bound_the_result(self):
        body = (await get(self.store, a=P + 'a', b=P + 'b', include_owner=1, k=2, max_len=2)).json()
        self.assertEqual(len(body['paths']), 2)
        self.assertTrue(all(path['length'] <= 2 for path in body['paths']))

    async def test_search_past_a_660_link_hub_is_fast(self):
        started = time.monotonic()
        response = await get(self.store, a=P + 'hub-a', b=P + 'hub-b', max_len=4)
        elapsed = time.monotonic() - started
        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertTrue(body['nodes'][P + 'hub']['is_hub'])
        self.assertEqual(body['nodes'][P + 'hub']['link_count'], 660)
        self.assertEqual(len(body['paths']), 5)
        self.assertTrue(all(path['length'] == 4 and path['nodes'][1] == P + 'hub' for path in body['paths']))
        self.assertLess(elapsed, 5)

    async def test_cap_stops_the_search_before_it_is_exhausted(self):
        with patch('app.graph.PATH_CANDIDATES', 100):
            body = (await get(self.store, a=P + 'hub-a', b=P + 'hub-b', max_len=4)).json()
        self.assertFalse(body['exhausted'])
        self.assertEqual(len(body['paths']), 5)

    async def test_budget_stops_a_real_search(self):
        # No owner setting, so the path query is the first database call the budget can cut.
        with patch.object(main, 'PATH_SEARCH_SECONDS', 0.001):
            response = await get(self.store, owner='', a=P + 'hub-a', b=P + 'hub-b', max_len=4)
        self.assertEqual(response.json(), {'detail': 'path_search_timeout'})
        # The same store still answers afterwards.
        self.assertEqual((await get(self.store, a=P + 'a', b=P + 'b', max_len=2)).status_code, 200)


if __name__ == '__main__':
    unittest.main()
