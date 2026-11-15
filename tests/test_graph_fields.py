"""Derived graph fields: support state, owner rule, link counts and the documents sort."""
import asyncio
import json
import os
import unittest
from unittest.mock import patch
from urllib.parse import urlparse

from tests.runtime import configure_test_environment
configure_test_environment()
from app.graph import GraphStore
from app.graph_fields import link_support, owner_rule
from neo4j import AsyncGraphDatabase

TEST_URI = os.environ.get('NEO4J_TEST_URI', '')
PREFIX = 'graph-fields-test-'
FIRST_ID = 940000000


def record(doc, inferred):
    return json.dumps({'source_doc': doc, 'inferred': inferred})


class SupportStateTests(unittest.TestCase):
    def test_zero_records_use_the_link_flags(self):
        self.assertEqual(link_support({}), {'support_state': 'supported', 'stated_count': 0, 'inferred_count': 0})
        for flag in ('inferred', 'implied'):
            with self.subTest(flag=flag):
                self.assertEqual(link_support({flag: True})['support_state'], 'inferred')

    def test_record_mixes(self):
        cases = {
            'supported': ([record(1, False), record(2, False)], 2, 0),
            'inferred': ([record(1, True), record(2, True)], 0, 2),
            'mixed': ([record(1, False), record(2, True)], 1, 1),
        }
        for state, (records, stated, inferred) in cases.items():
            with self.subTest(state=state):
                # The stored link flag never overrides the records.
                self.assertEqual(link_support({'support_records': records, 'inferred': True}),
                                 {'support_state': state, 'stated_count': stated, 'inferred_count': inferred})


class OwnerRuleTests(unittest.TestCase):
    PERSONS = [
        {'uuid': 'a', 'name': 'Alex Example', 'aliases': ['Ａlex  E.', 'A. Example']},
        {'uuid': 'b', 'name': 'Blair Sample', 'aliases': None},
        {'uuid': 'c', 'name': 'Casey Twin', 'aliases': []},
        {'uuid': 'd', 'name': 'Dana Other', 'aliases': ['casey twin']},
    ]

    def test_empty_setting_is_unset(self):
        for setting in ('', '   ', None):
            with self.subTest(setting=setting):
                self.assertEqual(owner_rule(setting, self.PERSONS), ('unset', None))

    def test_one_match_by_alias_after_normalizing(self):
        self.assertEqual(owner_rule('  alex   e. ', self.PERSONS), ('found', 'a'))

    def test_name_and_alias_of_one_person_count_once(self):
        self.assertEqual(owner_rule('BLAIR SAMPLE', self.PERSONS), ('found', 'b'))

    def test_zero_matches(self):
        self.assertEqual(owner_rule('Nobody Known', self.PERSONS), ('not_found', None))

    def test_two_matches(self):
        self.assertEqual(owner_rule('Casey Twin', self.PERSONS), ('ambiguous', None))


@unittest.skipUnless(TEST_URI, 'Set NEO4J_TEST_URI to a disposable localhost Bolt URI')
class Neo4jGraphFieldTests(unittest.IsolatedAsyncioTestCase):
    @classmethod
    def setUpClass(cls):
        if urlparse(TEST_URI).hostname not in {'localhost', '127.0.0.1'}:
            raise ValueError('Graph field integration tests require a disposable localhost database')

        async def seed():
            async with AsyncGraphDatabase.driver(TEST_URI, auth=None) as driver:
                async with driver.session() as session:
                    await (await session.run('MATCH (n:GraphFieldsAudit) DETACH DELETE n')).consume()
                    # A busy entity: 120 mentioning documents and 5 other links.
                    await (await session.run('''
                        CREATE (busy:Organization:GraphFieldsAudit {uuid: $prefix + 'busy', name: 'Busy Fields Org'})
                        WITH busy
                        UNWIND range(0, 119) AS i
                        CREATE (:Document:GraphFieldsAudit {uuid: $prefix + 'doc-' + toString(i),
                                paperless_id: $first + i, title: 'fields document ' + toString(i)})-[:MENTIONS]->(busy)
                    ''', prefix=PREFIX, first=FIRST_ID)).consume()
                    await (await session.run('''
                        MATCH (busy:GraphFieldsAudit {uuid: $prefix + 'busy'})
                        UNWIND range(0, 4) AS i
                        CREATE (busy)-[:EMPLOYS]->(:Person:GraphFieldsAudit {uuid: $prefix + 'p' + toString(i),
                                                                            name: 'Fields Person ' + toString(i)})
                    ''', prefix=PREFIX)).consume()
                    # Stored document lists win over linked documents; ties order by name.
                    await (await session.run('''
                        CREATE (:Person:GraphFieldsAudit {uuid: $prefix + 'listed-b', name: 'Bravo Fields',
                                                          source_doc_ids: [1, 2, 3]}),
                               (:Person:GraphFieldsAudit {uuid: $prefix + 'listed-a', name: 'alpha Fields',
                                                          aliases: ['Fields Nickname'], source_doc_ids: [4, 5, 6]})
                    ''', prefix=PREFIX)).consume()
        asyncio.run(seed())

    @classmethod
    def tearDownClass(cls):
        async def clean():
            async with AsyncGraphDatabase.driver(TEST_URI, auth=None) as driver:
                async with driver.session() as session:
                    await (await session.run('MATCH (n:GraphFieldsAudit) DETACH DELETE n')).consume()
        asyncio.run(clean())

    async def asyncSetUp(self):
        self.store = GraphStore()
        self.store.driver = AsyncGraphDatabase.driver(TEST_URI, auth=None)

    async def asyncTearDown(self):
        await self.store.close()

    async def test_mentions_do_not_count_as_links(self):
        fields = await self.store.get_node_fields([PREFIX + 'busy', PREFIX + 'listed-a'])
        self.assertEqual(fields[PREFIX + 'busy'],
                         {'evidence_doc_count': 120, 'link_count': 5, 'is_hub': False, 'is_owner': False})
        self.assertEqual(fields[PREFIX + 'listed-a']['evidence_doc_count'], 3)

    async def test_owner_flag_follows_the_setting(self):
        with patch('app.graph.settings.owner_name', ' FIELDS  nickname '):
            status, owner = await self.store.get_owner()
            fields = await self.store.get_node_fields([PREFIX + 'listed-a'], owner)
        self.assertEqual((status, owner), ('found', PREFIX + 'listed-a'))
        self.assertTrue(fields[PREFIX + 'listed-a']['is_owner'])

    async def test_mixed_link_keeps_its_stored_flag(self):
        for doc, inferred in ((FIRST_ID, False), (FIRST_ID + 1, True)):
            # Two Persons, so the busy entity's link count stays at 5 in any test order.
            await self.store.create_relationship(PREFIX + 'p0', 'Person', PREFIX + 'p1', 'Person',
                                                 'SPONSORS', {'source_doc': doc, 'inferred': inferred})
        node = await self.store.get_node(PREFIX + 'p0')
        props = next(r['rel_props'] for r in node['relationships'] if r['rel_type'] == 'SPONSORS')
        self.assertTrue(props['inferred'])
        self.assertEqual(link_support(props),
                         {'support_state': 'mixed', 'stated_count': 1, 'inferred_count': 1})
        # Reading never writes: the stored flag is unchanged.
        node = await self.store.get_node(PREFIX + 'p0')
        props = next(r['rel_props'] for r in node['relationships'] if r['rel_type'] == 'SPONSORS')
        self.assertTrue(props['inferred'])

    async def test_documents_sort_lists_top_entities_without_documents(self):
        from app import main
        from httpx import ASGITransport, AsyncClient
        with patch.object(main, 'graph_store', self.store), patch('app.graph.settings.owner_name', 'Bravo Fields'):
            async with AsyncClient(transport=ASGITransport(app=main.app), base_url='http://test') as client:
                response = await client.get('/graph/search', params={
                    'q': '', 'type': 'GraphFieldsAudit', 'sort': 'documents', 'exclude_documents': 1, 'limit': 3})
                alias = await client.get('/graph/search', params={'q': 'nickname', 'type': 'GraphFieldsAudit'})
                untyped = await client.get('/graph/search', params={
                    'q': '', 'sort': 'documents', 'exclude_documents': 1, 'limit': 200})
        self.assertEqual(response.status_code, 200)
        untyped = untyped.json()['results']
        self.assertTrue(untyped)
        self.assertTrue(all('Document' not in row['labels'] for row in untyped))
        counts = [row['evidence_doc_count'] for row in untyped]
        self.assertEqual(counts, sorted(counts, reverse=True))
        rows = response.json()['results']
        self.assertEqual([row['properties']['uuid'] for row in rows],
                         [PREFIX + 'busy', PREFIX + 'listed-a', PREFIX + 'listed-b'])
        self.assertEqual([row['evidence_doc_count'] for row in rows], [120, 3, 3])
        self.assertEqual([row['is_owner'] for row in rows], [False, False, True])
        self.assertTrue(all('Document' not in row['labels'] for row in rows))
        self.assertEqual(alias.json()['results'][0]['matched_alias'], 'Fields Nickname')

    async def test_default_search_rows_keep_their_shape(self):
        rows = await self.store.search_nodes('Busy Fields', node_type='GraphFieldsAudit')
        self.assertEqual(set(rows[0]), {'labels', 'properties'})
