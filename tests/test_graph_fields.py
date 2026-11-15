"""Derived graph fields: support state, owner rule, link counts and the documents sort."""
import asyncio
import json
import os
import unittest
from itertools import permutations
from pathlib import Path
from unittest.mock import patch
from urllib.parse import urlparse

from tests.runtime import configure_test_environment
configure_test_environment()
from app.graph import GraphStore
from app.graph_fields import link_support, owner_matches
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


def owner_rule(setting, persons, aliases=''):
    """(status, owner uuid, match count), the shape these tests assert."""
    status, owner, matches = owner_matches(setting, persons, aliases)
    return status, owner, len(matches)


async def get_owner(store):
    status, owner, matches = await store.get_owner_matches()
    return status, owner, len(matches)


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
                self.assertEqual(owner_rule(setting, self.PERSONS), ('unset', None, 0))

    def test_one_match_by_alias_after_normalizing(self):
        self.assertEqual(owner_rule('  alex   e. ', self.PERSONS), ('found', 'a', 1))

    def test_name_and_alias_of_one_person_count_once(self):
        self.assertEqual(owner_rule('BLAIR SAMPLE', self.PERSONS), ('found', 'b', 1))

    def test_zero_matches(self):
        self.assertEqual(owner_rule('Nobody Known', self.PERSONS), ('not_found', None, 0))

    def test_ranking_uses_documents_then_links_then_uuid_in_any_order(self):
        cases = [
            ([3, 5, 4], [9, 1, 8], 'b'),
            ([5, 5, 4], [1, 2, 9], 'b'),
            ([5, 5, 5], [2, 2, 2], 'a'),
        ]
        for documents, links, expected in cases:
            persons = [dict(uuid=uuid, name='Owner Example', aliases=['Owner Example'],
                            evidence_doc_count=docs, link_count=count)
                       for uuid, docs, count in zip('abc', documents, links)]
            for order in permutations(persons):
                with self.subTest(documents=documents, links=links, order=[p['uuid'] for p in order]):
                    self.assertEqual(owner_rule('Owner Example', order), ('chosen', expected, 3))

    def test_two_matches(self):
        self.assertEqual(owner_rule('Casey Twin', self.PERSONS), ('chosen', 'c', 2))

    def test_matches_list_the_owner_first_then_its_possible_duplicates(self):
        persons = [dict(uuid=uuid, name='Owner Example', evidence_doc_count=docs) for uuid, docs in zip('abc', [2, 9, 5])]
        status, owner, matches = owner_matches('owner example', [*persons, *self.PERSONS])
        self.assertEqual((status, owner, [p['uuid'] for p in matches]), ('chosen', 'b', ['b', 'c', 'a']))
        self.assertEqual(owner_matches('Nobody Known', self.PERSONS), ('not_found', None, []))
        self.assertEqual(owner_matches('', self.PERSONS), ('unset', None, []))


class OwnerAliasTests(unittest.TestCase):
    ALIASES = 'Rowan T Vale; Vale, Rowan T'
    PERSONS = [
        {'uuid': 'a', 'name': 'Rowan Vale', 'evidence_doc_count': 2},
        {'uuid': 'b', 'name': 'Rowan T Vale', 'evidence_doc_count': 9},
        {'uuid': 'c', 'name': 'Someone Else', 'aliases': ['VALE,  rowan t'], 'evidence_doc_count': 5},
        {'uuid': 'd', 'name': 'Vale', 'aliases': ['Rowan T'], 'evidence_doc_count': 50},
    ]

    def test_every_owner_name_matches_and_the_existing_order_chooses(self):
        status, owner, matches = owner_matches('Rowan Vale', self.PERSONS, self.ALIASES)
        self.assertEqual((status, owner, [p['uuid'] for p in matches]), ('chosen', 'b', ['b', 'c', 'a']))

    def test_empty_aliases_change_nothing(self):
        for aliases in ('', ' ; ;  ', None):
            with self.subTest(aliases=aliases):
                self.assertEqual(owner_matches('Rowan Vale', self.PERSONS, aliases),
                                 owner_matches('Rowan Vale', self.PERSONS))
                self.assertEqual(owner_rule('Rowan Vale', self.PERSONS, aliases), ('found', 'a', 1))

    def test_empty_entries_and_surrounding_spaces_are_ignored(self):
        self.assertEqual(owner_rule('Rowan Vale', self.PERSONS, ' ;  Rowan T Vale  ;;\t; '), ('chosen', 'b', 2))

    def test_aliases_need_an_owner_name(self):
        self.assertEqual(owner_rule('', self.PERSONS, self.ALIASES), ('unset', None, 0))

    def test_sample_environments_list_the_setting(self):
        root = Path(__file__).resolve().parents[1]
        for path in (root / '.env.example', root / 'examples/kg-local.env.example'):
            with self.subTest(path=path.name):
                self.assertIn('OWNER_ALIASES=', path.read_text().splitlines())


@unittest.skipUnless(TEST_URI, 'Set NEO4J_TEST_URI to a disposable localhost Bolt URI')
class Neo4jGraphFieldTests(unittest.IsolatedAsyncioTestCase):
    @classmethod
    def setUpClass(cls):
        if urlparse(TEST_URI).hostname not in {'localhost', '127.0.0.1'}:
            raise ValueError('Graph field integration tests require a disposable localhost database')

        async def seed():
            async with AsyncGraphDatabase.driver(TEST_URI, auth=None) as driver:
                async with driver.session() as session:
                    await (await session.run('MATCH (n) WHERE n:GraphFieldsAudit OR n:OwnerRuleAudit DETACH DELETE n')).consume()
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
                    await (await session.run('''
                        CREATE (a:Person:OwnerRuleAudit {uuid: $prefix + 'owner-a', name: 'Fields Owner Example',
                                                          source_doc_ids: [1]}),
                               (b:Person:OwnerRuleAudit {uuid: $prefix + 'owner-b', name: 'Fields Owner Example',
                                                          source_doc_ids: [1, 2, 3]}),
                               (c:Person:OwnerRuleAudit {uuid: $prefix + 'owner-c', name: 'Fields Owner Example',
                                                          source_doc_ids: []}),
                               (a)-[:RELATED_TO]->(b), (a)-[:WORKS_AT]->(b), (a)-[:SIBLING_OF]->(c)
                        WITH c
                        UNWIND range(0, 3) AS i
                        CREATE (d:Document:OwnerRuleAudit {uuid: $prefix + 'owner-doc-' + toString(i),
                                                            paperless_id: $first + 200 + i})
                        CREATE (d)-[:MENTIONS]->(c), (d)-[:SIGNED_BY]->(c)
                    ''', prefix=PREFIX, first=FIRST_ID)).consume()
        asyncio.run(seed())

    @classmethod
    def tearDownClass(cls):
        async def clean():
            async with AsyncGraphDatabase.driver(TEST_URI, auth=None) as driver:
                async with driver.session() as session:
                    await (await session.run('MATCH (n) WHERE n:GraphFieldsAudit OR n:OwnerRuleAudit DETACH DELETE n')).consume()
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
            status, owner, count = await get_owner(self.store)
            fields = await self.store.get_node_fields([PREFIX + 'listed-a'], owner)
        self.assertEqual((status, owner, count), ('found', PREFIX + 'listed-a', 1))
        self.assertTrue(fields[PREFIX + 'listed-a']['is_owner'])

    async def test_owner_aliases_follow_the_setting(self):
        with patch('app.graph.settings.owner_name', 'Nobody Known'), \
                patch('app.graph.settings.owner_aliases', 'Someone Else; fields nickname'):
            self.assertEqual(await get_owner(self.store), ('found', PREFIX + 'listed-a', 1))

    async def test_duplicate_owner_agrees_in_search_and_summary_without_writes(self):
        from app import main
        from httpx import ASGITransport, AsyncClient
        ids = [PREFIX + 'owner-' + suffix for suffix in 'abc']
        before = [await self.store.get_node(uuid) for uuid in ids]
        with patch.object(main, 'graph_store', self.store), patch('app.graph.settings.owner_name', 'Fields Owner Example'):
            self.assertEqual(await get_owner(self.store), ('chosen', ids[2], 3))
            async with AsyncClient(transport=ASGITransport(app=main.app), base_url='http://test') as client:
                response = await client.get('/graph/search', params={
                    'q': 'Fields Owner Example', 'type': 'OwnerRuleAudit', 'exclude_documents': 1})
                self.assertEqual(response.status_code, 200)
                rows = response.json()['results']
                self.assertEqual({row['properties']['uuid']: row['is_owner'] for row in rows},
                                 dict(zip(ids, [False, False, True])))
                # The matches the rule did not choose are flagged, so lists can leave them out with the owner.
                self.assertEqual({row['properties']['uuid']: row['owner_match'] for row in rows},
                                 dict(zip(ids, [True, True, False])))
                for uuid in ids:
                    response = await client.get('/graph/node/' + uuid + '?summary=1')
                    self.assertEqual(response.status_code, 200)
                    summary = response.json()
                    self.assertEqual(summary['owner_status'], 'chosen')
                    self.assertEqual(summary['owner_match_count'], 3)
                    self.assertEqual(summary['focus']['is_owner'], uuid == ids[2])
                    for link in summary['links']:
                        self.assertEqual(link['neighbor']['is_owner'], link['neighbor']['id'] == ids[2])
                    # Only the owner's summary lists the other matches, in rank order, linked or not.
                    self.assertEqual(summary['owner_duplicates'], [
                        {'id': ids[1], 'label': 'Person', 'name': 'Fields Owner Example', 'evidence_doc_count': 3},
                        {'id': ids[0], 'label': 'Person', 'name': 'Fields Owner Example', 'evidence_doc_count': 1},
                    ] if uuid == ids[2] else [])
        self.assertEqual([await self.store.get_node(uuid) for uuid in ids], before)

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
        self.assertEqual([row['owner_match'] for row in rows], [False, False, False])
        self.assertTrue(all('Document' not in row['labels'] for row in rows))
        self.assertEqual(alias.json()['results'][0]['matched_alias'], 'Fields Nickname')

    async def test_default_search_rows_keep_their_shape(self):
        rows = await self.store.search_nodes('Busy Fields', node_type='GraphFieldsAudit')
        self.assertEqual(set(rows[0]), {'labels', 'properties'})
