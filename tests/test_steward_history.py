"""Synthetic public Steward runs: legacy history is not an update target."""
import ast
import copy
from pathlib import Path
from types import SimpleNamespace, ModuleType
import unittest
from unittest.mock import AsyncMock

from app.entity_policy import IDENTIFIER_PROPERTIES, extract_identifiers


def load_steward(rows):
    ledger = SimpleNamespace(rows=copy.deepcopy(rows), writes=[])

    async def get_decisions():
        return copy.deepcopy(ledger.rows)

    async def add(left, right, decision, note, **kwargs):
        ledger.writes.append((left, right, decision))
        row = next((r for r in ledger.rows if (r['left_uuid'], r['right_uuid'], r['decision']) == (left, right, decision)), None)
        if row:
            row.update(note=note, created_at='rewritten', **kwargs)
        else:
            ledger.rows.append(dict(left_uuid=left, right_uuid=right, decision=decision, note=note, **kwargs))
        return row

    ledger.get_entity_review_decisions = get_decisions
    ledger.add_entity_review_decision = add
    candidates = [dict(left=dict(uuid='left', name='Cobalt Tools Inc', properties={}),
                       right=dict(uuid='right', name='Cobalt Tools LLC', properties={}), label='Organization', score=100)]

    async def get_candidates(ignored, limit):
        return [copy.deepcopy(c) for c in candidates if tuple(sorted((c['left']['uuid'], c['right']['uuid']))) not in ignored][:limit]

    async def get_entities_by_type(label):
        return [dict(node['properties'], uuid=node['uuid'], name=node['name']) for c in candidates
                if c['label'] == label for node in (c['left'], c['right'])]

    graph = SimpleNamespace(get_entity_review_candidates=get_candidates, get_entities_by_type=get_entities_by_type)
    agent = SimpleNamespace(review_entity_candidate=AsyncMock(return_value=dict(recommendation='merge', confidence=.99, risk='low')))
    path = Path(__file__).resolve().parents[1] / 'app/entity_steward.py'
    tree = ast.parse(path.read_text(), str(path))
    tree.body = [n for n in tree.body if not (isinstance(n, ast.ImportFrom) and (n.module or '').startswith('app.') and n.module != 'app.entity_policy')]
    module = ModuleType('isolated_steward_history')
    module.__dict__.update(settings=SimpleNamespace(entity_steward_candidate_limit=40, entity_steward_auto_merge='shadow',
                                                    entity_steward_auto_merge_max_per_run=10),
                           embeddings_store=ledger, graph_store=graph, strands_orchestrator=agent,
                           entity_resolver=SimpleNamespace(), EntityMergeProhibited=ValueError, AutoMergeBlocked=ValueError)
    exec(compile(tree, str(path), 'exec'), module.__dict__)
    return module, ledger, agent


class StewardHistoryTests(unittest.IsolatedAsyncioTestCase):
    async def test_legacy_suggestions_are_not_rewritten_or_promoted(self):
        for decision in ('suggest_merge', 'suggest_split', 'suggest_review'):
            for provenance in ('legacy_unknown', None):
                with self.subTest(decision=decision, provenance=provenance):
                    rows = [dict(id=1, left_uuid='left', right_uuid='right', decision=decision,
                                 note='original history', created_at='original timestamp')]
                    if provenance is not None:
                        rows[0]['provenance'] = provenance
                    module, ledger, agent = load_steward(rows)
                    steward = module.EntitySteward()
                    result = await steward.run_once(reason='post-sync')
                    self.assertEqual(ledger.rows, rows)
                    self.assertEqual(ledger.writes, [])
                    self.assertEqual(result['reviewed_count'], 0)
                    agent.review_entity_candidate.assert_not_awaited()
                    self.assertFalse(steward._running)

    async def test_new_and_provenanced_automated_suggestions_still_run(self):
        for rows in ([], [dict(left_uuid='left', right_uuid='right', decision='suggest_merge',
                               note='prior automation', created_at='before', provenance='automated_suggestion')]):
            module, ledger, agent = load_steward(rows)
            steward = module.EntitySteward()
            result = await steward.run_once(reason='post-sync')
            self.assertEqual(result['reviewed_count'], 1)
            self.assertEqual(len(ledger.writes), 1)
            self.assertEqual(ledger.rows[0]['provenance'], 'automated_suggestion')
            agent.review_entity_candidate.assert_awaited_once()

    async def test_terminal_review_still_excluded(self):
        rows = [dict(left_uuid='left', right_uuid='right', decision='split', provenance='human_review')]
        module, ledger, agent = load_steward(rows)
        steward = module.EntitySteward()
        result = await steward.run_once()
        self.assertEqual(result['reviewed_count'], 0)
        self.assertEqual(ledger.rows, rows)
        agent.review_entity_candidate.assert_not_awaited()


class StewardScoringTests(unittest.TestCase):
    def setUp(self):
        self.module, _, _ = load_steward([])

    def candidate(self, left_name, right_name, properties=None, label='Organization'):
        return dict(left=dict(name=left_name, properties=properties or {}),
                    right=dict(name=right_name, properties=properties or {}), label=label, score=100)

    def test_bookkeeping_properties_are_not_shared_identifiers(self):
        candidate = self.candidate('Widget 2000', 'Widget 3000', {
            'resolution_policy': 'evidence-identity-v2', 'source_doc_ids': [1234],
            'uuid': 'node-1234', 'identity_id': 'mention-1234', 'review_id': 'review-1234',
            'account_notes': 'same note', 'phone_verified': True,
        }, label='Product')
        result = self.module.score_candidate(candidate)
        self.assertEqual(result['shared_identifiers'], [])
        self.assertEqual(result['recommendation'], 'review')

    def test_ingestion_identifier_map_normalizes_values_and_skips_empty_values(self):
        self.assertEqual(IDENTIFIER_PROPERTIES, {'identity_hints', 'policy_number', 'reference_number'})
        self.assertEqual(extract_identifiers({
            'identity_hints': ['Owner@Example.test', ' owner@example.test ', None, {}, True, '---'],
            'policy_number': ' POL 1234 ', 'reference_number': 1234,
            'resolution_policy': 'evidence-identity-v2', 'source_doc_ids': [1234],
        }), {'identity_hints': ['owner@example.test'], 'policy_number': ['pol1234'],
             'reference_number': ['1234']})
        self.assertEqual(extract_identifiers({'policy_number': None, 'reference_number': ''}), {})

    def test_identifier_matches_need_the_same_property_and_value(self):
        candidate = self.candidate('Example Inc', 'Example LLC', {
            'identity_hints': ['Owner@Example.test'], 'policy_number': 'POL 1234',
            'reference_number': 'INV 5678',
        })
        self.assertEqual(self.module.score_candidate(candidate)['shared_identifiers'],
                         ['identity_hints:owner@example.test', 'policy_number:pol1234', 'reference_number:inv5678'])
        candidate['right']['properties'] = {'policy_number': 'OTHER 1234', 'reference_number': 'POL 1234'}
        self.assertEqual(self.module.score_candidate(candidate)['shared_identifiers'], [])

    def test_exact_names_follow_typed_identity_policy(self):
        for label, left, right, expected in [
            ('Organization', 'Example Inc', 'Example LLC', False),
            ('Organization', 'Example, Inc.', 'Example Incorporated', True),
            ('Person', 'Smith, Jane', 'Jane Smith', True),
            ('Organization', 'Café LLC', 'Cafe LLC', False),
        ]:
            with self.subTest(label=label, left=left, right=right):
                result = self.module.score_candidate(self.candidate(left, right, label=label))
                self.assertEqual(result['exact_name'], expected)

    def test_unusable_agent_review_never_falls_back_to_merge(self):
        deterministic = dict(score=.9, risk='low', recommendation='merge')
        self.assertTrue(self.module.should_ask_agent(deterministic))
        for review in (None, {}, ['merge'], 'merge', True, 1, {'unrelated': 'value'}, {'recommendation': 'merge'}):
            with self.subTest(review=review):
                self.assertEqual(self.module.choose_recommendation(deterministic, review), 'review')

    def test_unrequested_agent_review_keeps_deterministic_result(self):
        for score, risk, recommendation in ((.98, 'low', 'merge'), (.4, 'medium', 'split')):
            deterministic = dict(score=score, risk=risk, recommendation=recommendation)
            self.assertFalse(self.module.should_ask_agent(deterministic))
            self.assertEqual(self.module.choose_recommendation(deterministic, None), recommendation)


class StewardFailedReviewTests(unittest.IsolatedAsyncioTestCase):
    async def test_failed_agent_review_is_persisted_as_suggest_review(self):
        for review in (None, {}, ['merge']):
            module, ledger, agent = load_steward([])
            agent.review_entity_candidate.return_value = review
            report = await module.EntitySteward().run_once()
            agent.review_entity_candidate.assert_awaited_once()
            self.assertEqual(ledger.writes[0][2], 'suggest_review')
            self.assertEqual(report['suggest_review'], 1)
            self.assertEqual(report['suggest_merge'], 0)


if __name__ == '__main__':
    unittest.main()
