"""Synthetic public Steward runs: legacy history is not an update target."""
import ast
import copy
from pathlib import Path
from types import SimpleNamespace, ModuleType
import unittest
from unittest.mock import AsyncMock


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

    graph = SimpleNamespace(get_entity_review_candidates=get_candidates)
    agent = SimpleNamespace(review_entity_candidate=AsyncMock(return_value=dict(recommendation='merge', confidence=.99, risk='low')))
    path = Path(__file__).resolve().parents[1] / 'app/entity_steward.py'
    tree = ast.parse(path.read_text(), str(path))
    tree.body = [n for n in tree.body if not (isinstance(n, ast.ImportFrom) and (n.module or '').startswith('app.'))]
    module = ModuleType('isolated_steward_history')
    module.__dict__.update(settings=SimpleNamespace(entity_steward_candidate_limit=40), embeddings_store=ledger,
                           graph_store=graph, strands_orchestrator=agent)
    exec(compile(tree, str(path), 'exec'), module.__dict__)
    return module.EntitySteward(), ledger, agent


class StewardHistoryTests(unittest.IsolatedAsyncioTestCase):
    async def test_legacy_suggestions_are_not_rewritten_or_promoted(self):
        for decision in ('suggest_merge', 'suggest_split', 'suggest_review'):
            for provenance in ('legacy_unknown', None):
                with self.subTest(decision=decision, provenance=provenance):
                    rows = [dict(id=1, left_uuid='left', right_uuid='right', decision=decision,
                                 note='original history', created_at='original timestamp')]
                    if provenance is not None:
                        rows[0]['provenance'] = provenance
                    steward, ledger, agent = load_steward(rows)
                    result = await steward.run_once(reason='post-sync')
                    self.assertEqual(ledger.rows, rows)
                    self.assertEqual(ledger.writes, [])
                    self.assertEqual(result['reviewed_count'], 0)
                    agent.review_entity_candidate.assert_not_awaited()
                    self.assertFalse(steward._running)

    async def test_new_and_provenanced_automated_suggestions_still_run(self):
        for rows in ([], [dict(left_uuid='left', right_uuid='right', decision='suggest_merge',
                               note='prior automation', created_at='before', provenance='automated_suggestion')]):
            steward, ledger, agent = load_steward(rows)
            result = await steward.run_once(reason='post-sync')
            self.assertEqual(result['reviewed_count'], 1)
            self.assertEqual(len(ledger.writes), 1)
            self.assertEqual(ledger.rows[0]['provenance'], 'automated_suggestion')
            agent.review_entity_candidate.assert_awaited_once()

    async def test_terminal_review_still_excluded(self):
        rows = [dict(left_uuid='left', right_uuid='right', decision='split', provenance='human_review')]
        steward, ledger, agent = load_steward(rows)
        result = await steward.run_once()
        self.assertEqual(result['reviewed_count'], 0)
        self.assertEqual(ledger.rows, rows)
        agent.review_entity_candidate.assert_not_awaited()


if __name__ == '__main__':
    unittest.main()
