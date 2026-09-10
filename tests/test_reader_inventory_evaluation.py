"""No runtime relevance classifier may be hidden behind a passing v6 grade."""
import copy
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, Mock, patch

from tests.runtime import configure_test_environment
configure_test_environment()
from scripts.conservative_query_admission import validate_case_grades, sha
from scripts.eval_question_pipeline import ROOT, execute, manifest_for
from tests.test_conservative_query_admission import fresh_grade_snapshot


def inventory_grades():
    snapshot, result, review = fresh_grade_snapshot(0)
    receipt = result['final']['finalization']['fact_conservation']
    receipt.pop('dispositions'); receipt.pop('reviews')
    receipt['version'] = 4
    snapshot['result.json'] = json.dumps(result).encode()
    snapshot['attempt-000-input.json'] = json.dumps({'index': 0, 'name': 'source_auditor'}).encode()
    snapshot['attempt-000-output.json'] = snapshot['attempt-000-input.json']
    for axis in ('spec', 'standards'):
        grade = json.loads(snapshot[f'grade-{axis}.json'])
        grade.update(result_sha256=sha(snapshot['result.json']), fact_filter_calls=0)
        snapshot[f'grade-{axis}.json'] = json.dumps(grade).encode()
        review['grade_sha256'][axis] = sha(snapshot[f'grade-{axis}.json'])
    review['fact_filter_calls'] = 0
    return snapshot, result, review


class ReaderInventoryGradeTests(unittest.TestCase):
    def test_zero_stage_count_requires_actual_bound_stage_evidence(self):
        snapshot, result, review = inventory_grades()
        validate_case_grades(snapshot, result, review)
        for name in ('fact_selector', 'fact_exclusion', ''):
            changed = copy.deepcopy(snapshot)
            changed['attempt-000-input.json'] = json.dumps({'index': 0, 'name': name}).encode()
            with self.assertRaisesRegex(ValueError, 'stage evidence'):
                validate_case_grades(changed, result, review)
        for name in ('fact_exclusion', 'fact_selector', 'other_stage', ''):
            changed = copy.deepcopy(snapshot)
            changed['attempt-000-output.json'] = json.dumps({'index': 0, 'name': name}).encode()
            with self.assertRaisesRegex(ValueError, 'stage evidence'):
                validate_case_grades(changed, result, review)
        changed = copy.deepcopy(snapshot)
        changed.pop('attempt-000-output.json')
        with self.assertRaisesRegex(ValueError, 'stage evidence'):
            validate_case_grades(changed, result, review)
        snapshot.pop('attempt-000-input.json')
        with self.assertRaisesRegex(ValueError, 'stage evidence'):
            validate_case_grades(snapshot, result, review)

    def test_missing_counts_and_legacy_exclusion_shortcuts_do_not_qualify(self):
        snapshot, result, review = inventory_grades()
        for value in (None, True, 1):
            changed = {**review, 'fact_filter_calls': value}
            with self.assertRaisesRegex(ValueError, 'counts'):
                validate_case_grades(snapshot, result, changed)
        for field in ('dispositions', 'reviews'):
            changed = copy.deepcopy(result)
            changed['final']['finalization']['fact_conservation'][field] = []
            with self.assertRaisesRegex(ValueError, 'semantic exclusion'):
                validate_case_grades(snapshot, changed, review)
        grade = json.loads(snapshot['grade-spec.json']); grade.pop('fact_filter_calls')
        snapshot['grade-spec.json'] = json.dumps(grade).encode()
        review['grade_sha256']['spec'] = sha(snapshot['grade-spec.json'])
        with self.assertRaisesRegex(ValueError, 'counts'):
            validate_case_grades(snapshot, result, review)


class ReaderInventoryDispatchTests(unittest.IsolatedAsyncioTestCase):
    async def test_forbidden_stages_are_captured_but_never_dispatched_to_model(self):
        dataset = ROOT / 'evals/reliability/question-development-r2.json'
        for name in ('fact_selector', 'fact_exclusion'):
            with self.subTest(name=name), tempfile.TemporaryDirectory() as directory:
                native = AsyncMock(return_value='must not run')
                adapter = SimpleNamespace(enabled=True, _text_agent=native, _model=Mock(), close=AsyncMock())
                async def plan(*args, **kwargs):
                    return await adapter._text_agent(name, 'diagnostic', '{}')
                adapter.plan_query = plan
                with patch('scripts.eval_question_pipeline.runtime_snapshot', return_value={
                        'packages': {'strands-agents': '1.55.0'}, 'model': 'synthetic'}), patch(
                        'app.strands_orchestrator.StrandsQueryOrchestrator', return_value=adapter), patch(
                        'app.strands_orchestrator.Agent', type('FakeAgent', (), {})):
                    manifest = manifest_for(dataset)
                    self.assertEqual(manifest['fact_filtering'], 'disabled')
                    returned = await execute(dataset, manifest, Path(directory), 0)
                self.assertEqual(returned['error'], 'ValueError')
                native.assert_not_awaited(); adapter.close.assert_awaited_once()
                case = Path(directory) / 'case-00'
                attempted = json.loads((case / 'attempt-000-input.json').read_bytes())
                failed = json.loads((case / 'attempt-000-output.json').read_bytes())
                self.assertEqual(attempted['name'], name)
                self.assertEqual(failed['exception_type'], 'ValueError')
                self.assertNotIn('native_result', failed)
