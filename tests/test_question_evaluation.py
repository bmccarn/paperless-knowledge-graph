"""Native evaluation admission controls; no model calls."""
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from tests.runtime import configure_test_environment
configure_test_environment()
from scripts.eval_question_pipeline import manifest_for, previous_runs, digest, execute, ROOT


class EvaluationAdmissionTests(unittest.IsolatedAsyncioTestCase):
    def runtime(self):
        return {'packages': {'strands-agents': '1.55.0'}, 'model': 'synthetic'}

    async def test_changed_frozen_manifest_fails_before_output_or_model(self):
        dataset = ROOT / 'evals/reliability/question-development.json'
        with tempfile.TemporaryDirectory() as directory, patch(
                'scripts.eval_question_pipeline.runtime_snapshot', side_effect=self.runtime):
            manifest = manifest_for(dataset)
            manifest['max_native_calls'] += 1
            with self.assertRaisesRegex(ValueError, 'Frozen'):
                await execute(dataset, manifest, Path(directory), 0)
            self.assertEqual(list(Path(directory).iterdir()), [])

    def test_previous_case_requires_both_reviews_bound_to_exact_result(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory); case = root / 'case-00'; case.mkdir()
            payload = json.dumps({'native_call_count': 7, 'elapsed_seconds': 9, 'error': None}).encode()
            (case / 'result.json').write_bytes(payload)
            for review in ({}, {'spec': 'pass', 'standards': 'fail', 'result_sha256': digest(payload)},
                           {'spec': 'pass', 'standards': 'pass', 'result_sha256': 'wrong'}):
                (case / 'review.json').write_text(json.dumps(review))
                with self.assertRaisesRegex(ValueError, 'review'):
                    previous_runs(root, 1)
            (case / 'review.json').write_text(json.dumps({'spec': 'pass', 'standards': 'pass',
                                                        'result_sha256': digest(payload)}))
            self.assertEqual(previous_runs(root, 1), (7, 9))
            (case / 'result.json').write_bytes(payload + b' ')
            with self.assertRaisesRegex(ValueError, 'review'):
                previous_runs(root, 1)
