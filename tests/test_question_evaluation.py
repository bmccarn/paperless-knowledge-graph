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

    @patch('app.answer_coverage.restore_question_coverage', return_value={'complete': True})
    def test_previous_case_requires_both_reviews_bound_to_exact_result(self, _restore):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory); case = root / 'case-00'; case.mkdir()
            payload = json.dumps({'native_call_count': 7, 'elapsed_seconds': 9, 'error': None, 'case_index': 0, 'case_id': 'case', 'manifest_sha256': digest(b'{}'),
                                  'final': {'finalization': {'complete': True}}}).encode()
            (case / 'result.json').write_bytes(payload)
            (case / 'manifest.json').write_text('{}')
            (case / 'case.json').write_text('{"id":"case"}')
            for review in ({}, {'spec': 'pass', 'standards': 'fail', 'result_sha256': digest(payload)},
                           {'spec': 'pass', 'standards': 'pass', 'result_sha256': 'wrong'}):
                (case / 'review.json').write_text(json.dumps(review))
                with self.assertRaisesRegex(ValueError, 'review'):
                    previous_runs(root, 1, {}, [{'id': 'case'}])
            (case / 'review.json').write_text(json.dumps({'spec': 'pass', 'standards': 'pass',
                                                        'result_sha256': digest(payload)}))
            self.assertEqual(previous_runs(root, 1, {}, [{'id': 'case'}]), (7, 9))
            for recorded in ({'old_manifest': True},):
                (case / 'manifest.json').write_text(json.dumps(recorded))
                with self.assertRaisesRegex(ValueError, 'review'):
                    previous_runs(root, 1, {}, [{'id': 'case'}])
            (case / 'manifest.json').write_text('{}')
            with self.assertRaisesRegex(ValueError, 'review'):
                previous_runs(root, 1, {}, [{'id': 'different-case'}])
            (case / 'result.json').write_bytes(payload + b' ')
            with self.assertRaisesRegex(ValueError, 'review'):
                previous_runs(root, 1, {}, [{'id': 'case'}])

    async def test_execute_reads_dataset_once_before_any_native_work(self):
        dataset = ROOT / 'evals/reliability/question-development.json'
        frozen = dataset.read_bytes()
        class Dataset:
            calls = 0
            def read_bytes(self):
                self.calls += 1
                if self.calls > 1:
                    raise AssertionError('Dataset re-read after admission')
                return frozen
        source = Dataset()
        with tempfile.TemporaryDirectory() as directory, patch(
                'scripts.eval_question_pipeline.runtime_snapshot', side_effect=self.runtime):
            manifest = manifest_for(dataset)
            with self.assertRaises(FileNotFoundError):
                await execute(source, manifest, Path(directory), 1)
            self.assertEqual(source.calls, 1)
