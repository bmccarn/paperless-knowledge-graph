"""Evaluation controls fail closed before production readers or models attach."""
import asyncio
import copy
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import AsyncMock, patch

from scripts.live_query_evaluation import (
    EvaluationRoutes, SingleRequest, UnscheduledRequest, reviewed_snapshot, sha256,
    admit_all_modes, FAILURE_COUNTS,
)


class GradeBindingTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.result = b'{"retained":"result"}'
        self.grade = {'verdict': 'pass', 'result_sha256': sha256(self.result),
                      'coverage_underreported_aspects': 2,
                      **dict.fromkeys(FAILURE_COUNTS, 0)}
        encoded = json.dumps(self.grade).encode()
        self.review = {'result_sha256': sha256(self.result), 'spec': 'pass',
                       'standards': 'pass', 'reviewers': ['reader-one', 'reader-two'],
                       'grade_sha256': dict.fromkeys(('spec', 'standards'), sha256(encoded)),
                       'coverage_underreported_aspects': 2,
                       **dict.fromkeys(FAILURE_COUNTS, 0)}
        for axis in ('spec', 'standards'):
            (self.root / f'grade-{axis}.json').write_bytes(encoded)

    def snapshot(self):
        return {'result.json': self.result, 'review.json': json.dumps(self.review).encode()}

    def test_conservative_coverage_pass_retains_both_exact_grade_files(self):
        captured = reviewed_snapshot(self.root, self.snapshot())
        self.assertIn('grade-spec.json', captured)
        self.assertIn('grade-standards.json', captured)
        self.assertEqual(captured['result.json'], self.result)

    def test_missing_or_changed_grade_and_changed_result_reject(self):
        path = self.root / 'grade-spec.json'
        original = path.read_bytes()
        path.unlink()
        with self.assertRaises(FileNotFoundError): reviewed_snapshot(self.root, self.snapshot())
        path.write_bytes(original + b' ')
        with self.assertRaises(ValueError): reviewed_snapshot(self.root, self.snapshot())
        path.write_bytes(original)
        self.result += b' '
        with self.assertRaises(ValueError): reviewed_snapshot(self.root, self.snapshot())

    def test_semantic_failure_or_disagreement_cannot_be_hidden_by_rehashing(self):
        for key in (*FAILURE_COUNTS, 'coverage_underreported_aspects'):
            with self.subTest(key=key):
                grade = {**self.grade, key: 1}
                encoded = json.dumps(grade).encode()
                (self.root / 'grade-spec.json').write_bytes(encoded)
                self.review['grade_sha256']['spec'] = sha256(encoded)
                with self.assertRaises(ValueError): reviewed_snapshot(self.root, self.snapshot())

    def test_same_reviewer_twice_does_not_count_as_two_reviews(self):
        self.review['reviewers'] = ['same', 'same']
        with self.assertRaises(ValueError): reviewed_snapshot(self.root, self.snapshot())


class RequestBoundaryTests(unittest.IsolatedAsyncioTestCase):
    async def test_wrong_question_history_mode_or_model_never_starts_operation(self):
        expected = {'question': 'What changed?', 'mode': 'deep', 'history': [], 'model': 'model-a'}
        gate = SingleRequest(**expected)
        operation = AsyncMock(return_value='native result')
        for key, value in [('question', 'Other'), ('mode', 'quick'), ('model', 'model-b'),
                           ('history', [{'role': 'assistant', 'content': 'Invented fact'}])]:
            with self.assertRaises(UnscheduledRequest):
                await gate.invoke(operation, **{**expected, key: value})
        operation.assert_not_awaited()
        self.assertEqual(await gate.invoke(operation, **expected), 'native result')
        operation.assert_awaited_once()

    async def test_concurrent_duplicate_and_failed_attempt_cannot_run_twice(self):
        expected = {'question': 'Question', 'mode': 'strict', 'history': [], 'model': None}
        gate = SingleRequest(**expected)
        operation = AsyncMock(side_effect=RuntimeError('retained failure'))
        outcomes = await asyncio.gather(*(gate.invoke(operation, **expected) for _ in range(2)),
                                        return_exceptions=True)
        self.assertIsInstance(outcomes[0], RuntimeError)
        self.assertIsInstance(outcomes[1], UnscheduledRequest)
        operation.assert_awaited_once()

    async def test_route_allowlist_prevents_registered_mutations_and_title_model(self):
        serving = AsyncMock()
        title = AsyncMock()
        guard = EvaluationRoutes(serving, adapters={('POST', '/generate-title'): title})
        receive, send = AsyncMock(), AsyncMock()
        base = {'type': 'http', 'server': ('127.0.0.1', 8080)}
        for method, path in [('POST', '/sync'), ('POST', '/reindex'), ('POST', '/review'),
                             ('POST', '/query'), ('GET', '/query/stream'),
                             ('POST', '/generate-title/'), ('GET', '/documents/999')]:
            await guard({**base, 'method': method, 'path': path}, receive, send)
            self.assertEqual(send.await_args_list[-2].args[0]['status'], 403)
        serving.assert_not_awaited()
        await guard({**base, 'method': 'POST', 'path': '/generate-title'}, receive, send)
        title.assert_awaited_once()
        serving.assert_not_awaited()
        await guard({**base, 'method': 'POST', 'path': '/query/stream'}, receive, send)
        serving.assert_awaited_once()

    async def test_non_loopback_or_startup_never_reaches_real_application(self):
        serving = AsyncMock(); guard = EvaluationRoutes(serving, adapters={})
        send = AsyncMock()
        await guard({'type': 'http', 'server': ('0.0.0.0', 8080), 'method': 'POST',
                     'path': '/query/stream'}, AsyncMock(), send)
        self.assertEqual(send.await_args_list[0].args[0]['status'], 403)
        with self.assertRaises(ValueError):
            await guard({'type': 'lifespan'}, AsyncMock(), send)
        serving.assert_not_awaited()
        with self.assertRaises(ValueError):
            EvaluationRoutes(serving, adapters={('POST', '/sync'): AsyncMock()})


class PrerequisiteTests(unittest.TestCase):
    def test_different_candidate_rejected_before_reading_case_or_opening_clients(self):
        # The existing harness owns full code/runtime policy validation; this
        # boundary must compare its complete expected manifest before case reads.
        from tests.runtime import configure_test_environment
        configure_test_environment()
        from scripts.eval_question_pipeline import ROOT
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            expected = {'code_sha256': {'app/query.py': 'original'}, 'runtime': {'model': 'one'}}
            for key in ('code_sha256', 'runtime'):
                changed = copy.deepcopy(expected); changed[key] = {'changed': True}
                (root / 'manifest.json').write_text(json.dumps(changed))
                with patch('scripts.eval_question_pipeline.manifest_for', return_value=expected), patch(
                        'scripts.eval_question_pipeline.read_run') as read:
                    with self.assertRaisesRegex(ValueError, 'candidate, runtime'):
                        admit_all_modes(ROOT / 'evals/reliability/question-development-r2.json', root, root)
                    read.assert_not_called()
