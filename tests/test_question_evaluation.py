"""Native evaluation admission controls; no model calls."""
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from tests.runtime import configure_test_environment
configure_test_environment()
from scripts.eval_question_pipeline import manifest_for, previous_runs, digest, execute, ROOT


def graded_result(result):
    """Bound synthetic semantic grades for admission-only tests."""
    result['final'].setdefault('claim_ledger', {'claims': []})
    result['final']['finalization'].setdefault('fact_conservation',
        {'dispositions': [], 'reviews': [], 'mappings': []})
    payload = json.dumps(result).encode()
    counts = dict(raw_false_approvals=0, delivered_false_approvals=0,
                  missing_required_aspects=0, false_complete_coverage=0,
                  unsupported_extras=0, false_exclusion_approvals=0,
                  coverage_underreported_aspects=0, conservative_duplicate_rejections=0,
                  conservative_duplicate_targets=[])
    grade = json.dumps(dict(verdict='pass', result_sha256=digest(payload), **counts)).encode()
    review = dict(spec='pass', standards='pass', result_sha256=digest(payload),
                  reviewers=['spec', 'standards'],
                  grade_sha256={a: digest(grade) for a in ('spec', 'standards')}, **counts)
    return {'result.json': payload, 'review.json': json.dumps(review).encode(),
            'grade-spec.json': grade, 'grade-standards.json': grade}


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

    @patch('app.answer_coverage.restore_question_coverage', return_value={'complete': True, 'status': 'complete', 'planning_status': 'complete'})
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


class AllModeAdmissionTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.dataset = ROOT / 'evals/reliability/question-development-r2.json'
        self.cases = json.loads(self.dataset.read_bytes())['cases']
        self.runtime_patch = patch('scripts.eval_question_pipeline.runtime_snapshot',
                                   return_value={'packages': {'strands-agents': '1.55.0'}, 'model': 'synthetic'})
        self.runtime_patch.start(); self.addCleanup(self.runtime_patch.stop)
        self.restore = patch('app.answer_coverage.restore_question_coverage', return_value={'complete': False, 'status': 'partial', 'planning_status': 'complete'})
        self.restore.start(); self.addCleanup(self.restore.stop)
        self.original = manifest_for(self.dataset)
        from scripts.eval_question_pipeline import request_identity
        for index, case in enumerate(self.cases):
            directory = self.root / f'case-{index:02d}'; directory.mkdir()
            identity = request_identity(self.original, case, index, 'strict')
            result = {'native_call_count': 1, 'elapsed_seconds': 2, 'error': None,
                'case_id': case['id'], 'case_index': index,
                'manifest_sha256': digest(json.dumps(self.original, sort_keys=True).encode()),
                'final': {'mode': 'strict', 'question': case['question'],
                    'query_plan': {'mode': 'strict', 'original_question': case['question'],
                                   'request_identity_digest': identity},
                    'finalization': {'request_identity_digest': identity}}}
            for name, value in {'manifest': self.original, 'case': case,
                    'attempt-000-input': {'index': 0, 'prompt': 'original'},
                    'attempt-000-output': {'index': 0, 'response': 'approved'}}.items():
                (directory / f'{name}.json').write_text(json.dumps(value))
            result['attempt_sha256'] = {p.name: digest(p.read_bytes()) for p in directory.glob('attempt-*.json')}
            for name, payload in graded_result(result).items():
                (directory / name).write_bytes(payload)

    def manifest(self):
        return manifest_for(self.dataset, stage='all-modes', initial_output=self.root)

    async def test_fresh_contract_requires_bound_grades_and_strict_identity_without_exception(self):
        self.assertEqual(self.original['grading_version'], 2)
        self.assertNotIn('conservative_admission', self.original)
        self.assertIn('docs/specs/question-reader-inventory.md', self.original['code_sha256'])
        directory = self.root / 'case-00'
        original = {p.name: p.read_bytes() for p in directory.glob('*.json')}
        grade = directory / 'grade-spec.json'
        grade.unlink()
        with self.assertRaises(FileNotFoundError):
            await execute(self.dataset, self.original, self.root, 1)
        for name, payload in original.items(): (directory / name).write_bytes(payload)
        for field, value in [('question', 'Another question'), ('mode', 'quick')]:
            result = json.loads(original['result.json'])
            result['final'][field] = value
            for name, payload in graded_result(result).items(): (directory / name).write_bytes(payload)
            with self.assertRaisesRegex(ValueError, 'identity'):
                previous_runs(self.root, 1, self.original, self.cases)
            with self.assertRaisesRegex(ValueError, 'identity'):
                await execute(self.dataset, self.original, self.root, 1)
            for name, payload in original.items(): (directory / name).write_bytes(payload)
        result = json.loads(original['result.json'])
        result['final']['finalization']['request_identity_digest'] = 'other'
        for name, payload in graded_result(result).items(): (directory / name).write_bytes(payload)
        with self.assertRaisesRegex(ValueError, 'identity'):
            await execute(self.dataset, self.original, self.root, 1)

    def test_schedule_and_raw_artifact_identity_are_frozen(self):
        from scripts.eval_question_pipeline import request_identity, MODES
        manifest = self.manifest()
        self.assertEqual(len(manifest['schedule']), 48)
        self.assertEqual(manifest['schedule'][:4], [
            {'case_id': self.cases[0]['id'], 'mode': mode} for mode in MODES])
        self.assertEqual(len(manifest['initial_admission']), 96)
        self.assertEqual(len({request_identity(manifest, self.cases[0], i, mode)
                              for i, mode in enumerate(MODES)}), 4)
        raw = self.root / 'case-00/attempt-000-output.json'
        raw.write_text('{"index":0,"response":"changed"}')
        with self.assertRaisesRegex(ValueError, 'Raw attempt bytes'):
            self.manifest()
        raw.unlink()
        with self.assertRaisesRegex(ValueError, 'raw'):
            self.manifest()

    async def test_changed_initial_artifact_fails_before_output_or_model(self):
        manifest = self.manifest()
        output = self.root / 'all-mode'; output.mkdir()
        (self.root / 'case-00/attempt-000-input.json').write_text('{"index":0,"prompt":"changed"}')
        with self.assertRaisesRegex(ValueError, 'Raw attempt bytes'):
            await execute(self.dataset, manifest, output, 0, initial_output=self.root)
        self.assertEqual(list(output.iterdir()), [])

    def test_missing_or_failed_review_and_candidate_changes_reject_admission(self):
        review_path = self.root / 'case-11/review.json'
        original = review_path.read_bytes()
        review_path.unlink()
        with self.assertRaises(FileNotFoundError): self.manifest()
        review = json.loads(original); review['standards'] = 'fail'
        review_path.write_text(json.dumps(review))
        with self.assertRaisesRegex(ValueError, 'review'): self.manifest()
        review_path.write_bytes(original)
        from scripts.eval_question_pipeline import initial_admission
        current = self.manifest()
        import copy
        for change in ('runtime', 'dataset_sha256', 'app', 'helper', 'missing-key'):
            mutated = copy.deepcopy(current)
            if change == 'runtime': mutated['runtime']['model'] = 'other'
            elif change == 'dataset_sha256': mutated['dataset_sha256'] = 'other'
            elif change == 'app': mutated['code_sha256']['app/question_pipeline.py'] = 'other'
            elif change == 'helper': mutated['code_sha256']['scripts/eval_source_audit.py'] = 'other'
            else: mutated['code_sha256'].pop('app/question_pipeline.py')
            with self.assertRaises(ValueError, msg=change): initial_admission(self.root, mutated, self.cases)

    def test_initial_execution_policy_must_match_full_strict_contract(self):
        from scripts.eval_question_pipeline import read_run
        original = read_run(self.root / 'case-00')
        import copy
        for key, value in [('sdk_retry_policy', 'configured'), ('proxy_cache_policy', 'configured'),
                           ('retrieval', 'different subset'), ('unexpected_policy', True),
                           ('estimated_total_tokens', 1)]:
            snapshot = copy.deepcopy(original)
            manifest = json.loads(snapshot['manifest.json']); manifest[key] = value
            snapshot['manifest.json'] = json.dumps(manifest).encode()
            def captured(path, **kwargs):
                return snapshot if path.name == 'case-00' else read_run(path, **kwargs)
            with patch('scripts.eval_question_pipeline.read_run', side_effect=captured):
                with self.assertRaisesRegex(ValueError, 'full frozen Strict contract'):
                    self.manifest()

    async def test_mode_relabel_and_timeline_tampering_are_rejected(self):
        from scripts.eval_question_pipeline import request_identity, validate_scheduled_result
        from app.answer_finalization import AnswerFinalizer
        from tests.test_source_dates import ExactAuditor, pack
        import copy
        manifest = self.manifest(); case = self.cases[0]
        for text in ('Service requested January 3, 2024.', 'The recorded charge is $20 USD.'):
            final = await AnswerFinalizer(ExactAuditor()).finalize(
                case['question'], text, pack(text), mode='timeline')
            self.assertIn(final['finalization']['timeline']['status'], {'ready', 'no_dates'})
            identity = request_identity(manifest, case, 2, 'timeline')
            final.update(mode='timeline', question=case['question'], query_plan={
                'mode': 'timeline', 'original_question': case['question'], 'request_identity_digest': identity})
            final['finalization']['request_identity_digest'] = identity
            result = {'mode': 'timeline', 'final': final}
            validate_scheduled_result(result, manifest, case, 2, 'timeline')
            mutations = [lambda r: r['final']['finalization'].pop('timeline'),
                         lambda r: r['final'].update(timeline_events=[{'date': 'wrong'}]),
                         lambda r: r.update(mode='quick'),
                         lambda r: r.pop('mode'),
                         lambda r: r['final']['query_plan'].update(request_identity_digest='other')]
            for mutate in mutations:
                changed = copy.deepcopy(result); mutate(changed)
                with self.assertRaises(ValueError):
                    validate_scheduled_result(changed, manifest, case, 2, 'timeline')
            with self.assertRaisesRegex(ValueError, 'identity'):
                validate_scheduled_result(result, manifest, case, 3, 'timeline')

    def test_all_mode_continuation_binds_every_raw_attempt_to_reviewed_result(self):
        from scripts.eval_question_pipeline import request_identity
        manifest = self.manifest(); case = self.cases[0]
        root = self.root / 'all-mode'; root.mkdir(); directory = root / 'case-00'; directory.mkdir()
        original = self.root / 'case-00'
        for name in ('case.json', 'attempt-000-input.json', 'attempt-000-output.json'):
            (directory / name).write_bytes((original / name).read_bytes())
        result = json.loads((original / 'result.json').read_bytes())
        identity = request_identity(manifest, case, 0, 'quick')
        result.update(mode='quick', manifest_sha256=digest(json.dumps(manifest, sort_keys=True).encode()),
                      attempt_sha256={p.name: digest(p.read_bytes()) for p in directory.glob('attempt-*.json')})
        result['final']['mode'] = 'quick'
        result['final']['query_plan'].update(mode='quick', request_identity_digest=identity)
        result['final']['finalization']['request_identity_digest'] = identity
        for name, payload in graded_result(result).items():
            (directory / name).write_bytes(payload)
        (directory / 'manifest.json').write_text(json.dumps(manifest))
        self.assertEqual(previous_runs(root, 1, manifest, [case], modes=['quick']), (1, 2))
        self.assertEqual(previous_runs(root, 1, manifest, [case]), (1, 2))
        for name in ('attempt-000-input.json', 'attempt-000-output.json'):
            path = directory / name; saved = path.read_bytes(); path.unlink()
            with self.assertRaisesRegex(ValueError, 'raw'):
                previous_runs(root, 1, manifest, [case], modes=['quick'])
            path.write_text('{"index":0,"changed":true}')
            with self.assertRaisesRegex(ValueError, 'Raw'):
                previous_runs(root, 1, manifest, [case], modes=['quick'])
            path.write_bytes(saved)


    def test_initial_slice_cannot_exceed_either_frozen_budget(self):
        directory = self.root / 'case-00'
        original = json.loads((directory / 'result.json').read_bytes())
        import copy
        for key, value in [('elapsed_seconds', 1801), ('native_call_count', 301)]:
            result = copy.deepcopy(original); result[key] = value
            snapshot = {name: (directory / name).read_bytes() for name in
                        ('manifest.json', 'case.json', 'result.json', 'review.json',
                         'attempt-000-input.json', 'attempt-000-output.json')}
            snapshot.update(graded_result(result))
            from scripts.eval_question_pipeline import read_run
            def captured(path, **kwargs):
                return snapshot if path.name == 'case-00' else read_run(path, **kwargs)
            with patch('scripts.eval_question_pipeline.read_run', side_effect=captured):
                with self.assertRaisesRegex(ValueError, 'budget'): self.manifest()


class CoverageStageAdmissionTests(unittest.IsolatedAsyncioTestCase):
    async def test_valid_unavailable_and_coarse_receipts_do_not_qualify(self):
        from tests.test_question_pipeline import QuestionPipelineTests
        from app.answer_coverage import restore_question_coverage
        import copy
        fixture = QuestionPipelineTests()
        await fixture.asyncSetUp()
        try:
            ready = await fixture.engine.query('What monthly premium is recorded?', mode='strict')
            receipt = ready['finalization']['question_coverage']
            self.assertEqual(restore_question_coverage(ready)['status'], 'complete')
            unavailable = copy.deepcopy(ready)
            unavailable['finalization']['question_coverage'] = {
                **receipt, 'status': 'unavailable', 'assessment_status': 'unavailable',
                'complete': False, 'omitted_requested_aspects': None,
                'requirements': [{'requirement_id': r['id'], 'aspect': r['aspect'], 'status': 'unavailable',
                    'observation_ids': [], 'gap_reason': 'coverage_unavailable'}
                    for r in ready['query_plan']['requirements']]}
            coarse = copy.deepcopy(ready)
            coarse['query_plan']['requirements_status'] = 'coarse'
            coarse['finalization']['question_coverage'].update(status='partial', assessment_status='partial',
                complete=False, planning_status='coarse')
            for final in (unavailable, coarse):
                self.assertIsNotNone(restore_question_coverage(final))
                with tempfile.TemporaryDirectory() as path:
                    root = Path(path); directory = root / 'case-00'; directory.mkdir()
                    case = {'id': 'one'}
                    result = {'case_id': 'one', 'case_index': 0, 'native_call_count': 5,
                        'elapsed_seconds': 1, 'error': None, 'final': final, 'manifest_sha256': digest(b'{}')}
                    payload = json.dumps(result).encode()
                    (directory / 'result.json').write_bytes(payload)
                    for name, value in {'manifest': {}, 'case': case, 'review': {
                            'spec': 'pass', 'standards': 'pass', 'result_sha256': digest(payload)}}.items():
                        (directory / f'{name}.json').write_text(json.dumps(value))
                    with self.assertRaisesRegex(ValueError, 'review'):
                        previous_runs(root, 1, {}, [case])
        finally:
            await fixture.asyncTearDown()
