"""Synthetic provenance controls; these tests never contact a model or service."""
import copy
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from tests.runtime import configure_test_environment
configure_test_environment()
from scripts.conservative_query_admission import load_admission, sha, validate_case_grades
from scripts.eval_question_pipeline import ROOT, execute, manifest_for, previous_runs, request_identity


def encoded(value):
    return json.dumps(value, sort_keys=True).encode()


def bundle(root, code, runtime):
    """Two fully captured failed probes: only a full duplicate is falsely rejected."""
    policy = (ROOT / 'docs/specs/question-conservative-coverage-admission.md').read_bytes()
    snapshot = {'policy.md': policy}
    for axis in ('spec', 'standards'):
        snapshot[f'policy-{axis}-review.json'] = encoded({'reviewer': axis, 'verdict': 'pass',
                                                        'policy_sha256': sha(policy)})
    calls = []
    for index in range(9):
        calls.append({'id': f'case-{index}', 'payload': {'original_question': 'What is recorded?',
            'observations': [{'id': 'selected', 'text': 'The recorded value is 8.'},
                             {'id': 'omitted', 'text': 'The record gives a value of 8.'}],
            'delivered_ids': ['selected'], 'omitted_id': 'omitted'},
            'gold': {'decision': 'covered_by' if index == 8 else 'reject',
                     'target_ids': ['selected'] if index == 8 else []}})
    inputs = encoded({'calls': calls})
    for prefix, model in [('primary', 'gemini-3.8-flash'), ('comparison', 'gpt-5.5')]:
        def put(name, value):
            payload = value if isinstance(value, bytes) else encoded(value)
            snapshot[f'{prefix}/{name}'] = payload
            return sha(payload)
        put('inputs.json', inputs)
        reviews = {f'inputs-{axis}-review.json': put(f'inputs-{axis}-review.json', {
            'reviewer': axis, 'verdict': 'pass', 'input_sha256': sha(inputs)}) for axis in ('spec', 'standards')}
        recorded_runtime = {**runtime, 'model': model}
        manifest = {'experiment': 'exclusion-authority', 'input_sha256': sha(inputs), 'reviews': reviews,
            'runtime': recorded_runtime, 'code_sha256': code, 'prompt_sha256': sha(b'Original source only.'),
            'max_calls': 9, 'active_seconds': 900, 'sdk_retries': 0,
            'strands_retries': None, 'output_token_cap': None, 'proxy_cache': 'bypass'}
        manifest_sha = put('manifest.json', manifest)
        results, model_hashes, stage_hashes, call_hashes = [], {}, {}, {}
        for index, call in enumerate(calls):
            response = {'decisions': [{'observation_id': 'omitted', 'decision': 'reject', 'target_id': None}]}
            row = {'index': index, 'id': call['id'], 'status': 'valid_ungraded', 'response': response}
            results.append(row)
            call_hashes[f'call-{index:02d}.json'] = put(f'results/call-{index:02d}.json', row)
            stage = {'index': index, 'name': 'fact_exclusion', 'system_prompt': 'Original source only.',
                     'prompt': json.dumps(call['payload'])}
            request = {'model': model, 'messages': [
                {'role': 'system', 'content': stage['system_prompt']},
                {'role': 'user', 'content': [{'type': 'text', 'text': stage['prompt']}]}]}
            for kind in ('input', 'output'):
                recorded = copy.deepcopy(stage)
                transport = {'index': index, 'request': request}
                if kind == 'output':
                    recorded.update(outcome='completed', response=json.dumps(response), native_result={
                        'text': json.dumps(response), 'stop_reason': 'end_turn'})
                    transport.update(status='completed', chunks=[{'choices': [{'index': 0,
                        'delta': {'content': json.dumps(response)}, 'finish_reason': 'stop'}]}])
                stage_hashes[f'stage-{index:03d}-{kind}.json'] = put(f'results/stage-{index:03d}-{kind}.json', recorded)
                model_hashes[f'model-{index:03d}-{kind}.json'] = put(f'results/model-{index:03d}-{kind}.json', transport)
        result = {'input_sha256': sha(inputs), 'manifest_sha256': manifest_sha, 'calls': results,
            'native_call_count': 9, 'not_started_calls': 0, 'complete_execution': True,
            'interrupted': None, 'close_error': None, 'elapsed_seconds': 9,
            'capture_sha256': model_hashes, 'stage_sha256': stage_hashes, 'call_sha256': call_hashes}
        result_sha = put('results/result.json', result)
        counts = {'false_exclusion_approvals': 0, 'false_exclusion_rejections': 1,
                  'wrong_targets': 0, 'transport_failures': 0}
        grades = {axis: put(f'results/grade-{axis}.json', {'verdict': 'fail',
                      'result_sha256': result_sha, **counts}) for axis in ('spec', 'standards')}
        put('results/review.json', {'spec': 'fail', 'standards': 'fail', 'reviewers': ['spec', 'standards'],
            'grade_sha256': grades, 'result_sha256': result_sha, 'complete_execution': True, **counts})
    for name, payload in snapshot.items():
        path = root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(payload)
    receipt = {'version': 1, 'kind': 'conservative-duplicate-development', 'policy_sha256': sha(policy),
        'input_sha256': sha(inputs), 'known_rejection': {'case_id': 'case-8', 'omitted_id': 'omitted',
        'target_id': 'selected'}, 'artifacts_sha256': {name: sha(value) for name, value in snapshot.items()}}
    (root / 'admission.json').write_bytes(encoded(receipt))
    return root / 'admission.json', policy


def fresh_grade_snapshot(count=1):
    text = '- The recorded value is 8.'
    final = {'claim_ledger': {'claims': [{'id': 'u1', 'claim': text, 'status': 'supported'}]},
        'finalization': {'fact_conservation': {
            'dispositions': [{'observation_id': 'selected', 'status': 'delivered'},
                             {'observation_id': 'omitted', 'status': 'omitted'}],
            'reviews': [{'observation_id': 'omitted', 'decision': 'reject'}],
            'mappings': [{'observation_id': 'selected', 'unit_id': 'u1', 'status': 'preserved'}]}}}
    result = {'final': final}
    payload = encoded(result)
    targets = [{'omitted_id': 'omitted', 'target_id': 'selected', 'final_claim_text': text}] if count else []
    counts = {'raw_false_approvals': 0, 'delivered_false_approvals': 0, 'missing_required_aspects': 0,
              'false_complete_coverage': 0, 'unsupported_extras': 0, 'false_exclusion_approvals': 0}
    grade = {'verdict': 'pass', 'result_sha256': sha(payload), **counts,
             'coverage_underreported_aspects': count,
             'conservative_duplicate_rejections': count, 'conservative_duplicate_targets': targets}
    snapshot = {'result.json': payload, **{f'grade-{a}.json': encoded(grade) for a in ('spec', 'standards')}}
    review = {'reviewers': ['spec', 'standards'],
        **counts, 'coverage_underreported_aspects': count,
        'grade_sha256': {a: sha(snapshot[f'grade-{a}.json']) for a in ('spec', 'standards')},
        'conservative_duplicate_rejections': count, 'conservative_duplicate_targets': targets}
    return snapshot, result, review


class ConservativeAdmissionTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.runtime = {'packages': {'strands-agents': '1.55.0'}, 'model': 'gemini-3.8-flash'}
        self.code = {'app/query.py': sha(b'app'), 'requirements.lock': sha(b'lock')}
        self.path, self.policy = bundle(self.root, self.code, self.runtime)

    def load(self, **changes):
        return load_admission(self.path, **{'code_sha256': self.code, 'runtime': self.runtime,
                                          'policy_bytes': self.policy, **changes})

    def mutate(self, name, change, *, rebind=True):
        path = self.root / name
        value = json.loads(path.read_bytes())
        change(value)
        path.write_bytes(encoded(value))
        if rebind:
            receipt = json.loads(self.path.read_bytes())
            receipt['artifacts_sha256'][name] = sha(path.read_bytes())
            self.path.write_bytes(encoded(receipt))

    def test_exact_failed_probes_admit_without_relabeling_or_mutable_aliases(self):
        admission = self.load()
        self.assertEqual(admission['receipt_sha256'], sha(self.path.read_bytes()))
        self.assertEqual(json.loads((self.root/'primary/results/review.json').read_bytes())['spec'], 'fail')
        self.code['app/query.py'] = 'changed'
        self.assertNotEqual(admission['artifacts_sha256']['primary/manifest.json'], 'changed')
        with self.assertRaisesRegex(ValueError, 'application'): self.load()

    def test_evidence_tamper_missing_inventory_and_unsafe_paths_fail(self):
        raw = self.root / 'primary/results/model-000-output.json'
        original = raw.read_bytes()
        raw.write_bytes(original + b' ')
        with self.assertRaisesRegex(ValueError, 'bytes'): self.load()
        raw.write_bytes(original)
        (self.root/'primary/extra.json').write_text('{}')
        with self.assertRaisesRegex(ValueError, 'inventory'): self.load()
        (self.root/'primary/extra.json').unlink()
        raw.unlink()
        with self.assertRaisesRegex(ValueError, 'inventory'): self.load()

    def test_semantic_counts_and_dual_approval_are_not_optional_or_coercible(self):
        original = {p: p.read_bytes() for p in self.root.rglob('*') if p.is_file()}
        for name, change in [
            ('policy-standards-review.json', lambda r: r.update(reviewer='spec')),
            ('policy-spec-review.json', lambda r: r.update(verdict='fail')),
            ('primary/results/review.json', lambda r: r.update(false_exclusion_approvals=True)),
            ('comparison/results/review.json', lambda r: r.pop('false_exclusion_rejections')),
            ('primary/results/review.json', lambda r: r.update(false_exclusion_rejections=2)),
            ('primary/results/review.json', lambda r: r.update(reviewers=['one', 'one'])),
        ]:
            with self.subTest(name=name):
                self.mutate(name, change)
                with self.assertRaises(ValueError): self.load()
                for path, payload in original.items(): path.write_bytes(payload)

    def test_terminal_failures_reject_even_if_all_outer_hashes_are_recomputed(self):
        name = 'primary/results/model-000-output.json'
        self.mutate(name, lambda r: r['chunks'][0]['choices'][0].update(finish_reason=None))
        raw_sha = sha((self.root/name).read_bytes())
        self.mutate('primary/results/result.json', lambda r: r['capture_sha256'].update({'model-000-output.json': raw_sha}))
        result_sha = sha((self.root/'primary/results/result.json').read_bytes())
        for axis in ('spec', 'standards'):
            self.mutate(f'primary/results/grade-{axis}.json', lambda r: r.update(result_sha256=result_sha))
        self.mutate('primary/results/review.json', lambda r: r.update(result_sha256=result_sha,
            grade_sha256={a: sha((self.root/f'primary/results/grade-{a}.json').read_bytes()) for a in ('spec', 'standards')}))
        with self.assertRaisesRegex(ValueError, 'terminal'): self.load()

    def test_policy_runtime_lock_and_known_target_must_match(self):
        for changes in [{'policy_bytes': self.policy+b' '}, {'runtime': {**self.runtime, 'model': 'gpt-5.5'}},
                        {'code_sha256': {**self.code, 'requirements.lock': 'changed'}}]:
            with self.subTest(changes=list(changes)), self.assertRaises(ValueError): self.load(**changes)
        receipt = json.loads(self.path.read_bytes()); receipt['known_rejection']['target_id'] = 'foreign'
        self.path.write_bytes(encoded(receipt))
        with self.assertRaisesRegex(ValueError, 'semantic'): self.load()

    async def test_manifest_binds_receipt_and_missing_or_changed_admission_stops_before_dispatch(self):
        dataset = ROOT/'evals/reliability/question-development-r2.json'
        with patch('scripts.eval_question_pipeline.runtime_snapshot', return_value=self.runtime):
            base = manifest_for(dataset)
            self.path, self.policy = bundle(self.root, base['code_sha256'], self.runtime)
            manifest = manifest_for(dataset, conservative_admission=self.path)
            self.assertEqual(manifest['conservative_admission']['receipt_sha256'], sha(self.path.read_bytes()))
            output = self.root/'run'; output.mkdir()
            with self.assertRaisesRegex(ValueError, 'Frozen'):
                await execute(dataset, manifest, output, 0)
            raw = self.root/'comparison/results/model-000-input.json'
            raw.write_bytes(raw.read_bytes()+b' ')
            with self.assertRaisesRegex(ValueError, 'bytes'):
                await execute(dataset, manifest, output, 0, conservative_admission=self.path)
            self.assertEqual(list(output.iterdir()), [])

    async def test_initial_and_all_mode_continuation_bind_grades_raw_files_and_strict_identity(self):
        dataset = ROOT/'evals/reliability/question-development-r2.json'
        cases = json.loads(dataset.read_bytes())['cases']
        output = self.root/'initial'; output.mkdir()
        with patch('scripts.eval_question_pipeline.runtime_snapshot', return_value=self.runtime), patch(
                'app.answer_coverage.restore_question_coverage', return_value={
                    'status': 'partial', 'planning_status': 'complete'}):
            base = manifest_for(dataset)
            self.path, self.policy = bundle(self.root, base['code_sha256'], self.runtime)
            manifest = manifest_for(dataset, conservative_admission=self.path)
            for index, case in enumerate(cases):
                snapshot, result, review = fresh_grade_snapshot(0)
                identity = request_identity(manifest, case, index, 'strict')
                result.update(native_call_count=1, elapsed_seconds=1, error=None, case_index=index,
                              case_id=case['id'], manifest_sha256=sha(encoded(manifest)), mode='strict')
                result['final'].update(mode='strict', question=case['question'], query_plan={
                    'mode': 'strict', 'original_question': case['question'], 'request_identity_digest': identity})
                result['final']['finalization']['request_identity_digest'] = identity
                for kind in ('input', 'output'):
                    snapshot[f'attempt-000-{kind}.json'] = encoded({'index': 0})
                result['attempt_sha256'] = {k: sha(v) for k, v in snapshot.items() if k.startswith('attempt-')}
                snapshot['result.json'] = encoded(result)
                for axis in ('spec', 'standards'):
                    grade = json.loads(snapshot[f'grade-{axis}.json'])
                    grade['result_sha256'] = sha(snapshot['result.json'])
                    snapshot[f'grade-{axis}.json'] = encoded(grade)
                    review['grade_sha256'][axis] = sha(snapshot[f'grade-{axis}.json'])
                review.update(spec='pass', standards='pass', result_sha256=sha(snapshot['result.json']))
                snapshot.update({'review.json': encoded(review), 'manifest.json': encoded(manifest),
                                 'case.json': encoded(case)})
                directory = output/f'case-{index:02d}'; directory.mkdir()
                for name, value in snapshot.items(): (directory/name).write_bytes(value)
            self.assertEqual(previous_runs(output, 1, manifest, cases), (1, 1))
            all_modes = manifest_for(dataset, stage='all-modes', initial_output=output,
                                     conservative_admission=self.path)
            self.assertEqual(all_modes['conservative_admission'], manifest['conservative_admission'])
            self.assertIn('case-11/grade-standards.json', all_modes['initial_admission'])
            grade_path = output/'case-00/grade-standards.json'
            original_grade = grade_path.read_bytes()
            grade_path.write_bytes(original_grade+b' ')
            with self.assertRaisesRegex(ValueError, 'bound'):
                previous_runs(output, 1, manifest, cases)
            grade_path.write_bytes(original_grade)
            # Rebound passing grades cannot conceal a result for another request.
            snapshots = {name: (output/'case-00'/name).read_bytes() for name in snapshot}
            for change in (lambda f: f.update(question='Other question'),
                           lambda f: f.update(mode='quick'),
                           lambda f: f['query_plan'].update(request_identity_digest='wrong')):
                broken = json.loads(snapshots['result.json']); change(broken['final'])
                altered = {**snapshots, 'result.json': encoded(broken)}
                aggregate = json.loads(altered['review.json'])
                aggregate['result_sha256'] = sha(altered['result.json'])
                for axis in ('spec', 'standards'):
                    grade = json.loads(altered[f'grade-{axis}.json']); grade['result_sha256'] = aggregate['result_sha256']
                    altered[f'grade-{axis}.json'] = encoded(grade)
                    aggregate['grade_sha256'][axis] = sha(altered[f'grade-{axis}.json'])
                altered['review.json'] = encoded(aggregate)
                with patch('scripts.eval_question_pipeline.read_run', return_value=altered):
                    with self.assertRaisesRegex(ValueError, 'Scheduled'):
                        await execute(dataset, manifest, output, 1, conservative_admission=self.path)
            raw = output/'case-00/attempt-000-output.json'
            raw.write_bytes(raw.read_bytes()+b' ')
            with self.assertRaisesRegex(ValueError, 'Raw attempt'):
                manifest_for(dataset, stage='all-modes', initial_output=output, conservative_admission=self.path)


class ConservativeGradeTests(unittest.TestCase):
    def test_production_claim_key_and_explicit_zero_are_accepted(self):
        for count in (0, 1):
            validate_case_grades(*fresh_grade_snapshot(count))

    def test_missing_counts_grade_hashes_and_disagreement_are_rejected(self):
        for mutate in [lambda g: g.pop('conservative_duplicate_rejections'),
                       lambda g: g.update(conservative_duplicate_rejections=True),
                       lambda g: g.update(false_exclusion_approvals=1),
                       lambda g: g['conservative_duplicate_targets'][0].update(target_id='foreign')]:
            snapshot, result, review = fresh_grade_snapshot()
            grade = json.loads(snapshot['grade-spec.json']); mutate(grade)
            snapshot['grade-spec.json'] = encoded(grade)
            review['grade_sha256']['spec'] = sha(snapshot['grade-spec.json'])
            with self.assertRaises(ValueError): validate_case_grades(snapshot, result, review)
        snapshot, result, review = fresh_grade_snapshot()
        snapshot['grade-spec.json'] += b' '
        with self.assertRaisesRegex(ValueError, 'bound'): validate_case_grades(snapshot, result, review)

    def test_aggregate_failures_and_coverage_disagreement_cannot_be_deferred(self):
        for change in (lambda r: r.update(false_exclusion_approvals=1),
                       lambda r: r.update(raw_false_approvals=True),
                       lambda r: r.pop('missing_required_aspects'),
                       lambda r: r.update(coverage_underreported_aspects=True),
                       lambda r: r.update(coverage_underreported_aspects=0)):
            snapshot, result, review = fresh_grade_snapshot()
            change(review)
            with self.assertRaises(ValueError): validate_case_grades(snapshot, result, review)

    def test_unselected_removed_rejected_or_rewritten_target_cannot_justify_exception(self):
        for mutate in [lambda f: f['claim_ledger']['claims'][0].update(status='unsupported'),
                       lambda f: f['claim_ledger']['claims'][0].update(claim='Other text'),
                       lambda f: f['finalization']['fact_conservation']['mappings'][0].update(status='unresolved'),
                       lambda f: f['finalization']['fact_conservation']['dispositions'][0].update(status='omitted')]:
            snapshot, result, review = fresh_grade_snapshot()
            mutate(result['final'])
            with self.assertRaisesRegex(ValueError, 'survive'): validate_case_grades(snapshot, result, review)
