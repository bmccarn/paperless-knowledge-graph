"""The evaluation grader must fail on false approvals, omissions and unavailable runs."""
import copy
import json
from pathlib import Path
import tempfile
import unittest

from scripts.eval_source_audit import load_dataset, prepare, score_case, write_private

ROOT = Path(__file__).resolve().parents[1]
DATASET = ROOT / 'evals/reliability/development.json'


class SourceAuditEvaluationTests(unittest.TestCase):
    def setUp(self):
        self.case = {'id': 'paired', 'claims': [
            {'id': 'u1', 'text': 'A recorded fact.', 'expected_supported': True, 'required': True},
            {'id': 'u2', 'text': 'An unsupported inference.', 'expected_supported': False, 'required': False}]}
        self.ledger = {'complete': True, 'claims': [
            {'id': 'u1', 'status': 'supported'}, {'id': 'u2', 'status': 'unsupported'}]}
        self.audits = [{'assessments': [
            {'unit_id': 'u1', 'status': 'supported'}, {'unit_id': 'u2', 'status': 'unsupported'}]}]

    def test_correct_pair_passes(self):
        self.assertTrue(score_case(self.case, self.ledger, self.audits)['passed'])

    def test_later_source_guard_cannot_hide_false_semantic_approval(self):
        self.audits[0]['assessments'][1]['status'] = 'supported'
        result = score_case(self.case, self.ledger, self.audits)
        self.assertFalse(result['passed'])
        self.assertTrue(result['assertions'][1]['false_acceptance'])

    def test_rejecting_everything_cannot_win(self):
        self.ledger['claims'][0]['status'] = 'unsupported'
        result = score_case(self.case, self.ledger, self.audits)
        self.assertFalse(result['passed'])
        self.assertTrue(result['assertions'][0]['missing_required'])

    def test_unavailable_negative_is_not_correct_rejection(self):
        self.case['claims'] = self.case['claims'][1:]
        result = score_case(self.case, {}, [])
        self.assertFalse(result['passed'])
        self.assertFalse(result['assertions'][0]['available'])

    def test_omitted_positive_fails(self):
        self.ledger['claims'] = self.ledger['claims'][1:]
        self.assertFalse(score_case(self.case, self.ledger, self.audits)['passed'])

    def test_malformed_attempt_does_not_displace_valid_correction(self):
        self.audits.insert(0, {'audit_protocol_error': 'invalid_json'})
        self.assertTrue(score_case(self.case, self.ledger, self.audits)['passed'])

    def test_dataset_has_balanced_source_grounded_labels(self):
        data = load_dataset(DATASET)
        self.assertEqual(len({c['domain'] for c in data['cases']}), 4)
        claims = [c for case in data['cases'] for c in case['claims']]
        self.assertGreaterEqual(len(claims), 32)
        self.assertEqual(sum(c['expected_supported'] for c in claims), len(claims) // 2)

    def test_identity_and_label_corruption_fail_before_models(self):
        original = load_dataset(DATASET)
        for mutate in (
            lambda d: d['cases'].append(copy.deepcopy(d['cases'][0])),
            lambda d: d['cases'][0]['claims'][0].update(expected_supported='yes'),
            lambda d: d['cases'][0]['claims'][1].update(id='u1'),
            lambda d: d['cases'][0]['claims'][1].update(required=True),
            lambda d: d['cases'][1]['documents'][0].update(document_id=d['cases'][0]['documents'][0]['document_id']),
        ):
            data = copy.deepcopy(original)
            mutate(data)
            with tempfile.TemporaryDirectory() as directory:
                path = Path(directory) / 'dataset.json'
                path.write_text(json.dumps(data))
                with self.assertRaises(ValueError):
                    load_dataset(path)

    def test_manifest_freezes_source_and_implementation_without_importing_clients(self):
        manifest = prepare(DATASET, model='synthetic-route', repetitions=3, max_attempts=72,
                           seconds=1800, estimated_tokens=250000, cache_note='Unknown provider cache')
        self.assertEqual(len(manifest['dataset_sha256']), 64)
        self.assertIn('app/strands_orchestrator.py', manifest['code_sha256'])
        self.assertIn('app/answer_finalization.py', manifest['code_sha256'])
        self.assertFalse(manifest['independent_repetitions'])

    def test_private_artifacts_are_exclusive_and_owner_only(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'result.json'
            write_private(path, {'passed': False})
            self.assertEqual(path.stat().st_mode & 0o777, 0o600)
            with self.assertRaises(FileExistsError):
                write_private(path, {'passed': True})
            self.assertFalse(json.loads(path.read_text())['passed'])


if __name__ == '__main__':
    unittest.main()
