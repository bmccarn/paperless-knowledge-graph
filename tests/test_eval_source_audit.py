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

    def test_semantic_facet_guard_cannot_hide_raw_model_approval(self):
        self.audits[0]['assessments'][1]['model_status'] = 'supported'
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
        manifest = prepare(DATASET, model='synthetic-route', runtime=dict(model='synthetic-route', destination='http://127.0.0.1:1', call_timeout_seconds=45, audit_timeout_seconds=60, concurrency=4, enabled=True, packages={}), repetitions=3, max_attempts=72,
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

    def test_saved_ledger_units_roundtrip_without_adding_list_markers(self):
        from app.answer_observations import ObservationCandidate
        from scripts.eval_source_audit import observation_candidate
        original = ObservationCandidate.from_response({'observations': ['The form records capacity of 480 units.', 'The receipt records completed delivery.']})
        units = original.units()
        case = dict(candidate_encoding='rendered_observation_units',
                    claims=[dict(id=u['id'], text=u['text']) for u in units])
        restored = observation_candidate(case)
        self.assertEqual(restored.text, original.text)
        self.assertEqual(restored.units(), units)
        self.assertEqual(observation_candidate({'claims': [{'text': 'The form records capacity of 480 units.'}]}).units()[0]['text'], units[0]['text'])

    def test_captured_windows_keep_original_boundaries_and_reject_tampering(self):
        from tests.runtime import configure_test_environment
        configure_test_environment()
        from app.embeddings import chunk_text
        from app.evidence import evidence_item_id
        from scripts.eval_source_audit import evidence_pack
        text = ('Synthetic equipment record: approved capacity 480 units.\n' * 110)
        documents = [{'document_id': 17, 'title': 'Synthetic record', 'content': text}]
        items = []
        for index, chunk in enumerate(chunk_text(text)):
            item = dict(document_id=17, title='Synthetic record', chunk_index=index, content=chunk, source_kind='ocr')
            item['id'] = evidence_item_id(item)
            items.append(item)
        case = dict(documents=documents, evidence_pack={'items': items}, source_capture='Synthetic original-window fixture')
        self.assertEqual(evidence_pack(case), case['evidence_pack'])
        for metadata in ({'source_context': None}, {'source_context': {}}, {'source_context': False},
                         {'_source_document_content': ''}, {'document_id': True}):
            changed = copy.deepcopy(case)
            changed['evidence_pack']['items'][0].update(metadata)
            changed['evidence_pack']['items'][0]['id'] = evidence_item_id(changed['evidence_pack']['items'][0])
            with self.assertRaises(ValueError, msg=repr(metadata)):
                evidence_pack(changed)

        changed = copy.deepcopy(case)
        changed['evidence_pack']['items'][0]['source_content'] = text.replace('480', '960')
        with self.assertRaisesRegex(ValueError, 'original-source chunking'):
            evidence_pack(changed)
        changed = copy.deepcopy(case)
        changed['evidence_pack']['items'][0]['title'] = 'Invented title'
        changed['evidence_pack']['items'][0]['id'] = evidence_item_id(changed['evidence_pack']['items'][0])
        with self.assertRaisesRegex(ValueError, 'title differs'):
            evidence_pack(changed)

        changed = copy.deepcopy(case)
        changed['evidence_pack']['items'][0]['content'] += ' A fabricated completion.'
        with self.assertRaisesRegex(ValueError, 'original-source chunking'):
            evidence_pack(changed)
        changed = copy.deepcopy(case)
        changed['documents'][0]['content'] = text.replace('480', '960')
        with self.assertRaisesRegex(ValueError, 'original-source chunking'):
            evidence_pack(changed)

    def test_captured_context_requires_exact_unique_original_offsets(self):
        from tests.runtime import configure_test_environment
        configure_test_environment()
        from app.source_text import bind_document_context
        from app.evidence import evidence_item_id
        from app.answer_finalization import evidence_spans
        from scripts.eval_source_audit import evidence_pack
        text = 'Synthetic record: the approved capacity is 480 units.'
        item = dict(document_id=17, title='Synthetic record', chunk_index=0, content=text, source_kind='ocr')
        bind_document_context(item, text)
        item['id'] = evidence_item_id(item)
        case = dict(documents=[dict(document_id=17, title='Synthetic record', content=text)],
                    evidence_pack={'items': [item]}, source_capture='Synthetic original context')
        self.assertIn(text, [span['content'] for span in evidence_spans(evidence_pack(case), citation_safe=True)])
        changed = copy.deepcopy(case)
        changed['evidence_pack']['items'][0]['source_context']['start'] = 1
        with self.assertRaisesRegex(ValueError, 'invalid original offsets'):
            evidence_pack(changed)

    def test_copied_table_headers_require_explicit_invalid_input_admission(self):
        from tests.runtime import configure_test_environment
        configure_test_environment()
        from app.embeddings import chunk_text
        from app.evidence import evidence_item_id
        from scripts.eval_source_audit import evidence_pack, source_continuity
        text = '| Record | Capacity |\n| --- | --- |\n' + ''.join(f'| UNIT-{i:04d} | {i + 100} units |\n' for i in range(300))
        chunk = chunk_text(text)[1]
        self.assertNotIn(chunk, text)
        item = dict(document_id=17, title='Synthetic table', chunk_index=1, content=chunk, source_kind='ocr')
        item['id'] = evidence_item_id(item)
        case = dict(documents=[dict(document_id=17, title='Synthetic table', content=text)],
                    evidence_pack={'items': [item]}, source_capture='Copied-header reconstruction')
        with self.assertRaisesRegex(ValueError, 'Noncontiguous reconstructed input'):
            evidence_pack(case)
        retained = evidence_pack(case, allow_noncontiguous=True)
        self.assertEqual(retained, case['evidence_pack'])
        self.assertFalse(source_continuity(case, retained)['original_contiguous'])


class SourceAuditCaptureTests(unittest.IsolatedAsyncioTestCase):
    async def run_capture(self, directory, *, stalled=False):
        import asyncio
        from types import SimpleNamespace
        from unittest.mock import patch
        from tests.runtime import configure_test_environment
        configure_test_environment()
        from app import strands_orchestrator as native
        from app.config import settings
        from scripts.eval_source_audit import execute, runtime_snapshot

        class Result:
            stop_reason = 'max_tokens'
            message = {'role': 'assistant', 'content': [{'text': 'NONTERMINAL SYNTHETIC BODY'}]}
            metrics = SimpleNamespace(accumulated_usage={})
            def __str__(self):
                return 'NONTERMINAL SYNTHETIC BODY'

        class Agent:
            def __init__(self, **kwargs):
                pass
            async def invoke_async(self, prompt):
                if stalled:
                    await asyncio.sleep(10)
                return Result()

        data = load_dataset(DATASET)
        data['cases'] = data['cases'][:2]
        source = Path(directory) / 'dataset.json'
        source.write_text(json.dumps(data))
        output = Path(directory) / 'output'
        with patch.object(settings, 'strands_enabled', True), patch.object(native, 'STRANDS_AVAILABLE', True), \
             patch.object(native, 'Agent', Agent), patch.object(native.StrandsQueryOrchestrator, '_model', return_value=None):
            manifest = prepare(source, model=settings.strands_model or settings.gemini_model, runtime=runtime_snapshot(),
                               repetitions=1, max_attempts=4, seconds=0.02 if stalled else 10,
                               estimated_tokens=1000, cache_note='Controlled protocol transport; no model inference')
            report = await execute(source, manifest, output)
            self.assertIs(native.Agent, Agent)
        return report, output

    async def test_nonterminal_native_body_survives_adapter_rejection(self):
        with tempfile.TemporaryDirectory() as directory:
            report, output = await self.run_capture(directory)
            attempt = json.loads((output / 'attempt-0000-output.json').read_text())
            self.assertIsNone(attempt['response'])
            self.assertEqual(attempt['native_result']['text'], 'NONTERMINAL SYNTHETIC BODY')
            self.assertEqual(attempt['native_result']['stop_reason'], 'max_tokens')
            self.assertFalse(report['passed'])
            self.assertEqual(report['usage_reporting_attempts'], 0)
            self.assertFalse(report['usage_complete'])
            self.assertIsNone(report['usage']['totalTokens'])
            self.assertEqual(report['usage_coverage']['totalTokens'], 0)

    async def test_global_timeout_keeps_active_case_and_missing_denominator(self):
        with tempfile.TemporaryDirectory() as directory:
            report, output = await self.run_capture(directory, stalled=True)
            self.assertEqual(report['failure'], 'TimeoutError')
            self.assertEqual(report['recorded_runs'], 1)
            self.assertEqual(report['incomplete_runs'], 1)
            self.assertEqual(report['not_started_runs'], 1)
            self.assertEqual(report['unavailable'], 8)
            self.assertEqual(report['missing_required'], 4)
            self.assertTrue((output / 'case-0000.json').exists())
            self.assertTrue((output / 'attempt-0000-output.json').exists())

    async def test_runtime_drift_is_rejected_before_capture_or_inference(self):
        from unittest.mock import patch
        from tests.runtime import configure_test_environment
        configure_test_environment()
        from app.config import settings
        from scripts.eval_source_audit import execute, runtime_snapshot
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / 'output'
            manifest = prepare(DATASET, model=settings.strands_model or settings.gemini_model, runtime=runtime_snapshot(),
                               repetitions=1, max_attempts=4, seconds=10, estimated_tokens=1000, cache_note='Unverified')
            with patch.object(settings, 'litellm_url', 'http://127.0.0.1:9999'):
                with self.assertRaisesRegex(ValueError, 'runtime differs'):
                    await execute(DATASET, manifest, output)
            self.assertFalse(output.exists())


if __name__ == '__main__':
    unittest.main()
