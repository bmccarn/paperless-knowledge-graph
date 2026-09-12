"""Comparison uses owned originals equally; actual SDK mock checks no hidden reading context."""
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from tests.runtime import configure_test_environment
configure_test_environment()
from scripts import run_source_records as runner
from scripts.source_record_comparison_preflight import responder
from scripts.source_recovery_preflight import mock_sdk
from tests import test_source_recovery_runner as recovery_tests
from tests.test_source_recovery_runner import payload_from
from app.strands_orchestrator import StrandsQueryOrchestrator


class RecordComparisonTests(unittest.IsolatedAsyncioTestCase):
    def pair(self):
        pair = recovery_tests.RecoveryRunnerTests().pair()
        span = pair['request']['source_documents'][0]['windows'][0]['span']
        # Legacy fixture spans do not have absolute original coordinates; bind them here.
        text = span['content']
        original = {'id': 1, 'content': text}
        from scripts.prepare_source_recovery import full_pack
        return pair, original, full_pack

    async def fixture(self):
        pair, original, full_pack = self.pair()
        from app.source_reading import group_sources
        from app.answer_finalization import evidence_spans
        pair['evidence_pack'] = await full_pack(original, pair['request'])
        pair['request']['source_documents'] = group_sources(evidence_spans(pair['evidence_pack'], citation_safe=True))
        handle = pair['request']['source_documents'][0]['windows'][0]['span']['span_id']
        pair['primary']['documents'][0]['observations'][0]['references'] = [{'span_id': handle}]
        return pair, original

    async def execute(self, root, pair, original, answer=responder, max_calls=256, expected=None):
        async with mock_sdk('synthetic', answer):
            native = StrandsQueryOrchestrator(); native.enabled = True
            capture = runner.RecordCapture(root, max_calls=max_calls, seconds=15)
            capture.expected_wires = expected
            try:
                with runner.recovery.native_capture(native, capture, root, allowed_stages=runner.STAGES) as stages:
                    result = await runner.owned_call(runner.execute_pair(pair, original,
                        ['baseline', 'reader', 'records'], native, root, capture), deadline=capture.deadline)
                capture.require_complete()
                return result, capture, stages
            finally:
                await native.close(); capture.close_pending()

    async def test_actual_sdk_audits_share_originals_and_empty_prior_reading(self):
        pair, original = await self.fixture()
        with tempfile.TemporaryDirectory() as temp:
            result, capture, stages = await self.execute(Path(temp), pair, original)
            self.assertTrue(all(r['status'] == 'completed' for r in result.values()))
            audits = [json.loads(s['prompt']) for s in stages['attempts'] if s['name'] == 'source_auditor']
            self.assertEqual(len(audits), 2)
            for payload in audits:
                self.assertEqual(payload['source_documents'], pair['request']['source_documents'])
                self.assertEqual(payload['source_reading'], {'documents': [{'document_id': 1, 'observations': [], 'limitations': []}]})
            self.assertNotEqual(audits[0]['units'], audits[1]['units'])
            record = capture.read('source-records.json')
            self.assertEqual(len(record['attempts']), 1)
            self.assertEqual(len(capture.read('source-projection.json')['reading']['documents'][0]['observations']), 1)
            self.assertEqual(len(capture.wires), 3)

    async def test_changed_known_wire_stops_before_mock_provider(self):
        pair, original = await self.fixture()
        called = []
        def answer(body): called.append(body); return responder(body)
        with tempfile.TemporaryDirectory() as temp:
            with self.assertRaises(runner.IntegrityFailure):
                await self.execute(Path(temp), pair, original, answer, expected=[])
            self.assertEqual(called, [])

    async def test_reader_invalid_protocol_preserved_and_final_arm_not_run(self):
        pair, original = await self.fixture()
        def answer(body):
            return {} if 'focus_block_ids' in payload_from(body) else responder(body)
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            with self.assertRaises(runner.PairFailure): await self.execute(root, pair, original, answer)
            record = json.loads((root/'source-records.json').read_bytes())
            self.assertEqual(record['reason'], 'invalid_protocol')
            self.assertEqual(record['attempts'][0]['response'], '{}\n')
            self.assertTrue(record['pending_block_ids'])
            self.assertEqual(json.loads((root/'stages.json').read_bytes())['records']['status'], 'not_run')

    async def test_budget_failure_retains_primary_and_does_not_start_reader(self):
        pair, original = await self.fixture()
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            with self.assertRaises(runner.recovery.ModelCallBudgetExceeded):
                await self.execute(root, pair, original, max_calls=1)
            self.assertTrue((root/'baseline-candidate.json').exists())
            self.assertFalse((root/'source-records.json').exists())
            self.assertEqual(len(list(root.glob('model-*-input.json'))), 1)

    async def test_original_mismatch_prevents_dispatch(self):
        pair, original = await self.fixture()
        with tempfile.TemporaryDirectory() as temp:
            with self.assertRaises(ValueError):
                await self.execute(Path(temp), pair, {**original, 'content': 'Changed original.'})
            self.assertFalse(list(Path(temp).glob('model-*-input.json')))

    def test_verified_input_bytes_survive_disk_mutation(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp); path = root/'value.json'; path.write_bytes(b'{"value":1}')
            manifest = {'sha256': {'value.json': hashlib.sha256(path.read_bytes()).hexdigest()}}
            frozen = runner.freeze(manifest, root); path.write_bytes(b'{"value":2}')
            self.assertEqual(runner.bound(frozen, 'value.json'), {'value': 1})
            with self.assertRaises(runner.IntegrityFailure): runner.freeze(manifest, root)
            with self.assertRaises(runner.IntegrityFailure): runner.bound(frozen, 'other.json')

    async def package(self, root):
        from scripts.source_record_comparison_preflight import prepare
        from scripts.eval_source_audit import write_private
        def write(name, value):
            write_private(root/name, value); return name
        pair, original = await self.fixture()
        gold = write('gold.json', {})
        source_requests = {c: write(c+'-request.json', pair['request']) for c in runner.CASE_ORDER}
        originals = {c: write(c+'-original.json', original) for c in runner.CASE_ORDER}
        b2 = {'gold': gold, 'originals': originals, 'payloads': {c: {'F': source_requests[c]} for c in runner.CASE_ORDER}}
        b2['sha256'] = {p.name: runner.recovery.digest(p) for p in root.iterdir()}
        b2_manifest = write('b2-manifest.json', b2)
        rows, old, inventory = [], [], {}
        for index, row in enumerate(runner.schedule()):
            primary = write(f'primary-{index:02d}.json', pair['primary'])
            rows.append({**row, 'input': write(f'pair-{index:02d}.json', pair), 'primary': primary, 'b2_index': index})
            old.append({'case': row['case'], 'repetition': row['repetition'], 'arm': 'F', 'index': index, 'status': 'completed'})
            inventory[f'invocation-{index:02d}/reading.json'] = runner.recovery.digest(root/primary)
        grade = write('grade.json', {'synthetic': True})
        manifest = {'kind': 'source-record-comparison-v1', 'status': 'admitted', 'limits': runner.LIMITS,
            'schedule': runner.schedule(), 'pairs': rows, 'requests': source_requests, 'originals': originals,
            'failure_policy': 'stop_pair_continue_controls_stop_shared', 'provider_capacity': 'unknown',
            'b2_manifest': b2_manifest, 'b2_run': write('b2-run.json', {'rows': old}),
            'b2_inventory': write('b2-inventory.json', inventory), 'b2_grade_spec': grade,
            'b2_grade_standards': grade, 'b2_adjudication': grade, 'gold': gold,
            'protocol': write('protocol.json', {}), 'runtime': None, 'code_sha256': runner.recovery.code_identity()}
        manifest['sha256'] = {p.name: runner.recovery.digest(p) for p in root.iterdir()}
        write('manifest-prepared.json', manifest)
        await prepare(root/'manifest-prepared.json', root/'preflight', 'synthetic')
        manifest['runtime'] = runner.recovery.runtime_identity()
        manifest['preflight'] = 'preflight/report.json'
        manifest['sha256'] = {str(p.relative_to(root)): runner.recovery.digest(p) for p in root.rglob('*') if p.is_file()}
        manifest['review_receipts'] = [{'reviewer': name, 'status': 'approved',
            'subject_sha256': runner.admission_subject(manifest)} for name in ('synthetic-a', 'synthetic-b')]
        write('manifest.json', manifest)
        return manifest

    async def test_full_runner_pair_failure_continues_but_capture_failure_stops(self):
        from app import strands_orchestrator as native
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            with patch.object(native.settings, 'strands_enabled', True), patch.object(native, 'STRANDS_AVAILABLE', True), \
                    patch.object(native.settings, 'strands_model', 'synthetic'):
                manifest = await self.package(root)
                frozen = runner.freeze(manifest, root)
                report_name = manifest['preflight']
                for corruption in ('empty', 'missing_dynamic', 'wrong_input'):
                    changed = dict(frozen); report = json.loads(changed[report_name])
                    if corruption == 'empty': report['rows'][0]['known_wires'] = []
                    elif corruption == 'missing_dynamic':
                        hashes = report['rows'][0]['model_sha256']
                        del hashes['records-audit-00.json']
                    else: report['input_manifest_sha256'] = '0'*64
                    changed[report_name] = json.dumps(report).encode()
                    with self.assertRaises(runner.IntegrityFailure): runner.known_wires(manifest, changed, 0)
                # Expiry during identity/setup is terminal aggregate exhaustion before pair admission.
                import time
                short = {**runner.LIMITS, 'total_seconds': .01}
                short_manifest = {**manifest, 'limits': short, 'review_receipts': []}
                short_manifest['review_receipts'] = [{'reviewer': name, 'status': 'approved',
                    'subject_sha256': runner.admission_subject(short_manifest)} for name in ('synthetic-a', 'synthetic-b')]
                (root/'short-manifest.json').write_text(json.dumps(short_manifest))
                identity = runner.recovery.code_identity; checks = []
                def delayed_identity():
                    checks.append(True)
                    if len(checks) == 2: time.sleep(.02)
                    return identity()
                with patch.object(runner, 'LIMITS', short), patch.object(runner.recovery, 'code_identity', delayed_identity):
                    async with mock_sdk('synthetic', responder) as bodies:
                        await runner.run(root/'short-manifest.json', root/'expired')
                        self.assertEqual(bodies, [])
                expired = json.loads((root/'expired/run.json').read_bytes())
                self.assertEqual(expired['stop_reason'], 'aggregate_budget_exhausted')
                self.assertTrue(all(r['status'] == 'not_run' for r in expired['rows']))
                calls = []
                def failing_first(body):
                    calls.append(body)
                    return TimeoutError('synthetic') if len(calls) == 1 else responder(body)
                async with mock_sdk('synthetic', failing_first):
                    await runner.run(root/'manifest.json', root/'individual')
                result = json.loads((root/'individual/run.json').read_bytes())
                self.assertEqual(result['rows'][0]['status'], 'failed')
                self.assertTrue(all(r['status'] == 'completed' for r in result['rows'][1:]))
                self.assertEqual(result['native_attempts'], 34)
                native_write = runner.recovery.ModelCapture.write
                def broken_ledger(capture, name, payload):
                    if name == 'baseline-audit-00.json': raise OSError('synthetic ledger failure')
                    return native_write(capture, name, payload)
                async with mock_sdk('synthetic', responder):
                    with patch.object(runner.recovery.ModelCapture, 'write', broken_ledger):
                        with self.assertRaises(runner.IntegrityFailure):
                            await runner.run(root/'manifest.json', root/'shared')
                result = json.loads((root/'shared/run.json').read_bytes())
                self.assertEqual(result['native_attempts'], 1)
                self.assertTrue(all(r['status'] == 'not_run' for r in result['rows'][1:]))
                # Admission receipts bind the exact schedule and budget, before output/calls.
                manifest['limits'] = {**runner.LIMITS, 'pair_attempts': 1}
                (root/'bad-manifest.json').write_text(json.dumps(manifest))
                with self.assertRaises(runner.IntegrityFailure):
                    await runner.run(root/'bad-manifest.json', root/'bad-admission')
                self.assertFalse((root/'bad-admission').exists())
