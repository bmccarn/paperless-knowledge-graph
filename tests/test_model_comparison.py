"""Diagnostic contracts over real SDK localhost serialization; never provider traffic."""
import copy
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from tests.runtime import configure_test_environment
configure_test_environment()
from app import strands_orchestrator as native
from scripts import run_model_comparison as runner
from scripts import prepare_model_comparison as preparation
from scripts.model_comparison_preflight import prepare as preflight, responder
from scripts.source_recovery_preflight import mock_sdk
from scripts.eval_source_audit import write_private
from tests import test_source_record_comparison as record_tests
from tests.test_source_recovery_runner import payload_from


class ModelComparisonTests(unittest.IsolatedAsyncioTestCase):
    async def package(self, root):
        legacy = root/'legacy'; legacy.mkdir()
        await record_tests.RecordComparisonTests().package(legacy)
        source = json.loads((legacy/'manifest-prepared.json').read_bytes())
        labels = root/'labels.json'
        labels.write_text(json.dumps({c: [] for c in runner.CASE_ORDER}, indent=3))
        async def copy_prepared(b2, output):
            output.mkdir()
            for name in source['sha256']:
                target = output/name; target.parent.mkdir(parents=True, exist_ok=True)
                target.write_bytes((legacy/name).read_bytes())
            write_private(output/'manifest-prepared.json', source)
        inputs = root/'inputs'
        with patch.object(preparation, 'prepare_records', copy_prepared):
            await preparation.prepare(legacy, labels, inputs)
        self.assertEqual((inputs/'appendix-labels.json').read_bytes(), labels.read_bytes())
        await preflight(inputs/'manifest-prepared.json', inputs/'preflight')
        manifest = json.loads((inputs/'manifest-prepared.json').read_bytes())
        manifest.update(status='admitted', limits={'pair_seconds': 30, 'total_seconds': 300,
            'pair_attempts': 32, 'native_attempts': 1152}, preflight='preflight/report.json',
            destinations='destinations.json', runtimes={})
        write_private(inputs/'destinations.json', {r: {'provider': 'synthetic', 'model': r,
            'destination': 'localhost MockTransport'} for r in runner.ROUTES})
        for route in runner.ROUTES:
            with runner.route_profile(route): manifest['runtimes'][route] = runner.recovery.runtime_identity()
        manifest['sha256'] = {str(p.relative_to(inputs)): runner.recovery.digest(p)
                              for p in inputs.rglob('*') if p.is_file()}
        manifest['review_receipts'] = [{'reviewer': r, 'status': 'approved',
            'subject_sha256': runner.admission_subject(manifest)} for r in ('synthetic-a', 'synthetic-b')]
        write_private(inputs/'manifest.json', manifest)
        return inputs, manifest

    async def test_complete_known_admission_and_two_route_isolation(self):
        with tempfile.TemporaryDirectory() as temp, patch.object(native.settings, 'strands_enabled', True), patch.object(native, 'STRANDS_AVAILABLE', True):
            inputs, manifest = await self.package(Path(temp))
            frozen = runner.freeze(manifest, inputs)
            runner.validate_manifest(manifest, frozen)
            self.assertEqual(len(manifest['executions']), 36)
            for index in [0,1,12,24]:
                wires = runner.known_wires(manifest, frozen, index)
                self.assertEqual(len(wires), 3)  # initial/corrected reader + fixed baseline
                for body in wires:
                    p = payload_from(body)
                    self.assertNotIn('family', p); self.assertNotIn('gold', p)
                    if 'expected_unit_ids' in p:
                        self.assertTrue(all(not d['observations'] for d in p['source_reading']['documents']))
                    else: self.assertNotIn('source_reading', p)
            for corruption in ('known', 'fresh', 'model', 'input'):
                altered = dict(frozen); report = json.loads(altered[manifest['preflight']])
                if corruption == 'known': report['rows'][0]['known_wires'] = []
                elif corruption == 'fresh': del report['rows'][0]['model_sha256']['fresh-audit-00.json']
                elif corruption == 'model': report['rows'][0]['route'] = runner.ROUTES[1]
                else: report['input_manifest_sha256'] = '0'*64
                altered[manifest['preflight']] = json.dumps(report).encode()
                with self.assertRaises(runner.IntegrityFailure): runner.known_wires(manifest, altered, 0)
            origin_name = manifest['cases'][runner.CASE_ORDER[0]]['origins']
            for field, value in [('kind', 'appendix'), ('b2_index', 999), ('ordinal', 999)]:
                altered = dict(frozen); origins = json.loads(altered[origin_name])
                origins[0][field] = value; altered[origin_name] = json.dumps(origins).encode()
                with self.assertRaises(runner.IntegrityFailure): runner.validate_sources(manifest, altered)
                del origins[0][field]; altered[origin_name] = json.dumps(origins).encode()
                with self.assertRaises(runner.IntegrityFailure): runner.validate_sources(manifest, altered)
            original_model = native.settings.strands_model
            original_timeout = native.settings.strands_call_timeout_seconds
            async with mock_sdk('synthetic', responder) as bodies:
                before = native.settings.strands_model
                await runner.run(inputs/'manifest.json', Path(temp)/'run')
                self.assertEqual(native.settings.strands_model, before)
            self.assertEqual(native.settings.strands_model, original_model)
            self.assertEqual(native.settings.strands_call_timeout_seconds, original_timeout)
            rows = json.loads((Path(temp)/'run/run.json').read_bytes())
            self.assertTrue(all(r['status'] == 'completed' for r in rows['rows']))
            self.assertEqual(rows['native_attempts'], 144)
            self.assertEqual({b['model'] for b in bodies}, set(runner.ROUTES))

    async def test_failed_route_continues_but_integrity_stops_and_profiles_restore(self):
        with tempfile.TemporaryDirectory() as temp, patch.object(native.settings, 'strands_enabled', True), patch.object(native, 'STRANDS_AVAILABLE', True):
            inputs, manifest = await self.package(Path(temp))
            attempts = []
            def failure(body):
                attempts.append(body)
                return TimeoutError('synthetic') if len(attempts) == 1 else responder(body)
            async with mock_sdk('synthetic', failure):
                before = native.settings.strands_model
                await runner.run(inputs/'manifest.json', Path(temp)/'failed')
                self.assertEqual(native.settings.strands_model, before)
            report = json.loads((Path(temp)/'failed/run.json').read_bytes())
            self.assertEqual(report['rows'][0]['status'], 'failed')
            self.assertTrue(all(r['status'] == 'completed' for r in report['rows'][1:]))
            self.assertEqual(report['native_attempts'], 141)
            original_write = runner.recovery.ModelCapture.write
            def fail_capture(capture, name, data):
                if name == 'baseline-audit-00.json': raise OSError('synthetic')
                return original_write(capture, name, data)
            async with mock_sdk('synthetic', responder):
                before = native.settings.strands_model
                with patch.object(runner.recovery.ModelCapture, 'write', fail_capture):
                    with self.assertRaises(runner.IntegrityFailure):
                        await runner.run(inputs/'manifest.json', Path(temp)/'shared')
                self.assertEqual(native.settings.strands_model, before)
            report = json.loads((Path(temp)/'shared/run.json').read_bytes())
            self.assertTrue(all(r['status'] == 'not_run' for r in report['rows'][1:]))
            changed = copy.deepcopy(manifest); changed['limits']['native_attempts'] += 1
            write_private(inputs/'changed.json', changed)
            with self.assertRaises(runner.IntegrityFailure): await runner.run(inputs/'changed.json', Path(temp)/'bad')
            self.assertFalse((Path(temp)/'bad').exists())

    async def test_cancellation_joins_reader_before_restoring_route(self):
        import asyncio
        with tempfile.TemporaryDirectory() as temp:
            pair, original = await record_tests.RecordComparisonTests().fixture()
            closed, entered = asyncio.Event(), asyncio.Event()
            class Waiting:
                async def read_question_sources(self, request):
                    entered.set()
                    try: await asyncio.Event().wait()
                    finally:
                        self_route = native.settings.strands_model
                        await asyncio.sleep(0)
                        if self_route != native.settings.strands_model: raise AssertionError('Route restored before join')
                        closed.set()
            capture = runner.ComparisonCapture(Path(temp), max_calls=2, seconds=10)
            before = native.settings.strands_model
            async def invoke():
                with runner.route_profile(runner.ROUTES[1]):
                    await runner.owned_call(runner.execute_pair(pair, original, ['reader','fresh','baseline'],
                        Waiting(), Path(temp), capture), deadline=capture.deadline)
            task = asyncio.create_task(invoke()); await entered.wait(); task.cancel()
            with self.assertRaises(asyncio.CancelledError): await task
            self.assertTrue(closed.is_set()); self.assertEqual(native.settings.strands_model, before)
            stages = capture.read('stages.json')
            self.assertEqual(stages['reader']['status'], 'failed'); self.assertEqual(stages['fresh']['status'], 'not_run')

    async def test_native_reader_timeout_cannot_detach_cleanup_at_outer_deadline(self):
        import asyncio
        pair, original = await record_tests.RecordComparisonTests().fixture()
        cleaned = []
        class Agent:
            def __init__(self, **kwargs): pass
            async def invoke_async(self, prompt):
                try: await asyncio.Event().wait()
                finally:
                    await asyncio.sleep(.08)
                    cleaned.append(True)
        reader = native.StrandsQueryOrchestrator(); reader.enabled = True
        with tempfile.TemporaryDirectory() as temp, patch.object(native, 'Agent', Agent), \
                patch.object(reader, '_model', return_value=None), \
                patch.object(native.settings, 'strands_call_timeout_seconds', 1):
            capture = runner.ComparisonCapture(Path(temp), max_calls=5, seconds=1.04)
            with self.assertRaises(TimeoutError):
                await runner.owned_call(runner.execute_pair(pair, original, ['reader','fresh','baseline'],
                    reader, Path(temp), capture), deadline=capture.deadline)
            self.assertEqual(cleaned, [True])

    async def test_audit_stage_deadline_bounds_all_waves_and_subset_work(self):
        import asyncio
        pair, original = await record_tests.RecordComparisonTests().fixture()
        closed, calls = [], []
        async def slow_audit(*args):
            calls.append(True)
            try: await asyncio.Event().wait()
            finally:
                await asyncio.sleep(.01)
                closed.append(True)
        with tempfile.TemporaryDirectory() as temp, patch.object(runner.recovery, 'audit', slow_audit), \
                patch.object(native.settings, 'answer_audit_timeout_seconds', .01):
            capture = runner.ComparisonCapture(Path(temp), max_calls=5, seconds=1)
            start = asyncio.get_running_loop().time()
            with self.assertRaises(TimeoutError):
                await runner.owned_call(runner.execute_pair(pair, original, ['baseline','reader','fresh'],
                    None, Path(temp), capture), deadline=capture.deadline)
            self.assertLess(asyncio.get_running_loop().time()-start, .5)
            self.assertEqual(closed, [True]); self.assertEqual(calls, [True])
            self.assertEqual(capture.read('stages.json')['reader']['status'], 'not_run')
