"""Diagnostic execution contracts using synthetic transport artifacts only."""
import asyncio
import json
import tempfile
import unittest
from pathlib import Path
from contextlib import contextmanager
from unittest.mock import patch
from tests.runtime import configure_test_environment
configure_test_environment()
from scripts import run_reader_retention as runner
from app import strands_orchestrator as native


class ReaderRunnerTests(unittest.IsolatedAsyncioTestCase):
    def package(self, root):
        payload = {'question':'What is recorded?','evaluated_at':'2026-09-10','source_date_order':'mdy',
                   'source_documents':[{'document_id':1,'windows':[{'ordinal':0,'span':{
                       'document_id':1,'span_id':'s','content':'A record exists.'}}]}]}
        (root/'payload.json').write_text(json.dumps(payload));(root/'gold.json').write_text('{}')
        code_root = Path(runner.__file__).resolve().parents[1]
        code = {str(p.relative_to(code_root)):runner.digest(p) for folder in ('app','scripts')
                for p in (code_root/folder).glob('*.py')}
        code['requirements.lock']=runner.digest(code_root/'requirements.lock')
        manifest = {'kind':'reader-retention-v1','schedule':[list(x) for x in runner.schedule()],
            'limits':{'logical_seconds':120,'total_seconds':3000,'native_attempts':48},
            'failure_policy':'continue_individual_stop_shared','provider_capacity':'unknown',
            'review_status':'admitted','runtime':runner.runtime_identity(),'code_sha256':code,
            'sha256':{name:runner.digest(root/name) for name in ('payload.json','gold.json')},
            'gold':'gold.json','payloads':{case:{arm:'payload.json' for arm in ('R','F')} for case in runner.CASE_ORDER}}
        manifest['protocol']='gold.json'
        manifest['originals']={case:'gold.json' for case in runner.CASE_ORDER}
        manifest['review_receipts']=[{'reviewer':name,'status':'approved','subject_sha256':runner.admission_subject(manifest)}
                                     for name in ('synthetic-a','synthetic-b')]
        (root/'manifest.json').write_text(json.dumps(manifest));return root/'manifest.json'

    async def execute(self, root, responses):
        manifest = self.package(root); count = [0]
        class Fake(native.StrandsQueryOrchestrator):
            def __init__(self):super().__init__();self.enabled=True
        @contextmanager
        def capture_stages(orchestrator,capture,directory,**kwargs):
            state={'attempts':[],'hashes':{}}
            async def text(*args,**kwargs):
                number=count[0];count[0]+=1
                answer=responses(number)
                if isinstance(answer,BaseException):raise answer
                stage={'native_result':{'stop_reason':'end_turn'}};state['attempts'].append(stage)
                attempt=capture.start('test:chat',{'model':'synthetic'})
                capture.finish(attempt,response={'choices':[{'index':0,'message':{'content':answer},'finish_reason':'stop'}]})
                return answer
            orchestrator._text_agent=text
            yield state
        with patch.object(native,'StrandsQueryOrchestrator',Fake),patch.object(runner,'capture_stages',capture_stages):
            await runner.run(manifest,root/'results')
        return json.loads((root/'results/run.json').read_text())

    def valid(self):
        return json.dumps({'documents':[{'document_id':1,'observations':[],'limitations':[]}]})

    async def test_fixed_schedule_runs_both_arms_and_repetitions(self):
        with tempfile.TemporaryDirectory() as directory:
            result=await self.execute(Path(directory),lambda n:self.valid())
            self.assertEqual(result['status'],'completed');self.assertEqual(result['native_attempts'],24)
            self.assertEqual([(x['repetition'],x['case'],x['arm']) for x in result['rows']],runner.schedule())

    async def test_protocol_correction_retains_both_attempts(self):
        with tempfile.TemporaryDirectory() as directory:
            result=await self.execute(Path(directory),lambda n:'{}' if n==0 else self.valid())
            self.assertEqual(result['native_attempts'],25)
            self.assertEqual(result['rows'][0]['native_attempts'],2)

    async def test_individual_failure_continues_and_shared_failure_stops(self):
        for error, stop in [(TimeoutError(),False),(ValueError('capture issue'),True)]:
            with tempfile.TemporaryDirectory() as directory:
                root=Path(directory)
                if stop:
                    with self.assertRaises(ValueError):await self.execute(root,lambda n:error if n==0 else self.valid())
                    result=json.loads((root/'results/run.json').read_text())
                    self.assertEqual(sum(x['status']=='not_run' for x in result['rows']),23)
                else:
                    result=await self.execute(root,lambda n:error if n==0 else self.valid())
                    self.assertEqual(result['rows'][0]['status'],'failed')
                    self.assertEqual(result['rows'][-1]['status'],'completed')

    async def test_changed_payload_rejected_before_output(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);manifest=self.package(root);(root/'payload.json').write_text('{}')
            with self.assertRaises(ValueError):await runner.run(manifest,root/'results')
            self.assertFalse((root/'results').exists())

    async def test_pending_capture_failure_preserves_spent_attempts(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)
            with patch.object(runner.DiagnosticCapture,'close_pending',side_effect=OSError('synthetic write failure')):
                with self.assertRaises(OSError):await self.execute(root,lambda n:self.valid())
            result=json.loads((root/'results/run.json').read_text())
            self.assertEqual(result['native_attempts'],1)
            self.assertEqual(result['rows'][0]['native_attempts'],1)
            self.assertTrue(result['rows'][0]['cleanup_failed'])
            self.assertEqual(result['rows'][1]['status'],'not_run')

    async def test_repeated_cancellation_joins_owned_cleanup(self):
        entered=asyncio.Event();release=asyncio.Event();finished=[]
        async def worker():
            try:await asyncio.Event().wait()
            finally:
                entered.set();await release.wait();finished.append(True)
        task=asyncio.create_task(runner.owned_call(worker()))
        await asyncio.sleep(0);task.cancel();await entered.wait()
        task.cancel();await asyncio.sleep(0);release.set()
        with self.assertRaises(asyncio.CancelledError):await task
        self.assertEqual(finished,[True])

    def test_local_expiry_is_not_capture_integrity_failure(self):
        import time
        with tempfile.TemporaryDirectory() as directory:
            capture=runner.DiagnosticCapture(Path(directory),max_calls=2,seconds=1)
            capture.deadline=time.monotonic()-1
            with self.assertRaises(TimeoutError):capture.start('test:chat',{})
            self.assertIsNone(capture.integrity_error)

    def test_deadline_crossing_during_capture_start_stays_individual(self):
        with tempfile.TemporaryDirectory() as directory:
            capture=runner.DiagnosticCapture(Path(directory),max_calls=2,seconds=1)
            capture.deadline=100
            with patch('time.monotonic',side_effect=[99.999,100.001,100.002]):
                with self.assertRaises(TimeoutError):capture.start('test:chat',{})
            self.assertIsNone(capture.integrity_error)

    async def test_payload_swap_after_validation_never_reaches_reader(self):
        original=runner.validate_inputs;calls=[]
        def swapping(manifest,root):
            result=original(manifest,root);calls.append(True)
            if len(calls)==2:(root/'payload.json').write_text('{"changed":true}')
            return result
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)
            with patch.object(runner,'validate_inputs',swapping):
                with self.assertRaises(ValueError):await self.execute(root,lambda n:self.valid())
            result=json.loads((root/'results/run.json').read_text())
            self.assertEqual(result['native_attempts'],0)
            self.assertTrue(all(row['status']=='not_run' for row in result['rows']))
