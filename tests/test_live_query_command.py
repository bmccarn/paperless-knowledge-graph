"""Static admission precedes remote runtime; actual control owner joins teardown."""
import asyncio
from contextlib import asynccontextmanager
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from scripts.live_query_command import run_case, preparation_options
from scripts.live_query_evaluation import sha256


class LiveCommandTests(unittest.IsolatedAsyncioTestCase):
    async def test_failed_admission_opens_no_runtime_or_attempt(self):
        with tempfile.TemporaryDirectory() as tmp:
            output=Path(tmp);manifest={'configuration':{'cluster':{'pod_uid':'pod'}}}
            identity={'manifest_sha256':sha256(json.dumps(manifest,sort_keys=True).encode()),'pod_uid':'pod','nonce':'n'*32}
            with patch('scripts.live_query_command.admit_case',side_effect=ValueError('Failed prerequisites')), \
                    patch('scripts.live_query_runtime.query_runtime') as runtime:
                with self.assertRaisesRegex(ValueError,'Failed prerequisites'):
                    await run_case({},manifest,output,0,identity,None,None)
                runtime.assert_not_called();self.assertEqual(list(output.iterdir()),[])

    async def test_exact_admission_passes_remaining_budget_and_waits_for_stop(self):
        with tempfile.TemporaryDirectory() as tmp:
            output=Path(tmp);manifest={'configuration':{'cluster':{'pod_uid':'pod'}},'corpus_snapshot':{'generation':'redis:1'},'evaluated_at':'2026-09-10'}
            identity={'manifest_sha256':sha256(json.dumps(manifest,sort_keys=True).encode()),'pod_uid':'pod','nonce':'n'*32}
            request={'question':'Frozen question'};events=[];messages=[]
            reader=asyncio.StreamReader()
            @asynccontextmanager
            async def runtime(actual,directory,**kwargs):
                self.assertEqual(actual,request);self.assertEqual(directory,output/'case-00')
                self.assertEqual(kwargs,{'max_calls':17,'seconds':10,'expected_corpus':manifest['corpus_snapshot'],'evaluated_at':manifest['evaluated_at']})
                events.append('enter')
                try:yield {'port':12345}
                finally:events.append('closed')
            async def emit(message):
                messages.append(message)
                if message['event']=='ready':
                    reader.feed_data((json.dumps({'command':'stop',**identity})+'\n').encode())
                else:self.assertEqual(events[-1],'closed')
            with patch('scripts.live_query_command.admit_case',return_value={'request':request,'max_calls':17,'seconds':10}), \
                    patch('scripts.live_query_runtime.query_runtime',runtime):
                await run_case({},manifest,output,0,identity,reader,emit)
            self.assertEqual([m['event'] for m in messages],['ready','closed'])
            self.assertEqual((output/'case-00').stat().st_mode & 0o777,0o700)

    async def test_changed_control_manifest_rejects_before_admission(self):
        identity={'manifest_sha256':'a'*64,'pod_uid':'pod','nonce':'n'*32}
        with patch('scripts.live_query_command.admit_case') as admission:
            with self.assertRaisesRegex(ValueError,'Control identity'):
                await run_case({}, {},Path('/unused'),0,identity,None,None)
            admission.assert_not_called()

    def test_preparation_configuration_must_match_actual_settings(self):
        runtime={'paperless_url':'http://synthetic.invalid','strands_model':'synthetic',
                 'gemini_model':'synthetic','source_date_order':'mdy'}
        options={'dataset':'dataset','initial_output':'initial','all_mode_output':'all','inputs':'private',
                 'configuration':{'runtime':runtime,**runtime},'corpus_snapshot':{},'evaluated_at':'2026-09-10'}
        with patch('scripts.live_query_command.runtime_configuration',return_value=runtime):
            self.assertEqual(preparation_options(json.dumps(options)),options)
            options['configuration']['paperless_url']='http://wrong.invalid'
            with self.assertRaisesRegex(ValueError,'Request configuration'):preparation_options(json.dumps(options))
            options['configuration']['runtime']={}
            with self.assertRaisesRegex(ValueError,'Actual runtime'):preparation_options(json.dumps(options))
