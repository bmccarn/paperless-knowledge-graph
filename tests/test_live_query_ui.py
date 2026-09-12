"""Actual built browser, real query SSE and private history; synthetic engine only."""
import asyncio
import json
import os
from pathlib import Path
import tempfile
import shutil
import unittest

from scripts.live_query_frontend import child, free_port
from scripts.live_query_process import _reap


@unittest.skipUnless(os.environ.get('LIVE_FRONTEND_TEST_NODE'), 'Opt-in built frontend and Chromium')
class BuiltLiveUITests(unittest.IsolatedAsyncioTestCase):
    async def test_source_timeline_and_context_history_in_built_browser(self):
        from tests.runtime import configure_test_environment
        configure_test_environment()
        from app import main
        from app.query import QueryEngine
        from scripts.live_query_session import delivery_session
        from scripts.live_query_server import loopback_server
        from scripts.live_query_browser import browser_adapters
        root=Path(__file__).resolve().parents[1]
        with tempfile.TemporaryDirectory() as tmp:
            directory=Path(tmp);request={'id':'synthetic','question':'What did the earlier record say?',
                'mode':'timeline','model':'synthetic', 'history':[{'role':'user','content':'What about the later record?'}]}
            class Engine:
                query_stream=QueryEngine.query_stream
                calls=0
                async def query(self,question,history=None,model_override=None,mode='strict'):
                    self.calls+=1
                    return {'question':question,'mode':mode,'confidence':1.0,'answer':'The January 1, 2025 record lists 20 units.',
                        'sources':[{'document_id':42,'title':'Synthetic record','excerpt':'January 1, 2025: 20 units.'}],
                        'timeline_events':[{'date':'2025-01-01','precision':'day','date_text':'January 1, 2025',
                            'claim':'The record lists 20 units.','claim_id':'u1','candidate_digest':'synthetic',
                            'presentation':'observation','references':[{'document_id':42,'quote':'January 1, 2025: 20 units.','source_title':'Synthetic record'}]}],
                        'finalization':{'answer_verified':True,'timeline':{'version':'verified-dates-v1',
                            'status':'ready','candidate_digest':'synthetic','event_count':1}}}
            engine=Engine()
            async with delivery_session(main,engine,request,
                    lambda history:browser_adapters(history,request,paperless_url=main._get_paperless_url())) as delivery:
                async with loopback_server(delivery['app']) as server:
                    port=free_port();node=os.environ['LIVE_FRONTEND_TEST_NODE']
                    env={**os.environ,'BACKEND_URL':server['url'],'HOSTNAME':'127.0.0.1','PORT':str(port),'NEXT_TELEMETRY_DISABLED':'1'}
                    async with child([node,'.next/standalone/server.js'],directory,'frontend.log',b'Ready in',env=env,cwd=root/'frontend'):
                        options={'base':f'http://127.0.0.1:{port}','request':request,'directory':str(directory),'timeout_ms':30000}
                        (directory/'options.json').write_text(json.dumps(options))
                        process=await asyncio.create_subprocess_exec(node,str(root/'scripts/live_query_ui.mjs'),str(directory/'options.json'),stdout=asyncio.subprocess.PIPE,stderr=asyncio.subprocess.STDOUT)
                        try:
                            output,_=await asyncio.wait_for(process.communicate(),90)
                            self.assertEqual(process.returncode,0,output.decode())
                        finally:await _reap(process,5)
                    final=delivery['delivery'].conserved_final(delivery['observed']['final'],main._get_paperless_url())
            report=json.loads((directory/'browser.json').read_bytes())
            self.assertTrue(report['completed']);self.assertEqual(engine.calls,1)
            self.assertEqual(report['submission_count'],1);self.assertEqual(report['inspected_sources'],[42])
            self.assertEqual(report['inspected_timeline_events'],1)
            self.assertEqual(final['answer'],delivery['conversation'].messages[-1]['content'])
            self.assertTrue(all(task.done() for task in delivery['workers']))
            self.assertGreaterEqual(len(report['screenshots_sha256']),5)
            if os.environ.get('LIVE_UI_ARTIFACTS'):
                shutil.copytree(directory,os.environ['LIVE_UI_ARTIFACTS'])
