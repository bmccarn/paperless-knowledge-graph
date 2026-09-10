"""Local child ownership controls; optional built frontend smoke uses synthetic HTTP."""
import asyncio
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

from scripts.live_query_frontend import child, local_frontend


class FrontendChildTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup)
        self.root=Path(self.temp.name)

    async def test_own_banner_and_join_after_body_failure(self):
        command=[sys.executable,'-u','-c',"import time; print('OWN READY',flush=True); time.sleep(60)"]
        with self.assertRaises(LookupError):
            async with child(command,self.root,'child.log',b'OWN READY') as process:
                self.assertIsNone(process.returncode)
                raise LookupError('browser failure')
        self.assertIsNotNone(process.returncode)
        self.assertEqual((self.root/'child.log').stat().st_mode & 0o777,0o600)

    async def test_wrong_child_banner_cannot_admit_an_existing_listener(self):
        command=[sys.executable,'-u','-c',"print('WRONG READY',flush=True)"]
        with self.assertRaises(RuntimeError):
            async with child(command,self.root,'child.log',b'OWN READY'):
                self.fail('Wrong child cannot qualify readiness')

    async def test_partial_startup_cancellation_joins_child(self):
        pid=self.root/'pid'
        command=[sys.executable,'-u','-c',
                 "import os,time,sys; open(sys.argv[1],'w').write(str(os.getpid())); time.sleep(60)",str(pid)]
        async def start():
            async with child(command,self.root,'child.log',b'NEVER READY'): pass
        task=asyncio.create_task(start())
        async with asyncio.timeout(3):
            while not pid.exists(): await asyncio.sleep(.01)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError): await task
        with self.assertRaises(ProcessLookupError): os.kill(int(pid.read_text()),0)

    @unittest.skipUnless(os.environ.get('LIVE_FRONTEND_TEST_NODE'), 'Opt-in built frontend and loopback test')
    async def test_actual_standalone_frontend_reaches_owned_synthetic_backend(self):
        import httpx
        frontend=Path(__file__).resolve().parents[1]/'frontend'
        fake=self.root/'synthetic-kubectl'
        fake.write_text('#!'+sys.executable+'''\nimport json,sys
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
local,remote=map(int,sys.argv[-1].split(':'))
class Handler(BaseHTTPRequestHandler):
 def log_message(self,*args): pass
 def do_GET(self):
  self.send_response(200); self.send_header('Content-Type','application/json'); self.end_headers()
  self.wfile.write(json.dumps({'fixture':'paperless-live-evaluation-v1'}).encode())
 def do_POST(self): self.send_response(403); self.end_headers()
server=ThreadingHTTPServer(('127.0.0.1',local),Handler)
print(f'Forwarding from 127.0.0.1:{local} -> {remote}',flush=True)
server.serve_forever()
''');fake.chmod(0o700)
        async with local_frontend({'port':12345},kubectl=fake,context='synthetic',namespace='synthetic',
                pod='synthetic',node=os.environ['LIVE_FRONTEND_TEST_NODE'],frontend=frontend,
                directory=self.root) as local:
            async with httpx.AsyncClient(trust_env=False) as client:
                response=await client.get(local['url']+'/query')
                self.assertEqual(response.status_code,200)
                self.assertIn('<html',response.text)
                denied=await client.post(local['url']+'/api/sync')
                self.assertEqual(denied.status_code,403)
        self.assertIsNotNone(local['frontend'].returncode)
        self.assertIsNotNone(local['forward'].returncode)

    async def test_oversized_unterminated_output_cannot_deadlock_killed_child(self):
        marker=self.root/'written'
        command=[sys.executable,'-u','-c',
            "import os,signal,sys,time; signal.signal(signal.SIGTERM,signal.SIG_IGN); "
            "print('OWN READY',flush=True); sys.stdout.write('x'*10000000); sys.stdout.flush(); "
            "open(sys.argv[1],'w').write('done'); time.sleep(60)",str(marker)]
        async with asyncio.timeout(8):
            async with child(command,self.root,'large.log',b'OWN READY') as process:
                while not marker.exists(): await asyncio.sleep(.01)
        self.assertEqual(process.returncode,-9)
        self.assertGreater((self.root/'large.log').stat().st_size,10000000)
