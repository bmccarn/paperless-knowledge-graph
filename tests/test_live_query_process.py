"""Exercise real synthetic subprocesses and forwarding lifetime, without models."""
import asyncio
from contextlib import asynccontextmanager
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

from scripts.live_query_process import process_bridge

IDENTITY = {'manifest_sha256': 'a' * 64, 'pod_uid': 'synthetic-pod', 'nonce': 'b' * 32}
CHILD = '''
import json,sys,time
identity=json.loads(sys.argv[1]); behavior=sys.argv[2]
if behavior == 'wrong_identity': identity['nonce']='c'*32
print(json.dumps({'event':'ready',**identity,'port':12345}),flush=True)
line=sys.stdin.readline()
if behavior == 'hang': time.sleep(60)
message=json.loads(line) if line else {}
reason='stop' if message == {'command':'stop',**identity} else 'control_eof'
print(json.dumps({'event':'closed',**identity,'ready':True,'reason':reason,
                 'exception_type':None,'cleanup_exception_type':None}),flush=True)
if behavior == 'extra': print('unexpected',flush=True)
if behavior == 'nonzero': sys.exit(3)
'''


class ProcessTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup)
        self.root=Path(self.temp.name); self.events=[]

    @asynccontextmanager
    async def locals(self, ready):
        self.events.append('local_enter')
        try: yield {'url':'http://127.0.0.1:12345'}
        finally: self.events.append('local_exit')

    def bridge(self, behavior='normal', factory=None, **kwargs):
        return process_bridge([sys.executable,'-u','-c',CHILD,json.dumps(IDENTITY),behavior],
                    factory or self.locals,self.root,identity=IDENTITY,
                    active_seconds=2,shutdown_seconds=.2,**kwargs)

    def report(self):
        return json.loads((self.root/'process.json').read_text())

    async def test_success_has_exact_ack_clean_exit_and_private_artifacts(self):
        async with self.bridge() as local:
            self.assertIn('url',local); self.events.append('browser_done')
        self.assertEqual(self.events,['local_enter','browser_done','local_exit'])
        report=self.report()
        self.assertTrue(report['remote_cleanup_confirmed']); self.assertEqual(report['returncode'],0)
        self.assertFalse(report['forced_local_exit'])
        self.assertEqual((self.root/'process.json').stat().st_mode & 0o777,0o600)
        self.assertEqual((self.root/'remote-stderr.log').stat().st_mode & 0o777,0o600)

    async def test_body_error_sends_eof_and_preserves_primary(self):
        with self.assertRaisesRegex(LookupError,'browser failed'):
            async with self.bridge(): raise LookupError('browser failed')
        report=self.report()
        self.assertEqual(report['closed']['reason'],'control_eof')
        self.assertFalse(report['remote_cleanup_confirmed'])
        self.assertEqual(report['exception_type'],'LookupError')
        self.assertEqual(self.events[-1],'local_exit')

    async def test_wrong_identity_never_opens_local_resources(self):
        with self.assertRaises(ValueError):
            async with self.bridge('wrong_identity'): self.fail('unreachable')
        self.assertEqual(self.events,[])
        self.assertFalse(self.report()['remote_cleanup_confirmed'])

    async def test_lost_ack_is_bounded_and_not_inferred_from_termination(self):
        with self.assertRaises(TimeoutError):
            async with self.bridge('hang'): pass
        report=self.report()
        self.assertFalse(report['remote_cleanup_confirmed']); self.assertTrue(report['forced_local_exit'])
        self.assertEqual(self.events[-1],'local_exit')

    async def test_extra_output_or_nonzero_exit_invalidates_ack(self):
        for behavior in ('extra','nonzero'):
            with self.subTest(behavior=behavior):
                self.root=Path(self.temp.name)/behavior; self.root.mkdir()
                with self.assertRaises(ValueError):
                    async with self.bridge(behavior): pass
                self.assertFalse(self.report()['remote_cleanup_confirmed'])

    async def test_partial_local_startup_failure_still_closes_remote(self):
        @asynccontextmanager
        async def broken(ready):
            self.events.append('partial')
            try: raise RuntimeError('frontend failed')
            finally: self.events.append('partial_closed')
            yield
        with self.assertRaisesRegex(RuntimeError,'frontend failed'):
            async with self.bridge(factory=broken): pass
        self.assertEqual(self.events,['partial','partial_closed'])
        self.assertFalse(self.report()['remote_cleanup_confirmed'])
        self.assertEqual(self.report()['returncode'],0)

    async def test_forwarding_stays_alive_until_remote_ack(self):
        @asynccontextmanager
        async def forwarding(ready):
            try: yield {}
            finally:
                # Child writes its marker before its ACK; local forwarding outlives it.
                self.assertTrue((self.root/'remote-closed').exists())
        child=CHILD.replace("print(json.dumps({'event':'closed'", "open(sys.argv[3],'w').write('closed')\nprint(json.dumps({'event':'closed'")
        async with process_bridge([sys.executable,'-u','-c',child,json.dumps(IDENTITY),'normal',
                                   str(self.root/'remote-closed')], forwarding,self.root,
                                  identity=IDENTITY,active_seconds=2,shutdown_seconds=.2): pass
        self.assertTrue(self.report()['remote_cleanup_confirmed'])

    async def test_repeated_owner_cancellation_does_not_interrupt_local_cleanup(self):
        entered, closing, release = asyncio.Event(), asyncio.Event(), asyncio.Event()
        @asynccontextmanager
        async def locals(ready):
            try:
                entered.set()
                yield {}
            finally:
                closing.set()
                await release.wait()
                self.events.append('joined')
        async def run():
            async with self.bridge(factory=locals): await asyncio.Event().wait()
        owner=asyncio.create_task(run())
        await entered.wait(); owner.cancel(); await closing.wait()
        owner.cancel(); await asyncio.sleep(0); self.assertFalse(owner.done())
        release.set()
        with self.assertRaises(asyncio.CancelledError): await owner
        self.assertEqual(self.events,['joined'])
        self.assertFalse(self.report()['remote_cleanup_confirmed'])

    async def test_deadline_consumes_attempt_and_joins_local_resources(self):
        with self.assertRaises(TimeoutError):
            async with process_bridge([sys.executable,'-u','-c',CHILD,json.dumps(IDENTITY),'normal'],
                    self.locals,self.root,identity=IDENTITY,active_seconds=.2,shutdown_seconds=.2):
                await asyncio.Event().wait()
        self.assertEqual(self.events,['local_enter','local_exit'])
        self.assertFalse(self.report()['remote_cleanup_confirmed'])

    async def test_reap_failure_cannot_skip_local_teardown(self):
        async def broken_reap(process, seconds):
            if process.returncode is None:
                process.kill()
                await process.wait()
            raise ProcessLookupError('reap raced with exit')
        with patch('scripts.live_query_process._reap', side_effect=broken_reap):
            with self.assertRaises(ProcessLookupError):
                async with self.bridge(): pass
        self.assertEqual(self.events,['local_enter','local_exit'])
        self.assertEqual(self.report()['cleanup_exception_type'],'ProcessLookupError')

    async def test_swallowed_deadline_cannot_be_reported_as_success(self):
        with self.assertRaises(TimeoutError):
            async with process_bridge([sys.executable,'-u','-c',CHILD,json.dumps(IDENTITY),'normal'],
                    self.locals,self.root,identity=IDENTITY,active_seconds=.2,shutdown_seconds=.2):
                try: await asyncio.sleep(10)
                except asyncio.CancelledError: pass
        self.assertFalse(self.report()['remote_cleanup_confirmed'])
        self.assertEqual(self.report()['closed']['reason'],'control_eof')

    async def test_kill_exit_race_still_joins_process(self):
        from scripts.live_query_process import _reap
        class RacingChild:
            returncode=None
            waits=0
            def terminate(self): pass
            def kill(self): raise ProcessLookupError('already gone')
            async def wait(self):
                self.waits += 1
                if self.waits == 1: await asyncio.Event().wait()
                self.returncode=0
                return 0
        process=RacingChild()
        self.assertTrue(await _reap(process,.01))
        self.assertEqual(process.waits,2)
        self.assertEqual(process.returncode,0)
