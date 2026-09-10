"""Local admission, immutable transfer and cleanup controls without production."""
import asyncio
import io
import json
from pathlib import Path
import tarfile
import tempfile
import unittest
from unittest.mock import AsyncMock, patch

from scripts.live_query_local import unpack_retained, run_local
from scripts.live_query_local_identity import tree_digest


def archive(entries):
    output=io.BytesIO()
    with tarfile.open(fileobj=output,mode='w:gz') as tar:
        for name,value in entries:
            entry=tarfile.TarInfo(name);entry.size=len(value)
            tar.addfile(entry,io.BytesIO(value))
    return output.getvalue()


class RetainedTransferTests(unittest.TestCase):
    def test_existing_identical_bytes_are_retained_and_changed_bytes_reject(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);payload=archive([('originals/42.json',b'original')])
            unpack_retained(payload,root);unpack_retained(payload,root)
            self.assertEqual((root/'originals/42.json').stat().st_mode & 0o777,0o600)
            with self.assertRaisesRegex(ValueError,'differs'):
                unpack_retained(archive([('originals/42.json',b'changed')]),root)
            self.assertEqual((root/'originals/42.json').read_bytes(),b'original')

    def test_traversal_duplicate_and_symlink_targets_reject(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp)
            for entries in ([('../escape',b'x')],[('/absolute',b'x')],[('same',b'1'),('same',b'2')]):
                with self.assertRaises(ValueError):unpack_retained(archive(entries),root)
            (root/'link').symlink_to('/tmp')
            with self.assertRaisesRegex(ValueError,'Symlinked'):
                unpack_retained(archive([('link/escape',b'x')]),root)

    def test_runtime_tree_binds_internal_links_and_rejects_external_links(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);(root/'version').write_bytes(b'binary');(root/'current').symlink_to('version')
            first=tree_digest(root);self.assertEqual(first['current'],'symlink:version')
            (root/'version').write_bytes(b'changed');self.assertNotEqual(first,tree_digest(root))
            (root/'outside').symlink_to('/tmp')
            with self.assertRaises(ValueError):tree_digest(root)


class LocalOwnerTests(unittest.IsolatedAsyncioTestCase):
    async def test_changed_build_prevents_cluster_access(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);(root/'manifest.json').write_text('{}')
            options={'index':0,'manifest':str(root/'manifest.json'),'inputs':'unused','frontend':'unused','node':'unused'}
            with patch('scripts.live_query_local.validate_local',side_effect=ValueError('Build changed')), \
                 patch('scripts.live_query_local.check_pod',new_callable=AsyncMock) as cluster:
                with self.assertRaisesRegex(ValueError,'Build changed'):await run_local(options)
                cluster.assert_not_called()

    async def test_existing_attempt_prevents_cluster_access(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);(root/'manifest.json').write_text('{}');(root/'case-00').mkdir()
            options={'index':0,'manifest':str(root/'manifest.json'),'inputs':'unused','frontend':'unused','node':'unused','output':str(root)}
            with patch('scripts.live_query_local.validate_local',return_value=[{}]), \
                 patch('scripts.live_query_local.check_pod',new_callable=AsyncMock) as cluster:
                with self.assertRaisesRegex(ValueError,'sequence'):await run_local(options)
                cluster.assert_not_called()

    @unittest.skipUnless(__import__('os').environ.get('LIVE_FRONTEND_TEST_NODE'),'Opt-in assembled loopback/browser owner')
    async def test_assembled_remote_control_forwarding_browser_and_collection(self):
        await self.assembled()

    @unittest.skipUnless(__import__('os').environ.get('LIVE_FRONTEND_TEST_NODE'),'Opt-in assembled loopback/browser owner')
    async def test_assembled_browser_startup_failure_cleans_remote_and_retains_attempt(self):
        await self.assembled(browser_failure=True)

    async def assembled(self,browser_failure=False):
        import os,sys
        root=Path(__file__).resolve().parents[1]
        with tempfile.TemporaryDirectory(dir='/private/tmp' if Path('/private/tmp').is_dir() else '/tmp') as tmp:
            temp=Path(tmp).resolve();remote=temp/'remote';remote.mkdir();output=temp/'local';output.mkdir()
            code=remote/'code/scripts';code.mkdir(parents=True)
            wrapper="import sys;sys.path.insert(0,"+repr(str(root))+");from tests.live_query_owner_fixture import main;main()\n"
            (code/'live_query_command.py').write_text(wrapper)
            fake=temp/'kubectl';fake.write_text('#!'+sys.executable+'\n'+wrapper);fake.chmod(0o700)
            request={'id':'synthetic','question':'Exact question','mode':'strict','model':'synthetic','history':[{'role':'user','content':'Earlier question'}]}
            (remote/'synthetic-request.json').write_text(json.dumps(request))
            manifest={'active_seconds':90,'configuration':{'cluster':{'context':'synthetic','namespace':'synthetic','pod':'synthetic','pod_uid':'synthetic-pod'}}};manifest_path=temp/'manifest.json';manifest_path.write_text(json.dumps(manifest))
            options={'index':0,'manifest':str(manifest_path),'inputs':'synthetic','output':str(output),
                     'frontend':str(root/'frontend'),'node':os.environ['LIVE_FRONTEND_TEST_NODE'],
                     'cluster':{'kubectl':str(fake),'context':'synthetic','namespace':'synthetic','pod':'synthetic','pod_uid':'synthetic-pod'},
                     'remote':{'python':sys.executable,'code':str(remote/'code'),'output':str(remote),'manifest':'synthetic','preparation':'synthetic'}}
            if browser_failure:
                fake_node=temp/'node';actual_node=options['node']
                fake_node.write_text('#!'+sys.executable+'\nimport os,sys\nif sys.argv[1].endswith("live_query_ui.mjs"):sys.exit(7)\nos.execv('+repr(actual_node)+',['+repr(actual_node)+',*sys.argv[1:]])\n')
                fake_node.chmod(0o700);options['node']=str(fake_node)
            # Only qualification prerequisites and factual-result packaging are
            # substituted. Ownership, transfer, real HTTP/SSE, UI and stop ACK run.
            with patch('scripts.live_query_local.validate_local',return_value=[request]), \
                 patch('scripts.live_query_local.capture_result',return_value={'synthetic_only':True}):
                if browser_failure:
                    with self.assertRaisesRegex(RuntimeError,'Actual built browser failed'):await run_local(options)
                else:await run_local(options)
            case=output/'case-00';report=json.loads((case/'process.json').read_bytes())
            if browser_failure:
                self.assertFalse(report['remote_cleanup_confirmed'])
                native=json.loads((case/'synthetic-runtime.json').read_bytes())
                self.assertEqual(native['calls'],0);self.assertTrue(native['workers_joined'])
                self.assertFalse((case/'result.json').exists())
                return
            self.assertTrue(report['remote_cleanup_confirmed']);self.assertFalse(report['forced_local_exit'])
            native=json.loads((case/'synthetic-runtime.json').read_bytes())
            self.assertEqual(native['calls'],1);self.assertTrue(native['workers_joined'])
            browser=json.loads((case/'browser.json').read_bytes())
            self.assertTrue(browser['completed']);self.assertEqual(browser['submission_count'],1)
            self.assertTrue((case/'result.json').exists())

    async def test_repeated_cancellation_still_joins_transfer_child(self):
        import os,sys
        from scripts.live_query_local import checked
        with tempfile.TemporaryDirectory() as tmp:
            marker=Path(tmp)/'pid'
            command=[sys.executable,'-c',"import os,signal,sys,time;signal.signal(signal.SIGTERM,signal.SIG_IGN);open(sys.argv[1],'w').write(str(os.getpid()));sys.stdout.buffer.write(b'x'*10000000);sys.stdout.flush();time.sleep(60)",str(marker)]
            task=asyncio.create_task(checked(command))
            async with asyncio.timeout(3):
                while not marker.exists():await asyncio.sleep(.01)
            pid=int(marker.read_text());task.cancel();await asyncio.sleep(.05);task.cancel()
            try:
                with self.assertRaises(asyncio.CancelledError):await task
                with self.assertRaises(ProcessLookupError):os.kill(pid,0)
            finally:
                try:os.kill(pid,9)
                except ProcessLookupError:pass

    async def test_remote_traversal_is_rejected_before_cluster_access(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);(root/'manifest.json').write_text('{}')
            options={'index':0,'manifest':str(root/'manifest.json'),'inputs':'unused','frontend':'unused','node':'unused',
                     'output':str(root),'cluster':{},'remote':{'output':'/tmp/../../outside-evaluation'}}
            with patch('scripts.live_query_local.validate_local',return_value=[{}]), \
                 patch('scripts.live_query_local.check_pod',new_callable=AsyncMock) as cluster:
                with self.assertRaisesRegex(ValueError,'temporary'):await run_local(options)
                cluster.assert_not_called()

    def test_browser_identity_cannot_bind_another_frontend(self):
        from scripts.live_query_local_identity import local_identity
        with self.assertRaisesRegex(ValueError,'same repository frontend'):
            local_identity('/tmp/other-frontend','/tmp/node')


    def test_node_environment_excludes_unbound_preloads_and_browser_overrides(self):
        from scripts.live_query_local_identity import node_environment
        with patch.dict('os.environ',{'NODE_OPTIONS':'--import=/tmp/unbound.mjs','NODE_PATH':'/tmp/modules',
                                     'PLAYWRIGHT_BROWSERS_PATH':'/tmp/browser','HOME':'/synthetic/home'}):
            env=node_environment()
            self.assertEqual(env['HOME'],'/synthetic/home')
            for name in ('NODE_OPTIONS','NODE_PATH','PLAYWRIGHT_BROWSERS_PATH'):self.assertNotIn(name,env)

    @unittest.skipUnless(__import__('os').environ.get('LIVE_FRONTEND_TEST_NODE'),'Opt-in exact Node/browser identity')
    def test_node_preload_cannot_execute_during_identity_capture(self):
        import os
        from scripts.live_query_local_identity import local_identity
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);marker=root/'executed';preload=root/'preload.cjs'
            preload.write_text('require("node:fs").writeFileSync('+json.dumps(str(marker))+',"unbound");')
            with patch.dict('os.environ',{'NODE_OPTIONS':'--require '+str(preload)}):
                local_identity(Path(__file__).resolve().parents[1]/'frontend',os.environ['LIVE_FRONTEND_TEST_NODE'])
            self.assertFalse(marker.exists())
