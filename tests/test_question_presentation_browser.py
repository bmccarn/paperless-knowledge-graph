"""Opt-in built-browser checks over the actual pipeline with controlled models."""
import asyncio
import copy
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from tests import test_question_presentation as controls
from app import main
from scripts.live_query_browser import browser_adapters
from scripts.live_query_session import delivery_session
from scripts.live_query_server import loopback_server
from scripts.live_query_frontend import child, free_port
from scripts.live_query_local_identity import node_environment


@unittest.skipUnless(os.environ.get('LIVE_FRONTEND_TEST_NODE'), 'Opt-in actual built browser')
class QuestionPresentationBrowserTests(unittest.IsolatedAsyncioTestCase):
    asyncSetUp = controls.QuestionPresentationTests.asyncSetUp
    asyncTearDown = controls.QuestionPresentationTests.asyncTearDown
    model = controls.QuestionPresentationTests.model

    async def test_successful_source_card_and_restored_followups(self):
        await self.exercise(False)

    async def test_failed_answer_has_no_index_card_or_presupposed_followups(self):
        await self.exercise(True)

    async def exercise(self, failed):
        root = Path(__file__).resolve().parents[1]
        node = os.environ['LIVE_FRONTEND_TEST_NODE']
        artifact_root = os.environ.get('QUESTION_PRESENTATION_ARTIFACTS')
        directory = Path(tempfile.mkdtemp(prefix='failure-' if failed else 'success-', dir=artifact_root))
        self.fail_stage = 'source_reader' if failed else None
        request = {'question': 'What monthly premium is recorded?', 'mode': 'strict',
                   'model': 'default-model', 'history': []}
        async def gaps(question, context, history, mode, broad=False):
            return {'follow_up_suggestions': controls.UNSAFE}, context, [], []
        with patch.object(self.engine, '_gap_review', side_effect=gaps), \
                patch.object(self.engine, '_build_sources', return_value=copy.deepcopy([controls.INDEX_SOURCE])):
            async with delivery_session(main, self.engine, request,
                    lambda history: browser_adapters(history, request, paperless_url=main._get_paperless_url())) as delivery:
                async with loopback_server(delivery['app']) as backend:
                    port = free_port()
                    env = {**node_environment(), 'BACKEND_URL': f'http://127.0.0.1:{backend["port"]}',
                           'HOSTNAME': '127.0.0.1', 'PORT': str(port), 'NEXT_TELEMETRY_DISABLED': '1'}
                    async with child([node, '.next/standalone/server.js'], directory, 'frontend.log',
                                     b'Ready in', env=env, cwd=root/'frontend'):
                        options = directory/'options.json'
                        options.write_text(json.dumps({'base': f'http://127.0.0.1:{port}',
                            'directory': str(directory), 'question': request['question'],
                            'failed': failed, 'neutral': controls.NEUTRAL,
                            **({'acquisition_complete': self.expected_acquisition_complete}
                               if hasattr(self, 'expected_acquisition_complete') else {})}))
                        options.chmod(0o600)
                        with (directory/'browser.log').open('xb') as log:
                            process = await asyncio.create_subprocess_exec(node,
                                str(root/'tests/question_presentation_browser.mjs'), str(options),
                                stdout=log, stderr=asyncio.subprocess.STDOUT, env=node_environment())
                            try:
                                async with asyncio.timeout(90):
                                    self.assertEqual(await process.wait(), 0, str(directory))
                            finally:
                                if process.returncode is None:
                                    process.kill()
                                await process.wait()
        report = json.loads((directory/'browser.json').read_text())
        self.assertTrue(report['completed'])
        self.assertEqual(report['submissions'], 1)
        self.assertTrue(all(worker.done() for worker in delivery['workers']))
