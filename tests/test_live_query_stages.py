"""Actual locked SDK stages over a synthetic HTTP transport, without network."""
import asyncio
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from tests.runtime import configure_test_environment
configure_test_environment()
from app import strands_orchestrator as native_module
from scripts.live_query_capture import ModelCapture, CapturedClient
from scripts.live_query_stages import capture_stages


class StageCaptureTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.capture = ModelCapture(self.root, max_calls=5, seconds=30)

    async def test_retired_filter_stages_are_recorded_without_transport_calls(self):
        from types import SimpleNamespace
        from unittest.mock import AsyncMock, Mock
        original = AsyncMock(return_value='must not dispatch')
        orchestrator = SimpleNamespace(_text_agent=original, _model=Mock())
        with patch.object(native_module, 'Agent', type('UnusedAgent', (), {})):
            with capture_stages(orchestrator, self.capture, self.root, reader_inventory=True) as stages:
                for name in ('fact_selector', 'fact_exclusion'):
                    with self.assertRaisesRegex(ValueError, 'forbids semantic filtering'):
                        await orchestrator._text_agent(name, 'system', '{}')
        original.assert_not_awaited()
        self.assertEqual(self.capture.attempts, [])
        self.assertEqual(len(stages['hashes']), 4)
        for index, name in enumerate(('fact_selector', 'fact_exclusion')):
            for kind in ('input', 'output'):
                record = json.loads((self.root / f'stage-{index:03d}-{kind}.json').read_bytes())
                self.assertEqual(record['name'], name)
                if kind == 'output':
                    self.assertEqual(record['exception_type'], 'ValueError')
                    self.assertNotIn('native_result', record)

    async def test_concurrent_native_stages_bind_actual_requests_and_close_clients(self):
        import httpx
        from strands.models.openai import OpenAIModel

        requests, clients = [], []
        async def respond(request):
            body = json.loads(request.content); requests.append(body)
            await asyncio.sleep(0)
            answer = body['messages'][-1]['content']
            if isinstance(answer, list):
                answer = ''.join(item['text'] for item in answer)
            chunks = [
                {'id': 'synthetic', 'object': 'chat.completion.chunk', 'created': 1, 'model': 'test',
                 'choices': [{'index': 0, 'delta': {'role': 'assistant', 'content': answer}, 'finish_reason': None}]},
                {'id': 'synthetic', 'object': 'chat.completion.chunk', 'created': 1, 'model': 'test',
                 'choices': [{'index': 0, 'delta': {}, 'finish_reason': 'stop'}],
                 'usage': {'prompt_tokens': 2, 'completion_tokens': 1, 'total_tokens': 3}},
            ]
            return httpx.Response(200, headers={'content-type': 'text/event-stream'},
                content=''.join('data: ' + json.dumps(c) + '\n\n' for c in chunks) + 'data: [DONE]\n\n')

        def model(**kwargs):
            client = httpx.AsyncClient(transport=httpx.MockTransport(respond)); clients.append(client)
            return OpenAIModel(client_args={'api_key': 'synthetic-secret', 'max_retries': 0,
                'base_url': 'http://synthetic.invalid/v1', 'http_client': client}, model_id='test')

        orchestrator = native_module.StrandsQueryOrchestrator()
        from strands import Agent
        agent_patch = patch.object(native_module, 'Agent', Agent)
        agent_patch.start(); self.addCleanup(agent_patch.stop)
        native_agent = native_module.Agent
        original_text = orchestrator._text_agent
        with patch.object(orchestrator, '_model', model):
            with capture_stages(orchestrator, self.capture, self.root, reader_inventory=True) as stages:
                result = await asyncio.gather(orchestrator._text_agent('first', 'System', 'One'),
                                               orchestrator._text_agent('second', 'System', 'Two'))
        self.assertEqual(result, ['One\n', 'Two\n'])
        self.assertIs(native_module.Agent, native_agent)
        self.assertEqual(orchestrator._text_agent, original_text)
        self.assertTrue(all(client.is_closed for client in clients))
        self.assertEqual(len(stages['hashes']), 4)
        self.assertEqual(len(self.capture.attempts), 2)
        for stage in stages['attempts']:
            self.assertEqual(stage['native_result']['text'], stage['prompt'] + '\n')
            self.assertEqual(stage['outcome'], 'completed')
            captured = [a for a in self.capture.attempts if a['kind'] == f"stage-{stage['index']:03d}:chat"]
            self.assertEqual(len(captured), 1)
            self.assertEqual(captured[0]['request']['messages'][-1]['content'],
                             [{'type': 'text', 'text': stage['prompt']}])
            self.assertEqual(captured[0]['request']['extra_body']['cache'], {'no-cache': True, 'no-store': True})
        self.capture.require_complete()
        self.assertNotIn('synthetic-secret', ''.join(p.read_text() for p in self.root.iterdir()))

    async def test_direct_query_and_embedding_bypass_is_captured_and_conflicts_rejected(self):
        from types import SimpleNamespace
        from unittest.mock import AsyncMock
        chat = AsyncMock(return_value={'choices': [{'index': 0, 'finish_reason': 'stop'}]})
        embedding = AsyncMock(return_value={'data': [{'embedding': [1.0]}]})
        client = SimpleNamespace(max_retries=0, chat=SimpleNamespace(completions=SimpleNamespace(create=chat)),
                                 embeddings=SimpleNamespace(create=embedding))
        wrapped = CapturedClient(client, self.capture, label='query', bypass_cache=True)
        await wrapped.chat.completions.create(model='test', messages=[])
        await wrapped.embeddings.create(model='test', input='query')
        for operation in (chat, embedding):
            self.assertEqual(operation.await_args.kwargs['extra_body']['cache'],
                             {'no-cache': True, 'no-store': True})
        with self.assertRaises(ValueError):
            await wrapped.chat.completions.create(model='test', extra_body={'cache': {}})
        self.assertEqual(chat.await_count, 1)
        self.capture.require_complete()
