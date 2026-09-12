"""Actual SDK-shaped streams are conserved; limits and interrupted output retained."""
import asyncio
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import AsyncMock

from scripts.live_query_capture import (
    ModelCapture, CapturedClient, ModelCallBudgetExceeded, stream_completion_error,
)


class ModelCaptureTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.capture = ModelCapture(self.root, max_calls=2, seconds=30)

    async def test_locked_strands_adapter_preserves_real_openai_stream_and_usage(self):
        import httpx
        from openai import AsyncOpenAI
        from strands.models.openai import OpenAIModel

        events = [
            {'id': 'test', 'object': 'chat.completion.chunk', 'created': 1, 'model': 'test',
             'choices': [{'index': 0, 'delta': {'role': 'assistant', 'content': 'Recorded value'}, 'finish_reason': None}]},
            {'id': 'test', 'object': 'chat.completion.chunk', 'created': 1, 'model': 'test',
             'choices': [{'index': 0, 'delta': {}, 'finish_reason': 'stop'}],
             'usage': {'prompt_tokens': 5, 'completion_tokens': 2, 'total_tokens': 7}},
        ]
        requests = []
        def respond(request):
            requests.append(json.loads(request.content))
            return httpx.Response(200, headers={'content-type': 'text/event-stream'},
                                  content=''.join('data: ' + json.dumps(v) + '\n\n' for v in events) + 'data: [DONE]\n\n')
        async with AsyncOpenAI(api_key='synthetic-secret-never-capture', base_url='http://synthetic.invalid/v1',
                               max_retries=0, http_client=httpx.AsyncClient(transport=httpx.MockTransport(respond))) as client:
            model = OpenAIModel(client=CapturedClient(client, self.capture, label='strands'), model_id='test')
            output = [event async for event in model.stream([{'role': 'user', 'content': [{'text': 'What is recorded?'}]}])]
        text = ''.join(e.get('contentBlockDelta', {}).get('delta', {}).get('text', '') for e in output)
        self.assertEqual(text, 'Recorded value')
        self.assertEqual(len(requests), 1)
        recorded = json.loads((self.root / 'model-000-output.json').read_bytes())
        self.assertEqual(recorded['status'], 'completed')
        self.assertEqual(recorded['chunks'][-1]['usage']['total_tokens'], 7)
        self.assertEqual(recorded['request']['messages'], requests[0]['messages'])
        self.assertNotIn('synthetic-secret', ''.join(p.read_text() for p in self.root.iterdir()))
        self.assertTrue(all(p.stat().st_mode & 0o077 == 0 for p in self.root.iterdir()))
        self.capture.require_complete()

    async def test_actual_sdk_synthetic_end_turn_cannot_qualify_truncated_stream(self):
        import httpx
        from openai import AsyncOpenAI
        from strands.models.openai import OpenAIModel
        chunk = {'id': 'test', 'object': 'chat.completion.chunk', 'created': 1, 'model': 'test',
                 'choices': [{'index': 0, 'delta': {'role': 'assistant', 'content': 'truncated'},
                              'finish_reason': None}]}
        transport = httpx.MockTransport(lambda request: httpx.Response(
            200, headers={'content-type': 'text/event-stream'}, content='data: ' + json.dumps(chunk) + '\n\n'))
        async with AsyncOpenAI(api_key='synthetic', base_url='http://synthetic.invalid/v1', max_retries=0,
                               http_client=httpx.AsyncClient(transport=transport)) as client:
            model = OpenAIModel(client=CapturedClient(client, self.capture, label='strands'), model_id='test')
            events = [event async for event in model.stream([{'role': 'user', 'content': [{'text': 'Question'}]}])]
        self.assertTrue(any(e.get('messageStop', {}).get('stopReason') == 'end_turn' for e in events))
        artifact = json.loads((self.root / 'model-000-output.json').read_bytes())
        self.assertEqual(artifact['exception_type'], 'MissingProviderTermination')
        self.assertEqual(artifact['chunks'][0]['choices'][0]['delta']['content'], 'truncated')
        with self.assertRaisesRegex(ValueError, 'incomplete'):
            self.capture.require_complete()

    def test_every_choice_needs_a_successful_provider_termination(self):
        attempt = {'request': {'n': 2}}
        chunks = [{'choices': [{'index': 0, 'finish_reason': 'stop'}]}]
        self.assertEqual(stream_completion_error(attempt, chunks), 'MissingProviderTermination')
        chunks.append({'choices': [{'index': 1, 'finish_reason': 'length'}]})
        self.assertEqual(stream_completion_error(attempt, chunks), 'IncompleteProviderTermination')
        chunks[-1]['choices'][0]['finish_reason'] = 'stop'
        self.assertIsNone(stream_completion_error(attempt, chunks))
        for index in (2, -1, None, True, '1'):
            with self.subTest(index=index):
                malformed = [*chunks, {'choices': [{'index': index, 'finish_reason': None}]}]
                self.assertEqual(stream_completion_error(attempt, malformed), 'UnexpectedProviderChoice')

    async def test_nonstream_chat_without_termination_and_empty_run_cannot_qualify(self):
        with self.assertRaises(ValueError): self.capture.require_complete()
        response = {'choices': [{'index': 0, 'message': {'content': 'partial'}, 'finish_reason': None}]}
        returned = await self.capture.create('query:chat', AsyncMock(return_value=response), {'model': 'test'})
        self.assertIs(returned, response)
        self.assertEqual(self.capture.failures, {0: 'MissingProviderTermination'})
        with self.assertRaises(ValueError): self.capture.require_complete()

    async def test_partial_stream_is_retained_when_consumer_stops(self):
        async def chunks():
            yield {'text': 'retained prefix'}
            yield {'text': 'unread remainder'}
        operation = AsyncMock(return_value=chunks())
        stream = await self.capture.create('chat', operation, {'model': 'test', 'stream': True})
        self.assertEqual(await stream.__anext__(), {'text': 'retained prefix'})
        self.capture.close_pending()
        result = json.loads((self.root / 'model-000-output.json').read_bytes())
        self.assertEqual(result['status'], 'failed')
        self.assertEqual(result['exception_type'], 'IncompleteStream')
        self.assertEqual(result['chunks'], [{'text': 'retained prefix'}])
        await stream.iterator.aclose()

    async def test_application_retries_count_and_cap_precedes_third_dispatch(self):
        operation = AsyncMock(side_effect=[RuntimeError('first request failed'), {'value': 'fallback'}])
        with self.assertRaises(RuntimeError):
            await self.capture.create('chat', operation, {'model': 'one'})
        await self.capture.create('chat', operation, {'model': 'fallback'})
        with self.assertRaises(ModelCallBudgetExceeded):
            await self.capture.create('chat', operation, {'model': 'third'})
        self.assertEqual(operation.await_count, 2)
        self.assertEqual(len(self.capture.hashes), 4)
        self.assertTrue(self.capture.exhausted)
        self.assertEqual(json.loads((self.root / 'model-000-output.json').read_bytes())['status'], 'failed')

    async def test_cancellation_is_captured_and_unknown_options_do_not_dispatch(self):
        operation = AsyncMock(side_effect=asyncio.CancelledError)
        with self.assertRaises(ValueError):
            await self.capture.create('chat', operation, {'unreviewed_option': True})
        operation.assert_not_awaited()
        with self.assertRaises(asyncio.CancelledError):
            await self.capture.create('chat', operation, {'model': 'test'})
        output = json.loads((self.root / 'model-000-output.json').read_bytes())
        self.assertEqual(output['exception_type'], 'CancelledError')
        self.assertFalse(self.capture.pending)
