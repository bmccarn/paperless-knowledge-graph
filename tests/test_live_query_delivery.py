"""Actual HTTP/SSE handlers with one guarded synthetic engine and private history."""
import copy
import json
import unittest
from unittest.mock import patch

from tests.runtime import configure_test_environment
configure_test_environment()
from app import main
from app.query import QueryEngine
from scripts.live_query_evaluation import EvaluationRoutes
from scripts.live_query_delivery import PrivateConversation, DeliveredEvents, guard_query


class Engine:
    query_stream = QueryEngine.query_stream

    def __init__(self):
        self.calls = []

    async def query(self, question, conversation_history=None, model_override=None, mode='strict'):
        self.calls.append((question, copy.deepcopy(conversation_history), model_override, mode))
        return {'question': question, 'answer': 'A retained answer.', 'mode': mode,
                'sources': [{'document_id': 42, 'excerpt': 'Original excerpt'}],
                'finalization': {'answer_verified': True, 'test_identity': 'synthetic'}}


class LiveDeliveryTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.request = {'question': 'What about the earlier record?', 'mode': 'deep',
            'history': [{'role': 'user', 'content': 'The later record?'}]}

    async def test_real_stream_uses_frozen_history_and_saves_only_private_messages(self):
        import httpx
        engine = Engine(); original = engine.query
        history = PrivateConversation(self.request['history'])
        boundary = DeliveredEvents(EvaluationRoutes(main.app, adapters={}))
        with guard_query(engine, self.request) as observed, patch.object(main, 'query_engine', engine), \
                patch.object(main, 'conversations', history):
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=boundary),
                                         base_url='http://127.0.0.1') as client:
                denied = await client.post('/sync')
                self.assertEqual(denied.status_code, 403)
                result = await client.post('/query/stream', json={
                    'question': self.request['question'], 'mode': 'deep', 'conversation_id': history.id})
        self.assertEqual(result.status_code, 200)
        self.assertEqual(engine.calls, [(self.request['question'], self.request['history'], None, 'deep')])
        self.assertEqual(engine.query, original)
        delivered = boundary.conserved_final(observed['final'], main._get_paperless_url())
        self.assertEqual(delivered['answer'], history.messages[-1]['content'])
        self.assertEqual(history.messages[-1]['metadata']['finalization'], delivered['finalization'])
        self.assertNotIn('paperless_url', observed['final']['sources'][0])
        self.assertTrue(delivered['sources'][0]['paperless_url'].endswith('/documents/42/details'))
        self.assertEqual(len(history.messages), 3)

    async def test_duplicate_or_changed_request_never_invokes_another_model_run(self):
        import httpx
        engine = Engine()
        boundary = DeliveredEvents(EvaluationRoutes(main.app, adapters={}))
        request = {**self.request, 'history': []}
        with guard_query(engine, request), patch.object(main, 'query_engine', engine):
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=boundary),
                                         base_url='http://127.0.0.1') as client:
                body = {'question': request['question'], 'mode': request['mode']}
                await client.post('/query/stream', json={**body, 'mode': 'quick'})
                self.assertEqual(engine.calls, [])
                await client.post('/query/stream', json=body)
                duplicate = await client.post('/query/stream', json=body)
        self.assertEqual(len(engine.calls), 1)
        self.assertIn('"type": "error"', duplicate.text)
        with self.assertRaises(ValueError): boundary.conserved_final({}, 'http://127.0.0.1')

    def test_changed_missing_duplicate_or_post_terminal_data_fails_conservation(self):
        expected = {'answer': 'Exact answer', 'finalization': {'digest': 'test'}}
        terminal = {'type': 'complete', **expected}
        for events in ([{**terminal, 'answer': 'Changed'}], [], [terminal, terminal],
                       [terminal, {'type': 'status', 'message': 'After final'}],
                       [{'type': 'error', 'message': 'Failed'}]):
            capture = DeliveredEvents(None); capture.statuses = [200]; capture.completed = [True]
            capture.bodies = [(''.join('data: ' + json.dumps(e) + '\n\n' for e in events)).encode()]
            with self.assertRaises(ValueError): capture.conserved_final(expected, 'http://127.0.0.1')

    async def test_failed_terminal_send_or_missing_frame_separator_never_qualifies(self):
        expected = {'answer': 'Exact answer'}
        body = ('data: ' + json.dumps({'type': 'complete', **expected}) + '\n\n').encode()
        async def app(scope, receive, send):
            await send({'type': 'http.response.start', 'status': 200})
            await send({'type': 'http.response.body', 'body': body})
        async def failed_send(message):
            if message['type'] == 'http.response.body': raise OSError('Disconnected')
        capture = DeliveredEvents(app)
        with self.assertRaises(OSError):
            await capture({'type': 'http', 'method': 'POST', 'path': '/query/stream'}, None, failed_send)
        self.assertEqual(capture.bodies, [body])
        self.assertEqual(capture.completed, [False])
        with self.assertRaises(ValueError): capture.conserved_final(expected, 'http://127.0.0.1')
        capture.completed = [True]; capture.bodies = [body[:-2]]
        with self.assertRaisesRegex(ValueError, 'termination'):
            capture.conserved_final(expected, 'http://127.0.0.1')

    async def test_private_conversation_never_reads_or_mutates_another_id(self):
        history = PrivateConversation(self.request['history'])
        with self.assertRaises(ValueError): await history.get_conversation_history('production-id')
        with self.assertRaises(ValueError): await history.add_message('production-id', 'user', 'wrong')
        returned = await history.get_conversation_history(history.id)
        returned[0]['content'] = 'Caller changed history'
        self.assertEqual(history.messages, self.request['history'])
