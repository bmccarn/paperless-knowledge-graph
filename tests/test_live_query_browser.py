"""The built frontend's request sequence through actual isolated query routes."""
import unittest
from tests.runtime import configure_test_environment
configure_test_environment()
from app import main
from scripts.live_query_browser import browser_adapters
from scripts.live_query_delivery import PrivateConversation
from scripts.live_query_session import delivery_session
from tests.test_live_query_delivery import Engine


class LiveBrowserAdapterTests(unittest.IsolatedAsyncioTestCase):
    async def exercise(self, history):
        import httpx
        engine = Engine()
        request = {'question': 'Exact question', 'mode': 'deep', 'model': 'frozen-model', 'history': history}
        def adapters(private):
            return browser_adapters(private, request, paperless_url='https://paperless.invalid')
        async with delivery_session(main, engine, request, adapters) as run:
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=run['app']),
                                         base_url='http://127.0.0.1') as client:
                model = (await client.get('/models')).json()['default']
                conversations = (await client.get('/conversations?limit=50')).json()
                if not history:
                    self.assertEqual(conversations, [])
                    conv = (await client.post('/conversations', json={'title': 'New conversation'})).json()
                else:
                    self.assertEqual(len(conversations), 1)
                    conv = (await client.get('/conversations/evaluation')).json()
                    self.assertEqual([m['content'] for m in conv['messages']], [m['content'] for m in history])
                response = await client.post('/query/stream', json={
                    'question': request['question'], 'mode': 'deep', 'model': model, 'conversation_id': conv['id']})
                self.assertEqual(response.status_code, 200)
                final = run['delivery'].conserved_final(run['observed']['final'], main._get_paperless_url())
                title = (await client.post('/generate-title', json={'message': request['question']})).json()['title']
                self.assertEqual((await client.patch('/conversations/evaluation', json={'title': title})).status_code, 200)
                saved = (await client.get('/conversations/evaluation')).json()
                self.assertEqual(saved['messages'][-1]['content'], final['answer'])
                self.assertEqual(saved['messages'][-1]['finalization'], final['finalization'])
                self.assertEqual(saved['messages'][-1]['sources'], final['sources'])
                self.assertEqual(saved['title'], title)
                for method, path, body in [('POST', '/conversations', {'title': 'New conversation'}),
                        ('POST', '/generate-title', {'message': 'Unscheduled'}),
                        ('PATCH', '/conversations/production', {'title': 'No'}),
                        ('DELETE', '/conversations/evaluation', {}),
                        ('GET', '/document/42/detail', None), ('POST', '/sync', {})]:
                    self.assertEqual((await client.request(method, path, json=body)).status_code, 403)
        self.assertEqual(engine.calls, [(request['question'], history, model, 'deep')])

    async def test_new_conversation_and_replay_use_only_the_frozen_model_request(self):
        await self.exercise([])

    async def test_followup_preserves_frozen_antecedent_through_frontend_sequence(self):
        await self.exercise([{'role': 'user', 'content': 'Later record?'},
                             {'role': 'assistant', 'content': 'Frozen antecedent.'}])

    def test_missing_model_is_rejected_before_browser_can_select_an_unfrozen_default(self):
        with self.assertRaises(ValueError):
            browser_adapters(PrivateConversation([]), {'question': 'Exact question'}, paperless_url='')
