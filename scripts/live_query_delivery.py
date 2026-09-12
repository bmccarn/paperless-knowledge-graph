"""Request and delivery conservation around the real isolated HTTP/SSE routes."""
from contextlib import contextmanager
import copy
import json

from scripts.live_query_evaluation import SingleRequest


class PrivateConversation:
    """One frozen antecedent and its new messages; never a production datastore."""
    def __init__(self, history):
        self.id = 'evaluation'
        self.messages = copy.deepcopy(history)

    def require_id(self, conversation_id):
        if conversation_id != self.id:
            raise ValueError('Only the private evaluation conversation is admitted')

    async def get_conversation_history(self, conversation_id):
        self.require_id(conversation_id)
        return [{'role': m['role'], 'content': m['content']} for m in self.messages]

    async def add_message(self, conversation_id, role, content, **fields):
        self.require_id(conversation_id)
        self.messages.append(copy.deepcopy({'role': role, 'content': content, **fields}))


@contextmanager
def guard_query(engine, request):
    """Keep QueryEngine's actual stream implementation and guard its query call."""
    guard = SingleRequest(question=request['question'], mode=request['mode'],
                          history=request.get('history'), model=request.get('model'))
    original = engine.query
    observed = {'guard': guard, 'final': None}

    async def query(question, conversation_history=None, model_override=None, mode='strict'):
        async def run():
            final = await original(question, conversation_history, model_override, mode)
            observed['final'] = copy.deepcopy(final)
            return final
        return await guard.invoke(run, question=question, history=conversation_history,
                                  model=model_override, mode=mode)

    engine.query = query
    try:
        yield observed
    finally:
        engine.query = original


class DeliveredEvents:
    """Retain exact SSE body bytes without capturing request headers or secrets."""
    def __init__(self, app):
        self.app = app
        self.bodies = []
        self.statuses = []
        self.completed = []

    async def __call__(self, scope, receive, send):
        captured = scope['type'] == 'http' and scope['path'] == '/query/stream' and scope['method'] == 'POST'
        body = bytearray()
        failed, terminated, returned = False, False, False
        async def observe(message):
            nonlocal failed, terminated
            if captured:
                if message['type'] == 'http.response.start':
                    self.statuses.append(message['status'])
                elif message['type'] == 'http.response.body':
                    body.extend(message.get('body', b''))
            try:
                await send(message)
            except BaseException:
                failed = True
                raise
            if message['type'] == 'http.response.body' and not message.get('more_body', False):
                terminated = True
        try:
            await self.app(scope, receive, observe)
            returned = True
        finally:
            if captured:
                self.bodies.append(bytes(body))
                self.completed.append(returned and terminated and not failed)

    def conserved_final(self, expected, paperless_url):
        if (self.statuses != [200] or self.completed != [True]
                or len(self.bodies) != 1 or not isinstance(expected, dict)):
            raise ValueError('Exactly one successful captured SSE response is required')
        if not self.bodies[0].endswith(b'\n\n'):
            raise ValueError('Incomplete SSE frame termination')
        events = []
        for frame in self.bodies[0].decode('utf-8').split('\n\n'):
            if not frame.strip() or frame.startswith(':'):
                continue
            if not frame.startswith('data: ') or '\n' in frame:
                raise ValueError('Unexpected SSE framing')
            event = json.loads(frame[6:])
            if not isinstance(event, dict) or event.get('type') not in {'status', 'complete'}:
                raise ValueError('Unexpected or failed SSE event')
            events.append(event)
        terminal = [e for e in events if e['type'] == 'complete']
        if len(terminal) != 1 or not events or events[-1] != terminal[0]:
            raise ValueError('Exactly one final complete event must end delivery')
        projected = copy.deepcopy(expected)
        # The real HTTP routes only add this display URL to the engine's result.
        for source in projected.get('sources', []):
            if source.get('document_id'):
                source['paperless_url'] = f"{paperless_url}/documents/{source['document_id']}/details"
        actual = {k: v for k, v in terminal[0].items() if k != 'type'}
        if projected != actual:
            raise ValueError('SSE delivery differs from the captured engine result')
        return actual
