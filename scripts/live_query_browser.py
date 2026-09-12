"""No-model metadata and private history adapters for the real built frontend."""
import copy
from datetime import datetime, timezone

from starlette.requests import Request
from starlette.responses import JSONResponse


def browser_adapters(history, request, *, paperless_url):
    """Freeze a single model option to match the scheduled engine request.

    History replay uses this in-memory adapter, not production conversation
    persistence. Source drawers use the engine's delivered source excerpts; no
    document, graph, mutation or arbitrary model endpoint is exposed here.
    """
    model = request.get('model')
    if not isinstance(model, str) or not model.strip():
        raise ValueError('Browser evaluation requires an explicit frozen model')
    title = 'Evaluation conversation'
    created = bool(history.messages)
    stamp = datetime.now(timezone.utc).isoformat()

    def conversation(include_messages=False):
        result = {'id': history.id, 'title': title, 'created_at': stamp,
                  'updated_at': stamp, 'message_count': len(history.messages)}
        if include_messages:
            result['messages'] = [
                {**copy.deepcopy(message.get('metadata') or {}),
                 **{k: copy.deepcopy(v) for k, v in message.items() if k != 'metadata'},
                 'created_at': stamp}
                for message in history.messages
            ]
        return result

    async def adapter(scope, receive, send):
        nonlocal title, created
        path, method = scope['path'], scope['method']
        payload, status = None, 200
        if method == 'GET':
            if path == '/_fixture':
                payload = {'fixture': 'paperless-live-evaluation-v1'}
            elif path == '/config':
                payload = {'paperless_url': paperless_url}
            elif path == '/models':
                payload = {'models': [{'id': model, 'name': model}], 'default': model}
            elif path == '/conversations':
                payload = [conversation()] if created else []
            else:
                payload, status = (conversation(True), 200) if created else ({'detail': 'Not created'}, 404)
        else:
            try:
                body = await Request(scope, receive).json()
                if not isinstance(body, dict):
                    raise ValueError('Object body required')
            except (ValueError, UnicodeDecodeError):
                await JSONResponse({'detail': 'Invalid request'}, status_code=400)(scope, receive, send)
                return
            if path == '/generate-title':
                if body != {'message': request['question']}:
                    payload, status = {'detail': 'Unscheduled title'}, 403
                else:
                    payload = {'title': request['question'][:80]}
            elif path == '/conversations':
                if created or body != {'title': 'New conversation'}:
                    payload, status = {'detail': 'Only one private conversation is admitted'}, 403
                else:
                    created = True
                    payload = conversation()
            elif (created and set(body) == {'title'} and isinstance(body['title'], str)
                  and 0 < len(body['title']) <= 200):
                title = body['title']
                payload = conversation()
            else:
                payload, status = {'detail': 'Invalid private conversation update'}, 403
        await JSONResponse(payload, status_code=status)(scope, receive, send)

    paths = [('GET', path) for path in ('/_fixture', '/config', '/models', '/conversations',
                                       '/conversations/' + history.id)]
    paths += [('POST', '/conversations'), ('POST', '/generate-title'),
              ('PATCH', '/conversations/' + history.id)]
    return {key: adapter for key in paths}
