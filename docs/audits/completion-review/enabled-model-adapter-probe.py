import asyncio
import json
import os
from pathlib import Path
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
REPO = next(p for p in (Path.cwd(), *Path(__file__).resolve().parents) if (p / 'app' / 'pipeline.py').exists())
sys.path.insert(0, str(REPO))
from tests.runtime import configure_test_environment
configure_test_environment()
os.environ['STRANDS_ENABLED'] = 'true'
os.environ['STRANDS_MODEL'] = 'openai/synthetic-fixture'
requests_seen = []
reply = {'assessments': [{'unit_id': 'u1', 'status': 'supported', 'temporal_scope': 'historical', 'references': [{'span_id': 'span1', 'evidence_id': 'e1', 'document_id': 101, 'quote': 'Premium: $25.'}]}]}
class Handler(BaseHTTPRequestHandler):
    def log_message(self, *args): pass
    def do_POST(self):
        payload = json.loads(self.rfile.read(int(self.headers['content-length'])))
        requests_seen.append({'path': self.path, 'stream': payload.get('stream'), 'model': payload.get('model')})
        if payload.get('stream'):
            chunks = [
                {'id': 'fixture', 'object': 'chat.completion.chunk', 'created': 1, 'model': 'synthetic-fixture', 'choices': [{'index': 0, 'delta': {'role': 'assistant', 'content': json.dumps(reply)}, 'finish_reason': None}]},
                {'id': 'fixture', 'object': 'chat.completion.chunk', 'created': 1, 'model': 'synthetic-fixture', 'choices': [{'index': 0, 'delta': {}, 'finish_reason': 'stop'}]},
            ]
            body = ''.join('data: ' + json.dumps(chunk) + '\n\n' for chunk in chunks) + 'data: [DONE]\n\n'
            content_type = 'text/event-stream'
        else:
            body = json.dumps({'id': 'fixture', 'object': 'chat.completion', 'created': 1, 'model': 'synthetic-fixture', 'choices': [{'index': 0, 'message': {'role': 'assistant', 'content': json.dumps(reply)}, 'finish_reason': 'stop'}]})
            content_type = 'application/json'
        self.send_response(200)
        self.send_header('Content-Type', content_type)
        self.send_header('Content-Length', str(len(body.encode())))
        self.end_headers()
        self.wfile.write(body.encode())
server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
os.environ['LITELLM_URL'] = f'http://127.0.0.1:{server.server_port}/v1'
thread = threading.Thread(target=server.serve_forever, daemon=True)
thread.start()
async def main():
    from app.strands_orchestrator import strands_orchestrator
    assert strands_orchestrator.enabled, strands_orchestrator.status
    result = await asyncio.wait_for(strands_orchestrator.audit_answer_units('Recorded premium?', [{'id': 'u1', 'text': 'Premium: $25.', 'start': 0, 'end': 13}], [{'span_id': 'span1', 'evidence_id': 'e1', 'document_id': 101, 'content': 'Premium: $25.'}], {'evaluated_at': '2026-09-04'}), timeout=20)
    await strands_orchestrator.close()
    assert result == reply, result
    assert len(requests_seen) == 1 and requests_seen[0]['path'] == '/v1/chat/completions', requests_seen
    print(json.dumps({'status': 'passed', 'actual_sdk': 'Strands Agent and LiteLLM provider', 'provider': 'synthetic localhost HTTP response', 'requests': requests_seen}, indent=2))
try:
    asyncio.run(main())
finally:
    server.shutdown()
    server.server_close()
    thread.join(timeout=2)
