"""Actual pinned SDK serialization over localhost MockTransport, never a model."""
from contextlib import asynccontextmanager
import json
from pathlib import Path
from unittest.mock import patch

from scripts.eval_source_audit import write_private
from scripts.run_source_recovery import RecoveryCapture, execute_pair, native_capture


def synthetic_response(body):
    content = body['messages'][-1]['content']
    payload = json.loads(content if isinstance(content, str) else ''.join(part['text'] for part in content))
    documents = payload['source_documents']
    if 'expected_unit_ids' not in payload:
        return {'documents': [{'document_id': documents[0]['document_id'], 'observations': [], 'limitations': []}]}
    handle = documents[0]['windows'][0]['span']['span_id']
    return {'assessments': [{
        'unit_id': identity, 'status': 'unsupported',
        'source_basis': 'Synthetic serialization response, not a factual evaluation.',
        'checks': {'subject': 'not_established', 'predicate': 'not_established',
                   'record_role': 'not_established', 'conditions': 'not_applicable',
                   'temporal': 'not_applicable', 'comparison': 'not_applicable'},
        'unresolved_assumptions': [], 'references': [{'span_id': handle}],
        'temporal_scope': 'none', 'temporal_assertion': 'source_observation',
        'comparison_scope': None, 'comparison_document_ids': []} for identity in payload['expected_unit_ids']]}


@asynccontextmanager
async def mock_sdk(model, responder=synthetic_response):
    import httpx
    from openai import AsyncOpenAI
    from strands import Agent
    from strands.models.openai import OpenAIModel
    from app import strands_orchestrator as native
    clients, bodies = [], []
    async def handle(request):
        if request.url.host != '127.0.0.1': raise ValueError('Mock transport must use localhost')
        body = json.loads(request.content); bodies.append(body)
        result = responder(body)
        if isinstance(result, BaseException): raise result
        text = result if isinstance(result, str) else json.dumps(result)
        chunk = {'id': 'serialization-only', 'object': 'chat.completion.chunk', 'created': 0,
            'model': model, 'choices': [{'index': 0, 'delta': {'role': 'assistant', 'content': text}, 'finish_reason': None}]}
        terminal = {**chunk, 'choices': [{'index': 0, 'delta': {}, 'finish_reason': 'stop'}]}
        data = ''.join('data: ' + json.dumps(c) + '\n\n' for c in (chunk, terminal)) + 'data: [DONE]\n\n'
        return httpx.Response(200, content=data.encode(), headers={'content-type': 'text/event-stream'})
    def client_factory(**kwargs):
        kwargs.update(base_url='http://127.0.0.1:1', api_key='serialization-only', max_retries=0)
        client = httpx.AsyncClient(transport=httpx.MockTransport(handle)); clients.append(client)
        return AsyncOpenAI(http_client=client, **kwargs)
    try:
        with patch.object(native, 'Agent', Agent), patch.object(native, 'OpenAIModel', OpenAIModel), \
                patch.object(native.settings, 'strands_model', model), patch(
                    'strands.models.openai.openai.AsyncOpenAI', side_effect=client_factory):
            yield bodies
        if any(not client.is_closed for client in clients): raise ValueError('Unclosed SDK mock client')
    finally:
        for client in clients:
            if not client.is_closed: await client.aclose()


async def prepare(inputs, output, model):
    from app.strands_orchestrator import StrandsQueryOrchestrator
    inputs, output = Path(inputs), Path(output)
    manifest = json.loads((inputs/'manifest-prepared.json').read_bytes())
    output.mkdir(mode=0o700, exist_ok=False)
    rows = []
    async with mock_sdk(model):
        for index, row in enumerate(manifest['pairs']):
            pair = json.loads((inputs/row['input']).read_bytes())
            directory = output/f'pair-{index:02d}'; directory.mkdir(mode=0o700)
            orchestrator = StrandsQueryOrchestrator(); orchestrator.enabled = True
            capture = RecoveryCapture(directory, max_calls=32, seconds=300)
            try:
                with native_capture(orchestrator, capture, directory) as stages:
                    await execute_pair(pair, row['order'], orchestrator, directory, capture)
                capture.require_complete()
                wires = [json.loads(p.read_bytes()) for p in sorted(directory.glob('wire-*-input.json'))]
                rows.append({'index': index, 'requests': len(wires), 'largest_bytes': max(w['bytes'] for w in wires),
                             'model_sha256': capture.hashes, 'stage_sha256': stages['hashes']})
            finally:
                await orchestrator.close(); capture.close_pending()
    result = {'scope': 'SDK localhost MockTransport serialization only', 'native_model_calls': 0,
              'rows': rows, 'provider_capacity': 'unknown'}
    write_private(output/'preflight.json', result)
    return {'pairs': len(rows), 'requests': sum(r['requests'] for r in rows),
            'largest_bytes': max(r['largest_bytes'] for r in rows), 'native_model_calls': 0}
