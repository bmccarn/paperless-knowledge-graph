"""Prepare matched reader inputs from local frozen originals; never call models."""
import asyncio
import hashlib
import json
from pathlib import Path


def digest(value):
    return hashlib.sha256(value).hexdigest()


def encoded(value):
    return json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(',', ':'), allow_nan=False).encode()


def original_from(value, document_id):
    if 'documents' in value:
        rows = [d for d in value['documents'] if d.get('document_id') == document_id]
        if len(rows) != 1: raise ValueError('ambiguous_original_identity')
        return {**rows[0], 'id': document_id}
    if type(value.get('id')) is not int or value['id'] != document_id:
        raise ValueError('original_identity_mismatch')
    return value


def retained_intervals(payload, original, segment_mappings=None):
    """Validate exact original locations; never infer ownership from similar text."""
    from app.source_acquisition import missing_intervals
    documents = payload['source_documents']
    if len(documents) != 1 or documents[0]['document_id'] != original['id']:
        raise ValueError('retained_document_mismatch')
    text = original['content']; intervals = []; handles = set()
    segment_mappings = segment_mappings or {}; used_mappings = set()
    for window in documents[0]['windows']:
        span = window['span']; context = span.get('source_context')
        if span['span_id'] in segment_mappings:
            if context is not None or span['document_id'] != original['id'] or span.get('feedback_open'):
                raise ValueError('invalid_reconstruction_mapping')
            if span['span_id'] in handles: raise ValueError('duplicate_retained_handle')
            cursor = 0
            for segment in segment_mappings[span['span_id']]:
                a, b, x, y = [segment[k] for k in ('span_start', 'span_end', 'original_start', 'original_end')]
                if (any(type(v) is not int for v in (a,b,x,y)) or a != cursor
                        or not a < b <= len(span['content']) or not 0 <= x < y <= len(text)
                        or span['content'][a:b] != text[x:y]):
                    raise ValueError('invalid_reconstruction_mapping')
                cursor = b; intervals.append({'start': x, 'end': y})
            if cursor != len(span['content']): raise ValueError('incomplete_reconstruction_mapping')
            used_mappings.add(span['span_id']); handles.add(span['span_id'])
            continue
        offset = 0
        if context is not None:
            if (context['document_id'] != original['id'] or context['digest'] != digest(text.encode())
                    or type(context['start']) is not int or type(context['end']) is not int
                    or not 0 <= context['start'] < context['end'] <= len(text)):
                raise ValueError('retained_context_mismatch')
            offset = context['start']
        start, end = offset + span['start'], offset + span['end']
        if (span['document_id'] != original['id'] or span.get('feedback_open')
                or span['span_id'] in handles or not 0 <= start < end <= len(text)
                or text[start:end] != span['content']):
            raise ValueError('retained_interval_mismatch')
        handles.add(span['span_id']); intervals.append({'start': start, 'end': end})
    if set(segment_mappings) != used_mappings: raise ValueError('unused_reconstruction_mapping')
    return {'intervals': intervals, 'missing_intervals': missing_intervals(len(text), intervals),
            'explicit_segment_mappings': segment_mappings}


async def prepare_case(case_id, original, capture, *, segment_mappings=None):
    from app.paperless import PaperlessClient
    from app.source_acquisition import SourceAcquisition, Execution
    from app.source_reading import READER_PROMPT, response_format, group_sources
    from app.answer_finalization import evidence_spans
    from app.question_evidence import validate_requirements
    if capture.get('name') != 'source_reader' or capture.get('system_prompt') != READER_PROMPT:
        raise ValueError('reader_prompt_changed')
    retained = json.loads(capture['prompt'])
    expected = {'question', 'resolved_question', 'requirements', 'evaluated_at', 'source_date_order', 'source_documents'}
    if set(retained) != expected: raise ValueError('unexpected_reader_fields')
    validate_requirements({k: retained[k] for k in ('resolved_question', 'requirements')})
    if capture['response_format'] != response_format(retained['source_documents']):
        raise ValueError('reader_schema_changed')
    coverage_r = retained_intervals(retained, original, segment_mappings)
    generation = digest(encoded(original))
    class Originals:
        async def get_skip_tag_ids(self): return set()
        async def get_document(self, document_id):
            if document_id != original['id']: raise ValueError('unexpected_original')
            return json.loads(encoded(original))
    class Index:
        async def acquisition_document_page(self, *args, **kwargs): raise AssertionError('No discovery during arm preparation')
        async def get_open_feedback_document_ids(self, ids): return set()
        async def get_incomplete_document_ids(self, ids): return set()
        async def get_ingestion_fingerprints(self, ids): return {original['id']: PaperlessClient.ingestion_fingerprint(original)}
        async def get_doc_hash(self, document_id): return PaperlessClient.content_hash(original['content'])
    request = {k: retained[k] for k in ('question', 'resolved_question', 'evaluated_at', 'source_date_order')}
    request.update(mode='strict', conversation_context='', corpus_generation=generation)
    bundle = await SourceAcquisition(Index(), Originals(), lambda: asyncio.sleep(0, result=generation)).collect(
        request, [{'id': 'frozen_original', 'document_ids': [original['id']], 'status': 'complete',
                   'sampling': 'enumerated'}], Execution(concurrency=1))
    if not bundle.receipt['complete']: raise ValueError('full_original_transfer_incomplete')
    full = {**retained, 'source_documents': group_sources(evidence_spans(bundle.evidence_pack, citation_safe=True))}
    coverage_f = retained_intervals(full, original)
    arms = {}
    for name, payload, coverage in [('R', retained, coverage_r), ('F', full, coverage_f)]:
        prompt = capture['prompt'] if name == 'R' else json.dumps(payload, ensure_ascii=False)
        schema = response_format(payload['source_documents'])
        arms[name] = {'payload': payload, 'prompt': prompt, 'response_format': schema,
            'payload_sha256': digest(encoded(payload)), 'prompt_sha256': digest(prompt.encode()),
            'source_documents_bytes': len(encoded(payload['source_documents'])),
            'prompt_bytes': len(prompt.encode()), 'schema_bytes': len(encoded(schema)),
            'system_bytes': len(READER_PROMPT.encode()), 'coverage': coverage,
            'windows': len(payload['source_documents'][0]['windows'])}
    return {'id': case_id, 'document_id': original['id'], 'original_sha256': digest(encoded(original)),
            'original_text_sha256': digest(original['content'].encode()),
            'original_bytes': len(original['content'].encode()), 'original_codepoints': len(original['content']),
            'retained_capture_sha256': digest(encoded(capture)), 'system_sha256': digest(READER_PROMPT.encode()),
            'acquisition_inventory_digest': bundle.inventory_digest, 'arms': arms,
            'same_semantic_extent': coverage_r['missing_intervals'] == coverage_f['missing_intervals'],
            'provider_request_bytes': 'pending_frozen_runtime_serialization',
            'provider_context_capacity': 'not_established'}


async def prepare(descriptors, directory):
    """Exclusive private preparation only; incomplete packages cannot be admitted."""
    from scripts.eval_source_audit import write_private
    directory = Path(directory); directory.mkdir(mode=0o700, exist_ok=False)
    ids = set(); cases = []; hashes = {}
    for descriptor in descriptors:
        if descriptor['id'] in ids: raise ValueError('duplicate_case_id')
        ids.add(descriptor['id'])
        original_path, retained_path = Path(descriptor['original']), Path(descriptor['retained'])
        original_bytes, retained_bytes = original_path.read_bytes(), retained_path.read_bytes()
        original = original_from(json.loads(original_bytes), descriptor['document_id'])
        case = await prepare_case(descriptor['id'], original, json.loads(retained_bytes),
                                  segment_mappings=descriptor.get('segment_mappings'))
        case['input_file_sha256'] = {'original': digest(original_bytes), 'retained': digest(retained_bytes)}
        name = descriptor['id'] + '.json'
        if not descriptor['id'].replace('-', '').isalnum(): raise ValueError('invalid_case_path')
        write_private(directory / name, {'original': original, **case})
        hashes[name] = digest((directory / name).read_bytes()); cases.append(case['id'])
    write_private(directory / 'preparation.json', {'status': 'prepared_not_admitted', 'case_ids': cases,
        'sha256': hashes, 'native_model_calls': 0})
    return {'cases': len(cases), 'native_model_calls': 0, 'preparation_sha256': digest((directory/'preparation.json').read_bytes())}


async def provider_request_measurement(payload, *, model, include_protocol_correction=False):
    """Serialize through the pinned SDK into a local mock transport; no network.

    The empty response exists only to close the serialization path. It is never a
    model observation, a diagnostic outcome or an input to semantic grading.
    """
    import httpx
    from openai import AsyncOpenAI
    from strands import Agent
    from strands.models.openai import OpenAIModel
    from unittest.mock import patch
    from app import strands_orchestrator as module
    from scripts.eval_source_audit import configure_proxy_cache
    requests, clients = [], []
    async def handle(request):
        if request.url.host != '127.0.0.1': raise ValueError('serialization_must_be_local')
        body = json.loads(request.content)
        requests.append({'bytes': len(request.content), 'sha256': digest(request.content), 'body': body})
        result = {'documents': [{'document_id': payload['source_documents'][0]['document_id'],
                                 'observations': [], 'limitations': []}]}
        if include_protocol_correction and len(requests) == 1:result = {}
        chunk = {'id': 'serialization-only', 'object': 'chat.completion.chunk', 'created': 0,
            'model': model, 'choices': [{'index': 0, 'delta': {'role': 'assistant', 'content': json.dumps(result)}, 'finish_reason': None}]}
        terminal = {**chunk, 'choices': [{'index': 0, 'delta': {}, 'finish_reason': 'stop'}]}
        data = ''.join('data: ' + json.dumps(c) + '\n\n' for c in (chunk, terminal)) + 'data: [DONE]\n\n'
        return httpx.Response(200, content=data.encode(), headers={'content-type': 'text/event-stream'})
    def client_factory(**kwargs):
        kwargs.update(base_url='http://127.0.0.1:1', api_key='serialization-only', max_retries=0)
        client = httpx.AsyncClient(transport=httpx.MockTransport(handle)); clients.append(client)
        return AsyncOpenAI(http_client=client, **kwargs)
    orchestrator = module.StrandsQueryOrchestrator(); orchestrator.enabled = True
    original_model = orchestrator._model
    orchestrator._model = lambda **kwargs: configure_proxy_cache(original_model(**kwargs), 'bypass')
    try:
        with patch.object(module, 'Agent', Agent), patch.object(module, 'OpenAIModel', OpenAIModel), \
                patch.object(module.settings, 'strands_model', model), patch(
                'strands.models.openai.openai.AsyncOpenAI', side_effect=client_factory):
            await orchestrator.read_question_sources(payload)
        if len(requests) != (2 if include_protocol_correction else 1) or any(not c.is_closed for c in clients):
            raise ValueError('serialization_did_not_complete_once')
        return {**requests[0], **({'protocol_correction':requests[1]} if include_protocol_correction else {}), 'scope': 'local mock transport serialization only', 'native_model_calls': 0}
    finally:
        await orchestrator.close()
        for client in clients:
            if not client.is_closed: await client.aclose()
