"""Pinned-SDK source-record reader serialization, forced local MockTransport only."""
import asyncio
import hashlib
import json
from pathlib import Path

from app.source_records import SourceRecordInventory
from app.source_record_reader import read_records, project_reading
from scripts.source_recovery_preflight import mock_sdk
from scripts.run_source_recovery import code_identity, runtime_identity, schedule


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


async def prepare(inputs, output, model):
    from app.strands_orchestrator import StrandsQueryOrchestrator
    inputs, output = Path(inputs), Path(output)
    manifest_bytes = (inputs/'manifest.json').read_bytes()
    manifest = json.loads(manifest_bytes)
    frozen, code = {}, code_identity()
    if [(r['case'],r['repetition']) for r in manifest['pairs']] != [(r['case'],r['repetition']) for r in schedule()]:
        raise ValueError('Unexpected preflight schedule')
    for name, expected in manifest['sha256'].items():
        path = Path(name)
        if path.is_absolute() or '..' in path.parts:
            raise ValueError('Unbound preflight path')
        raw_bytes = (inputs/path).read_bytes()
        if hashlib.sha256(raw_bytes).hexdigest() != expected:
            raise ValueError('Frozen source input changed')
        frozen[name] = raw_bytes
    for row in manifest['pairs']:
        if any(manifest[k].get(row['case']) not in frozen for k in ('originals','requests')):
            raise ValueError('Unbound preflight source')
    output.mkdir(mode=0o700, exist_ok=False)
    def responder(body):
        content = body['messages'][-1]['content']
        payload = json.loads(content if isinstance(content, str) else ''.join(p['text'] for p in content))
        return {'blocks': [{'block_id': i, 'status': 'unresolved', 'observations': [],
                           'reason': 'Synthetic serialization response, not semantic evaluation.'}
                          for i in payload['focus_block_ids']]}
    raw, rows = [], []
    async with mock_sdk(model, responder, raw_bodies=raw):
        runtime = runtime_identity()
        for index, binding in enumerate(manifest['pairs']):
            case = binding['case']
            original = json.loads(frozen[manifest['originals'][case]])
            frozen_request = json.loads(frozen[manifest['requests'][case]])
            request = {k: frozen_request[k] for k in ('question','resolved_question','requirements','evaluated_at','source_date_order')}
            inventory = SourceRecordInventory.build([{'document_id': original['id'], 'content': original['content'],
                'content_digest': hashlib.sha256(original['content'].encode()).hexdigest()}])
            native = StrandsQueryOrchestrator(); native.enabled = True
            start = len(raw)
            try:
                result = await read_records(inventory, request, native, deadline=asyncio.get_running_loop().time()+120)
            finally:
                await native.close()
            if result.record['status'] != 'completed': raise ValueError('Mock reading incomplete')
            documents = frozen_request['source_documents']
            anchor = hashlib.sha256(json.dumps(documents,sort_keys=True,separators=(',',':'),ensure_ascii=False).encode()).hexdigest()
            projection = project_reading(result.bound_reading, documents, documents_digest=anchor)
            directory = output/f'case-{index:02d}'; directory.mkdir(mode=0o700)
            for offset, body in enumerate(raw[start:]):
                (directory/f'wire-{offset:03d}.json').write_bytes(body)
            for name, data in [('reading',result.record),('projection',projection.record)]:
                (directory/f'{name}.json').write_text(json.dumps(data,indent=2,ensure_ascii=False)+'\n')
            rows.append({'index':index,'case':case,'repetition':binding['repetition'],
                'requests':len(raw)-start,'largest_request_bytes':max(map(len,raw[start:])),
                'inventory_digest':inventory.digest})
    if code_identity() != code:
        raise ValueError('Preflight code changed')
    report = {'scope':'reader serialization and source projection only; audit requests and native admission pending',
        'native_model_calls':0,'model':model,'runtime':runtime,'code_sha256':code,
        'input_manifest_sha256':hashlib.sha256(manifest_bytes).hexdigest(),'pairs':rows,'requests':len(raw),
        'largest_request_bytes':max(map(len,raw))}
    (output/'report.json').write_text(json.dumps(report,indent=2)+'\n')
    inventory = {str(p.relative_to(output)):digest(p) for p in sorted(output.rglob('*')) if p.is_file()}
    (output/'inventory.json').write_text(json.dumps(inventory,indent=2)+'\n')
    return {k:report[k] for k in ('native_model_calls','requests','largest_request_bytes')}
