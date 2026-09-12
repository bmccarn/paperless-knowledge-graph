"""Closed source-scope experiment profile; execution/ownership lives in the shared runner."""
from dataclasses import dataclass
import hashlib
import json

from app.answer_finalization import evidence_spans
from app.question_evidence import QuestionEvidence, canonical_json
from app.source_reading import group_sources, parse_reading
from app.source_scopes import SourceScope
from scripts import run_model_comparison as runner

KIND = 'source-scope-diagnostic-v1'
PARTIALS = {'conditional-hours-prefix': ('conditional-hours', 119, 188),
            'refund-action-prefix': ('refund-action', 147, 263)}
PROFILES = ('control', 'scoped')


def require(condition):
    if not condition: raise runner.IntegrityFailure('Source scope diagnostic binding failed')


def digest(value):
    return hashlib.sha256(canonical_json(value).encode()).hexdigest()


def schedule():
    rows = []
    cases = (*runner.CASE_ORDER, *PARTIALS)
    for trial in (1, 2, 3):
        ordered = cases[::-1] if trial == 2 else cases[3:] + cases[:3] if trial == 3 else cases
        for index, case in enumerate(ordered):
            routes = runner.ROUTES[::-1] if trial == 2 or (trial == 3 and index % 2) else runner.ROUTES
            for route in routes:
                profiles = PROFILES[::-1] if (trial + index) % 2 == 0 else PROFILES
                for profile in profiles:
                    rows.append(dict(case=case, trial=trial, route=route, profile=profile,
                        order=['reader', 'fresh', 'baseline'] if trial == 2 else ['baseline', 'reader', 'fresh']))
    return rows


@dataclass(frozen=True, repr=False)
class PreparedExecution:
    _snapshot: str

    @property
    def record(self): return json.loads(self._snapshot)

    @property
    def digest(self): return hashlib.sha256(self._snapshot.encode()).hexdigest()


def original_record(original):
    content = original['content']
    return dict(document_id=original['id'], content=content, extent=len(content),
                content_digest=hashlib.sha256(content.encode()).hexdigest())


def prefix_pair(base, original, fixture):
    """Reconstruct the reviewed prefix pack; no full-source guard context is copied."""
    case, end, extent = PARTIALS[fixture['id']]
    require(fixture['case_id'] == case and len(original['content']) == extent)
    prefix = original['content'][:end]
    item = dict(document_id=original['id'], chunk_index=0, title=original['title'],
                content=prefix, source_kind='ocr', feedback_open=False)
    item['id'] = digest(dict(case_id=case, document_id=original['id'],
        original_content_sha256=hashlib.sha256(original['content'].encode()).hexdigest(), start=0, end=end))[:24]
    pack = dict(question=base['request']['question'], mode='strict', items=[item], coverage={
        'retrieval_is_exhaustive': False, 'source_document_count': 1, 'available_chunk_count': 1})
    spans = evidence_spans(pack, citation_safe=True)
    request = {**base['request'], 'source_documents': group_sources(spans)}
    chunk = dict(document_id=original['id'], content_digest=hashlib.sha256(prefix.encode()).hexdigest(), start=0, end=end)
    require(canonical_json(pack) == canonical_json(fixture['evidence_pack'])
            and canonical_json(request) == canonical_json(fixture['request'])
            and fixture['canonical_spans_sha256'] == digest(spans)
            and canonical_json(fixture['chunk_bindings']) == canonical_json([chunk]))
    scope = SourceScope.bind([original_record(original)], spans, chunks=[chunk])
    require(canonical_json(scope.view(spans).documents[0]['source_scope']) == canonical_json(fixture['expected_scope']))
    return dict(request=request, evidence_pack=pack), [chunk]


def prepare(manifest, frozen):
    """Input preparation never dispatches and never substitutes for admission."""
    require(manifest.get('kind') == KIND and canonical_json(manifest.get('executions')) == canonical_json(schedule()))
    parent = runner.bound(frozen, manifest['retained_manifest'])
    retained = {name: frozen['retained/' + name] for name in parent['sha256']}
    for name, raw in retained.items():
        require(hashlib.sha256(raw).hexdigest() == parent['sha256'][name])
    runner.validate_sources(parent, retained)
    challenge = runner.bound(frozen, manifest['challenges'])
    fixtures = {r['id']: r for r in challenge['partial_fixtures']}
    require(len(fixtures) == len(challenge['partial_fixtures']) and set(fixtures) == set(PARTIALS))
    require(len(challenge['challenges']) == 10 and len({c['id'] for c in challenge['challenges']}) == 10)
    cases = {}
    for case in (*runner.CASE_ORDER, *PARTIALS):
        parent_case = PARTIALS[case][0] if case in PARTIALS else case
        pair, original = runner.execution_inputs(parent, retained, {'case': parent_case})
        chunks = []
        if case in PARTIALS:
            pair, chunks = prefix_pair(pair, original, fixtures[case])
            primary = dict(documents=[dict(document_id=original['id'], observations=[], limitations=[])])
        else:
            primary = pair['primary']
        selected = [c for c in challenge['challenges'] if c['case_id'] == parent_case
                    and c['fixture_variant'] == (case if case in PARTIALS else 'full')]
        for row in selected:
            require(row['document_id'] == original['id'])
            primary['documents'][0]['observations'].append(row['observation'])
        parse_reading(canonical_json(primary), pair['request']['source_documents'])
        pair['primary'] = primary
        spans = evidence_spans(pair['evidence_pack'], citation_safe=True)
        SourceScope.bind([original_record(original)], spans, chunks=chunks)
        cases[case] = dict(pair=pair, original=original, chunks=chunks,
                          challenge_ids=[r['id'] for r in selected])
    require(sum(len(c['challenge_ids']) for c in cases.values()) == 10)
    return tuple(PreparedExecution(canonical_json(dict(index=i, binding=b, **cases[b['case']])))
                 for i, b in enumerate(schedule()))


def execution_context(prepared):
    record = prepared.record
    pair = record['pair']
    request, primary, previous, plan = runner.recovery.prepared_inputs(pair)
    empty = {'documents': [dict(document_id=d['document_id'], observations=[], limitations=[])
                           for d in request['source_documents']]}
    scope = None
    snapshot = json.loads(previous._snapshot)
    if record['binding']['profile'] == 'scoped':
        spans = evidence_spans(pair['evidence_pack'], citation_safe=True)
        scope = SourceScope.bind([original_record(record['original'])], spans, chunks=record['chunks'])
        snapshot['source_scope_digest'] = scope.view(spans).digest
    evidence = QuestionEvidence(canonical_json(snapshot), canonical_json(empty), scope)
    return pair, request, primary, evidence, plan, scope


async def execute(prepared, orchestrator, capture, directory):
    pair, request, primary, evidence, plan, scope = execution_context(prepared)
    return await runner.execute_stages(pair, request, primary, evidence, plan,
        prepared.record['binding']['order'], orchestrator, directory, capture, source_scope=scope)


def validate_admission(manifest, frozen, prepared):
    runner.validate_admission(manifest, frozen, kind=KIND, artifacts=(
        'protocol', 'retained_manifest', 'challenges', 'preflight', 'destinations'))
    require(len(prepared) == len(schedule()))
    for execution in prepared: known_wires(manifest, frozen, execution)


def known_wires(manifest, frozen, prepared):
    from app.answer_observations import ObservationCandidate
    from app.source_scopes import ScopedReading
    report = runner.bound(frozen, manifest['preflight'])
    data = prepared.record; index, binding = data['index'], data['binding']
    require(report.get('scope') == 'source scope SDK localhost MockTransport only'
            and type(report.get('native_model_calls')) is int and report['native_model_calls'] == 0
            and report.get('code_sha256') == manifest['code_sha256']
            and len(report.get('rows', [])) == len(schedule())
            and report.get('input_manifest_sha256') == manifest['sha256'].get('manifest-prepared.json')
            and report.get('runtimes', {}).get(binding['route']) == manifest['runtimes'][binding['route']])
    row = report['rows'][index]
    require(type(row.get('index')) is int and row['index'] == index and row.get('prepared_digest') == prepared.digest
            and canonical_json({k: row.get(k) for k in binding}) == canonical_json(binding))
    base = f'preflight/execution-{index:02d}/'
    artifacts = runner.preflight_artifacts(manifest, frozen, row, base)
    def artifact(name):
        require(name in artifacts)
        return artifacts[name]
    stages = artifact('stages.json')
    require(set(stages) == set(binding['order']) and all(v.get('status') == 'completed' for v in stages.values()))
    pair, request, primary, evidence, plan, scope = execution_context(prepared)
    fresh = parse_reading(canonical_json(artifact('reading.json')), request['source_documents'])
    if scope is not None:
        receipt = artifact('reading-scope-receipt.json')
        ScopedReading(canonical_json(dict(reading=fresh, receipt=receipt))).validate(scope, request['source_documents'])
    else:
        require('reading-scope-receipt.json' not in artifacts)
    def candidate(reading):
        return ObservationCandidate.from_response({'observations': [o['text'] for d in reading['documents']
                                                    for o in d['observations']]})
    fixed_candidate, fresh_candidate = candidate(primary), candidate(fresh)
    subset_candidate, _ = fresh_candidate.supported_subset(artifact('fresh-audit-00.json')['claims'])
    require(subset_candidate is not None and 0 < len(subset_candidate.observations) < len(fresh_candidate.observations))
    subset_candidate.supported_subset(artifact('fresh-audit-01.json')['claims'])
    def batches(candidate):
        values = candidate.units()
        return [values[i:i+4] for i in range(0, len(values), 4)]
    view = evidence.scope_view
    documents = view.documents if view is not None else request['source_documents']
    first = {**request, 'source_documents': documents}
    corrected = {**first, 'reading_protocol_correction': {
        'error': 'invalid_source_reading',
        'instruction': 'Re-read these unchanged originals and return the required structure '
                       + ('using only this document ID and its offered typed references. ' if view is not None else
                          'using only this document ID and its exact supplied span_id references. ') +
                       'This corrects the response protocol, not a requested factual verdict.'}}
    readers, seen_fixed, known = [], [], []
    fresh_initial, fresh_corrected, fresh_subset = [], [], []
    wires = sorted((v for k, v in artifacts.items() if k.startswith('wire-')), key=lambda w: w['model_index'])
    require(len(wires) == row.get('requests') and [w['model_index'] for w in wires] == list(range(len(wires))))
    for wire in wires:
        body, payload, stage = runner.preflight_wire(wire, artifact, binding['route'])
        operation = wire['operation']
        if operation == 'reader':
            require(stage['name'] == 'source_reader')
            readers.append(payload); known.append(body)
            continue
        require(operation in {'baseline', 'fresh'} and stage['name'] == 'source_auditor')
        require(canonical_json(payload['source_documents']) == canonical_json(documents)
                and all(payload.get(k) == request[k] for k in request if k != 'source_documents')
                and payload.get('source_reading') == json.loads(evidence._reading))
        require(payload.get('source_scope_digest') == (view.digest if view is not None else None))
        require(isinstance(payload.get('units'), list) and bool(payload['units'])
                and payload.get('expected_unit_ids') == [u['id'] for u in payload['units']]
                and payload.get('unitization') == ObservationCandidate.strategy
                and payload.get('answer_context') == '')
        if operation == 'baseline':
            require(wire['audit_generation'] == 0 and payload.get('protocol_correction') is None)
            seen_fixed.append(payload['units']); known.append(body)
        else:
            require(type(wire['audit_generation']) is int and wire['audit_generation'] in (0, 1))
            if wire['audit_generation'] == 1:
                require(payload.get('protocol_correction') is None)
                fresh_subset.append(payload['units'])
            elif payload.get('protocol_correction') is None:
                fresh_initial.append(payload['units'])
            else:
                fresh_corrected.append(payload['units'])
    require(readers == [first, corrected]
            and sorted(map(canonical_json, seen_fixed)) == sorted(map(canonical_json, batches(fixed_candidate)))
            and sorted(map(canonical_json, fresh_initial)) == sorted(map(canonical_json, batches(fresh_candidate)))
            and sorted(map(canonical_json, fresh_corrected)) == sorted(map(canonical_json, batches(fresh_candidate)))
            and sorted(map(canonical_json, fresh_subset)) == sorted(map(canonical_json, batches(subset_candidate))))
    artifact('baseline-audit-00.json'); artifact('fresh-audit-00.json'); artifact('fresh-audit-01.json')
    require(row.get('total_request_bytes') == sum(w['bytes'] for w in wires)
            and row.get('largest_request_bytes') == max(w['bytes'] for w in wires))
    return known
