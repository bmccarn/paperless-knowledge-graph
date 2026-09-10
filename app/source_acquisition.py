"""Request-owned transfer of discovered original sources, without semantic exclusions."""
import asyncio
from dataclasses import dataclass
from datetime import date
import hashlib
import json
import math

from app.answer_finalization import evidence_spans
from app.evidence import build_evidence_item, build_evidence_pack
from app.history_coverage import HISTORY_WORDS
from app.evidence import query_terms
from app.paperless import PaperlessClient
from app.source_text import bind_document_context

ACQUISITION_VERSION = 'source-acquisition-v1'


def _json(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False, allow_nan=False)


def acquisition_digest(value):
    return hashlib.sha256(_json(value).encode()).hexdigest()


@dataclass(frozen=True)
class Execution:
    concurrency: int = 4
    page_size: int = 100
    deadline: float | None = None

    def validate(self):
        if (type(self.concurrency) is not int or self.concurrency < 1
                or type(self.page_size) is not int or self.page_size < 1
                or (self.deadline is not None and (type(self.deadline) not in (int, float)
                    or not math.isfinite(self.deadline)))):
            raise ValueError('invalid_acquisition_execution')


@dataclass(frozen=True)
class AcquisitionBundle:
    _pack: str
    _receipt: str
    inventory_digest: str

    @property
    def evidence_pack(self): return json.loads(self._pack)

    @property
    def receipt(self): return json.loads(self._receipt)


def _validate_request(request):
    required = {'question', 'resolved_question', 'conversation_context', 'mode', 'evaluated_at',
                'source_date_order', 'corpus_generation'}
    if (not isinstance(request, dict) or set(request) != required
            or any(not isinstance(request[k], str) for k in required - {'corpus_generation'})
            or not request['question'].strip() or not request['resolved_question'].strip()
            or len(request['conversation_context']) > 12000
            or request['mode'] not in {'quick', 'strict', 'deep', 'timeline'}
            or request['source_date_order'] not in {'mdy', 'dmy', 'reject_ambiguous'}
            or type(request['corpus_generation']) not in (str, int)
            or date.fromisoformat(request['evaluated_at']).isoformat() != request['evaluated_at']):
        raise ValueError('invalid_acquisition_request')


def _ids(values):
    if not isinstance(values, list) or any(type(i) is not int or i < 1 for i in values):
        raise ValueError('invalid_discovery_document_ids')
    return list(dict.fromkeys(values))


def _span_inventory(pack):
    result = {}
    for span in evidence_spans(pack, citation_safe=True):
        result.setdefault(span['document_id'], []).append({
            'start': span['start'], 'end': span['end'], 'span_id': span['span_id'],
            'reader_span_digest': acquisition_digest(span)})
    return result


def missing_intervals(length, spans):
    """Exact Unicode-offset union, including whitespace and interior gaps."""
    cursor, missing = 0, []
    for span in sorted(spans, key=lambda s: (s['start'], s['end'])):
        start, end = span['start'], span['end']
        if type(start) is not int or type(end) is not int or not 0 <= start < end <= length:
            raise ValueError('invalid_acquisition_interval')
        if start > cursor: missing.append([cursor, start])
        cursor = max(cursor, end)
    if cursor < length: missing.append([cursor, length])
    return missing


def validate_receipt(receipt, anchor, request=None):
    """The anchor is retained outside the receipt; recomputing it is not restoration."""
    if not isinstance(receipt, dict): raise ValueError('invalid_acquisition_receipt')
    body = {k: v for k, v in receipt.items() if k != 'digest'}
    if (receipt.get('version') != ACQUISITION_VERSION or acquisition_digest(body) != anchor
            or receipt.get('digest') != anchor
            or (request is not None and receipt.get('request_digest') != acquisition_digest(request))):
        raise ValueError('acquisition_binding_mismatch')
    complete = (receipt['snapshot_status'] == 'complete'
                and all(o['status'] == 'complete' for o in receipt['operations'])
                and all(d['state'] in {'supplied', 'ineligible'} for d in receipt['documents']))
    if type(receipt.get('complete')) is not bool or complete != receipt['complete']:
        raise ValueError('invalid_acquisition_completeness')
    return receipt


def validate_bundle(pack, receipt, anchor, request):
    _validate_request(request)
    validate_receipt(receipt, anchor, request)
    spans = _span_inventory(pack)
    items = {i['document_id']: i for i in pack['items']}
    admitted = {d['document_id'] for d in receipt['documents'] if d['admitted']}
    if len(items) != len(pack['items']) or set(items) != admitted:
        raise ValueError('acquisition_inventory_mismatch')
    for row in receipt['documents']:
        if not row['admitted']: continue
        item = items[row['document_id']]
        text = item['content']
        expected_context = {'document_id': row['document_id'], 'digest': row['content_digest'],
                            'start': 0, 'end': len(text)}
        if (item.get('_source_document_content') != text or item.get('source_context') != expected_context
                or item.get('source_kind') != 'ocr' or item.get('source_content', text) != text
                or len(text) != row['extent'] or hashlib.sha256(text.encode()).hexdigest() != row['content_digest']
                or spans.get(row['document_id'], []) != row['spans']
                or missing_intervals(len(text), row['spans']) != row['missing_intervals']):
            raise ValueError('acquisition_original_mismatch')
    return receipt


class SourceAcquisition:
    def __init__(self, index, originals, generation):
        self.index, self.originals, self.generation = index, originals, generation
        self._progress = None

    @property
    def progress(self):
        return json.loads(_json(self._progress)) if self._progress is not None else None

    async def collect(self, request, discovery, execution):
        execution.validate(); _validate_request(request)
        if not callable(self.generation): raise ValueError('invalid_acquisition_generation')
        for adapter, names in ((self.index, ('acquisition_document_page', 'get_open_feedback_document_ids',
                'get_incomplete_document_ids', 'get_ingestion_fingerprints', 'get_doc_hash')),
                (self.originals, ('get_skip_tag_ids', 'get_document'))):
            if any(not callable(getattr(adapter, name, None)) for name in names):
                raise ValueError('invalid_acquisition_adapter')
        request, discovery = json.loads(_json(request)), json.loads(_json(discovery))
        if not isinstance(discovery, list): raise ValueError('invalid_discovery')
        operations, leads, items = [], {}, {}
        def add(ids, path):
            for i in _ids(ids):
                paths = leads.setdefault(i, [])
                if path not in paths: paths.append(path)
        op_ids = set()
        for original in discovery:
            if (not isinstance(original, dict) or not isinstance(original.get('id'), str)
                    or not original['id'] or original['id'] in op_ids
                    or original.get('status') not in {'complete', 'failed', 'pending'}
                    or original.get('sampling') not in {'sampled', 'enumerated'}
                    or ('query' in original and not isinstance(original['query'], str))):
                raise ValueError('invalid_discovery_operation')
            op_ids.add(original['id']); add(original.get('document_ids', []), original['id'])
            operations.append(original)
        rows = {}
        snapshot_status = 'pending'
        self._progress = {'operations': operations, 'documents': rows, 'snapshot_status': 'pending'}

        async def enumerate_query(op):
            page_op = {'id': 'lexical:' + op['id'], 'query': op['query'], 'terms': sorted(query_terms(op['query']) - HISTORY_WORDS),
                       'sampling': 'enumerated', 'status': 'pending', 'pages': [], 'document_ids': []}
            operations.append(page_op)
            after, total = 0, None
            try:
                while True:
                    page = await self.index.acquisition_document_page(page_op['terms'], after=after, limit=execution.page_size)
                    ids = _ids(page['document_ids']); count = page['candidate_count']; nxt = page['next_after']
                    if (type(count) is not int or count < 0 or (total is not None and count != total)
                            or ids != sorted(ids) or len(ids) != len(page['document_ids'])
                            or any(i <= after for i in ids) or len(ids) > execution.page_size
                            or (nxt is not None and (type(nxt) is not int or not ids or nxt != ids[-1]))):
                        raise ValueError('invalid_discovery_page')
                    total = count
                    add(ids, page_op['id']); page_op['document_ids'].extend(ids)
                    page_op['pages'].append({'after': after, 'next_after': nxt, 'count': len(ids), 'total': total})
                    seen = len(page_op['document_ids'])
                    if seen > total or (nxt is None and seen != total) or (nxt is not None and seen >= total):
                        raise ValueError('incomplete_discovery_page')
                    if nxt is None: break
                    after = nxt
                page_op['status'] = 'complete'
            except Exception as exc:
                page_op.update(status='failed', error=type(exc).__name__)

        async def transfer(row, blocked, incomplete, fingerprints, skip_tags):
            i = row['document_id']
            try:
                doc = await self.originals.get_document(i)
                if not isinstance(doc, dict) or type(doc.get('id')) is not int or doc['id'] != i:
                    raise ValueError('original_identity_mismatch')
                if PaperlessClient.has_any_tag(doc, skip_tags):
                    row.update(state='ineligible', reason='configured_skip_tag'); return
                if i in blocked: row.update(state='feedback_blocked'); return
                if i in incomplete: row.update(state='unindexed'); return
                text = doc.get('content')
                if not isinstance(text, str) or not text.strip():
                    row.update(state='unavailable', reason='empty_original'); return
                content_hash = PaperlessClient.content_hash(text)
                fingerprint = PaperlessClient.ingestion_fingerprint(doc)
                row.update(content_digest=content_hash, extent=len(text), original_bytes=len(text.encode()), ingestion_fingerprint=fingerprint)
                if (await self.index.get_doc_hash(i) != content_hash or fingerprints.get(i) != fingerprint):
                    row.update(state='stale'); return
                item = build_evidence_item({'document_id': i, 'title': doc.get('title', ''),
                    'content': text, 'source_kind': 'ocr', 'chunk_index': 0}, row['priority'] + 1, request['question'])
                bind_document_context(item, text)
                spans = _span_inventory({'items': [item]}).get(i, [])
                missing = missing_intervals(len(text), spans)
                row.update(state='unavailable' if missing else 'supplied', admitted=True,
                           spans=spans, missing_intervals=missing)
                if missing: row['reason'] = 'citation_interval_gap'
                items[i] = item
            except Exception as exc:
                row.update(state='unavailable', error=type(exc).__name__)

        async def work():
            nonlocal snapshot_status
            if await self.generation() != request['corpus_generation']:
                raise ValueError('acquisition_generation_changed')
            # Fixed declared proposals, no recursive model expansion. Pagination must
            # progress within the first reported finite count.
            for op in discovery:
                if op.get('query', '').strip(): await enumerate_query(op)
            for priority, (i, paths) in enumerate(leads.items()):
                rows[i] = {'document_id': i, 'paths': paths, 'priority': priority, 'state': 'pending',
                           'admitted': False, 'spans': [], 'missing_intervals': []}
            ids = list(rows)
            skip_tags = await self.originals.get_skip_tag_ids()
            blocked = await self.index.get_open_feedback_document_ids(ids)
            incomplete = await self.index.get_incomplete_document_ids(ids)
            fingerprints = await self.index.get_ingestion_fingerprints(ids)
            iterator = iter(rows.values())
            async def worker():
                for row in iterator:
                    await transfer(row, blocked, incomplete, fingerprints, skip_tags)
            async with asyncio.TaskGroup() as group:
                for _ in range(min(execution.concurrency, len(rows))): group.create_task(worker())
            if await self.generation() != request['corpus_generation']:
                raise ValueError('acquisition_generation_changed')
            snapshot_status = 'complete'

        try:
            async with asyncio.timeout_at(execution.deadline): await work()
        except TimeoutError:
            snapshot_status = 'deadline_exhausted'
        except BaseException as exc:
            self._progress['snapshot_status'] = ('cancelled' if isinstance(exc, asyncio.CancelledError) else 'failed')
            raise
        self._progress['snapshot_status'] = snapshot_status
        # External cancellation propagates after TaskGroup joins; inconsistent
        # generations and shared datastore failures invalidate rather than certify.
        for priority, (i, paths) in enumerate(leads.items()):
            rows.setdefault(i, {'document_id': i, 'paths': paths, 'priority': priority,
                'state': 'pending', 'admitted': False, 'spans': [], 'missing_intervals': []})
        if snapshot_status != 'complete':
            # A timed-out snapshot was never checked at delivery; retain diagnostics
            # but do not send its originals to models.
            items.clear()
            for row in rows.values():
                row['admitted'] = False
        def expire_packaging():
            nonlocal snapshot_status
            if (snapshot_status != 'complete' or execution.deadline is None
                    or asyncio.get_running_loop().time() < execution.deadline):
                return False
            snapshot_status = 'deadline_exhausted'
            self._progress['snapshot_status'] = snapshot_status
            items.clear()
            for row in rows.values(): row['admitted'] = False
            return True

        def package():
            admitted_items = list(items.values())
            pack = build_evidence_pack(request['question'], {'mode': request['mode']},
                admitted_items, [], max_items=max(1, len(admitted_items)))
            # The standard pack derives UI aggregates, while these originals retain
            # their already checked whole-document provenance bindings.
            pack['items'] = admitted_items
            receipt = {'version': ACQUISITION_VERSION, 'request_digest': acquisition_digest(request),
                       'corpus_generation': request['corpus_generation'], 'snapshot_status': snapshot_status,
                       'operations': operations, 'documents': list(rows.values())}
            receipt['complete'] = (snapshot_status == 'complete' and all(o['status'] == 'complete' for o in operations)
                                  and all(r['state'] in {'supplied', 'ineligible'} for r in rows.values()))
            receipt['measurements'] = {'discovered_documents': len(rows), 'admitted_documents': len(items),
                'original_characters': sum(r.get('extent', 0) for r in rows.values() if r.get('spans')),
                'original_bytes': sum(r.get('original_bytes', 0) for r in rows.values() if r.get('spans')),
                'reference_windows': sum(len(r['spans']) for r in rows.values())}
            anchor = acquisition_digest(receipt); receipt['digest'] = anchor
            validate_bundle(pack, receipt, anchor, request)
            return AcquisitionBundle(_json(pack), _json(receipt), anchor)

        # Synchronous packaging cannot be preempted by asyncio's timer. Never
        # admit an original after that work has consumed the owner deadline.
        # Completed transfer diagnostics survive; this is not a wall-time bound.
        expire_packaging()
        bundle = package()
        if expire_packaging(): bundle = package()
        return bundle



def augment_acquisition_coverage(coverage, receipt):
    """Acquisition gaps constrain completeness without changing factual support."""
    if receipt is None:
        return coverage
    complete = coverage['complete'] and receipt['complete']
    return {**coverage, 'acquisition_complete': receipt['complete'],
            'acquisition_digest': receipt['digest'], 'complete': complete,
            'status': 'partial' if coverage['status'] == 'complete' and not complete else coverage['status']}
