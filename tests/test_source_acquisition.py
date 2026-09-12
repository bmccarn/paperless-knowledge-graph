"""Discovered originals survive ranking and paging; gaps cannot become completeness."""
import asyncio
import copy
import unittest
from tests.runtime import configure_test_environment
configure_test_environment()
from app.source_acquisition import SourceAcquisition, Execution, validate_bundle, acquisition_digest
from app.paperless import PaperlessClient


class Originals:
    def __init__(self, docs): self.docs = docs; self.active = 0; self.peak = 0
    async def get_skip_tag_ids(self): return {99}
    async def get_document(self, doc_id):
        self.active += 1; self.peak = max(self.peak, self.active)
        try:
            await asyncio.sleep(0)
            value = self.docs[doc_id]
            if isinstance(value, Exception): raise value
            return copy.deepcopy(value)
        finally: self.active -= 1


class Index:
    def __init__(self, docs):
        self.ids = sorted(docs); self.calls = []; self.blocked = set(); self.missing = set()
        self.hashes = {i: PaperlessClient.content_hash(d['content']) for i, d in docs.items()}
        self.fingerprints = {i: PaperlessClient.ingestion_fingerprint(d) for i, d in docs.items()}
    async def acquisition_document_page(self, terms, *, after, limit):
        self.calls.append(after)
        rows = [i for i in self.ids if i > after][:limit]
        return {'document_ids': rows, 'candidate_count': len(self.ids),
                'next_after': rows[-1] if rows and rows[-1] != self.ids[-1] else None}
    async def get_open_feedback_document_ids(self, ids): return self.blocked & set(ids)
    async def get_incomplete_document_ids(self, ids): return self.missing & set(ids)
    async def get_ingestion_fingerprints(self, ids): return {i: self.fingerprints.get(i) for i in ids}
    async def get_doc_hash(self, i): return self.hashes.get(i)


def docs(count):
    return {i: {'id': i, 'title': f'Record {i}', 'content': f'Station {i} reports normal pressure.\n', 'tags': []}
            for i in range(1, count + 1)}


def request():
    return {'question': 'How did the stations change?', 'resolved_question': 'Station history',
            'conversation_context': 'Original prior question', 'mode': 'strict',
            'evaluated_at': '2026-09-10', 'source_date_order': 'mdy', 'corpus_generation': 'g1'}


class AcquisitionTests(unittest.IsolatedAsyncioTestCase):
    async def collect(self, originals, index=None, discovery=None, execution=None):
        self.originals = Originals(originals); self.index = index or Index(originals)
        self.collector = SourceAcquisition(self.index, self.originals, lambda: asyncio.sleep(0, result='g1'))
        return await self.collector.collect(request(), discovery or [
            {'id': 'planned:0', 'query': 'station pressure', 'status': 'complete',
             'sampling': 'sampled', 'document_ids': []}], execution or Execution(concurrency=3, page_size=71))

    async def test_packaging_expiration_withholds_completed_originals(self):
        from unittest.mock import patch
        from app import source_acquisition as module
        loop = asyncio.get_running_loop()
        current = [loop.time()]
        deadline = current[0] + 100
        original_pack = module.build_evidence_pack
        def expensive_pack(*args, **kwargs):
            result = original_pack(*args, **kwargs)
            current[0] = deadline + 1
            return result
        with patch.object(loop, 'time', lambda: current[0]), patch.object(module, 'build_evidence_pack', expensive_pack):
            bundle = await self.collect(docs(1), execution=Execution(deadline=deadline))
        self.assertFalse(bundle.receipt['complete'])
        self.assertEqual(bundle.receipt['snapshot_status'], 'deadline_exhausted')
        self.assertEqual(bundle.evidence_pack['items'], [])
        row = self.collector.progress['documents']['1']
        self.assertEqual(row['state'], 'supplied')
        self.assertFalse(row['admitted'])
        self.assertTrue(row['spans'])

    async def test_blank_ocr_runs_are_transferred_without_becoming_blank_citations(self):
        from app.answer_finalization import evidence_spans, validate_reference
        original = docs(1)
        text = (' ' * 12000 + 'Station was inspected.\n' + ' ' * 24000
                + 'Completion is not recorded.\n' + ' ' * 12000)
        original[1]['content'] = text
        bundle = await self.collect(original)
        self.assertTrue(bundle.receipt['complete'])
        self.assertEqual(bundle.evidence_pack['items'][0]['content'], text)
        spans = evidence_spans(bundle.evidence_pack, citation_safe=True)
        self.assertTrue(all(span['content'].strip() for span in spans))
        self.assertEqual(bundle.receipt['documents'][0]['missing_intervals'], [])
        for span in spans:
            self.assertEqual(span['content'], text[span['start']:span['end']])
            self.assertIsNotNone(validate_reference({'span_id':span['span_id']}, spans))

    async def test_all_521_matches_and_late_sections_transfer(self):
        originals = docs(521)
        originals[9]['content'] = 'Initial station inspection.\n' * 500 + 'Final qualification: proposed only.\n'
        bundle = await self.collect(originals)
        receipt = bundle.receipt
        self.assertTrue(receipt['complete'])
        self.assertEqual(len(bundle.evidence_pack['items']), 521)
        self.assertGreater(len(self.index.calls), 7)
        self.assertLessEqual(self.originals.peak, 3)
        ninth = next(x for x in bundle.evidence_pack['items'] if x['document_id'] == 9)
        self.assertEqual(ninth['content'], originals[9]['content'])
        validate_bundle(bundle.evidence_pack, receipt, bundle.inventory_digest, request())
        changed = bundle.receipt; changed['documents'].pop()
        changed['digest'] = acquisition_digest({k: v for k, v in changed.items() if k != 'digest'})
        with self.assertRaises(ValueError): validate_bundle(bundle.evidence_pack, changed, bundle.inventory_digest, request())

    async def test_graph_only_leads_and_every_query_are_accounted(self):
        original = docs(12); index = Index(original); index.ids = []
        discovery = [{'id': f'gap:{i}', 'query': f'station pressure {i}', 'status': 'complete',
                      'sampling': 'sampled', 'document_ids': [i + 1, 12]} for i in range(5)]
        bundle = await self.collect(original, index, discovery)
        self.assertEqual({r['document_id'] for r in bundle.receipt['documents']}, {1, 2, 3, 4, 5, 12})
        self.assertEqual(len(self.index.calls), 5)
        self.assertEqual(len(next(r for r in bundle.receipt['documents'] if r['document_id'] == 12)['paths']), 5)

    async def test_known_gaps_survive_and_cannot_be_removed_from_receipt(self):
        original = docs(6); index = Index(original)
        index.blocked = {2}; index.missing = {3}; original[4]['tags'] = [99]
        original[5]['title'] = 'Changed metadata without OCR change'
        original[6] = OSError('private details must not escape')
        bundle = await self.collect(original, index)
        self.assertFalse(bundle.receipt['complete'])
        self.assertEqual([r['state'] for r in bundle.receipt['documents']],
                         ['supplied', 'feedback_blocked', 'unindexed', 'ineligible', 'stale', 'unavailable'])
        self.assertNotIn('private details', str(bundle.receipt))
        changed = bundle.receipt; changed['documents'] = changed['documents'][:1]; changed['complete'] = True
        changed['digest'] = acquisition_digest({k: v for k, v in changed.items() if k != 'digest'})
        with self.assertRaises(ValueError): validate_bundle(bundle.evidence_pack, changed, bundle.inventory_digest, request())

    async def test_nonprogressing_page_retains_first_page_leads_as_incomplete(self):
        original = docs(3); index = Index(original)
        async def bad(*args, **kwargs):
            return {'document_ids': [1], 'candidate_count': 3, 'next_after': 1}
        index.acquisition_document_page = bad
        bundle = await self.collect(original, index)
        self.assertFalse(bundle.receipt['complete'])
        self.assertEqual([r['document_id'] for r in bundle.receipt['documents']], [1])
        self.assertEqual(bundle.receipt['operations'][-1]['status'], 'failed')

    async def test_invalid_controls_fail_before_io(self):
        for execution in (Execution(concurrency=0), Execution(page_size=0), Execution(deadline=float('nan'))):
            with self.subTest(execution=execution), self.assertRaises(ValueError):
                await self.collect(docs(1), execution=execution)
            self.assertEqual(self.index.calls, [])

    async def test_cancellation_drains_owned_workers(self):
        original = docs(10); index = Index(original); sources = Originals(original)
        entered = asyncio.Event(); active = 0
        async def blocked(i):
            nonlocal active
            active += 1; entered.set()
            try: await asyncio.Event().wait()
            finally: active -= 1
        sources.get_document = blocked
        collector = SourceAcquisition(index, sources, lambda: asyncio.sleep(0, result='g1'))
        task = asyncio.create_task(collector.collect(request(), [{'id': 'direct', 'status': 'complete',
            'sampling': 'sampled', 'document_ids': list(original)}], Execution(concurrency=2)))
        await entered.wait(); task.cancel()
        with self.assertRaises(asyncio.CancelledError): await task
        self.assertEqual(active, 0)

    async def test_full_reader_context_and_eligibility_are_bound(self):
        bundle = await self.collect(docs(2))
        from app.source_text import bind_document_context
        for change in ('title', 'feedback', 'context', 'tail'):
            pack = bundle.evidence_pack; item = pack['items'][0]
            if change == 'title': item['title'] = 'Different authoritative record'
            if change == 'feedback': item['feedback_open'] = True
            if change == 'context': bind_document_context(item, 'Different earlier record.\n' + item['content'])
            if change == 'tail':
                item['content'] = item['content'][:-5]
                bind_document_context(item, item['content'])
            with self.subTest(change=change), self.assertRaises(ValueError):
                validate_bundle(pack, bundle.receipt, bundle.inventory_digest, request())

    async def test_deadline_withholds_admission_but_preserves_transfer_work(self):
        original = docs(2); sources = Originals(original)
        get = sources.get_document
        async def partial(i):
            if i == 2: await asyncio.Event().wait()
            return await get(i)
        sources.get_document = partial
        collector = SourceAcquisition(Index(original), sources, lambda: asyncio.sleep(0, result='g1'))
        bundle = await collector.collect(request(), [{'id': 'direct', 'status': 'complete',
            'sampling': 'sampled', 'document_ids': [1, 2]}],
            Execution(concurrency=1, deadline=asyncio.get_running_loop().time() + .05))
        self.assertFalse(bundle.receipt['complete'])
        self.assertEqual(bundle.evidence_pack['items'], [])
        self.assertEqual(bundle.receipt['documents'][0]['state'], 'supplied')
        self.assertTrue(bundle.receipt['documents'][0]['spans'])
        self.assertEqual(bundle.receipt['documents'][1]['state'], 'pending')
        self.assertGreater(bundle.receipt['measurements']['original_bytes'], 0)
        self.assertEqual(collector.progress['snapshot_status'], 'deadline_exhausted')
