import asyncio
import copy
import hashlib
import json
import unittest
from unittest.mock import AsyncMock, patch
from tests.runtime import configure_test_environment
configure_test_environment()
from app.source_records import SourceRecordInventory, SourceRecordError
from app.source_record_reader import read_records, requests, project_reading, SourceRecordTransportError
from app.strands_orchestrator import StrandsQueryOrchestrator


def inventory(text='Subject.\n\nCondition.', doc_id=1):
    return SourceRecordInventory.build([{'document_id': doc_id, 'content': text,
        'content_digest': hashlib.sha256(text.encode()).hexdigest()}])


def question():
    return {'question': 'What does the record establish?', 'resolved_question': 'What does the record establish?',
        'requirements': [{'id': 'r1', 'aspect': 'Recorded facts', 'temporal_scope': 'none', 'comparison_scope': 'none'}],
        'evaluated_at': '2026-09-10', 'source_date_order': 'mdy'}


def response(payload):
    return json.dumps({'blocks': [{'block_id': i, 'status': 'interpreted', 'reason': None,
        'observations': [{'text': 'Recorded fact.', 'references': [{'block_id': i}]}]}
        for i in payload['focus_block_ids']]})


class RecordReaderTests(unittest.IsolatedAsyncioTestCase):
    async def test_batches_cover_focus_once_with_full_own_original_and_bound_request(self):
        inv = inventory('\n\n'.join(f'Paragraph {i}.' for i in range(17)))
        q = question(); old = copy.deepcopy(q); seen = []
        async def read(payload):
            seen.append(copy.deepcopy(payload)); q['question'] = 'Caller mutation'
            answer = response(payload); payload['original_document']['blocks'].clear(); return answer
        adapter = type('Adapter', (), {'read_source_record_blocks': staticmethod(read)})()
        result = await read_records(inv, q, adapter, deadline=asyncio.get_running_loop().time()+10)
        self.assertEqual([len(r['focus_block_ids']) for r in seen], [8, 8, 1])
        self.assertTrue(all(len(r['original_document']['blocks']) == 17 for r in seen))
        self.assertTrue(all(r['question'] == old['question'] for r in seen))
        self.assertEqual(result.record['request_digest'], hashlib.sha256(json.dumps(old,sort_keys=True,separators=(',',':')).encode()).hexdigest())
        self.assertEqual(len(result.bound_reading.record['blocks']), 17)
        self.assertEqual(result.record['pending_block_ids'], [])

    async def test_invalid_batch_stops_without_retry_preserving_prior_and_raw_output(self):
        inv = inventory('\n\n'.join(f'Paragraph {i}.' for i in range(17)))
        n = 0
        async def read(payload):
            nonlocal n
            n += 1
            return response(payload) if n == 1 else '{bad'
        adapter = type('Adapter', (), {'read_source_record_blocks': staticmethod(read)})()
        result = await read_records(inv, question(), adapter, deadline=asyncio.get_running_loop().time()+10)
        self.assertEqual(n, 2); self.assertEqual(len(result.record['rows']), 8)
        self.assertEqual(len(result.record['pending_block_ids']), 9)
        self.assertEqual(result.record['attempts'][-1]['response'], '{bad')
        with self.assertRaises(SourceRecordError): _ = result.bound_reading

    async def test_expiry_transport_and_cancellation_have_owner_semantics(self):
        inv = inventory(); adapter = type('Adapter', (), {})()
        adapter.read_source_record_blocks = AsyncMock(side_effect=SourceRecordTransportError())
        result = await read_records(inv, question(), adapter, deadline=asyncio.get_running_loop().time()-1)
        adapter.read_source_record_blocks.assert_not_awaited()
        self.assertEqual(result.record['reason'], 'deadline_exhausted')
        result = await read_records(inv, question(), adapter, deadline=asyncio.get_running_loop().time()+10)
        self.assertEqual(result.record['reason'], 'transport_failed')
        entered, closed = asyncio.Event(), asyncio.Event()
        async def waiting(payload):
            entered.set()
            try: await asyncio.Event().wait()
            finally: closed.set()
        adapter.read_source_record_blocks = waiting
        task = asyncio.create_task(read_records(inv, question(), adapter, deadline=asyncio.get_running_loop().time()+10))
        await entered.wait(); task.cancel()
        with self.assertRaises(asyncio.CancelledError): await task
        self.assertTrue(closed.is_set())

    async def test_repeated_cancel_cannot_detach_adapter_cleanup(self):
        entered, cleaning, release = asyncio.Event(), asyncio.Event(), asyncio.Event()
        async def read(payload):
            entered.set()
            try: await asyncio.Event().wait()
            finally:
                cleaning.set()
                await release.wait()
        adapter = type('Adapter', (), {'read_source_record_blocks': staticmethod(read)})()
        task = asyncio.create_task(read_records(inventory(), question(), adapter, deadline=asyncio.get_running_loop().time()+10))
        await entered.wait(); task.cancel(); await cleaning.wait(); task.cancel()
        await asyncio.sleep(0); self.assertFalse(task.done())
        release.set()
        with self.assertRaises(asyncio.CancelledError): await task

    async def test_nested_timeout_cannot_cancel_cleanup_twice(self):
        cleaned = []
        async def read(payload):
            async with asyncio.timeout(.01):
                try: await asyncio.Event().wait()
                finally:
                    await asyncio.sleep(.04)
                    cleaned.append(True)
        adapter = type('Adapter', (), {'read_source_record_blocks': staticmethod(read)})()
        result = await read_records(inventory(), question(), adapter, deadline=asyncio.get_running_loop().time()+.025)
        self.assertEqual(cleaned, [True])
        self.assertEqual(result.record['reason'], 'deadline_exhausted')

    async def test_native_timeout_cleanup_survives_outer_deadline(self):
        from app.config import settings
        cleaned = []
        class Agent:
            def __init__(self, **kwargs): pass
            async def invoke_async(self, prompt):
                try: await asyncio.Event().wait()
                finally:
                    await asyncio.sleep(.08)
                    cleaned.append(True)
        native=StrandsQueryOrchestrator(); native.enabled=True
        with patch('app.strands_orchestrator.Agent',Agent), patch.object(native,'_model',return_value=None), patch.object(settings,'strands_call_timeout_seconds',1):
            result=await read_records(inventory(),question(),native,deadline=asyncio.get_running_loop().time()+1.04)
        self.assertEqual(cleaned,[True])
        self.assertEqual(result.record['reason'],'deadline_exhausted')

    async def test_native_adapter_retains_unexpected_failures_and_actual_stage(self):
        orchestrator = StrandsQueryOrchestrator(); orchestrator.enabled = True
        payload = requests(inventory(), question())[0]
        with patch.object(orchestrator, '_text_agent', AsyncMock(return_value=response(payload))) as native:
            self.assertEqual(await orchestrator.read_source_record_blocks(payload), response(payload))
            self.assertEqual(native.await_args.kwargs['name'], 'source_record_reader')
        with patch.object(orchestrator, '_text_agent', AsyncMock(side_effect=RuntimeError('program error'))):
            with self.assertRaises(RuntimeError): await orchestrator.read_source_record_blocks(payload)

    async def test_actual_strands_boundary_preserves_timeout_transport_and_integrity(self):
        import httpx
        native = StrandsQueryOrchestrator(); native.enabled = True
        for error, expected in ((TimeoutError(), 'adapter_timeout'),
                                (httpx.ConnectError('synthetic'), 'transport_failed'),
                                (ValueError('capture integrity'), None), (SourceRecordError('capture integrity'), None)):
            class Agent:
                def __init__(self, **kwargs): pass
                async def invoke_async(self, prompt): raise error
            with patch('app.strands_orchestrator.Agent', Agent), patch.object(native, '_model', return_value=None):
                if expected:
                    run = await read_records(inventory(), question(), native, deadline=asyncio.get_running_loop().time()+10)
                    self.assertEqual(run.record['reason'], expected)
                    self.assertEqual(len(run.record['attempts']), 1)
                else:
                    with self.assertRaisesRegex(ValueError, 'capture integrity'):
                        await read_records(inventory(), question(), native, deadline=asyncio.get_running_loop().time()+10)

    async def test_projection_retains_occurrences_and_rejects_gaps_or_changed_source(self):
        inv = inventory('First statement.\n\nConditional statement.')
        adapter = type('Adapter', (), {'read_source_record_blocks': staticmethod(lambda p: asyncio.sleep(0, result=response(p)))})()
        run = await read_records(inv, question(), adapter, deadline=asyncio.get_running_loop().time()+10)
        text = ''.join(b['content'] for b in inv.record['documents'][0]['blocks'])
        docs = [{'document_id': 1, 'windows': [{'span': {'span_id': 's1', 'document_id': 1, 'start': 0, 'end': 22, 'content': text[:22]}},
              {'span': {'span_id': 's2', 'document_id': 1, 'start': 18, 'end': len(text), 'content': text[18:]}}]}]
        for window in docs[0]['windows']: window['span']['content_digest'] = inv.record['documents'][0]['content_digest']
        anchor = hashlib.sha256(json.dumps(docs,sort_keys=True,separators=(',',':')).encode()).hexdigest()
        projected = project_reading(run.bound_reading, docs, documents_digest=anchor).reading
        self.assertEqual(len(projected['documents'][0]['observations']), 2)
        self.assertEqual(projected['documents'][0]['observations'][1]['references'], [{'span_id':'s1'},{'span_id':'s2'}])
        for variant in ('gap','changed','foreign','handle','digest','bool','null'):
            bad=copy.deepcopy(docs)
            if variant=='gap': bad[0]['windows'][1]['span'].update(start=25,content=text[25:])
            if variant=='changed': bad[0]['windows'][1]['span']['content']='changed'
            if variant=='handle': bad[0]['windows'][1]['span']['span_id']='foreign-handle'
            if variant=='digest': bad[0]['windows'][1]['span']['content_digest']='0'*64
            if variant=='bool': bad[0]['windows'][1]['span']['document_id']=True
            if variant=='null': bad[0]['windows'][1]['span']['span_id']=None
            if variant=='foreign': bad[0]['windows'][1]['span']['document_id']=2
            with self.subTest(variant=variant), self.assertRaises(SourceRecordError): project_reading(run.bound_reading,bad,documents_digest=anchor)
            if variant != 'handle':
                malformed_anchor=hashlib.sha256(json.dumps(bad,sort_keys=True,separators=(',',':')).encode()).hexdigest()
                with self.assertRaises(SourceRecordError): project_reading(run.bound_reading,bad,documents_digest=malformed_anchor)

    def test_request_rejects_unbound_context(self):
        with self.assertRaises(SourceRecordError): requests(inventory(), {**question(), 'prior_reading': 'wrong'})
