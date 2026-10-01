"""Source-reading mechanics through the native reader interface, not model accuracy."""
import asyncio
import copy
import json
import unittest
from unittest.mock import AsyncMock, patch

from tests.runtime import configure_test_environment
configure_test_environment()
from app.strands_orchestrator import StrandsQueryOrchestrator
from app.answer_finalization import evidence_spans
from app.source_reading import SourceReadingError, group_sources, response_format
from tests.source_audit_fixtures import decision


class SourceReadingTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.pack = {'items': [
            {'id': 'receipt', 'document_id': 17, 'chunk_index': 0, 'title': 'Receipt',
             'source_kind': 'ocr', 'content': 'Vendor completed a refund of $125.',
             'source_content': 'Vendor completed a refund of $125.'},
            {'id': 'request', 'document_id': 18, 'chunk_index': 0, 'title': 'Request',
             'source_kind': 'ocr', 'content': 'A refund of $250 was requested.',
             'source_content': 'A refund of $250 was requested.'}]}
        self.spans = evidence_spans(self.pack, citation_safe=True)
        self.units = [{'id': 'u1', 'text': 'CANDIDATE-ONLY text', 'start': 0, 'end': 19}]
        self.reading = {'documents': [{'document_id': s['document_id'],
            'observations': [{'text': 'PRIVATE NOTE describing source field roles.',
                              'references': [{'span_id': s['span_id']}]}],
            'limitations': []} for s in self.spans]}
        self.verdict = json.dumps({'assessments': [decision(references=[{'span_id': self.spans[0]['span_id']}])]})

    def auditor(self):
        auditor = StrandsQueryOrchestrator()
        auditor.enabled = True
        return auditor

    def read(self, auditor, spans=None):
        payload = {'question': 'Question', 'evaluated_at': '2026-09-09', 'source_date_order': 'day_first',
                   'source_documents': group_sources(self.spans if spans is None else spans)}
        return auditor.read_question_sources(payload)

    async def test_reader_wire_requires_independent_scope_and_referenced_negative_facts(self):
        auditor = self.auditor()
        reading = json.dumps({'documents': [self.reading['documents'][0]]})
        with patch.object(auditor, '_text_agent', AsyncMock(return_value=reading)) as calls:
            await self.read(auditor, self.spans[:1])
        reader = calls.await_args_list[0].kwargs
        self.assertIn('Each observation', reader['system_prompt'])
        self.assertIn('without relying on sibling observations', reader['system_prompt'])
        self.assertIn('material negative facts', reader['system_prompt'])
        self.assertIn('Missing records do not prove', reader['system_prompt'])
        properties = reader['response_format']['json_schema']['schema']['properties']['documents']['items']['properties']
        observation = properties['observations']['items']['properties']
        self.assertIn('subject or record', observation['text']['description'])
        self.assertIn('referenced observations', properties['limitations']['description'])

    async def test_audit_without_prepared_evidence_sends_spans_in_one_call(self):
        auditor = self.auditor()
        with patch.object(auditor, '_text_agent', AsyncMock(return_value=self.verdict)) as calls:
            await auditor.audit_answer_units('Question', self.units, self.spans, {})
        payload = json.loads(calls.await_args.kwargs['prompt'])
        self.assertEqual(payload['source_spans'], self.spans)
        self.assertNotIn('source_documents', payload)
        self.assertEqual(calls.await_count, 1)

    async def test_invalid_reader_never_falls_back(self):
        valid = {'documents': [self.reading['documents'][0]]}
        changes = [
            lambda d: d['documents'].pop(),
            lambda d: d['documents'].append(copy.deepcopy(d['documents'][0])),
            lambda d: d['documents'][0].update(document_id=True),
            lambda d: d['documents'][0].update(extra='not allowed'),
            lambda d: d['documents'][0]['observations'][0]['references'][0].update(span_id='foreign'),
            lambda d: d['documents'][0]['observations'][0]['references'][0].update(span_id=self.spans[1]['span_id']),
            lambda d: d['documents'][0]['observations'][0].update(references=[]),
            lambda d: d['documents'][0]['observations'][0].update(text=''),
        ]
        responses = [None, '', 'not json', '{"documents":[],"documents":[]}']
        for change in changes:
            d = copy.deepcopy(valid); change(d); responses.append(json.dumps(d))
        for response in responses:
            with self.subTest(response=response):
                auditor = self.auditor()
                with patch.object(auditor, '_text_agent', AsyncMock(return_value=response)):
                    with self.assertRaises(SourceReadingError):
                        await self.read(auditor, self.spans[:1])

    def test_vertex_schema_has_no_non_string_enums(self):
        def check(value):
            if isinstance(value, dict):
                if 'enum' in value:
                    self.assertEqual(value['type'], 'string')
                    self.assertTrue(all(isinstance(v, str) for v in value['enum']))
                for child in value.values():
                    check(child)
            elif isinstance(value, list):
                for child in value:
                    check(child)
        check(response_format(group_sources(self.spans)))


class DocumentLocalReadingTests(unittest.IsolatedAsyncioTestCase):
    setUp = SourceReadingTests.setUp
    auditor = SourceReadingTests.auditor
    read = SourceReadingTests.read

    async def test_each_reader_is_isolated_and_completion_order_does_not_reorder_notes(self):
        auditor = self.auditor()
        second_done = asyncio.Event()
        readers = []

        async def transport(name, system_prompt, prompt, **kwargs):
            payload = json.loads(prompt)
            readers.append(payload)
            documents = payload['source_documents']
            self.assertEqual(len(documents), 1)
            doc_id = documents[0]['document_id']
            if doc_id == 17:
                await second_done.wait()
            else:
                second_done.set()
            return json.dumps({'documents': [r for r in self.reading['documents'] if r['document_id'] == doc_id]})

        with patch.object(auditor, '_text_agent', side_effect=transport):
            result = await self.read(auditor)
        self.assertEqual(result, self.reading)
        self.assertEqual(len(readers), 2)
        for reader in readers:
            self.assertEqual(set(reader), {'question', 'evaluated_at', 'source_date_order', 'source_documents'})

    async def test_failure_cancels_and_drains_other_readers_before_return(self):
        auditor = self.auditor()
        started, drained = asyncio.Event(), asyncio.Event()

        async def transport(name, system_prompt, prompt, **kwargs):
            doc_id = json.loads(prompt)['source_documents'][0]['document_id']
            if doc_id == 17:
                started.set()
                try:
                    await asyncio.Event().wait()
                finally:
                    drained.set()
            await started.wait()
            # This other document's valid handle must fail local ownership.
            return json.dumps({'documents': [self.reading['documents'][0]]})

        with patch.object(auditor, '_text_agent', side_effect=transport):
            with self.assertRaises(SourceReadingError):
                await self.read(auditor)
        self.assertTrue(drained.is_set())

    async def test_parent_cancellation_drains_reader_tasks(self):
        auditor = self.auditor()
        ready = asyncio.Event()
        active = set()

        async def transport(name, system_prompt, prompt, **kwargs):
            task = asyncio.current_task()
            active.add(task)
            if len(active) == 2:
                ready.set()
            try:
                await asyncio.Event().wait()
            finally:
                active.remove(task)

        with patch.object(auditor, '_text_agent', side_effect=transport):
            task = asyncio.create_task(self.read(auditor))
            await ready.wait()
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task
        self.assertFalse(active)

    async def test_reader_task_count_is_bounded_and_all_documents_are_read(self):
        from app.config import settings
        spans = [{**self.spans[0], 'document_id': i + 100, 'span_id': f'span-{i}'} for i in range(20)]
        active, maximum, seen = 0, 0, []

        async def transport(name, system_prompt, prompt, **kwargs):
            nonlocal active, maximum
            active += 1
            maximum = max(maximum, active)
            doc_id = json.loads(prompt)['source_documents'][0]['document_id']
            seen.append(doc_id)
            try:
                await asyncio.sleep(0)
                return json.dumps({'documents': [{'document_id': doc_id, 'observations': [], 'limitations': []}]})
            finally:
                active -= 1

        with patch.object(settings, 'strands_max_concurrent_calls', 2):
            auditor = self.auditor()
            with patch.object(auditor, '_text_agent', side_effect=transport):
                await self.read(auditor, spans)
        self.assertEqual(maximum, 2)
        self.assertEqual(seen, list(range(100, 120)))

    async def test_simultaneous_readings_share_native_call_limit(self):
        from types import SimpleNamespace
        from app.config import settings
        from app import strands_orchestrator as native
        active, maximum = 0, 0
        stages = []
        spans = [{**self.spans[0], 'document_id': i + 100, 'span_id': f'span-{i}'} for i in range(8)]

        class Result:
            stop_reason = 'end_turn'
            metrics = SimpleNamespace(accumulated_usage={})
            def __init__(self, text):
                self.text = text
            def __str__(self):
                return self.text

        class Agent:
            def __init__(self, **kwargs):
                self.name = kwargs['name']
            async def invoke_async(self, prompt):
                nonlocal active, maximum
                active += 1
                maximum = max(maximum, active)
                stages.append(self.name)
                try:
                    await asyncio.sleep(0.001)
                    doc = json.loads(prompt)['source_documents'][0]
                    return Result(json.dumps({'documents': [{'document_id': doc['document_id'],
                        'observations': [], 'limitations': []}]}))
                finally:
                    active -= 1

        with patch.object(settings, 'strands_max_concurrent_calls', 2), patch.object(native, 'Agent', Agent):
            auditor = self.auditor()
            with patch.object(auditor, '_model', return_value=None):
                await asyncio.gather(*(self.read(auditor, spans) for _ in range(2)))
        self.assertEqual(maximum, 2)
        self.assertEqual(active, 0)
        self.assertEqual(stages.count('source_reader'), 16)


class ReaderReferenceRecoveryTests(unittest.IsolatedAsyncioTestCase):
    setUp = SourceReadingTests.setUp
    auditor = SourceReadingTests.auditor
    read = SourceReadingTests.read

    async def test_local_recovery_preserves_sources_and_does_not_replay_bad_notes(self):
        auditor = self.auditor()
        valid = {'documents': [self.reading['documents'][0]]}
        wrong = copy.deepcopy(valid)
        wrong['documents'][0]['observations'][0] = {'text': 'FAILED-NOTE-DO-NOT-REPLAY',
                                                  'references': [{'span_id': 'foreign'}]}
        with patch.object(auditor, '_text_agent', AsyncMock(side_effect=[json.dumps(wrong), json.dumps(valid)])) as calls:
            result = await self.read(auditor, self.spans[:1])
        self.assertEqual(result, valid)
        self.assertEqual(calls.await_count, 2)
        initial, corrected = [json.loads(c.kwargs['prompt']) for c in calls.await_args_list]
        marker = corrected.pop('reading_protocol_correction')
        self.assertTrue(marker)
        self.assertEqual(initial, corrected)
        self.assertNotIn('FAILED-NOTE-DO-NOT-REPLAY', json.dumps(corrected))

    async def test_reader_retry_bound_and_absent_output_do_not_fall_back(self):
        for responses, expected in ((['not json', 'not json'], 2), ([None], 1), ([''], 1), (['   '], 1)):
            with self.subTest(responses=responses):
                auditor = self.auditor()
                with patch.object(auditor, '_text_agent', AsyncMock(side_effect=responses)) as calls:
                    with self.assertRaises(SourceReadingError):
                        await self.read(auditor, self.spans[:1])
                self.assertEqual(calls.await_count, expected)

    async def test_cancellation_is_not_a_protocol_retry(self):
        auditor = self.auditor()
        with patch.object(auditor, '_text_agent', AsyncMock(side_effect=asyncio.CancelledError)) as calls:
            with self.assertRaises(asyncio.CancelledError):
                await self.read(auditor, self.spans[:1])
        self.assertEqual(calls.await_count, 1)


if __name__ == '__main__':
    unittest.main()
