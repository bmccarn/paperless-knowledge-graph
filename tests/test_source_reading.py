"""Source-reading mechanics through the native audit interface, not model accuracy."""
import asyncio
import copy
import json
import unittest
from unittest.mock import AsyncMock, patch

from tests.runtime import configure_test_environment
configure_test_environment()
from app.strands_orchestrator import StrandsQueryOrchestrator
from app.answer_finalization import AnswerFinalizer, evidence_spans
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
        self.plan = {'answer_context': 'ANSWER-CONTEXT', 'conversation_context': 'OLD-VERDICT',
                     'audit_protocol_recovery': 'CORRECTION-ONLY', 'evaluated_at': '2026-09-09'}
        self.reading = {'documents': [{'document_id': s['document_id'],
            'observations': [{'text': 'PRIVATE NOTE describing source field roles.',
                              'references': [{'span_id': s['span_id']}]}],
            'limitations': []} for s in self.spans]}
        self.verdict = json.dumps({'assessments': [decision(references=[{'span_id': self.spans[0]['span_id']}])]})

    def auditor(self, strategy):
        auditor = StrandsQueryOrchestrator(audit_strategy=strategy)
        auditor.enabled = True
        return auditor

    async def test_reader_is_candidate_blind_and_verifier_keeps_all_originals(self):
        auditor = self.auditor('source_first')
        with patch.object(auditor, '_text_agent', AsyncMock(side_effect=[json.dumps(self.reading), self.verdict])) as calls:
            result = await auditor.audit_answer_units('What do the records establish?', self.units, self.spans, self.plan)
        self.assertEqual(result['assessments'][0]['status'], 'supported')
        self.assertEqual(calls.await_count, 2)
        reader, verifier = [json.loads(c.kwargs['prompt']) for c in calls.await_args_list]
        for marker in ('CANDIDATE-ONLY', 'ANSWER-CONTEXT', 'OLD-VERDICT', 'CORRECTION-ONLY'):
            self.assertNotIn(marker, json.dumps(reader))
        self.assertEqual(verifier['units'], self.units)
        self.assertEqual(verifier['source_reading']['documents'], self.reading['documents'])
        for payload in (reader, verifier):
            windows = [w for d in payload['source_documents'] for w in d['windows']]
            self.assertEqual([w['span'] for w in sorted(windows, key=lambda w: w['ordinal'])], self.spans)
        self.assertNotIn('PRIVATE NOTE', json.dumps(result))

    async def test_grouping_retains_interleaved_windows_and_every_field(self):
        spans = [self.spans[0], self.spans[1], {**self.spans[0], 'span_id': 'third', 'content': 'Another passage.'}]
        before = copy.deepcopy(spans)
        auditor = self.auditor('grouped')
        with patch.object(auditor, '_text_agent', AsyncMock(return_value=self.verdict)) as calls:
            await auditor.audit_answer_units('Question', self.units, spans, {})
        payload = json.loads(calls.await_args.kwargs['prompt'])
        self.assertEqual(calls.await_count, 1)
        self.assertNotIn('source_reading', payload)
        self.assertNotIn('source_spans', payload)
        self.assertEqual([d['document_id'] for d in payload['source_documents']], [17, 18])
        windows = [w for d in payload['source_documents'] for w in d['windows']]
        self.assertEqual([w['span'] for w in sorted(windows, key=lambda w: w['ordinal'])], before)
        self.assertEqual(spans, before)

    async def test_flat_default_retains_existing_request_and_one_call(self):
        auditor = StrandsQueryOrchestrator()
        auditor.enabled = True
        with patch.object(auditor, '_text_agent', AsyncMock(return_value=self.verdict)) as calls:
            await auditor.audit_answer_units('Question', self.units, self.spans, {})
        payload = json.loads(calls.await_args.kwargs['prompt'])
        self.assertEqual(payload['source_spans'], self.spans)
        self.assertNotIn('source_documents', payload)
        self.assertEqual(calls.await_count, 1)

    async def test_invalid_reader_never_falls_back_or_invokes_verifier(self):
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
            d = copy.deepcopy(self.reading); change(d); responses.append(json.dumps(d))
        for response in responses:
            with self.subTest(response=response):
                auditor = self.auditor('source_first')
                with patch.object(auditor, '_text_agent', AsyncMock(return_value=response)) as calls:
                    self.assertIsNone(await auditor.audit_answer_units('Question', self.units, self.spans, {}))
                self.assertEqual(calls.await_count, 1)

    async def test_cancellation_propagates_without_verifier(self):
        auditor = self.auditor('source_first')
        with patch.object(auditor, '_text_agent', AsyncMock(side_effect=asyncio.CancelledError)) as calls:
            with self.assertRaises(asyncio.CancelledError):
                await auditor.audit_answer_units('Question', self.units, self.spans, {})
        self.assertEqual(calls.await_count, 1)

    async def test_reading_note_cannot_certify_invented_value(self):
        auditor = self.auditor('source_first')
        self.reading['documents'][0]['observations'][0]['text'] = 'PRIVATE NOTE: Vendor refunded $999.'
        with patch.object(auditor, '_text_agent', AsyncMock(side_effect=[json.dumps(self.reading), self.verdict])):
            result = await AnswerFinalizer(auditor).finalize('What was refunded?', 'Vendor completed a refund of $999.', self.pack)
        self.assertFalse(result['finalization']['answer_verified'])
        self.assertNotIn('PRIVATE NOTE', json.dumps(result))
        self.assertIn('value_mismatch', result['claim_ledger']['claims'][0]['rejection_reasons'])

    def test_vertex_schema_has_no_non_string_enums(self):
        from app.source_reading import group_sources, response_format
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

    def test_unknown_strategy_is_rejected(self):
        with self.assertRaises(ValueError):
            self.auditor('silently-enable-something')


class DocumentLocalReadingTests(unittest.IsolatedAsyncioTestCase):
    setUp = SourceReadingTests.setUp
    auditor = SourceReadingTests.auditor

    async def test_each_reader_is_isolated_and_completion_order_does_not_reorder_notes(self):
        auditor = self.auditor('document_local')
        second_done = asyncio.Event()
        readers, verifiers = [], []

        async def transport(name, system_prompt, prompt, **kwargs):
            payload = json.loads(prompt)
            if name == 'source_auditor':
                verifiers.append(payload)
                return self.verdict
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
            result = await auditor.audit_answer_units('Question', self.units, self.spans, self.plan)
        self.assertEqual(result['assessments'][0]['status'], 'supported')
        self.assertEqual(len(readers), 2)
        for reader in readers:
            self.assertEqual(set(reader), {'question', 'evaluated_at', 'source_date_order', 'source_documents'})
            for marker in ('CANDIDATE-ONLY', 'ANSWER-CONTEXT', 'OLD-VERDICT', 'CORRECTION-ONLY'):
                self.assertNotIn(marker, json.dumps(reader))
        verifier = verifiers[0]
        self.assertEqual(verifier['source_reading'], self.reading)
        windows = [w for d in verifier['source_documents'] for w in d['windows']]
        self.assertEqual([w['span'] for w in sorted(windows, key=lambda w: w['ordinal'])], self.spans)

    async def test_failure_cancels_and_drains_other_readers_before_return(self):
        auditor = self.auditor('document_local')
        started, drained = asyncio.Event(), asyncio.Event()
        calls = []

        async def transport(name, system_prompt, prompt, **kwargs):
            calls.append(name)
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
            result = await auditor.audit_answer_units('Question', self.units, self.spans, {})
        self.assertIsNone(result)
        self.assertTrue(drained.is_set())
        self.assertNotIn('source_auditor', calls)

    async def test_parent_cancellation_drains_reader_tasks(self):
        auditor = self.auditor('document_local')
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
            task = asyncio.create_task(auditor.audit_answer_units('Question', self.units, self.spans, {}))
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
            if name == 'source_auditor':
                return self.verdict
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
            auditor = self.auditor('document_local')
            with patch.object(auditor, '_text_agent', side_effect=transport):
                await auditor.audit_answer_units('Question', self.units, spans, {})
        self.assertEqual(maximum, 2)
        self.assertEqual(seen, list(range(100, 120)))

    async def test_simultaneous_audits_share_native_call_limit(self):
        from types import SimpleNamespace
        from app.config import settings
        from app import strands_orchestrator as native
        active, maximum = 0, 0
        stages = []
        spans = [{**self.spans[0], 'document_id': i + 100, 'span_id': f'span-{i}'} for i in range(8)]
        verdict = self.verdict

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
                    if self.name == 'source_auditor':
                        return Result(verdict)
                    doc = json.loads(prompt)['source_documents'][0]
                    return Result(json.dumps({'documents': [{'document_id': doc['document_id'],
                        'observations': [], 'limitations': []}]}))
                finally:
                    active -= 1

        with patch.object(settings, 'strands_max_concurrent_calls', 2), patch.object(native, 'Agent', Agent):
            auditor = self.auditor('document_local')
            with patch.object(auditor, '_model', return_value=None):
                await asyncio.gather(*(auditor.audit_answer_units('Question', self.units, spans, {}) for _ in range(2)))
        self.assertEqual(maximum, 2)
        self.assertEqual(active, 0)
        self.assertEqual(stages.count('source_reader'), 16)
        self.assertEqual(stages.count('source_auditor'), 2)
