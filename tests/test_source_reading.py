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

    def test_unknown_strategy_is_rejected(self):
        with self.assertRaises(ValueError):
            self.auditor('silently-enable-something')
