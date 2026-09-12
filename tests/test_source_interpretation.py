"""Omission recovery mechanics; synthetic adapters do not establish model accuracy."""
import asyncio
import copy
import json
import unittest
from unittest.mock import AsyncMock, patch

from tests.runtime import configure_test_environment
configure_test_environment()
from app.source_interpretation import recover
from app.source_reading import SourceReadingError
from app.strands_orchestrator import StrandsQueryOrchestrator


class SourceInterpretationTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.request = {'question': 'What does the work record establish?',
            'evaluated_at': '2026-09-10', 'source_date_order': 'mdy',
            'source_documents': [{'document_id': 1, 'windows': [{'ordinal': 0, 'span': {
                'document_id': 1, 'span_id': 's1',
                'content': 'Five hours authorized only if training is complete. Attendance not recorded.'}}]}]}
        self.primary = self.reading('Five hours were worked.')
        self.addition = self.reading('The work record authorizes five hours only if training is complete; attendance is not recorded.')

    def reading(self, text=None):
        return {'documents': [{'document_id': 1, 'observations': ([{
            'text': text, 'references': [{'span_id': 's1'}]}] if text else []), 'limitations': []}]}

    def adapter(self, *responses):
        class Adapter:
            review_source_omissions = AsyncMock(side_effect=responses)
        return Adapter()

    async def test_conserves_false_primary_and_additions_as_unverified_occurrences(self):
        adapter = self.adapter(json.dumps(self.addition))
        result = await recover(self.request, self.primary, adapter)
        self.assertEqual(result.reading['documents'][0]['observations'],
            self.primary['documents'][0]['observations'] + self.addition['documents'][0]['observations'])
        record = result.record
        self.assertEqual([r['origin'] for r in record['occurrences']], ['primary', 'addition'])
        self.assertEqual(len({r['id'] for r in record['occurrences']}), 2)
        self.assertTrue(record['receipt']['execution_complete'])
        self.assertNotIn('supported', record['receipt'])
        self.assertNotIn('complete', record['receipt'])
        self.assertEqual(record['primary'], self.primary)

    async def test_no_semantic_dedup_or_replacement_and_reference_repair_is_an_addition(self):
        self.request['source_documents'][0]['windows'].append({'ordinal': 1, 'span': {
            'document_id': 1, 'span_id': 's2', 'content': 'Attendance is not recorded.'}})
        repaired = copy.deepcopy(self.primary)
        repaired['documents'][0]['observations'][0]['references'].append({'span_id': 's2'})
        repaired['documents'][0]['observations'].append(copy.deepcopy(repaired['documents'][0]['observations'][0]))
        result = await recover(self.request, self.primary, self.adapter(json.dumps(repaired)))
        self.assertEqual(len(result.record['occurrences']), 3)
        self.assertEqual(len({r['id'] for r in result.record['occurrences']}), 3)
        self.assertEqual(result.record['primary'], self.primary)
        self.assertEqual(result.record['additions'], repaired)

    async def test_freezes_input_before_adapter_mutation_and_returns_fresh_copies(self):
        request, primary = copy.deepcopy(self.request), copy.deepcopy(self.primary)
        class Adapter:
            async def review_source_omissions(inner, payload):
                payload['source_documents'][0]['windows'].clear()
                payload['prior_reading']['documents'].clear()
                self.request.clear(); self.primary.clear()
                return json.dumps(self.addition)
        result = await recover(self.request, self.primary, Adapter())
        self.assertEqual(result.record['request'], request)
        self.assertEqual(result.record['primary'], primary)
        result.record['occurrences'].clear()
        result.reading['documents'].clear()
        self.assertEqual(len(result.record['occurrences']), 2)

    async def test_rejects_foreign_context_and_malformed_primary_before_calls(self):
        invalid = []
        for key in ('answer', 'gold', 'conversation_context', 'prior_reading'):
            invalid.append({**self.request, key: 'private sibling'})
        other = copy.deepcopy(self.request)
        other['source_documents'].append(copy.deepcopy(other['source_documents'][0]))
        invalid.append(other)
        for key, value in [('gold', {'expected': 'EVALUATOR ONLY'}),
                ('source_context', {'digest': 'd', 'document_id': 1, 'start': 0, 'end': 5,
                                    'gold': 'EVALUATOR ONLY'}),
                ('field_leaders', [{'gold': 'EVALUATOR ONLY'}]),
                ('title', {'gold': 'EVALUATOR ONLY'})]:
            other = copy.deepcopy(self.request)
            other['source_documents'][0]['windows'][0]['span'][key] = value
            invalid.append(other)
        for payload in invalid:
            adapter = self.adapter()
            with self.assertRaises(SourceReadingError):
                await recover(payload, self.primary, adapter)
            adapter.review_source_omissions.assert_not_awaited()
        adapter = self.adapter()
        with self.assertRaises(SourceReadingError):
            await recover(self.request, {'documents': []}, adapter)
        adapter.review_source_omissions.assert_not_awaited()

    async def test_empty_primary_and_empty_valid_additions_are_execution_only(self):
        result = await recover(self.request, self.reading(), self.adapter(json.dumps(self.reading())))
        self.assertEqual(result.record['receipt']['status'], 'completed')
        self.assertEqual(result.record['occurrences'], [])
        self.assertEqual(len(result.record['attempts']), 1)

    async def test_protocol_correction_retains_raw_attempt_and_identical_original_context(self):
        adapter = self.adapter('{"documents": []}', json.dumps(self.addition))
        result = await recover(self.request, self.primary, adapter)
        calls = [call.args[0] for call in adapter.review_source_omissions.await_args_list]
        correction = calls[1].pop('reading_protocol_correction')
        self.assertEqual(calls[0], calls[1])
        self.assertEqual(calls[0], {**self.request, 'prior_reading': self.primary})
        self.assertEqual(correction['error'], 'invalid_source_reading')
        self.assertEqual([a['status'] for a in result.record['attempts']], ['invalid', 'completed'])
        self.assertEqual(result.record['attempts'][0]['response'], '{"documents": []}')

    async def test_unavailable_timeout_and_twice_invalid_preserve_primary_without_success(self):
        for responses, status, count in [([None], 'unavailable', 1), ([' '], 'unavailable', 1),
                ([TimeoutError()], 'timeout', 1), (['{}', '{}'], 'invalid', 2),
                (['{}', None], 'unavailable', 2)]:
            with self.subTest(status=status, count=count):
                adapter = self.adapter(*responses)
                result = await recover(self.request, self.primary, adapter)
                self.assertEqual(result.record['primary'], self.primary)
                self.assertEqual(result.reading, self.primary)
                self.assertFalse(result.record['receipt']['execution_complete'])
                self.assertEqual(result.record['receipt']['status'], status)
                self.assertEqual(adapter.review_source_omissions.await_count, count)

    async def test_foreign_references_and_protocol_extensions_never_enter_inventory(self):
        values = []
        for handle in ('foreign', ''):
            value = copy.deepcopy(self.addition)
            value['documents'][0]['observations'][0]['references'][0]['span_id'] = handle
            values.append(value)
        values.append({**self.addition, 'replace_primary': True})
        for value in values:
            adapter = self.adapter(json.dumps(value), json.dumps(value))
            result = await recover(self.request, self.primary, adapter)
            self.assertEqual(result.record['receipt']['status'], 'invalid')
            self.assertEqual(result.reading, self.primary)

    async def test_cancellation_joins_adapter_and_integrity_errors_propagate(self):
        entered, cleaned = asyncio.Event(), asyncio.Event()
        class Adapter:
            async def review_source_omissions(inner, payload):
                try:
                    entered.set()
                    await asyncio.Event().wait()
                finally:
                    cleaned.set()
        task = asyncio.create_task(recover(self.request, self.primary, Adapter()))
        await entered.wait(); task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertTrue(cleaned.is_set())
        with self.assertRaisesRegex(ValueError, 'capture integrity'):
            await recover(self.request, self.primary, self.adapter(ValueError('capture integrity')))

    async def test_request_and_primary_changes_bind_distinct_receipts(self):
        first = await recover(self.request, self.primary, self.adapter(json.dumps(self.reading())))
        self.request['evaluated_at'] = '2026-09-11'
        second = await recover(self.request, self.primary, self.adapter(json.dumps(self.reading())))
        self.assertNotEqual(first.record['receipt']['request_digest'], second.record['receipt']['request_digest'])
        self.assertNotEqual(first.record['occurrences'][0]['id'], second.record['occurrences'][0]['id'])
        third = await recover(self.request, self.addition, self.adapter(json.dumps(self.reading())))
        self.assertNotEqual(second.record['receipt']['primary_digest'], third.record['receipt']['primary_digest'])

    async def test_native_adapter_uses_distinct_stage_and_owned_schema(self):
        native = StrandsQueryOrchestrator(); native.enabled = True
        with patch.object(native, '_text_agent', AsyncMock(return_value=json.dumps(self.addition))) as call:
            await recover(self.request, self.primary, native)
        call.assert_awaited_once()
        wire = call.await_args.kwargs
        self.assertEqual(wire['name'], 'source_omission_review')
        self.assertEqual(json.loads(wire['prompt']), {**self.request, 'prior_reading': self.primary})
        self.assertEqual(wire['response_format']['json_schema']['name'], 'source_omission_review')

    async def test_actual_native_adapter_preserves_timeout_and_integrity_failures(self):
        native = StrandsQueryOrchestrator(); native.enabled = True
        for error in (TimeoutError(), ValueError('capture integrity')):
            class Agent:
                def __init__(inner, **kwargs): pass
                async def invoke_async(inner, prompt): raise error
            with patch('app.strands_orchestrator.Agent', Agent), patch.object(native, '_model', return_value=None):
                if isinstance(error, TimeoutError):
                    result = await recover(self.request, self.primary, native)
                    self.assertEqual(result.record['receipt']['status'], 'timeout')
                else:
                    with self.assertRaisesRegex(ValueError, 'capture integrity'):
                        await recover(self.request, self.primary, native)
                # Existing query stages retain their unavailable fallback.
                self.assertIsNone(await native._text_agent('source_reader', 'system', 'prompt'))

    async def test_correct_addition_does_not_replace_false_primary_before_subset_audit(self):
        from app.answer_finalization import AnswerFinalizer, evidence_spans
        from app.answer_observations import ObservationCandidate
        from tests.source_audit_fixtures import decision
        content = 'Vendor requested a refund. Completion is not recorded.'
        pack = {'items': [{'id': 'request', 'document_id': 1, 'chunk_index': 0,
            'title': 'Request', 'source_kind': 'ocr', 'content': content, 'source_content': content}]}
        spans = evidence_spans(pack, citation_safe=True)
        self.request['source_documents'][0]['windows'] = [{'ordinal': 0, 'span': spans[0]}]
        primary = self.reading('Vendor completed a refund.')
        addition = self.reading('Vendor requested a refund; completion is not recorded.')
        for reading in (primary, addition):
            reading['documents'][0]['observations'][0]['references'] = [{'span_id': spans[0]['span_id']}]
        result = await recover(self.request, primary, self.adapter(json.dumps(addition)))
        candidate = ObservationCandidate.from_response({'observations': [r['text'] for r in result.record['occurrences']]})
        raw_audits = []
        class Auditor:
            async def audit_answer_units(inner, question, units, supplied, plan):
                self.assertEqual(supplied, spans)
                assessments = [decision(unit_id=u['id'],
                    status='unsupported' if u['text'] == '- Vendor completed a refund.' else 'supported',
                    references=[{'span_id': spans[0]['span_id']}], temporal_assertion='source_observation')
                    for u in units]
                raw_audits.append(copy.deepcopy(assessments))
                return {'assessments': assessments}
        final = await AnswerFinalizer(Auditor(), repairer=None, allow_subset=True).finalize(
            'What does the refund record establish?', candidate, pack, evaluated_at='2026-09-10')
        self.assertEqual([row['status'] for row in raw_audits[0]], ['unsupported', 'supported'])
        self.assertEqual(len(raw_audits), 2)  # The surviving subset gets a fresh audit.
        self.assertEqual(final['finalization']['disposition'], 'partial')
        self.assertFalse(final['finalization']['complete'])
        self.assertEqual([c['claim'] for c in final['claim_ledger']['claims']],
                         ['- Vendor requested a refund; completion is not recorded.'])
        self.assertNotIn('Vendor completed a refund.', final['answer'])
        self.assertEqual(result.record['primary'], primary)
