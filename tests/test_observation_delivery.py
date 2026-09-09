"""Explicit observation units survive repair without borrowing record context."""
import copy
import hashlib
import asyncio
import json
import unittest
from unittest.mock import AsyncMock, patch

from tests.runtime import configure_test_environment
configure_test_environment()

from app.answer_finalization import AnswerFinalizer, citation_safe_span, evidence_spans, validate_reference
from app.answer_observations import ObservationCandidate
from tests.test_source_dates import pack


class HandleAuditor:
    def __init__(self):
        self.calls = []

    async def audit_answer_units(self, question, units, spans, plan):
        self.calls.append((copy.deepcopy(units), copy.deepcopy(spans), dict(plan)))
        assessments = []
        for unit in units:
            missing_subject = unit['text'].startswith(('- Its ', '- Amount:'))
            status = 'missing' if missing_subject else 'unsupported' if 'REJECT' in unit['text'] else 'supported'
            assessments.append({'unit_id': unit['id'], 'status': status,
                                'temporal_scope': 'historical', 'temporal_assertion': 'source_observation',
                                'references': [] if missing_subject else [{'span_id': spans[0]['span_id']}]})
        return {'assessments': assessments}


class ObservationRepairer:
    def __init__(self, response):
        self.response = response
        self.calls = 0

    async def repair_answer(self, *args):
        self.calls += 1
        return self.response


class ObservationDeliveryTests(unittest.IsolatedAsyncioTestCase):
    async def test_independent_earlier_and_latest_records_survive_atomic_omission(self):
        for early, rejected, latest in (
            ('Cedar invoice records $20 in January 2024.',
             'Maple invoice records $30. Its recipient is REJECT.',
             'Cedar invoice records $40 in January 2026.'),
            ('The Cedar measurement records 5 mg on January 1, 2024.',
             'The Maple measurement records 10 mg. Its subject is REJECT.',
             'The Cedar measurement records 15 mg on January 1, 2026.'),
        ):
            auditor = HandleAuditor()
            repairer = ObservationRepairer({'observations': [early, rejected, latest]})
            result = await AnswerFinalizer(auditor, repairer).finalize(
                'Compare the earlier and latest documented records.', 'Original draft REJECT.',
                pack(' '.join((early, rejected, latest))))
            self.assertEqual(result['finalization']['disposition'], 'partial')
            self.assertIn(early, result['answer'])
            self.assertIn(latest, result['answer'])
            self.assertNotIn('Maple', result['answer'])
            self.assertEqual(repairer.calls, 1)
            self.assertEqual(len(auditor.calls), 3)
            self.assertEqual(len(auditor.calls[1][0]), 3)  # Compound observation is one unit.
            self.assertEqual(len(auditor.calls[2][0]), 2)
            for _, _, plan in auditor.calls[1:]:
                self.assertEqual(plan['answer_context'], '')
                self.assertEqual(plan['unitization'], 'observations_v1')
            ledger = result['claim_ledger']
            self.assertEqual(ledger['unitization'], 'observations_v1')
            candidate = '\n\n'.join('- ' + text for text in (early, latest))
            self.assertEqual(ledger['candidate_digest'], hashlib.sha256(candidate.encode()).hexdigest())
            for claim in ledger['claims']:
                self.assertEqual(candidate[claim['start']:claim['end']], claim['claim'])

    async def test_scalar_and_pronoun_fragments_are_audited_without_sibling_context(self):
        good = 'Cedar invoice records $20 in January 2026.'
        auditor = HandleAuditor()
        result = await AnswerFinalizer(auditor, ObservationRepairer(
            {'observations': [good, 'Amount: $30.', 'Its recipient is Casey.']})).finalize(
                'Compare the documented invoices.', 'REJECT.', pack(good + ' Maple invoice records $30 for Casey.'))
        self.assertEqual(result['finalization']['disposition'], 'partial')
        self.assertIn(good, result['answer'])
        self.assertNotIn('$30', result['answer'])
        self.assertNotIn('Casey', result['answer'])
        self.assertEqual(auditor.calls[1][2]['answer_context'], '')

    async def test_malformed_observations_never_fall_back_to_an_answer_field(self):
        for observations in ([], 'text', [None], [''], ['# Heading'], ['First\nSecond'], ['x' * 1201]):
            result = await AnswerFinalizer(HandleAuditor(), ObservationRepairer(
                {'observations': observations, 'answer': 'Cedar records $20.'})).finalize(
                    'What is recorded?', 'REJECT.', pack('Cedar records $20.'))
            self.assertFalse(result['finalization']['answer_verified'], observations)
            self.assertNotIn('Cedar records', result['answer'])

    async def test_malformed_items_reject_the_complete_repair_without_truncation(self):
        good = 'Cedar records $20.'
        for bad in (None, '', '  padded', '# Heading', 'First\nSecond', '**Emphasis**',
                    '<b>HTML</b>', '[link](/documents/101)', 'x' * 1199):
            auditor = HandleAuditor()
            result = await AnswerFinalizer(auditor, ObservationRepairer(
                {'observations': [good, bad]})).finalize('What is recorded?', 'REJECT.', pack(good))
            self.assertEqual(result['finalization']['disposition'], 'audit_failed', bad)
            self.assertNotIn(good, result['answer'])
            self.assertEqual(len(auditor.calls), 1)

    def test_persisted_rendering_and_ranges_require_canonical_observations(self):
        original = ObservationCandidate.from_response({'observations': [
            'Cedar invoice records $20. The recipient is Casey.', 'Maple invoice records $30.']})
        restored = ObservationCandidate.from_text(json.loads(json.dumps(original.text)))
        self.assertEqual(restored.units(), original.units())
        self.assertEqual(len(restored.units()), 2)
        claims = [{**unit, 'claim': unit['text'], 'status': 'supported'} for unit in restored.units()]
        claims[0]['end'] -= 1
        with self.assertRaises(ValueError):
            restored.supported_subset(claims)
        for text in ('Cedar invoice records $20.', '- First\n- Second', '- First\n\n# Heading'):
            with self.assertRaises(ValueError):
                ObservationCandidate.from_text(text)

    async def test_failed_or_conflicting_subset_never_falls_back_to_repaired_answer(self):
        good, bad = 'Cedar invoice records $20.', 'Maple invoice is REJECT.'
        for failure in ('conflicting', 'protocol', 'timeout', 'exception', 'cancelled'):
            started = asyncio.Event()
            class FailingSubset(HandleAuditor):
                async def audit_answer_units(self, *args):
                    raw = await super().audit_answer_units(*args)
                    if len(self.calls) >= 3:
                        if failure == 'timeout':
                            raise TimeoutError()
                        if failure == 'exception':
                            raise RuntimeError('synthetic unavailable')
                        if failure == 'cancelled':
                            started.set()
                            await asyncio.sleep(10)
                        if failure == 'protocol':
                            return {'assessments': []}
                        raw['assessments'][0]['status'] = 'conflicting'
                    return raw
            auditor = FailingSubset()
            call = AnswerFinalizer(auditor, ObservationRepairer({'observations': [good, bad]})).finalize(
                'What is recorded?', 'REJECT.', pack(good + ' ' + bad))
            if failure == 'cancelled':
                task = asyncio.create_task(call)
                try:
                    await asyncio.wait_for(started.wait(), 1)
                    task.cancel()
                    with self.assertRaises(asyncio.CancelledError):
                        await task
                finally:
                    task.cancel()
                    await asyncio.gather(task, return_exceptions=True)
                continue
            result = await call
            self.assertFalse(result['finalization']['answer_verified'], failure)
            self.assertNotIn(good, result['answer'])
            self.assertEqual(result['claim_ledger']['unitization'], 'observations_v1')
            self.assertEqual(len(auditor.calls), 4 if failure == 'protocol' else 3)

    async def test_atomic_units_survive_public_stream_cache_and_source_binding(self):
        from app.cache import invalidate_on_sync
        from tests.test_query_delivery import RetrievedEngine
        good = 'Cedar invoice records $20. The recipient is Casey.'
        for partial in (False, True):
            class Adapter(HandleAuditor):
                async def repair_answer(self, *args):
                    return {'observations': [good] + (['Maple invoice is REJECT.'] if partial else [])}
            engine = RetrievedEngine()
            invalidate_on_sync()
            with patch('app.query.strands_orchestrator', Adapter()), \
                    patch.object(engine, '_build_evidence_pack', AsyncMock(return_value=pack(good))), \
                    patch.object(engine, '_final_synthesis', AsyncMock(return_value={'answer': 'REJECT.'})), \
                    patch('app.query.embeddings_store.get_incomplete_document_ids', AsyncMock(return_value=set())), \
                    patch('app.query.embeddings_store.get_open_feedback_document_ids', AsyncMock(return_value=set())):
                ordinary = await engine.query('What is recorded?', mode='strict')
                events = [event async for event in engine.query_stream('What is recorded?', mode='strict')]
            streamed = events[-1]
            self.assertEqual(streamed['cached'], not partial)
            self.assertEqual(streamed['answer'], ordinary['answer'])
            self.assertEqual(streamed['claim_ledger'], ordinary['claim_ledger'])
            self.assertEqual(len(streamed['claim_ledger']['claims']), 1)
            self.assertEqual(streamed['claim_ledger']['unitization'], 'observations_v1')
            self.assertIn(good + ' [Document 101]', streamed['answer'])
            self.assertEqual(streamed['evidence_pack']['items'][0]['support_spans'][0]['quote'], good)

    def test_handle_identity_feedback_and_clipped_edges_keep_legacy_guards(self):
        for source in (
            ' ' * 3996 + '-$500.01 remaining source.',
            ' ' * 3994 + '$**500**.01 remaining source.',
            'a' * 3800 + '500 USD remaining source.',
            'Dose - ' + '*' * 3793 + '5 mg remaining source.',
        ):
            spans = evidence_spans(pack(source), citation_safe=True)
            self.assertTrue(spans)
            for span in spans:
                reference = validate_reference({'span_id': span['span_id']}, [span])
                self.assertIsNotNone(reference, span)
                self.assertEqual(reference['quote'], source[reference['start']:reference['end']])
                self.assertLessEqual(reference['end'] - reference['start'], 4000)
        spans = evidence_spans(pack('Cedar records $20.'), citation_safe=True)
        span = spans[0]
        for handle, reason in (({'span_id': 'unselected'}, 'unknown_or_unselected_span'),
                               ({'span_id': span['span_id'], 'document_id': 102}, 'identity_mismatch'),
                               ({'span_id': span['span_id'], 'quote': 'Cedar records $20.'}, 'identity_mismatch')):
            failures = []
            self.assertIsNone(validate_reference(handle, spans, diagnostics=failures))
            self.assertEqual(failures, [reason])
        failures = []
        self.assertIsNone(validate_reference({'span_id': span['span_id']}, [{**span, 'feedback_open': True}], diagnostics=failures))
        self.assertEqual(failures, ['source_feedback_open'])

    def test_repeated_text_is_trimmed_at_its_actual_original_offset(self):
        source = 'x' * 3800 + 'A ' * 2000
        raw = evidence_spans(pack(source))[1]
        safe = citation_safe_span(raw)
        self.assertIsNotNone(safe)
        self.assertEqual(safe['start'], 3802)
        self.assertEqual(safe['end'], 7800)
        self.assertEqual(safe['content'], source[3802:7800])
        self.assertIsNotNone(validate_reference({'span_id': safe['span_id']}, [safe]))

    async def test_source_membership_does_not_override_semantic_rejection(self):
        class WrongSubject(HandleAuditor):
            async def audit_answer_units(self, *args):
                raw = await super().audit_answer_units(*args)
                raw['assessments'][0]['status'] = 'unsupported'
                return raw
        result = await AnswerFinalizer(WrongSubject()).finalize(
            'What does Cedar record?', 'Cedar records $30.', pack('Cedar records $20. Maple records $30.'))
        self.assertFalse(result['finalization']['answer_verified'])
        claim = result['claim_ledger']['claims'][0]
        self.assertEqual(claim['model_status'], 'unsupported')
        self.assertEqual(claim['value_mismatches'], {})
        self.assertTrue(claim['references'])

    async def test_timeline_uses_the_same_safe_source_handles_and_separate_window_diagnostics(self):
        from tests.test_query_delivery import RetrievedEngine
        source = 'x' * 4200 + ' Cedar invoice dated January 1, 2026 records $20.'
        supplied = []
        class TimelineAdapter(HandleAuditor):
            async def extract_timeline(self, question, evidence):
                spans = json.loads(evidence)
                supplied.extend(spans)
                return [{'date': '2026-01-01', 'title': 'Cedar invoice records $20',
                         'summary': '', 'document_id': 101,
                         'references': [{'span_id': spans[0]['span_id']}]}]
        with patch('app.query.strands_orchestrator', TimelineAdapter()):
            events, trace = await RetrievedEngine()._extract_timeline_events(
                'What is the dated invoice history?', {}, [], 'timeline', evidence_pack=pack(source))
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]['references'][0]['span_id'], supplied[0]['span_id'])
        self.assertEqual(events[0]['references'][0]['quote'], source[supplied[0]['start']:supplied[0]['end']])
        self.assertEqual(trace[0]['data']['source_diagnostics']['unavailable_citation_windows'], 1)
        self.assertEqual(trace[0]['data']['rejection_reasons'], {})

    async def test_handle_resolves_to_a_contiguous_safe_source_range(self):
        source = 'Invoice Cedar records $20. ' + 'Routine descriptive text repeats here. ' * 140
        auditor = HandleAuditor()
        result = await AnswerFinalizer(auditor).finalize(
            'What does Invoice Cedar record?', 'Invoice Cedar records $20.', pack(source))
        self.assertTrue(result['finalization']['answer_verified'])
        reference = result['claim_ledger']['claims'][0]['references'][0]
        self.assertEqual(source[reference['start']:reference['end']], reference['quote'])
        self.assertLessEqual(len(reference['quote']), 4000)
        self.assertEqual(reference['content_digest'], hashlib.sha256(source.encode()).hexdigest())
        self.assertEqual(validate_reference(reference, auditor.calls[0][1]), reference)

    async def test_unknown_handle_and_wrong_value_remain_unsupported(self):
        for wrong_handle in (False, True):
            class WrongAuditor(HandleAuditor):
                async def audit_answer_units(self, *args):
                    raw = await super().audit_answer_units(*args)
                    if wrong_handle:
                        raw['assessments'][0]['references'] = [{'span_id': 'not-supplied'}]
                    return raw
            result = await AnswerFinalizer(WrongAuditor()).finalize(
                'What is recorded?', 'Invoice Cedar records $999.', pack('Invoice Cedar records $20.'))
            self.assertFalse(result['finalization']['answer_verified'])
            self.assertNotIn('$999', result['answer'])

    async def test_reference_diagnostics_are_bounded_without_ignoring_rejections(self):
        class InvalidReferences(HandleAuditor):
            async def audit_answer_units(self, *args):
                raw = await super().audit_answer_units(*args)
                raw['assessments'][0]['references'] *= 100
                raw['assessments'][0]['references'] += [{'span_id': 'unselected'}] * 100
                return raw
        result = await AnswerFinalizer(InvalidReferences()).finalize(
            'What is recorded?', 'Cedar records $20.', pack('Cedar records $20.'))
        self.assertFalse(result['finalization']['answer_verified'])
        claim = result['claim_ledger']['claims'][0]
        self.assertEqual(len(claim['reference_diagnostics']), 8)
        self.assertEqual(claim['reference_diagnostic_counts'], {'unknown_or_unselected_span': 100})
        self.assertEqual(claim['reference_diagnostics_omitted'], 92)
        self.assertEqual(claim['model_status'], 'supported')
        self.assertEqual(claim['status'], 'unsupported')
