"""All reader occurrences reach audit; conservation never grants factual support."""
import asyncio
import copy
import json
import unittest
from dataclasses import FrozenInstanceError
from unittest.mock import patch

from tests.runtime import configure_test_environment
configure_test_environment()
from app.answer_finalization import AnswerFinalizer
from app.answer_observations import ObservationCandidate
from app.question_evidence import QuestionEvidence, QuestionEvidenceError, PIPELINE_VERSION, canonical_json


QUESTION = 'What do the records establish?'
REQUIREMENTS = {'resolved_question': QUESTION, 'requirements': [{
    'id': 'r1', 'aspect': 'Documented status', 'temporal_scope': 'historical',
    'comparison_scope': 'none'}]}


async def prepared(observations=None, *, context=''):
    texts = observations if observations is not None else [
        ['The Cedar request is pending.'], ['The Maple request is pending.']]
    pack = {'items': [{'id': f'doc-{index}', 'document_id': index,
        'chunk_index': 0, 'title': name, 'source_kind': 'ocr',
        'content': ' '.join(values) or 'No requested status is recorded.',
        'source_content': ' '.join(values) or 'No requested status is recorded.'}
        for index, (name, values) in enumerate(zip(('Cedar record', 'Maple record'), texts), 1)]}

    class Reader:
        async def read_question_sources(self, payload):
            return {'documents': [{'document_id': doc['document_id'],
                'observations': [{'text': text, 'references': [
                    {'span_id': doc['windows'][0]['span']['span_id']}]} for text in values],
                'limitations': ['PRIVATE LIMITATION is not an inventory fact.']}
                for doc, values in zip(payload['source_documents'], texts)]}

    evidence = await QuestionEvidence.prepare(Reader(), QUESTION, REQUIREMENTS,
        pack, evaluated_at='2026-09-09')
    if context:
        source = evidence.composition_input
        reading = source.pop('source_reading')
        source['conversation_context'] = context
        evidence = QuestionEvidence(canonical_json(source), canonical_json(reading))
    return evidence, pack

from app.answer_fact_selection import prepare_facts, restore_fact_conservation, selection_payload, parse_exclusion


async def audited(evidence, pack, candidate, *, all_sources=False):
    class Auditor:
        async def audit_answer_units(self, question, units, spans, plan):
            refs = [{'span_id': span['span_id']} for span in (spans if all_sources else spans[:1])]
            return {'assessments': [{'unit_id': u['id'], 'status': 'supported',
                'temporal_scope': 'historical', 'temporal_assertion': 'source_observation',
                'references': refs} for u in units]}
    final = await AnswerFinalizer(Auditor()).finalize(QUESTION, candidate, pack, evaluated_at='2026-09-09')
    final['query_plan'] = {**REQUIREMENTS, 'original_question': QUESTION,
        'requirements_status': 'complete', 'pipeline_version': PIPELINE_VERSION,
        'evaluated_at': '2026-09-09', 'source_date_order': 'mdy', 'request_identity_digest': 'a' * 64}
    final['finalization'].update(pipeline_version=PIPELINE_VERSION,
        request_identity_digest='a' * 64, evidence_snapshot_digest=evidence.digest, reader_inventory_digest=prepare_facts(evidence).inventory_digest)
    return final


class FactInventoryTests(unittest.IsolatedAsyncioTestCase):
    async def test_every_occurrence_keeps_source_order_and_exact_final_bindings(self):
        evidence, source_pack = await prepared([['Cedar is pending.', 'Cedar requires authorization.'],
                                               ['Maple is pending.', 'Maple requires a review.']])
        inventory = prepare_facts(evidence)
        expected = ('Cedar is pending.', 'Cedar requires authorization.',
                    'Maple is pending.', 'Maple requires a review.')
        self.assertEqual(inventory.candidate.observations, expected)
        final = await audited(evidence, source_pack, inventory.candidate, all_sources=True)
        receipt = inventory.bind_final(evidence, final)
        self.assertEqual([m['unit_id'] for m in receipt['mappings']], ['u1', 'u2', 'u3', 'u4'])
        self.assertEqual(set(receipt), {'version', 'status', 'complete', 'inventory', 'mappings', 'summary', 'binding'})
        self.assertEqual(receipt['version'], 4)
        self.assertTrue(receipt['complete'])
        self.assertEqual(receipt['summary']['excluded'], 0)
        final['finalization']['fact_conservation'] = receipt
        self.assertEqual(restore_fact_conservation(final), receipt)
        changes = [lambda r, v=v: r.update(version=v) for v in (1, 2, 3, True)]
        changes += [lambda r: r['mappings'][1].update(status='excluded', unit_id=None),
                    lambda r: r.update(dispositions=[]), lambda r: r.update(reviews=[]),
                    lambda r: r['mappings'][1].update(target_id='u1'),
                    lambda r: r['inventory'][1]['references'][0].update(span_id='foreign'),
                    lambda r: r['summary'].update(excluded=1),
                    lambda r: r['inventory'].reverse()]
        for change in changes:
            changed = copy.deepcopy(final)
            change(changed['finalization']['fact_conservation'])
            self.assertIsNone(restore_fact_conservation(changed))

        rewritten = await audited(evidence, source_pack, ObservationCandidate((
            expected[0], 'Authorization is required for Cedar.', expected[2], expected[3])), all_sources=True)
        receipt = inventory.bind_final(evidence, rewritten)
        self.assertEqual(receipt['mappings'][1]['status'], 'unresolved')
        self.assertEqual(receipt['summary']['preserved'], 3)
        rewritten['finalization']['fact_conservation'] = receipt
        self.assertEqual(restore_fact_conservation(rewritten), receipt)
        substituted = await audited(evidence, source_pack, inventory.candidate)
        receipt = inventory.bind_final(evidence, substituted)
        self.assertEqual([m['status'] for m in receipt['mappings']],
                         ['preserved', 'preserved', 'unresolved', 'unresolved'])
        substituted['finalization']['fact_conservation'] = receipt
        self.assertEqual(restore_fact_conservation(substituted), receipt)

    async def test_duplicate_occurrences_are_never_collapsed_or_discarded(self):
        evidence, source_pack = await prepared([['Cedar is pending.'] * 3, []])
        inventory = prepare_facts(evidence)
        self.assertEqual(inventory.candidate.observations, ('Cedar is pending.',) * 3)
        final = await audited(evidence, source_pack, inventory.candidate)
        self.assertEqual([r['unit_id'] for r in inventory.bind_final(evidence, final)['mappings']],
                         ['u1', 'u2', 'u3'])
        subset = await audited(evidence, source_pack, ObservationCandidate(('Cedar is pending.',)))
        receipt = inventory.bind_final(evidence, subset)
        self.assertEqual(receipt['summary'], {'total': 3, 'preserved': 1, 'excluded': 0,
                                             'unresolved': 2, 'unavailable': 0})
        subset['finalization']['fact_conservation'] = receipt
        self.assertEqual(restore_fact_conservation(subset), receipt)
        receipt['mappings'][1].update(unit_id='u1', status='preserved', reason=None)
        self.assertIsNone(restore_fact_conservation(subset))

    async def test_full_candidate_controls_real_audit_batches_and_capacity_without_truncation(self):
        from app.query_metrics import CURRENT_QUERY_METRICS, QueryMetrics, record_native_stage
        for size in (6, 81):
            with self.subTest(size=size):
                texts = [f'The record contains observation {i}.' for i in range(size)]
                evidence, source_pack = await prepared([texts, []])
                inventory = prepare_facts(evidence)
                self.assertEqual(inventory.candidate.observations, tuple(texts))
                calls = []
                class Auditor:
                    async def audit_answer_units(inner, question, units, spans, plan):
                        record_native_stage('source_auditor')
                        calls.append([u['text'] for u in units])
                        return {'assessments': [{'unit_id': u['id'], 'status': 'supported',
                            'temporal_scope': 'historical', 'temporal_assertion': 'source_observation',
                            'references': [{'span_id': spans[0]['span_id']}]} for u in units]}
                metrics = QueryMetrics(); token = CURRENT_QUERY_METRICS.set(metrics)
                try:
                    final = await AnswerFinalizer(Auditor()).finalize(
                        QUESTION, inventory.candidate, source_pack, evaluated_at='2026-09-09')
                finally:
                    CURRENT_QUERY_METRICS.reset(token)
                self.assertEqual([len(batch) for batch in calls],
                                 [4, 2] if size == 6 else [4] * 20 + [1])
                self.assertEqual(sum(calls, []), ['- ' + text for text in texts])
                self.assertTrue(final['finalization']['answer_verified'])
                batches = 2 if size == 6 else 21
                self.assertEqual(metrics.report()['audit_batches'], batches)
                self.assertEqual(metrics.report()['native_call_ceiling'], batches * 2)
                final['finalization']['reader_inventory_digest'] = inventory.inventory_digest
                self.assertEqual(final['claim_ledger']['summary']['audited'], size)
                self.assertEqual(inventory.bind_final(evidence, final)['summary']['preserved'], size)

    async def test_context_and_narrowed_planner_never_delete_inventory(self):
        from app.question_evidence import CONVERSATION_CONTEXT_MAX_CHARS
        evidence, _ = await prepared(context='x' * CONVERSATION_CONTEXT_MAX_CHARS)
        self.assertEqual(len(prepare_facts(evidence).inventory), 2)
        oversized, _ = await prepared(context='x' * (CONVERSATION_CONTEXT_MAX_CHARS + 1))
        with self.assertRaises(QuestionEvidenceError): prepare_facts(oversized)
        source = evidence.composition_input
        reading = source.pop('source_reading')
        source['question'] = 'What do the Cedar and Maple requests each establish?'
        source['resolved_question'] = 'What does the Cedar request establish?'
        source['requirements'][0]['aspect'] = 'Cedar status only'
        direct = QuestionEvidence(canonical_json(source), canonical_json(reading))
        self.assertEqual(prepare_facts(direct).candidate.observations,
                         ('The Cedar request is pending.', 'The Maple request is pending.'))

    async def test_failed_final_audit_retains_failure_without_conservation_authority(self):
        evidence, source_pack = await prepared()
        inventory = prepare_facts(evidence)
        class Pending:
            async def audit_answer_units(self, *args): await asyncio.Event().wait()
        timeout = await AnswerFinalizer(Pending(), timeout_seconds=.01).finalize(
            QUESTION, inventory.candidate, source_pack, evaluated_at='2026-09-09')
        unavailable = await AnswerFinalizer(None).finalize(
            QUESTION, inventory.candidate, source_pack, evaluated_at='2026-09-09')
        for final in (timeout, unavailable):
            final['finalization']['reader_inventory_digest'] = inventory.inventory_digest
            before = copy.deepcopy(final)
            receipt = inventory.bind_final(evidence, final)
            self.assertEqual(final, before)
            self.assertEqual(receipt['status'], 'unavailable')
            self.assertFalse(receipt['complete'])
            self.assertEqual(receipt['summary']['unavailable'], 2)
            self.assertTrue(all(row['unit_id'] is None for row in receipt['mappings']))
            final['finalization']['fact_conservation'] = receipt
            self.assertIsNone(restore_fact_conservation(final))
        self.assertEqual(timeout['finalization']['disposition'], 'timeout')
        for key, value in (('answer_verified', True), ('complete', True), ('disposition', []),
                           ('answer_digest', 'a' * 64)):
            malformed = copy.deepcopy(timeout); malformed['finalization'][key] = value
            with self.subTest(key=key), self.assertRaises(QuestionEvidenceError):
                inventory.bind_final(evidence, malformed)

    async def test_caller_cancellation_joins_actual_audit_workers(self):
        evidence, source_pack = await prepared([[f'Recorded fact {i}.' for i in range(8)], []])
        inventory = prepare_facts(evidence)
        started, cleaned, active = asyncio.Event(), [], 0
        class Pending:
            async def audit_answer_units(inner, *args):
                nonlocal active
                active += 1
                if active == 2: started.set()
                try: await asyncio.Event().wait()
                finally:
                    await asyncio.sleep(0)
                    cleaned.append(asyncio.current_task().cancelling())
                    active -= 1
        task = asyncio.create_task(AnswerFinalizer(Pending(), concurrency=2).finalize(
            QUESTION, inventory.candidate, source_pack, evaluated_at='2026-09-09'))
        await asyncio.wait_for(started.wait(), 1)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError): await task
        self.assertEqual(active, 0)
        self.assertEqual(cleaned, [1, 1])

    async def test_inventory_is_immutable_and_ids_bind_source_position_text_and_references(self):
        evidence, _ = await prepared([['Status is pending.', 'Status is pending.'], ['Status is pending.']],
            context='User: Tell me about Cedar.')
        first = selection_payload(evidence)
        identities = [row['id'] for row in first['observations']]
        self.assertEqual(len(set(identities)), 3)
        self.assertEqual(identities, [row['id'] for row in selection_payload(evidence)['observations']])
        self.assertNotIn('PRIVATE LIMITATION', json.dumps(first))
        self.assertNotIn('requirements', first); self.assertNotIn('resolved_question', first)
        self.assertIn('Cedar', first['conversation_context'])
        first['observations'][0]['text'] = 'Changed caller copy.'
        inventory = prepare_facts(evidence)
        changed = inventory.inventory; changed[0]['text'] = 'Changed returned copy.'
        self.assertEqual(inventory.candidate.observations, ('Status is pending.',) * 3)
        self.assertEqual(inventory.snapshot_digest, evidence.digest)
        with self.assertRaises((FrozenInstanceError, AttributeError)):
            inventory.snapshot_digest = 'changed'

    async def test_identical_text_for_two_documents_cannot_share_one_subset_survivor(self):
        evidence, source_pack = await prepared([['Status is pending.'], ['Status is pending.']])
        inventory = prepare_facts(evidence)
        class Auditor:
            async def audit_answer_units(self, question, units, spans, plan):
                second = next(s['span_id'] for s in spans if s['document_id'] == 2)
                return {'assessments': [{'unit_id': u['id'],
                    'status': 'unsupported' if len(units) == 2 and i == 0 else 'supported',
                    'temporal_scope': 'historical', 'temporal_assertion': 'source_observation',
                    'references': [{'span_id': second}]} for i, u in enumerate(units)]}
        final = await AnswerFinalizer(Auditor()).finalize(QUESTION, inventory.candidate, source_pack,
                                                         evaluated_at='2026-09-09')
        self.assertEqual(final['finalization']['disposition'], 'partial')
        final['query_plan'] = {**REQUIREMENTS, 'original_question': QUESTION,
            'requirements_status': 'complete', 'pipeline_version': PIPELINE_VERSION,
            'evaluated_at': '2026-09-09', 'source_date_order': 'mdy', 'request_identity_digest': 'a' * 64}
        final['finalization'].update(pipeline_version=PIPELINE_VERSION,
            request_identity_digest='a' * 64, evidence_snapshot_digest=evidence.digest, reader_inventory_digest=prepare_facts(evidence).inventory_digest)
        receipt = inventory.bind_final(evidence, final)
        self.assertEqual(receipt['summary'], {'total': 2, 'preserved': 1, 'excluded': 0,
                                             'unresolved': 1, 'unavailable': 0})
        self.assertEqual([m['unit_id'] for m in receipt['mappings']], [None, 'u1'])
        final['finalization']['fact_conservation'] = receipt
        self.assertEqual(restore_fact_conservation(final), receipt)
        changed = copy.deepcopy(final)
        changed['finalization']['fact_conservation']['mappings'][0].update(unit_id='u1', status='preserved')
        self.assertIsNone(restore_fact_conservation(changed))

    async def test_source_ownership_changed_reading_and_malformed_inventory_cannot_bind(self):
        evidence, source_pack = await prepared()
        inventory = prepare_facts(evidence)
        final = await audited(evidence, source_pack, inventory.candidate, all_sources=True)
        for anchor in (None, False, 'a' * 64):
            changed_final = copy.deepcopy(final)
            changed_final['finalization']['reader_inventory_digest'] = anchor
            with self.subTest(anchor=anchor), self.assertRaisesRegex(QuestionEvidenceError, 'inventory_snapshot_mismatch'):
                inventory.bind_final(evidence, changed_final)
        source = evidence.composition_input; reading = source.pop('source_reading')
        changed_reading = copy.deepcopy(reading)
        changed_reading['documents'][0]['observations'][0]['text'] = 'Unrelated interpretation.'
        changed = QuestionEvidence(canonical_json(source), canonical_json(changed_reading))
        self.assertEqual(changed.digest, evidence.digest)
        with self.assertRaisesRegex(QuestionEvidenceError, 'evidence_snapshot_mismatch'):
            inventory.bind_final(changed, final)
        foreign = reading['documents'][1]['observations'][0]['references'][0]['span_id']
        for refs in ([{'span_id': foreign}], [{'span_id': 'unknown'}], [],
                     reading['documents'][0]['observations'][0]['references'] * 2):
            malformed = copy.deepcopy(reading)
            malformed['documents'][0]['observations'][0]['references'] = refs
            bad = QuestionEvidence(canonical_json(source), canonical_json(malformed))
            with self.assertRaises(QuestionEvidenceError): prepare_facts(bad)
        for value in (None, False, {}, [], '', '  ', 'two\n\nparagraphs'):
            malformed = copy.deepcopy(reading)
            malformed['documents'][0]['observations'][0]['text'] = value
            bad = QuestionEvidence(canonical_json(source), canonical_json(malformed))
            with self.subTest(value=value), self.assertRaises(ValueError): prepare_facts(bad)
        empty, _ = await prepared([[], []])
        empty_inventory = prepare_facts(empty)
        self.assertEqual(empty_inventory.inventory, [])
        self.assertIsNone(empty_inventory.candidate)

    def test_historical_exclusion_parser_remains_strict_without_runtime_authority(self):
        row = {'observation_id': 'omitted', 'decision': 'covered_by', 'target_id': 'selected'}
        valid = json.dumps({'decisions': [row]})
        invalid = [None, '', 'null', 'false', '{}', '[]', 'prefix ' + valid, valid + ' suffix',
                   '```json\n' + valid + '\n```', '{"decisions":[],"decisions":[]}',
                   json.dumps({'decisions': []}), json.dumps({'decisions': [row, row]}),
                   json.dumps({'decisions': [{**row, 'extra': 'not permitted'}]})]
        invalid.extend(json.dumps({'decisions': [{**row, 'target_id': target}]})
                       for target in (None, False, [], ['selected'], 'omitted', 'foreign'))
        for text in invalid:
            with self.subTest(text=text), self.assertRaises(QuestionEvidenceError):
                parse_exclusion(text, 'omitted', ['selected'])
        for classification in ('outside_request', 'reject'):
            result = parse_exclusion(json.dumps({'decisions': [{**row,
                'decision': classification, 'target_id': None}]}), 'omitted', [])
            self.assertEqual(result['decision'], classification)
        for target in ('first', 'second'):
            result = parse_exclusion(json.dumps({'decisions': [{**row, 'target_id': target}]}),
                                     'omitted', ['first', 'second'])
            self.assertEqual(result['target_id'], target)
