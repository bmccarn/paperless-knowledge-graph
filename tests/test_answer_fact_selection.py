"""Source fact routing conserves meaning without granting factual support."""
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


class Adapter:
    """Controlled responses at the native model boundary."""
    def __init__(self, select=None, review=None):
        self.select = select
        self.review = review
        self.selections = []
        self.exclusions = []

    async def select_question_facts(self, payload):
        self.selections.append(copy.deepcopy(payload))
        if self.select:
            return await self.select(payload)
        return json.dumps({'dispositions': [{'observation_id': row['id'],
            'status': 'delivered'} for row in payload['observations']]})

    async def review_fact_exclusion(self, payload):
        self.exclusions.append(copy.deepcopy(payload))
        if self.review:
            return await self.review(payload)
        return json.dumps({'decisions': [{'observation_id': payload['omitted_id'],
            'decision': 'outside_request', 'target_id': None}]})


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
        request_identity_digest='a' * 64, evidence_snapshot_digest=evidence.digest)
    return final


class FactSelectionTests(unittest.IsolatedAsyncioTestCase):
    async def test_direct_context_bound_is_enforced_before_selection(self):
        from app.answer_fact_selection import select_facts
        from app.question_evidence import CONVERSATION_CONTEXT_MAX_CHARS
        evidence, _ = await prepared(context='x' * CONVERSATION_CONTEXT_MAX_CHARS)
        self.assertEqual(len((await select_facts(Adapter(), evidence)).inventory), 2)
        evidence, _ = await prepared(context='x' * (CONVERSATION_CONTEXT_MAX_CHARS + 1))
        adapter = Adapter()
        with self.assertRaises(QuestionEvidenceError):
            await select_facts(adapter, evidence)
        self.assertEqual(adapter.selections, [])

    async def test_standalone_multipart_question_keeps_both_aspects_despite_narrowed_plan(self):
        from app.answer_fact_selection import select_facts
        evidence, _ = await prepared()
        source = evidence.composition_input
        reading = source.pop('source_reading')
        source['question'] = 'What do the Cedar and Maple requests each establish?'
        source['resolved_question'] = 'What does the Cedar request establish?'
        source['requirements'][0]['aspect'] = 'Cedar status only'
        direct = QuestionEvidence(canonical_json(source), canonical_json(reading))

        async def select(payload):
            self.assertEqual(payload['original_question'], source['question'])
            self.assertNotIn('conversation_context', payload)
            self.assertNotIn('resolved_question', payload)
            self.assertNotIn('requirements', payload)
            self.assertEqual({r['text'] for r in payload['observations']}, {
                'The Cedar request is pending.', 'The Maple request is pending.'})
            return json.dumps({'dispositions': [{'observation_id': r['id'],
                'status': 'delivered'} for r in payload['observations']]})

        selection = await select_facts(Adapter(select=select), direct)
        self.assertEqual(selection.candidate.observations,
                         ('The Cedar request is pending.', 'The Maple request is pending.'))

    async def test_failed_final_audit_retains_failure_without_conservation_authority(self):
        from app.answer_fact_selection import select_facts, restore_fact_conservation
        evidence, pack = await prepared()
        selection = await select_facts(Adapter(), evidence)

        class Pending:
            async def audit_answer_units(self, *args):
                await asyncio.Event().wait()

        timeout = await AnswerFinalizer(Pending(), timeout_seconds=.01).finalize(
            QUESTION, selection.candidate, pack, evaluated_at='2026-09-09')
        unavailable = await AnswerFinalizer(None).finalize(
            QUESTION, selection.candidate, pack, evaluated_at='2026-09-09')
        for final in (timeout, unavailable):
            before = copy.deepcopy(final)
            receipt = selection.bind_final(evidence, final)
            self.assertEqual(final, before)
            self.assertFalse(final['finalization']['answer_verified'])
            self.assertEqual(receipt['status'], 'unavailable')
            self.assertFalse(receipt['complete'])
            self.assertEqual(receipt['summary']['unavailable'], 2)
            self.assertTrue(all(row['unit_id'] is None for row in receipt['mappings']))
            final['finalization']['fact_conservation'] = receipt
            self.assertIsNone(restore_fact_conservation(final))
        self.assertEqual(timeout['finalization']['disposition'], 'timeout')
        for key, value in (('answer_verified', True), ('complete', True), ('disposition', []),
                           ('answer_digest', 'a' * 64)):
            malformed = copy.deepcopy(timeout)
            malformed['finalization'][key] = value
            with self.subTest(key=key), self.assertRaises(QuestionEvidenceError):
                selection.bind_final(evidence, malformed)

    async def test_exact_facts_have_immutable_position_and_source_bound_identities(self):
        from app.answer_fact_selection import select_facts, selection_payload
        evidence, _ = await prepared([['Status is pending.', 'Status is pending.'], ['Status is pending.']],
            context='User: Tell me about Cedar.\nAssistant: An untrusted earlier answer.')
        first = selection_payload(evidence)
        identities = [row['id'] for row in first['observations']]
        self.assertEqual(len(set(identities)), 3)
        self.assertEqual(identities, [row['id'] for row in selection_payload(evidence)['observations']])
        self.assertNotIn('PRIVATE LIMITATION', json.dumps(first))
        self.assertNotIn('requirements', first)
        self.assertNotIn('resolved_question', first)
        self.assertIn('User: Tell me about Cedar.', first['conversation_context'])
        first['observations'][0]['text'] = 'Changed caller copy.'

        async def mutate_payload(payload):
            rows = [{'observation_id': row['id'], 'status': 'delivered'}
                    for row in payload['observations']]
            payload['observations'][0]['text'] = 'Changed model copy.'
            return json.dumps({'dispositions': rows})

        adapter = Adapter(select=mutate_payload)
        selected = await select_facts(adapter, evidence)
        self.assertEqual(selected.candidate.observations, ('Status is pending.',) * 3)
        self.assertEqual(selected.snapshot_digest, evidence.digest)
        self.assertEqual(adapter.exclusions, [])
        with self.assertRaises((FrozenInstanceError, AttributeError)):
            selected.snapshot_digest = 'changed'

    async def test_each_exclusion_receives_only_its_decision_and_failures_stay_unavailable(self):
        from app.answer_fact_selection import select_facts
        evidence, _ = await prepared([['Cedar is pending.', 'A signature is recorded.', 'No receipt is recorded.'],
                                      ['Maple is pending.']])

        async def select(payload):
            ids = [o['id'] for o in payload['observations']]
            return json.dumps({'dispositions': [
                {'observation_id': identity, 'status': 'delivered' if index == 0 else 'omitted'}
                for index, identity in reversed(list(enumerate(ids)))]})

        async def review(payload):
            self.assertNotIn('proposal', payload)
            self.assertEqual(len(payload['delivered_ids']), 1)
            self.assertEqual(len(payload['observations']), 4)
            identity = payload['omitted_id']
            index = next(i for i, row in enumerate(payload['observations']) if row['id'] == identity)
            if index == 1:
                return json.dumps({'decisions': [{'observation_id': identity, 'decision': 'reject', 'target_id': None}]})
            if index == 2:
                return json.dumps({'decisions': [{'observation_id': 'foreign', 'decision': 'outside_request', 'target_id': None}]})
            return json.dumps({'decisions': [{'observation_id': identity, 'decision': 'outside_request', 'target_id': None}]})

        adapter = Adapter(select, review)
        selected = await select_facts(adapter, evidence)
        self.assertEqual(selected.candidate.observations, ('Cedar is pending.',))
        self.assertEqual([r['status'] for r in selected.reviews], ['rejected', 'unavailable', 'accepted'])
        self.assertEqual(len(adapter.exclusions), 3)
        copied = selected.reviews
        copied[0]['status'] = 'accepted'
        self.assertEqual(selected.reviews[0]['status'], 'rejected')

    async def test_identical_text_for_two_documents_cannot_share_one_subset_survivor(self):
        from app.answer_fact_selection import select_facts, restore_fact_conservation
        evidence, pack = await prepared([['Status is pending.'], ['Status is pending.']])
        selected = await select_facts(Adapter(), evidence)

        class Auditor:
            async def audit_answer_units(self, question, units, spans, plan):
                second = next(s['span_id'] for s in spans if s['document_id'] == 2)
                return {'assessments': [{'unit_id': u['id'],
                    'status': 'unsupported' if len(units) == 2 and i == 0 else 'supported',
                    'temporal_scope': 'historical', 'temporal_assertion': 'source_observation',
                    'references': [{'span_id': second}]} for i, u in enumerate(units)]}

        final = await AnswerFinalizer(Auditor()).finalize(QUESTION, selected.candidate, pack,
                                                         evaluated_at='2026-09-09')
        self.assertEqual(final['finalization']['disposition'], 'partial')
        final['query_plan'] = {**REQUIREMENTS, 'original_question': QUESTION,
            'requirements_status': 'complete', 'pipeline_version': PIPELINE_VERSION,
            'evaluated_at': '2026-09-09', 'source_date_order': 'mdy',
            'request_identity_digest': 'a' * 64}
        final['finalization'].update(pipeline_version=PIPELINE_VERSION,
            request_identity_digest='a' * 64, evidence_snapshot_digest=evidence.digest)
        receipt = selected.bind_final(evidence, final)
        self.assertEqual(receipt['status'], 'partial')
        self.assertEqual(receipt['summary'], {'total': 2, 'preserved': 1, 'excluded': 0,
                                             'unresolved': 1, 'unavailable': 0})
        self.assertEqual([m['unit_id'] for m in receipt['mappings']], [None, 'u1'])
        final['finalization']['fact_conservation'] = receipt
        self.assertEqual(restore_fact_conservation(final), receipt)
        changed = copy.deepcopy(final)
        changed['finalization']['fact_conservation']['mappings'][0].update(unit_id='u1', status='preserved')
        self.assertIsNone(restore_fact_conservation(changed))

    async def test_approved_duplicate_is_the_only_way_to_share_a_final_unit(self):
        from app.answer_fact_selection import select_facts, restore_fact_conservation
        evidence, pack = await prepared([['Cedar is pending.', 'Cedar is pending.'], []])

        async def choose_duplicate(payload):
            left, right = [o['id'] for o in payload['observations']]
            return json.dumps({'dispositions': [
                {'observation_id': left, 'status': 'delivered'},
                {'observation_id': right, 'status': 'omitted'}]})

        async def classify_duplicate(payload):
            self.assertNotIn('proposal', payload)
            return json.dumps({'decisions': [{'observation_id': payload['omitted_id'],
                'decision': 'covered_by', 'target_id': payload['delivered_ids'][0]}]})

        duplicate = await select_facts(Adapter(select=choose_duplicate, review=classify_duplicate), evidence)
        final = await audited(evidence, pack, duplicate.candidate)
        receipt = duplicate.bind_final(evidence, final)
        self.assertEqual(receipt['version'], 2)
        self.assertEqual(receipt['dispositions'][1]['status'], 'omitted')
        self.assertEqual(receipt['reviews'][0]['decision'], 'covered_by')
        self.assertTrue(receipt['complete'])
        self.assertEqual(receipt['summary'], {'total': 2, 'preserved': 1, 'excluded': 1,
                                             'unresolved': 0, 'unavailable': 0})
        self.assertEqual([r['unit_id'] for r in receipt['mappings']], ['u1', 'u1'])
        final['finalization']['fact_conservation'] = receipt
        self.assertEqual(restore_fact_conservation(final), receipt)

        selected_twice = await select_facts(Adapter(), evidence)
        unapproved_collapse = selected_twice.bind_final(evidence, final)
        self.assertEqual(unapproved_collapse['summary']['unresolved'], 1)
        self.assertFalse(unapproved_collapse['complete'])

        rewritten = await audited(evidence, pack, ObservationCandidate((
            "The sentence 'Cedar is pending.' was recorded.",)))
        lost = duplicate.bind_final(evidence, rewritten)
        self.assertEqual(lost['summary']['unresolved'], 2)
        self.assertEqual(lost['mappings'][1]['reason'], 'missing_covered_target')

    async def test_only_reviewer_classifies_omission_and_targets_are_bound_to_actual_selection(self):
        from app.answer_fact_selection import select_facts, parse_exclusion, restore_fact_conservation
        evidence, pack = await prepared([['Cedar is pending.', 'Cedar is pending.', 'A signature is recorded.'], []])

        async def select(payload):
            return json.dumps({'dispositions': [{'observation_id': row['id'],
                'status': 'omitted' if index == 1 else 'delivered'}
                for index, row in enumerate(payload['observations'])]})

        async def covered(payload):
            self.assertEqual(set(payload), {'original_question', 'evaluated_at', 'source_documents',
                'observations', 'delivered_ids', 'omitted_id'})
            self.assertEqual(payload['omitted_id'], payload['observations'][1]['id'])
            return json.dumps({'decisions': [{'observation_id': payload['omitted_id'],
                'decision': 'covered_by', 'target_id': payload['delivered_ids'][0]}]})

        adapter = Adapter(select=select, review=covered)
        selection = await select_facts(adapter, evidence)
        self.assertEqual(len(adapter.exclusions), 1)
        final = await audited(evidence, pack, selection.candidate)
        receipt = selection.bind_final(evidence, final)
        final['finalization']['fact_conservation'] = receipt
        self.assertEqual(restore_fact_conservation(final), receipt)
        for mutate in (lambda r: r.update(version=1),
                       lambda r: r['reviews'][0].update(decision='outside_request'),
                       lambda r: r['reviews'][0].update(target_id=selection.inventory[2]['id']),
                       lambda r: r['reviews'][0].update(status='unavailable'),
                       lambda r: r['dispositions'][1].update(status='outside_request')):
            changed = copy.deepcopy(final)
            mutate(changed['finalization']['fact_conservation'])
            self.assertIsNone(restore_fact_conservation(changed))

        # A supported sibling does not stand in for the declared target removed
        # by the final audit; the proposal and reviewer decision are not changed.
        retained_sibling = await audited(evidence, pack, ObservationCandidate(('A signature is recorded.',)))
        lost = selection.bind_final(evidence, retained_sibling)
        self.assertEqual(lost['summary']['preserved'], 1)
        self.assertEqual(lost['summary']['unresolved'], 2)
        self.assertEqual(lost['mappings'][1]['reason'], 'missing_covered_target')

        # A model-mutated delivered-ID copy cannot expand parser authority.
        async def mutate_targets(payload):
            payload['delivered_ids'].append('foreign')
            return json.dumps({'decisions': [{'observation_id': payload['omitted_id'],
                'decision': 'covered_by', 'target_id': 'foreign'}]})
        rejected = await select_facts(Adapter(select=select, review=mutate_targets), evidence)
        self.assertEqual(rejected.reviews[0]['status'], 'unavailable')
        self.assertIsNone(rejected.reviews[0]['target_id'])

        # Either valid single target is mechanically allowed. The model and
        # frozen semantic gold, not this parser, establish complete equivalence.
        for target in ('first', 'second'):
            row = parse_exclusion(json.dumps({'decisions': [{'observation_id': 'omitted',
                'decision': 'covered_by', 'target_id': target}]}), 'omitted', ['first', 'second'])
            self.assertEqual(row['target_id'], target)

    async def test_exclusion_protocol_rejects_legacy_malformed_and_nonselected_targets_without_retry(self):
        from app.answer_fact_selection import parse_exclusion, select_facts
        row = {'observation_id': 'omitted', 'decision': 'covered_by', 'target_id': 'selected'}
        valid = json.dumps({'decisions': [row]})
        invalid = [None, '', 'null', 'false', '{}', '[]', 'prefix ' + valid, valid + ' suffix',
                   '```json\n' + valid + '\n```', '{"decisions":[],"decisions":[]}',
                   json.dumps({'decisions': []}), json.dumps({'decisions': [row, row]}),
                   json.dumps({'decisions': [{**row, 'extra': 'not permitted'}]})]
        invalid.extend(json.dumps({'decisions': [{**row, 'target_id': target}]})
                       for target in (None, False, [], ['selected'], 'omitted', 'other-omission', 'foreign'))
        invalid.extend(json.dumps({'decisions': [{**row, 'decision': decision}]})
                       for decision in ('accept', 'reject', 'outside_request', None, False))
        invalid.append(json.dumps({'decisions': [{'observation_id': 'omitted', 'decision': 'reject'}]}))
        invalid.append(json.dumps({'decisions': [{**row, 'observation_id': 'foreign'}]}))
        for text in invalid:
            with self.subTest(text=text), self.assertRaises(QuestionEvidenceError):
                parse_exclusion(text, 'omitted', ['selected'])
        for decision, status in (('outside_request', 'accepted'), ('reject', 'rejected')):
            result = parse_exclusion(json.dumps({'decisions': [
                {**row, 'decision': decision, 'target_id': None}]}), 'omitted', ['selected'])
            self.assertEqual(result['status'], status)
        for delivered in ([], None, False, ['selected', 'selected'], ['omitted'], [False]):
            with self.subTest(delivered=delivered), self.assertRaises(QuestionEvidenceError):
                parse_exclusion(valid, 'omitted', delivered)

        evidence, _ = await prepared()
        async def select(payload):
            return json.dumps({'dispositions': [{'observation_id': row['id'],
                'status': 'delivered' if index == 0 else 'omitted'}
                for index, row in enumerate(payload['observations'])]})
        async def malformed(payload): return 'null'
        adapter = Adapter(select=select, review=malformed)
        selection = await select_facts(adapter, evidence)
        self.assertEqual(len(adapter.exclusions), 1)
        self.assertEqual(selection.reviews[0]['status'], 'unavailable')
        self.assertIsNone(selection.reviews[0]['decision'])

    async def test_source_ownership_and_changed_reading_are_checked_before_binding(self):
        from app.answer_fact_selection import select_facts
        evidence, pack = await prepared()
        selected = await select_facts(Adapter(), evidence)
        final = await audited(evidence, pack, selected.candidate, all_sources=True)
        source = evidence.composition_input
        reading = source.pop('source_reading')
        changed_reading = copy.deepcopy(reading)
        changed_reading['documents'][0]['observations'][0]['text'] = 'An unrelated reader interpretation.'
        changed = QuestionEvidence(canonical_json(source), canonical_json(changed_reading))
        self.assertEqual(changed.digest, evidence.digest)
        with self.assertRaisesRegex(QuestionEvidenceError, 'evidence_snapshot_mismatch'):
            selected.bind_final(changed, final)

        foreign = reading['documents'][1]['observations'][0]['references'][0]['span_id']
        for refs in ([{'span_id': foreign}], [{'span_id': 'unknown'}], [],
                     reading['documents'][0]['observations'][0]['references'] * 2):
            malformed = copy.deepcopy(reading)
            malformed['documents'][0]['observations'][0]['references'] = refs
            bad = QuestionEvidence(canonical_json(source), canonical_json(malformed))
            adapter = Adapter()
            with self.assertRaises(QuestionEvidenceError): await select_facts(adapter, bad)
            self.assertEqual(adapter.selections, [])

    async def test_malformed_and_empty_selections_never_start_exclusion_review(self):
        from app.answer_fact_selection import select_facts
        evidence, _ = await prepared()
        for invalid in (None, '', 'null', 'false', '{}', '[]',
                        '{"dispositions":[],"dispositions":[]}'):
            async def respond(payload, invalid=invalid): return invalid
            adapter = Adapter(select=respond)
            with self.subTest(invalid=invalid), self.assertRaises(QuestionEvidenceError):
                await select_facts(adapter, evidence)
            self.assertEqual(adapter.exclusions, [])

        async def invalid_rows(payload, variant):
            a, b = [row['id'] for row in payload['observations']]
            row = lambda identity, status='delivered': {'observation_id': identity, 'status': status}
            variants = [[row(a)], [row(a), row(a)], [row(a), row('foreign')],
                [row(a, 'duplicate_of'), row(b)],
                [{**row(a), 'target_id': None}, row(b)],
                [row(a, 'omitted'), row(b, 'omitted')], [row(a, 'outside_request'), row(b)]]
            return json.dumps({'dispositions': variants[variant]})
        for variant in range(7):
            async def respond(payload, variant=variant): return await invalid_rows(payload, variant)
            adapter = Adapter(select=respond)
            with self.subTest(variant=variant), self.assertRaises(QuestionEvidenceError):
                await select_facts(adapter, evidence)
            self.assertEqual(adapter.exclusions, [])
        empty, _ = await prepared([[], []])
        adapter = Adapter()
        with self.assertRaisesRegex(QuestionEvidenceError, 'empty_fact_inventory'):
            await select_facts(adapter, empty)
        self.assertEqual(adapter.selections, [])

    async def test_exclusion_timeout_preserves_completed_reviews_and_bounds_worker_allocation(self):
        from app.answer_fact_selection import select_facts
        from app.config import settings
        from app.query_metrics import CURRENT_QUERY_METRICS, QueryMetrics
        evidence, _ = await prepared([['Keep the requested fact.'] +
                                     [f'Additional source observation {i}.' for i in range(40)], []])
        baseline_tasks = len(asyncio.all_tasks())
        active = 0
        peak = 0
        cleanups = []
        maximum_tasks = 0

        async def select(payload):
            return json.dumps({'dispositions': [{'observation_id': row['id'],
                'status': 'delivered' if i == 0 else 'omitted'}
                for i, row in enumerate(payload['observations'])]})

        async def review(payload):
            nonlocal active, peak, maximum_tasks
            identity = payload['omitted_id']
            if identity == payload['observations'][1]['id']:
                return json.dumps({'decisions': [{'observation_id': identity, 'decision': 'reject', 'target_id': None}]})
            active += 1
            peak = max(peak, active)
            maximum_tasks = max(maximum_tasks, len(asyncio.all_tasks()) - baseline_tasks)
            try:
                await asyncio.Event().wait()
            finally:
                await asyncio.sleep(0)
                cleanups.append(asyncio.current_task().cancelling())
                active -= 1

        adapter = Adapter(select, review)
        metrics = QueryMetrics()
        token = CURRENT_QUERY_METRICS.set(metrics)
        try:
            with patch.object(settings, 'strands_max_concurrent_calls', 2), \
                 patch.object(settings, 'answer_audit_timeout_seconds', 0.05):
                selected = await select_facts(adapter, evidence)
        finally:
            CURRENT_QUERY_METRICS.reset(token)
        self.assertEqual(metrics.exclusion_observations, 40)
        self.assertEqual(peak, 2)
        self.assertLessEqual(maximum_tasks, 3)
        self.assertEqual(active, 0)
        self.assertEqual(cleanups, [1, 1])
        self.assertEqual(len(adapter.exclusions), 3)
        self.assertEqual(selected.reviews[0]['status'], 'rejected')
        self.assertTrue(all(row['status'] == 'unavailable' and row['reason'] == 'review_timeout'
                            for row in selected.reviews[1:]))

    async def test_caller_cancellation_joins_review_workers_and_never_returns_a_selection(self):
        from app.answer_fact_selection import select_facts
        from app.config import settings
        evidence, _ = await prepared([['Keep.', 'Other.', 'Another.'], []])
        active = 0
        started = asyncio.Event()
        cleaned = []

        async def select(payload):
            return json.dumps({'dispositions': [{'observation_id': row['id'],
                'status': 'delivered' if i == 0 else 'omitted'}
                for i, row in enumerate(payload['observations'])]})

        async def review(payload):
            nonlocal active
            active += 1
            if active == 2: started.set()
            try: await asyncio.Event().wait()
            finally:
                await asyncio.sleep(0)
                cleaned.append(asyncio.current_task().cancelling())
                active -= 1

        with patch.object(settings, 'strands_max_concurrent_calls', 2), \
             patch.object(settings, 'answer_audit_timeout_seconds', 10):
            task = asyncio.create_task(select_facts(Adapter(select, review), evidence))
            await asyncio.wait_for(started.wait(), 1)
            task.cancel()
            with self.assertRaises(asyncio.CancelledError): await task
        self.assertEqual(active, 0)
        self.assertEqual(cleaned, [1, 1])
