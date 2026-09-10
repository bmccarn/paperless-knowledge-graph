"""Conservation augments final coverage without changing the planner's assessment."""
import copy
import json
import unittest

from tests.runtime import configure_test_environment
configure_test_environment()

from app.answer_coverage import augment_fact_coverage, digest
from app.answer_coverage import parse_coverage, restore_question_coverage, restore_pipeline_metadata, unavailable_coverage
from app.answer_finalization import AnswerFinalizer
from app.question_evidence import QuestionEvidence, QuestionEvidenceError, PIPELINE_VERSION
from tests.test_observation_delivery import HandleAuditor
from tests.test_source_dates import pack


class FactCoverageTests(unittest.TestCase):
    def base(self, status='complete'):
        return {'status': status, 'complete': status == 'complete',
                'requirements': [{'requirement_id': 'r1', 'aspect': 'Recorded hours',
                    'status': 'unavailable' if status == 'unavailable' else 'answered',
                    'observation_ids': [] if status == 'unavailable' else ['u1'],
                    'gap_reason': 'coverage_unavailable' if status == 'unavailable' else None}],
                'omitted_requested_aspects': None if status == 'unavailable' else status == 'partial',
                'planning_status': 'complete', 'binding': {'snapshot_digest': 'original'}}

    def conservation(self, status='complete'):
        return {'version': 4, 'status': status, 'complete': status == 'complete',
                'inventory': [{'id': 'f1', 'text': 'Private reader interpretation.'}],
                'mappings': [], 'binding': {'snapshot_digest': 'original'},
                'summary': {'total': 1, 'preserved': int(status == 'complete'), 'excluded': 0,
                            'unresolved': int(status == 'partial'), 'unavailable': int(status == 'unavailable')}}

    def test_all_status_combinations_preserve_the_actual_planner_assessment(self):
        for assessed in ('complete', 'partial', 'unavailable'):
            for conserved in ('complete', 'partial', 'unavailable'):
                with self.subTest(assessed=assessed, conserved=conserved):
                    base, facts = self.base(assessed), self.conservation(conserved)
                    public = augment_fact_coverage(base, facts)
                    complete = assessed == conserved == 'complete'
                    expected = ('unavailable' if 'unavailable' in (assessed, conserved)
                                else 'complete' if complete else 'partial')
                    self.assertEqual(public['status'], expected)
                    self.assertIs(public['complete'], complete)
                    self.assertEqual(public['assessment_status'], assessed)
                    self.assertEqual(public['conservation_status'], conserved)
                    self.assertEqual(public['conservation_summary'], facts['summary'])
                    self.assertEqual(public['requirements'], base['requirements'])
                    self.assertEqual(public['omitted_requested_aspects'], base['omitted_requested_aspects'])
                    self.assertEqual(public['binding']['fact_conservation_digest'], digest(facts))
                    self.assertNotIn('Private reader interpretation', json.dumps(public))

    def test_binding_covers_all_conservation_details_without_mutating_internal_receipts(self):
        base, facts = self.base(), self.conservation()
        before_base, before_facts = copy.deepcopy(base), copy.deepcopy(facts)
        public = augment_fact_coverage(base, facts)
        self.assertEqual(base, before_base)
        self.assertEqual(facts, before_facts)
        facts['inventory'][0]['text'] = 'A changed interpretation.'
        self.assertNotEqual(public['binding']['fact_conservation_digest'],
                            augment_fact_coverage(base, facts)['binding']['fact_conservation_digest'])
        base['requirements'][0]['observation_ids'].clear()
        base['binding']['snapshot_digest'] = 'changed'
        facts['summary']['preserved'] = 0
        self.assertEqual(public['requirements'][0]['observation_ids'], ['u1'])
        self.assertEqual(public['binding']['snapshot_digest'], 'original')
        self.assertEqual(public['conservation_summary']['preserved'], 1)

    def test_invalid_status_flags_or_cross_candidate_binding_do_not_gain_public_coverage(self):
        mutations = (
            lambda base, facts: base.update(complete=1),
            lambda base, facts: facts.update(complete=1),
            lambda base, facts: base.update(complete=False),
            lambda base, facts: facts.update(status='partial'),
            lambda base, facts: facts.update(status=[]),
            lambda base, facts: facts.update(status='unknown'),
            lambda base, facts: facts['binding'].update(snapshot_digest='another candidate'),
            lambda base, facts: base.update(assessment_status='complete'),
            lambda base, facts: facts['summary'].update(total=True),
            lambda base, facts: facts['summary'].update(preserved=-1),
            lambda base, facts: facts['summary'].update(total=2),
            lambda base, facts: facts['summary'].update(explanation='private detail'),
        )
        for change in mutations:
            with self.subTest(change=change):
                base, facts = self.base(), self.conservation()
                change(base, facts)
                with self.assertRaisesRegex(QuestionEvidenceError, '^invalid_fact_coverage$'):
                    augment_fact_coverage(base, facts)


class FactCoverageRestorationTests(unittest.IsolatedAsyncioTestCase):
    async def test_inventory_deletion_cannot_upgrade_partial_when_receipt_and_coverage_are_recomputed(self):
        from app.answer_fact_selection import prepare_facts
        from app.answer_observations import ObservationCandidate
        from tests.test_answer_fact_selection import prepared, audited
        for values in ([['Cedar is pending.', 'Cedar requires authorization.'], []],
                       [['Cedar is pending.'], ['Maple requires authorization.']]):
            with self.subTest(values=values):
                evidence, source_pack = await prepared(values)
                inventory = prepare_facts(evidence)
                final = await audited(evidence, source_pack, ObservationCandidate(('Cedar is pending.',)))
                receipt = inventory.bind_final(evidence, final)
                final['finalization']['fact_conservation'] = receipt
                base = parse_coverage(json.dumps({'requirements': [
                    {'requirement_id': 'r1', 'status': 'answered', 'observation_ids': ['u1']}],
                    'omitted_requested_aspects': False}), evidence, final)
                final['finalization']['question_coverage'] = augment_fact_coverage(base, receipt)
                self.assertEqual(restore_question_coverage(final)['status'], 'partial')
                changed = copy.deepcopy(final)
                receipt = changed['finalization']['fact_conservation']
                receipt['inventory'].pop()
                receipt['mappings'].pop()
                receipt.update(status='complete', complete=True,
                    summary={'total': 1, 'preserved': 1, 'excluded': 0, 'unresolved': 0, 'unavailable': 0})
                # All inner/public checksums and completeness flags agree. The
                # independent original-inventory anchor must still reject it.
                changed['finalization']['question_coverage'] = augment_fact_coverage(base, receipt)
                self.assertIsNone(restore_question_coverage(changed))
                self.assertFalse(restore_pipeline_metadata(changed, changed['answer'])['finalization']['answer_verified'])

    async def prepared(self, *, retained=True, assessment='complete'):
        from app.answer_fact_selection import prepare_facts
        from app.answer_observations import ObservationCandidate
        text = 'Cedar invoice records $20 USD.'
        unrelated = 'The form identifies Casey as the operator.'
        question = 'What amount does the invoice record?'
        requested = {'resolved_question': question, 'requirements': [
            {'id': 'r1', 'aspect': 'Recorded invoice amount', 'temporal_scope': 'none',
             'comparison_scope': 'none'}]}
        source_pack = pack(text + ' ' + unrelated)

        class Adapter:
            async def read_question_sources(inner, payload):
                doc = payload['source_documents'][0]
                handle = doc['windows'][0]['span']['span_id']
                return {'documents': [{'document_id': doc['document_id'], 'observations': [
                    {'text': value, 'references': [{'span_id': handle}]} for value in (text, unrelated)],
                    'limitations': []}]}


        adapter = Adapter()
        evidence = await QuestionEvidence.prepare(adapter, question, requested, source_pack,
                                                  evaluated_at='2026-09-09')
        selection = prepare_facts(evidence)
        candidate = selection.candidate if retained else ObservationCandidate((text,))
        final = await AnswerFinalizer(HandleAuditor()).finalize(
            question, candidate, source_pack, evaluated_at='2026-09-09')
        self.assertTrue(final['finalization']['answer_verified'])
        plan = {**requested, 'original_question': question, 'requirements_status': 'complete',
                'evaluated_at': '2026-09-09', 'source_date_order': 'mdy',
                'request_identity_digest': 'a' * 64, 'pipeline_version': PIPELINE_VERSION}
        final['query_plan'] = plan
        final['finalization'].update(pipeline_version=PIPELINE_VERSION,
            request_identity_digest=plan['request_identity_digest'], evidence_snapshot_digest=evidence.digest, reader_inventory_digest=selection.inventory_digest)
        conservation = selection.bind_final(evidence, final)
        final['finalization']['fact_conservation'] = conservation
        if assessment == 'unavailable':
            base = unavailable_coverage(evidence, final)
        else:
            base = parse_coverage(json.dumps({'requirements': [{'requirement_id': 'r1',
                'status': 'answered', 'observation_ids': ['u1']}],
                'omitted_requested_aspects': assessment == 'partial'}), evidence, final)
        final['finalization']['question_coverage'] = augment_fact_coverage(base, conservation)
        return final, base

    async def test_restoration_distinguishes_missing_inventory_from_planner_unavailability(self):
        for retained in (True, False):
            for assessment in ('complete', 'partial', 'unavailable'):
                with self.subTest(retained=retained, assessment=assessment):
                    final, base = await self.prepared(retained=retained, assessment=assessment)
                    before = copy.deepcopy(final)
                    restored = restore_question_coverage(final)
                    self.assertIsNotNone(restored)
                    self.assertEqual(restored, final['finalization']['question_coverage'])
                    self.assertEqual(restored['assessment_status'], assessment)
                    self.assertEqual(restored['requirements'], base['requirements'])
                    self.assertIs(restored['complete'], retained and assessment == 'complete')
                    self.assertEqual(final, before)

    async def test_fact_and_final_candidate_tampering_invalidates_saved_success(self):
        original, _ = await self.prepared()
        self.assertTrue(restore_question_coverage(original)['complete'])
        mutations = (
            lambda x: x['finalization'].pop('reader_inventory_digest'),
            lambda x: x['finalization'].update(reader_inventory_digest=True),
            lambda x: x['finalization'].update(reader_inventory_digest='a' * 64),
            lambda x: x['finalization'].pop('fact_conservation'),
            lambda x: x['finalization']['fact_conservation']['inventory'][0].update(text='An invented amount.'),
            lambda x: x['finalization']['fact_conservation']['inventory'][0]['references'][0].update(span_id='foreign'),
            lambda x: x['finalization']['fact_conservation'].update(dispositions=[]),
            lambda x: x['finalization']['fact_conservation'].update(reviews=[]),
            lambda x: x['finalization']['fact_conservation']['mappings'][0].update(status='excluded'),
            lambda x: x['finalization']['fact_conservation']['mappings'][0].update(target_id='foreign-fact'),
            lambda x: x['finalization']['fact_conservation'].update(version=1),
            lambda x: x['claim_ledger']['claims'][0].update(claim='- Another recorded amount.'),
            lambda x: x['finalization'].update(candidate_digest='f' * 64),
            lambda x: x['query_plan'].update(pipeline_version='question-evidence-v3'),
            lambda x: x['finalization']['question_coverage'].update(assessment_status='unavailable'),
            lambda x: x['finalization']['question_coverage'].update(complete=1),
            lambda x: x['finalization']['question_coverage']['conservation_summary'].update(preserved=0),
            lambda x: x['finalization']['question_coverage']['conservation_summary'].update(preserved=True),
        )
        for mutate in mutations:
            with self.subTest(mutate=mutate):
                changed = copy.deepcopy(original)
                mutate(changed)
                # Updating the outer digest alone must not bypass conservation's
                # independent structural/source/final-unit validation.
                facts = changed['finalization'].get('fact_conservation')
                if facts is not None:
                    changed['finalization']['question_coverage']['binding']['fact_conservation_digest'] = digest(facts)
                self.assertIsNone(restore_question_coverage(changed))
                restored = restore_pipeline_metadata(changed, changed['answer'])
                self.assertEqual(restored['answer'], original['answer'])
                self.assertFalse(restored['finalization']['answer_verified'])
                self.assertEqual(restored['finalization']['disposition'], 'stored_binding_unavailable')

    async def test_legacy_base_receipt_cannot_certify_v6_even_with_a_valid_fact_receipt(self):
        final, base = await self.prepared()
        final['finalization']['question_coverage'] = base
        self.assertIsNone(restore_question_coverage(final))


if __name__ == '__main__':
    unittest.main()
