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
        return {'version': 2, 'status': status, 'complete': status == 'complete',
                'inventory': [{'id': 'f1', 'text': 'Private reader interpretation.'}],
                'reviews': [], 'binding': {'snapshot_digest': 'original'},
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
    async def prepared(self, *, review='accepted', assessment='complete'):
        from app.answer_fact_selection import select_facts
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

            async def select_question_facts(inner, payload):
                return json.dumps({'dispositions': [{'observation_id': row['id'],
                    'status': 'delivered' if index == 0 else 'omitted'}
                    for index, row in enumerate(payload['observations'])]})

            async def review_fact_exclusion(inner, payload):
                if review == 'unavailable':
                    raise TimeoutError('private transport detail')
                # The exclusion interface supplies one proposal alongside the
                # immutable inventory. Locate its ID without guessing one.
                excluded = next(row['id'] for row in payload['observations'] if row['text'] == unrelated)
                return json.dumps({'decisions': [{'observation_id': excluded,
                    'decision': 'outside_request' if review == 'accepted' else 'reject', 'target_id': None}]})

        adapter = Adapter()
        evidence = await QuestionEvidence.prepare(adapter, question, requested, source_pack,
                                                  evaluated_at='2026-09-09')
        selection = await select_facts(adapter, evidence)
        final = await AnswerFinalizer(HandleAuditor()).finalize(
            question, selection.candidate, source_pack, evaluated_at='2026-09-09')
        self.assertTrue(final['finalization']['answer_verified'])
        plan = {**requested, 'original_question': question, 'requirements_status': 'complete',
                'evaluated_at': '2026-09-09', 'source_date_order': 'mdy',
                'request_identity_digest': 'a' * 64, 'pipeline_version': PIPELINE_VERSION}
        final['query_plan'] = plan
        final['finalization'].update(pipeline_version=PIPELINE_VERSION,
            request_identity_digest=plan['request_identity_digest'], evidence_snapshot_digest=evidence.digest)
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

    async def test_restoration_distinguishes_conservation_unavailability_from_planner_unavailability(self):
        for review in ('accepted', 'rejected', 'unavailable'):
            for assessment in ('complete', 'partial', 'unavailable'):
                with self.subTest(review=review, assessment=assessment):
                    final, base = await self.prepared(review=review, assessment=assessment)
                    before = copy.deepcopy(final)
                    restored = restore_question_coverage(final)
                    self.assertIsNotNone(restored)
                    self.assertEqual(restored, final['finalization']['question_coverage'])
                    self.assertEqual(restored['assessment_status'], assessment)
                    self.assertEqual(restored['requirements'], base['requirements'])
                    self.assertIs(restored['complete'], review == 'accepted' and assessment == 'complete')
                    self.assertEqual(final, before)

    async def test_fact_and_final_candidate_tampering_invalidates_saved_success(self):
        original, _ = await self.prepared()
        self.assertTrue(restore_question_coverage(original)['complete'])
        mutations = (
            lambda x: x['finalization'].pop('fact_conservation'),
            lambda x: x['finalization']['fact_conservation']['inventory'][0].update(text='An invented amount.'),
            lambda x: x['finalization']['fact_conservation']['inventory'][0]['references'][0].update(span_id='foreign'),
            lambda x: x['finalization']['fact_conservation']['dispositions'][0].update(status='omitted'),
            lambda x: x['finalization']['fact_conservation']['reviews'][0].update(status='rejected'),
            lambda x: x['finalization']['fact_conservation']['reviews'][0].update(decision='covered_by'),
            lambda x: x['finalization']['fact_conservation']['reviews'][0].update(target_id='foreign-fact'),
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

    async def test_legacy_base_receipt_cannot_certify_v3_even_with_a_valid_fact_receipt(self):
        final, base = await self.prepared()
        final['finalization']['question_coverage'] = base
        self.assertIsNone(restore_question_coverage(final))


if __name__ == '__main__':
    unittest.main()
