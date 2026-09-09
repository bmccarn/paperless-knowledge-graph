"""Final answer and its historical date view are one certified candidate."""
import copy
import hashlib
import unittest
from app.answer_finalization import AnswerFinalizer
from app.timeline import restore_timeline
from tests.test_source_dates import ExactAuditor, pack


class TimelineProjectionTests(unittest.IsolatedAsyncioTestCase):
    async def result(self, claim, source=None, **kwargs):
        return await AnswerFinalizer(ExactAuditor()).finalize(
            'What was documented?', claim, pack(source or claim), mode='timeline', **kwargs)

    async def test_request_does_not_become_completed_action_and_dates_preserve_roles(self):
        claim = 'Cedar requested a service cancellation on January 3, 2024, with a requested effective date of February 1, 2024.'
        result = await self.result(claim)
        self.assertTrue(result['finalization']['answer_verified'])
        events = result['timeline_events']
        self.assertEqual([e['date'] for e in events], ['2024-01-03', '2024-02-01'])
        for event in events:
            self.assertEqual(event['claim'], claim)
            self.assertEqual(event['presentation'], 'answer_context')
            self.assertNotIn('summary', event)
            self.assertNotIn('title', event)
            self.assertEqual(event['references'], result['claim_ledger']['claims'][0]['references'])
            self.assertEqual(result['claim_ledger']['candidate_text'][event['date_start']:event['date_end']], event['date_text'])
        self.assertEqual(restore_timeline(result)[0], events)

    async def test_prose_headings_stay_in_full_answer_context_when_dates_sort(self):
        claim = '## Invoice Alpha\n\nAccount: A1.\n\nIssued: January 3, 2025.\n\n## Invoice Beta\n\nAccount: B2.\n\nIssued: February 4, 2024.'
        result = await self.result(claim)
        self.assertEqual([e['date'] for e in result['timeline_events']], ['2024-02-04', '2025-01-03'])
        self.assertTrue(all(e['presentation'] == 'answer_context' for e in result['timeline_events']))
        self.assertEqual(result['claim_ledger']['candidate_text'], claim)

    async def test_precision_equivalence_dedup_and_explicit_calendar_range(self):
        result = await self.result('The Cedar service term is 2024-2025; renewal was in March 2026, confirmed March 2, 2026 (2026-03-02).')
        self.assertEqual([(e['date'], e['precision']) for e in result['timeline_events']],
                         [('2024', 'year'), ('2025', 'year'), ('2026-03', 'month'), ('2026-03-02', 'day')])
        self.assertEqual(len(result['timeline_events']), 4)

    async def test_no_dates_differs_from_unavailable(self):
        for claim in ['The 2023 vehicle has a charge of $1200 USD.', 'Invoice number 2026-09-01.', 'Service date 09/01/26.']:
            result = await self.result(claim)
            self.assertTrue(result['finalization']['answer_verified'], claim)
            self.assertEqual(result['finalization']['timeline']['status'], 'no_dates', claim)
            self.assertEqual(restore_timeline(result)[1]['status'], 'no_dates')

    async def test_invalid_dates_and_expanded_centuries_cannot_publish(self):
        for claim, source in [('Record date 2026-02-30.', 'Record date 2026-02-30.'),
                              ('Record date September 1, 2026.', 'Record date 09/01/26.'),
                              ('Recorded date 2026-09-01.', 'Policy #2026-09-01.'),
                              ('Recorded date 2026-09-01.', 'Record identifier: 08042026040459.'),
                              ('Recorded date 2026-09-01.', 'Record date 2026-09-01-02.'),
                              ('Recorded date 2026-09-01.', 'Record date 2026-09-01/02.'),
                              ('Record date 2026-02.', 'Record date 2026-02-30.')]:
            result = await self.result(claim, source)
            self.assertEqual(result['timeline_events'], [], (claim, source))
            self.assertEqual(result['finalization']['timeline']['status'], 'unavailable')

    async def test_source_order_and_written_calendar_equivalence(self):
        for order, expected in [('mdy','2026-04-08'), ('dmy','2026-08-04')]:
            result = await AnswerFinalizer(ExactAuditor(), date_order=order).finalize(
                'Recorded date?', 'Service date 04/08/2026.', pack('Service date 04/08/2026.'), mode='timeline')
            self.assertEqual(result['timeline_events'][0]['date'], expected)
        result = await AnswerFinalizer(ExactAuditor(), date_order='reject_ambiguous').finalize(
            'Recorded date?', 'Service date 04/08/2026.', pack('Service date 04/08/2026.'), mode='timeline')
        self.assertEqual(result['timeline_events'], [])
        result = await self.result('Service date 2026-09-01.', 'Service date September 1, 2026.')
        self.assertEqual(result['timeline_events'][0]['references'][0]['quote'], 'Service date September 1, 2026.')

    async def test_restoration_rejects_swapped_or_modified_binding_without_mutation(self):
        original = await self.result('Service requested January 3, 2024.')
        def offset(r): r['claim_ledger']['claims'][0]['start'] += 1
        def refs(r): r['timeline_events'][0]['references'][0]['document_id'] = 999
        def ledger_refs(r): r['claim_ledger']['claims'][0]['references'][0]['document_id'] = 999
        def digest(r): r['claim_ledger']['candidate_digest'] = '0' * 64
        def date(r): r['timeline_events'][0]['date'] = '2025-01-03'
        def title(r): r['timeline_events'][0]['title'] = 'Service completed'
        def answer(r):
            r['answer'] = 'Another valid answer.'
            r['finalization']['answer_digest'] = hashlib.sha256(r['answer'].encode()).hexdigest()
        for mutate in [offset, refs, ledger_refs, digest, date, title, answer]:
            result = copy.deepcopy(original); mutate(result); before=copy.deepcopy(result)
            self.assertEqual(restore_timeline(result)[1]['status'], 'unavailable', mutate.__name__)
            self.assertEqual(result, before)
        other = await self.result('Invoice issued January 4, 2025.')
        other['claim_ledger'] = original['claim_ledger']
        other['finalization']['candidate_digest'] = original['finalization']['candidate_digest']
        other['finalization']['timeline'] = original['finalization']['timeline']
        other['timeline_events'] = original['timeline_events']
        self.assertEqual(restore_timeline(other)[1]['status'], 'unavailable')

    async def test_failed_quick_incomplete_and_legacy_metadata_cannot_upgrade(self):
        original = await self.result('Service requested January 3, 2024.')
        for disposition in ['unaudited','unsupported','audit_failed','conflicting','corpus_changed']:
            result=copy.deepcopy(original);result['finalization']['disposition']=disposition
            self.assertEqual(restore_timeline(result)[0], [])
        for key in ['candidate_text','complete']:
            result=copy.deepcopy(original);result['claim_ledger'].pop(key)
            self.assertEqual(restore_timeline(result)[0], [])
        result=copy.deepcopy(original);result['finalization'].pop('timeline')
        self.assertEqual(restore_timeline(result)[0], [])
        self.assertEqual(restore_timeline(None)[0], [])

    async def test_all_references_preserved_without_single_document_reassociation(self):
        class Multi(ExactAuditor):
            async def audit_answer_units(self, q, units, spans, plan):
                return {'assessments':[{'unit_id':u['id'],'status':'supported','temporal_scope':'historical',
                    'references':[{'span_id':s['span_id']} for s in spans]} for u in units]}
        evidence=pack('Cedar requested service January 3, 2024.')
        evidence['items'].append({'id':'other','document_id':102,'title':'Acknowledgment',
                                 'content':'Cedar service request acknowledged January 4, 2024.'})
        claim='Cedar requested service January 3, 2024 and its request was acknowledged January 4, 2024.'
        result=await AnswerFinalizer(Multi()).finalize('Service history?',claim,evidence,mode='timeline')
        self.assertEqual(len(result['timeline_events']),2)
        for event in result['timeline_events']:
            self.assertEqual({r['document_id'] for r in event['references']},{101,102})
            self.assertNotIn('document_id',event)

    async def test_compound_unsupported_and_omitted_observations_never_resurrect(self):
        class Mixed(ExactAuditor):
            async def audit_answer_units(self,*args):
                result=await super().audit_answer_units(*args)
                for unit, assessment in zip(args[1],result['assessments']):
                    if 'completed' in unit['text']: assessment['status']='unsupported'
                return result
            async def repair_answer(self,*args):
                return {'observations':['Cedar requested service January 3, 2024.',
                    'Cedar completed service February 4, 2024.', 'Cedar requested renewal March 5, 2025.']}
        source='Cedar requested service January 3, 2024. Cedar completed service February 4, 2024. Cedar requested renewal March 5, 2025.'
        result=await AnswerFinalizer(Mixed(),Mixed()).finalize('History?',source,pack(source),mode='timeline')
        self.assertEqual(result['finalization']['disposition'],'partial')
        self.assertEqual([e['date'] for e in result['timeline_events']],['2024-01-03','2025-03-05'])
        self.assertTrue(all(e['presentation']=='observation' for e in result['timeline_events']))
        self.assertTrue(all('completed' not in event['claim'] for event in result['timeline_events']))
        self.assertEqual(restore_timeline(result)[0],result['timeline_events'])

    async def test_malformed_audit_and_conflicting_source_prevent_projection(self):
        class Malformed:
            async def audit_answer_units(self,*args): return {'assessments':[None]}
        class Conflict(ExactAuditor):
            async def audit_answer_units(self,*args):
                result=await super().audit_answer_units(*args)
                self.asserted_counterevidence=any('never occurred' in s['content'] for s in args[2])
                for a in result['assessments']: a['status']='conflicting'
                return result
        evidence=pack('Meeting date February 29, 2024.')
        evidence['items'].append({'id':'correction','document_id':102,'title':'Correction','content':'The meeting never occurred.'})
        auditor=Conflict()
        for adapter in [Malformed(),auditor]:
            result=await AnswerFinalizer(adapter).finalize('When was the meeting?', 'Meeting date February 29, 2024.',evidence,mode='timeline')
            self.assertEqual(result['timeline_events'],[])
        self.assertTrue(auditor.asserted_counterevidence)

    async def test_safe_source_handles_preserve_tail_dates_and_window_diagnostics(self):
        from tests.test_observation_delivery import HandleAuditor
        source='x'*4200+' Cedar invoice dated January 1, 2026 records $20.'
        result=await AnswerFinalizer(HandleAuditor()).finalize('Dated invoice history?',
            'Cedar invoice dated January 1, 2026 records $20.',pack(source),mode='timeline')
        self.assertEqual(len(result['timeline_events']),1)
        ref=result['timeline_events'][0]['references'][0]
        self.assertEqual(ref['quote'],source[ref['start']:ref['end']])
        self.assertEqual(result['claim_ledger']['source_diagnostics']['unavailable_citation_windows'],1)
