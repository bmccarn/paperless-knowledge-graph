"""Assertion frame and calendar applicability through the real audit/finalizer seam."""
import json
import unittest
from unittest.mock import AsyncMock, patch

from tests.runtime import configure_test_environment
configure_test_environment()
from app.source_audit import parse_decisions
from app.strands_orchestrator import StrandsQueryOrchestrator
from app.answer_finalization import AnswerFinalizer
from app.answer_observations import ObservationCandidate
from tests.source_audit_fixtures import decision

SOURCE = ('Equipment form. Current capacity: 480 units. Custodian: Quinn Example. '
          'Change approval: blank. This form does not establish present-day operating status.')
PACK = {'items': [{'id': 'form', 'document_id': 17, 'chunk_index': 0,
                  'source_kind': 'ocr', 'content': SOURCE, 'source_content': SOURCE}]}
REPORT = 'The form records the current capacity field as 480 units.'
ANCILLARY = 'The custodian field names Quinn Example.'
WORLD = 'The equipment currently has capacity of 480 units.'


class SourceRelativeTemporalTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.auditor = StrandsQueryOrchestrator(); self.auditor.enabled = True
        self.addAsyncCleanup(self.auditor.close)

    def row(self, *, scope='none', assertion='source_observation', temporal='not_applicable', **fields):
        row=decision(temporal_scope=scope,temporal_assertion=assertion,**fields)
        row['checks']['temporal']=temporal
        return row

    async def final(self, texts, rows, *, question='What does the equipment form record?', mode='strict', repairer=None):
        async def native(name, system_prompt, prompt, **kwargs):
            self.assertEqual(name,'source_auditor')
            data=json.loads(prompt)
            handle=data['source_spans'][0]['span_id']
            return json.dumps({'assessments':[{**row,'unit_id':unit['id'],
                            'references':[{'span_id':handle}]} for row,unit in zip(rows,data['units'])]})
        candidate=ObservationCandidate.from_response({'observations':texts})
        with patch.object(self.auditor,'_text_agent',side_effect=native) as calls:
            result=await AnswerFinalizer(self.auditor,repairer).finalize(question,candidate,PACK,
                           plan={'requires_current':False},mode=mode,evaluated_at='2026-09-10')
        return result,calls.await_count

    def test_undated_source_frame_is_valid_without_calendar_applicability(self):
        parsed=parse_decisions(json.dumps({'assessments':[self.row()]}),['u1'])
        self.assertEqual(parsed['assessments'][0]['status'],'supported')
        self.assertEqual(parsed['assessments'][0]['semantic_decision']['rejection_reasons'],[])

    async def test_source_field_and_unrelated_fact_survive_all_modes(self):
        for mode in ('quick','deep','timeline','strict'):
            with self.subTest(mode=mode):
                result,calls=await self.final([REPORT,ANCILLARY],[self.row(),self.row(assertion='none')],mode=mode)
                self.assertTrue(result['finalization']['answer_verified'])
                self.assertEqual(calls,1)
                self.assertIn(REPORT,result['answer']); self.assertIn(ANCILLARY,result['answer'])
                self.assertEqual(result['finalization']['disposition'],'qualified')

    async def test_dated_source_and_unrelated_nontemporal_unit_do_not_conflict(self):
        result,_=await self.final([REPORT,ANCILLARY],[self.row(scope='historical',temporal='supported'),
                                                    self.row(assertion='none')])
        self.assertTrue(result['finalization']['answer_verified'])

    async def test_source_frame_cannot_override_negative_semantics(self):
        for facet in ('subject','predicate','record_role','conditions','temporal','comparison'):
            row=self.row();row['checks'][facet]='not_established'
            result,calls=await self.final([REPORT],[row])
            self.assertFalse(result['finalization']['answer_verified'],facet)
            self.assertEqual(calls,1,facet)

    async def test_qualified_unit_cannot_launder_unscoped_or_present_world_unit(self):
        for other in (self.row(assertion='none'),self.row(scope='current',assertion='present_world',temporal='supported')):
            result,_=await self.final([REPORT,WORLD],[self.row(),other])
            self.assertFalse(result['finalization']['answer_verified'])

    async def test_old_current_bearing_none_none_remains_insufficient(self):
        result,_=await self.final([REPORT],[self.row(assertion='none')])
        self.assertEqual(result['finalization']['disposition'],'current_unresolved')

    async def test_current_question_with_only_nontemporal_claims_is_unresolved(self):
        result,_=await self.final([ANCILLARY],[self.row(assertion='none')],question='Who is the current custodian?')
        self.assertEqual(result['finalization']['disposition'],'current_unresolved')

    async def test_inconsistent_calendar_scope_cannot_be_treated_as_undated_source(self):
        result,_=await self.final([REPORT],[self.row(scope='historical')])
        self.assertFalse(result['finalization']['answer_verified'])

    async def test_exact_temporal_failure_reaches_editor_and_public_explanation(self):
        editor=type('Editor',(),{'repair_answer':AsyncMock(return_value={'observations':[WORLD]})})()
        result,_=await self.final([WORLD],[self.row(assertion='none')],repairer=editor)
        findings=editor.repair_answer.await_args.args[3]
        expected=[{'unit_id':'u1','reason':'unscoped_current_assertion'}]
        self.assertEqual(findings['temporal_failures'],expected)
        self.assertEqual(result['current_state']['temporal_failures'],expected)
        self.assertNotIn('Not every material claim has validated source support.',result['verification']['missing_evidence'])

    async def test_full_question_pipeline_and_saved_receipts_keep_source_frame(self):
        import copy
        from app.question_pipeline import finalize_question
        from app.answer_coverage import restore_question_coverage, restore_pipeline_metadata
        from app.answer_fact_selection import restore_fact_conservation
        from tests.test_fact_pipeline_integration import plan
        question='What capacity and custodian does the form record?'
        for mode in ('quick','deep','timeline','strict'):
            for scoped in (True,False):
                with self.subTest(mode=mode,scoped=scoped):
                    p=plan(question,['Recorded capacity','Named custodian'])
                    calls=[]
                    async def native(name,system_prompt,prompt,**kwargs):
                        calls.append(name)
                        if name=='answer_editor':
                            return json.dumps({'observations':[REPORT,ANCILLARY]})
                        data=json.loads(prompt)
                        if name=='source_reader':
                            doc=data['source_documents'][0]
                            return json.dumps({'documents':[{'document_id':doc['document_id'],
                                'observations':[{'text':text,'references':[{'span_id':doc['windows'][0]['span']['span_id']}]} for text in (REPORT,ANCILLARY)],
                                'limitations':[]}]})
                        if name=='source_auditor':
                            handle=data['source_documents'][0]['windows'][0]['span']['span_id']
                            rows=[self.row(assertion='source_observation' if scoped else 'none'),self.row(assertion='none')]
                            return json.dumps({'assessments':[{**row,'unit_id':unit['id'],'references':[{'span_id':handle}]}
                                            for row,unit in zip(rows,data['units'])]})
                        if name=='answer_editor':
                            return json.dumps({'observations':[REPORT,ANCILLARY]})
                        if name=='answer_coverage':
                            return json.dumps({'requirements':[{'requirement_id':f'r{i}','status':'answered','observation_ids':[f'u{i}']}
                                                              for i in (1,2)],'omitted_requested_aspects':False})
                        self.fail('Unexpected stage '+name)
                    with patch.object(self.auditor,'_text_agent',side_effect=native):
                        final=await finalize_question(self.auditor,question,PACK,p,mode)
                    final.update(question=question,query_plan=p,mode=mode)
                    self.assertEqual(restore_pipeline_metadata(final,final['answer']),final)
                    if scoped:
                        self.assertTrue(final['finalization']['answer_verified'])
                        self.assertEqual(calls,['source_reader','source_auditor','answer_coverage'])
                        self.assertTrue(restore_question_coverage(final)['complete'])
                        self.assertTrue(restore_fact_conservation(final)['complete'])
                        changed=copy.deepcopy(final)
                        changed['claim_ledger']['claims'][0]['temporal_assertion']='none'
                        self.assertIsNone(restore_question_coverage(changed))
                        self.assertFalse(restore_pipeline_metadata(changed,changed['answer'])['finalization']['answer_verified'])
                    else:
                        self.assertFalse(final['finalization']['answer_verified'])
                        self.assertEqual(final['finalization']['disposition'],'current_unresolved')
                        self.assertIsNone(restore_question_coverage(final))
                        self.assertNotIn('answer_coverage',calls)

    async def test_unrelated_report_cannot_launder_question_implied_current_claim(self):
        for answer in (ANCILLARY,'The custodian is Quinn Example.'):
            result,_=await self.final([answer,REPORT],[self.row(assertion='none'),self.row()],
                                      question='Who is the current custodian?')
            self.assertFalse(result['finalization']['answer_verified'])
            self.assertIn({'unit_id':'u1','reason':'current_question_not_source_scoped'},
                          result['current_state']['temporal_failures'])

    async def test_explicit_undated_report_can_qualify_current_question_without_world_claim(self):
        result,_=await self.final([ANCILLARY,REPORT],[self.row(),self.row()],
                                  question='Who is the current custodian?')
        self.assertTrue(result['finalization']['answer_verified'])
        self.assertIn('Current status',result['answer'])
        self.assertIn('not established',result['answer'])

    async def test_active_comparison_metadata_cannot_hide_in_undated_source_report(self):
        for scope,ids in (('retrieved_documents',[999]),('retrieved_documents',[]),(None,[17])):
            row=self.row(comparison_scope=scope,comparison_document_ids=ids)
            row['checks']['comparison']='supported'
            result,calls=await self.final([REPORT],[row])
            self.assertFalse(result['finalization']['answer_verified'],(scope,ids))
            # Contradictory metadata gets one correction, never automatic acceptance.
            self.assertEqual(calls,2)

    async def test_documented_comparison_still_requires_actual_compared_sources(self):
        for ids,accepted in (([17],True),([999],False),([],False)):
            row=self.row(scope='documented',assertion='retrieved_comparison',temporal='supported',
                         comparison_scope='retrieved_documents',comparison_document_ids=ids)
            row['checks']['comparison']='supported'
            result,_=await self.final([REPORT],[row])
            self.assertEqual(result['finalization']['answer_verified'],accepted,ids)
