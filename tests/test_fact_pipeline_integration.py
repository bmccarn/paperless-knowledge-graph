"""Real inactive pipeline with deterministic transports, never model-accuracy claims."""
import json
import unittest
from unittest.mock import patch

from tests.runtime import configure_test_environment
configure_test_environment()
from app.question_pipeline import finalize_question
from app.question_evidence import PIPELINE_VERSION
from app.strands_orchestrator import StrandsQueryOrchestrator
from app.answer_coverage import restore_question_coverage
from tests.source_audit_fixtures import decision


def item(identity,content):
    return {'id':f'doc-{identity}','document_id':identity,'chunk_index':0,'title':f'Record {identity}',
            'source_kind':'ocr','content':content,'source_content':content}


def plan(question,aspects,context=''):
    return {'original_question':question,'resolved_question':question,
            'requirements':[{'id':f'r{i}','aspect':text,'temporal_scope':'none','comparison_scope':'none'}
                            for i,text in enumerate(aspects,1)],
            'evaluated_at':'2026-09-09','source_date_order':'mdy','requirements_status':'complete',
            'request_identity_digest':'b'*64,'pipeline_version':PIPELINE_VERSION,'conversation_context':context}


class FactPipelineTests(unittest.IsolatedAsyncioTestCase):
    async def test_followup_context_resolves_subject_without_narrowed_plan_authority(self):
        question='What charge does it record?'
        context='User: Tell me about account Alpha.'
        p=plan(question,['The referenced account charge'],context)
        p['resolved_question']='What is account Beta charge?'
        texts={1:'Account Alpha records a monthly charge of $100 USD.',
               2:'Account Beta records a monthly charge of $200 USD.'}
        pack={'items':[item(k,v) for k,v in texts.items()]}
        orchestrator=StrandsQueryOrchestrator();orchestrator.enabled=True
        for exclusion_decision in ('outside_request','reject'):
            payloads=[]
            async def model(**kwargs):
                name=kwargs['name'];x=json.loads(kwargs['prompt']);payloads.append((name,x))
                if name=='source_reader':
                    doc=x['source_documents'][0]
                    return json.dumps({'documents':[{'document_id':doc['document_id'],
                        'observations':[{'text':texts[doc['document_id']],
                         'references':[{'span_id':doc['windows'][0]['span']['span_id']}]}],'limitations':[]}]})
                if name=='fact_selector':
                    self.assertEqual(x['original_question'],question)
                    self.assertEqual(x['conversation_context'],context)
                    self.assertNotIn('resolved_question',x);self.assertNotIn('requirements',x)
                    return json.dumps({'dispositions':[{'observation_id':r['id'],
                        'status':'delivered' if 'Alpha' in r['text'] else 'omitted'} for r in x['observations']]})
                if name=='fact_exclusion':
                    self.assertEqual(x['conversation_context'],context)
                    return json.dumps({'decisions':[{'observation_id':x['omitted_id'],
                        'decision':exclusion_decision,'target_id':None}]})
                if name=='source_auditor':
                    doc=next(d for d in x['source_documents'] if d['document_id']==1)
                    return json.dumps({'assessments':[decision(unit_id=u['id'],
                        references=[{'span_id':doc['windows'][0]['span']['span_id']}]) for u in x['units']]})
                if name=='answer_coverage':
                    return json.dumps({'requirements':[{'requirement_id':'r1','status':'answered',
                        'observation_ids':['u1']}],'omitted_requested_aspects':False})
                self.fail('Unexpected stage '+name)
            with patch.object(orchestrator,'_text_agent',side_effect=model):
                final=await finalize_question(orchestrator,question,pack,p,'strict')
            final.update(question=question,query_plan=p)
            self.assertTrue(final['finalization']['answer_verified'],final)
            self.assertIn(texts[1],final['answer']);self.assertNotIn(texts[2],final['answer'])
            self.assertEqual(restore_question_coverage(final)['complete'],exclusion_decision=='outside_request')
            self.assertEqual(sum(n=='fact_exclusion' for n,_ in payloads),1)

    async def test_derived_comparison_uses_completion_and_combined_source_audit(self):
        question='What are the original and revised amounts, and how much did they decrease?'
        first='The invoice records an original charge of $310 USD.'
        second='The invoice records a revised balance of $270 USD.'
        pack={'items':[item(1,first+' '+second+' Credit adjustment: $40 USD.')]}
        p=plan(question,['Original amount','Revised amount','Amount of decrease'])
        orchestrator=StrandsQueryOrchestrator();orchestrator.enabled=True
        for difference in (40,41):
            calls=[]
            comparison=f'The original charge of $310 USD exceeds the revised balance of $270 USD by ${difference} USD.'
            async def model(**kwargs):
                name=kwargs['name'];x=json.loads(kwargs['prompt']);calls.append(name)
                if name=='source_reader':
                    doc=x['source_documents'][0];span=doc['windows'][0]['span']['span_id']
                    return json.dumps({'documents':[{'document_id':1,'observations':[
                        {'text':t,'references':[{'span_id':span}]} for t in (first,second)],'limitations':[]}]})
                if name=='fact_selector':
                    return json.dumps({'dispositions':[{'observation_id':r['id'],'status':'delivered'}
                        for r in x['observations']]})
                if name=='answer_completion':
                    span=x['source_documents'][0]['windows'][0]['span']['span_id']
                    return json.dumps({'observations':[comparison],
                        'requirement_mapping':[{'requirement_id':'r3','status':'proposed','observation_ids':['u1']}],
                        'source_references':[{'observation_id':'u1','span_ids':[span]}]})
                if name=='source_auditor':
                    span=x['source_documents'][0]['windows'][0]['span']['span_id']
                    return json.dumps({'assessments':[decision(unit_id=u['id'],references=[{'span_id':span}],
                        status='unsupported' if '$41' in u['text'] else 'supported') for u in x['units']]})
                if name=='answer_coverage':
                    added='answer_completion' in calls
                    return json.dumps({'requirements':[
                        {'requirement_id':'r1','status':'answered','observation_ids':['u1']},
                        {'requirement_id':'r2','status':'answered','observation_ids':['u2']},
                        {'requirement_id':'r3','status':'answered' if added else 'unresolved',
                         'observation_ids':['u3'] if added else []}],'omitted_requested_aspects':False})
                self.fail('Unexpected stage '+name)
            with patch.object(orchestrator,'_text_agent',side_effect=model):
                final=await finalize_question(orchestrator,question,pack,p,'strict')
            final.update(question=question,query_plan=p)
            self.assertTrue(final['finalization']['answer_verified'],final)
            self.assertIn(first,final['answer']);self.assertIn(second,final['answer'])
            self.assertEqual(comparison in final['answer'],difference==40)
            self.assertEqual(restore_question_coverage(final)['complete'],difference==40)
            self.assertEqual(calls.count('answer_completion'),1)
            self.assertEqual(calls.count('source_auditor'),2)
