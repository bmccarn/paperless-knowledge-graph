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
    async def test_recovered_reader_assertion_still_requires_original_audit_and_new_editor_audit(self):
        question = 'What balance and refund activity are recorded?'
        good = 'The Cedar invoice records a balance of $20 USD.'
        source = 'The Cedar refund request for $10 USD is pending.'
        invented = 'The Cedar refund of $10 USD was paid.'
        pack = {'items': [item(1, good), item(2, source)]}
        p = plan(question, ['Recorded balance', 'Refund activity'])
        orchestrator = StrandsQueryOrchestrator(); orchestrator.enabled = True
        calls, audited_candidates = [], []

        async def model(**kwargs):
            name = kwargs['name']; calls.append(name)
            if name == 'answer_editor':
                self.assertIn(invented, kwargs['prompt'])
                self.assertIn(source, kwargs['prompt'])
                return json.dumps({'observations': [good]})
            x = json.loads(kwargs['prompt'])
            if name == 'source_reader':
                doc = x['source_documents'][0]
                return json.dumps({'documents': [{'document_id': doc['document_id'], 'observations': [
                    {'text': good if doc['document_id'] == 1 else invented,
                     'references': [{'span_id': doc['windows'][0]['span']['span_id']}]}], 'limitations': []}]})
            if name == 'fact_selector':
                return json.dumps({'dispositions': [{'observation_id': row['id'],
                    'status': 'delivered' if i == 0 else 'omitted'} for i, row in enumerate(x['observations'])]})
            if name == 'fact_exclusion':
                return json.dumps({'decisions': [{'observation_id': x['omitted_id'],
                    'decision': 'reject', 'target_id': None}]})
            if name == 'source_auditor':
                audited_candidates.append([u['text'] for u in x['units']])
                self.assertEqual({d['document_id'] for d in x['source_documents']}, {1, 2})
                assessments = []
                for unit in x['units']:
                    wrong = unit['text'] == '- ' + invented
                    doc = next(d for d in x['source_documents'] if d['document_id'] == (2 if wrong else 1))
                    assessments.append(decision(unit_id=unit['id'], status='unsupported' if wrong else 'supported',
                        references=[{'span_id': doc['windows'][0]['span']['span_id']}]))
                return json.dumps({'assessments': assessments})
            if name == 'answer_coverage':
                return json.dumps({'requirements': [
                    {'requirement_id': 'r1', 'status': 'answered', 'observation_ids': ['u1']},
                    {'requirement_id': 'r2', 'status': 'unresolved', 'observation_ids': []}],
                    'omitted_requested_aspects': False})
            self.fail('Unexpected extra stage ' + name)

        with patch.object(orchestrator, '_text_agent', side_effect=model):
            final = await finalize_question(orchestrator, question, pack, p, 'strict')
        final.update(question=question, query_plan=p)
        self.assertEqual(audited_candidates, [['- ' + good, '- ' + invented], ['- ' + good]])
        self.assertTrue(final['finalization']['answer_verified'])
        self.assertNotIn(invented, final['answer'])
        self.assertIn(good, final['answer'])
        facts = final['finalization']['fact_conservation']
        self.assertEqual(facts['reviews'][0]['decision'], 'reject')
        self.assertEqual(facts['mappings'][1]['status'], 'unresolved')
        self.assertEqual(facts['mappings'][1]['reason'], 'missing_final_fact')
        self.assertFalse(restore_question_coverage(final)['complete'])
        self.assertEqual(calls.count('answer_editor'), 1)
        self.assertEqual(calls.count('source_auditor'), 2)
        self.assertNotIn('answer_completion', calls)

    async def test_zero_retained_facts_preserve_reviews_without_a_verified_answer_or_audit(self):
        question = 'What payment is recorded?'
        text = 'The form names Cedar as the operator.'
        pack = {'items': [item(1, text)]}
        p = plan(question, ['Recorded payment'])
        for mode in ('quick', 'deep', 'timeline', 'strict'):
            for review in ('outside_request', 'unavailable'):
                with self.subTest(mode=mode, review=review):
                    orchestrator = StrandsQueryOrchestrator(); orchestrator.enabled = True
                    calls = []

                    async def model(**kwargs):
                        name = kwargs['name']; x = json.loads(kwargs['prompt']); calls.append(name)
                        if name == 'source_reader':
                            doc = x['source_documents'][0]
                            return json.dumps({'documents': [{'document_id': 1, 'observations': [
                                {'text': text, 'references': [{'span_id': doc['windows'][0]['span']['span_id']}]}],
                                'limitations': []}]})
                        if name == 'fact_selector':
                            return json.dumps({'dispositions': [{'observation_id': row['id'], 'status': 'omitted'}
                                                                for row in x['observations']]})
                        if name == 'fact_exclusion':
                            self.assertEqual(x['delivered_ids'], [])
                            if review == 'unavailable': return 'null'
                            return json.dumps({'decisions': [{'observation_id': x['omitted_id'],
                                'decision': 'outside_request', 'target_id': None}]})
                        self.fail('No audit or coverage is permitted: ' + name)

                    with patch.object(orchestrator, '_text_agent', side_effect=model):
                        final = await finalize_question(orchestrator, question, pack, p, mode)
                    self.assertEqual(calls, ['source_reader', 'fact_selector', 'fact_exclusion'])
                    state = final['finalization']
                    self.assertIs(state['answer_verified'], False)
                    self.assertIs(state['complete'], False)
                    self.assertEqual(state['pipeline_failure'], 'no_retained_facts')
                    self.assertEqual(state['pipeline_version'], PIPELINE_VERSION)
                    self.assertEqual(state['request_identity_digest'], p['request_identity_digest'])
                    self.assertEqual(final['evidence']['score'], 0)
                    self.assertEqual(final['claim_ledger']['claims'], [])
                    self.assertNotIn(text, final['answer'])
                    facts = state['fact_conservation']
                    self.assertEqual(facts['version'], 3)
                    self.assertEqual(facts['dispositions'][0]['status'], 'omitted')
                    self.assertEqual(facts['reviews'][0]['status'],
                                     'accepted' if review == 'outside_request' else 'unavailable')
                    self.assertEqual(facts['status'], 'unavailable')
                    self.assertFalse(state['question_coverage']['complete'])

    async def test_rejected_omissions_reach_audit_and_delivery_despite_narrowed_coverage(self):
        question = 'How did scheduled hours change, and what do the latest records establish?'
        texts = {
            1: ['Robin records weekly scheduled hours of 24.'],
            2: ['Robin has an approved change to weekly scheduled hours of 32.'],
            3: ['Robin is permitted 8 additional hours only if training is completed.',
                'Robin completed training; no overtime attendance is recorded.'],
        }
        pack = {'items': [item(k, ' '.join(v)) for k, v in texts.items()]}
        p = plan(question, ['Earlier scheduled hours', 'Latest scheduled hours'])
        p['resolved_question'] = 'How did scheduled hours change?'
        for mode in ('quick', 'deep', 'timeline', 'strict'):
            with self.subTest(mode=mode):
                orchestrator = StrandsQueryOrchestrator(); orchestrator.enabled = True
                calls, audited_texts = [], []

                async def model(**kwargs):
                    name = kwargs['name']; x = json.loads(kwargs['prompt']); calls.append(name)
                    if name == 'source_reader':
                        doc = x['source_documents'][0]
                        span = doc['windows'][0]['span']['span_id']
                        return json.dumps({'documents': [{'document_id': doc['document_id'],
                            'observations': [{'text': value, 'references': [{'span_id': span}]}
                                             for value in texts[doc['document_id']]], 'limitations': []}]})
                    if name == 'fact_selector':
                        return json.dumps({'dispositions': [{'observation_id': row['id'],
                            'status': 'delivered' if i < 2 else 'omitted'}
                            for i, row in enumerate(x['observations'])]})
                    if name == 'fact_exclusion':
                        return json.dumps({'decisions': [{'observation_id': x['omitted_id'],
                            'decision': 'reject', 'target_id': None}]})
                    if name == 'source_auditor':
                        self.assertEqual({d['document_id'] for d in x['source_documents']}, {1, 2, 3})
                        audited_texts.extend(u['text'] for u in x['units'])
                        assessments = []
                        for unit in x['units']:
                            owner = next(k for k, values in texts.items() if unit['text'][2:] in values)
                            doc = next(d for d in x['source_documents'] if d['document_id'] == owner)
                            assessments.append(decision(unit_id=unit['id'],
                                references=[{'span_id': doc['windows'][0]['span']['span_id']}],
                                temporal_scope='historical', temporal_assertion='source_observation'))
                        return json.dumps({'assessments': assessments})
                    if name == 'answer_coverage':
                        # Reproduce the false-negative gap detection: the narrowed
                        # planner is satisfied and would not trigger completion.
                        return json.dumps({'requirements': [
                            {'requirement_id': 'r1', 'status': 'answered', 'observation_ids': ['u1']},
                            {'requirement_id': 'r2', 'status': 'answered', 'observation_ids': ['u2']}],
                            'omitted_requested_aspects': False})
                    self.fail('Unexpected stage ' + name)

                with patch.object(orchestrator, '_text_agent', side_effect=model):
                    final = await finalize_question(orchestrator, question, pack, p, mode)
                final.update(question=question, query_plan=p)
                expected = [text for values in texts.values() for text in values]
                self.assertEqual(audited_texts, ['- ' + text for text in expected])
                self.assertEqual([c['claim'] for c in final['claim_ledger']['claims']], audited_texts)
                self.assertTrue(final['finalization']['answer_verified'], final['claim_ledger'])
                self.assertTrue(restore_question_coverage(final)['complete'])
                facts = final['finalization']['fact_conservation']
                self.assertEqual([row['status'] for row in facts['dispositions']],
                                 ['delivered', 'delivered', 'omitted', 'omitted'])
                self.assertEqual([row['decision'] for row in facts['reviews']], ['reject', 'reject'])
                self.assertEqual(facts['summary']['preserved'], 4)
                self.assertEqual(calls.count('source_reader'), 3)
                self.assertEqual(calls.count('fact_selector'), 1)
                self.assertEqual(calls.count('fact_exclusion'), 2)
                self.assertEqual(calls.count('source_auditor'), 1)
                self.assertEqual(calls.count('answer_coverage'), 1)
                self.assertNotIn('answer_completion', calls)

    async def test_followup_context_resolves_subject_without_narrowed_plan_authority(self):
        question='What charge does it record?'
        context='User: Tell me about account Alpha.'
        p=plan(question,['The referenced account charge'],context)
        p['resolved_question']='What is account Beta charge?'
        texts={1:'Account Alpha records a monthly charge of $100 USD.',
               2:'Account Beta records a monthly charge of $200 USD.'}
        pack={'items':[item(k,v) for k,v in texts.items()]}
        orchestrator=StrandsQueryOrchestrator();orchestrator.enabled=True
        for exclusion_decision in ('outside_request','unavailable'):
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
                    if exclusion_decision == 'unavailable': return 'null'
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
