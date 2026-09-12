"""Real inactive pipeline with deterministic transports, never model-accuracy claims."""
import json
import unittest
from unittest.mock import patch

from tests.runtime import configure_test_environment
configure_test_environment()
from app.question_pipeline import finalize_question
from app.question_evidence import PIPELINE_VERSION
from app.strands_orchestrator import StrandsQueryOrchestrator
from app.answer_coverage import restore_question_coverage, digest
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
    async def test_source_resolved_referent_and_ambiguous_association_keep_independent_audit_authority(self):
        # Controlled decisions check authority, not model reasoning reliability.
        question = 'What do the capacity record and review memo establish?'
        unique = 'Approval for the change was granted.'
        ambiguous = 'The UNIT-J change was approved.'
        repaired = 'The review memo records approval without identifying which unit change was approved.'
        originals = {
            1: 'UNIT-K capacity increase request. Approval for the change was granted.',
            2: 'Review memo: UNIT-J and UNIT-M changes were requested. Approval is recorded, '
               'but the memo does not identify which unit change was approved.',
        }
        pack = {'items': [item(k, value) for k, value in originals.items()]}
        p = plan(question, ['Capacity approval', 'Review memo approval scope'])
        orchestrator = StrandsQueryOrchestrator(); orchestrator.enabled = True
        calls, audits = [], []
        async def model(**kwargs):
            name = kwargs['name']; calls.append(name)
            if name == 'answer_editor':
                self.assertIn(originals[2], kwargs['prompt'])
                return json.dumps({'observations': [unique, repaired]})
            x = json.loads(kwargs['prompt'])
            if name == 'source_reader':
                doc = x['source_documents'][0]
                return json.dumps({'documents': [{'document_id': doc['document_id'], 'observations': [
                    {'text': unique if doc['document_id'] == 1 else ambiguous,
                     'references': [{'span_id': doc['windows'][0]['span']['span_id']}]}], 'limitations': []}]})
            if name == 'source_auditor':
                audits.append([u['text'] for u in x['units']])
                rows = []
                for u in x['units']:
                    wrong = u['text'] == '- ' + ambiguous
                    owner = 1 if u['text'] == '- ' + unique else 2
                    doc = next(d for d in x['source_documents'] if d['document_id'] == owner)
                    row = decision(unit_id=u['id'], status='unsupported' if wrong else 'supported',
                        references=[{'span_id': doc['windows'][0]['span']['span_id']}])
                    if wrong: row['checks']['subject'] = 'not_established'
                    rows.append(row)
                return json.dumps({'assessments': rows})
            if name == 'answer_coverage':
                return json.dumps({'requirements': [
                    {'requirement_id': 'r1', 'status': 'answered', 'observation_ids': ['u1']},
                    {'requirement_id': 'r2', 'status': 'answered', 'observation_ids': ['u2']}],
                    'omitted_requested_aspects': False})
            self.fail('Unexpected stage ' + name)
        with patch.object(orchestrator, '_text_agent', side_effect=model):
            final = await finalize_question(orchestrator, question, pack, p, 'strict')
        final.update(question=question, query_plan=p)
        self.assertEqual(audits, [['- ' + unique, '- ' + ambiguous], ['- ' + unique, '- ' + repaired]])
        self.assertTrue(final['finalization']['answer_verified'])
        self.assertIn(unique, final['answer'])
        self.assertIn(repaired, final['answer'])
        self.assertNotIn(ambiguous, final['answer'])
        receipt = final['finalization']['fact_conservation']
        self.assertEqual(final['finalization']['reader_inventory_digest'], digest(receipt['inventory']))
        self.assertEqual([r['status'] for r in receipt['mappings']], ['preserved', 'unresolved'])
        self.assertEqual(restore_question_coverage(final)['status'], 'partial')
        self.assertEqual(calls.count('answer_editor'), 1)
        self.assertNotIn('fact_selector', calls)
        self.assertNotIn('fact_exclusion', calls)

    async def test_approval_and_material_negatives_cannot_be_removed_by_semantic_filters(self):
        question = 'How has capacity changed, and what is the latest documented capacity?'
        facts = {
            1: ['UNIT-Z had authorized capacity of 300 units in January 2020.'],
            2: ['UNIT-Z selected an increase to 480 units effective April 1, 2024.',
                'Approval for the UNIT-Z capacity increase was granted March 21, 2024.',
                'The UNIT-Z record leaves the change-of-custodian option unselected.'],
        }
        pack = {'items': [item(k, ' '.join(v)) for k, v in facts.items()]}
        p = plan(question, ['Capacity history', 'Latest documented capacity'])
        for mode in ('quick', 'deep', 'timeline', 'strict'):
            with self.subTest(mode=mode):
                orchestrator = StrandsQueryOrchestrator(); orchestrator.enabled = True
                calls, audited_text = [], []
                async def model(**kwargs):
                    name = kwargs['name']; x = json.loads(kwargs['prompt']); calls.append(name)
                    if name == 'source_reader':
                        doc = x['source_documents'][0]
                        return json.dumps({'documents': [{'document_id': doc['document_id'],
                            'observations': [{'text': text, 'references': [
                                {'span_id': doc['windows'][0]['span']['span_id']}]} for text in facts[doc['document_id']]],
                            'limitations': ['PRIVATE PROCESSING NOTE must never become an answer unit.']}]})
                    if name == 'fact_selector':
                        return json.dumps({'dispositions': [{'observation_id': row['id'],
                            'status': 'delivered' if i < 2 else 'omitted'} for i, row in enumerate(x['observations'])]})
                    if name == 'fact_exclusion':
                        return json.dumps({'decisions': [{'observation_id': x['omitted_id'],
                            'decision': 'outside_request', 'target_id': None}]})
                    if name == 'source_auditor':
                        audited_text.extend(u['text'] for u in x['units'])
                        assessments = []
                        for u in x['units']:
                            identity = next(k for k, texts in facts.items() if u['text'][2:] in texts)
                            d = next(d for d in x['source_documents'] if d['document_id'] == identity)
                            assessments.append(decision(unit_id=u['id'], temporal_scope='historical',
                                temporal_assertion='source_observation',
                                references=[{'span_id': d['windows'][0]['span']['span_id']}]))
                        return json.dumps({'assessments': assessments})
                    if name == 'answer_coverage':
                        return json.dumps({'requirements': [
                            {'requirement_id': 'r1', 'status': 'answered', 'observation_ids': ['u1', 'u2']},
                            {'requirement_id': 'r2', 'status': 'answered', 'observation_ids': ['u2']}],
                            'omitted_requested_aspects': False})
                    self.fail('Unexpected stage: ' + name)
                with patch.object(orchestrator, '_text_agent', side_effect=model):
                    final = await finalize_question(orchestrator, question, pack, p, mode)
                final.update(question=question, query_plan=p)
                expected = ['- ' + text for values in facts.values() for text in values]
                self.assertEqual(audited_text, expected)
                self.assertEqual([c['claim'] for c in final['claim_ledger']['claims']], expected)
                self.assertTrue(restore_question_coverage(final)['complete'])
                self.assertNotIn('fact_selector', calls)
                self.assertNotIn('fact_exclusion', calls)
                self.assertNotIn('PRIVATE PROCESSING NOTE', final['answer'])

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
        self.assertNotIn('reviews', facts)
        self.assertNotIn('fact_selector', calls)
        self.assertNotIn('fact_exclusion', calls)
        self.assertEqual(facts['mappings'][1]['status'], 'unresolved')
        self.assertEqual(facts['mappings'][1]['reason'], 'missing_final_fact')
        self.assertFalse(restore_question_coverage(final)['complete'])
        self.assertEqual(calls.count('answer_editor'), 1)
        self.assertEqual(calls.count('source_auditor'), 2)
        self.assertNotIn('answer_completion', calls)

    async def test_empty_reader_inventory_is_unverified_without_audit_or_coverage(self):
        question = 'What payment is recorded?'
        text = 'The form names Cedar as the operator.'
        pack = {'items': [item(1, text)]}
        p = plan(question, ['Recorded payment'])
        for mode in ('quick', 'deep', 'timeline', 'strict'):
            with self.subTest(mode=mode):
                orchestrator = StrandsQueryOrchestrator(); orchestrator.enabled = True
                calls = []
                async def model(**kwargs):
                    name = kwargs['name']; calls.append(name)
                    self.assertEqual(name, 'source_reader')
                    return json.dumps({'documents': [{'document_id': 1, 'observations': [],
                        'limitations': ['The available excerpt does not provide a requested observation.']}]})
                with patch.object(orchestrator, '_text_agent', side_effect=model):
                    final = await finalize_question(orchestrator, question, pack, p, mode)
                self.assertEqual(calls, ['source_reader'])
                state = final['finalization']
                self.assertIs(state['answer_verified'], False)
                self.assertIs(state['complete'], False)
                self.assertEqual(state['pipeline_failure'], 'empty_fact_inventory')
                self.assertEqual(state['pipeline_version'], PIPELINE_VERSION)
                self.assertEqual(state['request_identity_digest'], p['request_identity_digest'])
                self.assertEqual(final['evidence']['score'], 0)
                self.assertEqual(final['claim_ledger']['claims'], [])
                self.assertNotIn(text, final['answer'])
                facts = state['fact_conservation']
                self.assertEqual(facts['version'], 4)
                self.assertEqual(facts['inventory'], [])
                self.assertEqual(facts['mappings'], [])
                self.assertNotIn('dispositions', facts)
                self.assertNotIn('reviews', facts)
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
                self.assertNotIn('dispositions', facts)
                self.assertNotIn('reviews', facts)
                self.assertEqual(facts['summary']['preserved'], 4)
                self.assertEqual(calls.count('source_reader'), 3)
                self.assertEqual(calls.count('fact_selector'), 0)
                self.assertEqual(calls.count('fact_exclusion'), 0)
                self.assertEqual(calls.count('source_auditor'), 1)
                self.assertEqual(calls.count('answer_coverage'), 1)
                self.assertNotIn('answer_completion', calls)

    async def test_followup_and_narrowed_plan_never_remove_original_reader_facts(self):
        question = 'What charge does it record?'
        context = 'User: Tell me about account Alpha.'
        p = plan(question, ['The referenced account charge'], context)
        p['resolved_question'] = 'What is account Beta charge?'
        texts = {1: 'Account Alpha records a monthly charge of $100 USD.',
                 2: 'Account Beta records a monthly charge of $200 USD.'}
        pack = {'items': [item(k, v) for k, v in texts.items()]}
        orchestrator = StrandsQueryOrchestrator(); orchestrator.enabled = True
        payloads = []
        async def model(**kwargs):
            name = kwargs['name']; x = json.loads(kwargs['prompt']); payloads.append((name, x))
            if name == 'source_reader':
                self.assertNotIn('conversation_context', x)
                doc = x['source_documents'][0]
                return json.dumps({'documents': [{'document_id': doc['document_id'],
                    'observations': [{'text': texts[doc['document_id']],
                     'references': [{'span_id': doc['windows'][0]['span']['span_id']}]}], 'limitations': []}]})
            if name == 'source_auditor':
                rows = []
                for u in x['units']:
                    owner = next(k for k, value in texts.items() if u['text'] == '- ' + value)
                    doc = next(d for d in x['source_documents'] if d['document_id'] == owner)
                    rows.append(decision(unit_id=u['id'],
                        references=[{'span_id': doc['windows'][0]['span']['span_id']}]))
                return json.dumps({'assessments': rows})
            if name == 'answer_coverage':
                self.assertEqual(x['question'], question)
                return json.dumps({'requirements': [{'requirement_id': 'r1', 'status': 'answered',
                    'observation_ids': ['u1']}], 'omitted_requested_aspects': False})
            self.fail('Unexpected stage ' + name)
        with patch.object(orchestrator, '_text_agent', side_effect=model):
            final = await finalize_question(orchestrator, question, pack, p, 'strict')
        final.update(question=question, query_plan=p)
        self.assertTrue(final['finalization']['answer_verified'], final)
        for value in texts.values(): self.assertIn(value, final['answer'])
        self.assertTrue(restore_question_coverage(final)['complete'])
        self.assertFalse(any(n in {'fact_selector', 'fact_exclusion'} for n, _ in payloads))

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
            receipt = final['finalization']['fact_conservation']
            self.assertEqual(len(receipt['inventory']), 2)
            self.assertEqual(final['finalization']['reader_inventory_digest'], digest(receipt['inventory']))
