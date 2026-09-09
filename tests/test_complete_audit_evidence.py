"""Audits receive every eligible canonical source opportunity without truncation."""
import copy
import json
import unittest

from tests.runtime import configure_test_environment
configure_test_environment()
from app.answer_finalization import AnswerFinalizer, evidence_spans
from tests.test_observation_delivery import ObservationRepairer


def source_group_fixture(kind='service'):
    records = [
        f'Acme {kind} record ZX742 has a term from March 9, 2026 to March 9, 2027.',
        f'On April 27, 2026, Acme recorded a {kind} adjustment after a report could not be obtained.',
        f'Riley signed an ORBIT {kind} cancellation request on September 1, 2026 for record ZX742.',
        f'Northwind issued {kind} record CD813 to Riley and Morgan for the term September 1, 2026 to March 1, 2027 covering three devices.',
    ]
    raw = [
        (10, 0, f'Acme {kind} record ZX742. Term: 03/09/2026 to 03/09/2027.'),
        (11, 0, f'Acme {kind} adjustment recorded 04/27/2026 after a report could not be obtained.'),
        (30, 0, f'ORBIT {kind} cancellation request. Riley. Record ZX742. Request date: 09/01/2026.'),
        (30, 1, f'ORBIT {kind} cancellation request for record ZX742. Signature: Riley. Signed: 09/01/2026.'),
        (40, 0, f'Northwind {kind} record CD813 issued to Riley and Morgan. Term: 09/01/2026 to 03/01/2027.'),
        (40, 1, f'Northwind {kind} record CD813 for Riley and Morgan. DEVICES COVERED\n| Unit | Name |\n| --- | --- |\n| 1 | Elm |\n| 2 | Fir |\n| 3 | Oak |'),
        (40, 2, f'Northwind {kind} record CD813 for Riley and Morgan. Summary: three devices. Term information follows.'),
    ]
    # The sole written month and unrelated verbs beat complete numeric-date
    # records under max-single-token document ranking. Long records then offer
    # several complementary but irrelevant windows before the real sources.
    for doc, words in ((50, 'September calendar reference'),
                       (51, 'signed cancel cancellation'),
                       (52, 'covering issued')):
        for chunk in range(4):
            raw.append((doc, chunk, f'{words if chunk == 0 else "Administrative annex"}. Acme Northwind {kind} Riley Morgan record ZX742 CD813. ORBIT request. Term March April 2026 2027.'))
    for doc in range(60, 80):
        raw.append((doc, 0, f'Acme Northwind {kind} Riley Morgan ORBIT record ZX742 CD813 request cancellation signed issued devices term.'))
    items = []
    for doc, chunk, text in raw:
        items.append({'id':f'coherent-{doc}-{chunk}', 'document_id':doc, 'chunk_index':chunk,
                      'title':f'{kind} record', 'source_kind':'ocr', 'history_reserved':True,
                      'content':text + ' ' + 'Administrative filing context. ' * ((1750-len(text))//31)})
    required = [{(10,0)}, {(11,0)}, {(30,0),(30,1)}, {(40,0),(40,1),(40,2)}]
    return {'items':items}, records, required


class GroupAuditor:
    def __init__(self, records, required):
        self.records, self.required, self.calls = records, required, []

    async def audit_answer_units(self, question, units, spans, plan):
        self.calls.append((copy.deepcopy(units),copy.deepcopy(spans),copy.deepcopy(plan)))
        assessments=[]
        for unit in units:
            text=unit['text'].removeprefix('- ')
            needed=self.required[self.records.index(text)] if text in self.records else {(-1,0)}
            found=[next((s for s in spans if (s['document_id'],s['chunk_index'])==key),None) for key in sorted(needed)]
            assessments.append({'unit_id':unit['id'], 'status':'supported' if all(found) else 'missing',
                'references':[{'span_id':s['span_id']} for s in found if s],
                'temporal_scope':'historical','temporal_assertion':'source_observation'})
        return {'assessments':assessments}


class CompleteAuditEvidenceTests(unittest.IsolatedAsyncioTestCase):
    async def test_mixed_batch_keeps_identity_signature_and_actual_table(self):
        for kind in ('service', 'measurement'):
            with self.subTest(kind=kind):
                pack,records,required=source_group_fixture(kind)
                auditor=GroupAuditor(records,required)
                result=await AnswerFinalizer(auditor,ObservationRepairer({'observations':records})).finalize(
                    f'How have my {kind} records changed and what is most recent?', 'Original draft REJECT.',pack)
                calls=[call for call in auditor.calls if len(call[0])==4]
                self.assertTrue(calls)
                supplied={(s['document_id'],s['chunk_index']) for s in calls[0][1]}
                self.assertTrue(set.union(*required)<=supplied, (kind,supplied))
                self.assertTrue(result['finalization']['answer_verified'])
                self.assertEqual(result['claim_ledger']['summary']['supported'],4)
                self.assertIn(records[-1],result['answer'])
                expected={s["span_id"] for s in evidence_spans(pack,citation_safe=True)}
                self.assertGreater(len(json.dumps(evidence_spans(pack,citation_safe=True),ensure_ascii=False)),28000)
                for _,spans,_ in calls:
                    self.assertEqual({s["span_id"] for s in spans},expected)

    async def test_one_observation_keeps_two_documents_behind_repeated_windows(self):
        for kind in ('service', 'measurement'):
            for with_peers in (False, True):
                with self.subTest(kind=kind, with_peers=with_peers):
                    first = f'Orion {kind} record AB742 names Casey.'
                    second = f'Orion {kind} record AB742 has a term beginning January 1, 2026.'
                    combined = f'Orion {kind} record AB742 names Casey and has a term beginning January 1, 2026.'
                    items = [{'id':f'repeated-{i}', 'document_id':1, 'chunk_index':i,
                              'content':first + ' Administrative filing context.' * 110}
                             for i in range(18)]
                    items.append({'id':'second-record', 'document_id':2, 'chunk_index':0, 'content':second})
                    peers = [f'{name} {kind} record identifies a separate subject.' for name in ('Cedar','Maple','Birch')]
                    items.extend({'id':f'peer-{i}', 'document_id':i+3, 'chunk_index':0, 'content':fact}
                                 for i,fact in enumerate(peers))
                    records = peers + [combined] if with_peers else [combined]
                    required = [{(i+3,0)} for i in range(3)] + [{(1,0),(2,0)}] if with_peers else [{(1,0),(2,0)}]
                    auditor = GroupAuditor(records, required)
                    pack = {'items':items}
                    result = await AnswerFinalizer(auditor, ObservationRepairer({'observations':records})).finalize(
                        'What do the records establish?', 'Original draft REJECT.', pack)
                    self.assertTrue(result['finalization']['answer_verified'])
                    self.assertIn(combined, result['answer'])
                    self.assertEqual(len(auditor.calls), 2)
                    for _, supplied, _ in auditor.calls:
                        self.assertEqual(supplied, evidence_spans(pack,citation_safe=True))

    async def test_feedback_and_uncertified_sources_never_reach_auditor_or_editor(self):
        from app.source_text import bind_document_context
        pack, records, required = source_group_fixture()
        bound = pack['items'][0]
        bind_document_context(bound, bound['content'] + '\n\n' + 'Spacer. ' * 60 + 'PRIVATE_FULL_DOCUMENT_ONLY')
        pack['items'].extend([
            {'id':'blocked','document_id':501,'chunk_index':0,'content':'FEEDBACK_BLOCKED','feedback_open':True},
            {'id':'generated','document_id':502,'chunk_index':9999,'content':'GENERATED_SUMMARY'},
            {'id':'invalid','document_id':503,'chunk_index':0,'content':'INVALID_CERTIFICATE',
             '_source_document_content':'Not the claimed source.', 'source_context':{}},
        ])
        class Repairer(ObservationRepairer):
            async def repair_answer(self, question, answer, context, verification):
                self.context = json.loads(context)
                return await super().repair_answer(question, answer, context, verification)
        repairer = Repairer({'observations':records})
        auditor = GroupAuditor(records,required)
        result = await AnswerFinalizer(auditor,repairer).finalize('What is recorded?', 'Original draft REJECT.', pack)
        expected = [s for s in evidence_spans(pack,citation_safe=True) if not s['feedback_open']]
        self.assertEqual(repairer.context, expected)
        for _, supplied, plan in auditor.calls:
            self.assertEqual(supplied,expected)
            coverage = plan['evidence_selection']
            self.assertEqual(coverage['excluded_windows'],1)
            self.assertEqual(coverage['excluded_documents'],1)
            self.assertEqual(coverage['eligible_windows'],len(expected))
            self.assertEqual(coverage['supplied_windows'],len(expected))
            self.assertEqual(coverage['serialized_bytes'],len(json.dumps(expected,ensure_ascii=False).encode()))
        text = json.dumps([auditor.calls,repairer.context,result])
        for secret in ('PRIVATE_FULL_DOCUMENT_ONLY','FEEDBACK_BLOCKED','GENERATED_SUMMARY',
                       'INVALID_CERTIFICATE','_source_document_content'):
            self.assertNotIn(secret,text)
        self.assertEqual(result['claim_ledger']['source_diagnostics']['unbound_source_chunks'],1)

    async def test_unavailable_or_incomplete_audit_cannot_be_repaired_to_success(self):
        for response in (None, {'assessments':[]}):
            class Auditor:
                async def audit_answer_units(self, *args):
                    return response
            repairer = ObservationRepairer({'observations':['Cedar records $20.']})
            result = await AnswerFinalizer(Auditor(),repairer).finalize('What is recorded?',
                'Cedar records $20.', {'items':[{'id':'source','document_id':1,'content':'Cedar records $20.'}]})
            self.assertEqual(repairer.calls,0)
            self.assertFalse(result['finalization']['answer_verified'])
            self.assertEqual(result['finalization']['disposition'],'incomplete')
            self.assertIn('source audit did not complete', result['verification']['missing_evidence'][0])

    async def test_timeout_preserves_admission_and_distinguishes_pending_batches(self):
        import asyncio
        fact = 'Cedar records $20.'
        class Auditor:
            async def audit_answer_units(self, *args):
                await asyncio.sleep(1)
        pack = {'items':[{'id':'source','document_id':1,'content':fact},
                         {'id':'blocked','document_id':2,'content':fact,'feedback_open':True}]}
        result = await AnswerFinalizer(Auditor(),timeout_seconds=.01,concurrency=1).finalize(
            'What is recorded?','\n'.join([fact]*9),pack)
        self.assertEqual(result['finalization']['disposition'],'timeout')
        ledger = result['claim_ledger']
        self.assertEqual(ledger['available_span_count'],1)
        self.assertEqual([row['dispatched'] for row in ledger['selection_coverage']],[True,False,False])
        self.assertEqual([row['supplied_windows'] for row in ledger['selection_coverage']],[1,0,0])
        self.assertEqual([row['supplied_documents'] for row in ledger['selection_coverage']],[1,0,0])
        self.assertEqual([row['status'] for row in ledger['audit_batches']],['cancelled','pending','pending'])
        for row in ledger['selection_coverage']:
            self.assertEqual(row['eligible_windows'],1)
            self.assertEqual(row['excluded_windows'],1)
        self.assertEqual(len(ledger['spans']),1)
        self.assertNotIn('content',ledger['spans'][0])
        self.assertNotIn(fact,result['answer'])

    async def test_subset_timeout_preserves_its_own_source_admission(self):
        import asyncio
        from tests.test_audit_evidence_continuity import continuity_fixture, ContinuityAuditor
        pack,facts,tails = continuity_fixture()
        class Auditor(ContinuityAuditor):
            async def audit_answer_units(self,*args):
                if self.last_seen:
                    await asyncio.sleep(1)
                return await super().audit_answer_units(*args)
        result = await AnswerFinalizer(Auditor(facts,tails),ObservationRepairer({'observations':facts}),
            timeout_seconds=.1,concurrency=1).finalize('What is recorded?','Original draft REJECT.',pack)
        self.assertEqual(result['finalization']['disposition'],'timeout')
        audit=result['claim_ledger']['subset_audit']
        self.assertEqual(audit['available_span_count'],len(evidence_spans(pack,citation_safe=True)))
        self.assertTrue(audit['selection_coverage'][0]['dispatched'])
        self.assertFalse(audit['selection_coverage'][1]['dispatched'])
        self.assertTrue(all('content' not in span for span in audit['spans']))
        self.assertFalse(result['finalization']['answer_verified'])
