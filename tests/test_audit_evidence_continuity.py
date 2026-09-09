"""Subset audits retain evidence opportunities, never earlier verdicts."""
import copy
import hashlib
import json
import unittest

from tests.runtime import configure_test_environment
configure_test_environment()
from app.answer_finalization import (AnswerFinalizer, EvidenceReservationError, evidence_spans, select_spans,
                                     subset_source_reservations)
from app.answer_observations import ObservationCandidate
from tests.test_observation_delivery import ObservationRepairer


def continuity_fixture(kind='invoice'):
    names = ['Aster', 'Birch', 'Cedar', 'Dogwood', 'Elm', 'Fir', 'Ginkgo', 'Holly']
    facts = [f'{name} {kind} records earlier service review for account {100+i} and charge ${200+i}.'
             for i, name in enumerate(names)]
    last = f'Orion {kind} records account 819, a service term beginning January 1, 2024, and charge $953.'
    tails = [f'Orion {kind} account 819.', f'Orion {kind} service term beginning January 1, 2024.',
             f'Orion {kind} charge $953.']
    items = []
    for i, fact in enumerate(facts):
        items.append({'id': f'fact-{i}', 'document_id': i+1, 'chunk_index': 0,
                      'title': f'{names[i]} {kind}', 'content': fact + ' ' +
                      'Document conditions apply. ' * ((3000-len(fact))//27)})
    for i, tail in enumerate(tails):
        items.append({'id': f'target-{i}', 'document_id': 99, 'chunk_index': i,
                      'title': f'Orion {kind}', 'content': tail + ' ' +
                      'Standard contract conditions apply. ' * ((3000-len(tail))//36)})
    for i in range(8):
        text = f'{names[i]} {kind} account charge service term beginning January 1, 2024. Conditions summary. '
        items.insert(0, {'id': f'noise-{i}', 'document_id': 200+i, 'chunk_index': 0,
                        'title': 'Service terms', 'content': text +
                        'Unrelated agreement description. ' * ((3000-len(text))//33)})
    return {'items': items}, facts + [last], tails


class ContinuityAuditor:
    def __init__(self, facts, tails, conflict=False):
        self.facts, self.tails, self.conflict = facts, tails, conflict
        self.calls, self.last_seen = [], 0

    async def audit_answer_units(self, question, units, spans, plan):
        self.calls.append((copy.deepcopy(units), copy.deepcopy(spans), copy.deepcopy(plan)))
        assessments = []
        for unit in units:
            text = unit['text'].removeprefix('- ')
            needed = self.tails if text == self.facts[-1] else [text]
            found = [next((s for s in spans if need in s['content']), None) for need in needed]
            status = 'supported' if all(found) else 'missing'
            if text in (self.facts[0], 'Original draft REJECT.'):
                status = 'unsupported'
            if text == self.facts[-1]:
                self.last_seen += 1
                if self.conflict and self.last_seen == 2:
                    status = 'conflicting'
            assessments.append({'unit_id': unit['id'], 'status': status,
                                'temporal_scope': 'historical', 'temporal_assertion': 'source_observation',
                                'references': [{'span_id': s['span_id']} for s in found if s]})
        return {'assessments': assessments}


class AuditEvidenceContinuityTests(unittest.IsolatedAsyncioTestCase):
    def test_duplicate_observations_keep_ordered_reference_identity(self):
        original = ObservationCandidate(('Cedar recorded $20.', 'Maple recorded $30.', 'Cedar recorded $20.'))
        claims = [{**unit, 'claim':unit['text'], 'status':'supported', 'references':[{'span_id':f'source-{i}'}]}
                  for i, unit in enumerate(original.units())]
        ledger = {'candidate_digest':hashlib.sha256(original.text.encode()).hexdigest(),
                  'unitization':'observations_v1', 'claims':claims}
        revised = ObservationCandidate((original.observations[0], original.observations[2]))
        result = subset_source_reservations(original.text, ledger, revised.units(), ['u1','u3'])
        self.assertEqual(result, {'u1':[{'span_id':'source-0'}], 'u2':[{'span_id':'source-2'}]})
        for alter in (lambda l:l.update(candidate_digest='0'*64),
                      lambda l:l['claims'][2].update(start=0),
                      lambda l:l['claims'][2].update(status='unsupported')):
            broken = copy.deepcopy(ledger)
            alter(broken)
            with self.assertRaises(EvidenceReservationError):
                subset_source_reservations(original.text, broken, revised.units(), ['u1','u3'])
        with self.assertRaises(EvidenceReservationError):
            subset_source_reservations(original.text, ledger, revised.units(), ['u3','u1'])

    def test_required_windows_fail_explicitly_when_missing_flagged_or_over_budget(self):
        evidence, facts, _ = continuity_fixture()
        spans = evidence_spans(evidence, citation_safe=True)
        required = tuple(s['span_id'] for s in spans if s['document_id']==99)
        cost = sum(len(json.dumps(s, ensure_ascii=False))+2 for s in spans if s['span_id'] in required)
        units = [{'id':'u1','text':facts[-1]}]
        diagnostics = {}
        selected = select_spans('Invoice history?', units, spans, budget=cost, serialized=True,
                                required_span_ids=required+required, diagnostics=diagnostics)
        self.assertEqual({s['span_id'] for s in selected}, set(required))
        self.assertEqual(diagnostics['source_reservations'], {'required_windows':3,'selected_windows':3})
        for available, ids, budget, reason in (
            (spans, required, cost-1, 'reserved_sources_exceed_budget'),
            (spans, ('unknown',), 28000, 'invalid_reserved_source'),
            ([{**s,'feedback_open':s['document_id']==99} for s in spans], required, 28000, 'invalid_reserved_source'),
        ):
            with self.assertRaises(EvidenceReservationError) as raised:
                select_spans('Invoice history?', units, available, budget=budget, serialized=True, required_span_ids=ids)
            self.assertEqual(raised.exception.reason, reason)

    async def test_regrouped_invoice_and_measurement_observations_keep_disjoint_sources(self):
        for kind in ('invoice', 'measurement'):
            evidence, facts, tails = continuity_fixture(kind)
            auditor = ContinuityAuditor(facts, tails)
            result = await AnswerFinalizer(auditor, ObservationRepairer({'observations': facts})).finalize(
                f'What is the {kind} history?', 'Original draft REJECT.', evidence)
            self.assertEqual(result['finalization']['disposition'], 'partial', kind)
            self.assertIn(facts[-1], result['answer'])
            self.assertNotIn(facts[0], result['answer'])
            last_calls = [call for call in auditor.calls if any(facts[-1] in u['text'] for u in call[0])]
            self.assertEqual([len(call[0]) for call in last_calls], [1, 4])
            for _, spans, plan in last_calls:
                self.assertTrue(all(any(tail in s['content'] for s in spans) for tail in tails))
                self.assertLessEqual(sum(len(json.dumps(s, ensure_ascii=False))+2 for s in spans), 28000)
                self.assertNotIn('supported', json.dumps(plan))
            self.assertEqual(auditor.last_seen, 2)

    async def test_new_conflict_cannot_inherit_the_previous_supported_verdict(self):
        evidence, facts, tails = continuity_fixture()
        result = await AnswerFinalizer(ContinuityAuditor(facts, tails, conflict=True),
            ObservationRepairer({'observations': facts})).finalize(
                'What is the invoice history?', 'Original draft REJECT.', evidence)
        self.assertFalse(result['finalization']['answer_verified'])
        self.assertEqual(result['claim_ledger']['subset_audit']['claims'][-1]['status'], 'conflicting')

    async def test_changed_or_revoked_source_stops_subset_before_any_fresh_calls(self):
        for change in ('content','feedback_open'):
            evidence, facts, tails = continuity_fixture()
            class MutatingAuditor(ContinuityAuditor):
                async def audit_answer_units(self, *args):
                    response = await super().audit_answer_units(*args)
                    if self.last_seen == 1:
                        item = next(i for i in evidence['items'] if i['id']=='target-0')
                        item[change] = 'Different original source.' if change=='content' else True
                    return response
            auditor = MutatingAuditor(facts, tails)
            result = await AnswerFinalizer(auditor, ObservationRepairer({'observations':facts})).finalize(
                'What is the invoice history?', 'Original draft REJECT.', evidence)
            self.assertFalse(result['finalization']['answer_verified'])
            self.assertEqual(result['claim_ledger']['subset_evidence_diagnostic']['reason'], 'invalid_reserved_source')
            self.assertEqual(auditor.last_seen, 1)
