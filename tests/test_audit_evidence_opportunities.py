"""Calendar witnesses and distinct alternatives survive bounded audit packing."""
import json
import unittest
from app.answer_finalization import AnswerFinalizer


def item(doc, index, text, *, recent=False):
    text = (text + ' ' + 'Administrative detail. ' * 100)[:1750]
    return {'id': f'opportunity-{doc}-{index}', 'document_id': doc,
            'chunk_index': index, 'title': 'Cedar service record',
            'source_kind': 'ocr', 'source_content': text, 'content': text,
            'recent_reserved': recent}


class AuditEvidenceOpportunityTests(unittest.IsolatedAsyncioTestCase):
    async def test_new_status_record_reaches_auditor_ahead_of_old_future_term_continuations(self):
        old = 'Cedar service record dated January 1, 2025 lists an open account.'
        new = 'Cedar service record dated January 1, 2026 closes the account.'
        items = [item(1, 0, old)]
        for doc, year, end in ((2, 2020, 2035), (3, 2021, 2036)):
            items += [item(doc, n, f'Cedar service record dated January 1, {year} lists an open account. Scheduled renewal ends January 1, {end}.') for n in range(10)]
        items.append(item(4, 0, new, recent=True))
        observed = []
        class Auditor:
            async def audit_answer_units(self, question, units, spans, plan):
                observed.append(spans)
                old_span = next(s for s in spans if s['document_id'] == 1)
                return {'assessments': [{'unit_id': u['id'],
                    'status': 'conflicting' if any(new in s['content'] for s in spans) else 'supported',
                    'references': [{'span_id': old_span['span_id']}],
                    'temporal_scope': 'documented', 'temporal_assertion': 'retrieved_comparison',
                    'comparison_scope': 'retrieved_documents',
                    'comparison_document_ids': sorted({s['document_id'] for s in spans})} for u in units]}
        result = await AnswerFinalizer(Auditor()).finalize(
            'What is the latest documented Cedar service record?',
            'The latest retrieved Cedar service record dated January 1, 2025 lists an open account.',
            {'items': items})
        self.assertTrue(observed)
        self.assertTrue(all(any(new in s['content'] for s in spans) for spans in observed))
        self.assertTrue(all(len(json.dumps(spans, ensure_ascii=False)) <= 28000 for spans in observed))
        self.assertEqual(result['claim_ledger']['claims'][0]['status'], 'conflicting')
        self.assertFalse(result['finalization']['answer_verified'])

    async def test_equivalent_full_dates_survive_lexically_richer_short_year_tables(self):
        for domain, unit_name in (('invoice', 'USD'), ('measurement', 'mg/dL')):
            with self.subTest(domain=domain):
                items, claims = [], []
                for doc, (name, code, amount) in enumerate((('Cedar', 'CDREF', 20), ('Maple', 'MPREF', 30),
                                                           ('Birch', 'BCREF', 40), ('Pine', 'PNREF', 50)), 1):
                    items += [item(doc, 0, f'{name} {domain} {code}. Record date: 01/01/2026.'),
                              item(doc, 1, f'{name} {domain} {code} issued 01/01/26 records {amount} {unit_name}. Form edition 2026.')]
                    claims.append(f'{name} {domain} {code} dated January 1, 2026 records {amount} {unit_name}.')
                for doc in range(10, 40):
                    items.append(item(doc, 0, f'Cedar Maple Birch Pine {domain} records 20 {unit_name}, 30 {unit_name}, 40 {unit_name}, 50 {unit_name}. Form edition 2026.'))
                supplied = []
                class Auditor:
                    async def audit_answer_units(self, question, units, spans, plan):
                        supplied.append(spans)
                        assessments = []
                        for u in units:
                            doc = claims.index(u['text']) + 1
                            refs = [{'span_id': s['span_id']} for s in spans if s['document_id'] == doc]
                            assessments.append({'unit_id': u['id'], 'status': 'supported',
                                                'references': refs, 'temporal_scope': 'historical',
                                                'temporal_assertion': 'source_observation'})
                        return {'assessments': assessments}
                result = await AnswerFinalizer(Auditor()).finalize('What dated records are documented?',
                    '\n\n'.join(claims), {'items': items})
                self.assertTrue(result['finalization']['answer_verified'])
                self.assertEqual(result['claim_ledger']['summary']['supported'], 4)
                for doc in range(1, 5):
                    self.assertTrue(any(s['document_id'] == doc and '01/01/2026' in s['content'] for s in supplied[0]))
                self.assertLessEqual(len(json.dumps(supplied[0], ensure_ascii=False)), 28000)

    async def test_known_omissions_constrain_comparisons_but_not_source_observations(self):
        first = 'Cedar service record dated January 1, 2025 describes the account as current.'
        second = 'Cedar service record dated January 1, 2026 describes the account as closed.'
        items = [item(1, 0, first), item(2, 0, second, recent=True)]
        items += [item(d, 0, f'Cedar service record dated January 1, 2024 describes an account review. Record {d}.') for d in range(10, 45)]
        for kind, claim in (
            ('cohort', 'The latest Cedar service record dated January 1, 2025 describes the account as current.'),
            ('pairwise_omitted', 'The Cedar service record dated January 1, 2026 describes a closed account compared with the current account described in the Cedar service record dated January 1, 2025.'),
            ('pairwise_complete', 'The Cedar service record dated January 1, 2026 describes a closed account compared with the current account described in the Cedar service record dated January 1, 2025.'),
            ('observation', first)):
            with self.subTest(kind=kind):
                seen = []
                class Auditor:
                    async def audit_answer_units(self, question, units, spans, plan):
                        seen.append(plan['evidence_selection'])
                        refs = [{'span_id': s['span_id']} for s in spans if s['document_id'] in ({1, 2} if kind.startswith('pairwise') else {1})]
                        return {'assessments': [{'unit_id': u['id'], 'status': 'supported', 'references': refs,
                            'temporal_scope': 'historical' if kind == 'observation' else 'documented',
                            'temporal_assertion': 'source_observation' if kind == 'observation' else 'retrieved_comparison',
                            'comparison_scope': 'retrieved_documents', 'comparison_document_ids': [1, 2] if kind.startswith('pairwise') else [1]} for u in units]}
                result = await AnswerFinalizer(Auditor()).finalize('What do the dated records say?', claim,
                    {'items': items[:2] if kind == 'pairwise_complete' else items})
                self.assertEqual(any(o['omitted_document_ids'] for s in seen for o in s['comparison_opportunities']), kind != 'pairwise_complete')
                if kind in {'cohort', 'pairwise_omitted'}:
                    self.assertFalse(result['finalization']['answer_verified'])
                    self.assertIn('invalid_comparison_scope', result['claim_ledger']['claims'][0]['rejection_reasons'])
                else:
                    self.assertTrue(result['finalization']['answer_verified'])
                coverage = result['claim_ledger']['selection_coverage'][0]
                self.assertIn('date_opportunities', coverage)
                self.assertNotIn(first, json.dumps(coverage))

    async def test_calendar_opportunities_preserve_date_order_precision_and_identifier_exclusions(self):
        for raw, order, accepted in (
            ('2026-05-02', 'mdy', True), ('05/02/2026', 'mdy', True),
            ('02/05/2026', 'dmy', True), ('02/05/2026', 'mdy', False),
            ('02/05/2026', 'reject_ambiguous', False), ('05/02/26', 'mdy', False),
            ('May 2026', 'mdy', False), ('2026-02-30', 'mdy', False),
            ('identifier: 2026-05-02', 'mdy', False), ('05022026', 'mdy', False)):
            with self.subTest(raw=raw, order=order):
                source = f'Cedar measurement ABREF records 12 mg/dL. Record date: {raw}.'
                class Auditor:
                    async def audit_answer_units(self, question, units, spans, plan):
                        return {'assessments': [{'unit_id': u['id'], 'status': 'supported',
                            'references': [{'span_id': s['span_id']} for s in spans],
                            'temporal_scope': 'historical', 'temporal_assertion': 'source_observation'} for u in units]}
                result = await AnswerFinalizer(Auditor(), date_order=order).finalize(
                    'What measurement is recorded?', 'Cedar measurement ABREF records 12 mg/dL on May 2, 2026.',
                    {'items': [item(1, 0, source)]})
                self.assertEqual(result['finalization']['answer_verified'], accepted)
                if accepted:
                    ref = result['claim_ledger']['claims'][0]['references'][0]
                    self.assertEqual(ref['document_id'], 1)
                    self.assertEqual(ref['quote'], source + ' ' + ('Administrative detail. ' * 100)[:1750-len(source)-1])

    async def test_unrelated_selected_opening_does_not_cover_an_omitted_comparison_passage(self):
        def raw_item(doc, index, text):
            return {'id': f'passage-{doc}-{index}', 'document_id': doc, 'chunk_index': index,
                    'title': 'Record', 'source_kind': 'ocr', 'source_content': text, 'content': text}
        old = 'The Cedar service record dated January 1, 2025 lists an open account.'
        new = 'The Cedar service record dated January 1, 2026 lists a closed account.'
        padding = 'Administrative material. '
        keywords = 'shipping logistics tracking addresses contacts transport package weight distance office map diagram recipient telephone fax mail department floor room receipt delivery departure arrival signature destination origin customer counter agent carrier freight'
        unrelated = (keywords + ' ' + padding * 180)[:3600]
        items = [raw_item(1, 0, old + ' ' + padding * 130), raw_item(2, 0, unrelated),
                 raw_item(2, 1, new + ' ' + padding * 130)]
        items += [raw_item(doc, 0, unrelated) for doc in range(3, 7)]
        seen = []
        class Auditor:
            async def audit_answer_units(self, question, units, spans, plan):
                seen.append(spans)
                return {'assessments': [{'unit_id': u['id'], 'status': 'supported',
                    'references': [{'span_id': s['span_id']} for s in spans if s['document_id'] == 1],
                    'temporal_scope': 'documented', 'temporal_assertion': 'retrieved_comparison',
                    'comparison_scope': 'retrieved_documents', 'comparison_document_ids': [1]} for u in units]}
        question = 'Review ' + ', '.join(f'document {doc}' for doc in range(1, 7)) + '. ' + keywords + '.'
        result = await AnswerFinalizer(Auditor()).finalize(question,
            'The latest Cedar service record dated January 1, 2025 lists an open account.', {'items': items})
        self.assertTrue(any(s['document_id'] == 2 for s in seen[0]))
        self.assertFalse(any(new in s['content'] for s in seen[0]))
        self.assertFalse(result['finalization']['answer_verified'])
        opportunities = result['claim_ledger']['selection_coverage'][0]['comparison_opportunities']
        self.assertIn(2, opportunities[0]['omitted_document_ids'])

    async def test_presentation_whitespace_cannot_bypass_comparison_omissions(self):
        source = 'Cedar service record dated January 1, 2025 describes an open account.'
        items = [item(doc, 0, source + f' Record {doc}.') for doc in range(1, 36)]
        class Auditor:
            async def audit_answer_units(self, question, units, spans, plan):
                return {'assessments': [{'unit_id': u['id'], 'status': 'supported',
                    'references': [{'span_id': s['span_id']} for s in spans if s['document_id'] == 1],
                    'temporal_scope': 'documented', 'temporal_assertion': 'retrieved_comparison',
                    'comparison_scope': 'retrieved_documents', 'comparison_document_ids': [1]} for u in units]}
        for expression in ('most recent', 'most  recent', 'most\trecent', 'most\u00a0recent',
                           'most **recent**', '**most** recent', 'most *recent*'):
            with self.subTest(expression=expression):
                result = await AnswerFinalizer(Auditor()).finalize('What do the dated records say?',
                    f'The {expression} Cedar service record dated January 1, 2025 describes an open account.', {'items': items})
                self.assertFalse(result['finalization']['answer_verified'])
                self.assertIn('invalid_comparison_scope', result['claim_ledger']['claims'][0]['rejection_reasons'])

    async def test_unrecognized_comparison_wording_cannot_prove_closed_scope(self):
        source = 'Cedar service record dated January 1, 2025 describes the account as current.'
        items = [item(1, 0, source)] + [item(doc, 0,
            f'Cedar service record dated January 1, 2026 describes an account review. Record {doc}.') for doc in range(10, 45)]
        class Auditor:
            async def audit_answer_units(self, question, units, spans, plan):
                return {'assessments': [{'unit_id': u['id'], 'status': 'supported',
                    'references': [{'span_id': s['span_id']} for s in spans if s['document_id'] == 1],
                    'temporal_scope': 'documented', 'temporal_assertion': 'retrieved_comparison',
                    'comparison_scope': 'retrieved_documents', 'comparison_document_ids': [1]} for u in units]}
        for claim in (
            'The Cedar service record dated January 1, 2025 is newer than every other retrieved Cedar service record.',
            'The current Cedar service record dated January 1, 2025 describes the account as current compared with earlier Cedar service records.',
            'The Cedar service record dated January 1, 2025 supersedes every other Cedar service record.'):
            with self.subTest(claim=claim):
                result = await AnswerFinalizer(Auditor()).finalize('What do the dated records say?', claim, {'items': items})
                self.assertFalse(result['finalization']['answer_verified'])
                self.assertIn('invalid_comparison_scope', result['claim_ledger']['claims'][0]['rejection_reasons'])

    async def test_selected_identity_cannot_hide_an_omitted_status_continuation(self):
        old = 'Cedar service record for account SHAREDREF dated January 1, 2025 lists an open account.'
        closure = 'The account was closed on January 1, 2026.'
        items = [item(1, 0, old),
                 item(2, 0, 'Cedar service record for account SHAREDREF. Identity and contact record.', recent=True),
                 item(2, 1, 'Delivery administrative instructions for this document.', recent=True),
                 item(2, 2, closure, recent=True)]
        items += [item(doc, 0, f'Cedar service record for account SHAREDREF dated January 1, 2024 describes an account review. Record {doc}.') for doc in range(3, 12)]
        seen = []
        class Auditor:
            async def audit_answer_units(self, question, units, spans, plan):
                seen.append(spans)
                return {'assessments': [{'unit_id': u['id'],
                    'status': 'conflicting' if any(closure in s['content'] for s in spans) else 'supported',
                    'references': [{'span_id': s['span_id']} for s in spans if s['document_id'] == 1],
                    'temporal_scope': 'documented', 'temporal_assertion': 'retrieved_comparison',
                    'comparison_scope': 'retrieved_documents', 'comparison_document_ids': [1]} for u in units]}
        result = await AnswerFinalizer(Auditor()).finalize('What is the latest documented service status?',
            'The latest Cedar service record for account SHAREDREF dated January 1, 2025 lists an open account.', {'items': items})
        self.assertFalse(result['finalization']['answer_verified'])
        if not any(closure in s['content'] for s in seen[0]):
            opportunity = result['claim_ledger']['selection_coverage'][0]['comparison_opportunities'][0]
            self.assertIn(2, opportunity['omitted_document_ids'])
            self.assertIn(2, opportunity['partially_selected_document_ids'])

    async def test_short_and_unicode_identities_preserve_omission_accounting(self):
        class Auditor:
            async def audit_answer_units(self, question, units, spans, plan):
                return {'assessments': [{'unit_id': u['id'], 'status': 'supported',
                    'references': [{'span_id': s['span_id']} for s in spans if s['document_id'] == 1],
                    'temporal_scope': 'documented', 'temporal_assertion': 'retrieved_comparison',
                    'comparison_scope': 'retrieved_documents', 'comparison_document_ids': [1]} for u in units]}
        for identity, alternative_identity in (('AB', 'AB'), ('A2', 'A2'), ('2A', '2A'), ('測定', '測定'), ('', ''),
                                               ('José', 'Jose\u0301'), ('ＡＢ', 'AB')):
            for scenario in ('other_documents', 'own_continuation', 'explicit_reservation'):
                with self.subTest(identity=identity, scenario=scenario):
                    old = f'{identity} Record date: 01/01/2025. Charge: 500 USD.'
                    new = f'{alternative_identity} Record date: 01/01/2026. Charge: 600 USD.'
                    if scenario == 'other_documents':
                        items = [item(1, 0, old)] + [item(doc, 0, new + f' Reference {doc}.') for doc in range(2, 37)]
                    else:
                        items = [item(1, index, old) for index in range(18)] + [item(1, 18, new)]
                    question = f'Which {identity} record is latest?' if scenario != 'explicit_reservation' else 'What is latest in document 1?'
                    result = await AnswerFinalizer(Auditor()).finalize(question,
                        f'The latest {identity} record is dated January 1, 2025 and records 500 USD.', {'items': items})
                    self.assertFalse(result['finalization']['answer_verified'])
                    opportunities = result['claim_ledger']['selection_coverage'][0]['comparison_opportunities']
                    self.assertTrue(opportunities[0]['omitted_document_ids'])
                    if scenario != 'other_documents':
                        self.assertIn(1, opportunities[0]['omitted_document_ids'])

    async def test_digit_leading_identity_retains_opposite_state_alternatives(self):
        class Auditor:
            async def audit_answer_units(self, question, units, spans, plan):
                return {'assessments': [{'unit_id': u['id'], 'status': 'supported',
                    'references': [{'span_id': s['span_id']} for s in spans if s['document_id'] == 1],
                    'temporal_scope': 'documented', 'temporal_assertion': 'retrieved_comparison',
                    'comparison_scope': 'retrieved_documents', 'comparison_document_ids': [1]} for u in units]}
        for identity in ('A2', '2A'):
            with self.subTest(identity=identity):
                items = [item(1, 0, f'{identity} record dated January 1, 2025 is open.')]
                items += [item(doc, 0, f'{identity} record dated January 1, 2026 is closed. Reference {doc}.') for doc in range(2, 37)]
                result = await AnswerFinalizer(Auditor()).finalize('What is the latest record?',
                    f'The latest {identity} record dated January 1, 2025 is open.', {'items': items})
                self.assertFalse(result['finalization']['answer_verified'])
                opportunity = result['claim_ledger']['selection_coverage'][0]['comparison_opportunities'][0]
                self.assertEqual(len(opportunity['eligible_document_ids']), 36)
                self.assertTrue(opportunity['omitted_document_ids'])

    async def test_identical_openings_keep_their_own_distinct_continuation_context(self):
        opening = 'Cedar service record for account SHAREDREF.'
        old = 'The account was reviewed on January 1, 2025.'
        new = 'The account was closed on January 1, 2026.'
        items = [item(1, 0, opening), item(1, 1, old),
                 item(2, 0, opening, recent=True), item(2, 1, new, recent=True)]
        items += [item(d, 0, opening) for d in range(10, 45)]
        seen = []
        class Auditor:
            async def audit_answer_units(self, question, units, spans, plan):
                seen.append(spans)
                own = [s for s in spans if s['document_id'] == 2]
                return {'assessments': [{'unit_id': u['id'], 'status': 'conflicting',
                    'references': [{'span_id': s['span_id']} for s in own],
                    'temporal_scope': 'documented', 'temporal_assertion': 'retrieved_comparison',
                    'comparison_scope': 'retrieved_documents', 'comparison_document_ids': [2]} for u in units]}
        result = await AnswerFinalizer(Auditor()).finalize('What is the latest documented Cedar service status?',
            'The latest Cedar service record for account SHAREDREF describes a review on January 1, 2025.', {'items': items})
        self.assertTrue(all(any(s['document_id'] == 2 and opening in s['content'] for s in spans)
                            and any(s['document_id'] == 2 and new in s['content'] for s in spans) for spans in seen))
        self.assertFalse(result['finalization']['answer_verified'])
        self.assertTrue(all(ref['document_id'] == 2 for ref in result['claim_ledger']['claims'][0]['references']))
