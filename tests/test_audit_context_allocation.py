"""Audit context serves every claim and retains opportunities to contradict it."""
import json
import unittest
from app.answer_finalization import AnswerFinalizer, evidence_spans, select_spans, span_coverage


def item(doc_id, text, *, reserved=False, chunk=0, title='Service invoice'):
    return {'id': f'source-{doc_id}-{chunk}', 'document_id': doc_id, 'chunk_index': chunk,
            'title': title, 'source_kind': 'ocr', 'history_reserved': reserved,
            'content': text + ' Filing context.' * 120}


def reference(span, quote):
    return {**{k: span[k] for k in ('span_id', 'evidence_id', 'document_id')}, 'quote': quote}


class AuditContextAllocationTests(unittest.IsolatedAsyncioTestCase):
    async def test_broad_first_unit_cannot_starve_narrow_later_units(self):
        facts = {901: 'Service invoice ALPHAX742 records $321 USD.',
                 902: 'Service invoice BETAY813 records $421 USD.',
                 903: 'Service invoice GAMMAZ926 records $521 USD.'}
        earlier = [f'Service invoice ARCHIVE{i} records a charge.' for i in range(8)]
        broad = 'The service invoice history includes ' + ', '.join(f'ARCHIVE{i} archivedetail{i}' for i in range(8)) + '.'
        items = [item(i+1, fact, reserved=True) for i, fact in enumerate(earlier)]
        items += [item(i+1, f'Archived service archivedetail{i}.', reserved=True, chunk=3) for i in range(8)]
        items += [item(100+i, 'Service invoice records charges $321 USD, $421 USD, $521 USD.') for i in range(30)]
        items += [item(doc, fact) for doc, fact in facts.items()]
        calls = []
        class Auditor:
            async def audit_answer_units(self, question, units, spans, plan):
                calls.append(spans)
                assessments = []
                for unit in units:
                    refs = [reference(s, fact) for doc, fact in facts.items() for s in spans
                            if unit['text'] == fact and s['document_id'] == doc and fact in s['content']]
                    assessments.append({'unit_id': unit['id'], 'status': 'supported' if refs else 'unsupported',
                                        'temporal_scope': 'historical', 'references': refs})
                return {'assessments': assessments}
        result = await AnswerFinalizer(Auditor()).finalize('How did my service charges change?',
                '\n'.join([broad, *facts.values()]), {'items': items})
        first = calls[0]
        self.assertTrue(set(facts).issubset({s['document_id'] for s in first}))
        self.assertLessEqual(len(json.dumps(first, ensure_ascii=False)), 28000)
        for fact in facts.values():
            self.assertIn(fact, result['answer'])
        self.assertNotIn(broad, result['answer'])
        coverage = result['claim_ledger']['selection_coverage'][0]
        self.assertEqual(coverage['reserved_document_ids'], [])

    async def test_latest_claim_auditor_sees_newer_unasserted_same_subject_record(self):
        old = 'Orchid service invoice OLDX742 dated January 1, 2020 records $321 USD.'
        new = 'Orchid service invoice NEWY813 dated September 1, 2026 records $421 USD.'
        claim = 'The latest Orchid service invoice is OLDX742 dated January 1, 2020 for $321 USD.'
        for variation in ('ordinary', 'short_title', 'large_amounts', 'dmy', 'later_footer', 'durations'):
            actual_new = new if variation != 'dmy' else new.replace('September 1, 2026', '02/05/2026')
            items = [item(1, old, reserved=True, title='Orchid service invoice 2020')]
            items += [item(i+2, f'Historical ARCHIVE{i} service charges.', reserved=True) for i in range(7)]
            items += [item(100+i, 'The latest Orchid service invoice is OLDX742 dated January 1, 2020 for $321 USD.',
                           title='Orchid service invoice 2020' if variation == 'short_title' else 'Administrative terms') for i in range(35)]
            items += [item(999, actual_new, title='Service invoice 2026' if variation == 'short_title' else 'Orchid service invoice 2026')]
            if variation == 'large_amounts':
                items += [item(888, 'Orchid service invoice dated January 1, 2021 records $5000 USD.', title='Orchid service invoice 2021'),
                          item(889, 'Orchid service invoice dated January 1, 2022 records $6000 USD.', title='Orchid service invoice 2022')]
            if variation == 'durations':
                items += [item(888, 'Orchid service invoice dated January 1, 2021. Billing period: 5000 hours.', title='Orchid service invoice 2021'),
                          item(889, 'Orchid service invoice dated January 1, 2022. Sampling period: 6000 ms.', title='Orchid service invoice 2022')]
            if variation == 'later_footer':
                items.append(item(999, 'Administrative printing record: October 1, 2026.', chunk=3, title='Orchid service invoice 2026'))
            if variation == 'dmy':
                items += [item(888, 'Orchid service invoice dated 03/02/2026 records $400 USD.', title='Orchid service invoice 2026'),
                          item(889, 'Orchid service invoice dated 04/02/2026 records $450 USD.', title='Orchid service invoice 2026')]
            seen = []
            class Auditor:
                async def audit_answer_units(self, question, units, spans, plan):
                    has_new = any(actual_new in s['content'] for s in spans)
                    seen.append(has_new)
                    old_span = next(s for s in spans if s['document_id'] == 1)
                    return {'assessments': [{'unit_id': u['id'], 'status': 'conflicting' if has_new else 'supported',
                        'temporal_scope': 'documented', 'temporal_assertion': 'retrieved_comparison',
                        'comparison_scope': 'retrieved_documents', 'comparison_document_ids': [s['document_id'] for s in spans], 'references': [reference(old_span, old)]} for u in units]}
            result = await AnswerFinalizer(Auditor(), date_order='dmy' if variation == 'dmy' else 'mdy').finalize('What is the latest Orchid service invoice?', claim, {'items': items})
            self.assertTrue(seen, variation)
            self.assertTrue(all(seen), variation)
            self.assertTrue(result['claim_ledger']['complete'], variation)
            self.assertEqual(result['claim_ledger']['claims'][0]['status'], 'conflicting', variation)
            self.assertFalse(result['finalization']['answer_verified'])
            self.assertNotIn(claim, result['answer'])

    def test_history_priority_is_stage_specific_and_explicit_requests_remain(self):
        question = 'Compare document 901 with historical service invoices.'
        spans = evidence_spans({'items': [item(i, 'Invoice history.', reserved=True) for i in range(1, 9)]
                                        + [item(901, 'Invoice CHOSEN742.') ]})
        audit = select_spans(question, [{'text': 'Invoice CHOSEN742.'}], spans, serialized=True)
        self.assertEqual(audit[0]['document_id'], 901)
        coverage = span_coverage(question, spans, audit, reserve_history=False)
        self.assertEqual(coverage['requested_document_ids'], [901])
        self.assertEqual(coverage['reserved_document_ids'], [])
        self.assertFalse(coverage['limited'])
        synthesis = select_spans(question, [], spans, serialized=True)
        self.assertTrue({*range(1, 9), 901}.issubset({s['document_id'] for s in synthesis}))
