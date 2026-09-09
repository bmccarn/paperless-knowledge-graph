"""Partial delivery preserves record associations, not just exact source values."""
import unittest

from markdown_it import MarkdownIt

from app.answer_finalization import AnswerFinalizer, answer_units
from app.answer_structure import supported_revision
from tests.test_source_dates import pack


class RecordingAuditor:
    def __init__(self, reject='REJECT', conflict=False):
        self.calls = []
        self.reject = reject
        self.conflict = conflict

    async def audit_answer_units(self, question, units, spans, plan):
        self.calls.append((units, plan['answer_context']))
        span = spans[0]
        return {'assessments': [{
            'unit_id': u['id'],
            'status': ('conflicting' if self.conflict else 'unsupported') if self.reject in u['text'] else 'supported',
            'temporal_scope': 'historical',
            'references': [{**{k: span[k] for k in ('span_id', 'evidence_id', 'document_id')},
                            'quote': span['content']}],
        } for u in units]}


class SubjectContextTests(unittest.IsolatedAsyncioTestCase):
    async def finalize(self, answer, **kwargs):
        auditor = RecordingAuditor(**kwargs)
        result = await AnswerFinalizer(auditor).finalize('Compare the documented records.', answer, pack(answer))
        return result, auditor

    async def test_rejected_peer_parent_cannot_reassign_children_to_surviving_record(self):
        for bad_first in (True, False):
            for indent in ('  ', '    '):
                good = f'- Invoice Cedar records delivery.\n{indent}- Amount: $20.\n{indent}- Recipient: Casey.'
                bad = f'- Invoice Maple REJECT records delivery.\n{indent}- Amount: $30.\n{indent}- Recipient: Morgan.'
                answer = '\n\n'.join((bad, good) if bad_first else (good, bad))
                result, auditor = await self.finalize(answer)
                self.assertEqual(result['finalization']['disposition'], 'partial')
                self.assertNotIn('Maple', result['answer'])
                self.assertNotIn('$30', result['answer'])
                self.assertNotIn('Morgan', result['answer'])
                self.assertIn('$20', result['answer'])
                self.assertIn('Casey', result['answer'])
                self.assertEqual(auditor.calls[-1][1].strip(), good)
                parsed = MarkdownIt().parse(auditor.calls[-1][1])
                self.assertEqual([t.level for t in parsed if t.type == 'list_item_open'], [1, 3, 3])
                omitted = result['verification']['partial']['omitted_units']
                self.assertEqual(len(omitted), 3)
                self.assertEqual([c['omission_reason'] for c in omitted],
                                 ['source_rejection', 'dependency_omitted', 'dependency_omitted'])
                self.assertTrue(all(c['claim'] for c in omitted))

    async def test_all_parent_assertions_govern_nested_children(self):
        answer = ('- Invoice Cedar records delivery. Its recipient is REJECT.\n'
                  '  - Package recorded.\n    - Weight: 20 kg.\n\n'
                  '- Invoice Maple records a separate delivery of 30 kg.')
        result, auditor = await self.finalize(answer)
        self.assertEqual(result['finalization']['disposition'], 'partial')
        self.assertNotIn('20 kg', result['answer'])
        self.assertNotIn('Package', result['answer'])
        self.assertIn('30 kg', result['answer'])
        self.assertIn('Invoice Cedar records delivery.', result['answer'])
        self.assertEqual(auditor.calls[-1][1],
                         '- Invoice Cedar records delivery. \n\n- Invoice Maple records a separate delivery of 30 kg.')
        self.assertIn(' \n  \n    \n\n- Invoice Maple', result['answer'])

    async def test_explicit_section_boundaries_preserve_supported_independent_records(self):
        for bad_heading, good_heading in (
            ('## Invoice Cedar REJECT', '## Invoice Maple'),
            ('Invoice Cedar REJECT\n--------------------', 'Invoice Maple\n-------------'),
            ('# Invoice Cedar REJECT', '# Invoice Maple'),
            ('## Invoice Cedar REJECT', '## Invoice: Maple'),
            ('Invoice Cedar REJECT\n--------------------', 'Invoice: Maple\n--------------'),
        ):
            answer = (bad_heading + '\n\n- Amount: $20.\n- Recipient: Casey.\n\n'
                      + good_heading + '\n\n- Amount: $30.\n- Recipient: Morgan.')
            result, _ = await self.finalize(answer)
            self.assertEqual(result['finalization']['disposition'], 'partial')
            self.assertNotIn('$20', result['answer'])
            self.assertNotIn('Casey', result['answer'])
            self.assertIn('Maple', result['answer'])
            self.assertIn('$30', result['answer'])
            self.assertIn('Morgan', result['answer'])

    async def test_flat_field_run_does_not_reset_context_at_a_rejected_subject_field(self):
        answer = ('- Invoice Cedar records delivery.\n- Amount: $20.\n'
                  '- **Subject:** Invoice Maple REJECT.\n- Reference No.: A-30.\n- Amount: $30.\n- Recipient: Morgan.\n\n'
                  '- Invoice Birch records delivery of 40 kg.')
        result, _ = await self.finalize(answer)
        self.assertEqual(result['finalization']['disposition'], 'partial')
        self.assertNotIn('$30', result['answer'])
        self.assertNotIn('Morgan', result['answer'])
        self.assertNotIn('A-30', result['answer'])
        self.assertIn('$20', result['answer'])
        self.assertIn('40 kg', result['answer'])

    async def test_unknown_field_run_is_withheld_without_losing_independent_observation(self):
        answer = '- Amount: $20.\n- Recipient: Casey.\n\nThe Maple invoice records $30.\n\nREJECT.'
        result, _ = await self.finalize(answer)
        self.assertEqual(result['finalization']['disposition'], 'partial')
        self.assertNotIn('$20', result['answer'])
        self.assertNotIn('Casey', result['answer'])
        self.assertIn('Maple invoice records $30', result['answer'])
        self.assertIn('dependency_unknown', [u['omission_reason'] for u in result['verification']['partial']['omitted_units']])

    async def test_original_indentation_is_in_every_audit_context(self):
        answer = '- Invoice Cedar records delivery.\n  - Amount: $20.\n\n- REJECT.'
        result, auditor = await self.finalize(answer)
        self.assertEqual(result['finalization']['disposition'], 'partial')
        self.assertEqual(auditor.calls[0][1], answer)
        self.assertIn('\n  - Amount: $20.', auditor.calls[-1][1])

    async def test_conflicting_context_never_uses_partial_delivery(self):
        answer = '- Invoice Cedar REJECT.\n  - Amount: $20.\n\n- Invoice Maple records $30.'
        result, auditor = await self.finalize(answer, conflict=True)
        self.assertFalse(result['finalization']['answer_verified'])
        self.assertEqual(len(auditor.calls), 1)

    async def test_failed_subset_still_retains_context_omission_diagnostics(self):
        answer = '- Invoice Cedar REJECT.\n  - Amount: $20.'
        result, _ = await self.finalize(answer)
        self.assertFalse(result['finalization']['answer_verified'])
        selection = result['claim_ledger']['subset_selection']
        self.assertEqual(selection['reason'], 'no_supported_context')
        self.assertEqual(selection['omitted_units'][1]['claim'], '- Amount: $20.')
        self.assertEqual(selection['omitted_units'][1]['omission_reason'], 'dependency_omitted')

    async def test_nested_heading_depth_and_multiple_supported_ancestors_survive(self):
        answer = ('# Recorded shipments\n\n## Invoice Cedar\n\n'
                  '- Cedar records delivery.\n  - Parcel recorded.\n    - Weight: 20 kg.\n\n'
                  '## Invoice Maple REJECT\n\n- Weight: 30 kg.\n\n'
                  '# Independent record\n\nBirch records 40 kg.')
        result, auditor = await self.finalize(answer)
        self.assertEqual(result['finalization']['disposition'], 'partial')
        self.assertNotIn('30 kg', result['answer'])
        self.assertIn('20 kg', result['answer'])
        self.assertIn('40 kg', result['answer'])
        self.assertIn('    - Weight: 20 kg.', auditor.calls[-1][1])

    async def test_ambiguous_literal_container_is_withheld(self):
        answer = '> Invoice Cedar records delivery.\n> Amount: $20.\n\nInvoice Maple records $30.\n\nREJECT.'
        result, _ = await self.finalize(answer)
        self.assertEqual(result['finalization']['disposition'], 'partial')
        self.assertNotIn('$20', result['answer'])
        self.assertIn('Maple records $30', result['answer'])

    async def test_omission_diagnostics_keep_failed_values_without_publishing_them(self):
        answer = '- Invoice Cedar records $999.\n  - Recipient: Casey.\n\n- Invoice Maple records $30.'
        result = await AnswerFinalizer(RecordingAuditor()).finalize(
            'Compare records.', answer, pack('Invoice Cedar records $20. Recipient: Casey. Invoice Maple records $30.'))
        self.assertEqual(result['finalization']['disposition'], 'partial')
        omitted = result['verification']['partial']['omitted_units']
        self.assertIn('value_mismatch', omitted[0]['rejection_reasons'])
        self.assertTrue(omitted[0]['value_mismatches'])
        self.assertNotIn('$999', result['answer'])
        self.assertNotIn('Casey', result['answer'])

    def test_invalid_raw_ranges_or_changed_reparse_fail_closed(self):
        answer = '- Invoice Cedar records delivery.\n  - Amount: $20.\n\nREJECT.'
        claims = [{**u, 'claim': u['text'], 'status': 'unsupported' if 'REJECT' in u['text'] else 'supported'}
                  for u in answer_units(answer)]
        for corrupt in ({'start': -1}, {'end': len(answer) + 1}, {'claim': 'Different'}, {'id': claims[1]['id']}):
            broken = [{**claims[0], **corrupt}, *claims[1:]]
            self.assertEqual(supported_revision(answer, broken, answer_units)['reason'], 'dependency_unknown')
        # Even identical words are insufficient if a revised parser places them
        # in a different container (the canonical-unit check rejects the change).
        self.assertEqual(supported_revision(answer, claims, lambda _: [])['reason'], 'structure_changed')

    async def test_colon_and_bold_labels_keep_following_fields_in_scope(self):
        for label in ('Invoice Cedar REJECT:', '**Invoice Cedar REJECT**', '__Invoice Cedar REJECT__'):
            for newline in ('\n', '\r\n', '\r'):
                answer = newline.join((label, '', 'Amount: $20.', '- Recipient: Casey.', '',
                                       '# Invoice Maple', 'Maple records $30.'))
                result, _ = await self.finalize(answer)
                self.assertEqual(result['finalization']['disposition'], 'partial')
                self.assertNotIn('$20', result['answer'])
                self.assertNotIn('Casey', result['answer'])
                self.assertIn('Maple records $30', result['answer'])

    async def test_field_dependencies_do_not_depend_on_spaces_or_bullets(self):
        for marker in ('', '- '):
            for field in ('Amount:$20.', '**Amount:**$20.', 'Amount: $20.', 'Amount:\u00a0$20.',
                          'Amount:\u2009$20.', 'Amount:\n  $20.'):
                answer = (marker + 'Invoice Cedar REJECT records delivery.\n\n'
                          + marker + field + '\n\n' + marker + 'Recipient:Casey.\n\n'
                          + ('# Independent record\n' if not marker else '') + marker + 'Invoice Maple records $30.')
                result, _ = await self.finalize(answer)
                self.assertEqual(result['finalization']['disposition'], 'partial')
                self.assertNotIn('$20', result['answer'])
                self.assertNotIn('Casey', result['answer'])
                self.assertIn('Maple records $30', result['answer'])

    async def test_audit_context_bounds_layout_only_whitespace(self):
        for padding in ('\n' + ' ' * 200000 + '\n', ' ' * 200000 + '\n'):
            answer = 'The invoice records $20.' + padding + 'The invoice records $30.'
            result, auditor = await self.finalize(answer)
            self.assertTrue(result['finalization']['answer_verified'])
            self.assertLess(len(auditor.calls[0][1]), 100)
        # Huge indentation that cannot be removed without changing structure
        # must fail closed before any provider call, at the existing prose bound.
        result, auditor = await self.finalize('The invoice records $30.\n' + ' ' * 200000 + 'The invoice records $20.')
        self.assertFalse(result['finalization']['answer_verified'])
        self.assertEqual(auditor.calls, [])

    async def test_all_assertions_of_a_plain_parent_paragraph_govern_its_fields(self):
        answer = ('Invoice Cedar REJECT records delivery. A parcel is recorded.\n\n'
                  'Weight: 20 kg.\nRecipient: Casey.\n\nInvoice Maple records 30 kg.')
        result, _ = await self.finalize(answer)
        self.assertEqual(result['finalization']['disposition'], 'partial')
        self.assertNotIn('20 kg', result['answer'])
        self.assertNotIn('Casey', result['answer'])
        self.assertIn('Maple records 30 kg', result['answer'])

    async def test_clock_times_in_independent_observations_do_not_start_field_runs(self):
        for cue in ('at', 'by', 'before', 'after', 'until', 'since', 'from', 'effective', 'around'):
            for clock in ('12:01 AM', '08:30', '23:59:00'):
                with self.subTest(cue=cue, clock=clock):
                    answer = ('- The Cedar contract records delivery.\n'
                              '- The earlier change is REJECT.\n'
                              f'- The Maple notice records a change {cue} {clock} on September 1, 2026.')
                    result, _ = await self.finalize(answer)
                    self.assertEqual(result['finalization']['disposition'], 'partial')
                    self.assertIn('Maple notice records a change', result['answer'])
                    self.assertIn(clock, result['answer'])

    async def test_time_valued_fields_still_require_their_record_context(self):
        fields = ['Start time: 12:01 AM.', '**Start time:**08:30.',
                      'Device1:20.', 'Device 1:20.', 'Item 1:20 kg.',
                      'Line 1:20.00 USD.', 'Item at 1:20 kg.', 'Line at 1:20.00 USD.',
                      '08:30: Released.',
                      '12:01 AM: Cancelled.', 'Departure at 08:30: Released.',
                      'Ratio: 3:1.']
        fields.extend(f'Item at 1:20{before}{separator}{after}000 USD.'
                      for separator in ('.', ',')
                      for before in ('', ' ', '\u00a0')
                      for after in ('', ' ', '\u00a0'))
        for field in fields:
            answer = ('- The Cedar contract REJECT records delivery.\n'
                      f'- {field}\n- Recipient: Casey.\n\n'
                      '- The Maple notice records delivery at 23:59:00.')
            result, _ = await self.finalize(answer)
            self.assertEqual(result['finalization']['disposition'], 'partial')
            self.assertNotIn(field, result['answer'])
            self.assertNotIn('Casey', result['answer'])
            self.assertIn('Maple notice records delivery at 23:59:00', result['answer'])
