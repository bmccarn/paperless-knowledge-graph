"""A batch that ends with an invalid reply is omitted, never delivered, and the rest is audited again."""
import asyncio
import json
import unittest
from unittest.mock import patch

from tests.runtime import configure_test_environment
configure_test_environment()
from app.answer_finalization import AnswerFinalizer, evidence_spans
from app.answer_observations import ObservationCandidate
from app.strands_orchestrator import StrandsQueryOrchestrator
from tests.source_audit_fixtures import decision
from tests.test_audited_missing_subset import CHARGE, DOSE, LAB, NAME
from tests.test_partial_answers import MixedAuditor, SOURCE
from tests.test_source_dates import pack

OK, HANG = object(), object()
PROTOCOL = {'audit_protocol_error': 'unknown_source_handle'}
EXTRA = 'The record names a laboratory.'
LINES = [CHARGE, DOSE, LAB, NAME, EXTRA]  # Two batches: four units, then EXTRA alone.


class ScriptedAuditor(MixedAuditor):
    """A batch holding a marker gets that marker's next scripted reply; OK audits it normally,
    and 'unsupported' audits it normally but rejects the marked unit."""
    def __init__(self, scripts, **kwargs):
        super().__init__(**kwargs)
        self.scripts = scripts

    async def audit_answer_units(self, question, units, spans, plan):
        result = await super().audit_answer_units(question, units, spans, plan)
        for marker, script in self.scripts.items():
            if script and any(marker in unit['text'] for unit in units):
                reply = script.pop(0)
                if reply is HANG:
                    await asyncio.sleep(30)
                if isinstance(reply, BaseException):
                    raise reply
                if reply == 'unsupported':
                    for assessment, unit in zip(result['assessments'], units):
                        if marker in unit['text']:
                            assessment['status'] = 'unsupported'
                elif reply is not OK:
                    return reply
        return result


async def finalize(auditor, answer='\n'.join(LINES), **kwargs):
    return await AnswerFinalizer(auditor, **kwargs).finalize('What is recorded?', answer, pack(SOURCE))


class ProtocolErrorPartialTests(unittest.IsolatedAsyncioTestCase):
    def assert_withheld(self, result, disposition):
        self.assertEqual(result['finalization']['disposition'], disposition)
        self.assertFalse(result['finalization']['answer_verified'])
        self.assertNotIn('$321', result['answer'])
        self.assertNotIn('partial', result['verification'])

    async def test_invalid_batch_is_omitted_and_the_rest_is_audited_again(self):
        for answer, text in [('\n'.join(LINES), '\n'.join),
                             (ObservationCandidate(tuple(LINES)), lambda kept: '\n\n'.join('- ' + x for x in kept))]:
            with self.subTest(observations=not isinstance(answer, str)):
                auditor = ScriptedAuditor({EXTRA: [PROTOCOL, PROTOCOL]})
                result = await finalize(auditor, answer)
                self.assertEqual(result['finalization']['disposition'], 'partial')
                self.assertTrue(result['finalization']['answer_verified'])
                # Two initial batches, one correction, one fresh subset audit.
                self.assertEqual(len(auditor.contexts), 4)
                if isinstance(answer, str):
                    self.assertEqual(auditor.contexts[-1], text(LINES[:4]))
                ledger = result['claim_ledger']
                self.assertTrue(ledger['complete'])
                self.assertEqual(ledger['summary']['supported'], ledger['summary']['total'])
                self.assertEqual(ledger['candidate_text'], text(LINES[:4]))
                self.assertNotIn(EXTRA, result['answer'])
                partial = result['verification']['partial']
                self.assertEqual((partial['omitted_count'], partial['original_total'], partial['original_supported']), (1, 5, 4))
                [omitted] = partial['omitted_units']
                self.assertEqual((omitted['id'], omitted['status'], omitted['omission_reason']),
                                 ('u5', 'unchecked', 'audit_reply_invalid'))
                self.assertIn('audit_reply_invalid', omitted['rejection_reasons'])

    async def test_second_subset_round_still_applies(self):
        # Round one omits EXTRA; the first subset audit rejects DOSE, so round two omits it too.
        auditor = ScriptedAuditor({EXTRA: [PROTOCOL, PROTOCOL], DOSE: [OK, 'unsupported']})
        result = await finalize(auditor)
        self.assertEqual(result['finalization']['disposition'], 'partial')
        self.assertEqual([(u['id'], u['omission_reason']) for u in result['verification']['partial']['omitted_units']],
                         [('u5', 'audit_reply_invalid'), ('u2', 'source_rejection')])
        self.assertNotIn('5 mg', result['answer'])

    async def test_subset_audit_with_an_invalid_reply_withholds(self):
        auditor = ScriptedAuditor({EXTRA: [PROTOCOL, PROTOCOL], CHARGE: [OK, PROTOCOL, PROTOCOL]})
        result = await finalize(auditor)
        self.assert_withheld(result, 'incomplete')
        self.assertEqual(len(auditor.contexts), 5)  # Two batches and a correction, then the subset batch and its correction.

    async def test_failed_subset_audit_is_not_retried(self):
        for failure, disposition in [(RuntimeError('unavailable'), 'audit_failed'), (HANG, 'timeout')]:
            with self.subTest(disposition):
                auditor = ScriptedAuditor({EXTRA: [PROTOCOL, PROTOCOL], CHARGE: [OK, failure]})
                result = await finalize(auditor, timeout_seconds=0.2)
                self.assert_withheld(result, disposition)
                self.assertEqual(len(auditor.contexts), 4)

    async def test_other_failures_keep_withholding(self):
        cases = {
            'empty reply': ({EXTRA: [None]}, 'incomplete'),
            'protocol error, then empty correction': ({EXTRA: [PROTOCOL, None]}, 'incomplete'),
            'protocol batch and empty batch': ({EXTRA: [PROTOCOL, PROTOCOL], CHARGE: [None]}, 'incomplete'),
            'every batch invalid': ({EXTRA: [PROTOCOL, PROTOCOL], CHARGE: [PROTOCOL, PROTOCOL]}, 'incomplete'),
            'raise': ({EXTRA: [RuntimeError('unavailable')]}, 'audit_failed'),
            'timeout': ({EXTRA: [TimeoutError()]}, 'audit_failed'),
            'hang': ({EXTRA: [HANG]}, 'timeout'),
        }
        for name, (scripts, disposition) in cases.items():
            with self.subTest(name):
                result = await finalize(ScriptedAuditor(scripts), timeout_seconds=0.2)
                self.assert_withheld(result, disposition)

    async def test_conflict_elsewhere_keeps_withholding(self):
        lines = [CHARGE, 'An extra charge is $999 USD.', LAB, NAME, EXTRA]
        result = await finalize(ScriptedAuditor({EXTRA: [PROTOCOL, PROTOCOL]}, conflict=True), '\n'.join(lines))
        self.assert_withheld(result, 'incomplete')

    async def test_repeated_unknown_handle_from_the_parser_is_omitted(self):
        text = 'The register names Vendor Example. The register lists a pump. The register lists a valve.'
        source = {'items': [{'id': 'register', 'document_id': 1, 'chunk_index': 0,
                             'source_kind': 'ocr', 'content': text, 'source_content': text}]}
        handle = evidence_spans(source, citation_safe=True)[0]['span_id']
        lines = ['The register names Vendor Example.', 'The register lists a pump.', 'The register lists a valve.',
                 'The register names Vendor Example again.', 'The register lists a pump again.']
        prompts = []

        async def reply(**kwargs):
            payload = json.loads(kwargs['prompt'])
            prompts.append(payload)
            # Only the initial audit has a fifth unit; it always cites a handle that was not offered.
            cited = 'not-offered' if 'u5' in payload['expected_unit_ids'] else handle
            return json.dumps({'assessments': [decision(unit_id=uid, references=[{'span_id': cited}])
                                               for uid in payload['expected_unit_ids']]})
        auditor = StrandsQueryOrchestrator()
        auditor.enabled = True
        with patch.object(auditor, '_text_agent', side_effect=reply):
            result = await AnswerFinalizer(auditor).finalize('What does the register record?', '\n'.join(lines), source)
        self.assertEqual(result['finalization']['disposition'], 'partial')
        self.assertEqual(len(prompts), 4)
        self.assertEqual(result['claim_ledger']['candidate_text'], '\n'.join(lines[:4]))
        [omitted] = result['verification']['partial']['omitted_units']
        self.assertEqual((omitted['claim'], omitted['omission_reason']), (lines[4], 'audit_reply_invalid'))
        self.assertNotIn('not-offered', json.dumps(result))


if __name__ == '__main__':
    unittest.main()
