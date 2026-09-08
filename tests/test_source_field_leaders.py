"""Certified field leaders are presentation, while signs remain factual."""
import unittest
from app.answer_finalization import AnswerFinalizer
from tests.test_source_dates import ExactAuditor, pack


class SourceFieldLeaderTests(unittest.IsolatedAsyncioTestCase):
    async def test_formatted_field_leaders_preserve_positive_values_and_raw_source(self):
        for label, value, claim in [('ANNUAL CHARGE', '$500', 'The annual charge is $500.'),
                                     ('RECORDED DOSE', '5 mg', 'The recorded dose is 5 mg.')]:
            source=f'**{label}** - - - - - **{value}**'
            result=await AnswerFinalizer(ExactAuditor()).finalize('What is recorded?',claim,pack(source))
            self.assertTrue(result['finalization']['answer_verified'])
            ref=result['claim_ledger']['claims'][0]['references'][0]
            self.assertEqual(ref['quote'],source)
            self.assertEqual(source[ref['start']:ref['end']],source)

    async def test_field_label_colons_preserve_source_and_sign(self):
        for label in ("**ANNUAL CHARGE:**", "**ANNUAL CHARGE**:"):
            for negative in (False,True):
                source=f'{label} - - - - **{"-" if negative else ""}$500**'
                result=await AnswerFinalizer(ExactAuditor()).finalize("What is recorded?","The annual charge is $500.",pack(source))
                self.assertEqual(result["finalization"]["answer_verified"],not negative)
                if not negative:self.assertEqual(result["claim_ledger"]["claims"][0]["references"][0]["quote"],source)

    async def test_leader_does_not_remove_attached_negative_or_literal_signs(self):
        sources=['**ANNUAL CHARGE** - - - - -$500', '**ANNUAL CHARGE** - $500',
                 'charge = - - - $500', '```\n**ANNUAL CHARGE** - - - - $500\n```',
                 '`**ANNUAL CHARGE** - - - - $500`', '<pre>\n**ANNUAL CHARGE** - - - - $500\n</pre>']
        for source in sources:
            result=await AnswerFinalizer(ExactAuditor()).finalize('What is recorded?','The annual charge is $500.',pack(source))
            self.assertFalse(result['finalization']['answer_verified'],source)

    async def test_negative_after_leader_stays_negative_and_positive_cannot_verify_negative(self):
        for source, claim in [("**ANNUAL CHARGE** - - - - **-$500**", "The annual charge is -$500."),
                              ("**RECORDED DOSE** - - - - **-5 mg**", "The recorded dose is -5 mg.")]:
            result=await AnswerFinalizer(ExactAuditor()).finalize("What is recorded?",claim,pack(source))
            self.assertTrue(result["finalization"]["answer_verified"])
            result=await AnswerFinalizer(ExactAuditor()).finalize("What is recorded?",claim,pack(source.replace("**-","**")))
            self.assertFalse(result["finalization"]["answer_verified"])

    async def test_unknown_chunk_and_sliced_literal_context_cannot_authorize_leader(self):
        evidence=pack('**ANNUAL CHARGE** - - - - $500')
        evidence['items'][0]['chunk_index']=1
        result=await AnswerFinalizer(ExactAuditor()).finalize('What is recorded?','The annual charge is $500.',evidence)
        self.assertFalse(result['finalization']['answer_verified'])
        source='```\n**ANNUAL CHARGE** - - - - $500\n```'
        class Auditor:
            async def audit_answer_units(self,question,units,spans,plan):
                span=spans[0]
                ref={**{k:span[k] for k in ('span_id','evidence_id','document_id')},
                     'quote':'**ANNUAL CHARGE** - - - - $500','source_field_leaders':[[18,26]]}
                return {'assessments':[{'unit_id':u['id'],'status':'supported','references':[ref]} for u in units]}
        result=await AnswerFinalizer(Auditor()).finalize('What is recorded?','The annual charge is $500.',pack(source))
        self.assertFalse(result['finalization']['answer_verified'])
