"""Request isolation and original-source conservation, without model semantics."""
import asyncio
import copy
import json
import unittest
from unittest.mock import patch

from tests.runtime import configure_test_environment
configure_test_environment()
from app.answer_finalization import AnswerFinalizer
from app.answer_observations import ObservationCandidate
from app.question_evidence import QuestionEvidence, QuestionEvidenceError
from app.strands_orchestrator import StrandsQueryOrchestrator
from tests.source_audit_fixtures import decision
from tests import test_source_reading as reading_fixtures


REQUIREMENTS = {'resolved_question': 'What do the refund records establish?', 'requirements': [
    {'id': 'r1', 'aspect': 'Documented refund actions', 'temporal_scope': 'historical',
     'comparison_scope': 'none'}]}


class QuestionEvidenceTests(unittest.IsolatedAsyncioTestCase):
    setUp = reading_fixtures.SourceReadingTests.setUp

    async def prepare(self, auditor, requirements=None, question='What do the records establish?'):
        return await QuestionEvidence.prepare(auditor, question, requirements or REQUIREMENTS,
            self.pack, evaluated_at='2026-09-09')

    async def test_read_once_before_candidates_and_conserve_sources_across_audits(self):
        auditor = StrandsQueryOrchestrator()  # Production default remains flat.
        auditor.enabled = True
        calls = []
        async def model(**kwargs):
            payload = json.loads(kwargs['prompt']); calls.append((kwargs['name'], payload))
            if kwargs['name'] == 'source_reader':
                self.assertEqual(len(payload['source_documents']), 1)
                d = payload['source_documents'][0]
                return json.dumps({'documents': [{'document_id': d['document_id'],
                    'observations': [{'text': 'UNTRUSTED NOTE', 'references': [
                        {'span_id': d['windows'][0]['span']['span_id']}]}], 'limitations': []}]})
            return json.dumps({'assessments': [decision(unit_id=u['id'], references=[
                {'span_id': payload['source_documents'][0]['windows'][0]['span']['span_id']}])
                for u in payload['units']]})
        with patch.object(auditor, '_text_agent', side_effect=model):
            snapshot = await self.prepare(auditor)
            self.assertEqual([n for n, _ in calls], ['source_reader', 'source_reader'])
            source_input = snapshot.composition_input
            source_input['source_documents'].clear()
            source_input['requirements'].clear()
            for text in ('Vendor completed a refund of $125.', 'Vendor completed a refund of $125.'):
                result = await AnswerFinalizer(snapshot.auditor(auditor)).finalize(
                    'What do the records establish?', ObservationCandidate((text,)), self.pack,
                    evaluated_at='2026-09-09', mode='quick')
                self.assertTrue(result['finalization']['answer_verified'])
                self.assertNotIn('UNTRUSTED NOTE', json.dumps(result))
        self.assertEqual([n for n, _ in calls], ['source_reader', 'source_reader', 'source_auditor', 'source_auditor'])
        for name, payload in calls:
            if name == 'source_reader':
                self.assertNotIn('units', payload)
                self.assertNotIn('source_reading', payload)
                self.assertEqual(payload['requirements'], REQUIREMENTS['requirements'])
            else:
                windows = [w for d in payload['source_documents'] for w in d['windows']]
                self.assertEqual([w['span'] for w in windows], self.spans)
                self.assertEqual(len(payload['source_reading']['documents']), 2)

    async def test_changed_question_date_or_originals_cannot_reuse_reading(self):
        class Reader:
            async def read_question_sources(inner, payload): return self.reading
        snapshot = await self.prepare(Reader())
        base = {'question': 'What do the records establish?', 'evaluated_at': '2026-09-09',
                'source_date_order': 'mdy', 'source_spans': self.spans}
        for key, value in (('question', 'Changed question'), ('evaluated_at', '2026-09-10'),
                           ('source_date_order', 'dmy'), ('source_spans', self.spans[:1]),
                           ('source_spans', [{**s, 'content': 'Changed original'} for s in self.spans])):
            with self.assertRaisesRegex(QuestionEvidenceError, '^evidence_snapshot_mismatch$'):
                snapshot.audit_payload({**base, key: value})
        other_requirements = copy.deepcopy(REQUIREMENTS)
        other_requirements['requirements'][0]['aspect'] = 'Requested refunds only'
        other = await self.prepare(Reader(), other_requirements)
        self.assertNotEqual(snapshot.digest, other.digest)
        other_requirements['requirements'].clear()
        self.assertEqual(len(other.composition_input['requirements']), 1)

    async def test_malformed_requirements_do_not_start_reading(self):
        class Reader:
            async def read_question_sources(inner, payload): self.fail('Invalid planning reached reader')
        for invalid in (None, {}, {'resolved_question': '', 'requirements': []},
                        {**REQUIREMENTS, 'requirements': REQUIREMENTS['requirements'] * 2},
                        {**REQUIREMENTS, 'requirements': [{**REQUIREMENTS['requirements'][0], 'id': True}]},
                        {**REQUIREMENTS, 'requirements': [{**REQUIREMENTS['requirements'][0], 'temporal_scope': []}]}):
            with self.assertRaises(QuestionEvidenceError):
                await QuestionEvidence.prepare(Reader(), 'Question', invalid, self.pack, evaluated_at='2026-09-09')

    async def test_cancellation_drains_document_readers(self):
        auditor = StrandsQueryOrchestrator(); auditor.enabled = True
        started = asyncio.Event(); active = 0
        async def model(**kwargs):
            nonlocal active
            active += 1; started.set()
            try: await asyncio.Event().wait()
            finally: active -= 1
        with patch.object(auditor, '_text_agent', side_effect=model):
            task = asyncio.create_task(self.prepare(auditor))
            await asyncio.wait_for(started.wait(), 1)
            task.cancel()
            with self.assertRaises(asyncio.CancelledError): await task
            self.assertEqual(active, 0)
