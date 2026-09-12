"""Typed scope through actual reader, audit parser and exact-source finalization."""
import copy
import json
import unittest
from unittest.mock import patch

from tests.runtime import configure_test_environment
configure_test_environment()
from app.answer_finalization import AnswerFinalizer, evidence_spans
from app.answer_observations import ObservationCandidate
from app.question_evidence import QuestionEvidence, QuestionEvidenceError
from app.source_scopes import SourceScope, SourceScopeError
from app.strands_orchestrator import StrandsQueryOrchestrator
from tests.source_audit_fixtures import decision
from tests.test_question_evidence import REQUIREMENTS
from tests.test_source_scopes import original


class ScopedQuestionTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        texts = ['Vendor completed a refund of $125. ' + 'Processing record. ' * 300,
                 'A refund of $250 was requested.']
        self.pack = {'items': [dict(id=f'd{i}', document_id=i + 17, chunk_index=0,
            title='Record', source_kind='ocr', content=text, source_content=text)
            for i, text in enumerate(texts)]}
        self.spans = evidence_spans(self.pack, citation_safe=True)
        self.scope = SourceScope.bind([original(i + 17, text) for i, text in enumerate(texts)], self.spans)
        self.auditor = StrandsQueryOrchestrator(); self.auditor.enabled = True
        self.calls = []
        self.passage_only = False
        self.bad_reference = None

    async def model(self, **kwargs):
        payload = json.loads(kwargs['prompt']); self.calls.append((kwargs, payload))
        docs = payload['source_documents']
        doc = docs[0]
        reference = (dict(kind='passage', handle=doc['windows'][0]['span']['span_id'])
                     if self.passage_only else doc['source_scope']['complete_original_reference'])
        reference = self.bad_reference or reference
        self.assertIn('Use typed references', kwargs['system_prompt'])
        if kwargs['name'] == 'source_reader':
            self.assertEqual(len(docs), 1)
            self.assertNotIn('units', payload)
            return json.dumps({'documents': [dict(document_id=doc['document_id'], observations=[
                dict(text='Record observation.', references=[reference])], limitations=[])]})
        return json.dumps({'assessments': [decision(unit_id=u['id'], references=[reference])
            for u in payload['units']]})

    async def prepare(self, scope=None):
        return await QuestionEvidence.prepare(self.auditor, 'What do the records establish?',
            REQUIREMENTS, self.pack, evaluated_at='2026-09-09', source_scope=scope or self.scope)

    async def test_original_selection_resolves_windows_and_retains_raw_receipts(self):
        with patch.object(self.auditor, '_text_agent', side_effect=self.model):
            evidence = await self.prepare()
            result = await AnswerFinalizer(evidence.auditor(self.auditor)).finalize(
                'What do the records establish?', ObservationCandidate(('Vendor completed a refund of $125.',)),
                self.pack, evaluated_at='2026-09-09', mode='quick')
        self.assertTrue(result['finalization']['answer_verified'])
        expected = [s['span_id'] for s in self.spans if s['document_id'] == 17]
        self.assertGreater(len(expected), 1)
        reading = evidence.composition_input['source_reading']['documents'][0]
        self.assertEqual(reading['observations'][0]['references'], [dict(span_id=h) for h in expected])
        receipt = evidence.scope_reading_receipt['resolutions'][0]['resolution']
        self.assertEqual(receipt['selections'][0]['reference']['kind'], 'complete_original')
        self.assertEqual(receipt['expanded_span_ids'], expected)
        self.assertIn('reference_scope_resolution', json.dumps(result))
        self.assertNotIn('Record observation.', json.dumps(result))
        evidence.scope_reading_receipt['resolutions'].clear()
        self.assertEqual(len(evidence.scope_reading_receipt['resolutions']), 2)
        for kwargs, payload in self.calls:
            self.assertNotIn('Return only their exact span_id', kwargs['system_prompt'])
            for doc in payload['source_documents']:
                self.assertEqual(doc['source_scope']['coverage'], 'complete_original')
        audit_payload = self.calls[-1][1]
        self.assertEqual(audit_payload['source_scope_digest'], evidence.scope_view.digest)

    async def test_passage_selection_is_not_upgraded_by_available_whole_original(self):
        self.passage_only = True
        with patch.object(self.auditor, '_text_agent', side_effect=self.model):
            evidence = await self.prepare()
            result = await evidence.auditor(self.auditor).audit_answer_units(
                'What do the records establish?', [dict(id='u1', text='Record observation.')], self.spans,
                {'evaluated_at': '2026-09-09'})
        selected = result['assessments'][0]['semantic_decision']['reference_scope_resolution']
        self.assertEqual(selected['expanded_span_ids'], [self.spans[0]['span_id']])
        self.assertEqual(selected['selections'][0]['reference']['kind'], 'passage')

    async def test_changed_sources_or_request_fail_before_audit_call(self):
        with patch.object(self.auditor, '_text_agent', side_effect=self.model):
            evidence = await self.prepare()
            before = len(self.calls)
            for spans, plan in [(self.spans[:-1], {'evaluated_at': '2026-09-09'}),
                                (self.spans, {'evaluated_at': '2026-09-10'})]:
                with self.assertRaises(QuestionEvidenceError):
                    await evidence.auditor(self.auditor).audit_answer_units(
                        'What do the records establish?', [], spans, plan)
            self.assertEqual(len(self.calls), before)
        bad = copy.deepcopy(self.spans); bad[0]['content'] = 'X' * len(bad[0]['content'])
        scope = SourceScope.bind([], bad)
        with patch.object(self.auditor, '_text_agent', side_effect=self.model):
            with self.assertRaises(SourceScopeError): await self.prepare(scope)

    async def test_unknown_typed_reference_is_protocol_failure_not_approval(self):
        with patch.object(self.auditor, '_text_agent', side_effect=self.model):
            evidence = await self.prepare()
            self.bad_reference = {'kind': 'complete_original', 'handle': self.spans[0]['span_id']}
            result = await evidence.auditor(self.auditor).audit_answer_units(
                'What do the records establish?', [dict(id='u1', text='Record observation.')], self.spans,
                {'evaluated_at': '2026-09-09'})
        self.assertEqual(result, {'audit_protocol_error': 'invalid_references'})

    async def test_semantic_rejection_survives_whole_original_scope_and_subset_reaudit(self):
        async def model(**kwargs):
            raw = await self.model(**kwargs)
            if kwargs['name'] != 'source_auditor': return raw
            payload, response = json.loads(kwargs['prompt']), json.loads(raw)
            for unit, row in zip(payload['units'], response['assessments']):
                if 'never happened' in unit['text']:
                    row['status'] = 'unsupported'
                    row['checks']['predicate'] = 'not_established'
            return json.dumps(response)
        with patch.object(self.auditor, '_text_agent', side_effect=model):
            evidence = await self.prepare()
            result = await AnswerFinalizer(evidence.auditor(self.auditor)).finalize(
                'What do the records establish?', ObservationCandidate((
                    'Vendor completed a refund of $125.', 'A further refund never happened.')),
                self.pack, evaluated_at='2026-09-09', mode='quick')
        self.assertTrue(result['finalization']['answer_verified'])
        self.assertNotIn('never happened', result['answer'])
        audits = [p for k, p in self.calls if k['name'] == 'source_auditor']
        self.assertEqual([len(p['units']) for p in audits], [2, 1])
        self.assertEqual(audits[0]['source_documents'], audits[1]['source_documents'])
        self.assertEqual(audits[0]['source_scope_digest'], audits[1]['source_scope_digest'])

    async def test_reader_correction_preserves_partition_and_discards_failed_selections(self):
        attempts = {}
        async def model(**kwargs):
            raw = await self.model(**kwargs)
            payload = json.loads(kwargs['prompt'])
            doc_id = payload['source_documents'][0]['document_id']
            attempts[doc_id] = attempts.get(doc_id, 0) + 1
            if attempts[doc_id] == 1:
                return raw.replace('"kind": "complete_original"',
                                   '"kind": "passage", "kind": "complete_original"')
            self.assertIn('offered typed references',
                          payload['reading_protocol_correction']['instruction'])
            return raw
        with patch.object(self.auditor, '_text_agent', side_effect=model):
            evidence = await self.prepare()
        self.assertEqual(attempts, {17: 2, 18: 2})
        self.assertEqual(len(evidence.scope_reading_receipt['resolutions']), 2)

    async def test_missing_altered_or_foreign_resolution_receipts_are_rejected(self):
        from app.source_scopes import ScopedReading
        mutations = [
            lambda r: r.clear(),
            lambda r: r.append(copy.deepcopy(r[0])),
            lambda r: r[0].update(observation_ordinal=1),
            lambda r: r[0].update(resolution=copy.deepcopy(r[1]['resolution'])),
            lambda r: r[0]['resolution'].update(expanded_span_ids=[self.spans[-1]['span_id']]),
            lambda r: r[0]['resolution']['selections'][0]['reference'].update(handle='foreign'),
            lambda r: r[0]['resolution']['selections'][0]['supply_scope']['supplied_intervals'][0].update(start=False),
        ]
        real = self.auditor.read_question_sources
        with patch.object(self.auditor, '_text_agent', side_effect=self.model):
            for mutate in mutations:
                async def corrupted(payload, *, source_scope):
                    actual = await real(payload, source_scope=source_scope)
                    rows = actual.receipt['resolutions']; mutate(rows)
                    return ScopedReading.create(actual.reading, rows, actual.receipt['view_digest'])
                with patch.object(self.auditor, 'read_question_sources', side_effect=corrupted):
                    with self.assertRaisesRegex(QuestionEvidenceError, '^scope_reading_mismatch$'):
                        await self.prepare()
        self.assertTrue(all(k['name'] == 'source_reader' for k, _ in self.calls))

    async def test_partial_pack_guards_and_model_sources_use_only_admitted_prefix(self):
        from tests.test_source_scopes import digest
        prefix = 'Vendor completed a refund of $125.\n'
        hidden = 'WITHHELD_SUFFIX_MARKER: A different action was completed.'
        self.pack['items'] = [{**self.pack['items'][0], 'content': prefix, 'source_content': prefix}]
        self.spans = evidence_spans(self.pack, citation_safe=True)
        chunk = dict(document_id=17, content_digest=digest(prefix), start=0, end=len(prefix))
        self.scope = SourceScope.bind([original(17, prefix + hidden)], self.spans, chunks=[chunk])
        self.passage_only = True
        with patch.object(self.auditor, '_text_agent', side_effect=self.model):
            evidence = await self.prepare()
            result = await AnswerFinalizer(evidence.auditor(self.auditor)).finalize(
                'What do the records establish?', ObservationCandidate(('Vendor completed a refund of $125.',)),
                self.pack, evaluated_at='2026-09-09', mode='quick')
        self.assertTrue(result['finalization']['answer_verified'])
        self.assertEqual(self.spans, evidence_spans(self.pack, citation_safe=True))
        for kwargs, payload in self.calls:
            self.assertNotIn(hidden, json.dumps(kwargs))
            doc = payload['source_documents'][0]
            self.assertEqual(doc['source_scope']['coverage'], 'partial_original')
            self.assertIsNone(doc['source_scope']['complete_original_reference'])
            self.assertEqual([w['span'] for w in doc['windows']], self.spans)
            self.assertEqual(doc['windows'][0]['span']['boundary_after'], '')
            self.assertEqual(doc['source_scope']['missing_intervals'], [[len(prefix), len(prefix + hidden)]])
