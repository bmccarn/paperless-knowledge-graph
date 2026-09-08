"""History reservations reach actual synthesis and source auditing across domains."""
import asyncio
import json
import unittest
from contextlib import ExitStack
from unittest.mock import AsyncMock, patch
from tests.runtime import configure_test_environment
configure_test_environment()
from app.history_coverage import choose_documents, subject_terms, MAX_DOCUMENTS
from app.query import QueryEngine
from app.cache import invalidate_on_sync


def fixtures(subject, doc_type):
    rows, chunks = [], []
    for doc_id, year in [(101, 2022), (102, 2024)] + [(i, 2026) for i in range(200, 270)]:
        title = f"{subject} record {year}"
        text = f"{subject} dated January 1, {year}: recorded amount 5 mg." if subject == "Laboratory" else f"{subject} dated January 1, {year}: recorded charge $321 USD."
        if doc_id == 269:
            text = text.replace("January 1", "September 1")
        rows.append({"document_id": doc_id, "title": title, "doc_type": doc_type, "preview": text})
        chunks.append({"document_id": doc_id, "title": title, "doc_type": doc_type, "chunk_index": 0,
                       "source_kind": "ocr", "source_content": text + " Reference context." * 300,
                       "content": text + " Reference context." * 300, "similarity": 0.01 if doc_id < 200 else 0.99})
    rows.append({"document_id": 999, "title": "Unrelated gardening record 2021", "doc_type": "gardening", "preview": "Seeds."})
    return rows, chunks


class HistoricalEngine(QueryEngine):
    def __init__(self, subject, chunks):
        self.model, self.subject, self.chunks = "synthetic-model", subject, chunks
        self.prompt = ""

    async def _build_query_plan(self, question, mode, history=None):
        return {"mode": mode, "domain": "unknown", "requires_current": True, "broad_query": False,
                "subqueries": [{"role": "primary", "query": question}], "evaluated_at": "2026-09-08"}, []

    async def _retrieve(self, text):
        return {"vector_results": self.chunks[2:]}

    _retrieve_light = _retrieve

    async def _gap_review(self, question, context, *args, **kwargs):
        return {}, self._merge_context(context, {}), [], []

    async def _expand_planned_graph(self, context, *args):
        return self._merge_context(context, {}), []

    async def _llm_generate(self, prompt):
        self.prompt = prompt
        old = self.chunks[0]["source_content"].split(" Reference context.")[0]
        new = self.chunks[-1]["source_content"].split(" Reference context.")[0]
        return old + "\n" + new

    async def _llm_json(self, *args): return {"confidence": 1}


class HistoryAuditor:
    def __init__(self): self.seen = set()
    async def extract_timeline(self, *args): return []
    async def audit_answer_units(self, question, units, spans, plan):
        self.seen.update(s["document_id"] for s in spans)
        assessments = []
        for unit in units:
            span = next(s for s in spans if unit["text"] in s["content"])
            assessments.append({"unit_id": unit["id"], "status": "supported", "temporal_scope": "historical",
                "references": [{"span_id": span["span_id"], "evidence_id": span["evidence_id"],
                                "document_id": span["document_id"], "quote": unit["text"]}]})
        return {"assessments": assessments}


class HistoricalCoverageTests(unittest.IsolatedAsyncioTestCase):
    async def test_old_and_latest_sources_reach_public_synthesis_audit_and_delivery(self):
        for subject, doc_type, old_metadata in [("Insurance", "insurance", True), ("Invoice", "financial", True),
                                                 ("Laboratory", "medical", True), ("Orchid", None, True),
                                                 ("Invoice", None, False), ("Laboratory", None, False)]:
            with self.subTest(subject=subject, old_metadata=old_metadata):
                invalidate_on_sync()
                rows, chunks = fixtures(subject, doc_type)
                old_title = f"Archived {subject} source" if old_metadata else "Archived statement"
                rows[0].update(document_id=900, title=old_title)
                chunks[0].update(document_id=900, title=old_title)
                rows[-2]["title"] = chunks[-1]["title"] = f"Renamed {subject} source"
                engine, auditor = HistoricalEngine(subject, chunks), HistoryAuditor()
                async def hydrate(ids, **kwargs): return [c for c in chunks if c["document_id"] in ids]
                with ExitStack() as stack:
                    stack.enter_context(patch("app.query.graph_store.get_document_dates", AsyncMock(return_value={})))
                    stack.enter_context(patch("app.query.embeddings_store.historical_document_candidates", AsyncMock(return_value={"documents": rows, "candidate_count": len(rows), "truncated": False})))
                    stack.enter_context(patch("app.query.embeddings_store.get_chunks_for_documents", side_effect=hydrate))
                    stack.enter_context(patch.object(engine, "_expand_source_documents", AsyncMock(return_value=[])))
                    stack.enter_context(patch("app.query.embeddings_store.get_open_feedback_document_ids", AsyncMock(return_value=set())))
                    stack.enter_context(patch("app.query.embeddings_store.get_incomplete_document_ids", AsyncMock(return_value=set())))
                    stack.enter_context(patch("app.query.strands_orchestrator", auditor))
                    result = await engine.query(f"How has my {subject} changed over the years and what is most current?", mode="timeline")
                self.assertEqual(result["finalization"]["disposition"], "qualified")
                self.assertIn("2022", result["answer"])
                self.assertIn("2026", result["answer"])
                pack = result["evidence_pack"]
                reserved = pack["coverage"]["history"]["reserved_document_ids"]
                self.assertTrue(any(item.get("history_reserved") and item["document_id"] == 900 for item in pack["items"]))
                self.assertIn(900, reserved)
                self.assertIn(269, reserved)
                self.assertNotIn(999, reserved)
                self.assertLessEqual(len(reserved), MAX_DOCUMENTS)
                self.assertTrue(pack["coverage"]["history"]["truncated"])
                # Inspect actual canonical JSON sent via _final_synthesis, not
                # the unbounded backup context or an intermediate ID list.
                canonical = engine.prompt.split("Canonical evidence pack used for this answer:\n", 1)[1]
                payload, _ = json.JSONDecoder().raw_decode(canonical)
                synthesis_ids = {span["document_id"] for span in payload["spans"]}
                self.assertTrue({900, 269}.issubset(synthesis_ids))
                self.assertTrue({900, 269}.issubset(auditor.seen))
                self.assertLessEqual(len(json.dumps(payload, ensure_ascii=False)), 28000)

    async def test_cancellation_drains_owned_historical_retrieval(self):
        rows, chunks = fixtures("Orchid", None)
        engine = HistoricalEngine("Orchid", chunks)
        entered, finished = asyncio.Event(), asyncio.Event()
        async def blocked(*args):
            entered.set()
            try: await asyncio.Event().wait()
            finally: finished.set()
        with patch("app.query.embeddings_store.historical_document_candidates", side_effect=blocked):
            task = asyncio.create_task(engine._execute_retrieval_plan("Orchid history", {"subqueries": []}, "timeline"))
            await asyncio.wait_for(entered.wait(), 1)
            task.cancel()
            with self.assertRaises(asyncio.CancelledError): await task
            self.assertTrue(finished.is_set())

    def test_unknown_subject_and_missing_dates_have_deterministic_bounded_fallback(self):
        rows = [{"document_id": i, "title": "Orchid note", "preview": "Undated orchid observation"} for i in range(20)]
        self.assertEqual(subject_terms("How have my orchids changed over the years?"), ["orchid", "orchids"])
        self.assertEqual(choose_documents(rows, "Orchid history"), choose_documents(list(reversed(rows)), "Orchid history"))
        self.assertEqual(len(choose_documents(rows, "Orchid history")), MAX_DOCUMENTS)

    def test_year_only_preview_preserves_higher_id_older_document(self):
        rows = [{"document_id": i, "title": "Invoice annual record", "preview": "Invoice for calendar year 2026. Charge $300."} for i in range(1, 15)]
        rows.append({"document_id": 20, "title": "Invoice annual record", "preview": "Invoice for calendar year 2020. Charge $300."})
        chosen = choose_documents(rows, "How have my invoices changed over the years?")
        self.assertIn(20, [row["document_id"] for row in chosen])
        self.assertEqual(next(row["period"] for row in chosen if row["document_id"] == 20), "2020")
        for row in rows:
            row["preview"] = "Invoice code 2020. Charge $300."
            row["indexed_date"] = "2026-01-01" if row["document_id"] != 20 else "2020-01-01"
        chosen = choose_documents(rows, "Invoice history")
        self.assertIn(20, [row["document_id"] for row in chosen])
        self.assertEqual(next(row["period"] for row in chosen if row["document_id"] == 20), "2020-01-01")

    def test_temporal_strata_survive_singleton_titles_and_duplicate_population(self):
        for subject, doc_type in [("Invoice", "financial"), ("Laboratory", "medical"), ("Orchid", None)]:
            rows, _ = fixtures(subject, doc_type)
            rows[0].update(document_id=900, title=f"{subject} archived source")
            rows[-2].update(title=f"Renamed {subject} most recent source")
            for row in rows[2:-2]:
                row["title"] = f"{subject} administrative series {chr(65 + row['document_id'] % 12)} revision"
            expected = choose_documents(rows, f"{subject} history")
            self.assertTrue({900, 269}.issubset({row["document_id"] for row in expected}))
            self.assertEqual(choose_documents(list(reversed(rows)), f"{subject} history"), expected)
            duplicates = [{**row, "document_id": row["document_id"]+2000} for row in rows[2:50]]
            actual = choose_documents(rows+duplicates, f"{subject} history")
            self.assertTrue({900, 269}.issubset({row["document_id"] for row in actual}))
            coverage = {}
            choose_documents(rows, f"{subject} history", limit=1, diagnostics=coverage)
            self.assertGreater(coverage["omitted_bucket_count"], 0)
            self.assertTrue(coverage["omitted_buckets"])

    def test_mixed_metadata_retains_source_fallback_and_reports_omitted_strata(self):
        rows = [{"document_id": 900, "title": "Archived statement", "doc_type": None,
                 "preview": "Invoice for calendar year 2020. Charge $300."},
                {"document_id": 1, "title": "Invoice January 2026", "doc_type": None,
                 "preview": "Invoice for calendar year 2026. Charge $350."}]
        coverage = {}
        chosen = choose_documents(rows, "Invoice history", diagnostics=coverage)
        self.assertEqual([row["document_id"] for row in chosen], [1, 900])
        self.assertEqual(coverage["relevant_candidate_count"], 2)
        self.assertEqual(coverage["omitted_bucket_count"], 0)
        choose_documents(rows, "Invoice history", limit=1, diagnostics=coverage)
        self.assertEqual(coverage["omitted_bucket_count"], 1)
        self.assertEqual(coverage["omitted_buckets"][0]["period"], "2020")

    async def test_opening_identity_and_later_observations_reach_synthesis_and_audit(self):
        for subject, doc_type, identifier in [('Invoice', 'financial', 'INVX742'), ('Contract', 'legal', 'CTRQ813'),
                                               ('Laboratory', 'medical', 'LABK926')]:
            with self.subTest(subject=subject):
                invalidate_on_sync()
                rows, chunks = fixtures(subject, doc_type)
                facts = [f'{subject} record identifier {identifier}A.',
                         f'{subject} observation dated January 1, 2022: recorded change $321 USD.',
                         f'{subject} record identifier {identifier}B.',
                         f'{subject} observation dated September 1, 2026: recorded change $421 USD.']
                contextual = []
                for doc_id, pair in [(101, facts[:2]), (269, facts[2:])]:
                    template = next(c for c in chunks if c['document_id'] == doc_id)
                    for index, fact in [(0, pair[0]), (3, pair[1])]:
                        contextual.append({**template, 'chunk_index': index, 'content': fact + ' Filing context.' * 250,
                                           'source_content': fact + ' Filing context.' * 250})
                chunks = [c for c in chunks if c['document_id'] not in (101, 269)] + contextual
                class Engine(HistoricalEngine):
                    async def _llm_generate(self, prompt):
                        self.prompt = prompt
                        return '\n'.join(facts)
                class Auditor(HistoryAuditor):
                    async def audit_answer_units(self, question, units, spans, plan):
                        self.delivered = spans
                        return await super().audit_answer_units(question, units, spans, plan)
                engine, auditor = Engine(subject, chunks), Auditor()
                async def hydrate(ids, **kwargs):
                    return [c for c in chunks if c['document_id'] in ids]
                with ExitStack() as stack:
                    stack.enter_context(patch('app.query.graph_store.get_document_dates', AsyncMock(return_value={})))
                    stack.enter_context(patch('app.query.embeddings_store.historical_document_candidates', AsyncMock(return_value={
                        'documents': rows, 'candidate_count': len(rows), 'truncated': False})))
                    hydration = stack.enter_context(patch('app.query.embeddings_store.get_chunks_for_documents', side_effect=hydrate))
                    stack.enter_context(patch.object(engine, '_expand_source_documents', AsyncMock(return_value=[])))
                    stack.enter_context(patch('app.query.embeddings_store.get_open_feedback_document_ids', AsyncMock(return_value=set())))
                    stack.enter_context(patch('app.query.embeddings_store.get_incomplete_document_ids', AsyncMock(return_value=set())))
                    stack.enter_context(patch('app.query.strands_orchestrator', auditor))
                    result = await engine.query(f'How has my {subject} changed over the years and what is most current?', mode='timeline')
                self.assertTrue(any(call.kwargs.get('include_opening') for call in hydration.call_args_list))
                payload, _ = json.JSONDecoder().raw_decode(engine.prompt.split('Canonical evidence pack used for this answer:\n', 1)[1])
                for fact in facts:
                    self.assertTrue(any(fact in s['content'] for s in payload['spans']), fact)
                    self.assertTrue(any(fact in s['content'] for s in auditor.delivered), fact)
                    self.assertIn(fact, result['answer'])
                self.assertTrue(result['finalization']['answer_verified'])
                self.assertLessEqual(len(json.dumps(payload, ensure_ascii=False)), 28000)

    async def test_rare_latest_record_terms_survive_repetitive_notice_ranking(self):
        from app.answer_finalization import AnswerFinalizer
        for subject, identifier in [('Invoice', 'IVQ742'), ('Laboratory', 'LABR928')]:
            fact = f'{subject} {identifier}: $421 USD.'
            boilerplate = f'{subject} service charges changed over the years most current recorded amount latest record $421 USD.'
            items = [{'id': f'notice-{i}', 'document_id': i+1, 'chunk_index': 0, 'source_kind': 'ocr',
                      'content': boilerplate + ' Repetitive filing language.' * 140} for i in range(40)]
            items.append({'id': 'latest-source', 'document_id': 999, 'chunk_index': 0, 'source_kind': 'ocr',
                          'content': fact + ' Supporting text.' * 220})
            auditor = HistoryAuditor()
            result = await AnswerFinalizer(auditor).finalize(f'How have my {subject} service charges changed over the years and what is the most current recorded amount?', fact, {'items': items})
            self.assertTrue(result['finalization']['answer_verified'])
            self.assertIn(999, auditor.seen)
            self.assertEqual(result['claim_ledger']['claims'][0]['references'][0]['document_id'], 999)
