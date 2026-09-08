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
        return {"vector_results": [c for c in self.chunks if c["document_id"] >= 200]}

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
        for subject, doc_type in [("Insurance", "insurance"), ("Invoice", "financial"), ("Laboratory", "medical"), ("Orchid", None)]:
            with self.subTest(subject=subject):
                invalidate_on_sync()
                rows, chunks = fixtures(subject, doc_type)
                engine, auditor = HistoricalEngine(subject, chunks), HistoryAuditor()
                async def hydrate(ids, **kwargs): return [c for c in chunks if c["document_id"] in ids]
                with ExitStack() as stack:
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
                self.assertIn(101, reserved)
                self.assertIn(269, reserved)
                self.assertNotIn(999, reserved)
                self.assertLessEqual(len(reserved), MAX_DOCUMENTS)
                self.assertTrue(pack["coverage"]["history"]["truncated"])
                # Inspect actual canonical JSON sent via _final_synthesis, not
                # the unbounded backup context or an intermediate ID list.
                canonical = engine.prompt.split("Canonical evidence pack used for this answer:\n", 1)[1]
                payload, _ = json.JSONDecoder().raw_decode(canonical)
                synthesis_ids = {span["document_id"] for span in payload["spans"]}
                self.assertTrue({101, 269}.issubset(synthesis_ids))
                self.assertTrue({101, 269}.issubset(auditor.seen))
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
