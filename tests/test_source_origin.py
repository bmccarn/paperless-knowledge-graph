"""Generated retrieval hints cannot become certifying document text."""
import copy
import json
from contextlib import ExitStack

import httpx
from openai import AsyncOpenAI
import unittest
from unittest.mock import AsyncMock, patch

from tests.runtime import configure_test_environment
configure_test_environment()
from app.answer_finalization import AnswerFinalizer, evidence_spans
from app.evidence import build_evidence_pack
from app.query import QueryEngine
from app.cache import invalidate_on_sync
from app.timeline import validate_timeline

OCR = "Monthly premium: $321.00 USD."
SUMMARY = {"document_id": 101, "chunk_index": 9999, "title": "January statement",
           "content": "DOCUMENT SUMMARY — January statement\n\nMonthly premium: $999.00 USD."}


class SourceAuditor:
    status = {"enabled": True, "provider": "synthetic"}

    async def plan_query(self, question, mode, conversation_context=""):
        return {"intent": "lookup", "domain": "general", "requires_current": False,
                "subqueries": [{"role": "primary", "query": question}]}

    async def audit_answer_units(self, question, units, spans, plan):
        assessments = []
        for unit in units:
            span = next((s for s in spans if unit["text"] in s["content"]), None)
            assessments.append({"unit_id": unit["id"], "status": "supported" if span else "missing",
                "references": [{"span_id": span["span_id"], "evidence_id": span["evidence_id"],
                                "document_id": span["document_id"], "quote": unit["text"]}] if span else []})
        return {"assessments": assessments}

    async def repair_answer(self, *args):
        return None


class SourceOriginTests(unittest.IsolatedAsyncioTestCase):
    async def test_timeline_dates_require_original_ocr_even_when_a_generated_quote_matches(self):
        quote = "2026-01-01: Premium. Monthly premium: $999.00 USD."
        for kind, index in (("ocr", 0), ("generated", 2), ("legacy", 9999)):
            with self.subTest(kind=kind):
                pack = {"items": [{"id": "dated-source", "document_id": 101, "chunk_index": index,
                                   "source_kind": kind, "content": quote}]}
                spans = evidence_spans(pack)
                event = {"date": "2026-01-01", "title": "Premium", "summary": "Monthly premium: $999.00 USD.",
                         "document_id": 101, "references": [{"span_id": spans[0]["span_id"] if spans else "rejected-origin",
                         "evidence_id": "dated-source", "document_id": 101, "quote": quote}]}
                result = await validate_timeline([event], pack, SourceAuditor(), "Premium timeline?")
                self.assertEqual(len(result), 1 if kind == "ocr" else 0)

    async def test_generated_legacy_summary_cannot_certify_an_answer(self):
        pack = build_evidence_pack("Recorded premium?", {}, [SUMMARY], [])
        result = await AnswerFinalizer(SourceAuditor()).finalize(
            "Recorded premium?", "Monthly premium: $999.00 USD.", pack)
        self.assertFalse(result["finalization"]["complete"])
        self.assertNotIn("999", result["answer"])
        self.assertEqual(evidence_spans(pack), [])

    async def test_origins_and_generated_metadata_are_excluded_from_exact_quotes(self):
        original = {"document_id": 101, "chunk_index": 0, "title": "Statement", "doc_type": "invoice",
                    "content": "Document: Statement\nType: invoice\nDate: 2099-01-01\n\n" + OCR}
        pack = build_evidence_pack("Recorded premium?", {}, [original, {**SUMMARY, "chunk_index": 2, "source_kind": "generated"}], [])
        self.assertEqual(len(pack["items"]), 1)
        self.assertEqual(pack["items"][0]["content"], OCR)
        self.assertEqual((await AnswerFinalizer(SourceAuditor()).finalize("Recorded premium?", OCR, pack))["finalization"]["disposition"], "supported")
        self.assertEqual(evidence_spans({"items": [{**SUMMARY, "id": "bypass"}]}), [])

    async def test_query_hydrates_original_ocr_or_abstains_when_only_a_summary_is_retrieved(self):
        for hydration in (False, True):
            with self.subTest(hydration=hydration):
                source = {"id": 101, "title": "Statement", "content": OCR, "created": "2026-01-01"}
                fetch = AsyncMock(return_value=source) if hydration else AsyncMock(side_effect=ConnectionError("source unavailable"))
                result = await query_from_summary(OCR if hydration else "Monthly premium: $999.00 USD.", fetch)
                self.assertEqual(result["finalization"]["complete"], hydration)
                self.assertNotIn("999", result["answer"])
                self.assertTrue(all(item["chunk_index"] != 9999 for item in result["evidence_pack"]["items"]))
                self.assertEqual(fetch.await_args.args, (101,))

    async def test_live_source_metadata_is_not_quoteable_ocr(self):
        fetch = AsyncMock(return_value={"id": 101, "title": "Invented premium: $999.00 USD.", "content": OCR})
        result = await query_from_summary("Invented premium: $999.00 USD.", fetch)
        self.assertFalse(result["finalization"]["complete"])
        self.assertNotIn("999", result["answer"])


async def query_from_summary(answer, source_fetch):
    """Run unmodified QueryEngine orchestration with only external I/O scripted."""
    invalidate_on_sync()
    def model_response(request):
        body = json.loads(request.content)
        content = json.dumps({"entities": [], "entities_found": [], "draft_answer": answer,
                              "follow_up_queries": [], "confidence": 0.9}) if body.get("response_format") else answer
        return httpx.Response(200, json={"id": "synthetic", "object": "chat.completion", "created": 0,
            "model": "synthetic", "choices": [{"index": 0, "finish_reason": "stop",
            "message": {"role": "assistant", "content": content}}]})
    engine = QueryEngine()
    await engine.close()
    engine.client = AsyncOpenAI(base_url="http://synthetic.invalid/v1", api_key="synthetic-test-only",
        http_client=httpx.AsyncClient(transport=httpx.MockTransport(model_response)))
    try:
        with ExitStack() as stack:
            stack.enter_context(patch("app.query.strands_orchestrator", SourceAuditor()))
            stack.enter_context(patch("app.query.paperless_client.get_document", source_fetch))
            stack.enter_context(patch("app.query.graph_store.get_document_entities", AsyncMock(return_value=[])))
            for name, value in {
                "vector_search": [copy.deepcopy(SUMMARY)], "keyword_search": [], "entity_vector_search": [],
                "entity_keyword_search": [], "get_chunks_for_documents": [],
                "get_incomplete_document_ids": set(), "get_open_feedback_document_ids": set(),
            }.items():
                stack.enter_context(patch(f"app.query.embeddings_store.{name}", AsyncMock(return_value=value)))
            return await engine.query("Recorded premium?", mode="strict")
    finally:
        await engine.close()
