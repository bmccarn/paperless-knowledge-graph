"""Real Strands/LiteLLM serialization against a synthetic HTTP transport."""
import json
import unittest
from unittest.mock import patch

import httpx
from tests.runtime import configure_test_environment
configure_test_environment()

from strands import Agent
from strands.models.litellm import LiteLLMModel
from litellm.llms.custom_httpx.http_handler import AsyncHTTPHandler
from app import strands_orchestrator as module
from app.answer_finalization import AnswerFinalizer


QUOTE = "The listed policy provides liability coverage."
PACK = {"items": [{"id": "synthetic-policy", "document_id": 101,
    "title": "Synthetic policy", "chunk_index": 0, "source_kind": "ocr",
    "source_content": QUOTE, "content": QUOTE}]}
LIMIT_FIELDS = {"max_tokens", "max_completion_tokens", "max_output_tokens"}


class StrandsOutputLimitTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.requests = []
        self.force_length = False

        async def handle(request):
            self.assertEqual(request.url.host, "127.0.0.1")
            body = json.loads(request.content)
            self.requests.append(body)
            message = body["messages"][-1]["content"]
            if isinstance(message, list):
                message = "".join(block.get("text", "") for block in message)
            try:
                payload = json.loads(message)
            except (ValueError, TypeError):
                payload = {}
            result = {"ok": True}
            if "units" in payload:
                span = payload["source_spans"][0]
                result = {"assessments": [{"unit_id": unit["id"], "status": "supported",
                    "references": [{"span_id": span["span_id"], "evidence_id": span["evidence_id"],
                        "document_id": span["document_id"], "quote": QUOTE}]} for unit in payload["units"]]}
            # Simulate a provider completion that exceeds the former 6,000-token
            # allowance. Genuine provider truncation must remain a failed audit.
            truncated = self.force_length or bool(LIMIT_FIELDS.intersection(body))
            content = '{"assessments":[' if truncated else json.dumps(result)
            chunk = {"id": "synthetic-stream", "object": "chat.completion.chunk", "created": 0,
                "model": body["model"], "choices": [{"index": 0,
                    "delta": {"role": "assistant", "content": content}, "finish_reason": None}]}
            end = {**chunk, "choices": [{"index": 0, "delta": {},
                "finish_reason": "length" if truncated else "stop"}],
                "usage": {"prompt_tokens": 100, "completion_tokens": 6501, "total_tokens": 6601}}
            data = "".join("data: " + json.dumps(part) + "\n\n" for part in (chunk, end)) + "data: [DONE]\n\n"
            return httpx.Response(200, content=data.encode(), headers={"content-type": "text/event-stream"})

        self.patches = [patch.object(module, "Agent", Agent),
            patch.object(module, "LiteLLMModel", LiteLLMModel),
            patch.object(module, "STRANDS_AVAILABLE", True),
            patch.object(module.settings, "strands_enabled", True),
            patch.object(AsyncHTTPHandler, "_create_async_transport", return_value=httpx.MockTransport(handle))]
        for item in self.patches:
            item.start()
        self.addCleanup(lambda: [item.stop() for item in reversed(self.patches)])
        self.orchestrator = module.StrandsQueryOrchestrator()
        self.addAsyncCleanup(self.orchestrator.close)

    async def test_supported_audit_can_finish_above_former_output_cap(self):
        result = await AnswerFinalizer(self.orchestrator).finalize("What coverage is listed?", QUOTE, PACK)
        self.assertEqual(len(self.requests), 1)
        self.assertTrue(LIMIT_FIELDS.isdisjoint(self.requests[0]), self.requests[0].keys())
        self.assertEqual(result["finalization"]["disposition"], "supported")
        self.assertEqual(result["claim_ledger"]["summary"]["supported"], 1)

    async def test_helper_requests_omit_output_caps(self):
        calls = [(lambda: self.orchestrator.plan_query("Synthetic question?", "strict"), {"ok": True}),
            (lambda: self.orchestrator.extract_timeline("Synthetic question?", "Synthetic evidence"), []),
            (lambda: self.orchestrator.repair_answer("Synthetic question?", "Synthetic answer", "Synthetic evidence", {}), {"ok": True}),
            (lambda: self.orchestrator.review_entity_candidate({}, {}), {"ok": True})]
        for call, expected in calls:
            with self.subTest(helper=len(self.requests)):
                result = await call()
                self.assertTrue(LIMIT_FIELDS.isdisjoint(self.requests[-1]), self.requests[-1].keys())
                self.assertEqual(result, expected)
        self.assertEqual(len(self.requests), len(calls))

    async def test_actual_provider_truncation_still_cannot_certify_an_answer(self):
        self.force_length = True
        result = await AnswerFinalizer(self.orchestrator).finalize("What coverage is listed?", QUOTE, PACK)
        self.assertTrue(LIMIT_FIELDS.isdisjoint(self.requests[0]))
        self.assertNotEqual(result["finalization"]["disposition"], "supported")
        self.assertFalse(result["finalization"]["complete"])
        self.assertNotIn(QUOTE, result["answer"])
