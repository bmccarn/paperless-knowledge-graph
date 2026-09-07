"""Real Strands serialization to the proxy through a synthetic HTTP transport."""
import json
import asyncio
import unittest
from unittest.mock import AsyncMock, patch
from contextlib import contextmanager

import httpx
from tests.runtime import configure_test_environment
configure_test_environment()

from strands import Agent
from strands.models.openai import OpenAIModel
from openai import AsyncOpenAI
from app import strands_orchestrator as module
from app.answer_finalization import AnswerFinalizer
from app.cache import invalidate_on_sync
from tests.test_query_delivery import RetrievedEngine


QUOTE = "The listed policy provides liability coverage."
PACK = {"items": [{"id": "synthetic-policy", "document_id": 101,
    "title": "Synthetic policy", "chunk_index": 0, "source_kind": "ocr",
    "source_content": QUOTE, "content": QUOTE}]}
LIMIT_FIELDS = {"max_tokens", "max_completion_tokens", "max_output_tokens"}


class StrandsOutputLimitTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.requests = []
        self.force_length = False
        self.active = self.peak = 0
        self.clients = []
        self.delay = .01
        self.started = asyncio.Event()

        async def handle(request):
            self.assertEqual(request.url.host, "127.0.0.1")
            body = json.loads(request.content)
            self.requests.append(body)
            self.active += 1
            self.peak = max(self.peak, self.active)
            if self.active == 4:
                self.started.set()
            try:
                await asyncio.sleep(self.delay)
            finally:
                self.active -= 1
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

        def client_factory(**kwargs):
            client = httpx.AsyncClient(transport=httpx.MockTransport(handle))
            self.clients.append(client)
            return AsyncOpenAI(http_client=client, **kwargs)

        self.patches = [patch.object(module, "Agent", Agent),
            patch.object(module, "OpenAIModel", OpenAIModel),
            patch.object(module, "STRANDS_AVAILABLE", True),
            patch.object(module.settings, "strands_enabled", True),
            patch("strands.models.openai.openai.AsyncOpenAI", side_effect=client_factory)]
        for item in self.patches:
            item.start()
        self.addCleanup(lambda: [item.stop() for item in reversed(self.patches)])
        self.orchestrator = module.StrandsQueryOrchestrator()
        self.addAsyncCleanup(self.orchestrator.close)

    async def asyncTearDown(self):
        self.assertTrue(all(client.is_closed for client in self.clients))

    @contextmanager
    def public_queries(self):
        invalidate_on_sync()
        engine = RetrievedEngine()
        with patch("app.query.strands_orchestrator", self.orchestrator), \
                patch.object(engine, "_build_evidence_pack", AsyncMock(return_value=PACK)), \
                patch.object(engine, "_final_synthesis", AsyncMock(return_value={"answer": "\n".join([QUOTE] * 56)})), \
                patch("app.query.embeddings_store.get_incomplete_document_ids", AsyncMock(return_value=set())), \
                patch("app.query.embeddings_store.get_open_feedback_document_ids", AsyncMock(return_value=set())):
            yield engine

    async def test_overlapping_public_queries_audit_all_units_through_real_sdk(self):
        with self.public_queries() as engine:
            async def streamed():
                return [event async for event in engine.query_stream("Recorded coverage in stream?", mode="strict")][-1]
            ordinary, stream, helper = await asyncio.gather(
                engine.query("Recorded coverage?", mode="strict"), streamed(),
                self.orchestrator.plan_query("Unrelated helper?", "strict"))
        for result in (ordinary, stream):
            self.assertEqual(result["finalization"]["disposition"], "supported")
            self.assertEqual(result["claim_ledger"]["summary"]["supported"], 56)
            self.assertEqual(result["claim_ledger"]["candidate_digest"], result["finalization"]["candidate_digest"])
        self.assertEqual(ordinary["answer"], stream["answer"])
        self.assertEqual(helper, {"ok": True})
        self.assertEqual(len(self.requests), 29)  # two 14-batch audits and one planner
        self.assertEqual(self.peak, 4)
        self.assertEqual(self.active, 0)
        self.assertTrue(all(LIMIT_FIELDS.isdisjoint(request) for request in self.requests))

    async def test_cancelled_public_query_does_not_close_another_queries_clients(self):
        self.delay = .02
        with self.public_queries() as engine:
            cancelled = asyncio.create_task(engine.query("Cancel this coverage query?", mode="strict"))
            survivor = asyncio.create_task(engine.query("Finish this coverage query?", mode="strict"))
            try:
                await asyncio.wait_for(self.started.wait(), 1)
                cancelled.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await cancelled
                result = await asyncio.wait_for(survivor, 3)
            finally:
                for task in (cancelled, survivor):
                    task.cancel()
                await asyncio.gather(cancelled, survivor, return_exceptions=True)
        self.assertEqual(result["claim_ledger"]["summary"]["supported"], 56)
        self.assertTrue(result["finalization"]["complete"])
        self.assertEqual(self.active, 0)
        self.assertLessEqual(self.peak, 4)

    async def test_supported_audit_can_finish_above_former_output_cap(self):
        result = await AnswerFinalizer(self.orchestrator).finalize("What coverage is listed?", QUOTE, PACK)
        self.assertEqual(len(self.requests), 1)
        self.assertTrue(LIMIT_FIELDS.isdisjoint(self.requests[0]), self.requests[0].keys())
        self.assertEqual(result["finalization"]["disposition"], "supported")
        self.assertEqual(result["claim_ledger"]["summary"]["supported"], 1)

    async def test_native_sdk_audit_payload_carries_ordered_answer_context(self):
        expected_context = "\n".join([QUOTE] * 8)
        answer = expected_context.replace("\n", "\n" * 10000, 1)
        result = await AnswerFinalizer(self.orchestrator).finalize("Recorded coverage?", answer, PACK)
        self.assertTrue(result["finalization"]["complete"])
        self.assertEqual(len(self.requests), 2)
        for request in self.requests:
            message = request["messages"][-1]["content"]
            if isinstance(message, list):
                message = "".join(block.get("text", "") for block in message)
            payload = json.loads(message)
            self.assertEqual(payload["answer_context"], expected_context)
            self.assertEqual(len(payload["units"]), 4)

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

    async def test_overlapping_helpers_use_independent_transport_lifetimes(self):
        results = await asyncio.gather(*(self.orchestrator.plan_query("Synthetic question?", "strict") for _ in range(8)))
        self.assertEqual(results, [{"ok": True}] * 8)
        self.assertGreater(self.peak, 1)
        self.assertLessEqual(self.peak, 4)
        self.assertEqual(self.active, 0)

    async def test_cancellation_closes_active_clients_and_releases_call_slots(self):
        self.delay = 10
        tasks = [asyncio.create_task(self.orchestrator.plan_query("Synthetic question?", "strict")) for _ in range(8)]
        try:
            await asyncio.wait_for(self.started.wait(), timeout=1)
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
        self.assertEqual(self.active, 0)
        self.assertTrue(all(client.is_closed for client in self.clients))
        self.delay = 0
        self.assertEqual(await self.orchestrator.plan_query("Synthetic question?", "strict"), {"ok": True})
