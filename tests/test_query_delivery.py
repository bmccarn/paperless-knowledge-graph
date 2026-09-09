"""Public delivery/cache contract with controlled retrieval and model adapters."""
import asyncio
import copy
import unittest
from unittest.mock import AsyncMock, patch

from tests.runtime import configure_test_environment
configure_test_environment()
from app.query import QueryEngine
from app.cache import invalidate_on_sync
from tests.test_answer_finalization import PACK, SupportedAuditor


class RetrievedEngine(QueryEngine):
    def __init__(self):
        self.model = "default-model"
        self.calls = []

    async def _build_query_plan(self, question, mode, history=None):
        return {"requires_current": False, "mode": mode}, []

    async def _execute_retrieval_plan(self, question, plan, mode):
        return {"vector_results": [], "graph_nodes": []}, [], False, []

    async def _gap_review(self, question, context, history, mode, broad=False):
        return {}, context, [], []

    async def _expand_planned_graph(self, context, entities):
        return context, []

    async def _build_evidence_pack(self, *args, **kwargs):
        return copy.deepcopy(PACK)

    async def _final_synthesis(self, question, context, draft, history, **kwargs):
        before = self._active_model()
        await asyncio.sleep(0.01)
        self.calls.append((before, self._active_model(), history))
        return {"answer": "Monthly premium: $321.00 USD.", "confidence": 0.99}


class QueryDeliveryTests(unittest.IsolatedAsyncioTestCase):
    async def test_full_56_unit_answer_completes_through_stream_and_ordinary_delivery(self):
        from tests.test_audit_execution import ConcurrentAuditor
        answer = "\n".join(["Monthly premium: $321.00 USD."] * 56)
        for streaming in (False, True):
            invalidate_on_sync()
            auditor = ConcurrentAuditor(delay=.01)
            with patch("app.query.strands_orchestrator", auditor), \
                    patch.object(self.engine, "_final_synthesis", AsyncMock(return_value={"answer": answer})), \
                    patch("app.query.settings.answer_audit_timeout_seconds", .1):
                result = ([event async for event in self.engine.query_stream("Recorded premium?", mode="strict")][-1]
                          if streaming else await self.engine.query("Recorded premium?", mode="strict"))
            self.assertEqual(result["finalization"]["disposition"], "supported")
            self.assertEqual(result["claim_ledger"]["summary"]["supported"], 56)
            self.assertEqual(auditor.peak, 4)
            self.assertEqual(auditor.active, 0)

    async def asyncSetUp(self):
        invalidate_on_sync()
        self.engine = RetrievedEngine()
        self.patches = [patch("app.query.strands_orchestrator", SupportedAuditor()),
                        patch("app.query.embeddings_store.get_incomplete_document_ids", AsyncMock(return_value=set())),
                        patch("app.query.embeddings_store.get_open_feedback_document_ids", AsyncMock(return_value=set()))]
        for p in self.patches:
            p.start()

    async def asyncTearDown(self):
        for p in self.patches:
            p.stop()

    async def test_ordinary_and_sse_have_identical_final_payload_no_draft_chunks(self):
        ordinary = await self.engine.query("Recorded premium?", mode="strict")
        events = [e async for e in self.engine.query_stream("Recorded premium?", mode="strict")]
        self.assertEqual([e["type"] for e in events], ["status", "complete"])
        streamed = {k: v for k, v in events[-1].items() if k != "type"}
        ordinary["cached"] = True
        self.assertEqual(ordinary, streamed)
        self.assertEqual(streamed["verification"]["status"], "verified")
        self.assertIn("[Document 101](/documents/101)", streamed["answer"])
        self.assertEqual(streamed["evidence_pack"]["items"][0]["support_spans"][0]["document_id"], 101)

    async def test_partial_is_consistent_across_delivery_and_never_cached(self):
        from tests.test_partial_answers import MixedAuditor, SOURCE
        from tests.test_source_dates import pack
        answer = "The invoice records a $321 USD service charge.\nAn extra charge is $999 USD."
        with patch("app.query.strands_orchestrator", MixedAuditor()), \
                patch.object(self.engine, "_build_evidence_pack", AsyncMock(side_effect=lambda *a, **kw: pack(SOURCE))), \
                patch.object(self.engine, "_final_synthesis", AsyncMock(return_value={"answer": answer})):
            ordinary = await self.engine.query("Recorded charges?", mode="strict")
            events = [e async for e in self.engine.query_stream("Recorded charges?", mode="strict")]
        streamed = {k: v for k, v in events[-1].items() if k != "type"}
        for result in (ordinary, streamed):
            for step in result["trace"]:
                step.pop("timestamp", None)
        self.assertEqual(ordinary, streamed)
        self.assertFalse(streamed["cached"])
        self.assertEqual(streamed["verification"]["status"], "partial")
        self.assertTrue(streamed["finalization"]["answer_verified"])
        self.assertFalse(streamed["evidence"]["coverage"]["answer_complete"])
        self.assertNotIn("999", streamed["answer"])
        self.assertEqual(len(streamed["claim_ledger"]["claims"]), 1)

    async def test_snapshot_change_clears_partial_verdict_and_diagnostics(self):
        from tests.test_partial_answers import MixedAuditor, SOURCE
        from tests.test_source_dates import pack
        with patch("app.query.strands_orchestrator", MixedAuditor()), \
                patch.object(self.engine, "_build_evidence_pack", AsyncMock(return_value=pack(SOURCE))), \
                patch.object(self.engine, "_final_synthesis", AsyncMock(return_value={"answer": "The invoice records a $321 USD service charge.\nAn extra charge is $999 USD."})), \
                patch("app.query.embeddings_store.get_incomplete_document_ids", AsyncMock(return_value={101})):
            result = await self.engine.query("Recorded charges?", mode="strict")
        self.assertEqual(result["finalization"]["disposition"], "corpus_changed")
        self.assertFalse(result["finalization"]["answer_verified"])
        self.assertNotIn("partial", result["verification"])
        self.assertNotIn("321", result["answer"])
        self.assertEqual(result["claim_ledger"]["claims"], [])

    async def test_concurrent_models_and_full_history_have_distinct_cache_identity(self):
        history_a = [{"role": "user", "content": "same " * 100 + "A"}]
        history_b = [{"role": "user", "content": "same " * 100 + "B"}]
        results = await asyncio.gather(
            self.engine.query("Recorded premium?", history_a, "model-a"),
            self.engine.query("Recorded premium?", history_b, "model-b"))
        self.assertTrue(all(not r["cached"] for r in results))
        self.assertEqual([(a, b) for a, b, _ in self.engine.calls], [("model-a", "model-a"), ("model-b", "model-b")])
        await self.engine.query("Recorded premium?", history_b, "model-a")
        self.assertEqual(len(self.engine.calls), 3)
        self.assertEqual(self.engine._active_model(), "default-model")

    async def test_new_generation_misses_previous_completed_answer(self):
        first = await self.engine.query("Recorded premium?")
        invalidate_on_sync()
        second = await self.engine.query("Recorded premium?")
        self.assertFalse(first["cached"])
        self.assertFalse(second["cached"])
        self.assertEqual(len(self.engine.calls), 2)

    async def test_helper_model_change_misses_cache_even_with_fixed_query_override(self):
        from app.config import settings
        with patch.object(settings, "strands_model", ""):
            for model in ("gemini-3.5-flash", "gemini-3.8-flash"):
                with patch.object(settings, "gemini_model", model):
                    result = await self.engine.query("Recorded premium?", model_override="fixed-query-model")
                    self.assertFalse(result["cached"])
                    cached = await self.engine.query("Recorded premium?", model_override="fixed-query-model")
                    self.assertTrue(cached["cached"])
        self.assertEqual(len(self.engine.calls), 2)

    async def test_auditor_receives_full_followup_context_without_echoing_history_publicly(self):
        history = [{"role": "user", "content": "context " * 200 + "important subject at the end"}]
        contexts = []
        class Auditor(SupportedAuditor):
            async def audit_answer_units(self, question, units, spans, plan):
                contexts.append(plan.get("conversation_context"))
                return await super().audit_answer_units(question, units, spans, plan)
        with patch("app.query.strands_orchestrator", Auditor()):
            result = await self.engine.query("Recorded premium?", history)
        self.assertIn("important subject at the end", contexts[0])
        self.assertNotIn("conversation_context", result["query_plan"])

    async def test_long_history_is_bounded_for_planning_synthesis_and_audit(self):
        history = [{"role": "user", "content": f"old-turn-{i} " + "detail " * 3000}
                   for i in range(40)]
        history[-1]["content"] += "latest follow-up subject"
        original = copy.deepcopy(history)
        contexts = []
        class Auditor(SupportedAuditor):
            async def audit_answer_units(self, question, units, spans, plan):
                contexts.append(plan["conversation_context"])
                return await super().audit_answer_units(question, units, spans, plan)
        with patch("app.query.strands_orchestrator", Auditor()):
            await self.engine.query("Recorded premium?", history)
        engine = object.__new__(QueryEngine)
        planner = AsyncMock(return_value=None)
        generation = AsyncMock(return_value={"draft_answer": "Recorded premium."})
        with patch("app.query.strands_orchestrator.plan_query", planner, create=True), \
             patch("app.query.strands_orchestrator.status", {}, create=True), \
             patch.object(engine, "_llm_json", generation):
            await engine._build_query_plan("Recorded premium?", "strict", history)
            await engine._synthesize_with_gaps("Recorded premium?", {}, history)
        contexts.append(planner.call_args.kwargs["conversation_context"])
        contexts.append(generation.call_args.args[0])
        contexts.append(engine._build_final_prompt("Recorded premium?", "strict", "", "", "",
                                                   history, plan={}))
        for context in contexts:
            self.assertLess(len(context), 20_000)
            self.assertIn("latest follow-up subject", context)
            self.assertNotIn("old-turn-0 ", context)
        self.assertEqual(history, original)

    async def test_inflight_mutation_or_incomplete_replacement_cannot_finalize(self):
        task = asyncio.create_task(self.engine.query("Recorded premium?"))
        await asyncio.sleep(0.005)
        invalidate_on_sync()
        result = await task
        self.assertEqual(result["finalization"]["disposition"], "corpus_changed")
        self.assertNotIn("321", result["answer"])
        self.assertEqual(result["claim_ledger"]["claims"], [])
        self.assertEqual(result["verification"]["supported_claims"], [])
        self.assertEqual(result["source_summary"]["claim_summary"]["supported"], 0)
        self.assertFalse(result["evidence"]["coverage"]["answer_complete"])
        with patch("app.query.embeddings_store.get_incomplete_document_ids", AsyncMock(return_value={101})):
            result = await self.engine.query("Recorded premium?")
        self.assertEqual(result["finalization"]["disposition"], "corpus_changed")
        self.assertEqual(result["confidence"], 0)
        self.assertEqual(result["claim_ledger"]["claims"], [])
        self.assertEqual(result["verification"]["supported_claims"], [])
        self.assertEqual(result["evidence"]["claim_summary"]["supported"], 0)

    async def test_unavailable_auditor_sends_same_abstention_through_both_paths(self):
        class Unavailable:
            async def audit_answer_units(self, *args):
                return None
            async def repair_answer(self, *args):
                return None
        with patch("app.query.strands_orchestrator", Unavailable()):
            result = await self.engine.query("Recorded premium?")
            complete = [e async for e in self.engine.query_stream("Recorded premium?")][-1]
        self.assertEqual(result["answer"], complete["answer"])
        self.assertNotIn("321", result["answer"])
        self.assertEqual(result["confidence"], 0)
        self.assertEqual(result["finalization"]["disposition"], "incomplete")

    async def test_retry_after_unavailable_audit_runs_again_then_caches_success(self):
        class Unavailable:
            async def audit_answer_units(self, *args):
                return None
            async def repair_answer(self, *args):
                return None
        for streaming in (False, True):
            invalidate_on_sync()
            self.engine.calls.clear()
            with patch("app.query.strands_orchestrator", Unavailable()):
                failed = await self.engine.query("Recorded premium?", mode="strict")
            self.assertFalse(failed["finalization"]["complete"])
            result = ([event async for event in self.engine.query_stream("Recorded premium?", mode="strict")][-1]
                      if streaming else await self.engine.query("Recorded premium?", mode="strict"))
            self.assertTrue(result["finalization"]["complete"])
            self.assertFalse(result["cached"])
            cached = await self.engine.query("Recorded premium?", mode="strict")
            self.assertTrue(cached["cached"])
            self.assertEqual(len(self.engine.calls), 2)

    async def test_historical_failed_cache_entries_are_ignored(self):
        successful = await self.engine.query("Recorded premium?", mode="strict")
        for disposition in ("incomplete", "unsupported", "timeout", "audit_failed", "current_unresolved", "corpus_changed"):
            failed = copy.deepcopy(successful)
            failed["finalization"].update(complete=False, disposition=disposition)
            with patch("app.query.cache_get", AsyncMock(return_value=failed)):
                result = await self.engine.query("Recorded premium?", mode="strict")
            self.assertFalse(result["cached"], disposition)
            self.assertTrue(result["finalization"]["complete"], disposition)

    async def test_explicitly_unaudited_quick_answer_still_reuses_cache(self):
        result = await self.engine.query("Recorded premium?", mode="quick")
        self.assertEqual(result["finalization"]["disposition"], "unaudited")
        cached = await self.engine.query("Recorded premium?", mode="quick")
        self.assertTrue(cached["cached"])
        self.assertEqual(len(self.engine.calls), 1)

    async def test_cached_answers_recheck_snapshot_before_ordinary_and_stream_delivery(self):
        import app.query as query_module
        for streaming in (False, True):
            with self.subTest(streaming=streaming):
                invalidate_on_sync()
                await self.engine.query("Recorded premium?")
                original_get = query_module.cache_get
                read_started, resume = asyncio.Event(), asyncio.Event()
                async def delayed_get(cache, key):
                    value = await original_get(cache, key)
                    if value is not None:
                        read_started.set()
                        await resume.wait()
                    return value
                async def deliver():
                    if streaming:
                        return [event async for event in self.engine.query_stream("Recorded premium?")][-1]
                    return await self.engine.query("Recorded premium?")
                with patch("app.query.cache_get", delayed_get):
                    request = asyncio.create_task(deliver())
                    await read_started.wait()
                    invalidate_on_sync()
                    resume.set()
                    result = await request
                self.assertEqual(result["finalization"]["disposition"], "corpus_changed")
                self.assertFalse(result["finalization"]["complete"])
                self.assertNotIn("321", result["answer"])
                self.assertEqual(result["claim_ledger"]["claims"], [])
                self.assertEqual(result["verification"]["supported_claims"], [])
                self.assertEqual(result["source_summary"]["claim_summary"]["supported"], 0)

    async def test_cached_answers_recheck_incomplete_source_markers(self):
        await self.engine.query("Recorded premium?")
        with patch("app.query.embeddings_store.get_incomplete_document_ids", AsyncMock(return_value={101})):
            result = await self.engine.query("Recorded premium?")
        self.assertEqual(result["finalization"]["disposition"], "corpus_changed")
        self.assertEqual(result["confidence"], 0)

    async def test_mutation_during_cache_write_is_rejected_at_final_delivery(self):
        import app.query as query_module
        for streaming in (False, True):
            with self.subTest(streaming=streaming):
                invalidate_on_sync()
                original_set = query_module.cache_set
                write_started, resume = asyncio.Event(), asyncio.Event()
                async def delayed_set(cache, key, value):
                    write_started.set()
                    await resume.wait()
                    await original_set(cache, key, value)
                async def deliver():
                    if streaming:
                        return [event async for event in self.engine.query_stream("Recorded premium?")][-1]
                    return await self.engine.query("Recorded premium?")
                with patch("app.query.cache_set", delayed_set):
                    request = asyncio.create_task(deliver())
                    await write_started.wait()
                    invalidate_on_sync()
                    resume.set()
                    result = await request
                self.assertEqual(result["finalization"]["disposition"], "corpus_changed")
                self.assertFalse(result["finalization"]["complete"])
                self.assertNotIn("321", result["answer"])


if __name__ == "__main__":
    unittest.main()
