"""Bounded pass recovery through the real OpenAI SDK; synthetic HTTP only."""

import asyncio
from collections import Counter
import json
import unittest
from unittest.mock import patch

import httpx
from openai import AsyncOpenAI

from tests.runtime import configure_test_environment

configure_test_environment()

from app.extractor import EntityExtractor, METADATA_EXTRACTION_PROMPTS
from tests.test_extraction import CompletionClient

STAGES = ("metadata", "entities", "entity_review", "relationships", "relationship_review")
SOURCE = "Alice Example works at Tail Widgets. Premium: 125 USD."


class RecoveryClock:
    def __init__(self):
        self.now = 0
        self.waits = []

    async def sleep(self, seconds):
        self.waits.append(seconds)
        self.now += seconds


class SDKWire:
    """A request-keyed cache can replay bad output; corrections must change its key."""

    def __init__(self, adapter=None, *, cached=False, faults=None, contents=None,
                 clock=None, unavailable_until=0):
        self.adapter = adapter or CompletionClient()
        self.requests = []
        self.timeouts = []
        self.cached = cached
        self.cache = {}
        self.cache_hits = 0
        self.faults = faults or {}
        self.contents = contents or {}
        self.clock = clock
        self.unavailable_until = unavailable_until
        self.request_times = []

    async def handle(self, request):
        assert request.url.host == "127.0.0.1"
        payload = json.loads(request.content)
        self.requests.append(payload)
        self.timeouts.append(request.extensions["timeout"])
        if self.clock is not None:
            self.request_times.append(self.clock.now)
            if self.clock.now < self.unavailable_until:
                raise httpx.ConnectError("synthetic-private-provider-body", request=request)
        fault = self.faults.get(len(self.requests))
        if fault == "connection":
            raise httpx.ConnectError("synthetic-private-provider-body", request=request)
        if fault == "timeout":
            raise httpx.ReadTimeout("synthetic-private-provider-body", request=request)
        if fault == "cancel":
            raise asyncio.CancelledError()
        if fault == "http_error":
            return httpx.Response(500, json={"error": {"message": "synthetic-private-provider-body"}})
        if type(fault) is int:
            return httpx.Response(fault, json={"error": {"message": "synthetic-private-provider-body connection unavailable"}})
        key = json.dumps(payload, sort_keys=True)
        if self.cached and key in self.cache:
            self.cache_hits += 1
            body = self.cache[key]
        else:
            result = await self.adapter.create(**payload)
            choice = result.choices[0]
            body = {
                "id": "synthetic-completion", "object": "chat.completion", "created": 0,
                "model": payload["model"],
                "choices": [{"index": 0, "finish_reason": choice.finish_reason,
                             "message": {"role": "assistant", "content": choice.message.content}}],
                "usage": {"prompt_tokens": 100, "completion_tokens": 6501, "total_tokens": 6601},
            }
            if len(self.requests) in self.contents:
                body["choices"][0]["message"]["content"] = self.contents[len(self.requests)]
            if self.cached:
                self.cache[key] = body
        return httpx.Response(200, json=body)

    def client(self, **kwargs):
        return AsyncOpenAI(
            **{"base_url": "http://127.0.0.1:1/v1", "api_key": "synthetic-test-key",
               "max_retries": 0, **kwargs},
            http_client=httpx.AsyncClient(transport=httpx.MockTransport(self.handle)),
        )

    def counts(self):
        return Counter(stage for stage, _, _ in self.adapter.calls)


def transient_bad(stage, bad, attempts=1):
    def override(good, adapter):
        count = sum(call_stage == stage for call_stage, _, _ in adapter.calls)
        return bad if count <= attempts else good
    return CompletionClient({stage: override})


class ExtractionRecoveryTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.clock = RecoveryClock()
        sleeper = patch("asyncio.sleep", new=self.clock.sleep)
        sleeper.start()
        self.addCleanup(sleeper.stop)

    async def extract(self, wire, source=SOURCE, **options):
        async with wire.client() as client:
            return await EntityExtractor(client, **options).extract("synthetic-private-title", source, "property_home")

    async def test_actual_sdk_retries_flat_metadata_then_accepts_valid_envelope(self):
        wire = SDKWire(transient_bad("metadata", {"amount": "125"}), cached=True)
        result = await self.extract(wire)
        self.assertEqual(result["extraction_coverage"]["status"], "complete")
        self.assertEqual(result["premium"], "125")
        self.assertEqual(wire.counts(), Counter({**dict.fromkeys(STAGES, 1), "metadata": 2}))
        self.assertEqual(wire.cache_hits, 0)

    async def test_all_five_wrong_envelopes_recover_on_second_or_third_attempt_only(self):
        for stage in STAGES:
            key = "metadata" if stage == "metadata" else ("entities" if stage in STAGES[1:3] else "relationships")
            for attempts in (1, 2):
                with self.subTest(stage=stage, attempts=attempts):
                    wire = SDKWire(transient_bad(stage, {key: ["scalar-row"]}, attempts), cached=True)
                    result = await self.extract(wire)
                    self.assertEqual(result["extraction_coverage"]["status"], "complete")
                    self.assertEqual(wire.counts(), Counter({**dict.fromkeys(STAGES, 1), stage: attempts + 1}))
                    self.assertEqual(len(wire.requests), 5 + attempts)
                    self.assertEqual(len(result["implied_relationships"]), 1)
                    self.assertEqual(wire.cache_hits, 0)

    async def test_persistent_invalid_envelopes_fail_closed_after_exactly_three_attempts(self):
        for stage in STAGES:
            key = "metadata" if stage == "metadata" else ("entities" if stage in STAGES[1:3] else "relationships")
            bad_responses = [{}, [], ["scalar"], None, {key: "not-a-list"}, {key: ["scalar-row"]}]
            if stage == "metadata":
                bad_responses += [{"metadata": {}}, {"evidence": []}, {"metadata": {}, "evidence": ["scalar"]}]
            for bad in bad_responses:
                with self.subTest(stage=stage, bad=bad):
                    wire = SDKWire(CompletionClient({stage: bad}), cached=True)
                    result = await self.extract(wire)
                    self.assertEqual(result["extraction_coverage"]["status"], "failed")
                    self.assertEqual(result["extraction_coverage"]["covered_characters"], 0)
                    self.assertEqual(result["extraction_coverage"]["adaptive_splits"], 0)
                    self.assertEqual(result["all_entities"], [])
                    self.assertEqual(result["implied_relationships"], [])
                    self.assertEqual(result["metadata_evidence"], {"0": {}})
                    self.assertNotIn("premium", result, "Failed-window metadata must not leak")
                    self.assertEqual(wire.counts()[stage], 3)
                    self.assertEqual(len(wire.requests), STAGES.index(stage) + 3)

    async def test_json_parser_does_not_invent_envelopes_values_or_close_truncated_objects(self):
        invalid = [
            '[{"metadata": {}, "evidence": []}]',
            '{"metadata": {}, "evidence": []',
            'prefix {"metadata": {}, "evidence": []} suffix',
            '{"metadata": {}, "evidence": [],}',
            "{'metadata': {}, 'evidence': []}",
            '',
        ]
        for raw in invalid:
            with self.subTest(raw=raw):
                wire = SDKWire(contents=dict.fromkeys((1, 2, 3), raw))
                result = await self.extract(wire)
                self.assertEqual(result["extraction_coverage"]["status"], "failed")
                self.assertEqual(len(wire.requests), 3)
                self.assertEqual(result["extraction_coverage"]["adaptive_splits"], 0)
        wire = SDKWire(contents={1: '```json\n{"metadata": {}, "evidence": []}\n```'})
        result = await self.extract(wire)
        self.assertEqual(result["extraction_coverage"]["status"], "complete")
        self.assertEqual(len(wire.requests), 5)

    async def test_good_neighbors_and_completed_prefix_are_not_repeated(self):
        chunks = [(label + " " + SOURCE).ljust(100, ".") for label in ("good-left", "bad-middle", "good-right")]
        def fail_middle(good, adapter):
            return {} if adapter.source.startswith("bad-middle") else good
        wire = SDKWire(CompletionClient({"relationship_review": fail_middle}))
        result = await self.extract(wire, "".join(chunks), window_characters=100, overlap_characters=0)
        coverage = result["extraction_coverage"]
        self.assertEqual(coverage["status"], "partial")
        self.assertEqual(coverage["covered_characters"], 200)
        self.assertEqual([w["status"] for w in coverage["windows"]], ["complete", "failed", "complete"])
        for chunk in chunks:
            counts = Counter(stage for stage, source, _ in wire.adapter.calls if source == chunk)
            self.assertEqual(counts, Counter({**dict.fromkeys(STAGES, 1),
                                              "relationship_review": 3 if chunk == chunks[1] else 1}))
        self.assertEqual(result["metadata_evidence"]["1"], {})
        for entity in result["all_entities"]:
            for span in entity["evidence"]:
                self.assertFalse(100 <= span["start"] < 200)
                self.assertEqual("".join(chunks)[span["start"]:span["end"]], span["quote"])

    async def test_valid_empty_entities_are_not_malformed_and_skip_unneeded_reviews(self):
        wire = SDKWire(CompletionClient({"entities": {"entities": []}}))
        result = await self.extract(wire)
        self.assertEqual(result["extraction_coverage"]["status"], "complete")
        self.assertEqual(result["all_entities"], [])
        self.assertEqual(wire.counts(), Counter(metadata=1, entities=1))

    async def test_cache_recovery_uses_only_deterministic_contract_corrections(self):
        async def run():
            wire = SDKWire(transient_bad("metadata", {"synthetic-private-bad-output": "do something else"}, 2), cached=True)
            result = await self.extract(wire)
            self.assertEqual(result["extraction_coverage"]["status"], "complete")
            self.assertEqual(wire.cache_hits, 0)
            return wire
        first, second = await run(), await run()
        self.assertEqual(first.requests, second.requests, "No random request nonce or timestamp")
        attempts = first.requests[:3]
        self.assertEqual(len({json.dumps(r["messages"], sort_keys=True) for r in attempts}), 3)
        self.assertEqual(len({r["messages"][1]["content"] for r in attempts}), 1, "Do not change source or proposal context")
        for index, request in enumerate(attempts):
            system = request["messages"][0]["content"]
            self.assertNotIn("synthetic-private", system)
            self.assertNotIn("do something else", system)
            if index:
                self.assertIn(f"Retry {index + 1}/3", system)
                self.assertIn('"metadata": {}, "evidence": []', system)
        for request in first.requests:
            self.assertTrue({"max_tokens", "max_completion_tokens", "max_output_tokens"}.isdisjoint(request))
            self.assertEqual(request["response_format"], {"type": "json_object"})

    async def test_default_constructor_uses_sdk_finite_timeout_and_disables_sdk_retries(self):
        wire = SDKWire()
        with patch("app.extractor.AsyncOpenAI", side_effect=wire.client) as factory:
            extractor = EntityExtractor()
        try:
            self.assertEqual(factory.call_args.kwargs["max_retries"], 0)
            self.assertNotIn("timeout", factory.call_args.kwargs)
            self.assertEqual(extractor.client.max_retries, 0)
            self.assertEqual(extractor.client.timeout.as_dict(), {"connect": 5, "read": 600, "write": 600, "pool": 600})
            result = await extractor.extract("Synthetic", SOURCE, "property_home")
            self.assertEqual(result["extraction_coverage"]["status"], "complete")
            self.assertTrue(all(timeout == {"connect": 5, "read": 600, "write": 600, "pool": 600} for timeout in wire.timeouts))
        finally:
            await extractor.close()

    async def test_timeout_http_error_and_bad_shape_share_the_same_three_attempt_budget(self):
        for fault in ("timeout", "http_error"):
            with self.subTest(fault=fault):
                self.clock.waits.clear()
                wire = SDKWire(transient_bad("metadata", {}), faults={1: fault})
                with self.assertLogs("app.extractor", level="INFO") as logs:
                    result = await self.extract(wire)
                self.assertEqual(result["extraction_coverage"]["status"], "complete")
                self.assertEqual(len(wire.requests), 7)  # 3 metadata + 4 later passes
                self.assertEqual(self.clock.waits, [20])
                self.assertNotIn("synthetic-private", "\n".join(logs.output))
        self.clock.waits.clear()
        wire = SDKWire(faults={1: "timeout", 2: "timeout", 3: "timeout"})
        result = await self.extract(wire)
        self.assertEqual(len(wire.requests), 3)
        self.assertEqual(result["extraction_coverage"]["status"], "failed")
        self.assertEqual(self.clock.waits, [20, 40])

    async def test_proxy_recycle_recovers_within_existing_three_attempts(self):
        wire = SDKWire(clock=self.clock, unavailable_until=55)
        with self.assertLogs("app.extractor", level="INFO") as logs:
            result = await self.extract(wire)
        self.assertEqual(result["extraction_coverage"]["status"], "complete")
        self.assertEqual(result["premium"], "125")
        self.assertEqual(len(wire.requests), 7)  # Three metadata attempts, four later passes.
        self.assertEqual(wire.request_times[:3], [0, 20, 60])
        self.assertEqual(self.clock.waits, [20, 40])
        self.assertNotIn("synthetic-private", "\n".join(logs.output))

    async def test_transient_failure_budget_has_no_final_wait(self):
        wire = SDKWire(clock=self.clock, unavailable_until=600)
        result = await self.extract(wire)
        self.assertEqual(result["extraction_coverage"]["status"], "failed")
        self.assertEqual(result["all_entities"], [])
        self.assertEqual(result["implied_relationships"], [])
        self.assertEqual(wire.request_times, [0, 20, 60])
        self.assertEqual(self.clock.waits, [20, 40])

    async def test_transient_http_errors_wait_but_permanent_errors_do_not(self):
        for status in (429, 500, 502, 503, 504, 400, 401, 403, 404):
            with self.subTest(status=status):
                self.clock.waits.clear()
                wire = SDKWire(faults={1: status, 2: status, 3: status})
                result = await self.extract(wire)
                self.assertEqual(result["extraction_coverage"]["status"], "failed")
                self.assertEqual(len(wire.requests), 3)
                self.assertEqual(self.clock.waits, [20, 40] if status >= 429 else [])

    async def test_cancellation_during_recovery_wait_prevents_more_requests(self):
        started = asyncio.Event()

        async def wait_for_cancellation(seconds):
            self.clock.waits.append(seconds)
            started.set()
            await asyncio.Event().wait()

        wire = SDKWire(faults={1: "connection"})
        with patch("asyncio.sleep", new=wait_for_cancellation):
            task = asyncio.create_task(self.extract(wire))
            try:
                await asyncio.wait_for(started.wait(), 1)
                task.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await task
            finally:
                if not task.done():
                    task.cancel()
                await asyncio.gather(task, return_exceptions=True)
        self.assertEqual(len(wire.requests), 1)
        self.assertEqual(self.clock.waits, [20])

    async def test_truncation_after_malformed_response_splits_immediately_without_parent_provenance(self):
        wire = SDKWire(transient_bad("relationship_review", {}))
        wire.adapter.truncate_stage = "relationship_review"
        # Only the second parent attempt is genuinely truncated.
        original = wire.adapter.overrides["relationship_review"]
        def truncate(good, adapter):
            adapter.finish_reason = "length" if len(adapter.source) > 350 and wire.counts()["relationship_review"] > 1 else "stop"
            return original(good, adapter)
        wire.adapter.overrides["relationship_review"] = truncate
        def reset_window(good, adapter):
            adapter.finish_reason = "stop"
            return good
        wire.adapter.overrides["metadata"] = reset_window
        source = SOURCE + "x" * 500
        result = await self.extract(wire, source, window_characters=len(source), overlap_characters=50, minimum_split_characters=200)
        coverage = result["extraction_coverage"]
        self.assertEqual(coverage["status"], "complete")
        self.assertEqual(coverage["adaptive_splits"], 1)
        self.assertEqual(self.clock.waits, [])
        parent = Counter(stage for stage, text, _ in wire.adapter.calls if text == source)
        self.assertEqual(parent, Counter({**dict.fromkeys(STAGES, 1), "relationship_review": 2}))
        self.assertEqual(len(result["metadata_evidence"]), len(coverage["windows"]))
        for rel in result["implied_relationships"]:
            for span in rel["evidence"]:
                self.assertEqual(source[span["start"]:span["end"]], span["quote"])

    async def test_unknown_relationship_support_after_recovery_is_still_rejected(self):
        def review(good, adapter):
            if sum(stage == "relationship_review" for stage, _, _ in adapter.calls) == 1:
                return {}
            good["relationships"][0]["support_status"] = "unknown"
            return good
        result = await self.extract(SDKWire(CompletionClient({"relationship_review": review})))
        self.assertEqual(result["extraction_coverage"]["status"], "complete")
        self.assertEqual(result["implied_relationships"], [])
        self.assertTrue(any("support was not established" in issue for issue in result["extraction_issues"]))

    async def test_complete_window_progress_is_positive_and_contains_no_private_values(self):
        wire = SDKWire()
        source = ("left " + SOURCE).ljust(100, ".") + ("right " + SOURCE).ljust(100, ".")
        with self.assertLogs("app.extractor", level="INFO") as logs:
            result = await self.extract(wire, source, window_characters=100, overlap_characters=0)
        self.assertEqual(result["extraction_coverage"]["status"], "complete")
        self.assertEqual([record.getMessage() for record in logs.records], [
            "Extraction window 0:100 accepted; phase=complete completed_windows=1 pending_windows=1",
            "Extraction window 100:200 accepted; phase=complete completed_windows=2 pending_windows=0",
        ])
        for private in ("Alice", "Tail Widgets", "125", "synthetic-private-title", source):
            self.assertNotIn(private, "\n".join(logs.output))

    async def test_cancellation_is_not_retried(self):
        wire = SDKWire(faults={1: "cancel"})
        with self.assertRaises(asyncio.CancelledError):
            await self.extract(wire)
        self.assertEqual(len(wire.requests), 1)

    async def test_metadata_prompts_describe_one_envelope_before_source_for_every_type(self):
        for doc_type in (*METADATA_EXTRACTION_PROMPTS, "generic"):
            wire = SDKWire()
            async with wire.client() as client:
                await EntityExtractor(client)._pass1_metadata_extraction("Synthetic", SOURCE, doc_type)
            prompt = wire.requests[0]["messages"][1]["content"]
            self.assertIn('"metadata": {}, "evidence": []', wire.requests[0]["messages"][0]["content"])
            self.assertTrue(prompt.startswith("SOURCE-BOUND OUTPUT CONTRACT"))
            self.assertIn("metadata member (not the top-level response)", prompt)
            self.assertNotIn("overrides earlier output shape", prompt)
            self.assertTrue(prompt.endswith(SOURCE))
            self.assertEqual(prompt.count("Return exactly one JSON object"), 1)

    async def test_prompt_examples_are_valid_json_after_single_formatting(self):
        wire = SDKWire()
        await self.extract(wire)
        for stage, _, prompt in wire.adapter.calls:
            if stage == "entities":
                example = prompt.split("Return a JSON object:\n", 1)[1]
            elif stage == "relationships":
                example = prompt.split("Return a JSON object with:\n", 1)[1]
            elif stage == "entity_review":
                example = prompt.split("Return a JSON object with ONLY the validated entities (remove all junk, correct wrong types):\n", 1)[1]
            else:
                continue
            self.assertIsInstance(json.JSONDecoder().raw_decode(example)[0], dict)


if __name__ == "__main__":
    unittest.main()
