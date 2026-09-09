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
        self.editor_text = json.dumps({"observations": [QUOTE]})
        self.editor_delay = 0
        self.editor_error = False
        self.editor_reject_schema = False
        self.editor_truncated = False
        self.editor_started = asyncio.Event()
        self.audit_count = 0
        self.audit_override = None
        self.reject_first_audit = False
        self.reject_all_audits = False
        self.quote = QUOTE
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
            is_editor = message.startswith("Repair this answer")
            if is_editor:
                self.editor_started.set()
                await asyncio.sleep(self.editor_delay)
                if self.editor_reject_schema and 'response_format' in body:
                    return httpx.Response(400, json={'error': {'message': 'Synthetic unsupported response format',
                                                             'type': 'invalid_request_error'}})
                if self.editor_error:
                    raise httpx.ConnectError("synthetic provider unavailable")
            result = {"ok": True}
            if "units" in payload:
                self.audit_count += 1
                span = payload["source_spans"][0]
                result = {"assessments": [{"unit_id": unit["id"], "status": "unsupported" if self.reject_all_audits or (self.reject_first_audit and self.audit_count == 1) else "supported",
                    "references": [{"span_id": span["span_id"]}]} for unit in payload["units"]]}
            # Simulate a provider completion that exceeds the former 6,000-token
            # allowance. Genuine provider truncation must remain a failed audit.
            truncated = self.force_length or (is_editor and self.editor_truncated) or bool(LIMIT_FIELDS.intersection(body))
            content = self.editor_text if is_editor else self.audit_override if self.audit_override is not None and "units" in payload and self.audit_count == 1 else '{"assessments":[' if truncated else json.dumps(result)
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

    async def test_nonempty_falsey_json_uses_existing_protocol_correction_through_real_sdk(self):
        for raw in ('{}', '[]', 'null', 'false', '0', '""'):
            self.audit_override, self.audit_count = raw, 0
            result = await AnswerFinalizer(self.orchestrator).finalize('What is recorded?', QUOTE, PACK)
            self.assertTrue(result['finalization']['answer_verified'], raw)
            self.assertEqual(self.audit_count, 2)
            self.assertEqual(result['claim_ledger']['audit_batches'][0]['status'], 'corrected')
            self.assertTrue(all(LIMIT_FIELDS.isdisjoint(request) for request in self.requests))

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
        expected_context = expected_context.replace("\n", "\n\n", 1)
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
            (lambda: self.orchestrator.repair_answer("Synthetic question?", "Synthetic answer", "Synthetic evidence", {}), {"observations": [QUOTE]}),
            (lambda: self.orchestrator.review_entity_candidate({}, {}), {"ok": True})]
        for call, expected in calls:
            with self.subTest(helper=len(self.requests)):
                result = await call()
                self.assertTrue(LIMIT_FIELDS.isdisjoint(self.requests[-1]), self.requests[-1].keys())
                self.assertEqual(result, expected)
        self.assertEqual(len(self.requests), len(calls))

    async def test_native_sdk_audit_receives_actual_selection_opportunities(self):
        from tests.test_audit_evidence_opportunities import item
        source = 'Cedar service record dated January 1, 2026 lists an open account.'
        result = await AnswerFinalizer(self.orchestrator).finalize('What is the latest service record?',
            'The latest Cedar service record dated January 1, 2026 lists an open account.',
            {'items': [item(doc, 0, source + f' Record {doc}.') for doc in range(1, 36)]})
        coverage = result['claim_ledger']['selection_coverage'][0]
        self.assertTrue(coverage['comparison_opportunities'][0]['omitted_document_ids'])
        for request in self.requests:
            message = request['messages'][-1]['content']
            if isinstance(message, list):
                message = ''.join(block.get('text', '') for block in message)
            payload = json.loads(message)
            expected = {key: coverage[key] for key in ('comparison_opportunities', 'date_opportunities')}
            self.assertEqual(payload['evidence_selection'], expected)
            self.assertNotIn(source, json.dumps(payload['evidence_selection']))
            self.assertLessEqual(len(json.dumps(payload['source_spans'], ensure_ascii=False)), 28000)
            self.assertTrue(LIMIT_FIELDS.isdisjoint(request))

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

    async def test_editor_observations_are_fully_reaudited_through_real_adapter(self):
        self.reject_first_audit = True
        self.quote = 'The label reads "Orchid" and the path is A/B.'
        self.editor_text = json.dumps({'observations': [self.quote]})
        source = {'items': [{**PACK['items'][0], 'content': self.quote, 'source_content': self.quote}]}
        result = await AnswerFinalizer(self.orchestrator, self.orchestrator).finalize('What is recorded?', 'An unsupported draft.', source)
        self.assertTrue(result['finalization']['answer_verified'])
        self.assertEqual(result['finalization']['attempts'], 2)
        self.assertIn(self.quote, result['answer'])
        self.assertEqual(result['claim_ledger']['unitization'], 'observations_v1')
        self.assertEqual(len(self.requests), 3)  # audit, exactly one editor, replacement audit
        self.assertEqual(self.audit_count, 2)
        self.assertTrue(all(LIMIT_FIELDS.isdisjoint(r) for r in self.requests))
        request = self.requests[-1]
        messages = request['messages']
        def text(message):
            content = message['content']
            return content if isinstance(content, str) else ''.join(block.get('text', '') for block in content)
        payload = json.loads(text(messages[-1]))
        self.assertEqual(payload['unitization'], 'observations_v1')
        self.assertEqual(payload['answer_context'], '')
        self.assertEqual(len(payload['units']), 1)
        self.assertIn('Do not use any sibling unit', text(messages[0]))
        self.assertNotIn('Answer context preserves surrounding headings', text(messages[0]))
        self.assertIn('references:[{span_id}]', text(messages[0]))

    async def test_editor_empty_exception_timeout_and_truncation_are_unavailable(self):
        for failure in ('empty', 'exception', 'timeout', 'truncation'):
            with self.subTest(failure=failure):
                self.editor_text = '' if failure == 'empty' else json.dumps({'observations': [QUOTE]})
                self.editor_error = failure == 'exception'
                self.editor_delay = 2 if failure == 'timeout' else 0
                self.editor_truncated = failure == 'truncation'
                self.reject_first_audit = True
                self.audit_count = 0
                count = len(self.requests)
                with patch.object(module.settings, 'strands_call_timeout_seconds', 1):
                    result = await AnswerFinalizer(self.orchestrator, self.orchestrator).finalize('What is recorded?', 'An unsupported draft.', PACK)
                self.assertEqual(result['finalization']['disposition'], 'audit_failed')
                self.assertFalse(result['finalization']['answer_verified'])
                self.assertEqual(self.audit_count, 1)
                self.assertEqual(len(self.requests) - count, 2)
                self.assertNotIn(QUOTE, result['answer'])
                self.assertEqual(result['finalization']['repair_diagnostic']['reason'], 'transport_unavailable')

    async def test_cancelled_editor_drains_transport_without_reaudit_or_cache(self):
        self.reject_first_audit = True
        self.editor_delay = 10
        engine = RetrievedEngine()
        invalidate_on_sync()
        with patch('app.query.strands_orchestrator', self.orchestrator), \
                patch.object(engine, '_build_evidence_pack', AsyncMock(return_value=PACK)), \
                patch.object(engine, '_final_synthesis', AsyncMock(return_value={'answer': 'An unsupported draft.'})), \
                patch('app.query.embeddings_store.get_incomplete_document_ids', AsyncMock(return_value=set())), \
                patch('app.query.embeddings_store.get_open_feedback_document_ids', AsyncMock(return_value=set())), \
                patch('app.query.cache_set', AsyncMock()) as cache:
            task = asyncio.create_task(engine.query('Cancelled edited answer?', mode='strict'))
            try:
                await asyncio.wait_for(self.editor_started.wait(), 2)
                task.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await task
            finally:
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
            cache.assert_not_awaited()
        self.assertEqual(self.audit_count, 1)
        self.assertEqual(len(self.requests), 2)
        self.assertTrue(all(client.is_closed for client in self.clients))

    async def test_unexpected_editor_format_and_refusal_still_require_source_audit(self):
        self.reject_all_audits = True
        for text in ('I cannot provide that answer.', '{"answer": "An unsupported fact."}',
                     '{"observations": []}', '{"observations": ["# Heading"]}'):
            self.editor_text = text
            count = self.audit_count
            result = await AnswerFinalizer(self.orchestrator, self.orchestrator).finalize('What is recorded?', 'An unsupported draft.', PACK)
            self.assertEqual(self.audit_count - count, 1)
            self.assertFalse(result['finalization']['answer_verified'])
            self.assertNotIn(text, result['answer'])

    async def test_editor_requires_complete_json_without_salvage_or_duplicate_keys(self):
        valid = json.dumps({'observations': [QUOTE]})
        for text in (valid + ' The observation is withdrawn.', 'Preface: ' + valid,
                     '```json\n' + valid + '\n```',
                     '{"observations":["A discarded assertion."],"observations":[' + json.dumps(QUOTE) + ']}',
                     valid + valid):
            self.editor_text, self.audit_count, self.reject_first_audit = text, 0, True
            before = len(self.requests)
            result = await AnswerFinalizer(self.orchestrator, self.orchestrator).finalize(
                'What is recorded?', 'An unsupported draft.', PACK)
            self.assertEqual(result['finalization']['disposition'], 'audit_failed')
            self.assertFalse(result['finalization']['answer_verified'])
            self.assertEqual(self.audit_count, 1)
            self.assertEqual(len(self.requests) - before, 2)

    async def test_editor_wire_schema_is_request_owned_and_other_helpers_stay_unchanged(self):
        self.reject_first_audit = True
        repaired, planned = await asyncio.gather(
            AnswerFinalizer(self.orchestrator, self.orchestrator).finalize('What is recorded?', 'An unsupported draft.', PACK),
            self.orchestrator.plan_query('Unrelated plan?', 'strict'))
        self.assertTrue(repaired['finalization']['answer_verified'])
        self.assertEqual(planned, {'ok': True})
        structured = [r for r in self.requests if 'response_format' in r]
        self.assertEqual(len(structured), 1)
        format = structured[0]['response_format']
        self.assertEqual(format['type'], 'json_schema')
        self.assertTrue(format['json_schema']['strict'])
        schema = format['json_schema']['schema']
        self.assertEqual(schema['required'], ['observations'])
        self.assertFalse(schema['additionalProperties'])
        self.assertEqual(schema['properties']['observations']['items']['type'], 'string')
        self.assertTrue(all(LIMIT_FIELDS.isdisjoint(r) for r in self.requests))

    async def test_repair_format_diagnostics_are_typed_and_do_not_contain_response_text(self):
        for raw, reason in (
            ('PRIVATE PREFIX {}', 'invalid_json'),
            ('{"observations":[' + '9' * 5000 + ']}', 'invalid_json'),
            ('{"observations":[],"observations":["PRIVATE"]}', 'duplicate_key'),
            ('[]', 'invalid_object'),
            ('{"observations":"PRIVATE"}', 'invalid_observations'),
            ('{"observations":[]}', 'empty_observations'),
            ('{"observations":[12]}', 'non_string_observation'),
            ('{"observations":[""]}', 'empty_observation'),
            ('{"observations":[" PRIVATE "]}', 'padded_observation'),
            ('{"observations":["PRIVATE\\nSECOND"]}', 'multiline_observation'),
            (json.dumps({'observations':[('PRIVATE ' * 200).strip()]}), 'oversized_observation'),
            ('{"observations":["**PRIVATE**"]}', 'formatted_observation'),
            ('', 'transport_unavailable'),
        ):
            self.editor_text, self.audit_count, self.reject_first_audit = raw, 0, True
            result = await AnswerFinalizer(self.orchestrator, self.orchestrator).finalize(
                'What is recorded?', 'An unsupported draft.', PACK)
            self.assertEqual(result['finalization']['disposition'], 'audit_failed')
            diagnostic = result['finalization']['repair_diagnostic']
            self.assertEqual(diagnostic['reason'], reason)
            self.assertNotIn('PRIVATE', json.dumps(diagnostic))
            self.assertEqual(self.audit_count, 1)

    async def test_disabled_editor_has_unavailable_diagnostic_without_transport(self):
        self.orchestrator.enabled = False
        result = await AnswerFinalizer(self.orchestrator, self.orchestrator).finalize(
            'What is recorded?', 'An unsupported draft.', PACK)
        self.assertEqual(result['finalization']['disposition'], 'audit_failed')
        self.assertEqual(result['finalization']['repair_diagnostic'], {'reason': 'transport_unavailable'})
        self.assertFalse(result['finalization']['answer_verified'])
        self.assertEqual(self.requests, [])

    async def test_valid_editor_over_audit_capacity_is_diagnosed_without_truncation(self):
        self.reject_first_audit = True
        self.editor_text = json.dumps({'observations': [QUOTE] * 81})
        result = await AnswerFinalizer(self.orchestrator, self.orchestrator).finalize(
            'What is recorded?', 'An unsupported draft.', PACK)
        self.assertEqual(result['finalization']['disposition'], 'incomplete')
        self.assertEqual(result['finalization']['repair_diagnostic'],
                         {'reason': 'audit_unit_limit', 'unit_count': 81, 'unit_limit': 80})
        self.assertFalse(result['finalization']['answer_verified'])
        self.assertEqual(result['claim_ledger']['summary']['total'], 81)
        self.assertEqual(result['claim_ledger']['summary']['audited'], 0)
        self.assertEqual(self.audit_count, 1)
        self.assertEqual(len(self.requests), 2)
        self.assertTrue(all(LIMIT_FIELDS.isdisjoint(r) for r in self.requests))

    async def test_unsupported_provider_schema_does_not_retry_without_constraints(self):
        self.editor_reject_schema = self.reject_first_audit = True
        result = await AnswerFinalizer(self.orchestrator, self.orchestrator).finalize(
            'What is recorded?', 'An unsupported draft.', PACK)
        self.assertEqual(result['finalization']['disposition'], 'audit_failed')
        self.assertEqual(result['finalization']['repair_diagnostic'], {'reason': 'transport_unavailable'})
        self.assertEqual(len(self.requests), 2)
        self.assertEqual(self.audit_count, 1)
