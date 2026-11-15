"""The accepted candidate and source dates travel only with delivered answers."""
import copy
import hashlib
import json
import unittest
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from tests.runtime import configure_test_environment
configure_test_environment()
from app import conversations
from app.answer_coverage import ledger_digest, restore_pipeline_metadata
from app.answer_delivery import render_verified_answer
from app.answer_finalization import AnswerFinalizer
from app.cache import invalidate_on_sync
from app.graph import graph_store, source_dates
from tests.test_answer_finalization import PACK, SupportedAuditor
from tests.test_partial_answers import MixedAuditor, SOURCE
from tests.test_query_delivery import RetrievedEngine
from tests.test_question_pipeline import QuestionPipelineTests
from tests.test_source_dates import pack

UNAUDITED = "Unaudited quick answer — verify the source documents before relying on it.\n\n"
REFERENCE_KEYS = {'content_digest', 'date_context_before', 'document_id', 'end', 'evidence_id', 'quote',
                  'source_field_leaders', 'source_list_markers', 'source_quantity_tables', 'source_title',
                  'span_id', 'start'}


async def indexed_dates(ids):
    return {doc_id: "2026-01-15" for doc_id in ids if doc_id == 101}


def sha(text):
    return hashlib.sha256(text.encode()).hexdigest()


class Historical(SupportedAuditor):
    async def audit_answer_units(self, *args):
        raw = await super().audit_answer_units(*args)
        for assessment in raw["assessments"]:
            assessment["temporal_scope"] = "historical"
        return raw


class Unsupported:
    async def audit_answer_units(self, question, units, spans, plan):
        return {"assessments": [{"unit_id": u["id"], "status": "unsupported", "references": []} for u in units]}


class Broken:
    async def audit_answer_units(self, *args):
        raise RuntimeError("provider failed")


class Slow:
    async def audit_answer_units(self, *args):
        import asyncio
        await asyncio.sleep(10)


class FinalizerCandidateTextTests(unittest.IsolatedAsyncioTestCase):
    async def finalize(self, auditor, answer, source=PACK, *, current=False, mode="strict", timeout=None):
        finalizer = AnswerFinalizer(auditor, **({"timeout_seconds": timeout} if timeout else {}))
        return await finalizer.finalize("Recorded premium?", answer, copy.deepcopy(source),
                                        plan={"requires_current": current}, mode=mode, evaluated_at="2026-09-04")

    def assert_bound(self, result):
        ledger, final = result["claim_ledger"], result["finalization"]
        text = ledger["candidate_text"]
        self.assertEqual(sha(text), ledger["candidate_digest"])
        self.assertEqual(sha(text), final["candidate_digest"])
        self.assertNotIn("[Document", text)
        without = {k: v for k, v in ledger.items() if k != "candidate_text"}
        self.assertEqual(ledger_digest(ledger), ledger_digest(without))
        for claim in ledger["claims"]:
            self.assertEqual(text[claim["start"]:claim["end"]], claim["claim"])
            for reference in claim["references"]:
                self.assertEqual(set(reference), REFERENCE_KEYS)
        return text

    async def test_verified_dispositions_carry_the_candidate_that_renders_the_answer(self):
        cases = [("supported", SupportedAuditor(), "The recorded monthly premium is $321.00 USD.", PACK, False),
                 ("qualified", Historical(), "The recorded monthly premium was $321.00 USD.", PACK, True),
                 ("partial", MixedAuditor(), "The invoice records a $321 USD service charge.\nAn extra charge is $999 USD.",
                  pack(SOURCE), False)]
        for disposition, auditor, answer, source, current in cases:
            for mode in ("strict", "deep", "timeline"):  # Quick prose is unaudited; tested below.
                with self.subTest(disposition=disposition, mode=mode):
                    result = await self.finalize(auditor, answer, source, current=current, mode=mode)
                    final = result["finalization"]
                    self.assertEqual(final["disposition"], disposition)
                    text = self.assert_bound(result)
                    self.assertEqual(result["answer"], render_verified_answer(
                        text, result["claim_ledger"]["claims"], partial=disposition == "partial",
                        qualified=final["documented_qualification"], evaluated_at=final["evaluated_at"]))
                    self.assertNotIn("Partial answer:", text)
                    self.assertNotIn("These are documented facts", text)
        partial = await self.finalize(MixedAuditor(), cases[2][2], pack(SOURCE))
        self.assertNotIn("999", partial["claim_ledger"]["candidate_text"])

    async def test_unaudited_quick_answer_carries_candidate_without_prefix_or_links(self):
        result = await self.finalize(None, "The premium is $321.00 USD [Document 101](/documents/101).", mode="quick")
        self.assertEqual(result["finalization"]["disposition"], "unaudited")
        text = self.assert_bound(result)
        self.assertEqual(result["answer"], UNAUDITED + text)
        self.assertFalse(text.startswith("Unaudited"))
        for answer, source in (("The premium is $321.00 USD.", {"items": []}), ("", PACK)):
            empty = await self.finalize(None, answer, source, mode="quick")
            self.assertEqual(empty["finalization"]["disposition"], "unaudited")
            self.assertNotIn("candidate_text", json.dumps(empty))

    async def test_withheld_outcomes_never_carry_candidate_text(self):
        cases = [("unsupported", Unsupported(), PACK, False, None),
                 ("incomplete", SupportedAuditor(), {"items": []}, False, None),
                 ("audit_failed", Broken(), PACK, False, None),
                 ("timeout", Slow(), PACK, False, 0.01),
                 ("current_unresolved", SupportedAuditor(), PACK, True, None)]
        for disposition, auditor, source, current, timeout in cases:
            for mode in ("strict", "timeline"):
                with self.subTest(disposition=disposition, mode=mode):
                    answer = "My current premium is $321.00 USD." if current else "The premium is $321.00 USD."
                    result = await self.finalize(auditor, answer, source, current=current, mode=mode, timeout=timeout)
                    self.assertEqual(result["finalization"]["disposition"], disposition)
                    self.assertNotIn("candidate_text", json.dumps(result))


class DeliveredCandidateTextTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        invalidate_on_sync()
        self.engine = RetrievedEngine()
        self.patches = [patch("app.query.strands_orchestrator", SupportedAuditor()),
                        patch("app.query.embeddings_store.get_incomplete_document_ids", AsyncMock(return_value=set())),
                        patch("app.query.embeddings_store.get_open_feedback_document_ids", AsyncMock(return_value=set())),
                        patch.object(graph_store, "get_document_dates", AsyncMock(side_effect=indexed_dates))]
        for p in self.patches:
            p.start()

    async def asyncTearDown(self):
        for p in self.patches:
            p.stop()

    async def test_every_mode_delivers_candidate_and_source_dates_through_both_paths(self):
        for mode in ("quick", "strict", "deep", "timeline"):
            with self.subTest(mode=mode):
                invalidate_on_sync()
                ordinary = await self.engine.query("Recorded premium?", mode=mode)
                streamed = [e async for e in self.engine.query_stream("Recorded premium?", mode=mode)][-1]
                for result in (ordinary, streamed):
                    expected = "unaudited" if mode == "quick" else "supported"
                    self.assertEqual(result["finalization"]["disposition"], expected)
                    self.assertEqual(result["claim_ledger"]["candidate_text"], "Monthly premium: $321.00 USD.")
                    # The map follows `sources`; this fixture's Quick answer lists none.
                    self.assertEqual([s["document_id"] for s in result["sources"]], [] if mode == "quick" else [101])
                    self.assertEqual(result["source_dates"], {} if mode == "quick" else {"101": "2026-01-15"})
                cached = await self.engine.query("Recorded premium?", mode=mode)
                self.assertTrue(cached["cached"])
                self.assertEqual(cached["claim_ledger"]["candidate_text"], "Monthly premium: $321.00 USD.")
                self.assertEqual(cached["source_dates"], ordinary["source_dates"])

    async def test_corpus_change_rejection_carries_no_candidate_or_dates(self):
        for mode in ("quick", "strict", "timeline"):
            for cached_first in (False, True):
                with self.subTest(mode=mode, cached_first=cached_first):
                    invalidate_on_sync()
                    if cached_first:
                        first = await self.engine.query("Recorded premium?", mode=mode)
                        self.assertIn("candidate_text", first["claim_ledger"])
                    with patch("app.query.embeddings_store.get_incomplete_document_ids", AsyncMock(return_value={101})):
                        result = await self.engine.query("Recorded premium?", mode=mode)
                    self.assertEqual(result["finalization"]["disposition"], "corpus_changed")
                    self.assertEqual(result["cached"], cached_first)
                    self.assertNotIn("candidate_text", json.dumps(result))
                    self.assertEqual(result["source_dates"], {})


class SourceDateLookupTests(unittest.IsolatedAsyncioTestCase):
    async def test_only_calendar_dates_are_returned_and_failures_omit_dates(self):
        stored = {101: "2026-01-15", 102: "", 103: "March 2026", 104: "2026-02-30", 105: "2026-01-15T08:00:00"}
        with patch.object(graph_store, "get_document_dates", AsyncMock(return_value=stored)) as lookup:
            self.assertEqual(await source_dates(graph_store, [101, 102, 103, 104, 105, 101]),
                             {"101": "2026-01-15", "105": "2026-01-15"})
        lookup.assert_awaited_once_with([101, 102, 103, 104, 105])
        with patch.object(graph_store, "get_document_dates", AsyncMock(side_effect=RuntimeError("down"))):
            self.assertEqual(await source_dates(graph_store, [101]), {})


class SavedAnswerTests(unittest.IsolatedAsyncioTestCase):
    asyncSetUp = QuestionPipelineTests.asyncSetUp
    asyncTearDown = QuestionPipelineTests.asyncTearDown
    model = QuestionPipelineTests.model

    async def reopen(self, metadata_rows, dates):
        now = datetime.now(timezone.utc)
        rows = [{'id': uuid.uuid4(), 'role': 'assistant', 'content': metadata['answer'], 'sources': None,
                 'entities': None, 'confidence': .8, 'query_time_ms': 10, 'cached': False,
                 'follow_ups': None, 'metadata': json.dumps(metadata), 'created_at': now} for metadata in metadata_rows]
        connection = SimpleNamespace(fetchrow=AsyncMock(return_value={
            'id': uuid.uuid4(), 'title': 'Saved query', 'created_at': now, 'updated_at': now}),
            fetch=AsyncMock(return_value=rows))
        @asynccontextmanager
        async def acquire():
            yield connection
        with patch.object(conversations, '_pool', SimpleNamespace(acquire=acquire)), \
                patch.object(graph_store, 'get_document_dates',
                             AsyncMock(side_effect=lambda ids: {i: d for i, d in dates.items() if i in ids})):
            return (await conversations.get_conversation(str(uuid.uuid4())))['messages']

    async def test_answer_saved_before_and_after_this_change_reopens_verified(self):
        result = await self.engine.query('What monthly premium is recorded?', mode='quick')
        self.assertIn('candidate_text', result['claim_ledger'])
        before = copy.deepcopy(result)
        del before['claim_ledger']['candidate_text']  # The shape saved before this change.
        changed = copy.deepcopy(result)
        changed['finalization'].pop('question_coverage')  # Fails saved-message validation.
        old, new, invalid = await self.reopen([before, result, changed], {101: '2026-01-15'})
        for message in (old, new):
            self.assertEqual(message['finalization']['disposition'], 'supported')
            self.assertTrue(message['finalization']['answer_verified'])
            self.assertTrue(message['finalization']['question_coverage']['complete'])
            self.assertEqual(message['source_dates'], {'101': '2026-01-15'})
        self.assertNotIn('candidate_text', old['claim_ledger'])
        self.assertEqual(new['claim_ledger']['candidate_text'], result['claim_ledger']['candidate_text'])
        self.assertEqual(invalid['finalization']['disposition'], 'stored_binding_unavailable')
        self.assertNotIn('candidate_text', json.dumps(invalid))
        self.assertEqual(invalid['source_dates'], {})
        undated, = await self.reopen([result], {101: ''})
        self.assertEqual(undated['source_dates'], {})
        self.assertNotIn('candidate_text', json.dumps(restore_pipeline_metadata(changed, changed['answer'])))
