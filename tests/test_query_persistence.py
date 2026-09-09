"""HTTP/SSE delivery must persist the same terminal answer and audit metadata."""

import asyncio
import copy
import json
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import httpx

from tests.runtime import configure_test_environment

configure_test_environment()

from app import main


METADATA_KEYS = ("source_summary", "query_plan", "trace", "verification", "claim_ledger",
                 "evidence_pack", "timeline_events", "evidence", "current_state", "finalization", "mode")


def final_payload():
    return {
        "answer": "The amount due is 125.00 USD.", "sources": [{"document_id": 9101}],
        "entities_found": [], "confidence": 0.65, "follow_up_suggestions": [], "mode": "strict",
        "source_summary": {"source_count": 1}, "query_plan": {"domain": "financial"},
        "trace": [{"stage": "finalization", "status": "supported"}],
        "verification": {"status": "verified"}, "claim_ledger": {"claims": [{"claim": "125.00 USD"}]},
        "evidence_pack": {"items": [{"document_id": 9101}]}, "timeline_events": [],
        "evidence": {"audit_status": "supported", "score": 0.8},
        "current_state": {"required": False, "status": "not_required"},
        "finalization": {"disposition": "supported", "complete": True, "policy_version": "test-contract"},
    }


class TerminalEngine:
    def __init__(self, behavior="complete"):
        self.behavior = behavior
        self.started = asyncio.Event()
        self.release = asyncio.Event()

    async def query(self, *_args, **_kwargs):
        return copy.deepcopy(final_payload())

    async def query_stream(self, *_args, **_kwargs):
        self.started.set()
        yield {"type": "answer_chunk", "content": "UNVERIFIED DRAFT 999 USD"}
        if self.behavior == "disconnect":
            await self.release.wait()
        if self.behavior == "error":
            raise RuntimeError("Synthetic generation failure")
        payload = copy.deepcopy(final_payload())
        if self.behavior == "missing_answer":
            payload.pop("answer")
        yield {"type": "complete", **payload}


class SavedMessages:
    def __init__(self):
        self.messages = []
        self.saved_assistant = asyncio.Event()

    async def get_conversation_history(self, *_args):
        return []

    async def add_message(self, conversation_id, role, content, **metadata):
        self.messages.append({"conversation_id": conversation_id, "role": role, "content": content, **copy.deepcopy(metadata)})
        if role == "assistant":
            self.saved_assistant.set()


class QueryPersistenceTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.saved = SavedMessages()
        self.engine = TerminalEngine()
        self.storage_patch = patch.object(main, "conversations", self.saved)
        self.engine_patch = patch.object(main, "query_engine", self.engine)
        self.storage_patch.start()
        self.engine_patch.start()
        self.client = httpx.AsyncClient(transport=httpx.ASGITransport(app=main.app), base_url="http://synthetic.local")

    async def asyncTearDown(self):
        await self.client.aclose()
        self.engine_patch.stop()
        self.storage_patch.stop()

    async def test_ordinary_and_sse_persist_identical_complete_metadata(self):
        ordinary = await self.client.post("/query", json={"question": "Synthetic question", "conversation_id": "ordinary", "mode": "strict"})
        self.assertEqual(ordinary.status_code, 200, ordinary.text)
        stream = await self.client.post("/query/stream", json={"question": "Synthetic question", "conversation_id": "stream", "mode": "strict"})
        self.assertEqual(stream.status_code, 200, stream.text)
        events = [json.loads(line[6:]) for line in stream.text.splitlines() if line.startswith("data: ")]
        terminal = next(event for event in events if event["type"] == "complete")
        assistants = [message for message in self.saved.messages if message["role"] == "assistant"]
        self.assertEqual(len(assistants), 2)
        for message in assistants:
            self.assertEqual(message["content"], ordinary.json()["answer"])
            self.assertEqual(message["sources"], ordinary.json()["sources"])
            self.assertEqual(message["metadata"], {key: final_payload()[key] for key in METADATA_KEYS})
        self.assertEqual(terminal["answer"], ordinary.json()["answer"])
        self.assertEqual(assistants[0]["metadata"], assistants[1]["metadata"])

    async def test_partial_metadata_roundtrips_ordinary_and_stream(self):
        from app.answer_finalization import AnswerFinalizer
        from tests.test_partial_answers import MixedAuditor, SOURCE
        from tests.test_source_dates import pack
        result = await AnswerFinalizer(MixedAuditor()).finalize("Recorded charges?",
            "The invoice records a $321 USD service charge.\nAn extra charge is $999 USD.", pack(SOURCE))
        payload = {**final_payload(), **result}
        with patch(__name__ + ".final_payload", return_value=payload):
            await self.test_ordinary_and_sse_persist_identical_complete_metadata()
        for row in self.saved.messages:
            if row["role"] == "assistant":
                self.assertEqual(row["metadata"]["verification"]["partial"]["omitted_count"], 1)
                self.assertFalse(row["metadata"]["finalization"]["complete"])
                self.assertNotIn("999", row["content"])

    async def test_stream_error_does_not_persist_draft_as_assistant(self):
        self.engine.behavior = "error"
        response = await self.client.post("/query/stream", json={"question": "Synthetic question", "conversation_id": "error"})
        self.assertIn('"type": "error"', response.text)
        self.assertEqual([message["role"] for message in self.saved.messages], ["user"])

    async def test_atomic_observation_metadata_roundtrips_ordinary_and_stream(self):
        from app.answer_finalization import AnswerFinalizer
        from app.answer_observations import ObservationCandidate
        from tests.test_observation_delivery import HandleAuditor, ObservationRepairer
        from tests.test_source_dates import pack
        good = 'Cedar invoice records $20. The recipient is Casey.'
        result = await AnswerFinalizer(HandleAuditor(), ObservationRepairer(
            {'observations': [good, 'Maple invoice is REJECT.']})).finalize(
                'What is recorded?', 'REJECT.', pack(good))
        payload = {**final_payload(), **result}
        with patch(__name__ + '.final_payload', return_value=payload):
            await self.test_ordinary_and_sse_persist_identical_complete_metadata()
        expected = ObservationCandidate.from_text('- ' + good).units()
        for row in self.saved.messages:
            if row['role'] == 'assistant':
                ledger = row['metadata']['claim_ledger']
                self.assertEqual(ledger['unitization'], 'observations_v1')
                self.assertEqual([{**{key: c[key] for key in ('id', 'start', 'end')}, 'text': c['claim']}
                                  for c in ledger['claims']], expected)
                self.assertEqual(row['metadata']['verification']['partial']['omitted_count'], 1)

    async def test_repair_diagnostic_roundtrips_without_rejected_response_content(self):
        from app.answer_finalization import AnswerFinalizer
        from tests.test_observation_delivery import HandleAuditor, ObservationRepairer
        from tests.test_source_dates import pack
        result = await AnswerFinalizer(HandleAuditor(), ObservationRepairer(
            {'observations': ['**PRIVATE REPAIR CONTENT**']})).finalize(
                'What is recorded?', 'REJECT.', pack('Cedar invoice records $20.'))
        payload = {**final_payload(), **result}
        with patch(__name__ + '.final_payload', return_value=payload):
            await self.test_ordinary_and_sse_persist_identical_complete_metadata()
        for row in self.saved.messages:
            if row['role'] == 'assistant':
                self.assertEqual(row['metadata']['finalization']['repair_diagnostic'],
                                 {'reason': 'formatted_observation', 'item_index': 0})
                self.assertNotIn('PRIVATE REPAIR CONTENT', json.dumps(row))

    async def test_complete_without_answer_does_not_promote_accumulated_draft(self):
        self.engine.behavior = "missing_answer"
        await self.client.post("/query/stream", json={"question": "Synthetic question", "conversation_id": "missing-answer"})
        self.assertFalse(any(message["role"] == "assistant" for message in self.saved.messages))

    async def test_disconnect_preserves_background_completion_and_only_saves_terminal_answer(self):
        self.engine.behavior = "disconnect"
        request_body = json.dumps({"question": "Synthetic question", "conversation_id": "disconnected"}).encode()
        request_sent = False

        async def receive():
            nonlocal request_sent
            if not request_sent:
                request_sent = True
                return {"type": "http.request", "body": request_body, "more_body": False}
            await self.engine.started.wait()
            return {"type": "http.disconnect"}

        async def send(_message):
            pass

        scope = {"type": "http", "asgi": {"version": "3.0", "spec_version": "2.0"}, "http_version": "1.1",
                 "method": "POST", "scheme": "http", "path": "/query/stream", "raw_path": b"/query/stream",
                 "query_string": b"", "headers": [(b"content-type", b"application/json")],
                 "client": ("127.0.0.1", 1), "server": ("synthetic.local", 80), "root_path": ""}
        await asyncio.wait_for(main.app(scope, receive, send), timeout=2)
        self.assertFalse(any(message["role"] == "assistant" for message in self.saved.messages))
        self.engine.release.set()
        await asyncio.wait_for(self.saved.saved_assistant.wait(), timeout=2)
        assistant = next(message for message in self.saved.messages if message["role"] == "assistant")
        self.assertEqual(assistant["content"], final_payload()["answer"])
        self.assertNotIn("999", assistant["content"])


if __name__ == "__main__":
    unittest.main()
