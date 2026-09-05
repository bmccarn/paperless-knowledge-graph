"""Timeline-only evidence survives public projection and late feedback is enforced."""

import copy
import json
import unittest
from unittest.mock import AsyncMock, patch

from tests.test_query_delivery import RetrievedEngine
from tests.test_answer_finalization import PACK
from app.cache import invalidate_on_sync


MEETING_QUOTE = "Meeting date: 2024-02-29."


class TimelineEngine(RetrievedEngine):
    async def _build_evidence_pack(self, *args, **kwargs):
        result = copy.deepcopy(PACK)
        result["items"].append({"id": "meeting-tail", "document_id": 102, "title": "Meeting record", "chunk_index": 0,
                                "content": "irrelevant filler " * 1500 + MEETING_QUOTE, "excerpt": "irrelevant filler"})
        return result


class TimelineAuditor:
    async def extract_timeline(self, question, context):
        spans = json.loads(context)
        span = next(span for span in spans if MEETING_QUOTE in span["content"])
        return [{"date": "2024-02-29", "title": "Meeting date", "summary": "Recorded meeting date.", "document_id": 102,
                 "references": [self.reference(span, MEETING_QUOTE)]}]

    @staticmethod
    def reference(span, quote):
        return {"span_id": span["span_id"], "evidence_id": span["evidence_id"], "document_id": span["document_id"], "quote": quote}

    async def audit_answer_units(self, question, units, spans, plan):
        result = []
        for unit in units:
            quote = MEETING_QUOTE if unit["id"] == "event" else "Monthly premium: $321.00 USD."
            span = next(span for span in spans if quote in span["content"])
            result.append({"unit_id": unit["id"], "status": "supported", "temporal_scope": "historical",
                           "references": [self.reference(span, quote)]})
        return {"assessments": result}


class TimelineDeliveryTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        invalidate_on_sync()
        self.engine = TimelineEngine()
        self.flags = AsyncMock(return_value=set())
        self.patches = [patch("app.query.strands_orchestrator", TimelineAuditor()),
                        patch("app.query.embeddings_store.get_incomplete_document_ids", AsyncMock(return_value=set())),
                        patch("app.query.embeddings_store.get_open_feedback_document_ids", self.flags)]
        for context in self.patches:
            context.start()

    async def asyncTearDown(self):
        for context in reversed(self.patches):
            context.stop()

    async def test_timeline_only_source_and_tail_quote_are_publicly_resolvable(self):
        result = await self.engine.query("Recorded premium and meeting timeline?", mode="timeline")
        self.assertEqual(result["finalization"]["disposition"], "supported")
        self.assertEqual({source["document_id"] for source in result["sources"]}, {101, 102})
        self.assertEqual(result["timeline_events"][0]["document_id"], 102)
        item = next(item for item in result["evidence_pack"]["items"] if item["document_id"] == 102)
        self.assertEqual(item["excerpt"], MEETING_QUOTE)
        self.assertGreater(item["support_spans"][0]["start"], 18000)
        self.assertEqual(item["support_spans"][0]["quote"], result["timeline_events"][0]["references"][0]["quote"])

    async def test_feedback_added_before_final_gate_removes_earlier_timeline_event(self):
        self.flags.return_value = {102}
        result = await self.engine.query("Recorded premium and meeting timeline?", mode="timeline")
        self.assertEqual(result["timeline_events"], [])
        self.assertEqual({source["document_id"] for source in result["sources"]}, {101})
        self.assertEqual(result["finalization"]["disposition"], "supported")
        item = next(item for item in result["evidence_pack"]["items"] if item["document_id"] == 102)
        self.assertTrue(item["feedback_open"])
        self.assertEqual(item["support_spans"], [])


if __name__ == "__main__":
    unittest.main()
