import json
import unittest
from app.evidence import build_evidence_pack, evidence_item_id, format_evidence_pack_for_llm


class EvidenceInputTests(unittest.TestCase):
    def test_synthesis_sees_relevant_tail_and_declares_selected_windows(self):
        chunk = {"document_id": 101, "chunk_index": 0, "title": "Statement",
                 "content": "Administrative header. " * 130 + "Monthly premium is $321.00 USD."}
        pack = build_evidence_pack("Monthly premium?", {}, [chunk], [])
        rendered = json.loads(format_evidence_pack_for_llm(pack, max_chars=6000))
        self.assertIn("$321.00 USD", rendered["spans"][0]["content"])
        self.assertEqual(rendered["selected_span_count"], 1)
        self.assertFalse(pack["coverage"]["retrieval_is_exhaustive"])

    def test_budget_drops_whole_spans_with_explicit_coverage_and_content_changes_identity(self):
        chunk = {"document_id": 101, "content": "source " * 800}
        pack = build_evidence_pack("source", {}, [chunk], [])
        rendered = json.loads(format_evidence_pack_for_llm(pack, max_chars=1000))
        self.assertEqual(rendered["selected_span_count"], 0)
        self.assertEqual(rendered["available_span_count"], 2)
        self.assertNotEqual(evidence_item_id(chunk), evidence_item_id({**chunk, "content": "changed"}))


if __name__ == "__main__":
    unittest.main()
