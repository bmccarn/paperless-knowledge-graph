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

    def test_named_document_survives_lexical_distractors_in_synthesis_and_audit(self):
        from app.answer_finalization import evidence_spans, select_spans
        question = ('For the document titled "Parcel Invoice 7812" (Paperless ID 101), '
                    'describe its subject with an exact source quotation. Use only this document.')
        target = {"document_id": 101, "title": "Parcel Invoice 7812", "content": "Certified postage: $321.00 USD."}
        distractor = "For the document describe its subject with an exact source quotation. " * 45
        chunks = [target] + [{"document_id": i, "title": "Unrelated notice", "content": distractor}
                             for i in range(200, 225)]
        for scoped_question in (question, question.replace(" (Paperless ID 101)", "")):
            pack = build_evidence_pack(scoped_question, {}, chunks, [])
            rendered = json.loads(format_evidence_pack_for_llm(pack, max_chars=5000))
            self.assertEqual(rendered["spans"][0]["document_id"], 101)
            selected = select_spans(scoped_question, [{"text": "This is a postage invoice."}],
                                    evidence_spans(pack), budget=4000)
            self.assertEqual(selected[0]["document_id"], 101)
            self.assertEqual(selected[0]["content"], target["content"])

    def test_document_reference_priority_does_not_interpret_account_numbers(self):
        from app.answer_finalization import select_spans
        spans = [{"document_id": 101, "title": "Unrelated notice", "content": "unrelated"},
                 {"document_id": 202, "title": "Postage", "content": "Postage account number 101"}]
        self.assertEqual(select_spans("Postage account number 101?", [], spans, budget=40)[0]["document_id"], 202)

    def test_budget_drops_whole_spans_with_explicit_coverage_and_content_changes_identity(self):
        chunk = {"document_id": 101, "content": "source " * 800}
        pack = build_evidence_pack("source", {}, [chunk], [])
        rendered = json.loads(format_evidence_pack_for_llm(pack, max_chars=1000))
        self.assertEqual(rendered["selected_span_count"], 0)
        self.assertEqual(rendered["available_span_count"], 2)
        self.assertNotEqual(evidence_item_id(chunk), evidence_item_id({**chunk, "content": "changed"}))


if __name__ == "__main__":
    unittest.main()
