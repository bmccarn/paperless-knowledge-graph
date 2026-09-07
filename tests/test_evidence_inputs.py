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

    def test_explicit_id_disambiguates_repeated_titles_and_hash_references(self):
        from app.answer_finalization import evidence_spans, select_spans
        target = {"document_id": 101, "title": "Invoice", "content": "Certified postage: $321.00 USD."}
        chunks = [target] + [{"document_id": i, "title": "Invoice",
                             "content": "describe the invoice subject using source quotation " * 70}
                            for i in range(200, 225)]
        for reference in ("Paperless ID 101", "document #101", "Paperless document #101"):
            question = f'For {reference}, titled "Invoice", describe the invoice subject using source quotation.'
            pack = build_evidence_pack(question, {}, chunks, [])
            rendered = json.loads(format_evidence_pack_for_llm(pack, max_chars=5000))
            self.assertEqual(rendered["spans"][0]["document_id"], 101)
            self.assertEqual(select_spans(question, [], evidence_spans(pack))[0]["document_id"], 101)

    def test_multiple_requested_documents_get_windows_before_repeated_long_source(self):
        from app.answer_finalization import evidence_spans, select_spans
        question = "Compare document 101 and document 102, describe each invoice subject using exact source quotations."
        chunks = [{"document_id": 101, "title": "Postage", "content": "Certified postage: $321.00 USD."}]
        chunks += [{"document_id": 102, "chunk_index": i, "title": "Invoice",
                    "content": "describe each invoice subject using exact source quotations " * 65}
                   for i in range(8)]
        for question in (question, 'Compare the documents titled "Postage" and "Invoice", '
                         'describe each invoice subject using exact source quotations.'):
            pack = build_evidence_pack(question, {}, chunks, [])
            rendered = json.loads(format_evidence_pack_for_llm(pack, max_chars=6000))
            self.assertEqual({s["document_id"] for s in rendered["spans"]}, {101, 102})
            selected = select_spans(question, [], evidence_spans(pack), budget=5000)
            self.assertEqual({s["document_id"] for s in selected}, {101, 102})
            self.assertLessEqual(sum(len(s["content"]) for s in selected), 5000)

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
