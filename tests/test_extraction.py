"""Behavior tests at EntityExtractor.extract with a controlled completion client."""

import json
import re
from types import SimpleNamespace
import unittest

from tests.runtime import configure_test_environment

configure_test_environment()

from app.extractor import EntityExtractor


class CompletionClient:
    """The source is synthetic; every application pass still builds/parses its prompt."""

    def __init__(self, overrides=None):
        self.chat = SimpleNamespace(completions=self)
        self.overrides = overrides or {}
        self.calls = []
        self.source = ""
        self.finish_reason = "stop"
        self.truncate_above = None

    async def create(self, **request):
        prompt = request["messages"][-1]["content"]
        if "SOURCE-BOUND OUTPUT CONTRACT" in prompt:
            stage = "metadata"
            self.source = prompt.split("Document content:\n", 1)[1].split("\n\nSOURCE-BOUND OUTPUT CONTRACT", 1)[0]
        elif prompt.startswith("You are an entity extraction"):
            stage = "entities"
        elif prompt.startswith("You are a quality reviewer"):
            stage = "entity_review"
        elif prompt.startswith("Infer relationships"):
            stage = "relationships"
        elif prompt.startswith("Independently review proposed relationships"):
            stage = "relationship_review"
        else:
            raise AssertionError("Unexpected completion prompt")
        self.calls.append((stage, self.source, prompt))
        response = self.response(stage)
        if stage in self.overrides:
            override = self.overrides[stage]
            response = override(response, self) if callable(override) else override
        finish_reason = self.finish_reason
        if stage == "metadata" and self.truncate_above is not None and len(self.source) > self.truncate_above:
            finish_reason = "length"
        return SimpleNamespace(choices=[SimpleNamespace(message=SimpleNamespace(content=json.dumps(response)), finish_reason=finish_reason)])

    def response(self, stage):
        entities = []
        for name, kind in [("Alice Example", "Person"), ("Tail Widgets", "Organization")]:
            if name in self.source:
                entities.append({"name": name, "type": kind, "confidence": 0.95,
                                 "description": "Named in source", "evidence_quote": name})
        relation_quote = "Alice Example works at Tail Widgets."
        relationships = []
        if relation_quote in self.source:
            relationships.append({"from_entity": "Alice Example", "to_entity": "Tail Widgets",
                                  "relationship_type": "WORKS_AT", "confidence": 0.95,
                                  "rationale": "The source explicitly states employment.",
                                  "evidence_quote": relation_quote,
                                  "support_status": "supported", "explicit": True})
        if stage == "metadata":
            premium = re.search(r"Premium: (\d+(?:\.\d+)?) USD", self.source)
            if premium:
                return {"metadata": {"premium": premium[1]}, "evidence": [{"path": "premium", "quote": premium[0]}]}
            return {"metadata": {}, "evidence": []}
        if stage in {"entities", "entity_review"}:
            return {"entities": entities}
        return {"relationships": relationships}

    async def close(self):
        pass


class ExtractionTests(unittest.IsolatedAsyncioTestCase):
    async def extract(self, source, client=None, **options):
        client = client or CompletionClient()
        return await EntityExtractor(client, **options).extract("Synthetic source", source, "insurance")

    async def test_long_document_tail_is_processed_with_original_source_offsets(self):
        source = "Blank filler. " * 2500 + "\nAlice Example works at Tail Widgets. Premium: 125.00 USD."
        client = CompletionClient()
        result = await self.extract(source, client)
        self.assertGreater(len(source), 31000)
        self.assertEqual(result["extraction_coverage"]["status"], "complete")
        self.assertEqual(result["extraction_coverage"]["covered_characters"], len(source))
        self.assertEqual(result["premium"], "125.00")
        self.assertEqual(len(result["all_entities"]), 2)
        relationship = result["implied_relationships"][0]
        self.assertEqual(relationship["relationship"], "WORKS_AT")
        self.assertFalse(relationship["inferred"])
        self.assertIn("explicitly", relationship["rationale"])
        for span in relationship["evidence"]:
            self.assertGreater(span["start"], 31000)
            self.assertEqual(source[span["start"]:span["end"]], span["quote"])
        for stage, window, prompt in client.calls:
            self.assertIn(window, prompt, f"{stage} omitted source text")
        self.assertLessEqual(len(client.calls), 5 * len(result["extraction_coverage"]["windows"]))

    async def test_overlap_does_not_duplicate_entities_or_relationships(self):
        source = "x " * 75 + "Alice Example works at Tail Widgets." + " y" * 150
        result = await self.extract(source, window_characters=240, overlap_characters=120)
        self.assertEqual(len(result["all_entities"]), 2)
        self.assertEqual(len(result["implied_relationships"]), 1)
        self.assertEqual(len(result["implied_relationships"][0]["evidence"]), 1)

    async def test_single_entity_is_reviewed_against_actual_source(self):
        client = CompletionClient({"entity_review": {"entities": []}})
        result = await self.extract("Alice Example appears in this document.", client)
        review_calls = [prompt for stage, _, prompt in client.calls if stage == "entity_review"]
        self.assertEqual(len(review_calls), 1)
        self.assertIn("Alice Example appears in this document.", review_calls[0])
        self.assertEqual(result["all_entities"], [])

    async def test_fabricated_entity_quote_and_verifier_added_names_are_rejected(self):
        def forged_proposal(response, _):
            response["entities"].append({"name": "Fabricated Person", "type": "Person", "confidence": 0.99, "evidence_quote": "Fabricated Person"})
            return response
        def added_review(response, _):
            response["entities"].append({"name": "Bob Example", "type": "Person", "confidence": 0.99, "evidence_quote": "Bob Example"})
            return response
        client = CompletionClient({"entities": forged_proposal, "entity_review": added_review})
        result = await self.extract("Alice Example and Bob Example are listed.", client)
        self.assertEqual([entity["name"] for entity in result["all_entities"]], ["Alice Example"])
        self.assertTrue(any("additions rejected" in issue for issue in result["extraction_issues"]))

    async def test_unknown_relationship_endpoint_is_never_created(self):
        rel = {"from_entity": "Alice Example", "to_entity": "Fabricated Person", "relationship_type": "KNOWS", "confidence": 1.0, "rationale": "Invented", "evidence_quote": "Alice Example"}
        client = CompletionClient({"relationships": {"relationships": [rel]}})
        result = await self.extract("Alice Example appears here.", client)
        self.assertEqual(result["implied_relationships"], [])
        self.assertEqual([entity["name"] for entity in result["all_entities"]], ["Alice Example"])
        self.assertTrue(any("endpoint" in issue for issue in result["extraction_issues"]))

    async def test_relationship_with_invented_quote_is_rejected(self):
        def wrong_quote(response, _):
            response["relationships"][0]["evidence_quote"] = "Alice Example owns Tail Widgets."
            return response
        client = CompletionClient({"relationships": wrong_quote})
        result = await self.extract("Alice Example works at Tail Widgets.", client)
        self.assertEqual(result["implied_relationships"], [])

    async def test_independent_relationship_review_rejects_cooccurrence(self):
        quote = "Alice Example and Tail Widgets appear in an address book."
        rel = {"from_entity": "Alice Example", "to_entity": "Tail Widgets", "relationship_type": "WORKS_AT", "confidence": 0.95, "rationale": "They co-occur", "evidence_quote": quote, "support_status": "unsupported", "explicit": False}
        client = CompletionClient({"relationships": {"relationships": [rel]}, "relationship_review": {"relationships": [rel]}})
        result = await self.extract(quote, client)
        self.assertEqual(result["implied_relationships"], [])
        self.assertTrue(any(stage == "relationship_review" for stage, _, _ in client.calls))

    async def test_inferred_relationship_remains_inferred(self):
        def inferred(response, _):
            response["relationships"][0]["explicit"] = False
            response["relationships"][0]["rationale"] = "Source-backed inference, not a stated role."
            return response
        result = await self.extract("Alice Example works at Tail Widgets.", CompletionClient({"relationship_review": inferred}))
        self.assertTrue(result["implied_relationships"][0]["inferred"])

    async def test_conflicting_duplicate_relationship_reviews_cannot_promote_support(self):
        def conflicting(response, _):
            response["relationships"].append(dict(response["relationships"][0], support_status="unsupported"))
            return response
        result = await self.extract("Alice Example works at Tail Widgets.", CompletionClient({"relationship_review": conflicting}))
        self.assertEqual(result["implied_relationships"], [])

    async def test_conflicting_entity_types_are_omitted_instead_of_arbitrarily_merged(self):
        def different_type(response, client):
            if "Company" in client.source:
                for entity in response["entities"]:
                    entity["type"] = "Organization"
            return response
        client = CompletionClient({"entities": different_type, "entity_review": different_type})
        source = "Alice Example is a person. " + "x " * 100 + "Company Alice Example is a business."
        result = await self.extract(source, client, window_characters=140, overlap_characters=20)
        self.assertEqual(result["all_entities"], [])
        self.assertTrue(any("Conflicting entity types" in issue for issue in result["extraction_issues"]))

    async def test_repeated_metadata_rows_are_unioned_with_window_provenance(self):
        def metadata(_, client):
            rows, refs = [], []
            for name, value in re.findall(r"(Lab [AB]): (\d+)", client.source):
                index = len(rows)
                rows.append({"name": name, "value": value})
                refs.extend([{"path": f"tests.{index}.{field}", "quote": f"{name}: {value}"} for field in ["name", "value"]])
            return {"metadata": {"tests": rows}, "evidence": refs}
        source = "x " * 75 + "Lab A: 5" + " y" * 90 + "Lab B: 6"
        result = await self.extract(source, CompletionClient({"metadata": metadata}), window_characters=240, overlap_characters=120)
        self.assertEqual(result["tests"], [{"name": "Lab A", "value": "5"}, {"name": "Lab B", "value": "6"}])
        for window_evidence in result["metadata_evidence"].values():
            for spans in window_evidence.values():
                for span in spans:
                    self.assertEqual(source[span["start"]:span["end"]], span["quote"])

    async def test_scalar_metadata_conflict_is_not_last_window_wins(self):
        source = "Premium: 100 USD.\n" + "x " * 90 + "Premium: 200 USD."
        result = await self.extract(source, window_characters=140, overlap_characters=20)
        self.assertIsNone(result["premium"])
        self.assertEqual(result["metadata_conflicts"], [{"path": "premium", "values": ["100", "200"]}])
        self.assertGreaterEqual(len(result["metadata_evidence"]), 2)

    async def test_fabricated_metadata_value_cannot_borrow_a_real_quote(self):
        client = CompletionClient({"metadata": {"metadata": {"premium": "999"}, "evidence": [{"path": "premium", "quote": "Premium: 125 USD"}]}})
        result = await self.extract("Premium: 125 USD", client)
        self.assertIsNone(result["premium"])
        self.assertTrue(any("value not present" in issue for issue in result["extraction_issues"]))

    async def test_metadata_number_cannot_match_a_prefix_or_drop_a_negative_sign(self):
        for source in ["Premium: 100.25 USD", "Premium: -100 USD", "Premium: 1,100 USD"]:
            with self.subTest(source=source):
                client = CompletionClient({"metadata": {"metadata": {"premium": "100"}, "evidence": [{"path": "premium", "quote": source}]}})
                result = await self.extract(source, client)
                self.assertIsNone(result["premium"])

    async def test_malformed_window_and_unavailable_verifier_are_incomplete(self):
        def failure(*_):
            raise RuntimeError("synthetic provider failure")
        for overrides in [{"entities": {"entities": "not a list"}}, {"metadata": {}}, {"entity_review": failure}, {"relationship_review": {"relationships": ["bad row"]}}]:
            with self.subTest(overrides=overrides):
                client = CompletionClient(overrides)
                result = await self.extract("Alice Example works at Tail Widgets.", client)
                self.assertEqual(result["extraction_coverage"]["status"], "failed")
                self.assertEqual(result["all_entities"], [])
                self.assertEqual(result["implied_relationships"], [])

    async def test_windows_continue_past_the_former_count_limit(self):
        source = "Blank filler. " * 300 + "Alice Example"
        result = await self.extract(source, window_characters=100, overlap_characters=20)
        coverage = result["extraction_coverage"]
        self.assertEqual(coverage["status"], "complete")
        self.assertEqual(coverage["covered_characters"], len(source))
        self.assertEqual(result["all_entities"][0]["name"], "Alice Example")
        span = result["all_entities"][0]["evidence"][0]
        self.assertEqual(source[span["start"]:span["end"]], span["quote"])

    async def test_output_truncation_adaptively_splits_only_the_oversized_window(self):
        source = "Blank filler. " * 30 + "\nAlice Example works at Tail Widgets."
        client = CompletionClient()
        client.truncate_above = 300
        result = await self.extract(
            source,
            client,
            window_characters=len(source),
            overlap_characters=40,
            minimum_split_characters=180,
        )
        coverage = result["extraction_coverage"]
        self.assertEqual(coverage["status"], "complete")
        self.assertEqual(coverage["covered_characters"], len(source))
        self.assertEqual(coverage["adaptive_splits"], 1)
        self.assertEqual(len(coverage["windows"]), 2)
        self.assertTrue(all(window["end"] - window["start"] <= 300 for window in coverage["windows"]))
        self.assertEqual({entity["name"] for entity in result["all_entities"]}, {"Alice Example", "Tail Widgets"})

    async def test_truncated_completion_is_not_accepted_as_a_complete_window(self):
        client = CompletionClient()
        client.finish_reason = "length"
        result = await self.extract("Alice Example", client)
        self.assertEqual(result["extraction_coverage"]["status"], "failed")
        self.assertEqual(len(client.calls), 1)

    async def test_empty_document_is_failed_not_successful_empty_extraction(self):
        result = await self.extract("")
        self.assertEqual(result["extraction_coverage"]["status"], "failed")
        self.assertEqual(result["confidence"], 0.0)


if __name__ == "__main__":
    unittest.main()
