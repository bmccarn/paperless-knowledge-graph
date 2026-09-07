"""Proposal/review transformations through public extraction and alias binding."""
import copy
import json
import unittest
from tests.runtime import configure_test_environment
configure_test_environment()
from app.entity_bindings import DocumentBindings
from app.entity_policy import trusted_aliases
from app.extractor import EntityExtractor
from tests.test_extraction import CompletionClient
from tests.test_entity_review_closure import Graph, Ledger, node, resolver_with, review_for

NAMES = ("Cedar Analytics", "Willow Research")
SOURCE = "Cedar Analytics also known as Willow Research signed this agreement."


def alias_client(transform=lambda rows: rows, *, kind="Organization", status="affirmed"):
    def entities(response, client):
        return {"entities": [{"name": name, "type": kind, "confidence": .95,
                              "evidence_quote": SOURCE} for name in NAMES]}
    def review(response, client):
        proposals = json.loads(client.calls[-1][2].split("Proposals:\n", 1)[1])
        result = entities(response, client)
        result["entities"] = transform(copy.deepcopy(result["entities"]))
        result["identity_reviews"] = [review_for(proposal, status=status) for proposal in proposals]
        return result
    return CompletionClient({"entities": entities, "entity_review": review})


class ReviewIdentityAdmissionTests(unittest.IsolatedAsyncioTestCase):
    async def test_case_and_whitespace_review_preserves_positive_alias_to_binding(self):
        for spelling in (str.upper, str.lower, lambda s: s.upper().replace(" ", "  ")):
            with self.subTest(spelling=spelling):
                def transform(rows):
                    for row in rows:
                        row["name"] = spelling(row["name"])
                    return rows
                extraction = await EntityExtractor(alias_client(transform)).extract("Synthetic alias", SOURCE, "general")
                self.assertEqual(len(extraction["all_entities"]), 2)
                self.assertEqual(extraction["extraction_coverage"]["status"], "complete")
                graph, ledger = Graph(), Ledger()
                graph.nodes = {"canonical": node("canonical", NAMES[0])}
                binding = DocumentBindings(101, extraction, SOURCE, resolver_with(graph, ledger))
                await binding.resolve_all()
                self.assertEqual(len(set(binding.resolved.values())), 1)
                self.assertEqual(len(trusted_aliases(graph.nodes["canonical"], "Organization", 101, SOURCE)), 1)

    async def test_new_source_mentioned_name_is_not_admitted_by_review(self):
        def transform(rows):
            return rows + [{**rows[0], "name": "agreement"}]
        extraction = await EntityExtractor(alias_client(transform)).extract("Synthetic alias", SOURCE, "general")
        self.assertEqual({e["name"] for e in extraction["all_entities"]}, set(NAMES))

    async def test_changed_discriminator_cannot_impersonate_a_proposal(self):
        def transform(rows):
            return [{**row, "identity_hint": "signed"} for row in rows]
        extraction = await EntityExtractor(alias_client(transform)).extract("Synthetic alias", SOURCE, "general")
        self.assertEqual(extraction["all_entities"], [])

    async def test_casing_with_unsupported_type_change_preserves_source_type(self):
        def transform(rows):
            return [{**row, "name": row["name"].upper(), "type": "Condition"} for row in rows]
        extraction = await EntityExtractor(alias_client(transform)).extract("Synthetic alias", SOURCE, "general")
        self.assertEqual({(e["name"], e["type"]) for e in extraction["all_entities"]},
                         {(name, "Organization") for name in NAMES})

    async def test_extraction_document_maps_to_graph_documentref_and_retains_alias(self):
        extraction = await EntityExtractor(alias_client(kind="Document")).extract("Synthetic alias", SOURCE, "general")
        self.assertEqual(len(extraction["all_entities"]), 2)
        graph, ledger = Graph(), Ledger()
        graph.nodes = {"canonical": node("canonical", NAMES[0], kind="DocumentRef")}
        binding = DocumentBindings(101, extraction, SOURCE, resolver_with(graph, ledger))
        await binding.resolve_all()
        self.assertEqual(len(set(binding.resolved.values())), 1)
        self.assertEqual(len(trusted_aliases(graph.nodes["canonical"], "DocumentRef", 101, SOURCE)), 1)
