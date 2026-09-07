"""Cross-window receipt and unrelated-veto regression contracts."""
import copy
import json
import unittest

from tests.runtime import configure_test_environment
configure_test_environment()

from app.entity_bindings import DocumentBindings
from app import entity_policy as policy, entity_decisions as decisions
from app.extractor import EntityExtractor
from tests.test_extraction import CompletionClient
from tests.test_entity_review_closure import Graph, Ledger, node, resolver_with, review_for


class ReceiptRegressionTests(unittest.IsolatedAsyncioTestCase):
    async def test_public_extraction_preserves_case_varied_denials_in_either_order(self):
        pairs = [("Cedar Analytics", "Willow Research"), ("Juniper Tools", "Maple Works")]
        for left, right in pairs:
            for first_case, second_case in ((str, str.upper), (str.upper, str.lower), (str.lower, str)):
                for deny_first in (False, True):
                    with self.subTest(pair=left, first_case=first_case.__name__, second_case=second_case.__name__, deny_first=deny_first):
                        first_names = (first_case(left), first_case(right))
                        second_names = (second_case(left), second_case(right))
                        def phrase(names, denied):
                            base = f"{names[0]} also known as {names[1]}"
                            return base + (". That assertion is disputed." if denied else " signed.")
                        first = phrase(first_names, deny_first)
                        second = phrase(second_names, not deny_first)
                        source = first.ljust(12000) + second
                        def entities(response, client):
                            names = second_names if len(client.source) < 12000 else first_names
                            return {"entities": [{"name": name, "type": "Organization", "confidence": .95,
                                                   "evidence_quote": client.source.strip()} for name in names]}
                        def reviews(response, client):
                            proposals = json.loads(client.calls[-1][2].split("Proposals:\n", 1)[1])
                            result = entities(response, client)
                            denied = "disputed" in client.source
                            result["identity_reviews"] = [review_for(p, status="denied" if denied else "affirmed",
                                scope="disputed" if denied else "current_direct") for p in proposals]
                            return result
                        result = await EntityExtractor(CompletionClient({"entities": entities, "entity_review": reviews})).extract(
                            "Synthetic cross-window identity", source, "general")
                        self.assertEqual(result["extraction_coverage"]["status"], "complete")
                        graph, ledger = Graph(), Ledger()
                        graph.nodes = {"canonical": node("canonical", left)}
                        binding = DocumentBindings(101, result, source, resolver_with(graph, ledger))
                        self.assertEqual({p["status"] for p in binding.identity_proofs}, {"affirmed", "denied"})
                        await binding.resolve_all()
                        self.assertEqual(len(set(binding.resolved.values())), 2)
                        self.assertEqual(graph.nodes["canonical"]["alias_records"], [])
                        self.assertEqual(policy.trusted_aliases(graph.nodes["canonical"], "Organization", 101, source), [])

    async def test_unrelated_partial_veto_does_not_isolate_other_identities(self):
        for kind, protected_name, ordinary_name in (("Organization", "Restricted Labs", "Acme"),
                                                   ("Organization", "Restricted Labs", "Harbor Works"),
                                                   ("Person", "Restricted Person", "Alice Morgan")):
            with self.subTest(kind=kind, ordinary_name=ordinary_name):
                graph, ledger = Graph(), Ledger()
                graph.nodes = {"protected": node("protected", protected_name, kind, docs=[1])}
                original = copy.deepcopy(graph.nodes["protected"])
                ledger.rows = [{"left_uuid": "protected", "right_uuid": "missing", "decision": "split",
                    "left_identity": decisions.entity_identity(original), "identity_status": "unresolved_legacy"}]
                resolver = resolver_with(graph, ledger)
                ids = [await resolver.resolve(ordinary_name, kind, doc, source=f"{ordinary_name} supplies this invoice.")
                       for doc in (101, 102, 103)]
                self.assertEqual(len(set(ids)), 1)
                self.assertEqual(graph.nodes[ids[0]]["resolution_status"], "new")
                self.assertEqual(graph.nodes[ids[0]]["source_doc_ids"], [101, 102, 103])
                self.assertEqual(graph.nodes["protected"], original)
                protected = [await resolver.resolve(protected_name, kind, 201, source=f"{protected_name} signed.") for _ in range(3)]
                self.assertEqual(len(set(protected)), 1)
                self.assertNotEqual(protected[0], "protected")
                self.assertEqual(graph.nodes[protected[0]]["resolution_status"], "isolated")

    async def test_partial_veto_isolation_is_idempotent_without_original_anchor(self):
        graph, ledger = Graph(), Ledger()
        identity = decisions.entity_identity(node("gone", "Restricted Labs", docs=[1]))
        ledger.rows = [{"left_uuid": "gone", "right_uuid": "missing", "decision": "split",
                        "left_identity": identity, "identity_status": "unresolved_legacy"}]
        resolver = resolver_with(graph, ledger)
        ids = [await resolver.resolve(name, "Organization", 101, source="Restricted Labs signed.")
               for name in ("Restricted Labs", "restricted labs", "RESTRICTED LABS")]
        self.assertEqual(len(set(ids)), 1)
        self.assertEqual(graph.nodes[ids[0]]["resolution_status"], "isolated")
        ordinary = [await resolver.resolve("Acme", "Organization", doc, source="Acme signed.") for doc in (201, 202)]
        self.assertEqual(len(set(ordinary)), 1)
        self.assertNotEqual(ordinary[0], ids[0])
