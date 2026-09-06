"""Resolver and ingestion-path tests with synthetic, strict storage adapters."""
import copy
import unittest
from unittest.mock import AsyncMock, patch

from tests.runtime import configure_test_environment
configure_test_environment()

from app import entity_resolver as module, pipeline
from app.entity_policy import RESOLUTION_POLICY, human_alias_record, digest
from app.entity_bindings import DocumentBindings
from app.extraction_evidence import validate_entities, validate_relationships
from app.paperless import PaperlessClient
from tests.test_entity_decisions import MemoryGraph, MemoryDecisions, person


def node(uuid, name, kind="Organization", docs=(), **props):
    return {**person(uuid, name, docs), "entity_type": kind, **props}


class EvidenceResolutionTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.graph = MemoryGraph()
        self.decisions = MemoryDecisions()
        self.resolver = module.EntityResolver()
        self.patches = [patch.object(module, "graph_store", self.graph),
                        patch.object(module, "embeddings_store", self.decisions)]
        for item in self.patches:
            item.start()
        self.addCleanup(lambda: [item.stop() for item in reversed(self.patches)])

    async def test_similarity_never_auto_links_or_adds_an_alias_matrix(self):
        for kind, canonical, incoming in [
            ("Person", "Alice Jane Example", "Alice J Example"),
            ("Person", "José García", "Jose Garcia"),
            ("Person", "Alicia Example", "Alice Example"),
            ("Organization", "Department of Defense", "U S Department of Energy"),
            ("Organization", "Example Financial", "Example Financial Holdings"),
            ("Organization", "Example Widgets Inc", "Example Widgets LLC"),
            ("Organization", "Orion", "Orion Card/Harbor Bank USA"),
            ("Organization", "Orion", "Orion Finance LLC"),
            ("Product", "Widget 10", "Widget 100"),
        ]:
            with self.subTest(kind=kind, canonical=canonical, incoming=incoming):
                self.graph.nodes = {"existing": node("existing", canonical, kind)}
                embedding = AsyncMock(return_value=[1, 1, 1])
                with patch.object(self.decisions, "generate_embedding", embedding), \
                     patch("openai.AsyncOpenAI", side_effect=AssertionError("name-only model called")):
                    resolved = await self.resolver.resolve(incoming, kind, 11)
                self.assertNotEqual(resolved, "existing")
                self.assertEqual(self.graph.nodes["existing"]["aliases"], [])
                embedding.assert_not_called()

    async def test_unique_exact_and_orthographic_names_are_reused_across_documents(self):
        for kind, canonical, incoming in [
            ("Person", "José García", "Jose\u0301 Garci\u0301a"),
            ("Person", "Alice Example", "Example, Alice"),
            ("Organization", "Example Widgets, Inc.", "Example Widgets Inc"),
            ("Organization", "Acme", "Acme"),
            ("Location", "Paris", "Paris"),
            ("Condition", "Example syndrome", "Example syndrome"),
        ]:
            with self.subTest(kind=kind):
                self.graph.nodes = {"existing": node("existing", canonical, kind, [10])}
                for doc_id in (11, 12, 13):
                    self.assertEqual(await self.resolver.resolve(incoming, kind, doc_id), "existing")
                self.assertEqual(len(self.graph.nodes), 1)
                self.assertEqual(self.graph.nodes["existing"]["source_doc_ids"], [10, 11, 12, 13])
                self.assertEqual(self.graph.nodes["existing"]["aliases"], [])

    async def test_poisoned_legacy_alias_cannot_override_real_canonical_match(self):
        self.graph.nodes = {"bad": node("bad", "Department of Defense", aliases=["U S Department of Energy"]),
                            "good": node("good", "U S Department of Energy")}
        self.assertEqual(await self.resolver.resolve_organization("U S Department of Energy", 11), "good")
        self.graph.nodes.pop("good")
        self.assertNotEqual(await self.resolver.resolve_organization("U S Department of Energy", 12), "bad")
        self.assertEqual(self.graph.nodes["bad"]["aliases"], ["U S Department of Energy"])

    async def test_reviewed_alias_matches_but_ambiguous_reviewed_alias_does_not_pick_first(self):
        for uuid, canonical in [("a", "Alice Example"), ("b", "Alice Smyth")]:
            self.graph.nodes[uuid] = node(uuid, canonical, "Person", alias_records=[human_alias_record(canonical, "Alice Jones", "Person", f"review-{uuid}")])
        first = await self.resolver.resolve_person("Alice Jones", 11)
        self.assertNotIn(first, {"a", "b"})
        # The unknown source-local identity must not explode into one UUID per edge.
        self.assertEqual(await self.resolver.resolve_person("Alice Jones", 11), first)
        self.graph.nodes = {"a": self.graph.nodes["a"]}
        self.assertEqual(await self.resolver.resolve_person("Alice Jones", 12), "a")

    async def test_explicit_source_alias_proof_is_not_similarity_or_global_acronym_cache(self):
        self.graph.nodes = {"a": node("a", "Network Entity Systems"), "b": node("b", "New Era Services")}
        first_source = "Network Entity Systems (NES) signed."
        second_source = "New Era Services (NES) signed."
        self.assertEqual(await self.resolver.resolve_organization("NES", 11, source=first_source), "a")
        self.assertEqual(await self.resolver.resolve_organization("NES", 22, source=second_source), "b")
        self.assertNotIn(await self.resolver.resolve_organization("NES", 33), {"a", "b"})
        self.assertEqual(self.graph.nodes["a"]["alias_records"][0]["provenance"], "source_coreference")
        self.assertEqual(self.graph.nodes["a"]["alias_records"][0]["source_hash"], digest(first_source))

    async def test_same_name_different_type_and_context_are_independent(self):
        self.graph.nodes = {"org": node("org", "MERS", "Organization", [11]),
                            "condition": node("condition", "MERS", "Condition", [22])}
        self.assertEqual(await self.resolver.resolve_organization("MERS", 11), "org")
        self.assertEqual(await self.resolver.resolve_generic("MERS", "Condition", 22), "condition")
        other = await self.resolver.resolve_organization("MERS", 33)
        self.assertNotIn(other, {"org", "condition"})
        self.assertEqual(self.graph.nodes[other]["entity_type"], "Organization")

    async def test_homonym_identifiers_disagree_without_conflation(self):
        source = "Alice Example E-101. Alice Example E-202."
        a = await self.resolver.resolve_person("Alice Example", 11, source=source, identity_hint="E-101")
        b = await self.resolver.resolve_person("Alice Example", 11, source=source, identity_hint="E-202")
        self.assertNotEqual(a, b)
        self.assertEqual(await self.resolver.resolve_person("Alice Example", 22, source=source, identity_hint="E-101"), a)
        self.assertNotIn(await self.resolver.resolve_person("Alice Example", 33), {a, b})
        with self.assertRaises(ValueError):
            await self.resolver.resolve_person("Alice Example", 44, identity_hint="invented")

    async def test_explicit_coreference_and_reviewed_alias_cannot_override_no_merge(self):
        self.graph.nodes = {"a": node("a", "Alice Example", "Person", [11]),
                            "b": node("b", "Alice Smyth", "Person", [22])}
        await self.resolver.record_decision("a", "b", "never_merge")
        self.graph.nodes.pop("b")
        self.graph.nodes["a"]["alias_records"] = [human_alias_record("Alice Example", "Alice Smyth", "Person", "review-1")]
        source = "Alice Example also known as Alice Smyth signed."
        self.assertNotEqual(await self.resolver.resolve_person("Alice Smyth", 22, source=source), "a")
        self.assertEqual((await self.resolver.resolve_all_entities())["total_merged"], 0)

    async def test_nineteen_missing_legacy_vetoes_are_preserved_and_quarantined_idempotently(self):
        self.decisions.rows = [{"left_uuid": f"missing-left-{i}", "right_uuid": f"missing-right-{i}",
                                "decision": "never_merge", "note": "legacy decision", "created_at": "original"}
                               for i in range(19)]
        original = copy.deepcopy(self.decisions.rows)
        for _ in range(2):
            report = await self.resolver.hydrate_review_identities()
            self.assertEqual(report["hydrated"], 0)
            self.assertEqual(len(report["unresolved"]), 19)
        self.assertEqual(len(self.decisions.rows), 19)
        for old, row in zip(original, self.decisions.rows):
            self.assertEqual(row, {**old, "identity_status": "unresolved_legacy"})
            self.assertFalse(row.get("left_identity"))
            self.assertFalse(row.get("right_identity"))

    async def test_reviewed_pair_survives_reindex_but_legacy_merged_does_not(self):
        self.graph.nodes = {"a": node("a", "Alice Example", "Person", [11]),
                            "b": node("b", "Alice Smyth", "Person", [22])}
        await self.resolver.merge_entities("a", "b")
        row = self.decisions.rows[-1]
        self.assertEqual(row["provenance"], "human_review")
        self.graph.nodes = {"rebuilt": node("rebuilt", "Alice Example", "Person", [11])}
        self.assertEqual(await self.resolver.resolve_person("Alice Smyth", 33), "rebuilt")
        row["provenance"] = "legacy_unknown"
        self.graph.nodes = {"rebuilt": node("rebuilt", "Alice Example", "Person", [11])}
        self.assertNotEqual(await self.resolver.resolve_person("Alice Smyth", 44), "rebuilt")

    async def test_human_reviewed_alias_chain_survives_uuid_replacement_without_legacy_poison(self):
        self.graph.nodes = {"a": node("a", "Alice Example", "Person", [11]),
                            "b": node("b", "Alice Smyth", "Person", [22], aliases=["Unreviewed Poison"]),
                            "c": node("c", "Alice Jones", "Person", [33])}
        await self.resolver.merge_entities("b", "a")
        await self.resolver.merge_entities("c", "b")
        self.graph.nodes = {"rebuilt": node("rebuilt", "Alice Jones", "Person", [33])}
        self.assertEqual(await self.resolver.resolve_person("Alice Example", 44), "rebuilt")
        self.assertNotEqual(await self.resolver.resolve_person("Unreviewed Poison", 55), "rebuilt")

    async def test_alias_quarantine_overrides_other_alias_records_and_reviewed_pair_replay(self):
        self.graph.nodes = {"a": node("a", "Alice Example", "Person", [11]), "b": node("b", "Alice Smyth", "Person", [22])}
        await self.resolver.merge_entities("a", "b")
        self.graph.nodes["a"].setdefault("alias_records", []).append({"alias": "Alice Smyth", "type": "Person", "status": "quarantined"})
        source = "Alice Example also known as Alice Smyth signed."
        self.assertNotEqual(await self.resolver.resolve_person("Alice Smyth", 33, source=source), "a")
        self.graph.nodes["a"]["resolution_status"] = "quarantined"
        self.assertNotEqual(await self.resolver.resolve_person("Alice Example", 44), "a")

    async def test_partial_legacy_veto_protects_identifiable_side_and_keeps_history(self):
        self.graph.nodes = {"z-left": node("z-left", "Alice Example", "Person", [11])}
        original = {"left_uuid": "z-left", "right_uuid": "a-missing", "decision": "never_merge",
                    "note": "original note", "created_at": "original timestamp"}
        self.decisions.rows = [copy.deepcopy(original)]
        report = await self.resolver.hydrate_review_identities()
        self.assertEqual(report["hydrated"], 1)
        row = self.decisions.rows[0]
        for key, value in original.items():
            self.assertEqual(row[key], value)
        self.assertEqual(row["identity_status"], "unresolved_legacy")
        self.assertTrue(row["left_identity"])
        self.assertFalse(row.get("right_identity"))
        self.assertNotEqual(await self.resolver.resolve_person("Alice Example", 22), "z-left")

    async def test_existing_same_name_ambiguity_is_never_bulk_merged(self):
        self.graph.nodes = {"a": node("a", "Alice Example", "Person", [11]), "b": node("b", "Alice Example", "Person", [22])}
        self.assertEqual((await self.resolver.resolve_all_entities())["total_merged"], 0)
        self.assertEqual(await self.resolver.resolve_person("Alice Example", 22), "b")
        self.assertNotIn(await self.resolver.resolve_person("Alice Example", 33), {"a", "b"})

    async def test_invalid_type_and_decision_failure_cannot_create_partial_identity(self):
        for kind in ("", "Unknown", "Person) DETACH DELETE n"):
            with self.assertRaises(ValueError):
                await self.resolver.resolve("Alice Example", kind, 11)
        self.decisions.unavailable = True
        with self.assertRaises(ConnectionError):
            await self.resolver.resolve_person("Alice Example", 11)
        self.assertEqual(self.graph.nodes, {})

    async def test_full_pipeline_binds_corrected_type_once_for_metadata_relationships_and_vectors(self):
        source = "Network Entity Systems (NES) signed for Alice Example."
        proposals = [{"name": name, "type": kind, "description": "Synthetic source",
                      "confidence": .95, "evidence_quote": source}
                     for name, kind in [("NES", "Organization"), ("Alice Example", "Person")]]
        accepted = validate_entities(proposals, source, 0, [])
        rel = validate_relationships([{"from_entity": "NES", "to_entity": "Alice Example",
            "relationship_type": "SIGNED_FOR", "confidence": .95, "rationale": "explicit", "evidence_quote": source}], accepted, source, 0, [])[0]
        implied = {**rel, "relationship": rel["relationship_type"], "inferred": False}
        extracted = {"all_entities": accepted, "implied_relationships": [implied] * 5,
                     "parties": [{"name": "NES"}, {"name": "Alice Example"}]}
        relations = []
        async def write_relation(left, left_type, right, right_type, kind, props):
            for uuid, typ in [(left, left_type), (right, right_type)]:
                if typ != "Document":
                    self.assertEqual(self.graph.nodes[uuid]["entity_type"], typ)
            relations.append((left, left_type, right, right_type, kind, props))
        self.graph.create_relationship = write_relation
        self.decisions.store_entity_embedding = AsyncMock()
        with patch.object(pipeline, "graph_store", self.graph), patch.object(pipeline, "entity_resolver", self.resolver), \
             patch.object(pipeline, "embeddings_store", self.decisions), patch.object(self.resolver, "resolve", wraps=self.resolver.resolve) as resolve:
            bindings = DocumentBindings(11, extracted, source, self.resolver)
            await pipeline._process_extraction(11, "11", "legal_contract", extracted, bindings=bindings)
            await pipeline._process_implied_relationships(11, extracted, bindings=bindings)
            await pipeline._store_entity_embeddings(11, extracted, bindings=bindings)
            self.assertEqual(resolve.await_count, 2)
        organization = bindings.lookup("NES", "Organization")
        self.assertTrue(all(edge[0] == organization and edge[1] == "Organization" for edge in relations if edge[4] == "SIGNED_FOR"))
        self.assertEqual(self.decisions.store_entity_embedding.await_count, 2)
        self.assertIsNone(pipeline._document_bindings.get())

    async def test_missing_relationship_binding_fails_closed_not_creates_endpoint(self):
        extracted = {"all_entities": [], "implied_relationships": [{"from_entity": "Unknown", "to_entity": "Unknown"}]}
        bindings = DocumentBindings(11, extracted, "", self.resolver)
        with self.assertRaisesRegex(ValueError, "endpoint"):
            await pipeline._process_implied_relationships(11, extracted, bindings=bindings)
        with self.assertRaisesRegex(ValueError, "bindings"):
            await pipeline._process_implied_relationships(11, extracted)
        self.assertEqual(self.graph.nodes, {})


class FingerprintTests(unittest.TestCase):
    def test_resolution_policy_invalidates_markers_without_changing_ocr_hash(self):
        doc = {"content": "Synthetic source", "title": "Synthetic title", "tags": []}
        before = PaperlessClient.ingestion_fingerprint(doc)
        with patch("app.entity_policy.RESOLUTION_POLICY", "future-policy"):
            self.assertNotEqual(PaperlessClient.ingestion_fingerprint(doc), before)
        self.assertEqual(PaperlessClient.content_hash(doc["content"]), digest(doc["content"]))
