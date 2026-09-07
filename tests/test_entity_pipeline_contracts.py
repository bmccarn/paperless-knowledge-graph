"""Document-lifetime evidence and metadata-role contracts (synthetic inputs)."""
import copy
import json
import os
import unittest
import uuid
from unittest.mock import AsyncMock, patch

from tests.runtime import configure_test_environment
configure_test_environment()

from app import pipeline
from app.entity_bindings import DocumentBindings, ResolvedEntity
from app.graph import GraphStore
from app.relationship_support import merge_support_properties, support_records
from tests.test_storage_integrity import local_url


class FixedResolver:
    """Already adjudicated identities; these tests exercise consumers, not matching."""
    def __init__(self, identities=None):
        self.identities = identities or {}
        self.hydrate_review_identities = AsyncMock(return_value={})

    async def resolve(self, name, kind, doc_id, **kwargs):
        return self.identities.get(name, f"accepted-{kind}-{name}")


class SupportGraph:
    """Match GraphStore's replacement of one source's previous edge support."""
    def __init__(self):
        self.edges, self.calls = {}, []
        self.created = 0

    async def create_node(self, kind, props):
        self.created += 1
        return f"result-{self.created}"

    async def create_document_node(self, **kwargs):
        return str(kwargs["paperless_id"])

    async def create_relationship(self, left, left_type, right, right_type, kind, props):
        key = (str(left), str(right), kind)
        records = support_records(self.edges.get(key, {}))
        records.pop(props.get("source_doc"), None)
        self.edges[key] = merge_support_properties(
            {"support_records": [json.dumps(r) for r in records.values()]}, props)
        self.calls.append((str(left), left_type, str(right), right_type, kind))

    async def delete_document_graph(self, doc_id):
        for key in list(self.edges):
            records = support_records(self.edges[key])
            records.pop(doc_id, None)
            if records:
                self.edges[key] = merge_support_properties(
                    {"support_records": [json.dumps(r) for r in records.values()]})
            else:
                del self.edges[key]


def alias_fixture(doc_id=101, source=""):
    names = ("Example Laboratories", "Example Labs", "Cedar Holdings", "Cedar Group")
    entities = []
    for index, name in enumerate(names):
        start = source.find(name) if source else 0
        entities.append({"entity_id": str(index), "name": name, "type": "Organization",
                         "confidence": .9, "description": "",
                         "evidence": [{"start": start, "end": start + len(name), "quote": name}] if source else []})
    relationships = []
    for left, right, quote in ((0, 2, "first assertion"), (1, 3, "second assertion")):
        relationships.append({"from_entity": names[left], "from_entity_id": str(left),
            "from_type": "Organization", "to_entity": names[right], "to_entity_id": str(right),
            "to_type": "Organization", "relationship": "PARTNERS_WITH", "confidence": .9,
            "inferred": False, "rationale": "synthetic reviewed assertion",
            "evidence": [{"start": left * 20, "end": left * 20 + len(quote), "quote": quote}]})
    return {"all_entities": entities, "implied_relationships": relationships,
            "confidence": .9, "extraction_coverage": {"status": "complete"}}


class PipelineContractTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.graph = SupportGraph()
        self.resolver = FixedResolver({"Example Laboratories": "org-a", "Example Labs": "org-a",
                                      "Cedar Holdings": "org-b", "Cedar Group": "org-b"})
        self.patchers = [patch.object(pipeline, "graph_store", self.graph),
                         patch.object(pipeline, "entity_resolver", self.resolver)]
        for item in self.patchers:
            item.start()
        self.addCleanup(lambda: [item.stop() for item in reversed(self.patchers)])

    async def test_implied_alias_edges_union_quotes_and_preserve_other_sources(self):
        extracted = alias_fixture()
        bindings = DocumentBindings(101, extracted, "", self.resolver)
        await pipeline._process_extraction(101, "101", "general", extracted, bindings=bindings)
        self.assertIsNone(pipeline._document_bindings.get())
        await self.graph.create_relationship("org-a", "Organization", "org-b", "Organization", "PARTNERS_WITH",
            {"source_doc": 202, "evidence_json": json.dumps([{"quote": "other document"}])})
        await pipeline._process_implied_relationships(101, extracted, bindings=bindings)
        records = support_records(self.graph.edges[("org-a", "org-b", "PARTNERS_WITH")])
        self.assertEqual(len(records[101]["evidence_spans"]), 2)
        self.assertEqual(records[202]["evidence_spans"], [{"quote": "other document"}])
        await pipeline._process_implied_relationships(101, extracted, bindings=bindings)
        self.assertEqual(len(support_records(self.graph.edges[("org-a", "org-b", "PARTNERS_WITH")])[101]["evidence_spans"]), 2)
        self.assertIsNone(pipeline._document_bindings.get())

    async def test_new_generation_replaces_own_support_not_other_document(self):
        old = alias_fixture()
        binding = DocumentBindings(101, old, "", self.resolver)
        await binding.resolve_all()
        await pipeline._process_implied_relationships(101, old, bindings=binding)
        await self.graph.create_relationship("org-a", "Organization", "org-b", "Organization", "PARTNERS_WITH",
            {"source_doc": 202, "evidence_json": json.dumps([{"quote": "unrelated source"}])})
        await self.graph.delete_document_graph(101)
        current = alias_fixture()
        current["implied_relationships"] = current["implied_relationships"][:1]
        current["implied_relationships"][0]["evidence"] = [{"quote": "current generation only"}]
        fresh = DocumentBindings(101, current, "", self.resolver)
        await fresh.resolve_all()
        await pipeline._process_implied_relationships(101, current, bindings=fresh)
        rows = support_records(self.graph.edges[("org-a", "org-b", "PARTNERS_WITH")])
        self.assertEqual(rows[101]["evidence_spans"], [{"quote": "current generation only"}])
        self.assertEqual(rows[202]["evidence_spans"], [{"quote": "unrelated source"}])

    async def test_context_restored_on_failed_implied_write(self):
        extracted = alias_fixture()
        bindings = DocumentBindings(101, extracted, "", self.resolver)
        await bindings.resolve_all()
        outer = object()
        token = pipeline._document_bindings.set(outer)
        try:
            with patch.object(self.graph, "create_relationship", AsyncMock(side_effect=RuntimeError("synthetic failure"))):
                with self.assertRaisesRegex(RuntimeError, "synthetic failure"):
                    await pipeline._process_implied_relationships(101, extracted, bindings=bindings)
            self.assertIs(pipeline._document_bindings.get(), outer)
        finally:
            pipeline._document_bindings.reset(token)

    async def test_relationship_type_normalization_uses_one_accumulator(self):
        bindings = DocumentBindings(101, {"all_entities": []}, "", self.resolver)
        token = pipeline._document_bindings.set(bindings)
        try:
            for kind, quote in (("partners with", "one"), ("PARTNERS_WITH", "two")):
                await pipeline._create_relationship("a", "Organization", "b", "Organization", kind,
                    {"source_doc": 101, "evidence_json": json.dumps([{"quote": quote}])})
        finally:
            pipeline._document_bindings.reset(token)
        rows = support_records(self.graph.edges[("a", "b", "PARTNERS_WITH")])
        self.assertEqual(len(rows[101]["evidence_spans"]), 2)

    async def test_process_document_keeps_bindings_for_all_writers_and_restores_context(self):
        source = "Example Laboratories Example Labs Cedar Holdings Cedar Group"
        extracted = alias_fixture(source=source)
        original = pipeline._process_implied_relationships
        seen = []
        async def inspect(doc_id, result, *, bindings):
            seen.append(pipeline._document_bindings.get() is bindings)
            await original(doc_id, result, bindings=bindings)
        with patch.object(pipeline.paperless_client, "get_skip_tag_ids", AsyncMock(return_value=set())), \
             patch.object(pipeline.paperless_client, "has_any_tag", return_value=False), \
             patch.object(pipeline.embeddings_store, "get_doc_hash", AsyncMock(return_value=None)), \
             patch.object(pipeline.embeddings_store, "get_ingestion_fingerprints", AsyncMock(return_value={})), \
             patch.object(pipeline.embeddings_store, "generate_embedding", AsyncMock(return_value=[1.0, 0.0])), \
             patch.object(pipeline.embeddings_store, "delete_doc_hash", AsyncMock()), \
             patch.object(pipeline.embeddings_store, "delete_document_embeddings", AsyncMock()), \
             patch.object(pipeline.embeddings_store, "store_document_embedding", AsyncMock()), \
             patch.object(pipeline.embeddings_store, "store_entity_embedding", AsyncMock()), \
             patch.object(pipeline.embeddings_store, "set_doc_hash", AsyncMock()), \
             patch.object(pipeline.classifier, "classify", AsyncMock(return_value={"doc_type": "general", "confidence": 1.0})), \
             patch.object(pipeline.extractor, "extract", AsyncMock(return_value=extracted)), \
             patch.object(pipeline, "_generate_document_summary", AsyncMock(return_value="")), \
             patch.object(pipeline, "invalidate_on_sync"), \
             patch.object(pipeline, "_process_implied_relationships", inspect):
            result = await pipeline.process_document({"id": 101, "title": "Synthetic source", "content": source})
        self.assertEqual(result["status"], "processed")
        self.assertEqual(seen, [True])
        self.assertIsNone(pipeline._document_bindings.get())
        self.assertEqual(len(support_records(self.graph.edges[("org-a", "org-b", "PARTNERS_WITH")])[101]["evidence_spans"]), 2)

    async def test_provider_role_retains_only_exact_matching_field_evidence(self):
        name = "Example Assurance Company"
        source = f"Insurer: {name}. Previous provider: Other Bank."
        good = {"start": 0, "end": len(f"Insurer: {name}."), "quote": f"Insurer: {name}."}
        unrelated = {"start": source.index("Other Bank"), "end": source.index("Other Bank") + 10, "quote": "Other Bank"}
        for kind in ("insurance", "medical_lab"):
            for spans, expected in (([good, good, unrelated, {**good, "start": 1}], [good]),
                                    ([unrelated, {**good, "start": 1}], [])):
                with self.subTest(kind=kind, valid=bool(expected)):
                    self.graph.edges.clear()
                    extracted = {"provider": name, "metadata_evidence": {"0": {"provider": spans}},
                                 "all_entities": [{"entity_id": "insurer", "name": name, "type": "Organization",
                                                   "evidence": [good]}]}
                    bindings = DocumentBindings(101, extracted, source, self.resolver)
                    await pipeline._process_extraction(101, "101", kind, extracted, bindings=bindings)
                    props = self.graph.edges[("101", f"accepted-Organization-{name}", "PROVIDER_FOR")]
                    self.assertEqual(support_records(props)[101].get("evidence_spans", []), expected)

    async def test_metadata_roles_reject_incompatible_corrected_types_matrix(self):
        cases = [
            ("medical_lab", {"patient_name": "Morgan Lee", "diagnoses": ["Atlas"]}, "DIAGNOSED_WITH"),
            ("medical_lab", {"ordering_physician": "Atlas"}, "AUTHORED_BY"),
            ("medical_lab", {"provider": "Atlas"}, "PROVIDER_FOR"),
            ("financial_invoice", {"vendor": "Atlas"}, "INVOICED_BY"),
            ("legal_contract", {"parties": [{"name": "Atlas", "role": "signatory"}]}, "CONTRACTED_WITH"),
            ("insurance", {"provider": "Atlas", "policyholder": "Atlas"}, "COVERS"),
            ("government_tax", {"filer_name": "Atlas", "preparer": "Atlas"}, "PREPARED_BY"),
            ("military", {"service_member": "Morgan Lee", "conditions": ["Atlas"]}, "HAS_CONDITION"),
            ("military", {"branch": "Atlas", "unit": "Atlas", "base": "Atlas"}, "STATIONED_AT"),
        ]
        for doc_type, metadata, relation in cases:
            with self.subTest(doc_type=doc_type, relation=relation):
                self.graph.calls.clear()
                extracted = {**metadata, "all_entities": [
                    {"entity_id": "p", "name": "Morgan Lee", "type": "Person"},
                    {"entity_id": "s", "name": "Atlas", "type": "System"}]}
                await pipeline._process_extraction(103, "103", doc_type, extracted)
                system_uuid = "accepted-System-Atlas"
                claims = [r for r in self.graph.calls if r[2] == system_uuid and r[4] != "MENTIONS"]
                self.assertEqual(claims, [])
                self.assertTrue(any(r[2] == system_uuid and r[4] == "MENTIONS" for r in self.graph.calls))

    async def test_valid_metadata_role_types_are_preserved_matrix(self):
        cases = [
            ("medical_lab", {"diagnoses": ["Atlas"]}, "Condition", "DIAGNOSED_WITH"),
            ("medical_lab", {"provider": "Atlas"}, "Person", "PROVIDER_FOR"),
            ("medical_lab", {"provider": "Atlas"}, "Organization", "PROVIDER_FOR"),
            ("financial_invoice", {"vendor": "Atlas"}, "Person", "INVOICED_BY"),
            ("financial_invoice", {"vendor": "Atlas"}, "Organization", "INVOICED_BY"),
            ("legal_contract", {"parties": [{"name": "Atlas", "role": "signatory"}]}, "Organization", "CONTRACTED_WITH"),
            ("insurance", {"policyholder": "Atlas"}, "Organization", "COVERS"),
            ("government_tax", {"preparer": "Atlas"}, "Organization", "PREPARED_BY"),
            ("property_home", {"parties": [{"name": "Atlas"}]}, "Organization", "MENTIONS"),
            ("military", {"base": "Atlas"}, "Address", "STATIONED_AT"),
            ("general", {}, "System", "MENTIONS"),
        ]
        for doc_type, metadata, kind, relation in cases:
            with self.subTest(doc_type=doc_type, kind=kind, relation=relation):
                self.graph.calls.clear()
                extracted = {**metadata, "all_entities": [{"entity_id": "e", "name": "Atlas", "type": kind}]}
                await pipeline._process_extraction(104, "104", doc_type, extracted)
                self.assertIn(("104", "Document", f"accepted-{kind}-Atlas", kind, relation), self.graph.calls)

    async def test_reviewed_implied_roles_are_not_restricted_by_metadata_assumptions(self):
        extracted = {"all_entities": [{"entity_id": "p", "name": "Morgan Lee", "type": "Person"},
                                      {"entity_id": "s", "name": "Atlas", "type": "System"}],
                     "implied_relationships": [{"from_entity": "Morgan Lee", "from_entity_id": "p", "from_type": "Person",
                         "to_entity": "Atlas", "to_entity_id": "s", "to_type": "System", "relationship": "OPERATES",
                         "confidence": .9, "inferred": False, "evidence": [{"quote": "Morgan Lee operates Atlas."}]}]}
        bindings = DocumentBindings(105, extracted, "", self.resolver)
        await bindings.resolve_all()
        await pipeline._process_implied_relationships(105, extracted, bindings=bindings)
        self.assertIn(("accepted-Person-Morgan Lee", "Person", "accepted-System-Atlas", "System", "OPERATES"), self.graph.calls)


@unittest.skipUnless(os.environ.get("NEO4J_TEST_BOLT"), "Set disposable localhost Neo4j test URL")
class PipelineDatastoreContracts(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        from neo4j import AsyncGraphDatabase
        self.prefix = "pipeline-contract-" + str(uuid.uuid4()) + "-"
        self.left, self.right = self.prefix + "left", self.prefix + "right"
        self.graph = GraphStore()
        self.graph.driver = AsyncGraphDatabase.driver(local_url("NEO4J_TEST_BOLT"), auth=None)
        self.resolver = FixedResolver({"Example Laboratories": self.left, "Example Labs": self.left,
                                      "Cedar Holdings": self.right, "Cedar Group": self.right})
        async with self.graph.driver.session() as session:
            await session.run("CREATE (:Organization {uuid:$left,name:'Example Laboratories'}), (:Organization {uuid:$right,name:'Cedar Holdings'}), (:Document {paperless_id:991731}), (:Document {paperless_id:991732})", left=self.left, right=self.right)

    async def asyncTearDown(self):
        async with self.graph.driver.session() as session:
            await session.run("MATCH (n) WHERE n.uuid STARTS WITH $prefix OR n.paperless_id IN [991731,991732] DETACH DELETE n", prefix=self.prefix)
        await self.graph.driver.close()

    async def edge_support(self):
        async with self.graph.driver.session() as session:
            result = await session.run("MATCH (a {uuid:$left})-[r:PARTNERS_WITH]->(b {uuid:$right}) RETURN properties(r) AS props", left=self.left, right=self.right)
            row = await result.single()
            return support_records(row["props"])

    async def test_provider_field_quotes_roundtrip_repeat_and_source_replacement(self):
        name = "Example Laboratories"
        async def process(doc_id, source):
            span = {"start": 0, "end": len(source), "quote": source}
            extracted = {"provider": name, "metadata_evidence": {"0": {"provider": [span]}},
                         "all_entities": [{"entity_id": "provider", "name": name, "type": "Organization",
                                           "evidence": [span]}]}
            bindings = DocumentBindings(doc_id, extracted, source, self.resolver)
            await pipeline._process_extraction(doc_id, str(doc_id), "medical_lab", extracted, bindings=bindings)
        async def records():
            async with self.graph.driver.session() as session:
                result = await session.run("MATCH (d:Document)-[r:PROVIDER_FOR]->(n {uuid:$uuid}) "
                                          "RETURN d.paperless_id AS id, properties(r) AS props", uuid=self.left)
                return {row["id"]: support_records(row["props"]) async for row in result}
        with patch.object(pipeline, "graph_store", self.graph):
            await process(991731, f"Provider: {name}.")
            await process(991732, f"Other source provider: {name}.")
            before = await records()
            self.assertEqual(before[991731][991731]["evidence_spans"][0]["quote"], f"Provider: {name}.")
            await process(991731, f"Provider: {name}.")
            self.assertEqual(await records(), before)
            await process(991731, f"Current clinic: {name}.")
            after = await records()
            self.assertEqual(after[991731][991731]["evidence_spans"],
                             [{"start": 0, "end": len(f"Current clinic: {name}."), "quote": f"Current clinic: {name}."}])
            self.assertEqual(after[991732], before[991732])

    async def test_pipeline_union_idempotence_and_source_replacement_on_real_graph(self):
        extracted = alias_fixture()
        bindings = DocumentBindings(991731, extracted, "", self.resolver)
        with patch.object(pipeline, "graph_store", self.graph):
            await pipeline._process_extraction(991731, "991731", "general", extracted, bindings=bindings)
            await self.graph.create_relationship(self.left, "Organization", self.right, "Organization", "PARTNERS_WITH",
                {"source_doc": 991732, "evidence_json": json.dumps([{"quote": "other source preserved"}])})
            await pipeline._process_implied_relationships(991731, extracted, bindings=bindings)
            first = await self.edge_support()
            self.assertEqual(len(first[991731]["evidence_spans"]), 2)
            await pipeline._process_implied_relationships(991731, extracted, bindings=bindings)
            self.assertEqual(await self.edge_support(), first)
            await self.graph.delete_document_graph(991731)
            after_delete = await self.edge_support()
            self.assertEqual(after_delete, {991732: first[991732]})
            new = copy.deepcopy(extracted)
            new["implied_relationships"] = new["implied_relationships"][:1]
            new["implied_relationships"][0]["evidence"] = [{"quote": "new generation"}]
            fresh = DocumentBindings(991731, new, "", self.resolver)
            await fresh.resolve_all()
            await pipeline._process_implied_relationships(991731, new, bindings=fresh)
            final = await self.edge_support()
            self.assertEqual(final[991731]["evidence_spans"], [{"quote": "new generation"}])
            self.assertEqual(final[991732], first[991732])
