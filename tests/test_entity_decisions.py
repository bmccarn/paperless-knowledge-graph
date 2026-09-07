"""Public entity-resolution/review regressions using controlled storage adapters."""
import copy
import asyncio
import json
import os
import unittest
import uuid
from urllib.parse import urlparse
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from tests.runtime import configure_test_environment

configure_test_environment()
os.environ["LITELLM_LOCAL_MODEL_COST_MAP"] = "True"

from app import entity_resolver as resolver_module
from app import entity_steward as steward_module
from app import main as main_module
from app import cache as cache_module
from app.graph import GraphStore
from neo4j import AsyncGraphDatabase


class MemoryDecisions:
    def __init__(self, rows=()):
        self.rows = copy.deepcopy(list(rows))
        self.unavailable = False

    async def get_entity_review_decisions(self):
        if self.unavailable:
            raise ConnectionError("decision store unavailable")
        return copy.deepcopy(self.rows)

    async def preserve_alias_revocations(self, rows):
        if self.unavailable:
            raise ConnectionError("decision store unavailable")
        for row in rows:
            if not any((old["left_uuid"], old["right_uuid"], old["decision"]) ==
                       (row["left_uuid"], row["right_uuid"], row["decision"]) for old in self.rows):
                self.rows.append(copy.deepcopy(row))

    async def add_entity_review_decision(self, left_uuid, right_uuid, decision, note="", **identities):
        if self.unavailable:
            raise ConnectionError("decision store unavailable")
        row = {"left_uuid": left_uuid, "right_uuid": right_uuid, "decision": decision, "note": note, **identities}
        if left_uuid > right_uuid:
            row["left_uuid"], row["right_uuid"] = right_uuid, left_uuid
            row["left_identity"], row["right_identity"] = row.get("right_identity"), row.get("left_identity")
        self.rows = [old for old in self.rows if (old["left_uuid"], old["right_uuid"], old["decision"]) != (row["left_uuid"], row["right_uuid"], decision)]
        self.rows.append(copy.deepcopy(row))
        return copy.deepcopy(row)

    async def hydrate_entity_review_identities(self, incoming, status):
        for row in self.rows:
            if (row["left_uuid"], row["right_uuid"], row["decision"]) == (incoming["left_uuid"], incoming["right_uuid"], incoming["decision"]):
                for side in ("left", "right"):
                    if not row.get(f"{side}_identity") and incoming.get(f"{side}_identity"):
                        row[f"{side}_identity"] = copy.deepcopy(incoming[f"{side}_identity"])
                row["identity_status"] = status
                return
        raise ValueError("Legacy decision disappeared")

    async def set_entity_decision_identity_status(self, left_uuid, right_uuid, decision, status):
        for row in self.rows:
            if set((row["left_uuid"], row["right_uuid"])) == set((left_uuid, right_uuid)) and row["decision"] == decision:
                row["identity_status"] = status

    async def generate_embedding(self, text):
        return []


class MemoryGraph:
    def __init__(self, nodes=()):
        self.nodes = {n["uuid"]: copy.deepcopy(n) for n in nodes}
        self.sequence = 0
        self.driver = SimpleNamespace(session=lambda: MemorySession(self))

    async def protect_review_anchor(self, node_uuid):
        if node_uuid in self.nodes:
            self.nodes[node_uuid]["review_anchor"] = True

    async def get_entities_by_type(self, kind):
        return [copy.deepcopy(n) for n in self.nodes.values() if n["entity_type"] == kind]

    async def record_entity_source(self, node_uuid, doc_id, *, identity_hint=""):
        node = self.nodes[node_uuid]
        node["source_doc_ids"] = sorted(set(node.get("source_doc_ids", [])) | {doc_id})
        if identity_hint:
            node["identity_hints"] = sorted(set(node.get("identity_hints", [])) | {identity_hint})

    async def add_entity_alias_record(self, node_uuid, record):
        node = self.nodes[node_uuid]
        node.setdefault("alias_records", []).append(copy.deepcopy(record))
        node.setdefault("aliases", []).append(record["alias"])

    async def get_all_persons(self):
        return [copy.deepcopy(n) for n in self.nodes.values() if n["entity_type"] == "Person"]

    async def get_all_organizations(self):
        return [copy.deepcopy(n) for n in self.nodes.values() if n["entity_type"] == "Organization"]

    async def find_person(self, name):
        return self.find(name, "Person")

    async def find_organization(self, name):
        return self.find(name, "Organization")

    def find(self, name, label):
        for node in self.nodes.values():
            if node["entity_type"] == label and name.lower() in [s.lower() for s in [node["name"], *node.get("aliases", [])]]:
                return copy.deepcopy(node)
        return None

    async def get_node(self, node_uuid):
        node = self.nodes.get(node_uuid)
        return {"labels": [node["entity_type"]], "properties": copy.deepcopy(node), "relationships": []} if node else None

    async def create_node(self, label, properties):
        self.sequence += 1
        node_uuid = f"created-{self.sequence}"
        self.nodes[node_uuid] = {**copy.deepcopy(properties), "uuid": node_uuid, "entity_type": label}
        return node_uuid

    async def create_person(self, **properties):
        return await self.create_node("Person", properties)

    async def create_organization(self, **properties):
        return await self.create_node("Organization", properties)

    async def add_person_alias(self, node_uuid, alias):
        self.nodes[node_uuid].setdefault("aliases", []).append(alias)

    add_org_alias = add_person_alias

    async def merge_entities(self, primary_uuid, duplicate_uuid, *, review_id=None, review_method=None):
        primary = self.nodes[primary_uuid]
        duplicate = self.nodes.pop(duplicate_uuid)
        primary["aliases"] = list(dict.fromkeys([*primary.get("aliases", []), duplicate["name"], *duplicate.get("aliases", [])]))
        primary["source_doc_ids"] = sorted(set(primary.get("source_doc_ids", [])) | set(duplicate.get("source_doc_ids", [])))
        if review_id:
            from app.entity_policy import human_alias_record, trusted_aliases
            aliases = trusted_aliases(duplicate, primary["entity_type"], -1, "")
            for alias in [duplicate["name"], *aliases]:
                primary.setdefault("alias_records", []).append(human_alias_record(primary["name"], alias, primary["entity_type"], review_id, review_method="entity_review_api"))
        return await self.get_node(primary_uuid)


class MemorySession:
    """Legacy graph-driver adapter; public tests do not inspect its queries."""
    def __init__(self, graph):
        self.graph = graph

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        return False

    async def run(self, query, **values):
        record = None
        records = []
        if "toLower($name)" in query:
            for node in self.graph.nodes.values():
                if node["name"].lower() == values["name"].lower():
                    records.append({"uuid": node["uuid"], "props": copy.deepcopy(node)})
            record = records[0] if records else None
        elif "DETACH DELETE n" in query:
            self.graph.nodes.pop(values["uuid"], None)
        elif "SET n.name = $name" in query:
            self.graph.nodes[values["uuid"]]["name"] = values["name"]
        elif "n.aliases = CASE" in query:
            await self.graph.add_person_alias(values["uuid"], values["alias"])
        elif "SET n.description = $desc" in query:
            self.graph.nodes[values["uuid"]]["description"] = values["desc"]
        async def single():
            return record
        async def data():
            return records
        return SimpleNamespace(single=single, data=data)


def person(node_uuid, name="John Smith", documents=()):
    return {"uuid": node_uuid, "name": name, "entity_type": "Person", "aliases": [], "source_doc_ids": list(documents)}


class EntityDecisionTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.graph = MemoryGraph([person("left"), person("right")])
        self.decisions = MemoryDecisions()
        self.patch_graph = patch.object(resolver_module, "graph_store", self.graph)
        self.patch_decisions = patch.object(resolver_module, "embeddings_store", self.decisions)
        self.patch_graph.start()
        self.patch_decisions.start()
        self.addCleanup(self.patch_graph.stop)
        self.addCleanup(self.patch_decisions.stop)
        self.resolver = resolver_module.EntityResolver()

    async def test_split_prevents_an_otherwise_eligible_bulk_merge(self):
        self.decisions.rows = [{"left_uuid": "right", "right_uuid": "left", "decision": "split", "note": "different people"}]
        report = await self.resolver.resolve_all_entities()
        self.assertEqual(report["errors"], [])
        self.assertEqual(report["total_merged"], 0)
        self.assertTrue(any("decision" in row["reason"] for row in report["skipped"]))

    async def test_split_acceptance_waits_for_inflight_identity_resolution(self):
        self.graph.nodes = {"left": person("left", documents=[11]), "right": person("right", documents=[22])}
        entered, release, accepted = asyncio.Event(), asyncio.Event(), asyncio.Event()
        original = self.graph.get_entities_by_type
        async def paused_find(name):
            entered.set()
            await release.wait()
            return await original(name)
        self.graph.get_entities_by_type = paused_find
        lookup = asyncio.create_task(self.resolver.resolve_person("John Smith", 22))
        await entered.wait()
        async def record():
            result = await self.resolver.record_decision("left", "right", "split", review_method="entity_review_api")
            accepted.set()
            return result
        decision = asyncio.create_task(record())
        try:
            await asyncio.sleep(0)
            self.assertFalse(accepted.is_set(), "A split cannot be acknowledged while an older lookup is still deciding")
        finally:
            release.set()
            await lookup
            await decision
        self.assertEqual(await self.resolver.resolve_person("John Smith", 22), "right")

    async def test_review_serialization_covers_organizations_and_generic_entities(self):
        for kind, name in (("Organization", "Example Company"), ("Condition", "Example Condition")):
            with self.subTest(kind=kind):
                self.graph.nodes = {key: {**person(key, name, docs), "entity_type": kind}
                                    for key, docs in (("left", [11]), ("right", [22]))}
                self.decisions.rows = []
                entered, release = asyncio.Event(), asyncio.Event()
                original_read = self.decisions.get_entity_review_decisions
                async def paused_read():
                    rows = await original_read()
                    entered.set()
                    await release.wait()
                    return rows
                with patch.object(self.decisions, "get_entity_review_decisions", paused_read):
                    resolve = lambda: self.resolver.resolve_organization(name, 22) if kind == "Organization" else self.resolver.resolve_generic(name, kind, 22)
                    lookup = asyncio.create_task(resolve())
                    await entered.wait()
                    decision = asyncio.create_task(self.resolver.record_decision("left", "right", "split", review_method="entity_review_api"))
                    try:
                        await asyncio.sleep(0)
                        self.assertFalse(decision.done())
                    finally:
                        release.set()
                        await lookup
                        await decision
                    self.assertEqual(await resolve(), "right")

    async def test_source_less_person_type_is_not_rewritten_from_name(self):
        result = await asyncio.wait_for(self.resolver.resolve_person("Example Insurance", 11), timeout=1)
        self.assertEqual((await self.graph.get_node(result))["labels"], ["Person"])

    async def test_generic_resolution_after_deletion_returns_a_live_identity(self):
        original = await self.resolver.resolve_generic("Example Condition", "Condition", 11)
        self.graph.nodes.pop(original)
        replacement = await self.resolver.resolve_generic("Example Condition", "Condition", 11)
        self.assertNotEqual(replacement, original)
        self.assertIsNotNone(await self.graph.get_node(replacement))

    async def test_recorded_split_survives_reindex_with_new_graph_uuids(self):
        self.graph.nodes["left"] = person("left", "John Smith", [11])
        self.graph.nodes["right"] = person("right", "John Smyth", [22])
        await self.resolver.record_decision("left", "right", "split", "different people", review_method="entity_review_api")
        self.graph.nodes = {
            "new-left": person("new-left", "John Smith", [11]),
            "new-right": person("new-right", "John Smyth", [22]),
        }
        report = await self.resolver.resolve_all_entities()
        self.assertEqual(report["errors"], [])
        self.assertEqual(report["total_merged"], 0)
        self.assertTrue(any("decision" in row["reason"] for row in report["skipped"]))

    async def test_ingestion_does_not_match_a_prohibited_fuzzy_person(self):
        self.graph.nodes["left"] = person("left", "John Smith", [11])
        self.graph.nodes["right"] = person("right", "John Smyth", [22])
        await self.resolver.record_decision("left", "right", "never_merge", review_method="entity_review_api")
        self.graph.nodes.pop("right")
        resolved = await self.resolver.resolve_person("John Smyth", 22)
        self.assertNotEqual(resolved, "left")
        self.assertEqual((await self.graph.get_node(resolved))["properties"]["name"], "John Smyth")

    async def test_legacy_veto_is_hydrated_before_its_graph_identity_disappears(self):
        self.graph.nodes["right"]["name"] = "John Smyth"
        self.decisions.rows = [{"left_uuid": "left", "right_uuid": "right", "decision": "never_merge", "note": "different people"}]
        report = await self.resolver.hydrate_review_identities()
        self.assertEqual(report["hydrated"], 1)
        self.assertEqual(report["unresolved"], [])
        self.assertEqual((await self.resolver.hydrate_review_identities())["hydrated"], 0)
        self.graph.nodes = {"new-left": person("new-left"), "new-right": person("new-right", "John Smyth")}
        merged = await self.resolver.resolve_all_entities()
        self.assertEqual(merged["total_merged"], 0)

    async def test_manual_merge_rejects_a_veto_and_keeps_both_entities(self):
        await self.resolver.record_decision("left", "right", "split", review_method="entity_review_api")
        with self.assertRaisesRegex(ValueError, "decision"):
            await self.resolver.merge_entities("right", "left", review_method="entity_review_api")
        self.assertIsNotNone(await self.graph.get_node("left"))
        self.assertIsNotNone(await self.graph.get_node("right"))

    async def test_allowed_canonical_merge_preserves_existing_veto(self):
        self.graph.nodes["left"] = person("left", "John Smith", [11])
        self.graph.nodes["right"] = person("right", "John Smyth", [22])
        self.graph.nodes["canonical"] = person("canonical", "John Alexander Smith", [11])
        await self.resolver.record_decision("left", "right", "never_merge", review_method="entity_review_api")
        merged = await self.resolver.merge_entities("canonical", "left", review_method="entity_review_api")
        self.assertEqual(merged["properties"]["uuid"], "canonical")
        self.graph.nodes = {
            "rebuilt-canonical": person("rebuilt-canonical", "John Alexander Smith", [11]),
            "rebuilt-right": person("rebuilt-right", "John Smyth", [22]),
        }
        with self.assertRaisesRegex(ValueError, "decision"):
            await self.resolver.merge_entities("rebuilt-canonical", "rebuilt-right", review_method="entity_review_api")

    async def test_human_merge_carries_a_related_veto_to_the_surviving_name(self):
        self.graph.nodes = {
            "canonical": person("canonical", "John Alexander Smith", [11]),
            "left": person("left", "John Smith", [11]),
            "right": person("right", "John Smyth", [22]),
        }
        await self.resolver.record_decision("left", "right", "split", review_method="entity_review_api")
        report = await self.resolver.resolve_all_entities()
        self.assertEqual(report["total_merged"], 0)
        await self.resolver.merge_entities("canonical", "left", review_method="entity_review_api")
        self.graph.nodes = {
            "rebuilt-canonical": person("rebuilt-canonical", "John Alexander Smith", [11]),
            "rebuilt-right": person("rebuilt-right", "John Smyth", [22]),
        }
        with self.assertRaisesRegex(ValueError, "decision"):
            await self.resolver.merge_entities("rebuilt-canonical", "rebuilt-right", review_method="entity_review_api")

    async def test_same_name_split_reuses_the_entity_for_the_correct_source(self):
        self.graph.nodes = {
            node_uuid: {**person(node_uuid, "Shared entity", docs), "entity_type": "Condition"}
            for node_uuid, docs in (("left", [11]), ("right", [22]))
        }
        await self.resolver.record_decision("left", "right", "split", review_method="entity_review_api")
        resolved = await self.resolver.resolve_generic("Shared entity", "Condition", 22)
        self.assertEqual(resolved, "right")
        self.assertEqual(await self.resolver.resolve_generic("Shared entity", "Condition", 22), "right")

    async def test_review_routes_record_identities_and_report_a_prohibited_merge(self):
        with patch.object(main_module, "entity_resolver", self.resolver), patch.object(main_module, "embeddings_store", self.decisions), patch.object(main_module, "graph_store", self.graph):
            recorded = await main_module.entity_review_split(main_module.EntityDecisionRequest(left_uuid="left", right_uuid="right", note="different people"))
            self.assertEqual(recorded["decision"]["left_identity"]["type"], "Person")
            self.assertEqual(recorded["decision"]["review_method"], "entity_review_api")
            self.assertTrue(recorded["decision"]["review_id"])
            with self.assertRaises(main_module.HTTPException) as raised:
                await main_module.entity_review_merge(main_module.EntityMergeRequest(primary_uuid="left", duplicate_uuid="right"))
            self.assertEqual(raised.exception.status_code, 409)

    async def test_decision_store_failure_prevents_resolution_and_merge(self):
        self.decisions.unavailable = True
        original = copy.deepcopy(self.graph.nodes)
        for operation in (
            lambda: self.resolver.resolve_person("John Smith", 11),
            lambda: self.resolver.resolve_organization("Example Company", 11),
            lambda: self.resolver.resolve_generic("Example Condition", "Condition", 11),
            lambda: self.resolver.merge_entities("left", "right", review_method="entity_review_api"),
            lambda: self.resolver.resolve_all_entities(),
        ):
            with self.assertRaises(ConnectionError):
                await operation()
            self.assertEqual(self.graph.nodes, original)

    async def test_ignored_pair_can_merge_without_overriding_an_existing_veto(self):
        await self.resolver.record_decision("left", "right", "ignore", review_method="entity_review_api")
        self.assertEqual((await self.resolver.resolve_all_entities())["total_merged"], 0)
        await self.resolver.merge_entities("left", "right", review_method="entity_review_api")
        self.graph.nodes = {"left": person("left"), "right": person("right")}
        await self.resolver.record_decision("left", "right", "split", review_method="entity_review_api")
        await self.resolver.record_decision("left", "right", "ignore", review_method="entity_review_api")
        self.assertEqual((await self.resolver.resolve_all_entities())["total_merged"], 0)

    async def test_same_name_person_and_organization_veto_allows_correct_source(self):
        for label, resolve in (("Person", self.resolver.resolve_person), ("Organization", self.resolver.resolve_organization)):
            self.decisions.rows = []
            self.graph.nodes = {key: {**person(key, "Shared Name", docs), "entity_type": label}
                                for key, docs in (("left", [11]), ("right", [22]))}
            await self.resolver.record_decision("left", "right", "split", review_method="entity_review_api")
            self.assertEqual(await resolve("Shared Name", 22), "right")
            self.graph.nodes.pop("right")
            replacement = await resolve("Shared Name", 22)
            self.assertNotEqual(replacement, "left")
            self.assertEqual(await resolve("Shared Name", 22), replacement)

    async def test_name_only_model_has_no_identity_authority(self):
        self.graph.nodes = {"left": person("left")}
        model = AsyncMock(side_effect=AssertionError("No name-only model may be called"))
        with patch("openai.AsyncOpenAI", model):
            for name in ("John Smythee", "Jonathan Smith", "John A Smith"):
                self.assertNotEqual(await self.resolver.resolve_person(name, 22), "left")
        model.assert_not_called()

    async def test_steward_records_review_for_unsafe_agent_merge(self):
        candidate = {"score": 100, "label": "Person", "left": {"uuid": "left", "name": "John Smith"},
                     "right": {"uuid": "right", "name": "John Smith"}}
        graph = SimpleNamespace(get_entity_review_candidates=AsyncMock(return_value=[candidate]))
        reviewer = SimpleNamespace(review_entity_candidate=AsyncMock(return_value={"recommendation": "merge", "risk": "low", "confidence": .99}))
        with patch.object(steward_module, "graph_store", graph), patch.object(steward_module, "embeddings_store", self.decisions), patch.object(steward_module, "strands_orchestrator", reviewer):
            report = await steward_module.EntitySteward().run_once()
        self.assertEqual(report["suggest_review"], 1)
        self.assertEqual(report["suggest_merge"], 0)
        self.assertEqual(self.decisions.rows[-1]["decision"], "suggest_review")

    async def test_failed_merge_routes_clear_answers_from_before_partial_mutation(self):
        failing = SimpleNamespace(resolve_all_entities=AsyncMock(side_effect=RuntimeError("metadata failed after graph write")),
                                  merge_entities=AsyncMock(side_effect=RuntimeError("metadata failed after graph write")))
        for operation in (main_module.resolve_entities,
                          lambda: main_module.entity_review_merge(main_module.EntityMergeRequest(primary_uuid="left", duplicate_uuid="right"))):
            cache_module.query_cache.set("answer-before-merge", {"answer": "old entity identity"})
            with patch.object(main_module, "entity_resolver", failing), self.assertRaises(main_module.HTTPException):
                await operation()
            self.assertIsNone(cache_module.query_cache.get("answer-before-merge"))


class StewardRecommendationTests(unittest.TestCase):
    def test_merge_suggestion_requires_both_low_risk_and_confident_assessment(self):
        for deterministic_risk, agent_risk, confidence in [
            ("high", "high", .01), ("high", "low", .99),
            ("low", "high", .99), ("low", "low", .81),
            ("low", "low", "unknown"), ("low", "low", float("nan")),
            ("low", "low", float("inf")), ("low", "low", 2),
            ("low", "low", True), ("low", None, .99),
        ]:
            with self.subTest(deterministic_risk=deterministic_risk, agent_risk=agent_risk, confidence=confidence):
                result = steward_module.choose_recommendation(
                    {"recommendation": "review", "risk": deterministic_risk},
                    {"recommendation": "merge", "risk": agent_risk, "confidence": confidence},
                )
                self.assertEqual(result, "review")
        self.assertEqual(steward_module.choose_recommendation(
            {"recommendation": "review", "risk": "low"},
            {"recommendation": "merge", "risk": "low", "confidence": .82},
        ), "merge")


@unittest.skipUnless(os.environ.get("NEO4J_TEST_BOLT"), "Set NEO4J_TEST_BOLT to explicitly authorized disposable localhost Neo4j")
class GraphEntityMergeTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        uri = os.environ["NEO4J_TEST_BOLT"]
        if urlparse(uri).hostname not in {"localhost", "127.0.0.1"}:
            raise ValueError("Entity merge tests require disposable localhost Neo4j")
        self.driver = AsyncGraphDatabase.driver(uri, auth=None)
        self.store = GraphStore()
        self.store.driver = self.driver
        self.prefix = f"entity-merge-test-{uuid.uuid4()}-"
        self.keep, self.remove, self.org = [self.prefix + suffix for suffix in ("keep", "remove", "org")]
        async with self.driver.session(database="neo4j") as session:
            await session.run("""
                CREATE (a:Person {uuid:$keep, name:'John Smith', aliases:['First'], entity_type:'Person', source_doc_ids:[11]}),
                       (b:Person {uuid:$remove, name:'John Smyth', aliases:['Second'], entity_type:'Person', source_doc_ids:[22]}),
                       (o:Organization {uuid:$org, name:'Example Company'}),
                       (a)-[:WORKS_AT {source_doc:11, confidence:0.9, inferred:false}]->(o),
                       (b)-[:WORKS_AT {source_doc:22, confidence:0.8, inferred:true}]->(o)
                """, keep=self.keep, remove=self.remove, org=self.org)

    async def asyncTearDown(self):
        async with self.driver.session(database="neo4j") as session:
            await session.run("MATCH (n) WHERE n.uuid STARTS WITH $prefix DETACH DELETE n", prefix=self.prefix)
        await self.driver.close()

    async def test_bulk_resolution_leaves_same_source_homonyms_for_human_review(self):
        for kind, name in (("Person", "John Smith"), ("Organization", "Example Company")):
            with self.subTest(kind=kind):
                async with self.driver.session() as session:
                    await session.run("MATCH (n) WHERE n.uuid STARTS WITH $prefix DETACH DELETE n",
                                      prefix=self.prefix)
                    for node_uuid, documents in ((self.keep, [11]), (self.remove, [22]), (self.org, [11])):
                        await session.run(
                            f"CREATE (n:{kind} {{uuid:$uuid, name:$name, entity_type:$kind, source_doc_ids:$docs}})",
                            uuid=node_uuid, name=name, kind=kind, docs=documents)
                left, right, duplicate = self.keep, self.remove, self.org
                decisions = MemoryDecisions()
                resolver = resolver_module.EntityResolver()
                with patch.object(resolver_module, "graph_store", self.store), \
                     patch.object(resolver_module, "embeddings_store", decisions):
                    await resolver.record_decision(left, right, "split", review_method="entity_review_api")
                    report = await resolver.resolve_all_entities()
                    self.assertEqual(report["errors"], [])
                    survivors = [node for node in (
                        await self.store.get_node(left), await self.store.get_node(duplicate)) if node]
                    self.assertEqual(len(survivors), 2, report)
                    self.assertEqual(survivors[0]["properties"]["source_doc_ids"], [11])
                    other = await self.store.get_node(right)
                    assert other is not None
                    self.assertEqual(other["properties"]["source_doc_ids"], [22])
                    with self.assertRaises(resolver_module.EntityMergeProhibited):
                        await resolver.merge_entities(survivors[0]["properties"]["uuid"], right, review_method="entity_review_api")

    async def test_actual_graph_resolver_roundtrips_source_alias_provenance_and_membership(self):
        from app.entity_policy import trusted_aliases
        from tests.test_entity_review_closure import reviewed_entities, proofs
        resolver = resolver_module.EntityResolver()
        decisions = MemoryDecisions()
        source = "John Smith also known as John Jones signed."
        with patch.object(resolver_module, "graph_store", self.store), patch.object(resolver_module, "embeddings_store", decisions):
            self.assertEqual(await resolver.resolve_person("John Smith", 33), self.keep)
            self.assertEqual(await resolver.resolve_person("John Jones", 33, source=source,
                identity_proofs=proofs(reviewed_entities("John Smith", "John Jones", source, "Person"))), self.keep)
        props = (await self.store.get_node(self.keep))["properties"]
        self.assertEqual(props["source_doc_ids"], [11, 33])
        self.assertEqual(trusted_aliases(props, "Person", 33, source), ["John Jones"])
        self.assertEqual(trusted_aliases(props, "Person", 44, source), [])
        await self.store.delete_document_graph(33)
        self.assertEqual((await self.store.get_node(self.keep))["properties"]["source_doc_ids"], [11])

    async def test_actual_human_merge_blesses_only_reviewed_names_not_all_legacy_aliases(self):
        from app.entity_policy import trusted_aliases
        await self.store.merge_entities(self.keep, self.remove, review_id="synthetic-review-1", review_method="entity_review_api")
        props = (await self.store.get_node(self.keep))["properties"]
        self.assertEqual(trusted_aliases(props, "Person", -1, ""), ["John Smyth"])
        self.assertEqual(set(props["aliases"]), {"First", "Second", "John Smyth"})

    async def test_storage_cannot_mint_human_alias_with_automated_origin(self):
        before_keep = await self.store.get_node(self.keep)
        before_remove = await self.store.get_node(self.remove)
        with self.assertRaisesRegex(ValueError, "explicit review origin"):
            await self.store.merge_entities(self.keep, self.remove, review_id="not-human", review_method="auto_dedup")
        self.assertEqual(await self.store.get_node(self.keep), before_keep)
        self.assertEqual(await self.store.get_node(self.remove), before_remove)

    async def test_relationship_type_mismatch_fails_in_storage_not_silently_rebinds(self):
        before = await self.store.get_node(self.keep)
        with self.assertRaisesRegex(ValueError, "endpoints"):
            await self.store.create_relationship(self.keep, "Condition", self.org, "Organization", "KNOWS", {"source_doc": 11})
        self.assertEqual(await self.store.get_node(self.keep), before)

    async def test_merge_preserves_aliases_and_each_documents_relationship_support(self):
        merged = await self.store.merge_entities(self.keep, self.remove)
        self.assertEqual(set(merged["properties"]["aliases"]), {"First", "Second", "John Smyth"})
        self.assertEqual(merged["properties"]["source_doc_ids"], [11, 22])
        detail = await self.store.get_node(self.keep)
        relations = [edge for edge in detail["relationships"] if edge["rel_type"] == "WORKS_AT"]
        self.assertEqual(len(relations), 1)
        props = relations[0]["rel_props"]
        self.assertEqual(props["source_doc_ids"], [11, 22])
        self.assertEqual({json.loads(record)["source_doc"] for record in props["support_records"]}, {11, 22})
        self.assertEqual(props["confidence"], .8)
        self.assertTrue(props["inferred"])
        self.assertIsNone(await self.store.get_node(self.remove))

    async def test_merge_rejects_partial_uuid_without_mutation(self):
        with self.assertRaises(ValueError):
            await self.store.merge_entities(self.keep[:-2], self.remove)
        self.assertIsNotNone(await self.store.get_node(self.keep))
        self.assertIsNotNone(await self.store.get_node(self.remove))

    async def test_merge_preserves_inbound_support_and_distinct_same_document_quotes(self):
        first = {"source_doc": 11, "confidence": .9, "evidence_spans": [{"text": "John Smith is employed here."}], "rationale": "first quote"}
        second = {"source_doc": 11, "confidence": .8, "evidence_spans": [{"text": "John Smyth is employed here."}], "rationale": "second quote"}
        async with self.driver.session() as session:
            await session.run("""MATCH (a {uuid:$keep})-[r:WORKS_AT]->(), (b {uuid:$remove})-[s:WORKS_AT]->()
                                 SET r.support_records=[$first], s.support_records=[$second], s.source_doc=11
                                 WITH b MATCH (o {uuid:$org}) CREATE (o)-[:EMPLOYS {source_doc:22}]->(b)""",
                              keep=self.keep, remove=self.remove, org=self.org, first=json.dumps(first), second=json.dumps(second))
        await self.store.merge_entities(self.keep, self.remove)
        detail = await self.store.get_node(self.keep)
        edge = next(r for r in detail["relationships"] if r["rel_type"] == "WORKS_AT")
        records = [json.loads(value) for value in edge["rel_props"]["support_records"]]
        self.assertEqual(len(records), 1)
        self.assertCountEqual(records[0]["evidence_spans"], first["evidence_spans"] + second["evidence_spans"])
        self.assertCountEqual(records[0]["rationale"], ["first quote", "second quote"])
        inbound = next(r for r in detail["relationships"] if r["rel_type"] == "EMPLOYS")
        self.assertEqual(inbound["direction"], "in")
        self.assertEqual(inbound["rel_props"]["source_doc_ids"], [22])

    async def test_merge_preserves_ingestion_quotes_across_both_evidence_fields(self):
        first = {"quote": "John Smith works at Example.", "start": 0, "end": 28}
        second = {"quote": "John Smyth is employed by Example.", "start": 40, "end": 73}
        await self.store.create_relationship(self.keep, "Person", self.org, "Organization", "WORKS_AT",
            {"source_doc": 11, "evidence_json": json.dumps([first])})
        await self.store.create_relationship(self.remove, "Person", self.org, "Organization", "WORKS_AT",
            {"source_doc": 11, "evidence_json": json.dumps([second]), "evidence_spans": [first]})
        await self.store.merge_entities(self.keep, self.remove)
        detail = await self.store.get_node(self.keep)
        edge = next(r for r in detail["relationships"] if r["rel_type"] == "WORKS_AT")
        records = [json.loads(value) for value in edge["rel_props"]["support_records"]]
        record = next(r for r in records if r["source_doc"] == 11)
        self.assertEqual(record.get("evidence_spans"), [first, second])
        self.assertEqual(edge["rel_props"]["source_doc_ids"], [11, 22])
        self.assertEqual(edge["rel_props"]["weight"], 2)

    async def test_invalid_support_rolls_back_all_graph_changes(self):
        async with self.driver.session() as session:
            await session.run("""MATCH (b {uuid:$remove}), (o {uuid:$org})
                                 CREATE (b)-[:INVALID_SUPPORT {support_records:['invalid JSON']}]->(o)""",
                              remove=self.remove, org=self.org)
        before_keep = await self.store.get_node(self.keep)
        before_remove = await self.store.get_node(self.remove)
        with self.assertRaises(ValueError):
            await self.store.merge_entities(self.keep, self.remove)
        self.assertEqual(await self.store.get_node(self.keep), before_keep)
        self.assertEqual(await self.store.get_node(self.remove), before_remove)


if __name__ == "__main__":
    unittest.main()
