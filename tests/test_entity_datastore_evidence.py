"""Entity evidence/review contracts across real, disposable Neo4j and PostgreSQL.

No live app startup, model calls, production documents, or production credentials.
"""
import asyncio
import json
import os
import unittest
import uuid
from unittest.mock import patch

import asyncpg
from neo4j import AsyncGraphDatabase

from tests.runtime import configure_test_environment
configure_test_environment()
from tests.test_storage_integrity import local_url
from app import entity_resolver as module
from app.embeddings import EmbeddingsStore, INIT_SQL
from app.entity_policy import human_alias_record, trusted_aliases
from app.graph import GraphStore


@unittest.skipUnless(os.environ.get("STORAGE_TEST_DSN") and os.environ.get("NEO4J_TEST_BOLT"),
                     "Set both disposable localhost PostgreSQL and Neo4j test URLs")
class EntityDatastoreEvidenceTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.prefix = f"entity-evidence-{uuid.uuid4()}-"
        self.store = EmbeddingsStore()
        dsn = local_url("STORAGE_TEST_DSN")
        connection = await asyncpg.connect(dsn)
        try:
            await connection.execute("CREATE EXTENSION IF NOT EXISTS vector WITH SCHEMA public; CREATE EXTENSION IF NOT EXISTS pg_trgm WITH SCHEMA public")
            await connection.execute("CREATE SCHEMA IF NOT EXISTS entity_evidence_test")
        finally:
            await connection.close()
        self.store.pool = await asyncpg.create_pool(dsn, min_size=1, max_size=2,
            server_settings={"search_path": "entity_evidence_test,public"})
        async with self.store.pool.acquire() as conn:
            await conn.execute(INIT_SQL)
            await conn.execute("TRUNCATE entity_review_decisions")
        self.graph = GraphStore()
        self.graph.driver = AsyncGraphDatabase.driver(local_url("NEO4J_TEST_BOLT"), auth=None)
        self.resolver = module.EntityResolver()
        self.patches = [patch.object(module, "graph_store", self.graph),
                        patch.object(module, "embeddings_store", self.store)]
        for item in self.patches:
            item.start()
        self.addCleanup(lambda: [item.stop() for item in reversed(self.patches)])

    async def asyncTearDown(self):
        async with self.graph.driver.session() as session:
            await session.run("MATCH (n) WHERE n.uuid STARTS WITH $prefix OR n.paperless_id IN [991711,991712] DETACH DELETE n", prefix=self.prefix)
        await self.graph.driver.close()
        await self.store.close()

    async def seed(self, suffix, name, kind="Person", docs=None, **properties):
        node_uuid = self.prefix + suffix
        async with self.graph.driver.session() as session:
            await session.run(f"CREATE (n:{kind}) SET n=$props",
                props={"uuid": node_uuid, "name": name, "entity_type": kind, "source_doc_ids": docs or [], **properties})
        return node_uuid

    async def test_mixed_nineteen_vetoes_hydrate_quarantine_and_preserve_exact_history(self):
        pairs = []
        for index in range(19):
            left, right = self.prefix + f"z-left-{index}", self.prefix + f"a-right-{index}"
            pairs.append((left, right))
            if index < 17:
                suffix = f"z-left-{index}" if index % 2 else f"a-right-{index}"
                await self.seed(suffix, f"Synthetic identity {index}", docs=[100+index])
        async with self.store.pool.acquire() as conn:
            await conn.executemany("INSERT INTO entity_review_decisions(left_uuid,right_uuid,decision,note) VALUES($1,$2,'split','original decision')", pairs)
            original = [dict(row) for row in await conn.fetch("SELECT * FROM entity_review_decisions ORDER BY id")]
        report = await self.resolver.hydrate_review_identities()
        self.assertEqual(report["hydrated"], 17)
        self.assertEqual(len(report["unresolved"]), 19)
        second = await self.resolver.hydrate_review_identities()
        self.assertEqual(second["hydrated"], 0)
        self.assertEqual(len(second["unresolved"]), 19)
        rows = sorted(await self.store.get_entity_review_decisions(), key=lambda row: row["id"])
        self.assertEqual(len(rows), 19)
        for index, (before, after) in enumerate(zip(original, rows)):
            for field in ("id", "left_uuid", "right_uuid", "decision", "note", "created_at", "provenance", "review_method", "review_id"):
                self.assertEqual(before[field], after[field])
            self.assertEqual(after["identity_status"], "unresolved_legacy")
            self.assertEqual(sum(bool(after.get(f"{side}_identity")) for side in ("left", "right")), 1 if index < 17 else 0)
        live_side = pairs[0][1]
        other = await self.seed("new-other", "Synthetic identity 0", docs=[501])
        with self.assertRaises(module.EntityMergeProhibited):
            await self.resolver.merge_entities(live_side, other, review_method="entity_review_api")
        self.assertIsNotNone(await self.graph.get_node(live_side))
        self.assertIsNotNone(await self.graph.get_node(other))

    async def test_legacy_and_quarantined_aliases_cannot_become_trusted_during_real_merge(self):
        keep = await self.seed("keep", "Alice Example", aliases=["Legacy Alias"])
        remove = await self.seed("remove", "Alice Smyth", aliases=["Other Poison"])
        async with self.store.pool.acquire() as conn:
            await conn.execute("INSERT INTO entity_review_decisions(left_uuid,right_uuid,decision,note,left_identity,right_identity) VALUES($1,$2,'merged','',$3::jsonb,$4::jsonb)",
                keep, remove, json.dumps({"type": "Person", "canonical_name": "Alice Example"}),
                json.dumps({"type": "Person", "canonical_name": "Legacy Alias"}))
        self.assertEqual(trusted_aliases((await self.graph.get_node(keep))["properties"], "Person", 11, ""), [])
        with self.assertRaisesRegex(ValueError, "Explicit review origin"):
            await self.resolver.merge_entities(keep, remove)
        await self.resolver.merge_entities(keep, remove, review_method="entity_review_api")
        props = (await self.graph.get_node(keep))["properties"]
        self.assertEqual(set(props["aliases"]), {"Legacy Alias", "Other Poison", "Alice Smyth"})
        self.assertEqual(trusted_aliases(props, "Person", 11, ""), ["Alice Smyth"])
        reviewed = (await self.store.get_entity_review_decisions())[0]
        self.assertEqual(reviewed["review_method"], "entity_review_api")
        self.assertEqual(json.loads(props["alias_records"][-1])["review_id"], reviewed["review_id"])
        await self.graph.add_entity_alias_record(keep, {"alias": "Alice Smyth", "type": "Person", "status": "quarantined"})
        self.assertEqual(trusted_aliases((await self.graph.get_node(keep))["properties"], "Person", 11, ""), [])
        # The repair tombstone must also block decision-pair replay, not just
        # the alias reader. Preseed a legitimate same-name node to avoid orphan fixtures.
        alternate = await self.seed("alternate", "Alice Smyth", docs=[22])
        self.assertEqual(await self.resolver.resolve_person("Alice Smyth", 22), alternate)

    async def test_concurrent_idempotent_alias_and_source_updates_preserve_all_records(self):
        keep = await self.seed("keep", "Alice Example")
        records = [human_alias_record("Alice Example", f"Reviewed Alias {index}", "Person", f"review-{index}", review_method="entity_review_api") for index in range(4)]
        await asyncio.gather(*(self.graph.add_entity_alias_record(keep, record) for record in records * 2))
        await asyncio.gather(*(self.graph.record_entity_source(keep, index) for index in [11, 22, 33, 44] * 2))
        props = (await self.graph.get_node(keep))["properties"]
        self.assertEqual(len(props["alias_records"]), 4)
        self.assertEqual(set(props["aliases"]), {record["alias"] for record in records})
        self.assertEqual(sorted(props["source_doc_ids"]), [11, 22, 33, 44])
        resolved = await asyncio.gather(*(self.resolver.resolve_person("Alice Example", 55) for _ in range(8)))
        self.assertEqual(set(resolved), {keep})
        self.assertEqual(sorted((await self.graph.get_node(keep))["properties"]["source_doc_ids"]), [11,22,33,44,55])

    async def test_source_replacement_preserves_other_document_support_and_restores_valid_same_doc_link(self):
        good = await self.seed("org", "Network Entity Systems", "Organization", [991711,991712], aliases=["Old Alias"])
        bad = await self.seed("condition", "NES", "Condition", [991711,991712])
        for doc_id in (991711, 991712):
            await self.graph.create_document_node(doc_id, "Synthetic source", "test", "2026-09-06", "synthetic")
            for target, kind in ((good,"Organization"), (bad,"Condition")):
                await self.graph.create_relationship(str(doc_id), "Document", target, kind, "MENTIONS", {"source_doc": doc_id})
        await asyncio.gather(*(self.graph.create_relationship(good, "Organization", bad, "Condition", "RELATED_TO", {"source_doc": doc_id}) for doc_id in [991711,991712]*2))
        before_good = await self.graph.get_node(good)
        before_bad = await self.graph.get_node(bad)
        # This is the existing document replacement primitive, NOT an automatic
        # repair verdict. Restore only the parent-adjudicated valid test link.
        await self.graph.delete_document_graph(991711)
        await self.graph.delete_document_graph(991711)
        for target in (good, bad):
            props = (await self.graph.get_node(target))["properties"]
            self.assertEqual(props["source_doc_ids"], [991712])
        await self.graph.create_document_node(991711, "Synthetic source corrected", "test", "2026-09-06", "synthetic")
        await self.graph.create_relationship("991711", "Document", good, "Organization", "MENTIONS", {"source_doc":991711})
        await self.graph.record_entity_source(good,991711)
        after_good, after_bad = await self.graph.get_node(good), await self.graph.get_node(bad)
        self.assertEqual(after_good["properties"]["aliases"], before_good["properties"]["aliases"])
        for before, after in ((before_good,after_good), (before_bad,after_bad)):
            old_support = [edge for edge in before["relationships"] if (edge.get("neighbor_props") or {}).get("paperless_id") == 991712]
            new_support = [edge for edge in after["relationships"] if (edge.get("neighbor_props") or {}).get("paperless_id") == 991712]
            self.assertEqual(new_support, old_support)
        self.assertEqual(after_good["properties"]["source_doc_ids"], [991712,991711])
        self.assertEqual(after_bad["properties"]["source_doc_ids"], [991712])
        edge = next(edge for edge in after_good["relationships"] if edge["rel_type"] == "RELATED_TO")
        self.assertEqual(edge["rel_props"]["source_doc_ids"], [991712])
