"""Entity evidence/review contracts across real, disposable Neo4j and PostgreSQL.

No live app startup, model calls, production documents, or production credentials.
"""
import asyncio
import json
import os
import unittest
import uuid
from unittest.mock import AsyncMock, patch

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
        self.graph.review_store = self.store
        self.graph.new_uuid = lambda: self.prefix + str(uuid.uuid4())
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

    async def test_public_ingestion_preserves_review_alias_and_vector_contract_across_sources(self):
        from app import pipeline
        from app.extractor import EntityExtractor
        from app.paperless import PaperlessClient
        from app.entity_vector_consistency import classify_entity_vector
        from tests.test_review_identity_admission import alias_client, SOURCE, NAMES
        from tests.test_ingestion import PaperlessFixture, ClassifierFixture, document
        canonical = await self.seed("canonical", NAMES[0], "Organization", [991712])
        await self.graph.create_document_node(991712, "Synthetic retained source", "general", "2026-09-07", "synthetic")
        await self.graph.create_relationship("991712", "Document", canonical, "Organization", "MENTIONS", {"source_doc": 991712})
        original = await self.graph.get_node(canonical)
        doc = document(991711, SOURCE)
        def uppercase(rows):
            return [{**row, "name": row["name"].upper()} for row in rows]
        extractor = EntityExtractor(alias_client(uppercase))
        with patch.object(pipeline, "paperless_client", PaperlessFixture([doc])), \
                patch.object(pipeline, "classifier", ClassifierFixture()), \
                patch.object(pipeline, "extractor", extractor), \
                patch.object(pipeline, "graph_store", self.graph), \
                patch.object(pipeline, "embeddings_store", self.store), \
                patch.object(pipeline, "entity_resolver", self.resolver), \
                patch.object(pipeline, "_generate_document_summary", AsyncMock(return_value="")), \
                patch.object(self.store, "generate_embedding", AsyncMock(return_value=[.01] + [0.] * 3071)):
            for _ in range(2):
                result = await pipeline.process_document(doc, force=True)
                self.assertEqual(result["status"], "processed", result)
                persisted = await self.graph.get_node(canonical)
                props = persisted["properties"]
                self.assertEqual(len(trusted_aliases(props, "Organization", 991711, SOURCE)), 1)
                self.assertEqual(set(props["source_doc_ids"]), {991711, 991712})
                old_edges = [e for e in original["relationships"] if e.get("neighbor_props", {}).get("paperless_id") == 991712]
                retained = [e for e in persisted["relationships"] if e.get("neighbor_props", {}).get("paperless_id") == 991712]
                self.assertEqual(retained, old_edges)
                async with self.store.pool.acquire() as conn:
                    vector = dict(await conn.fetchrow("SELECT entity_uuid,entity_name,entity_type,vector_dims(embedding) AS dimension FROM entity_embeddings WHERE entity_uuid=$1", canonical))
                consistency = classify_entity_vector(vector, {"uuid": canonical, "name": props["name"], "labels": ["Organization"]},
                    verified_aliases=trusted_aliases(props, "Organization", 991711, SOURCE))
                self.assertTrue(consistency["accepted"], consistency)
                self.assertEqual((await self.store.get_ingestion_fingerprints([991711]))[991711], PaperlessClient.ingestion_fingerprint(doc))
            self.assertEqual((await pipeline.process_document(doc))["status"], "skipped")

    async def test_post_sync_steward_preserves_legacy_suggestion_history(self):
        from app import entity_steward as steward_module
        left = await self.seed("steward-left", "Cobalt Tools", "Organization", [991711])
        right = await self.seed("steward-right", "Cobalt Tools", "Organization", [991712])
        original = await self.store.add_entity_review_decision(left, right, "suggest_merge", "original legacy note")
        native_candidates = self.graph.get_entity_review_candidates

        async def scoped_candidates(ignored, limit):
            candidates = await native_candidates(ignored, limit=200)
            return [c for c in candidates if c["left"]["uuid"].startswith(self.prefix)
                    and c["right"]["uuid"].startswith(self.prefix)][:limit]

        agent = AsyncMock(return_value={"recommendation": "merge", "confidence": .99, "risk": "low"})
        with patch.object(steward_module, "graph_store", self.graph), \
                patch.object(steward_module, "embeddings_store", self.store), \
                patch.object(self.graph, "get_entity_review_candidates", scoped_candidates), \
                patch.object(steward_module.strands_orchestrator, "review_entity_candidate", agent):
            for _ in range(2):
                report = await steward_module.EntitySteward().run_once(reason="post-sync")
                self.assertEqual(report["reviewed_count"], 0)
        self.assertEqual(await self.store.get_entity_review_decisions(), [original])
        agent.assert_not_awaited()
        self.assertIsNotNone(await self.graph.get_node(left))
        self.assertIsNotNone(await self.graph.get_node(right))

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
        # All seventeen surviving legacy anchors remain after real orphan
        # cleanup, not just after snapshot hydration. Both-missing rows stay.
        await self.graph.create_document_node(991711, "Synthetic legacy support", "test", "2026-09-06", "synthetic")
        surviving = []
        for index, pair in enumerate(pairs[:17]):
            known = pair[0] if index % 2 else pair[1]
            surviving.append(known)
            await self.graph.create_relationship("991711", "Document", known, "Person", "MENTIONS", {"source_doc": 991711})
        await self.graph.delete_document_graph(991711)
        await self.graph.delete_document_graph(991711)
        for known in surviving:
            props = (await self.graph.get_node(known))["properties"]
            self.assertTrue(props["review_anchor"])
        after_cleanup = sorted(await self.store.get_entity_review_decisions(), key=lambda row: row["id"])
        self.assertEqual(after_cleanup, rows)

    async def test_quarantine_cleanup_new_uuid_blocks_durable_positive_replay(self):
        from app.entity_decisions import entity_identity
        from app.entity_policy import RESOLUTION_POLICY
        canonical, alias = "Quartz Laboratories", "Quartz Labs"
        old = await self.seed("old", canonical, "Organization", [991711])
        old_identity = entity_identity(await self.graph.get_node(old))
        alias_identity = {"type": "Organization", "canonical_name": alias, "names": [alias.casefold()]}
        positive = await self.store.add_entity_review_decision(old, self.prefix + "historic-duplicate", "merged", "historical approval",
            left_identity=old_identity, right_identity=alias_identity, provenance="human_review",
            identity_status="active", review_id="historic-review", review_method="entity_review_api")
        # Simulate the existing repair surface's node-local legacy tombstone:
        # cleanup itself must promote it before deleting this graph UUID.
        tombstone = {"alias": alias, "type": "Organization", "status": "quarantined", "policy": "old-policy"}
        async with self.graph.driver.session() as session:
            await session.run("MATCH(n {uuid:$uuid}) SET n.alias_records=$records", uuid=old,
                              records=[json.dumps(tombstone)])
        await self.graph.create_document_node(991711, "Synthetic source", "test", "2026-09-06", "synthetic")
        await self.graph.create_relationship("991711", "Document", old, "Organization", "MENTIONS", {"source_doc":991711})
        with patch.object(self.store, "preserve_alias_revocations", AsyncMock(side_effect=ConnectionError("synthetic ledger unavailable"))):
            with self.assertRaises(ConnectionError):
                await self.graph.delete_document_graph(991711)
        retained = await self.graph.get_node(old)
        self.assertTrue(any(edge.get("neighbor_props", {}).get("paperless_id") == 991711 for edge in retained["relationships"]))
        await self.graph.delete_document_graph(991711)
        self.assertIsNone(await self.graph.get_node(old))
        rows = await self.store.get_entity_review_decisions()
        self.assertEqual(next(row for row in rows if row["id"] == positive["id"]), positive)
        revocation = next(row for row in rows if row["decision"] == "alias_revoked")
        for generation in range(3):
            with self.subTest(generation=generation):
                current = await self.seed(f"recreated-{generation}", canonical, "Organization", [991711],
                                          resolution_policy=f"{RESOLUTION_POLICY}-future-{generation}")
                await self.graph.add_entity_alias_record(current, human_alias_record(canonical, "Crystal Research", "Organization",
                    f"independent-{generation}", review_method="entity_review_api"))
                source = f"{alias} listed in synthetic revision {generation}."
                bound = await self.resolver.resolve(alias, "Organization", 991711, source=source)
                self.assertNotEqual(bound, current)
                self.assertEqual(await self.resolver.resolve("Crystal Research", "Organization", 991711), current)
                # A later ordinary positive review cannot silently supersede the
                # quarantine. There is deliberately no last-write-wins rule.
                await self.store.add_entity_review_decision(current, self.prefix + f"alias-{generation}", "merged", "ordinary later review",
                    left_identity=entity_identity(await self.graph.get_node(current)), right_identity=alias_identity,
                    provenance="human_review", identity_status="active", review_id=f"later-{generation}", review_method="entity_review_api")
                self.assertNotEqual(await self.resolver.resolve(alias, "Organization", 991712, source=source + " Changed."), current)
                # Repeated repairs are idempotent even with changed source and
                # node policy; the original revocation's history does not move.
                await self.graph.add_entity_alias_record(current, tombstone)
                await self.graph.add_entity_alias_record(current, tombstone)
                await self.graph.create_document_node(991711, "Synthetic revised source", "test", "2026-09-06", "synthetic")
                await self.graph.create_relationship("991711", "Document", current, "Organization", "MENTIONS", {"source_doc":991711})
                await self.graph.delete_document_graph(991711)
                await self.graph.delete_document_graph(991711)
                self.assertIsNone(await self.graph.get_node(current))
                rows = await self.store.get_entity_review_decisions()
                negatives = [row for row in rows if row["decision"] == "alias_revoked"]
                self.assertEqual(negatives, [revocation])
                self.assertEqual(next(row for row in rows if row["id"] == positive["id"]), positive)

    async def test_partial_legacy_veto_reuses_persisted_isolated_source_binding(self):
        from app.entity_decisions import entity_identity
        protected = await self.seed("protected", "Restricted Labs", "Organization", [991712])
        await self.store.add_entity_review_decision(protected, self.prefix + "missing", "split", "original",
            left_identity=entity_identity(await self.graph.get_node(protected)), identity_status="unresolved_legacy")
        results = [await self.resolver.resolve(name, "Organization", 991711, source="Restricted Labs signed.")
                   for name in ("Restricted Labs", "RESTRICTED LABS", "restricted labs")]
        self.assertEqual(len(set(results)), 1)
        self.assertNotEqual(results[0], protected)
        persisted = (await self.graph.get_node(results[0]))["properties"]
        self.assertEqual(persisted["resolution_status"], "isolated")
        self.assertTrue(persisted["isolated_source_key"])
        restarted = module.EntityResolver()
        self.assertEqual(await restarted.resolve("Restricted Labs", "Organization", 991711, source="Restricted Labs signed."), results[0])
        self.assertNotEqual(await restarted.resolve("Restricted Labs", "Organization", 991712, source="Restricted Labs signed."), results[0])
        with self.assertRaises(module.EntityMergeProhibited):
            await restarted.merge_entities(protected, results[0], review_method="entity_review_api")

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
