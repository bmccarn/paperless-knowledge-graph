"""C01–C04 contract probes: stdlib-only, synthetic stores, no client imports.

The resolver class is compiled from the working source, omitting only client
imports. Real public datastore lifecycles live in test_entity_datastore_evidence.
"""
import ast
import copy
from pathlib import Path
import types
import unittest

from app import entity_policy as policy, entity_decisions as decisions
from app.entity_bindings import DocumentBindings
from app.extraction_evidence import (validate_entities, coreference_candidates,
    adjudicate_coreferences, adjudicate_name_usage, reconcile_entities)


def reviewed_entities(left, right, source, kind="Organization", *, status="affirmed", scope="current_direct", offset=0):
    entities = validate_entities([{"name": name, "type": kind, "confidence": .95,
        "evidence_quote": source} for name in (left, right)], source, offset, [])
    proposals = coreference_candidates(entities, source, offset)
    reviews = [review_for(proposal, status=status, scope=scope) for proposal in proposals]
    adjudicate_coreferences(proposals, reviews, entities, source, offset, [])
    return entities


def review_for(proposal, *, status="affirmed", scope="current_direct"):
    return {"proof_id": proposal["proof_id"], "left_id": proposal["left_id"], "right_id": proposal["right_id"],
        "evidence_quote": proposal["evidence"]["quote"], "status": status, "assertion_scope": scope,
        "explicit": True, "rationale": "Synthetic independent verdict considering the whole source window."}


def proofs(entities):
    return [proof for entity in entities for proof in entity.get("identity_proofs", [])]


def node(uuid, name, kind="Organization", docs=(), **props):
    return {"uuid": uuid, "name": name, "entity_type": kind, "source_doc_ids": list(docs),
            "aliases": [], "alias_records": [], **props}


class Graph:
    def __init__(self):
        self.nodes = {}
        self.sequence = 0

    async def get_entities_by_type(self, kind):
        return copy.deepcopy([node for node in self.nodes.values() if node["entity_type"] == kind])

    async def get_node(self, uuid):
        return {"properties": copy.deepcopy(self.nodes[uuid]), "labels": [self.nodes[uuid]["entity_type"]]} if uuid in self.nodes else None

    async def create_node(self, kind, props):
        self.sequence += 1
        uuid = f"created-{self.sequence}"
        self.nodes[uuid] = {**copy.deepcopy(props), "uuid": uuid}
        return uuid

    async def record_entity_source(self, uuid, doc_id, **kwargs):
        self.nodes[uuid]["source_doc_ids"] = sorted(set(self.nodes[uuid]["source_doc_ids"]) | {doc_id})

    async def add_entity_alias_record(self, uuid, record):
        if record not in self.nodes[uuid]["alias_records"]:
            self.nodes[uuid]["alias_records"].append(copy.deepcopy(record))

    async def protect_review_anchor(self, uuid):
        if uuid in self.nodes:
            self.nodes[uuid]["review_anchor"] = True


class Ledger:
    def __init__(self):
        self.rows = []

    async def get_entity_review_decisions(self):
        return copy.deepcopy(self.rows)

    async def preserve_alias_revocations(self, rows):
        for row in rows:
            if not any(old.get("review_id") == row["review_id"] and old["decision"] == "alias_revoked" for old in self.rows):
                self.rows.append(copy.deepcopy(row))

    async def hydrate_entity_review_identities(self, incoming, status):
        for row in self.rows:
            if row["left_uuid"] == incoming["left_uuid"] and row["right_uuid"] == incoming["right_uuid"]:
                row.update(copy.deepcopy(incoming), identity_status=status)


def resolver_with(graph, ledger):
    path = Path(__file__).resolve().parents[1] / "app/entity_resolver.py"
    tree = ast.parse(path.read_text(), str(path))
    tree.body = [item for item in tree.body if not (isinstance(item, ast.ImportFrom) and (item.module or "").startswith("app."))]
    module = types.ModuleType("isolated_resolver_contract")
    module.__dict__.update({name: getattr(policy, name) for name in dir(policy) if not name.startswith("_")})
    module.__dict__.update({name: getattr(decisions, name) for name in dir(decisions) if not name.startswith("_")})
    module.__dict__.update(graph_store=graph, embeddings_store=ledger)
    exec(compile(tree, str(path), "exec"), module.__dict__)
    return module.EntityResolver()


class ClosureTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.graph, self.ledger = Graph(), Ledger()
        self.resolver = resolver_with(self.graph, self.ledger)

    async def test_literal_regex_alone_cannot_authorize_even_affirmative_text(self):
        for left, right in [("Example Labs", "Test Services"), ("Cedar Analytics", "Willow Research")]:
            for source in [f'{left} also known as {right} operates here.',
                           f'The assertion "{left} also known as {right}" is untrue.']:
                self.graph.nodes = {"canonical": node("canonical", left)}
                result = await self.resolver.resolve(right, "Organization", 101, source=source)
                self.assertNotEqual(result, "canonical")
                self.assertEqual(self.graph.nodes["canonical"]["alias_records"], [])

    def test_literal_regex_span_cannot_mint_a_trusted_record(self):
        phrase = "Cedar Analytics also known as Willow Research operates here."
        with self.assertRaisesRegex(ValueError, "source-review identity proof"):
            policy.source_alias_record("Cedar Analytics", "Willow Research", "Organization", 101, phrase,
                                       policy.coreference_span("Cedar Analytics", "Willow Research", phrase))

    async def test_reviewed_affirmative_alias_initialism_and_nonzero_scope_bind(self):
        for left, right, phrase in [
            ("Cedar Analytics", "Willow Research", "Cedar Analytics also known as Willow Research operates here."),
            ("Harbor Network Systems", "HNS", "Harbor Network Systems (HNS) signed."),
            ("Quarry Tools LLC", "Rock Works", "Quarry Tools LLC doing business as Rock Works filed."),
            ("Alice Jane Mercer", "A J Mercer", "Alice Jane Mercer also known as A J Mercer signed.")]:
            kind = "Person" if left.startswith("Alice") else "Organization"
            source = "Preamble. " + phrase
            entities = reviewed_entities(left, right, phrase, kind, offset=10)
            self.graph.nodes = {"canonical": node("canonical", left, kind)}
            bindings = DocumentBindings(101, {"all_entities": entities}, source, self.resolver)
            await bindings.resolve_all()
            self.assertEqual(set(bindings.resolved.values()), {"canonical"})
            records = self.graph.nodes["canonical"]["alias_records"]
            self.assertEqual(len(records), 1)
            self.assertTrue(records[0]["identity_proof"]["proof_id"].startswith("coref-"))
            self.assertEqual(policy.trusted_aliases(self.graph.nodes["canonical"], kind, 101, source), [right])
            self.assertEqual(policy.trusted_aliases(self.graph.nodes["canonical"], kind, 102, source), [])
            self.assertEqual(policy.trusted_aliases(self.graph.nodes["canonical"], kind, 101, source + " Revision."), [])

    async def test_denied_quoted_hypothetical_historical_disputed_and_unknown_abstain(self):
        left, right = "Juniper Instruments", "Silver Bay Services"
        phrase = f"{left} also known as {right}"
        for source, status, scope in [
            (f'The assertion "{phrase}" is untrue.', "denied", "quoted"),
            (f'"{phrase}" appears in the exhibit.', "unknown", "quoted"),
            (f"Suppose {phrase} were accurate.", "unknown", "hypothetical"),
            (f"{phrase} operated in 1920; their identities are now distinct.", "unknown", "historical"),
            (f"{phrase}. The previous sentence is disputed by the signatories.", "denied", "disputed"),
            (f"{phrase}. That equivalence is unfounded.", "denied", "current_direct"),
            (f"{phrase} operates here.", "unknown", "unknown")]:
            with self.subTest(source=source):
                entities = reviewed_entities(left, right, source, status=status, scope=scope)
                self.graph.nodes = {"canonical": node("canonical", left)}
                bindings = DocumentBindings(101, {"all_entities": entities}, source, self.resolver)
                await bindings.resolve_all()
                self.assertEqual(len(set(bindings.resolved.values())), 2)
                self.assertEqual(self.graph.nodes["canonical"]["alias_records"], [])

    async def test_malformed_replayed_or_unaccepted_proofs_cannot_bind(self):
        left, right = "Amber Mechanics", "Copper Works"
        source = f"{left} also known as {right} operates here."
        entities = reviewed_entities(left, right, source)
        proposal = coreference_candidates(entities, source)[0]
        for field, value in [("proof_id", "forged"), ("left_id", "unknown"),
                             ("evidence_quote", left), ("explicit", "true"), ("rationale", "")]:
            raw = review_for(proposal)
            raw[field] = value
            candidates = validate_entities([{"name": name, "type": "Organization", "confidence": .9,
                "evidence_quote": source} for name in (left, right)], source, 0, [])
            adjudicate_coreferences([proposal], [raw], candidates, source, 0, [])
            self.assertEqual(proofs(candidates), [])
        for change in ("missing_endpoint", "changed_type", "changed_source", "changed_offset", "changed_proof_id"):
            items = copy.deepcopy(entities)
            text = source
            if change == "missing_endpoint":
                items = items[1:]
            elif change == "changed_type":
                items[0]["type"] = "Product"
            elif change == "changed_source":
                text = source.replace("operates", "operated")
                for item in items:
                    item["evidence"] = [{"start": 0, "end": len(text), "quote": text}]
            else:
                for item in items:
                    for proof in item["identity_proofs"]:
                        if change == "changed_offset":
                            proof["evidence"]["start"] += 1
                        else:
                            proof["proof_id"] = "old"
            self.graph.nodes = {"canonical": node("canonical", left)}
            binding = DocumentBindings(101, {"all_entities": items}, text, self.resolver)
            await binding.resolve_all()
            self.assertNotEqual(binding.lookup(right, "Organization"), "canonical")
        candidates = copy.deepcopy(entities)
        for item in candidates:
            item.pop("identity_proofs", None)
        adjudicate_coreferences([proposal], [review_for(proposal), review_for(proposal, status="denied")], candidates, source, 0, [])
        self.assertEqual(proofs(candidates), [])

    async def test_case_metamorphism_brands_and_evidence_bound_abbreviations(self):
        for brand in ("Acme", "Orion", "Zentara", "Lumos"):
            for canonical in (brand, brand.lower(), brand.upper()):
                for incoming in (brand, brand.lower(), brand.upper()):
                    self.graph.nodes = {"canonical": node("canonical", canonical, docs=[1])}
                    self.assertEqual(await self.resolver.resolve(incoming, "Organization", 2,
                        source=f"Invoice supplier: {incoming}."), "canonical")
        for abbreviation in ("HNS", "QRS", "MERS"):
            for incoming in (abbreviation, abbreviation.lower(), abbreviation.title()):
                self.graph.nodes = {"canonical": node("canonical", abbreviation, docs=[1], name_usage="abbreviation")}
                new = await self.resolver.resolve(incoming, "Organization", 2, name_usage="abbreviation")
                self.assertNotEqual(new, "canonical")
                self.assertEqual(await self.resolver.resolve(incoming.swapcase(), "Organization", 2, name_usage="abbreviation"), new)

    async def test_partial_veto_stable_isolated_identity_for_exact_source_generation(self):
        self.graph.nodes = {"protected": node("protected", "Restricted Labs", docs=[1])}
        self.ledger.rows = [{"left_uuid": "protected", "right_uuid": "missing", "decision": "split",
            "left_identity": decisions.entity_identity(self.graph.nodes["protected"]), "identity_status": "unresolved_legacy"}]
        ids = [await self.resolver.resolve(name, "Organization", 101, source="Restricted Labs signed.")
               for name in ("Restricted Labs", "restricted labs", "RESTRICTED LABS")]
        self.assertEqual(len(set(ids)), 1)
        self.assertNotEqual(ids[0], "protected")
        self.assertEqual(self.graph.nodes[ids[0]]["resolution_status"], "isolated")
        self.assertTrue(self.graph.nodes[ids[0]]["isolated_source_key"])
        self.assertNotEqual(await self.resolver.resolve("Restricted Labs", "Organization", 102, source="Restricted Labs signed."), ids[0])
        self.assertNotEqual(await self.resolver.resolve("Restricted Labs", "Organization", 101, source="Restricted Labs revised."), ids[0])
        self.assertEqual(self.graph.nodes["protected"]["source_doc_ids"], [1])
        self.assertTrue(decisions.merge_is_prohibited(decisions.entity_identity(self.graph.nodes["protected"]),
            decisions.entity_identity(self.graph.nodes[ids[0]]), self.ledger.rows))

    async def test_revocation_survives_new_uuid_policy_source_and_review_replay(self):
        canonical, alias = "Quartz Laboratories", "Quartz Labs"
        left, right = node("old", canonical), node("duplicate", alias)
        positive = {"left_uuid": "old", "right_uuid": "duplicate", "decision": "merged",
            "left_identity": decisions.entity_identity(left), "right_identity": decisions.entity_identity(right),
            "provenance": "human_review", "identity_status": "active", "review_id": "old-approval",
            "review_method": "entity_review_api", "created_at": "original"}
        self.ledger.rows = [copy.deepcopy(positive)]
        self.graph.nodes = {"old": {**left, "alias_records": [{"alias": alias, "type": "Organization", "status": "quarantined"}]}}
        self.assertNotEqual(await self.resolver.resolve(alias, "Organization", 101), "old")
        self.assertEqual(self.ledger.rows[0], positive)
        for index in range(3):
            current = f"new-{index}"
            self.graph.nodes = {current: node(current, canonical, resolution_policy=f"future-{index}", alias_records=[
                policy.human_alias_record(canonical, "Crystal Research", "Organization", "other-review", review_method="entity_review_api")])}
            self.assertNotEqual(await self.resolver.resolve(alias, "Organization", 102 + index, source=f"{alias} revision {index}"), current)
            self.assertEqual(await self.resolver.resolve("Crystal Research", "Organization", 200 + index), current)
        self.assertEqual(len([row for row in self.ledger.rows if row["decision"] == "alias_revoked"]), 1)
        self.assertEqual(self.ledger.rows[0], positive)
        # Independently reviewed alias of a DIFFERENT canonical is unaffected.
        self.graph.nodes = {"other": node("other", "Quartz Industries", alias_records=[
            policy.human_alias_record("Quartz Industries", alias, "Organization", "separate-review", review_method="entity_review_api")])}
        self.assertEqual(await self.resolver.resolve(alias, "Organization", 400), "other")

    def test_name_usage_review_requires_context_and_is_case_independent(self):
        for name in ("QRS", "qrs", "Qrs"):
            source = f"{name} is an abbreviation used for several different suppliers."
            raw = {"name": name, "type": "Organization", "confidence": .95, "evidence_quote": source,
                   "name_usage": "ambiguous", "name_usage_evidence_quote": source, "name_usage_rationale": "Multiple meanings in source"}
            entities = validate_entities([raw], source, 0, [])
            adjudicate_name_usage(entities, [raw], source, 0)
            self.assertEqual(entities[0]["name_usage"], "ambiguous")
            raw["name_usage_evidence_quote"] = name
            entities = validate_entities([raw], source, 0, [])
            adjudicate_name_usage(entities, [raw], source, 0)
            self.assertNotIn("name_usage", entities[0])

    def test_revocation_follows_positive_reviewed_alias_lineage_but_not_raw_aliases(self):
        canonical = node("old", "Quartz Laboratories", alias_records=[
            {"alias": "Quartz Labs", "type": "Organization", "status": "quarantined"}])
        rows = decisions.alias_revocations(canonical)
        rows.append({"decision": "merged", "identity_status": "active", "provenance": "human_review",
            "review_id": "old-positive", "review_method": "entity_review_api",
            "left_identity": {"type": "Organization", "canonical_name": "Quartz Laboratories",
                              "reviewed_names": ["Crystal Research"], "names": ["Unreviewed Poison"]},
            "right_identity": {"type": "Organization", "canonical_name": "Quartz Labs"}})
        self.assertTrue(decisions.alias_replay_prohibited(node("new", "Crystal Research"), "Quartz Labs", "Organization", rows))
        self.assertFalse(decisions.alias_replay_prohibited(node("other", "Unreviewed Poison"), "Quartz Labs", "Organization", rows))
        self.assertFalse(decisions.alias_replay_prohibited(node("new", "Quartz Laboratories"), "Crystal Research", "Organization", rows))

    def test_qualified_revocation_keys_do_not_discard_a_second_homonym_veto(self):
        rows = [decisions.alias_revocations(node(f"old-{hint}", "Quartz Laboratories", identity_hints=[hint],
            alias_records=[{"alias": "Quartz Labs", "type": "Organization", "status": "revoked"}]))[0]
            for hint in ("REG-101", "REG-202")]
        self.assertNotEqual(rows[0]["review_id"], rows[1]["review_id"])
        for hint in ("REG-101", "REG-202"):
            self.assertTrue(decisions.alias_replay_prohibited(node("new", "Quartz Laboratories", identity_hints=[hint]),
                            "Quartz Labs", "Organization", rows))
        self.assertFalse(decisions.alias_replay_prohibited(node("other", "Quartz Laboratories", identity_hints=["REG-303"]),
                         "Quartz Labs", "Organization", rows))

    def test_proof_receipts_survive_reconciliation_without_expanding_authority(self):
        source = "Cedar Analytics also known as Willow Research signed."
        entities = reviewed_entities("Cedar Analytics", "Willow Research", source)
        merged = reconcile_entities(entities + copy.deepcopy(entities), [])
        self.assertEqual(len(merged), 2)
        self.assertTrue(all(len(entity["identity_proofs"]) == 1 for entity in merged))

    async def test_disputed_second_window_cannot_be_erased_by_positive_reconciliation(self):
        left, right = "Cedar Analytics", "Willow Research"
        first = f"{left} also known as {right} signed."
        second = f"{left} also known as {right}. That assertion is disputed."
        source = first + "\n" + second
        entities = reconcile_entities(reviewed_entities(left, right, first) +
            reviewed_entities(left, right, second, offset=len(first) + 1, status="denied", scope="disputed"), [])
        self.graph.nodes = {"canonical": node("canonical", left, alias_records=[
            policy.human_alias_record(left, right, "Organization", "old-review", review_method="entity_review_api")])}
        bindings = DocumentBindings(101, {"all_entities": entities}, source, self.resolver)
        await bindings.resolve_all()
        self.assertEqual(len(set(bindings.resolved.values())), 2)
        self.assertFalse(any(record.get("provenance") == "source_coreference"
                             for record in self.graph.nodes["canonical"]["alias_records"]))
