"""Automatic merge of exact-duplicate low-risk entities (synthetic data only)."""
import copy
import json
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from tests.runtime import configure_test_environment

configure_test_environment()

from app import entity_resolver as resolver_module
from app import entity_steward as steward_module
from app.entity_policy import AUTO_MERGE_REVIEW_METHOD, address_key, auto_merge_blocker, auto_merge_key, auto_merge_survivor
from tests.test_entity_decisions import MemoryDecisions, MemoryGraph


def org(node_uuid, name="Example, Inc.", docs=(1,), label="Organization", **extra):
    return {"uuid": node_uuid, "name": name, "entity_type": label, "aliases": [], "source_doc_ids": list(docs),
            "identity_hints": [], **extra}


def address(node_uuid, full_address="14 Larkspur Ct, Springfield, IL 62701", **extra):
    """A document-owned Address as ingestion writes it: no name, no source_doc_ids."""
    return {"uuid": node_uuid, "full_address": full_address, "entity_type": "Address", **extra}


def node(props, labels=None, relationships=()):
    return {"labels": labels or [props["entity_type"]], "properties": props, "relationships": list(relationships)}


class AddressKeyTests(unittest.TestCase):
    def test_spelling_variants_share_a_key(self):
        key = address_key("14 Larkspur Ct, Springfield, IL 62701")
        for variant in ("14 LARKSPUR CT, SPRINGFIELD, IL 62701", "14  Larkspur Ct.,\nSpringfield,  IL 62701.",
                        "14 Larkspur Court Springfield IL 62701", "14 larkspur ct; springfield: il 62701"):
            with self.subTest(variant=variant):
                self.assertEqual(address_key(variant), key)
        self.assertEqual(address_key("200 Elm Street Apt 2"), address_key("200 Elm St. Apt 2"))
        for full, short in (("Avenue", "Ave"), ("Road", "Rd"), ("Drive", "Dr"), ("Lane", "Ln"), ("Boulevard", "Blvd"),
                            ("Place", "Pl"), ("Parkway", "Pkwy"), ("Circle", "Cir")):
            with self.subTest(full=full):
                self.assertEqual(address_key(f"5 Oak {full}"), address_key(f"5 Oak {short}."))

    def test_any_identifying_difference_gives_another_key(self):
        base = "14 Larkspur Ct Apt 2, Springfield, IL 62701"
        for other in ("14 Larkspur Ct Apt 3, Springfield, IL 62701",  # unit
                      "16 Larkspur Ct Apt 2, Springfield, IL 62701",  # house number
                      "14 Larkspur Way Apt 2, Springfield, IL 62701",  # street type
                      "14 Laurel Ct Apt 2, Springfield, IL 62701",  # street name
                      "14 Larkspur Ct Apt 2, Springfield, IL 62704",  # postal code
                      "14 Larkspur Ct Apt 2, Springfield, IL",  # postal code missing
                      "14 Larkspur Ct, Springfield, IL 62701",  # unit missing
                      "14 N Larkspur Ct Apt 2, Springfield, IL 62701",  # directional
                      "1-4 Larkspur Ct Apt 2, Springfield, IL 62701"):  # punctuation inside a number
            with self.subTest(other=other):
                self.assertNotEqual(address_key(other), address_key(base))
        # Only the stated street types expand; directionals and unit words do not.
        self.assertNotEqual(address_key("5 North Oak St"), address_key("5 N Oak St"))
        self.assertNotEqual(address_key("5 Oak St Suite 4"), address_key("5 Oak St Ste 4"))

    def test_empty_input_gives_no_key(self):
        for value in ("", "   ", " ., ", None, 12):
            with self.subTest(value=value):
                self.assertEqual(address_key(value), "")

    def test_auto_merge_key_reads_full_address_then_name(self):
        self.assertEqual(auto_merge_key(address("a"), "Address"), address_key("14 Larkspur Ct, Springfield, IL 62701"))
        self.assertEqual(auto_merge_key({"name": "14 Larkspur Court Springfield IL 62701"}, "Address"),
                         auto_merge_key(address("a"), "Address"))
        self.assertEqual(auto_merge_key({"name": "Elsewhere", "full_address": "9 Elm St 62701"}, "Address"),
                         address_key("9 Elm St 62701"))
        self.assertEqual(auto_merge_key({"name": " ", "full_address": ""}, "Address"), "")
        self.assertEqual(auto_merge_key({"name": "Example, Inc."}, "Organization"), "example inc")


class AutoMergeBlockerTests(unittest.TestCase):
    def blocker(self, left=None, right=None, label="Organization", count=2, **kwargs):
        left = left or node(org("a", "Example, Inc.", [1]))
        right = right or node(org("b", "Example Inc", [2]))
        return auto_merge_blocker(left, right, label, count, **kwargs)

    def test_exact_duplicate_low_risk_pair_is_eligible(self):
        self.assertEqual(self.blocker(), "")
        for label in ("Organization", "Product", "System", "DocumentRef", "Location"):
            with self.subTest(label=label):
                self.assertEqual(self.blocker(node(org("a", "First National Bank", [1], label)),
                                              node(org("b", "FIRST NATIONAL BANK", [2], label)), label), "")

    def test_every_rule_blocks(self):
        cases = {
            "label": [dict(left=node(org("a", "Jane Example", [1], "Person")),
                           right=node(org("b", "Jane Example", [2], "Person")), label="Person")]
                     + [dict(left=node(org("a", "Shared", [1], kind)), right=node(org("b", "Shared", [2], kind)), label=kind)
                        for kind in ("InsurancePolicy", "Contract", "FinancialItem", "Condition",
                                     "DateEvent", "Event")]
                     + [dict(left=node(org("a"), ["Organization", "Product"]))],
            "name_key": [dict(left=node(org("a", "Example Inc")), right=node(org("b", "Example LLC", [2]))),
                         dict(left=node(org("a", "Acme Holdings")), right=node(org("b", "Acme Insurance", [2]))),
                         # name_key keeps a trailing period on a non-legal word.
                         dict(left=node(org("a", "First National Bank")), right=node(org("b", "First National Bank.", [2])))],
            "group_size": [dict(count=3), dict(count=1)],
            "context_bound": [dict(left=node(org("a", "AB")), right=node(org("b", "ab", [2])))],
            "identity_hints": [dict(left=node(org("a", identity_hints=["acct 1111"]))),
                               dict(left=node(org("a", identity_hints=["acct 1111"])),
                                    right=node(org("b", "Example Inc", [2], identity_hints=["acct 2222"])))],
            "shared_document": [dict(left=node(org("a", docs=[1, 2])))],
            "review_anchor": [dict(left=node(org("a", review_anchor=True)))],
            "direct_edge": [dict(left=node(org("a"), relationships=[{"neighbor_props": {"uuid": "b"}}]))],
            "identifiers": [dict(left=node(org("a", policy_number="POL-1111")),
                                 right=node(org("b", "Example Inc", [2], policy_number="POL-2222"))),
                            dict(left=node(org("a", reference_number="REF-1111"))),
                            dict(left=node(org("a", reference_number="123")),
                                 right=node(org("b", "Example Inc", [2], reference_number="456"))),
                            dict(left=node(org("a", reference_number=12)))],
        }
        cases["resolution_status"] = [dict(left=node(org("a", resolution_status=status)))
                                      for status in ("ambiguous", "isolated", "quarantined")]
        for reason, variants in cases.items():
            for variant in variants:
                with self.subTest(reason=reason, variant=variant):
                    self.assertEqual(self.blocker(**variant), reason)

    def test_exact_duplicate_addresses_are_eligible(self):
        named = node(org("n", "14 Larkspur Court, Springfield, IL 62701", [3], "Address"))
        for left, right in ((node(address("a")), node(address("b", "14 LARKSPUR CT. SPRINGFIELD IL 62701"))),
                            (node(address("a")), named)):
            with self.subTest(right=right):
                self.assertEqual(self.blocker(left, right, "Address"), "")

    def test_address_rules_block(self):
        cases = {
            "address_key": [(address("a"), address("b", "14 Larkspur Ct Apt 2, Springfield, IL 62701")),
                            (address("a", ""), address("b", "")),
                            ({"uuid": "a", "entity_type": "Address"}, {"uuid": "b", "entity_type": "Address"})],
            "postal_code": [(address("a", "14 Larkspur Ct"), address("b", "14 Larkspur Court")),
                            (address("a", "62701 Larkspur Ct"), address("b", "62701 Larkspur Ct."))],
            "shared_document": [(address("a"), address("b", source_doc_ids=[7]))],
        }
        for reason, pairs in cases.items():
            for left, right in pairs:
                with self.subTest(reason=reason, left=left, right=right):
                    left_node = node(left, relationships=[{"rel_props": {"source_doc": 7}}])
                    self.assertEqual(self.blocker(left_node, node(right), "Address"), reason)

    def test_equal_hints_and_identifiers_and_bookkeeping_do_not_block(self):
        left = node(org("a", identity_hints=["acct 1111"], policy_number="POL 1111", resolution_policy="old"))
        right = node(org("b", "Example Inc", [2], identity_hints=["acct 1111"], policy_number="pol 1111",
                         resolution_policy="evidence-identity-v2"))
        self.assertEqual(self.blocker(left, right), "")
        # get_node returns one null-neighbor row for a node without edges.
        self.assertEqual(self.blocker(left=node(org("a"), relationships=[{"neighbor_props": None}])), "")

    def test_survivor_prefers_documents_then_current_policy_then_smaller_uuid(self):
        def pick(left, right):
            return auto_merge_survivor(left, right)[0]["uuid"]
        self.assertEqual(pick(org("a", docs=[1]), org("b", docs=[2, 3])), "b")
        self.assertEqual(pick(org("a", resolution_policy="old"), org("b", resolution_policy="evidence-identity-v2")), "b")
        self.assertEqual(pick(org("b"), org("a")), "a")


class StewardAutoMergeTests(unittest.IsolatedAsyncioTestCase):
    """The steward pass with fake stores; every merge goes through a spy."""

    def setUp(self):
        self.nodes = [org("a", "Example, Inc.", [1]), org("b", "Example Inc", [2]),
                      org("c", "Other Co", [3]), org("d", "Other Co", [4]), org("e", "Other Co", [5])]
        self.decisions = MemoryDecisions()
        self.graph = SimpleNamespace(
            get_entities_by_type=AsyncMock(side_effect=lambda label: [copy.deepcopy(n) for n in self.nodes
                                                                       if n["entity_type"] == label]),
            get_entity_review_candidates=AsyncMock(return_value=[]))
        self.resolver = SimpleNamespace(
            auto_merge_check=AsyncMock(side_effect=lambda left, right: {
                "blocker": "", "primary_uuid": left, "duplicate_uuid": right}),
            merge_entities=AsyncMock(return_value={}))
        self.agent = SimpleNamespace(review_entity_candidate=AsyncMock(
            return_value={"recommendation": "merge", "confidence": .9, "risk": "low"}))

    async def run_steward(self, mode, cap=10):
        with patch.object(steward_module, "graph_store", self.graph), \
                patch.object(steward_module, "embeddings_store", self.decisions), \
                patch.object(steward_module, "entity_resolver", self.resolver), \
                patch.object(steward_module, "strands_orchestrator", self.agent), \
                patch.object(steward_module.settings, "entity_steward_auto_merge", mode), \
                patch.object(steward_module.settings, "entity_steward_auto_merge_max_per_run", cap):
            return await steward_module.EntitySteward().run_once(reason="test")

    async def test_off_makes_no_new_calls(self):
        report = await self.run_steward("off")
        self.graph.get_entities_by_type.assert_not_awaited()
        self.resolver.auto_merge_check.assert_not_awaited()
        self.resolver.merge_entities.assert_not_awaited()
        self.assertNotIn("auto_merged", report)

    async def test_default_and_shadow_record_suggestions_and_never_merge(self):
        self.assertEqual(steward_module.settings.entity_steward_auto_merge, "shadow")
        report = await self.run_steward(steward_module.settings.entity_steward_auto_merge)
        self.resolver.merge_entities.assert_not_awaited()
        self.assertEqual(report["would_auto_merge"], 1)
        self.assertEqual(report["auto_merged"], 0)
        self.assertEqual(report["auto_merge_groups_over_two"], 1)
        [row] = self.decisions.rows
        self.assertEqual((row["left_uuid"], row["right_uuid"], row["decision"]), ("a", "b", "suggest_auto_merge"))
        self.assertEqual((row["provenance"], row["review_method"]), ("automated_suggestion", "entity_steward"))
        self.assertEqual(json.loads(row["note"])["auto_merge"]["primary_uuid"], "a")
        # The regular pass skips the pair, so its newer row cannot hide the suggestion.
        ignored = self.graph.get_entity_review_candidates.await_args.args[0]
        self.assertIn(("a", "b"), ignored)
        self.assertIn("suggest_auto_merge", steward_module.SUGGESTION_DECISIONS)

    async def test_on_merges_only_eligible_pairs_with_the_automatic_origin(self):
        report = await self.run_steward("on")
        self.resolver.merge_entities.assert_awaited_once()
        args, kwargs = self.resolver.merge_entities.await_args
        self.assertEqual(args, ("a", "b"))
        self.assertEqual(kwargs["review_method"], AUTO_MERGE_REVIEW_METHOD)
        self.assertEqual(report["auto_merged"], 1)
        self.assertEqual(self.decisions.rows, [])

    async def test_cap_bounds_agent_calls_and_merges(self):
        self.nodes = [org(f"{i}{side}", f"Company {i}", [i * 10 + n]) for i in range(5) for n, side in enumerate("ab")]
        report = await self.run_steward("on", cap=2)
        self.assertEqual(self.resolver.merge_entities.await_count, 2)
        self.assertEqual(self.agent.review_entity_candidate.await_count, 2)
        self.assertEqual(report["auto_merge_blocked"]["cap"], 3)
        # Deferred pairs wait for a later run; the regular pass must not refresh them.
        self.assertTrue({("2a", "2b"), ("3a", "3b"), ("4a", "4b")}
                        <= self.graph.get_entity_review_candidates.await_args.args[0])
        # Deferred pairs skip the resolver's fresh reads too.
        self.assertEqual(self.resolver.auto_merge_check.await_count, 2)
        report = await self.run_steward("on", cap=0)
        self.assertEqual(self.resolver.merge_entities.await_count, 2)
        self.assertEqual(report["auto_merge_blocked"]["cap"], 5)

    async def test_shadow_runs_move_through_the_backlog(self):
        self.nodes = [org(f"{i}{side}", f"Company {i}", [i * 10 + n]) for i in range(3) for n, side in enumerate("ab")]
        seen = []
        for when in ("2026-01-01", "2026-01-02", "2026-01-03"):
            await self.run_steward("shadow", cap=1)
            row = max(self.decisions.rows, key=lambda r: r.get("created_at") or "9999")
            row["created_at"] = when
            seen.append(row["left_uuid"])
        self.assertEqual(sorted(seen), ["0a", "1a", "2a"])

    async def test_every_agent_answer_but_a_confident_low_risk_merge_blocks(self):
        reviews = [None, {}, [], "merge", {"recommendation": "merge", "confidence": .81, "risk": "low"},
                   {"recommendation": "merge", "confidence": True, "risk": "low"},
                   {"recommendation": "merge", "confidence": float("nan"), "risk": "low"},
                   {"recommendation": "merge", "confidence": "0.95", "risk": "low"},
                   {"recommendation": "merge", "confidence": 1.5, "risk": "low"},
                   {"recommendation": "merge", "confidence": .95},
                   {"recommendation": "merge", "confidence": .95, "risk": "medium"},
                   {"recommendation": "review", "confidence": .95, "risk": "low"},
                   RuntimeError("agent timeout")]
        for review in reviews:
            with self.subTest(review=review):
                self.setUp()
                if isinstance(review, Exception):
                    self.agent.review_entity_candidate.side_effect = review
                else:
                    self.agent.review_entity_candidate.return_value = review
                report = await self.run_steward("on")
                self.resolver.merge_entities.assert_not_awaited()
                self.assertEqual(report["auto_merge_blocked"], {"agent": 1})
                self.assertEqual([r["decision"] for r in self.decisions.rows], ["suggest_review"])

    async def test_terminal_rows_block_before_any_fresh_read(self):
        self.decisions.rows.append({"left_uuid": "a", "right_uuid": "b", "decision": "never_merge",
                                    "provenance": "human_review"})
        report = await self.run_steward("on")
        self.resolver.auto_merge_check.assert_not_awaited()
        self.assertEqual(report["auto_merge_blocked"], {"decision_row": 1})

    async def test_resolver_blocks_are_counted_and_skip_the_agent(self):
        self.resolver.auto_merge_check.side_effect = lambda left, right: {
            "blocker": "decision_row", "primary_uuid": left, "duplicate_uuid": right}
        report = await self.run_steward("on")
        self.agent.review_entity_candidate.assert_not_awaited()
        self.assertEqual(report["auto_merge_blocked"], {"decision_row": 1})

    async def test_prohibited_merge_is_recorded_for_review_and_the_run_continues(self):
        self.nodes = [org("a", "Example, Inc.", [1]), org("b", "Example Inc", [2]),
                      org("c", "Other Co", [3]), org("d", "Other Co", [4])]
        self.resolver.merge_entities.side_effect = [steward_module.EntityMergeProhibited("vetoed"), {}]
        report = await self.run_steward("on")
        self.assertEqual(self.resolver.merge_entities.await_count, 2)
        self.assertEqual(report["auto_merged"], 1)
        self.assertEqual(report["auto_merge_blocked"], {"prohibited": 1})
        self.assertEqual([(r["left_uuid"], r["decision"]) for r in self.decisions.rows], [("a", "suggest_review")])


class StewardAddressAutoMergeTests(StewardAutoMergeTests):
    """The same pass over Address nodes, keyed by the address."""

    def setUp(self):
        super().setUp()
        self.nodes = [address("a"), address("b", "14 LARKSPUR COURT, SPRINGFIELD IL 62701"),
                      address("c", "9 Elm St, Springfield, IL 62701"), address("d", "9 Elm St Apt 2, Springfield, IL 62701"),
                      {"uuid": "e", "entity_type": "Address"}, {"uuid": "f", "entity_type": "Address"}]

    async def test_shadow_suggests_the_exact_pair_and_names_its_key(self):
        report = await self.run_steward("shadow")
        self.resolver.merge_entities.assert_not_awaited()
        self.assertEqual((report["would_auto_merge"], report["auto_merged"]), (1, 0))
        [row] = self.decisions.rows
        self.assertEqual((row["left_uuid"], row["right_uuid"], row["decision"]), ("a", "b", "suggest_auto_merge"))
        facts = json.loads(row["note"])["auto_merge"]
        self.assertEqual((facts["label"], facts["key"]), ("Address", "14 larkspur ct springfield il 62701"))
        candidate = self.agent.review_entity_candidate.await_args.args[0]
        self.assertEqual(candidate["left"]["name"], "14 Larkspur Ct, Springfield, IL 62701")

    # The inherited Organization fixtures do not apply here.
    test_default_and_shadow_record_suggestions_and_never_merge = None
    test_cap_bounds_agent_calls_and_merges = None
    test_shadow_runs_move_through_the_backlog = None
    test_prohibited_merge_is_recorded_for_review_and_the_run_continues = None


class ResolverAutoMergeTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.graph = MemoryGraph([org("a", "Example, Inc.", [1]), org("b", "Example Inc", [2])])
        self.decisions = MemoryDecisions()
        self.graph.review_store = self.decisions
        for target, value in ((resolver_module, "graph_store"), (resolver_module, "embeddings_store")):
            patcher = patch.object(target, value, self.graph if value == "graph_store" else self.decisions)
            patcher.start()
            self.addCleanup(patcher.stop)
        patcher = patch.object(resolver_module.settings, "entity_steward_auto_merge", "on")
        patcher.start()
        self.addCleanup(patcher.stop)
        self.resolver = resolver_module.EntityResolver()

    async def auto_merge(self, primary="a", duplicate="b"):
        return await self.resolver.merge_entities(primary, duplicate, review_method=AUTO_MERGE_REVIEW_METHOD,
                                                  note='{"gate":"passed"}')

    async def test_success_records_auto_merged_without_a_human_alias(self):
        self.assertEqual((await self.resolver.auto_merge_check("b", "a"))["blocker"], "")
        merged = await self.auto_merge()
        self.assertNotIn("b", self.graph.nodes)
        self.assertEqual(self.graph.nodes["a"].get("alias_records", []), [])
        [row] = self.decisions.rows
        self.assertEqual((row["decision"], row["provenance"], row["review_method"], row["review_id"]),
                         ("auto_merged", "automated_merge", AUTO_MERGE_REVIEW_METHOD, None))
        self.assertEqual(row["note"], '{"gate":"passed"}')
        self.assertEqual(row["right_identity"]["canonical_name"], "Example Inc")
        self.assertIn(merged["merge_id"], self.decisions.journal)

    async def test_refused_unless_the_setting_is_on(self):
        for mode in ("off", "shadow"):
            with self.subTest(mode=mode), patch.object(resolver_module.settings, "entity_steward_auto_merge", mode):
                with self.assertRaisesRegex(ValueError, "not enabled"):
                    await self.auto_merge()
        self.assertEqual(set(self.graph.nodes), {"a", "b"})
        self.assertEqual(self.decisions.rows, [])

    async def test_recorded_split_blocks(self):
        await self.resolver.record_decision("a", "b", "split", review_method="entity_review_api")
        self.assertEqual((await self.resolver.auto_merge_check("a", "b"))["blocker"], "decision_row")
        with self.assertRaisesRegex(resolver_module.AutoMergeBlocked, "decision_row"):
            await self.auto_merge()
        self.assertEqual(set(self.graph.nodes), {"a", "b"})

    async def test_legacy_row_blocks(self):
        self.decisions.rows.append({"left_uuid": "a", "right_uuid": "b", "decision": "suggest_merge"})
        with self.assertRaisesRegex(resolver_module.AutoMergeBlocked, "decision_row"):
            await self.auto_merge()

    async def test_third_node_created_after_the_scan_blocks(self):
        self.assertEqual((await self.resolver.auto_merge_check("a", "b"))["blocker"], "")
        self.graph.nodes["c"] = org("c", "EXAMPLE INC", [3])
        with self.assertRaisesRegex(resolver_module.AutoMergeBlocked, "group_size"):
            await self.auto_merge()
        self.assertEqual(set(self.graph.nodes), {"a", "b", "c"})

    async def test_wrong_survivor_blocks(self):
        with self.assertRaisesRegex(resolver_module.AutoMergeBlocked, "survivor"):
            await self.auto_merge("b", "a")

    async def test_auto_merged_row_never_authorizes_a_different_name(self):
        await self.auto_merge()
        self.decisions.rows[0]["right_identity"]["reviewed_names"] = ["Example Holdings"]
        created = await self.resolver.resolve_organization("Example Holdings", 9)
        self.assertNotEqual(created, "a")

    async def test_reingesting_the_duplicates_document_binds_to_the_survivor(self):
        await self.auto_merge()
        self.assertEqual(await self.resolver.resolve_organization("Example Inc", 2), "a")
        self.assertEqual(await self.resolver.resolve_organization("Example, Inc.", 7), "a")
        self.assertEqual(set(self.graph.nodes), {"a"})

    async def test_unmerge_of_an_automatic_merge_reverts_its_decision(self):
        merged = await self.auto_merge()
        result = await self.resolver.unmerge_entities(merged["merge_id"], review_method="entity_review_api")
        self.assertEqual(result["status"], "reverted")
        statuses = {row["decision"]: row["identity_status"] for row in self.decisions.rows}
        self.assertEqual(statuses, {"auto_merged": "reverted", "split": "active"})
        self.assertEqual(set(self.graph.nodes), {"a", "b"})


class ResolverAddressAutoMergeTests(ResolverAutoMergeTests):
    """Document-owned Address nodes have no name; the address is the identity."""

    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.graph.nodes = {"a": address("a"), "b": address("b", "14 LARKSPUR COURT SPRINGFIELD IL 62701")}

    async def test_nameless_pair_merges_records_and_undoes(self):
        self.assertEqual((await self.resolver.auto_merge_check("b", "a"))["blocker"], "")
        merged = await self.auto_merge()
        self.assertEqual(set(self.graph.nodes), {"a"})
        [row] = self.decisions.rows
        self.assertEqual((row["decision"], row["provenance"]), ("auto_merged", "automated_merge"))
        self.assertEqual(row["right_identity"]["canonical_name"], "14 LARKSPUR COURT SPRINGFIELD IL 62701")
        self.assertEqual(self.decisions.journal[merged["merge_id"]]["review_method"], AUTO_MERGE_REVIEW_METHOD)
        result = await self.resolver.unmerge_entities(merged["merge_id"], review_method="entity_review_api")
        self.assertEqual(result["status"], "reverted")
        self.assertEqual(self.graph.nodes, {"a": address("a"), "b": address("b", "14 LARKSPUR COURT SPRINGFIELD IL 62701")})
        self.assertEqual(self.decisions.embeddings, {})

    async def test_undo_still_blocks_after_a_reindex_recreates_the_node(self):
        merged = await self.auto_merge()
        await self.resolver.unmerge_entities(merged["merge_id"], review_method="entity_review_api")
        # A reindex deletes the orphaned document-owned node and attaches a new UUID,
        # possibly with another spelling of the same address.
        for spelling in ("14 LARKSPUR COURT SPRINGFIELD IL 62701", "14 Larkspur Ct. Springfield IL 62701"):
            with self.subTest(spelling=spelling):
                self.graph.nodes.pop("b", None)
                self.graph.nodes["c"] = address("c", spelling)
                self.assertEqual((await self.resolver.auto_merge_check("a", "c"))["blocker"], "prohibited")

    async def test_undo_after_drift_names_the_documents_to_reindex(self):
        merged = await self.auto_merge()
        journal = self.decisions.journal[merged["merge_id"]]
        journal["before_image"]["edges"] = [
            {"type": "LOCATED_AT", "start": {"paperless_id": doc}, "end": {"uuid": node_uuid}, "props": {"source_doc": doc}}
            for doc, node_uuid in ((11, "a"), (12, "b"))]
        self.graph.unmerge_entities = AsyncMock(side_effect=resolver_module.MergeDrift("changed"))
        self.graph.reset_unmerged_fields = AsyncMock(return_value=True)
        result = await self.resolver.unmerge_entities(merged["merge_id"], review_method="entity_review_api")
        self.assertEqual((result["status"], result["reindex_doc_ids"]), ("conflict", [12]))

    async def test_address_without_name_or_full_address_never_qualifies(self):
        self.graph.nodes = {"a": {"uuid": "a", "entity_type": "Address"}, "b": {"uuid": "b", "entity_type": "Address"}}
        self.assertEqual((await self.resolver.auto_merge_check("a", "b"))["blocker"], "address_key")
        with self.assertRaisesRegex(ValueError, "Malformed canonical identity"):
            await self.auto_merge()
        self.assertEqual(set(self.graph.nodes), {"a", "b"})

    async def test_human_merge_of_a_nameless_address_is_still_refused(self):
        with self.assertRaisesRegex(ValueError, "Malformed canonical identity"):
            await self.resolver.merge_entities("a", "b", review_method="entity_review_api")

    # Organization-only behavior: ingestion and name changes.
    test_success_records_auto_merged_without_a_human_alias = None
    test_auto_merged_row_never_authorizes_a_different_name = None
    test_reingesting_the_duplicates_document_binds_to_the_survivor = None
    test_third_node_created_after_the_scan_blocks = None


if __name__ == "__main__":
    unittest.main()
