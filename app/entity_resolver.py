"""Conservative identity resolution: proof selects; similarity only suggests."""
import asyncio
import logging
import uuid

from app.config import settings
from app.embeddings import embeddings_store
from app.graph import MergeDrift, graph_store
from app.entity_decisions import (EntityMergeProhibited, NO_MERGE_DECISIONS,
                                  carried_vetoes, entity_identity, merge_is_prohibited, identity_has_unresolved_veto,
                                  alias_revocations, alias_replay_prohibited)
from app.entity_policy import (ENTITY_TYPES, RESOLUTION_POLICY, EXPLICIT_REVIEW_METHOD, display_name, name_key,
    AUTO_MERGE_REVIEW_METHOD, AUTO_MERGE_LABELS, TERMINAL_DECISIONS, auto_merge_blocker, auto_merge_key, auto_merge_survivor,
    context_bound_name, trusted_aliases, source_alias_record, alias_is_quarantined, initialism_expansions,
    has_local_alias_definition, verified_coreference, valid_identity_receipt, digest)

logger = logging.getLogger(__name__)


class AutoMergeBlocked(ValueError):
    """Fresh reads under the lock no longer pass the automatic merge gate."""


class EntityResolver:
    def __init__(self):
        self._mutation_lock = asyncio.Lock()

    async def _ensure_embedding(self, props: dict) -> str:
        if not isinstance(props.get("name"), str) or not props["name"].strip():
            return props["uuid"]  # nameless record nodes have no entity vector
        await embeddings_store.store_entity_embedding(
            props["uuid"], props["name"], props["entity_type"], props.get("description") or "",
            only_missing=True)
        return props["uuid"]

    async def record_decision(self, left_uuid: str, right_uuid: str, decision: str, note: str = "",
                              *, review_method: str | None = None) -> dict:
        """Record an explicitly admitted review; no caller origin is inferred."""
        if review_method != EXPLICIT_REVIEW_METHOD:
            raise ValueError("Explicit review origin is required")
        if decision not in {"split", "never_merge", "ignore"}:
            raise ValueError("Unsupported human entity decision")
        if left_uuid == right_uuid:
            raise ValueError("An entity decision requires two different entities")
        async with self._mutation_lock:
            left = await graph_store.get_node(left_uuid)
            right = await graph_store.get_node(right_uuid)
            if not left or not right:
                raise ValueError("Entity pair not found")
            return await embeddings_store.add_entity_review_decision(
                left_uuid, right_uuid, decision, note,
                left_identity=entity_identity(left), right_identity=entity_identity(right),
                provenance="human_review", identity_status="active",
                review_method=review_method, review_id=str(uuid.uuid4()),
            )

    async def _review_decisions(self) -> list[dict]:
        decisions = await embeddings_store.get_entity_review_decisions()
        for row in decisions:
            if row.get("decision") not in NO_MERGE_DECISIONS:
                continue
            for side in ("left", "right"):
                if not row.get(f"{side}_identity"):
                    node = await graph_store.get_node(row[f"{side}_uuid"])
                    if node:
                        row[f"{side}_identity"] = entity_identity(node)
            if not row.get("left_identity") or not row.get("right_identity"):
                row["identity_status"] = "unresolved_legacy"
        return decisions

    async def hydrate_review_identities(self) -> dict:
        """Persist legacy veto identities before reindex can remove their nodes."""
        report = {"hydrated": 0, "unresolved": []}
        async with self._mutation_lock:
            for row in await embeddings_store.get_entity_review_decisions():
                if row.get("decision") not in NO_MERGE_DECISIONS:
                    continue
                changed = False
                missing = []
                for side in ("left", "right"):
                    if row.get(f"{side}_identity"):
                        continue
                    node = await graph_store.get_node(row[f"{side}_uuid"])
                    if node:
                        row[f"{side}_identity"] = entity_identity(node)
                        changed = True
                    else:
                        missing.append(row[f"{side}_uuid"])
                status = "unresolved_legacy" if missing else "active"
                if missing:
                    # Keep every surviving anchor of a partial legacy veto. A
                    # snapshot is useful history, not permission to delete it.
                    for side in ("left", "right"):
                        if row[f"{side}_uuid"] not in missing:
                            await graph_store.protect_review_anchor(row[f"{side}_uuid"])
                if changed or row.get("identity_status") != status:
                    await embeddings_store.hydrate_entity_review_identities(row, status)
                if changed:
                    report["hydrated"] += 1
                if missing:
                    report["unresolved"].append({"left_uuid": row["left_uuid"], "right_uuid": row["right_uuid"], "missing_uuid": missing})
        if report["unresolved"]:
            logger.warning("%s legacy no-merge decisions have missing identities; reindex persistence cannot be fully recovered", len(report["unresolved"]))
        return report

    async def _candidate_allowed(self, incoming: dict, candidate: dict, decisions: list[dict]) -> bool:
        # Candidate lists come from fresh graph reads. Full neighborhoods are
        # needed only to distinguish the source context of a human veto.
        if not any(row.get("decision") in NO_MERGE_DECISIONS for row in decisions):
            return True
        node = await graph_store.get_node(candidate["uuid"])
        return bool(node) and not merge_is_prohibited(incoming, entity_identity(node), decisions)

    async def _auto_merge_gate(self, left_uuid: str, right_uuid: str, decisions: list[dict]) -> tuple[str, str, str]:
        """Every automatic merge rule except the agent and the cap, on fresh reads.

        Returns (blocker, primary_uuid, duplicate_uuid); "" means eligible. The caller holds the lock.
        """
        nodes = {left_uuid: await graph_store.get_node(left_uuid), right_uuid: await graph_store.get_node(right_uuid)}
        if not all(nodes.values()) or left_uuid == right_uuid:
            return "missing", left_uuid, right_uuid
        left, right = nodes[left_uuid], nodes[right_uuid]
        label = (left.get("labels") or [""])[0]
        if label not in AUTO_MERGE_LABELS:
            return "label", left_uuid, right_uuid
        key = auto_merge_key(left["properties"], label)
        # shortcut: reads the whole label per pair; batch the count if labels reach tens of thousands of nodes.
        count = sum(1 for node in await graph_store.get_entities_by_type(label) if auto_merge_key(node, label) == key)
        primary, duplicate = auto_merge_survivor({**left["properties"], "uuid": left_uuid},
                                                 {**right["properties"], "uuid": right_uuid})
        primary_uuid, duplicate_uuid = primary["uuid"], duplicate["uuid"]
        blocker = auto_merge_blocker(left, right, label, count)
        if blocker:
            return blocker, primary_uuid, duplicate_uuid
        if any({row.get("left_uuid"), row.get("right_uuid")} == {left_uuid, right_uuid}
               and (row.get("decision") in TERMINAL_DECISIONS
                    or (row.get("provenance") or "legacy_unknown") == "legacy_unknown")
               for row in decisions):
            return "decision_row", primary_uuid, duplicate_uuid
        keep, remove = entity_identity(nodes[primary_uuid]), entity_identity(nodes[duplicate_uuid])
        decisions = decisions + alias_revocations(left) + alias_revocations(right)
        if (alias_replay_prohibited(nodes[primary_uuid]["properties"], remove["canonical_name"], keep["type"], decisions)
                or merge_is_prohibited(keep, remove, decisions)):
            return "prohibited", primary_uuid, duplicate_uuid
        return "", primary_uuid, duplicate_uuid

    async def auto_merge_check(self, left_uuid: str, right_uuid: str) -> dict:
        """Read-only gate for the steward; merge_entities checks it again before merging."""
        async with self._mutation_lock:
            blocker, primary_uuid, duplicate_uuid = await self._auto_merge_gate(
                left_uuid, right_uuid, await self._review_decisions())
        return {"blocker": blocker, "primary_uuid": primary_uuid, "duplicate_uuid": duplicate_uuid}

    async def merge_entities(self, primary_uuid: str, duplicate_uuid: str,
                             *, review_method: str | None = None, note: str = "") -> dict:
        """Merge only with explicit review origin, after durably carrying vetoes.

        The steward's automatic origin also needs the setting "on" and the full
        automatic gate on fresh reads; it records `auto_merged` and no human alias.
        """
        automatic = review_method == AUTO_MERGE_REVIEW_METHOD
        if automatic and settings.entity_steward_auto_merge != "on":
            raise ValueError("Automatic entity merge is not enabled")
        if review_method != EXPLICIT_REVIEW_METHOD and not automatic:
            raise ValueError("Explicit review origin is required for entity merge")
        if primary_uuid == duplicate_uuid:
            raise ValueError("An entity merge requires two different entities")
        async with self._mutation_lock:
            primary = await graph_store.get_node(primary_uuid)
            duplicate = await graph_store.get_node(duplicate_uuid)
            if not primary or not duplicate:
                raise ValueError("Entity pair not found")
            keep = entity_identity(primary)
            remove = entity_identity(duplicate)
            # A nameless Address has the address as its identity; only the automatic gate merges one.
            if any(not isinstance(identity.get("canonical_name"), str) or not identity["canonical_name"].strip()
                   for identity in (keep, remove)) or (not automatic and any(
                       not isinstance(node["properties"].get("name"), str) for node in (primary, duplicate))):
                raise ValueError("Malformed canonical identity requires explicit repair before merge")
            if not keep["type"] or keep["type"] != remove["type"]:
                raise ValueError("Only entities of the same type may merge")
            if "Document" in primary.get("labels", []) or "Document" in duplicate.get("labels", []):
                raise ValueError("Paperless documents cannot be merged as entities")
            decisions = await self._review_decisions()
            if automatic:
                blocker, survivor, _ = await self._auto_merge_gate(primary_uuid, duplicate_uuid, decisions)
                if blocker or survivor != primary_uuid:
                    raise AutoMergeBlocked(blocker or "survivor")
            revocations = alias_revocations(primary) + alias_revocations(duplicate)
            await embeddings_store.preserve_alias_revocations(revocations)
            decisions += revocations
            if alias_replay_prohibited(primary["properties"], remove["canonical_name"], keep["type"], decisions):
                raise EntityMergeProhibited("Alias quarantine requires explicit fresh reauthorization")
            if merge_is_prohibited(keep, remove, decisions):
                raise EntityMergeProhibited("Human no-merge decision prohibits this entity pair")
            for row in carried_vetoes(keep, remove, decisions):
                await embeddings_store.add_entity_review_decision(
                    row["left_uuid"], row["right_uuid"], row["decision"], row.get("note") or "",
                    left_identity=row.get("left_identity"), right_identity=row.get("right_identity"),
                    provenance=row.get("provenance", "legacy_unknown"),
                    identity_status=row.get("identity_status", "active"),
                    review_method=row.get("review_method", "legacy_unknown"), review_id=row.get("review_id"),
                )
            # No review_id: the graph then writes no human_review alias record.
            review_id = None if automatic else str(uuid.uuid4())
            merge_id = str(uuid.uuid4())
            props = primary["properties"]
            await self._ensure_embedding({**props, "entity_type": keep["type"]})
            merged = await graph_store.merge_entities(primary_uuid, duplicate_uuid, merge_id=merge_id,
                                                       review_id=review_id, review_method=review_method)
            await embeddings_store.add_entity_review_decision(
                primary_uuid, duplicate_uuid, "auto_merged" if automatic else "merged", note,
                left_identity=keep, right_identity=remove,
                provenance="automated_merge" if automatic else "human_review", identity_status="active",
                review_id=review_id, review_method=review_method,
            )
            await embeddings_store.set_merge_journal_status(merge_id, "applied")
            return {**merged, "merge_id": merge_id}

    async def unmerge_entities(self, merge_id: str, note: str = "", *, review_method: str | None = None) -> dict:
        """Undo a journaled merge. PG says "separate" before the graph changes, so a
        failure leans toward separation and a second call resumes."""
        if review_method != EXPLICIT_REVIEW_METHOD:
            raise ValueError("Explicit review origin is required for entity unmerge")
        async with self._mutation_lock:
            journal = await embeddings_store.get_merge_journal(merge_id)
            if not journal:
                raise ValueError("Merge not found")
            if journal["status"] in {"reverted", "conflict"}:
                raise ValueError(f"Merge was already undone ({journal['status']})")
            primary_uuid, duplicate_uuid = journal["primary_uuid"], journal["duplicate_uuid"]
            nodes = {n["props"]["uuid"]: {"labels": n["labels"], "properties": n["props"]}
                     for n in journal["before_image"]["nodes"]}
            # The veto must match rebuilt nodes by name and documents, as for a live node.
            for edge in journal["before_image"]["edges"]:
                for near, far in (("start", "end"), ("end", "start")):
                    if edge[near].get("uuid") in nodes:
                        nodes[edge[near]["uuid"]].setdefault("relationships", []).append(
                            {"neighbor_props": edge[far], "rel_props": edge["props"]})
            await embeddings_store.set_merge_journal_status(merge_id, "reverting", reverted_by=review_method, note=note)
            merged_decision = "auto_merged" if journal.get("review_method") == AUTO_MERGE_REVIEW_METHOD else "merged"
            await embeddings_store.set_entity_review_decision_status(primary_uuid, duplicate_uuid, merged_decision, "reverted")
            await embeddings_store.add_entity_review_decision(
                primary_uuid, duplicate_uuid, "split", note,
                left_identity=entity_identity(nodes[primary_uuid]), right_identity=entity_identity(nodes[duplicate_uuid]),
                provenance="human_review", identity_status="active",
                review_method=review_method, review_id=str(uuid.uuid4()),
            )
            try:
                await graph_store.unmerge_entities(journal)
            except MergeDrift as drift:
                logger.warning("Merge %s drifted (%s); resetting fields and reindexing the duplicate's documents",
                               merge_id, drift)
                primary, duplicate = nodes[primary_uuid]["properties"], nodes[duplicate_uuid]["properties"]
                kind = primary.get("entity_type") or nodes[primary_uuid]["labels"][0]
                # A same-name revocation would quarantine the survivor's own name; the split veto suffices.
                if (await graph_store.reset_unmerged_fields(journal) and journal.get("review_id")
                        and name_key(duplicate["name"], kind) != name_key(primary["name"], kind)):
                    await graph_store.add_entity_alias_record(primary_uuid, {
                        "alias": duplicate["name"], "canonical_name": primary["name"], "type": kind,
                        "status": "revoked", "review_id": journal["review_id"], "review_method": review_method})
                await embeddings_store.set_merge_journal_status(merge_id, "conflict")
                # Document-owned nodes carry their document on the journaled edges.
                return {"status": "conflict", "merge_id": merge_id,
                        "reindex_doc_ids": entity_identity(nodes[duplicate_uuid])["source_doc_ids"]}
            await embeddings_store.restore_merge_embedding(merge_id)
            for node in nodes.values():
                props = node["properties"]
                await self._ensure_embedding({**props, "entity_type": props.get("entity_type") or node["labels"][0]})
            await embeddings_store.set_merge_journal_status(merge_id, "reverted")
            return {"status": "reverted", "merge_id": merge_id, "reindex_doc_ids": []}

    async def resolve_person(self, name: str, source_doc_id: int, role: str = None,
                             description: str = None, **evidence) -> str:
        return await self.resolve(name, "Person", source_doc_id, description=description, role=role, **evidence)

    async def resolve_organization(self, name: str, source_doc_id: int,
                                   org_type: str = None, description: str = None, **evidence) -> str:
        return await self.resolve(name, "Organization", source_doc_id, description=description, org_type=org_type, **evidence)

    async def resolve_generic(self, name: str, entity_type: str, source_doc_id: int,
                              description: str = None, **evidence) -> str:
        return await self.resolve(name, entity_type, source_doc_id, description=description, **evidence)

    async def resolve(self, name: str, entity_type: str, source_doc_id: int, *,
                      description: str = None, source: str = "", identity_hint: str = "",
                      role: str = None, org_type: str = None, identity_proofs: list | None = None,
                      name_usage: str = "") -> str:
        """Resolve once under the review lock; callers must bind the returned UUID.

        Source-less paths preserve the supplied type. Hints must already have
        passed literal source validation; direct callers cannot invent them.
        """
        if not isinstance(name, str) or not name.strip() or entity_type not in ENTITY_TYPES:
            raise ValueError("An explicit supported entity type and name are required")
        if identity_hint and (not source or identity_hint not in source):
            raise ValueError("Identity discriminator requires literal source evidence")
        name = display_name(name)
        label = "DocumentRef" if entity_type == "Document" else entity_type
        async with self._mutation_lock:
            decisions = await self._review_decisions()  # unavailable => no mutation
            incoming = entity_identity({"name": name, "entity_type": label,
                                        "source_doc_ids": [source_doc_id]})
            expansions = initialism_expansions(name, source)
            local_definition = has_local_alias_definition(name, source)
            source_folded = display_name(source).casefold()
            for expansion in expansions:
                incoming["names"] = sorted(set(incoming["names"]) | set(entity_identity({"name": expansion})["names"]))
            candidates = await graph_store.get_entities_by_type(label)
            revocations = [row for candidate in candidates for row in alias_revocations(candidate)]
            await embeddings_store.preserve_alias_revocations(revocations)
            decisions += revocations
            isolated_key = digest(str(source_doc_id) + ":" + digest(source) + ":" + label + ":"
                                  + name_key(name, label) + ":" + identity_hint + ":" + RESOLUTION_POLICY)
            # This is idempotent source identity, NOT attachment to a canonical
            # real-world identity. It may bypass candidate veto filtering only
            # for an explicitly isolated node with this exact generation key.
            isolated = [candidate for candidate in candidates
                        if candidate.get("isolated_source_key") == isolated_key
                        and candidate.get("resolution_status") == "isolated"
                        and candidate.get("source_doc_ids") == [source_doc_id]]
            if len(isolated) == 1:
                return await self._ensure_embedding(isolated[0])
            eligible = []
            # A quarantined candidate elsewhere in this type must not isolate
            # unrelated incoming identities. Preserve the incoming veto even
            # when its original graph anchor is no longer present.
            vetoed = identity_has_unresolved_veto(incoming, decisions)
            for candidate in candidates:
                if (not isinstance(candidate.get("name"), str) or not candidate["name"].strip()
                        or not isinstance(candidate.get("uuid"), str) or not candidate["uuid"]):
                    continue  # preserve malformed legacy rows, but never use them as identity proof
                if candidate.get("resolution_status") == "quarantined":
                    continue
                if candidate.get("resolution_status") == "isolated":
                    continue
                # The typed snapshot already contains the evidence needed to
                # reject unrelated identities. Load a full neighborhood only for
                # a possible match, retaining every source-context veto check.
                reason, span = self._match(name, label, source_doc_id, source, candidate, decisions,
                                          expansions, local_definition, source_folded, identity_proofs, name_usage)
                if not reason:
                    continue
                if not await self._candidate_allowed(incoming, candidate, decisions):
                    vetoed = True
                    continue
                hints = candidate.get("identity_hints") or []
                own_source = source_doc_id in (candidate.get("source_doc_ids") or [])
                if candidate.get("resolution_status") == "ambiguous" and not own_source:
                    continue
                if identity_hint and hints and identity_hint not in hints:
                    continue
                # An unqualified mention cannot choose between qualified homonyms.
                if bool(hints) != bool(identity_hint) and not own_source:
                    continue
                eligible.append((candidate, reason, span))
            # Prefer an already-bound source only among otherwise proven matches.
            scoped = [item for item in eligible if source_doc_id in (item[0].get("source_doc_ids") or [])]
            if len(scoped) == 1:
                eligible = scoped
            if len(eligible) == 1:
                candidate, reason, span = eligible[0]
                if span:
                    await graph_store.add_entity_alias_record(candidate["uuid"],
                        source_alias_record(candidate["name"], name, label, source_doc_id, source, span))
                await graph_store.record_entity_source(candidate["uuid"], source_doc_id,
                                                       identity_hint=identity_hint)
                return await self._ensure_embedding(candidate)
            # Ambiguous exact names/aliases are not first-row-wins. A source-local
            # node is reused next time, limiting duplicate growth without guessing.
            props = {"name": name, "entity_type": label, "aliases": [], "alias_records": [],
                     "source_doc_ids": [source_doc_id], "resolution_policy": RESOLUTION_POLICY,
                     "resolution_status": "ambiguous" if eligible else "new",
                     "identity_hints": [identity_hint] if identity_hint else [], "name_usage": name_usage}
            if vetoed:
                props.update(resolution_status="isolated", isolated_source_key=isolated_key)
            if description:
                props["description"] = description
            if label == "Person" and role:
                props["role"] = role
            if label == "Organization" and org_type:
                props["type"] = org_type
            node_uuid = await graph_store.create_node(label, props)
            return await self._ensure_embedding({**props, "uuid": node_uuid})

    @staticmethod
    def _match(name, kind, doc_id, source, candidate, decisions, expansions, local_definition, source_folded,
               identity_proofs=None, name_usage=""):
        canonical = candidate.get("name") or ""
        key = name_key(name, kind)
        if alias_is_quarantined(candidate, name, kind) or alias_replay_prohibited(candidate, name, kind, decisions):
            return "", None
        for proof in identity_proofs or []:
            if (valid_identity_receipt(proof, source)
                    and ("DocumentRef" if proof["type"] == "Document" else proof["type"]) == kind
                    and {name_key(proof["left_name"], kind), name_key(proof["right_name"], kind)} == {key, name_key(canonical, kind)}
                    and (proof["status"] != "affirmed" or proof["assertion_scope"] != "current_direct" or not proof["explicit"])):
                return "", None
        # Source definitions outrank a globally reviewed abbreviation meaning.
        # Multiple local expansions remain ambiguous even if only one exists in
        # the current graph; otherwise first-seen graph state would choose sense.
        if len(expansions) > 1:
            return "", None
        if expansions:
            expansion = expansions[0]
            if name_key(expansion, kind) != name_key(canonical, kind):
                return "", None
            proof = verified_coreference(expansion, name, kind, source, identity_proofs)
            return ("source_coreference", proof) if proof else ("", None)
        # Necessary literal-name prefilter avoids scanning a large source with
        # multiple regexes for every unrelated corpus candidate. This is local
        # to this call/revision, never a cross-document decision cache.
        direct_span = None
        if display_name(canonical).casefold() in source_folded:
            direct_span = verified_coreference(canonical, name, kind, source, identity_proofs)
        if local_definition and not direct_span:
            return "", None
        # Newly human-reviewed merge decisions survive orphan cleanup. Only the
        # reviewed canonical names are authoritative, NOT their legacy aliases.
        for row in decisions:
            if (row.get("decision") != "merged" or row.get("provenance") != "human_review"
                    or row.get("identity_status") != "active" or not row.get("review_id")
                    or row.get("review_method") != EXPLICIT_REVIEW_METHOD):
                continue
            a, b = row.get("left_identity") or {}, row.get("right_identity") or {}
            if a.get("type") != kind or b.get("type") != kind:
                continue
            pair = {name_key(value, kind) for snapshot in (a, b)
                    for value in [snapshot.get("canonical_name", ""), *(snapshot.get("reviewed_names") or [])]}
            if "" not in pair and key in pair and name_key(canonical, kind) in pair:
                return "human_review", None
        if any(key == name_key(alias, kind) for alias in trusted_aliases(candidate, kind, doc_id, source)):
            return "verified_alias", None
        if direct_span:
            return "source_coreference", direct_span
        if key == name_key(canonical, kind):
            if (not context_bound_name(name, kind, name_usage) and not context_bound_name(canonical, kind, candidate.get("name_usage", ""))) or doc_id in (candidate.get("source_doc_ids") or []):
                return "orthographic", None
        return "", None

    async def resolve_all_entities(self) -> dict:
        """Compatibility endpoint: candidate discovery only, never bulk merging.

        Even identical names can be homonyms. Ingestion reuses unique compatible
        exact identities; merging existing UUIDs is a separate human review act.
        """
        decisions = await self._review_decisions()
        report = {"merged_persons": [], "merged_orgs": [], "skipped": [], "errors": [],
                  "total_merged": 0, "policy": RESOLUTION_POLICY}
        for kind in ("Person", "Organization"):
            nodes = [node for node in await graph_store.get_entities_by_type(kind)
                     if isinstance(node.get("name"), str) and node["name"].strip() and node.get("uuid")]
            for index, left in enumerate(nodes):
                for right in nodes[index+1:]:
                    prohibited = merge_is_prohibited(entity_identity(left), entity_identity(right), decisions)
                    if prohibited or name_key(left.get("name", ""), kind) == name_key(right.get("name", ""), kind):
                        report["skipped"].append({"a": left["uuid"], "b": right["uuid"],
                            "reason": "human no-merge decision" if prohibited else "human identity review required"})
        report["total_skipped"] = len(report["skipped"])
        return report


entity_resolver = EntityResolver()
