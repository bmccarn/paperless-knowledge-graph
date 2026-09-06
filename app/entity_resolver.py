"""Conservative identity resolution: proof selects; similarity only suggests."""
import asyncio
import logging
import uuid

from app.embeddings import embeddings_store
from app.graph import graph_store
from app.entity_decisions import (EntityMergeProhibited, NO_MERGE_DECISIONS,
                                  carried_vetoes, entity_identity, merge_is_prohibited)
from app.entity_policy import (ENTITY_TYPES, RESOLUTION_POLICY, display_name, name_key,
    context_bound_name, coreference_span, trusted_aliases, source_alias_record, alias_is_quarantined)

logger = logging.getLogger(__name__)


class EntityResolver:
    def __init__(self):
        self._mutation_lock = asyncio.Lock()

    async def record_decision(self, left_uuid: str, right_uuid: str, decision: str, note: str = "") -> dict:
        """Record a human decision with identities that survive derived reindex."""
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

    async def merge_entities(self, primary_uuid: str, duplicate_uuid: str) -> dict:
        """Merge only after checking and durably carrying human no-merge decisions."""
        if primary_uuid == duplicate_uuid:
            raise ValueError("An entity merge requires two different entities")
        async with self._mutation_lock:
            primary = await graph_store.get_node(primary_uuid)
            duplicate = await graph_store.get_node(duplicate_uuid)
            if not primary or not duplicate:
                raise ValueError("Entity pair not found")
            keep = entity_identity(primary)
            remove = entity_identity(duplicate)
            if not keep["type"] or keep["type"] != remove["type"]:
                raise ValueError("Only entities of the same type may merge")
            if "Document" in primary.get("labels", []) or "Document" in duplicate.get("labels", []):
                raise ValueError("Paperless documents cannot be merged as entities")
            decisions = await self._review_decisions()
            if merge_is_prohibited(keep, remove, decisions):
                raise EntityMergeProhibited("Human no-merge decision prohibits this entity pair")
            for row in carried_vetoes(keep, remove, decisions):
                await embeddings_store.add_entity_review_decision(
                    row["left_uuid"], row["right_uuid"], row["decision"], row.get("note") or "",
                    left_identity=row.get("left_identity"), right_identity=row.get("right_identity"),
                    provenance=row.get("provenance", "legacy_unknown"),
                    identity_status=row.get("identity_status", "active"),
                )
            review_id = str(uuid.uuid4())
            merged = await graph_store.merge_entities(primary_uuid, duplicate_uuid,
                                                       review_id=review_id)
            await embeddings_store.add_entity_review_decision(
                primary_uuid, duplicate_uuid, "merged", "",
                left_identity=keep, right_identity=remove,
                provenance="human_review", identity_status="active", review_id=review_id,
            )
            return merged

    async def resolve_person(self, name: str, source_doc_id: int, role: str = None,
                             description: str = None, **evidence) -> str:
        return await self.resolve(name, "Person", source_doc_id, description=description, **evidence)

    async def resolve_organization(self, name: str, source_doc_id: int,
                                   org_type: str = None, description: str = None, **evidence) -> str:
        return await self.resolve(name, "Organization", source_doc_id, description=description, **evidence)

    async def resolve_generic(self, name: str, entity_type: str, source_doc_id: int,
                              description: str = None, **evidence) -> str:
        return await self.resolve(name, entity_type, source_doc_id, description=description, **evidence)

    async def resolve(self, name: str, entity_type: str, source_doc_id: int, *,
                      description: str = None, source: str = "", identity_hint: str = "") -> str:
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
            candidates = await graph_store.get_entities_by_type(label)
            eligible = []
            for candidate in candidates:
                if candidate.get("resolution_status") == "quarantined":
                    continue
                if not await self._candidate_allowed(incoming, candidate, decisions):
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
                reason, span = self._match(name, label, source_doc_id, source, candidate, decisions)
                if reason:
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
                return candidate["uuid"]
            # Ambiguous exact names/aliases are not first-row-wins. A source-local
            # node is reused next time, limiting duplicate growth without guessing.
            props = {"name": name, "entity_type": label, "aliases": [], "alias_records": [],
                     "source_doc_ids": [source_doc_id], "resolution_policy": RESOLUTION_POLICY,
                     "resolution_status": "ambiguous" if eligible else "new",
                     "identity_hints": [identity_hint] if identity_hint else []}
            if description:
                props["description"] = description
            return await graph_store.create_node(label, props)

    @staticmethod
    def _match(name, kind, doc_id, source, candidate, decisions):
        canonical = candidate.get("name") or ""
        key = name_key(name, kind)
        if alias_is_quarantined(candidate, name, kind):
            return "", None
        # Newly human-reviewed merge decisions survive orphan cleanup. Only the
        # reviewed canonical names are authoritative, NOT their legacy aliases.
        for row in decisions:
            if (row.get("decision") != "merged" or row.get("provenance") != "human_review"
                    or row.get("identity_status") != "active"):
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
        span = coreference_span(canonical, name, source)
        if span:
            return "source_coreference", span
        if key == name_key(canonical, kind):
            if (not context_bound_name(name, kind) and not context_bound_name(canonical, kind)) or doc_id in (candidate.get("source_doc_ids") or []):
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
            nodes = await graph_store.get_entities_by_type(kind)
            for index, left in enumerate(nodes):
                for right in nodes[index+1:]:
                    prohibited = merge_is_prohibited(entity_identity(left), entity_identity(right), decisions)
                    if prohibited or name_key(left.get("name", ""), kind) == name_key(right.get("name", ""), kind):
                        report["skipped"].append({"a": left["uuid"], "b": right["uuid"],
                            "reason": "human no-merge decision" if prohibited else "human identity review required"})
        report["total_skipped"] = len(report["skipped"])
        return report


entity_resolver = EntityResolver()
