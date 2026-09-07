"""Conservative human no-merge policy independent of graph UUID generations."""
import json
import re

NO_MERGE_DECISIONS = frozenset({"split", "never_merge"})


class EntityMergeProhibited(ValueError):
    """A human decision prohibits the requested entity merge."""


def _names(value):
    if not isinstance(value, list):
        value = [value]
    return {
        " ".join(re.sub(r"[^\w]+", " ", str(name).casefold()).split())
        for name in value if isinstance(name, str) and name.strip()
    }


def _document_ids(value):
    if not isinstance(value, (list, tuple, set)):
        value = [value]
    ids = set()
    for item in value:
        if isinstance(item, bool):
            continue
        try:
            ids.add(int(item))
        except (TypeError, ValueError):
            continue
    return ids


def entity_identity(node: dict) -> dict:
    """Capture review identity; names constrain merging, not prove sameness."""
    props = node.get("properties", node)
    labels = node.get("labels") or []
    kind = props.get("entity_type") or (labels[0] if labels else "")
    kind = "DocumentRef" if kind == "Document" else kind
    names = _names(props.get("name")) | _names(props.get("aliases") or [])
    docs = _document_ids(props.get("source_doc_ids") or [])
    for relationship in node.get("relationships") or []:
        docs |= _document_ids((relationship.get("neighbor_props") or {}).get("paperless_id"))
        rel_props = relationship.get("rel_props") or {}
        docs |= _document_ids(rel_props.get("source_doc"))
        docs |= _document_ids(rel_props.get("source_doc_ids") or [])
    from app.entity_policy import trusted_aliases
    reviewed_names = trusted_aliases(props, kind, -1, "")
    return {"uuid": props.get("uuid"), "type": kind, "canonical_name": props.get("name") or "",
            "reviewed_names": reviewed_names,
            "names": sorted(names), "source_doc_ids": sorted(docs)}


def _snapshot(value):
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except (ValueError, TypeError):
            return {}
    return value if isinstance(value, dict) else {}


def _matches(candidate, node_uuid, snapshot, other_snapshot):
    if candidate.get("uuid") and candidate["uuid"] == node_uuid:
        return True
    if not snapshot or candidate.get("type") != snapshot.get("type"):
        return False
    names = set(candidate.get("names") or [])
    snapshot_names = set(snapshot.get("names") or [])
    if not names & snapshot_names:
        return False
    # For reviewed same-name entities, a known source can identify the opposite
    # side. An unknown/new source remains ambiguous and cannot authorize merging.
    if snapshot_names & set(other_snapshot.get("names") or []):
        docs = set(candidate.get("source_doc_ids") or [])
        own_docs = set(snapshot.get("source_doc_ids") or [])
        other_docs = set(other_snapshot.get("source_doc_ids") or [])
        if docs & other_docs and not docs & own_docs:
            return False
    return True


def identity_has_unresolved_veto(identity: dict, decisions: list[dict]) -> bool:
    """Whether this identity itself matches an identifiable partial-veto side."""
    for row in decisions:
        if row.get("decision") not in NO_MERGE_DECISIONS or row.get("identity_status") != "unresolved_legacy":
            continue
        left, right = _snapshot(row.get("left_identity")), _snapshot(row.get("right_identity"))
        if left and right:
            continue
        for snapshot, side, other in ((left, "left", right), (right, "right", left)):
            if snapshot and _matches(identity, row.get(f"{side}_uuid"), snapshot, other):
                return True
    return False


def merge_is_prohibited(left: dict, right: dict, decisions: list[dict]) -> bool:
    """Evaluate both orientations of each durable human veto."""
    # Candidate rejection can protect either endpoint. Source isolation must
    # separately ask whether the incoming identity itself is implicated.
    if identity_has_unresolved_veto(left, decisions) or identity_has_unresolved_veto(right, decisions):
        return True
    for row in decisions:
        if row.get("decision") not in NO_MERGE_DECISIONS:
            continue
        a = _snapshot(row.get("left_identity"))
        b = _snapshot(row.get("right_identity"))
        for first, second in ((left, right), (right, left)):
            if _matches(first, row.get("left_uuid"), a, b) and _matches(second, row.get("right_uuid"), b, a):
                return True
    return False


def carried_vetoes(keep: dict, remove: dict, decisions: list[dict]) -> list[dict]:
    """Carry every affected veto onto the surviving canonical identity."""
    combined = {
        **keep,
        "names": sorted(set(keep.get("names") or []) | set(remove.get("names") or [])),
        "source_doc_ids": sorted(set(keep.get("source_doc_ids") or []) | set(remove.get("source_doc_ids") or [])),
    }
    carried = []
    for row in decisions:
        if row.get("decision") not in NO_MERGE_DECISIONS:
            continue
        result = dict(row)
        snapshots = {side: _snapshot(row.get(f"{side}_identity")) for side in ("left", "right")}
        changed = False
        for side, other in (("left", "right"), ("right", "left")):
            if any(_matches(candidate, row.get(f"{side}_uuid"), snapshots[side], snapshots[other]) for candidate in (keep, remove)):
                result[f"{side}_uuid"] = keep["uuid"]
                result[f"{side}_identity"] = {
                    **combined,
                    "names": sorted(set(combined["names"]) | set(snapshots[side].get("names") or [])),
                    "source_doc_ids": sorted(set(combined["source_doc_ids"]) | set(snapshots[side].get("source_doc_ids") or [])),
                }
                changed = True
        if changed:
            if result["left_uuid"] == result["right_uuid"]:
                raise EntityMergeProhibited("Human no-merge decision has ambiguous entity identities")
            carried.append(result)
    return carried


def alias_revocations(node: dict) -> list[dict]:
    """Stable negative authority with the same name lineage as positive replay.

    Independent of graph UUID, source revision and resolution-policy generation.
    Records are appended to the durable review ledger, never replacing reviews.
    """
    from app.entity_policy import alias_records, digest, name_key
    identity = entity_identity(node)
    props = node.get("properties", node)
    records = []
    for record in alias_records(props):
        if record.get("status") not in {"quarantined", "revoked", "untrusted"}:
            continue
        kind, alias = record.get("type"), record.get("alias")
        canonical = record.get("canonical_name") or identity["canonical_name"]
        if not isinstance(alias, str) or not alias.strip() or kind != identity["type"] or not canonical:
            continue
        hints = sorted(set(props.get("identity_hints") or []))
        key = digest(json.dumps([kind, name_key(canonical, kind), name_key(alias, kind), hints]))
        records.append({"left_uuid": "alias-lineage-" + key, "right_uuid": "alias-revocation-" + key,
            "decision": "alias_revoked", "identity_status": "active", "provenance": "alias_quarantine",
            "review_id": key, "review_method": "alias_quarantine",
            "left_identity": {**identity, "canonical_name": canonical,
                              "identity_hints": hints},
            "right_identity": {"type": kind, "canonical_name": alias},
            "note": "Durable alias revocation; historical positive reviews retained"})
    return records


def alias_replay_prohibited(canonical: dict, alias: str, kind: str, decisions: list[dict]) -> bool:
    """Negative pair authority follows explicitly reviewed lineage, not UUIDs."""
    from app.entity_policy import EXPLICIT_REVIEW_METHOD, name_key
    canonical_key = name_key(canonical.get("name", ""), kind)
    alias_key = name_key(alias, kind)
    for row in decisions:
        if row.get("decision") != "alias_revoked":
            continue
        left, right = _snapshot(row.get("left_identity")), _snapshot(row.get("right_identity"))
        if left.get("type") != kind or right.get("type") != kind:
            continue
        hints, current_hints = left.get("identity_hints") or [], canonical.get("identity_hints") or []
        if hints and current_hints and not set(hints) & set(current_hints):
            continue
        revoked = name_key(right.get("canonical_name", ""), kind)
        lineage = {name_key(left.get("canonical_name", ""), kind)}
        # Follow positive canonical lineage, but do not traverse the revoked
        # alias itself: that would turn a pair veto into a global name ban.
        changed = True
        while changed:
            changed = False
            for review in decisions:
                if (review.get("decision") != "merged" or review.get("provenance") != "human_review"
                        or review.get("identity_status") != "active"
                        or review.get("review_method") != EXPLICIT_REVIEW_METHOD or not review.get("review_id")):
                    continue
                snapshots = [_snapshot(review.get(f"{side}_identity")) for side in ("left", "right")]
                if any(snapshot.get("type") != kind for snapshot in snapshots):
                    continue
                names = {name_key(name, kind) for snapshot in snapshots
                         for name in [snapshot.get("canonical_name", ""), *(snapshot.get("reviewed_names") or [])]} - {"", revoked}
                if names & lineage and not names <= lineage:
                    lineage |= names
                    changed = True
        if (alias_key == revoked and canonical_key in lineage) or (canonical_key == revoked and alias_key in lineage):
            return True
    return False
