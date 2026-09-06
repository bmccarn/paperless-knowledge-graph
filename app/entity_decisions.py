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
    return {"uuid": props.get("uuid"), "type": kind, "canonical_name": props.get("name") or "",
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


def merge_is_prohibited(left: dict, right: dict, decisions: list[dict]) -> bool:
    """Evaluate both orientations of each durable human veto."""
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
