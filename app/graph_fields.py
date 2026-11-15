"""Read-time graph fields: link support state and the owner rule. Stored properties never change."""
import unicodedata

from app.relationship_families import EXPLICIT_FAMILIES
from app.relationship_support import support_records

# The `evidence` family has no keyword prefixes, so its explicit types are all of its types.
EVIDENCE_TYPES = sorted(t for t, family in EXPLICIT_FAMILIES.items() if family == "evidence")
HUB_LINKS = 100


def link_support(props: dict) -> dict:
    records = support_records(props).values()
    inferred = sum(1 for r in records if r["inferred"])
    stated = len(records) - inferred
    if not records:
        state = "inferred" if props.get("inferred") or props.get("implied") else "supported"
    else:
        state = "supported" if not inferred else "inferred" if not stated else "mixed"
    return {"support_state": state, "stated_count": stated, "inferred_count": inferred}


def normalize_name(value) -> str:
    return " ".join(unicodedata.normalize("NFKC", value).casefold().split()) if isinstance(value, str) else ""


def owner_matches(setting, persons) -> tuple[str, str | None, list[dict]]:
    """Return (status, owner uuid, every matching person), ranked by documents, links, then UUID.

    The owner is the first match; the others are possible duplicates of it.
    """
    target = normalize_name(setting)
    if not target:
        return "unset", None, []
    matches = {}
    for person in persons:
        aliases = person.get("aliases")
        names = [person.get("name"), *(aliases if isinstance(aliases, list) else [aliases])]
        if any(normalize_name(name) == target for name in names):
            matches[person["uuid"]] = person
    if not matches:
        return "not_found", None, []
    ranked = sorted(matches.values(), key=lambda p: (-p.get("evidence_doc_count", 0),
                                                     -p.get("link_count", 0), p["uuid"]))
    return ("found" if len(ranked) == 1 else "chosen"), ranked[0]["uuid"], ranked


def owner_rule(setting, persons) -> tuple[str, str | None, int]:
    """Return (status, owner uuid, match count); see owner_matches."""
    status, owner, matches = owner_matches(setting, persons)
    return status, owner, len(matches)
