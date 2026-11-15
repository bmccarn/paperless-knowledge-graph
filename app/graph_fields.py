"""Read-time graph fields: link support state and the owner rule. Stored properties never change."""
import unicodedata

from app.relationship_support import support_records

# The `evidence` family is explicit-only (docs/design/categories.md), so these are all its types.
EVIDENCE_TYPES = ["MENTIONS", "CONTAINS_RESULT"]
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


def owner_rule(setting, persons) -> tuple[str, str | None]:
    """Return (owner_status, owner uuid): the single Person whose name or alias equals the setting."""
    target = normalize_name(setting)
    if not target:
        return "unset", None
    matches = set()
    for person in persons:
        aliases = person.get("aliases")
        names = [person.get("name"), *(aliases if isinstance(aliases, list) else [aliases])]
        if any(normalize_name(name) == target for name in names):
            matches.add(person.get("uuid"))
    if len(matches) == 1:
        return "found", matches.pop()
    return ("ambiguous" if matches else "not_found"), None
