"""Read-only classification of identity vectors against authoritative graph state."""
from app.entity_policy import ENTITY_TYPES, name_key

VERIFIER_VERSION = "typed-vector-source-name-v1"
LEGACY_TYPES = {"Document": "DocumentRef", "Financialitem": "FinancialItem",
                "Insurancepolicy": "InsurancePolicy", "Dateevent": "DateEvent"}


def canonical_vector_type(kind):
    return LEGACY_TYPES.get(kind, kind)


def classify_entity_vector(vector: dict, graph: dict | None, *, verified_aliases=()) -> dict:
    """Aliases must already have passed the source/human provenance reader.

    Raw alias strings, similar names and vector descriptions are not authority.
    This result is diagnostic; it never grants permission to mutate either store.
    """
    if graph is None:
        return {"accepted": False, "problems": ["orphan_vector"], "name_agreement": "unavailable",
                "legacy_type_representation": False}
    problems = []
    if not vector.get("entity_uuid") or vector["entity_uuid"] != graph.get("uuid"):
        problems.append("uuid")
    vector_type = canonical_vector_type(vector.get("entity_type"))
    graph_types = {canonical_vector_type(label) for label in graph.get("labels", []) if label in ENTITY_TYPES or label in LEGACY_TYPES}
    if vector_type not in ENTITY_TYPES or graph_types != {vector_type}:
        problems.append("type")
    if type(vector.get("dimension")) is not int or vector["dimension"] != 3072:
        problems.append("dimension")
    stored, canonical = vector.get("entity_name"), graph.get("name")
    if not isinstance(stored, str) or not stored.strip() or not isinstance(canonical, str) or not canonical.strip():
        agreement = "unavailable"
    elif stored == canonical:
        agreement = "verbatim"
    elif name_key(stored, vector_type) == name_key(canonical, vector_type):
        agreement = "orthographic"
    elif any(isinstance(alias, str) and name_key(stored, vector_type) == name_key(alias, vector_type)
             for alias in verified_aliases):
        agreement = "verified_alias"
    else:
        agreement = "different"
    if agreement in {"different", "unavailable"}:
        problems.append("name")
    return {"accepted": not problems, "problems": problems, "name_agreement": agreement,
            "legacy_type_representation": vector.get("entity_type") in LEGACY_TYPES
                or any(label in LEGACY_TYPES for label in graph.get("labels", []))}
