import logging
import re
from typing import Optional

from rapidfuzz import fuzz

from app.embeddings import embeddings_store
from app.graph import graph_store

logger = logging.getLogger(__name__)

SIMILARITY_THRESHOLD = 85
EMBEDDING_THRESHOLD = 0.88


def normalize_name(name: str) -> str:
    """Normalize a name for comparison."""
    if not name:
        return ""
    # Handle "LAST, FIRST" format
    name = name.strip()
    if "," in name and len(name.split(",")) == 2:
        parts = name.split(",")
        name = f"{parts[1].strip()} {parts[0].strip()}"
    # Remove special chars, extra spaces
    name = re.sub(r"[™®©]", "", name)
    name = re.sub(r"\s+", " ", name).strip()
    return name


class EntityResolver:
    async def resolve_person(self, name: str, source_doc_id: int, role: str = None) -> str:
        """Resolve a person name to an existing or new node. Returns uuid."""
        if not name or not name.strip():
            return ""

        normalized = normalize_name(name)
        if not normalized:
            return ""

        # 1. Exact match in Neo4j
        existing = await graph_store.find_person(normalized)
        if existing:
            # Add alias if the original name differs
            if name != existing["name"] and name not in (existing.get("aliases") or []):
                await graph_store.add_person_alias(existing["uuid"], name)
            return existing["uuid"]

        # 2. Fuzzy match against all persons
        all_persons = await graph_store.get_all_persons()
        best_match = None
        best_score = 0

        for person in all_persons:
            # Check against canonical name
            score = fuzz.ratio(normalized.lower(), (person["name"] or "").lower())
            if score > best_score:
                best_score = score
                best_match = person

            # Check against aliases
            for alias in (person.get("aliases") or []):
                alias_score = fuzz.ratio(normalized.lower(), normalize_name(alias).lower())
                if alias_score > best_score:
                    best_score = alias_score
                    best_match = person

        if best_match and best_score >= SIMILARITY_THRESHOLD:
            logger.info(f"Fuzzy matched '{name}' to '{best_match['name']}' (score={best_score})")
            if name != best_match["name"] and name not in (best_match.get("aliases") or []):
                await graph_store.add_person_alias(best_match["uuid"], name)
            return best_match["uuid"]

        # 3. Embedding similarity for close but not fuzzy-matched names
        if all_persons and best_score >= 60:
            query_emb = await embeddings_store.generate_embedding(normalized)
            if query_emb:
                for person in all_persons:
                    if not person.get("name"):
                        continue
                    person_emb = await embeddings_store.generate_embedding(person["name"])
                    if person_emb:
                        sim = _cosine_similarity(query_emb, person_emb)
                        if sim >= EMBEDDING_THRESHOLD:
                            logger.info(f"Embedding matched '{name}' to '{person['name']}' (sim={sim:.3f})")
                            if name != person["name"]:
                                await graph_store.add_person_alias(person["uuid"], name)
                            return person["uuid"]

        # 4. Create new person
        node_uuid = await graph_store.create_person(
            name=normalized,
            aliases=[name] if name != normalized else [],
            role=role,
        )
        logger.info(f"Created new Person: '{normalized}' (uuid={node_uuid})")
        return node_uuid

    async def resolve_organization(self, name: str, source_doc_id: int,
                                    org_type: str = None) -> str:
        """Resolve an organization name. Returns uuid."""
        if not name or not name.strip():
            return ""

        normalized = normalize_name(name)
        if not normalized:
            return ""

        # Exact match
        existing = await graph_store.find_organization(normalized)
        if existing:
            if name != existing["name"] and name not in (existing.get("aliases") or []):
                await graph_store.add_org_alias(existing["uuid"], name)
            return existing["uuid"]

        # Fuzzy match
        all_orgs = await graph_store.get_all_organizations()
        best_match = None
        best_score = 0

        for org in all_orgs:
            score = fuzz.ratio(normalized.lower(), (org["name"] or "").lower())
            if score > best_score:
                best_score = score
                best_match = org
            for alias in (org.get("aliases") or []):
                alias_score = fuzz.ratio(normalized.lower(), normalize_name(alias).lower())
                if alias_score > best_score:
                    best_score = alias_score
                    best_match = org

        if best_match and best_score >= SIMILARITY_THRESHOLD:
            logger.info(f"Fuzzy matched org '{name}' to '{best_match['name']}' (score={best_score})")
            if name != best_match["name"] and name not in (best_match.get("aliases") or []):
                await graph_store.add_org_alias(best_match["uuid"], name)
            return best_match["uuid"]

        # Create new
        node_uuid = await graph_store.create_organization(
            name=normalized, org_type=org_type,
            aliases=[name] if name != normalized else [],
        )
        logger.info(f"Created new Organization: '{normalized}' (uuid={node_uuid})")
        return node_uuid


def _cosine_similarity(a: list[float], b: list[float]) -> float:
    if not a or not b or len(a) != len(b):
        return 0.0
    dot = sum(x * y for x, y in zip(a, b))
    norm_a = sum(x * x for x in a) ** 0.5
    norm_b = sum(x * x for x in b) ** 0.5
    if norm_a == 0 or norm_b == 0:
        return 0.0
    return dot / (norm_a * norm_b)


entity_resolver = EntityResolver()
