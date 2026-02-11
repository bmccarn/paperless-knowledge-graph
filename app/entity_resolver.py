import logging
import re
from typing import Optional

from rapidfuzz import fuzz

from app.embeddings import embeddings_store
from app.graph import graph_store

logger = logging.getLogger(__name__)

SIMILARITY_THRESHOLD = 85  # Auto-merge threshold (raised from 75)
LLM_TIEBREAKER_LOW = 70   # Below this: definitely different entities
LLM_TIEBREAKER_HIGH = 85  # 70-85 zone: would need LLM confirmation (skip for now)
SHORT_NAME_THRESHOLD = 95  # Names ≤5 chars need this score or higher
EMBEDDING_THRESHOLD = 0.88
NAME_PARTS_THRESHOLD = 0.70

# Common business suffixes to strip before comparing org names
COMMON_ORG_SUFFIXES = {
    "inc", "llc", "ltd", "corp", "corporation", "co", "company", "group",
    "holdings", "enterprises", "partners", "lp", "llp", "plc", "sa", "ag",
    "gmbh", "pllc", "pa", "pc", "na", "fsb", "services", "service",
    "mortgage", "insurance", "financial", "bank", "banking", "lending",
    "solutions", "associates", "association", "foundation", "trust",
    "management", "consulting", "advisors", "advisory", "capital",
    "properties", "realty", "real", "estate", "of", "the", "and", "a",
}

# Common name abbreviations
ABBREVIATIONS = {
    "wm": "william", "chas": "charles", "geo": "george", "jas": "james",
    "jno": "john", "thos": "thomas", "robt": "robert", "benj": "benjamin",
    "danl": "daniel", "edw": "edward", "fredk": "frederick", "saml": "samuel",
}


def normalize_name(name: str) -> str:
    """Normalize a name for comparison."""
    if not name:
        return ""
    name = name.strip()
    # Handle "LAST, FIRST" format
    if "," in name and len(name.split(",")) == 2:
        parts = name.split(",")
        name = f"{parts[1].strip()} {parts[0].strip()}"
    # Remove special chars, extra spaces
    name = re.sub(r"[™®©.\-']", " ", name)
    name = re.sub(r"\s+", " ", name).strip()
    return name


def get_name_parts(name: str) -> list[str]:
    """Get significant name parts (lowercased, no initials)."""
    normalized = normalize_name(name).lower()
    parts = normalized.split()
    return [p for p in parts if len(p) > 1]


def get_distinctive_org_words(name: str) -> set[str]:
    """Get distinctive words from an org name (strip common business suffixes)."""
    normalized = normalize_name(name).lower()
    parts = normalized.split()
    distinctive = {p for p in parts if p not in COMMON_ORG_SUFFIXES and len(p) > 1}
    return distinctive


def is_short_name(name: str) -> bool:
    """Check if a name is too short to be reliably fuzzy-matched."""
    normalized = normalize_name(name).strip()
    return len(normalized) <= 5


def expand_abbreviation(part: str) -> str:
    """Expand a single-letter initial or abbreviation."""
    p = part.lower().rstrip(".")
    return ABBREVIATIONS.get(p, p)


def is_initial_of(short: str, long: str) -> bool:
    """Check if 'short' is an initial/abbreviation of 'long'."""
    s = short.lower().rstrip(".")
    l = long.lower()
    if len(s) == 1:
        return l.startswith(s)
    return s == l or ABBREVIATIONS.get(s, s) == l


def name_parts_match_score(name_a: str, name_b: str) -> float:
    """Score how well two names match based on name parts.
    
    Handles: case differences, initials, abbreviations, name ordering.
    Returns 0.0-1.0.
    """
    parts_a = normalize_name(name_a).lower().split()
    parts_b = normalize_name(name_b).lower().split()
    
    if not parts_a or not parts_b:
        return 0.0
    
    shorter, longer = (parts_a, parts_b) if len(parts_a) <= len(parts_b) else (parts_b, parts_a)
    
    matched = 0
    used = set()
    
    for sp in shorter:
        for i, lp in enumerate(longer):
            if i in used:
                continue
            if sp == lp:
                matched += 1
                used.add(i)
                break
            if is_initial_of(sp, lp) or is_initial_of(lp, sp):
                matched += 0.8
                used.add(i)
                break
            if len(sp) > 2 and len(lp) > 2:
                ratio = fuzz.ratio(sp, lp)
                if ratio >= 80:
                    matched += ratio / 100.0
                    used.add(i)
                    break
    
    coverage_short = matched / len(shorter) if shorter else 0
    coverage_long = matched / len(longer) if longer else 0
    
    return 0.7 * coverage_short + 0.3 * coverage_long


def detect_joint_name(name: str) -> list[str]:
    """Detect if a name string contains multiple people (joint names)."""
    parts = normalize_name(name).split()
    if len(parts) <= 3:
        return [name]
    
    part_counts = {}
    for p in parts:
        pl = p.lower()
        part_counts[pl] = part_counts.get(pl, 0) + 1
    
    repeated = [p for p, c in part_counts.items() if c >= 2]
    if repeated:
        surname = repeated[0]
        remaining = []
        current_group = []
        for p in parts:
            if p.lower() == surname and current_group:
                remaining.append(current_group)
                current_group = []
            else:
                current_group.append(p)
        if current_group:
            remaining.append(current_group)
        
        if len(remaining) >= 2:
            names = []
            for group in remaining:
                given = " ".join(group)
                names.append(f"{given} {surname.title()}")
            return names
    
    if len(parts) > 5:
        return [name]
    
    return [name]


def org_distinctive_match(name_a: str, name_b: str) -> bool:
    """Check if two org names share distinctive words (not just common suffixes).
    
    Returns True if there's meaningful overlap in distinctive words.
    """
    words_a = get_distinctive_org_words(name_a)
    words_b = get_distinctive_org_words(name_b)
    
    if not words_a or not words_b:
        return False
    
    # Check for overlap in distinctive words
    overlap = words_a & words_b
    # Require at least one distinctive word in common
    return len(overlap) > 0


def should_auto_merge(name_a: str, name_b: str, score: float,
                      entity_type: str = "Person") -> bool:
    """Determine if two entities should be auto-merged based on safeguards.
    
    Returns True only if all safeguards pass.
    """
    # Short name protection
    if is_short_name(name_a) or is_short_name(name_b):
        if score < (SHORT_NAME_THRESHOLD / 100.0):
            logger.debug(
                f"Short name protection blocked merge: '{name_a}' <-> '{name_b}' "
                f"(score={score:.3f}, need {SHORT_NAME_THRESHOLD}%)"
            )
            return False
    
    # Organization specificity check
    if entity_type == "Organization":
        if not org_distinctive_match(name_a, name_b):
            logger.debug(
                f"Org specificity blocked merge: '{name_a}' <-> '{name_b}' "
                f"(no distinctive word overlap)"
            )
            return False
    
    # Must meet threshold
    if score < (SIMILARITY_THRESHOLD / 100.0):
        return False
    
    return True


def advanced_match_score(name_a: str, name_b: str) -> float:
    """Combined matching score using multiple strategies."""
    norm_a = normalize_name(name_a).lower()
    norm_b = normalize_name(name_b).lower()
    fuzzy_score = fuzz.ratio(norm_a, norm_b) / 100.0
    token_sort = fuzz.token_sort_ratio(norm_a, norm_b) / 100.0
    parts_score = name_parts_match_score(name_a, name_b)
    token_set = fuzz.token_set_ratio(norm_a, norm_b) / 100.0
    
    return max(
        fuzzy_score,
        token_sort,
        parts_score,
        token_set * 0.95,
    )


class EntityResolver:
    async def resolve_person(self, name: str, source_doc_id: int, role: str = None) -> str:
        """Resolve a person name to an existing or new node. Returns uuid."""
        if not name or not name.strip():
            return ""

        individual_names = detect_joint_name(name)
        if len(individual_names) > 1:
            logger.info(f"Detected joint name: '{name}' -> {individual_names}")
            uuids = []
            for ind_name in individual_names:
                uid = await self._resolve_single_person(ind_name.strip(), source_doc_id, role)
                if uid:
                    uuids.append(uid)
            return uuids[0] if uuids else ""

        return await self._resolve_single_person(name, source_doc_id, role)

    async def _resolve_single_person(self, name: str, source_doc_id: int, role: str = None) -> str:
        normalized = normalize_name(name)
        if not normalized:
            return ""

        # 1. Exact match
        existing = await graph_store.find_person(normalized)
        if existing:
            if name != existing["name"] and name not in (existing.get("aliases") or []):
                await graph_store.add_person_alias(existing["uuid"], name)
            return existing["uuid"]

        # 2. Advanced matching against all persons (same-type only: Person↔Person)
        all_persons = await graph_store.get_all_persons()
        best_match = None
        best_score = 0.0

        for person in all_persons:
            score = advanced_match_score(normalized, person["name"] or "")
            if score > best_score:
                best_score = score
                best_match = person
            for alias in (person.get("aliases") or []):
                alias_score = advanced_match_score(normalized, alias)
                if alias_score > best_score:
                    best_score = alias_score
                    best_match = person

        if best_match and should_auto_merge(normalized, best_match["name"], best_score, "Person"):
            logger.info(f"Matched '{name}' to '{best_match['name']}' (score={best_score:.3f})")
            if name != best_match["name"] and name not in (best_match.get("aliases") or []):
                await graph_store.add_person_alias(best_match["uuid"], name)
            return best_match["uuid"]

        # 3. Embedding similarity
        if all_persons and best_score >= 0.5:
            query_emb = await embeddings_store.generate_embedding(normalized)
            if query_emb:
                for person in all_persons:
                    if not person.get("name"):
                        continue
                    person_emb = await embeddings_store.generate_embedding(person["name"])
                    if person_emb:
                        sim = _cosine_similarity(query_emb, person_emb)
                        if sim >= EMBEDDING_THRESHOLD:
                            # Still apply safeguards even for embedding matches
                            if should_auto_merge(normalized, person["name"], sim, "Person"):
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

        # Advanced fuzzy match (same-type only: Organization↔Organization)
        all_orgs = await graph_store.get_all_organizations()
        best_match = None
        best_score = 0.0

        for org in all_orgs:
            score = advanced_match_score(normalized, org["name"] or "")
            if score > best_score:
                best_score = score
                best_match = org
            for alias in (org.get("aliases") or []):
                alias_score = advanced_match_score(normalized, alias)
                if alias_score > best_score:
                    best_score = alias_score
                    best_match = org

        if best_match and should_auto_merge(normalized, best_match["name"], best_score, "Organization"):
            logger.info(f"Fuzzy matched org '{name}' to '{best_match['name']}' (score={best_score:.3f})")
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

    async def resolve_all_entities(self) -> dict:
        """Scan all entities in Neo4j and merge duplicates. Returns a report."""
        report = {"merged_persons": [], "merged_orgs": [], "skipped": [], "errors": []}

        # Resolve persons (same-type only: Person↔Person)
        all_persons = await graph_store.get_all_persons()
        merged_uuids = set()

        for i, person_a in enumerate(all_persons):
            if person_a["uuid"] in merged_uuids:
                continue
            for person_b in all_persons[i + 1:]:
                if person_b["uuid"] in merged_uuids:
                    continue
                score = advanced_match_score(
                    person_a["name"] or "", person_b["name"] or ""
                )
                for alias in (person_a.get("aliases") or []):
                    s = advanced_match_score(alias, person_b["name"] or "")
                    score = max(score, s)
                for alias in (person_b.get("aliases") or []):
                    s = advanced_match_score(person_a["name"] or "", alias)
                    score = max(score, s)

                if not should_auto_merge(person_a["name"] or "", person_b["name"] or "", score, "Person"):
                    if score >= (LLM_TIEBREAKER_LOW / 100.0):
                        report["skipped"].append({
                            "a": person_a["name"],
                            "b": person_b["name"],
                            "score": round(score, 3),
                            "reason": "in tiebreaker zone or blocked by safeguard",
                        })
                    continue

                try:
                    await self._merge_nodes(
                        keep_uuid=person_a["uuid"],
                        remove_uuid=person_b["uuid"],
                        remove_name=person_b["name"],
                        remove_aliases=person_b.get("aliases") or [],
                        label="Person",
                    )
                    merged_uuids.add(person_b["uuid"])
                    report["merged_persons"].append({
                        "kept": person_a["name"],
                        "merged": person_b["name"],
                        "score": round(score, 3),
                    })
                    logger.info(
                        f"Merged Person '{person_b['name']}' into '{person_a['name']}' (score={score:.3f})"
                    )
                except Exception as e:
                    report["errors"].append(f"Failed to merge {person_b['name']} into {person_a['name']}: {e}")

        # NOTE: Removed cross-type merging (Organization→Person).
        # Entities should only merge within the same type.

        # Merge duplicate orgs (same-type only: Organization↔Organization)
        all_orgs = await graph_store.get_all_organizations()

        for i, org_a in enumerate(all_orgs):
            if org_a["uuid"] in merged_uuids:
                continue
            for org_b in all_orgs[i + 1:]:
                if org_b["uuid"] in merged_uuids:
                    continue
                score = advanced_match_score(org_a["name"] or "", org_b["name"] or "")

                if not should_auto_merge(org_a["name"] or "", org_b["name"] or "", score, "Organization"):
                    if score >= (LLM_TIEBREAKER_LOW / 100.0):
                        report["skipped"].append({
                            "a": org_a["name"],
                            "b": org_b["name"],
                            "score": round(score, 3),
                            "reason": "in tiebreaker zone or blocked by safeguard",
                        })
                    continue

                try:
                    await self._merge_nodes(
                        keep_uuid=org_a["uuid"],
                        remove_uuid=org_b["uuid"],
                        remove_name=org_b["name"],
                        remove_aliases=org_b.get("aliases") or [],
                        label="Organization",
                    )
                    merged_uuids.add(org_b["uuid"])
                    report["merged_orgs"].append({
                        "kept": org_a["name"],
                        "merged": org_b["name"],
                        "score": round(score, 3),
                    })
                except Exception as e:
                    report["errors"].append(f"Failed to merge org: {e}")

        report["total_merged"] = len(report["merged_persons"]) + len(report["merged_orgs"])
        report["total_skipped"] = len(report["skipped"])
        return report

    async def _merge_nodes(self, keep_uuid: str, remove_uuid: str,
                           remove_name: str, remove_aliases: list[str],
                           label: str):
        """Merge remove_uuid node into keep_uuid node in Neo4j."""
        async with graph_store.driver.session() as session:
            await session.run(
                """
                MATCH (remove) WHERE remove.uuid = $remove_uuid
                MATCH (keep) WHERE keep.uuid = $keep_uuid
                OPTIONAL MATCH (remove)-[r_out]->(target)
                WHERE target <> keep
                WITH keep, remove, collect({type: type(r_out), target: target, props: properties(r_out)}) AS out_rels
                UNWIND out_rels AS rel
                WITH keep, remove, rel
                WHERE rel.target IS NOT NULL
                CALL apoc.create.relationship(keep, rel.type, rel.props, rel.target) YIELD rel AS newRel
                RETURN count(newRel)
                """,
                keep_uuid=keep_uuid, remove_uuid=remove_uuid,
            )
            await session.run(
                """
                MATCH (remove) WHERE remove.uuid = $remove_uuid
                MATCH (keep) WHERE keep.uuid = $keep_uuid
                OPTIONAL MATCH (source)-[r_in]->(remove)
                WHERE source <> keep
                WITH keep, remove, collect({type: type(r_in), source: source, props: properties(r_in)}) AS in_rels
                UNWIND in_rels AS rel
                WITH keep, remove, rel
                WHERE rel.source IS NOT NULL
                CALL apoc.create.relationship(rel.source, rel.type, rel.props, keep) YIELD rel AS newRel
                RETURN count(newRel)
                """,
                keep_uuid=keep_uuid, remove_uuid=remove_uuid,
            )

            all_aliases = [remove_name] + remove_aliases
            for alias in all_aliases:
                if alias:
                    await session.run(
                        """
                        MATCH (n) WHERE n.uuid = $uuid
                        SET n.aliases = CASE
                            WHEN NOT $alias IN n.aliases THEN n.aliases + $alias
                            ELSE n.aliases
                        END
                        """,
                        uuid=keep_uuid, alias=alias,
                    )

            await session.run(
                "MATCH (n) WHERE n.uuid = $uuid DETACH DELETE n",
                uuid=remove_uuid,
            )


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
