import logging
import re
from typing import Optional

from rapidfuzz import fuzz

from app.embeddings import embeddings_store
from app.graph import graph_store

logger = logging.getLogger(__name__)

# Protected person names that should be allowed to merge single<->multi word.
# These are unique names (pets, nicknames) where "Ggarbo" == "Ggarbo McCarn".
PROTECTED_PERSON_NAMES = {
    "ggarbo", "ggarbo mccarn", "ggarbo mccam", "ggarbo mccarm",
}

SIMILARITY_THRESHOLD = 85  # Auto-merge threshold
LLM_MERGE_LOW = 70        # Below this: definitely different entities
LLM_MERGE_HIGH = 85       # 70-85 zone: use LLM to decide
_merge_llm_cache: dict[str, bool] = {}
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


# Title prefixes to strip from person names
TITLE_PREFIXES = {
    "dr", "dr.", "mr", "mr.", "mrs", "mrs.", "ms", "ms.", "prof", "prof.",
    "rev", "rev.", "hon", "hon.", "sgt", "cpl", "sra", "ssgt", "tsgt",
    "msgt", "smsgt", "cmsgt", "od", "md", "dds", "dvm", "d.v.m.",
    "esq", "esq.", "jr", "jr.", "sr", "sr.", "ii", "iii", "iv",
}

# Business suffixes that shouldn't appear at the START of a name
BUSINESS_PREFIX_SUFFIXES = {"llc", "inc", "corp", "ltd", "co", "the"}


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
    name = re.sub(r"[\u2122\u00ae\u00a9.\-\']", " ", name)
    name = re.sub(r"\s+", " ", name).strip()
    return name


def normalize_person_name(name: str) -> str:
    """Normalize a person name: strip titles, fix casing."""
    name = normalize_name(name)
    # Strip title prefixes
    parts = name.split()
    while parts and parts[0].lower().rstrip(".") in {t.rstrip(".") for t in TITLE_PREFIXES}:
        parts.pop(0)
    # Strip "AND" prefix from joint name splitting
    if parts and parts[0].upper() == "AND":
        parts.pop(0)
    return " ".join(parts) if parts else name


def normalize_org_name(name: str) -> str:
    """Normalize an org name: move misplaced suffixes from prefix to end."""
    name = normalize_name(name)
    parts = name.split()
    if not parts:
        return name
    # If first word is a business suffix (LLC, Inc), move it to end
    if parts[0].lower().rstrip(",") in BUSINESS_PREFIX_SUFFIXES and len(parts) > 1:
        suffix = parts.pop(0).rstrip(",")
        parts.append(suffix)
    return " ".join(parts)


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


def _looks_like_person_name(text: str) -> bool:
    """Check if a string looks like it could be a person name (not a phrase/action)."""
    words = text.strip().split()
    if not words:
        return False
    # Person names are typically 1-4 capitalized words, no verbs/gerunds
    if len(words) > 5:
        return False
    # If any word is lowercase (not an initial), it's probably a phrase
    for w in words:
        if len(w) > 2 and w[0].islower():
            return False
        # Gerunds and common verbs are not names
        if w.lower().endswith(("ing", "tion", "ment", "ness", "ity")):
            return False
    return True


def detect_joint_name(name: str) -> list[str]:
    """Detect if a name string contains multiple people (joint names).
    
    Handles patterns like:
    - "Blake & Chelsea McCarn"
    - "Blake T & Chelsea J McCarn"
    - "BLAKE T MCCARN CHELSEA J MCCARN" (two full names concatenated)
    - "MCCARN BLAKE THOMAS & MCCARN CHELSEA JOYCE"
    """
    parts = normalize_name(name).split()
    if len(parts) <= 3:
        return [name]
    
    joined = " ".join(parts)
    
    # Pattern: "First [M] & First [M] Last" or "First [M] Last & First [M] Last"
    # Match on & or "and"
    and_patterns = [
        r'^(.+?)\s+(?:&|and)\s+(.+)$',
    ]
    for pat in and_patterns:
        m = re.match(pat, joined, re.IGNORECASE)
        if m:
            left = m.group(1).strip()
            right = m.group(2).strip()
            # If left has no last name, borrow from right
            left_parts = left.split()
            right_parts = right.split()
            if len(left_parts) <= 2 and len(right_parts) >= 2:
                # Assume last word of right is shared surname
                surname = right_parts[-1]
                if not any(w.lower() == surname.lower() for w in left_parts):
                    left = left + " " + surname
            return [left, right]
    
    # Pattern: Two full names concatenated without separator
    # e.g., "BLAKE T MCCARN CHELSEA J MCCARN" — 6 parts, two 3-part names
    # Heuristic: if last name appears twice, split there
    if len(parts) >= 5:
        # Check if any word appears twice (likely a shared surname)
        lower_parts = [p.lower() for p in parts]
        for i, word in enumerate(lower_parts):
            if len(word) < 3:
                continue
            # Find second occurrence
            try:
                j = lower_parts.index(word, i + 1)
            except ValueError:
                continue
            # Split: parts[0:j] and parts[j:]
            # But only if both halves look like names (2-4 parts each)
            left = parts[0:j+1] if j < len(parts) - 1 else parts[0:j]
            right = parts[j+1:] if j < len(parts) - 1 else parts[j:]
            # Actually split at the boundary: first name ends at first occurrence of surname
            left = parts[0:i+1]
            right = parts[i+1:]
            if 2 <= len(left) <= 4 and 2 <= len(right) <= 4:
                logger.info(f"Detected concatenated names: '{name}' -> {[' '.join(left), ' '.join(right)]}")
                return [" ".join(left), " ".join(right)]
    
    # Pattern: "LAST FIRST MIDDLE & LAST FIRST MIDDLE" (inverted with &)
    # Already handled by & pattern above
    
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


def _first_names_compatible(name_a: str, name_b: str) -> bool:
    """Check if two person names have compatible first names.
    
    Blocks merges where first names are clearly different people
    (e.g., "Blake McCarn" vs "Chelsea McCarn").
    """
    parts_a = get_name_parts(name_a)
    parts_b = get_name_parts(name_b)
    
    if not parts_a or not parts_b:
        return True  # Can't determine, allow
    
    first_a = parts_a[0].lower().rstrip(".")
    first_b = parts_b[0].lower().rstrip(".")
    
    # Exact match
    if first_a == first_b:
        return True
    
    # Initial match (e.g., "B" matches "Blake")
    if len(first_a) == 1 and first_b.startswith(first_a):
        return True
    if len(first_b) == 1 and first_a.startswith(first_b):
        return True
    
    # Abbreviation match
    expanded_a = ABBREVIATIONS.get(first_a, first_a)
    expanded_b = ABBREVIATIONS.get(first_b, first_b)
    if expanded_a == expanded_b:
        return True
    
    # OCR typo tolerance: very similar first names (e.g., "Mccar" vs "Mccarn" in last name,
    # or "Blake" vs "Blak" in first name)
    if len(first_a) > 2 and len(first_b) > 2:
        ratio = fuzz.ratio(first_a, first_b)
        if ratio >= 80:
            return True
    
    return False


def should_auto_merge(name_a: str, name_b: str, score: float,
                      entity_type: str = "Person") -> bool:
    """Determine if two entities should be auto-merged based on safeguards.
    
    Returns True only if all safeguards pass.
    """
    # Minimum name length protection
    if len(normalize_name(name_a).strip()) < 4 or len(normalize_name(name_b).strip()) < 4:
        logger.debug(
            f"Min length protection blocked merge: '{name_a}' <-> '{name_b}' (one name too short)"
        )
        return False

    # Person-specific safeguards
    if entity_type == "Person":
        parts_a = get_name_parts(name_a)
        parts_b = get_name_parts(name_b)
        
        # Word-count mismatch protection: single-word names are too ambiguous
        # to merge with multi-word names. "Matthew" != "Matthew Smith".
        # Exception: protected names (e.g., pet names like "Ggarbo" -> "Ggarbo McCarn")
        if (len(parts_a) == 1) != (len(parts_b) == 1):
            # Exception: protected names (pets, etc.) can merge single<->multi
            single = name_a.lower() if len(parts_a) == 1 else name_b.lower()
            if single in PROTECTED_PERSON_NAMES:
                logger.info(
                    f"Single-word merge ALLOWED (protected name): '{name_a}' <-> '{name_b}'"
                )
                # Allow merge for protected entities
            else:
                # One is single-word, the other is not — block merge
                logger.info(
                    f"Single vs multi-word blocked merge: '{name_a}' ({len(parts_a)} parts) <-> '{name_b}' ({len(parts_b)} parts)"
                )
                return False
        
        # CRITICAL: First name must be compatible
        # Prevents "Blake McCarn" from merging with "Chelsea McCarn"
        if len(parts_a) >= 2 and len(parts_b) >= 2:
            if not _first_names_compatible(name_a, name_b):
                logger.info(
                    f"First name mismatch blocked merge: '{name_a}' <-> '{name_b}' "
                    f"(different first names)"
                )
                return False

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


def advanced_match_score(name_a: str, name_b: str, entity_type: str = "Person") -> float:
    """Combined matching score using multiple strategies.
    
    For Person entities, does NOT use token_set_ratio (it gives false positives
    when names share a surname, e.g. "Blake McCarn" vs "Chelsea McCarn").
    """
    norm_a = normalize_name(name_a).lower()
    norm_b = normalize_name(name_b).lower()
    fuzzy_score = fuzz.ratio(norm_a, norm_b) / 100.0
    token_sort = fuzz.token_sort_ratio(norm_a, norm_b) / 100.0
    parts_score = name_parts_match_score(name_a, name_b)
    
    if entity_type == "Person":
        # For persons, only use fuzzy_score, token_sort, and parts_score.
        # token_set_ratio is too dangerous — it matches any name sharing a surname.
        return max(fuzzy_score, token_sort, parts_score)
    else:
        # For orgs and other types, token_set is useful (e.g. "RapidRoute Solutions LLC" vs "LLC RapidRoute Solutions")
        token_set = fuzz.token_set_ratio(norm_a, norm_b) / 100.0
        return max(fuzzy_score, token_sort, parts_score, token_set * 0.95)




async def _llm_should_merge(name_a: str, name_b: str, entity_type: str) -> bool:
    """Ask LLM whether two entity names refer to the same real-world entity.
    
    Used in the gray zone (score 70-85) where fuzzy matching is uncertain.
    Leverages LLM general knowledge for OCR typos, abbreviations, and aliases.
    """
    cache_key = f"{entity_type}:{name_a.lower()}:{name_b.lower()}"
    rev_key = f"{entity_type}:{name_b.lower()}:{name_a.lower()}"
    if cache_key in _merge_llm_cache:
        return _merge_llm_cache[cache_key]
    if rev_key in _merge_llm_cache:
        return _merge_llm_cache[rev_key]
    
    try:
        from app.config import settings
        from app.retry import retry_with_backoff
        from openai import AsyncOpenAI
        import json as _json
        
        client = AsyncOpenAI(base_url=settings.litellm_url, api_key=settings.litellm_api_key)
        
        prompt = f"""Do these two names refer to the SAME real-world {entity_type.lower()}?

Name A: "{name_a}"
Name B: "{name_b}"

Consider: OCR errors, typos, abbreviations, middle names/initials, nicknames, 
alternate spellings, and your general knowledge.

Examples of SAME entity:
- "Blake T McCarn" and "Blake Thomas Mccarn" → same (middle initial = middle name)
- "USAA Federal Savings Bank" and "USAA" → same (abbreviation)
- "Blake McCarr" and "Blake McCarn" → same (OCR typo)

Examples of DIFFERENT entities:
- "Blake McCarn" and "Chelsea McCarn" → different (different first names, same family)
- "Matthew Smith" and "Matthew Johnson" → different (different last names)
- "RapidRoute Solutions" and "Rapid Transit Authority" → different (different companies)

Respond with ONLY: {{"same_entity": true}} or {{"same_entity": false}}"""

        async def _call():
            response = await client.chat.completions.create(
                model=settings.gemini_model,
                messages=[{{"role": "user", "content": prompt}}],
                response_format={{"type": "json_object"}},
            )
            raw = response.choices[0].message.content or ""
            try:
                return _json.loads(raw)
            except _json.JSONDecodeError:
                return {{"same_entity": False}}
        
        result = await retry_with_backoff(_call, operation="llm_merge_tiebreaker")
        is_same = result.get("same_entity", False)
        _merge_llm_cache[cache_key] = is_same
        
        if is_same:
            logger.info(f"LLM confirmed merge: '{name_a}' <-> '{name_b}' ({entity_type})")
        else:
            logger.info(f"LLM blocked merge: '{name_a}' <-> '{name_b}' ({entity_type})")
        
        return is_same
    
    except Exception as e:
        logger.warning(f"LLM merge tiebreaker failed for '{name_a}' <-> '{name_b}': {e}")
        return False  # When in doubt, don't merge


class EntityResolver:
    def __init__(self):
        self._cache = {}
    async def resolve_person(self, name: str, source_doc_id: int, role: str = None, description: str = None) -> str:
        """Resolve a person name to an existing or new node. Returns uuid."""
        if not name or not name.strip():
            return ""

        name = normalize_person_name(name)
        if not name or len(name) < 3:
            return ""

        individual_names = detect_joint_name(name)
        if len(individual_names) > 1:
            logger.info(f"Detected joint name: '{name}' -> {individual_names}")
            uuids = []
            for ind_name in individual_names:
                uid = await self._resolve_single_person(ind_name.strip(), source_doc_id, role, description)
                if uid:
                    uuids.append(uid)
            return uuids[0] if uuids else ""

        return await self._resolve_single_person(name, source_doc_id, role, description)

    async def _resolve_single_person(self, name: str, source_doc_id: int, role: str = None, description: str = None) -> str:
        normalized = normalize_name(name)
        if not normalized:
            return ""
        
        # Detect organizations misclassified as Person
        # If name contains business suffixes, redirect to org resolver
        name_lower = normalized.lower()
        org_indicators = ["llc", "inc", "corp", "ltd", "company", "co.", "solutions",
                         "services", "association", "foundation", "bank", "mortgage",
                         "insurance", "financial", "trust", "group", "partners"]
        if any(f" {ind}" in f" {name_lower} " or name_lower.endswith(f" {ind}") or name_lower.startswith(f"{ind} ") for ind in org_indicators):
            logger.info(f"Redirecting org-as-person to org resolver: '{name}'")
            return await self.resolve_organization(name, source_doc_id, description=description)

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
            score = advanced_match_score(normalized, person["name"] or "", entity_type="Person")
            if score > best_score:
                best_score = score
                best_match = person
            for alias in (person.get("aliases") or []):
                alias_score = advanced_match_score(normalized, alias, entity_type="Person")
                if alias_score > best_score:
                    best_score = alias_score
                    best_match = person

        if best_match and best_score >= (LLM_MERGE_LOW / 100.0):
            if should_auto_merge(normalized, best_match["name"], best_score, "Person"):
                logger.info(f"Matched '{name}' to '{best_match['name']}' (score={best_score:.3f})")
                if name != best_match["name"] and name not in (best_match.get("aliases") or []):
                    await graph_store.add_person_alias(best_match["uuid"], name)
                return best_match["uuid"]
            elif best_score >= (LLM_MERGE_LOW / 100.0) and best_score < (LLM_MERGE_HIGH / 100.0):
                # Gray zone — ask LLM
                if await _llm_should_merge(normalized, best_match["name"], "Person"):
                    logger.info(f"LLM-confirmed match '{name}' to '{best_match['name']}' (score={best_score:.3f})")
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
            description=description,
        )
        logger.info(f"Created new Person: '{normalized}' (uuid={node_uuid})")
        return node_uuid

    async def resolve_organization(self, name: str, source_doc_id: int,
                                    org_type: str = None, description: str = None) -> str:
        """Resolve an organization name. Returns uuid."""
        name = normalize_org_name(name)
        if not name or len(name) < 3:
            return ""

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
            score = advanced_match_score(normalized, org["name"] or "", entity_type="Organization")
            if score > best_score:
                best_score = score
                best_match = org
            for alias in (org.get("aliases") or []):
                alias_score = advanced_match_score(normalized, alias, entity_type="Organization")
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
            description=description,
        )
        logger.info(f"Created new Organization: '{normalized}' (uuid={node_uuid})")
        return node_uuid

    # Map entity types to Neo4j labels (avoids collision with Paperless Document nodes)
    ENTITY_TYPE_TO_LABEL = {
        "Document": "DocumentRef",
    }

    def _neo4j_label(self, entity_type: str) -> str:
        """Get Neo4j label for an entity type."""
        return self.ENTITY_TYPE_TO_LABEL.get(entity_type, entity_type)

    async def resolve_generic(self, name: str, entity_type: str, source_doc_id: int,
                              description: str = None) -> str:
        """Resolve a generic entity (Location, System, Product, etc.) — fuzzy match or create."""
        if not name or not name.strip():
            return ""

        name = name.strip()
        label = self._neo4j_label(entity_type)

        # Check cache
        cache_key = f"{entity_type}:{name.lower()}"
        if cache_key in self._cache:
            return self._cache[cache_key]

        # Try exact match in Neo4j
        async with graph_store.driver.session() as session:
            result = await session.run(
                f"MATCH (n:{label}) WHERE toLower(n.name) = toLower($name) RETURN n.uuid AS uuid LIMIT 1",
                name=name,
            )
            record = await result.single()
            if record:
                uuid = record["uuid"]
                self._cache[cache_key] = uuid
                # Update description if we have a new one
                if description:
                    await session.run(
                        f"MATCH (n:{label} {{uuid: $uuid}}) SET n.description = $desc, n.entity_type = $etype",
                        uuid=uuid, desc=description, etype=entity_type,
                    )
                return uuid

        # Create new entity
        import uuid as uuid_mod
        new_uuid = str(uuid_mod.uuid4())
        props = {
            "uuid": new_uuid,
            "name": name,
            "source_doc_ids": [source_doc_id],
            "entity_type": entity_type,
        }
        if description:
            props["description"] = description
        await graph_store.create_node(label, props)
        logger.info(f"Created new {entity_type} ({label}): '{name}' (uuid={new_uuid})")
        self._cache[cache_key] = new_uuid
        return new_uuid

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
                    # Pick canonical name
                    canonical = pick_canonical_name(person_a["name"] or "", person_b["name"] or "")
                    await self._merge_nodes(
                        keep_uuid=person_a["uuid"],
                        remove_uuid=person_b["uuid"],
                        remove_name=person_b["name"],
                        remove_aliases=person_b.get("aliases") or [],
                        label="Person",
                        canonical_name=canonical,
                    )
                    merged_uuids.add(person_b["uuid"])
                    report["merged_persons"].append({
                        "kept": canonical,
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
                    canonical = pick_canonical_name(org_a["name"] or "", org_b["name"] or "")
                    await self._merge_nodes(
                        keep_uuid=org_a["uuid"],
                        remove_uuid=org_b["uuid"],
                        remove_name=org_b["name"],
                        remove_aliases=org_b.get("aliases") or [],
                        label="Organization",
                        canonical_name=canonical,
                    )
                    merged_uuids.add(org_b["uuid"])
                    report["merged_orgs"].append({
                        "kept": canonical,
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
                           label: str, canonical_name: str = None):
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

            # Update canonical name if provided
            if canonical_name:
                await session.run(
                    "MATCH (n) WHERE n.uuid = $uuid SET n.name = $name",
                    uuid=keep_uuid, name=canonical_name,
                )

            await session.run(
                "MATCH (n) WHERE n.uuid = $uuid DETACH DELETE n",
                uuid=remove_uuid,
            )


def pick_canonical_name(name_a: str, name_b: str) -> str:
    """Pick the best canonical name: prefer longer, properly cased, most complete.
    
    "John A Doe" > "JOHN DOE" > "John" (longer, properly cased = better)
    """
    def score_name(name: str) -> tuple:
        n = name.strip()
        parts = n.split()
        # Score components:
        # 1. Number of name parts (more = better)
        num_parts = len(parts)
        # 2. Length (longer = better)
        length = len(n)
        # 3. Case quality: mixed case > all upper > all lower
        if n == n.upper():
            case_score = 1  # ALL CAPS
        elif n == n.lower():
            case_score = 0  # all lower
        else:
            case_score = 2  # Mixed/Title case (best)
        # 4. Has middle initial/name
        has_middle = 1 if num_parts >= 3 else 0
        # 5. Prefer names without numbers/special characters
        import re as _re
        has_clean_chars = 0 if _re.search(r'[0-9#@&%]', n) else 1
        return (num_parts, case_score, has_clean_chars, has_middle, length)
    
    score_a = score_name(name_a)
    score_b = score_name(name_b)
    return name_a if score_a >= score_b else name_b


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
