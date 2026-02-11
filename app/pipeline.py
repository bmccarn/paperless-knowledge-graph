import re
import asyncio
import logging
import time
from datetime import datetime, timezone

from app.config import settings
from app.paperless import paperless_client, PaperlessClient
from app.classifier import classifier
from app.extractor import extractor
from app.entity_resolver import entity_resolver
from app.graph import graph_store
from app.embeddings import embeddings_store, chunk_text

logger = logging.getLogger(__name__)

# Confidence threshold - entities below this are logged but not committed
CONFIDENCE_THRESHOLD = 0.5

# --- LLM Entity Validation ---
import json as _json
from openai import AsyncOpenAI as _AsyncOpenAI

_validation_cache: dict[str, bool] = {}
_validation_client = None

def _get_validation_client():
    global _validation_client
    if _validation_client is None:
        from app.config import settings
        _validation_client = _AsyncOpenAI(
            base_url=settings.litellm_url,
            api_key=settings.litellm_api_key,
        )
    return _validation_client

ENTITY_VALIDATION_PROMPT = """You are an entity validation system for a knowledge graph. Determine if this is a real, specific named entity worth storing.

Entity name: "{name}"
Entity type: {entity_type}
From document titled: "{doc_title}"

A VALID entity is a specific, identifiable thing: a real person, company, place, product, system, law, or event.
An INVALID entity is: a generic term, action/process description, role title without a name, sentence fragment, line item label, or date.

Examples:
- "John Doe" (Person) → VALID (specific person)
- "Department of Veterans Affairs" (Organization) → VALID (specific org)
- "e-QIP" (System) → VALID (specific system)
- "background investigations" (Event) → INVALID (generic process)
- "soliciting and verifying SSN" (Person) → INVALID (action phrase)
- "Owner/Operator" (Person) → INVALID (generic role)
- "LABOR" (Product) → INVALID (invoice line item)
- "DD-214" (Document) → VALID (specific form type)
- "Investigation Request" (Event) → INVALID (generic process)
- "Fort Bragg" (Location) → VALID (specific place)
- "military installations" (Location) → INVALID (generic term)

Respond with ONLY a JSON object: {"valid": true} or {"valid": false}"""


def _is_suspicious_entity(name: str, entity_type: str) -> bool:
    """Determine if an entity name is borderline and needs LLM validation."""
    name_clean = name.strip()
    words = name_clean.split()
    
    # Always validate Event entities (most error-prone type)
    if entity_type == "Event":
        return True
    
    # Person names that don't follow typical patterns
    if entity_type == "Person":
        # All caps multi-word (might be a label, not a name)
        if name_clean.isupper() and len(words) >= 3:
            return True
        # Contains numbers (usually not a person)
        if any(c.isdigit() for c in name_clean):
            return True
        # Very short single word
        if len(words) == 1 and len(name_clean) <= 4:
            return True
    
    # Product entities are often junk from invoices
    if entity_type == "Product":
        return True
    
    # Condition entities are generally trustworthy if they came from medical docs
    # Only validate if suspiciously short or generic
    if entity_type == "Condition":
        if len(words) == 1 and len(name_clean) <= 5:
            return True
        return False
    
    # Very long names (>60 chars) are usually descriptions, not entities
    if len(name_clean) > 60:
        return True
    
    # All lowercase multi-word strings
    if len(words) >= 2 and name_clean == name_clean.lower():
        return True
    
    return False


async def _validate_entity_with_llm(name: str, entity_type: str, doc_title: str) -> bool:
    """Use LLM to validate if a borderline entity is real. Returns True if valid."""
    cache_key = f"{entity_type}:{name.lower()}"
    if cache_key in _validation_cache:
        return _validation_cache[cache_key]
    
    try:
        from app.config import settings
        from app.retry import retry_with_backoff
        
        client = _get_validation_client()
        prompt = ENTITY_VALIDATION_PROMPT.format(
            name=name, entity_type=entity_type, doc_title=doc_title
        )
        
        async def _call():
            response = await client.chat.completions.create(
                model=settings.gemini_model,
                messages=[{"role": "user", "content": prompt}],
                response_format={"type": "json_object"},
            )
            return _json.loads(response.choices[0].message.content)
        
        result = await retry_with_backoff(_call, operation="validate_entity")
        is_valid = result.get("valid", True)
        _validation_cache[cache_key] = is_valid
        
        if not is_valid:
            logger.debug(f"LLM rejected entity: '{name}' ({entity_type})")
        
        return is_valid
        
    except Exception as e:
        logger.warning(f"Entity validation LLM call failed for '{name}': {e}")
        # On failure, allow the entity through (don't block on validation errors)
        return True




async def process_document(doc: dict) -> dict:
    """Process a single Paperless document through the full pipeline."""
    doc_id = doc["id"]
    title = doc.get("title", "")
    content = doc.get("content", "")

    if not content or not content.strip():
        logger.warning(f"Doc {doc_id} has no content, skipping")
        return {"doc_id": doc_id, "status": "skipped", "reason": "no content"}

    content_hash = PaperlessClient.content_hash(content)

    # Check if already processed with same content
    existing_hash = await embeddings_store.get_doc_hash(doc_id)
    if existing_hash == content_hash:
        logger.info(f"Doc {doc_id} unchanged, skipping")
        return {"doc_id": doc_id, "status": "skipped", "reason": "unchanged"}

    logger.info(f"Processing doc {doc_id}: {title}")

    try:
        # Step 1: Classify
        classification = await classifier.classify(title, content)
        doc_type = classification["doc_type"]
        logger.info(f"Doc {doc_id} classified as {doc_type} (confidence={classification['confidence']:.2f})")

        # Step 2: Extract (3-pass pipeline - no fallback)
        extracted = await extractor.extract(title, content, doc_type)
        if isinstance(extracted, list):
            logger.warning(f"Doc {doc_id}: extraction returned list instead of dict, wrapping")
            extracted = {"items": extracted} if extracted else {}
        if not isinstance(extracted, dict):
            logger.warning(f"Doc {doc_id}: extraction returned {type(extracted).__name__}, using empty dict")
            extracted = {}

        # Log extraction confidence
        extraction_confidence = extracted.get("confidence", 1.0)
        entity_count = _count_entities(extracted)
        logger.info(f"Doc {doc_id} extracted {entity_count} fields (confidence={extraction_confidence})")

        if entity_count == 0:
            logger.warning(f"Doc {doc_id} '{title}': no entities extracted (type={doc_type}, classification_conf={classification['confidence']:.2f})")

        # Step 3: Clean old graph data for this doc
        await graph_store.delete_document_graph(doc_id)
        await embeddings_store.delete_document_embeddings(doc_id)

        # Step 4: Create document node
        doc_date = _extract_date(doc, extracted)
        doc_node_id = await graph_store.create_document_node(
            paperless_id=doc_id, title=title, doc_type=doc_type,
            date=doc_date, content_hash=content_hash,
        )

        # Step 5: Process extracted entities based on doc type
        await _process_extraction(doc_id, doc_node_id, doc_type, extracted, title=title)

        # Step 5b: Process implied relationships
        await _process_implied_relationships(doc_id, extracted)

        # Step 6: Store embeddings — chunk content for granular retrieval
        chunks = chunk_text(content, chunk_size=4000, overlap=800)
        for i, chunk in enumerate(chunks):
            await embeddings_store.store_document_embedding(
                doc_id, chunk, chunk_index=i, title=title, doc_type=doc_type
            )
        logger.info(f"Doc {doc_id}: stored {len(chunks)} embedding chunks")

        # Step 6b: Store entity embeddings for resolved entities (ALL entity types)
        await _store_entity_embeddings(doc_id, extracted)

        # Step 7: Update hash
        await embeddings_store.set_doc_hash(doc_id, content_hash)

        return {"doc_id": doc_id, "status": "processed", "doc_type": doc_type,
                "entities_extracted": entity_count,
                "chunks": len(chunks),
                "confidence": extraction_confidence}

    except Exception as e:
        logger.error(f"Failed to process doc {doc_id}: {e}", exc_info=True)
        return {"doc_id": doc_id, "status": "error", "error": str(e)}


# Blocklist of generic terms that should not become entity nodes
BLOCKED_ENTITY_NAMES = {
    # Generic role terms
    "subject matter expert", "candidates", "applicant", "customer", "client",
    "employee", "employer", "vendor", "buyer", "seller", "user", "admin",
    "recipient", "sender", "owner", "tenant", "landlord", "borrower", "lender",
    "insured", "beneficiary", "claimant", "plaintiff", "defendant",
    "taxpayer", "filer", "spouse", "dependent", "subscriber", "member",
    "patient", "provider", "physician", "doctor", "nurse",
    "contractor", "subcontractor", "consultant", "freelancer", "specialist",
    # Placeholder/null values
    "n/a", "unknown", "none", "null", "other", "various", "multiple",
    "not specified", "not applicable", "see above", "see below",
    # Invoice/accounting line items
    "labor", "parts", "deductible", "sublet", "subtotal", "total",
    "total due", "amount due", "balance due", "sales tax", "tax",
    "shop supplies", "hazardous materials", "discounts", "discount",
    "special order deposit", "deposit", "payment", "credit",
    "warranties", "warranty", "shipping", "freight", "handling",
    "miscellaneous", "misc", "other charges", "surcharge", "fee",
    "document storage fee", "processing fee", "service charge",
    # Generic roles that aren't real entity names
    "owner/operator", "owner operator", "authorized representative",
    "account holder", "primary contact", "secondary contact",
    "emergency contact", "next of kin", "power of attorney",
    "legal guardian", "authorized agent", "authorized user",
    "motocross courses",
    # Process/action phrases that aren't entities
    "investigation request", "background investigations", "background investigation",
    "continuous evaluations", "personal interview", "investigation",
    "soliciting", "verifying ssn", "e qip",
}


def _is_valid_entity_name(name: str) -> bool:
    """Validate entity name - reject generic terms and junk."""
    if not name or len(name.strip()) < 2:
        return False
    
    name_clean = name.strip()
    name_lower = name_clean.lower()
    
    # Check blocked terms
    if name_lower in BLOCKED_ENTITY_NAMES:
        return False
    
    # Reject very short names
    if len(name_clean) < 3:
        return False
    
    # Reject single common words (not proper nouns)
    words = name_clean.split()
    if len(words) == 1:
        # Single word, all lowercase = probably not a proper noun
        if name_clean.islower() or name_lower in {"and", "or", "the", "a", "an", "in", "on", "at", "by", "for", "with", "to", "of"}:
            return False
    
    # Reject obvious role descriptions
    if any(term in name_lower for term in ["matter expert", "representative", "contact person", "point of contact"]):
        return False
    
    # Reject standalone numbers (zip codes, years, amounts)
    if re.match(r"^[\d,.$]+$", name_clean):
        return False
    
    # Reject lowercase phrases (not proper nouns) — 3+ words all lowercase
    if len(words) >= 3 and all(w.islower() for w in words):
        return False
    
    # Reject strings starting with common verbs/gerunds
    first_lower = words[0].lower() if words else ""
    if first_lower in {"soliciting", "verifying", "requesting", "processing", "providing",
                       "submitting", "reviewing", "conducting", "performing", "completing",
                       "maintaining", "obtaining", "ensuring", "managing", "handling"}:
        return False
    
    # Reject strings that are all uppercase and look like invoice codes/categories
    if name_clean.isupper() and len(words) <= 2 and name_clean not in {"FBI", "CIA", "IRS", "VA", "DOD", "NASA", "NCDOT", "DMV", "SSA", "USPS"}:
        # Allow known acronyms, block generic uppercase terms
        if len(name_clean) > 10:  # Long uppercase strings are usually line items
            return False
        
    return True




# Date patterns that should NOT be entity nodes
DATE_PATTERNS = [
    re.compile(r"^\w+ \d{1,2},? \d{4}$"),           # January 15, 2026
    re.compile(r"^\d{1,2}/\d{1,2}/\d{2,4}$"),       # 01/15/2026 or 12/23/25
    re.compile(r"^\d{4}-\d{2}-\d{2}$"),              # 2026-01-15
    re.compile(r"^\d{1,2}-\d{1,2}-\d{2,4}$"),       # 01-15-2026
    re.compile(r"^\w+ \d{4}$"),                        # January 2026
    re.compile(r"^\d{4}$"),                              # 2026
    re.compile(r"^(?:Q[1-4]|H[12])\s*\d{4}$", re.I), # Q1 2026, H1 2026
]

def _is_date_string(name: str) -> bool:
    """Check if a string is just a date (should not be an entity node)."""
    name = name.strip()
    if any(p.match(name) for p in DATE_PATTERNS):
        return True
    # Also catch dates with label prefixes like "Date of Issue: 2015-10-30" or "R/O Open Date 12/23/25"
    # Strip common prefixes and re-check
    stripped = re.sub(r'^(?:Date of |R/O |In-Service |Delivery |Freight Bill |Setup |Expected date of )?\w*\s*(?:Date|date)?:?\s*', '', name).strip()
    if stripped != name and stripped and any(p.match(stripped) for p in DATE_PATTERNS):
        return True
    # Catch "Month DD, YYYY - Month DD, YYYY" date ranges
    if re.match(r'^\w+ \d{1,2},? \d{4}\s*[-–]\s*\w+ \d{1,2},? \d{4}$', name):
        return True
    # Catch "TOD DTD MM/DD/YYYY" style
    if re.match(r'^[A-Z]{2,5}\s+(?:DTD\s+)?\d{1,2}/\d{1,2}/\d{2,4}$', name):
        return True
    return False



def _is_full_address(name: str) -> bool:
    """Check if a string is a full street address or too granular for a Location entity."""
    name = name.strip()
    # Standalone zip code
    if re.match(r"^\d{5}(-\d{4})?$", name):
        return True
    # Street address pattern: starts with number + street name
    if re.match(r"^\d+\s+(N|S|E|W|North|South|East|West|NE|NW|SE|SW)?\s*\w+\s+(St|Ave|Blvd|Rd|Dr|Ln|Way|Ct|Pl|Hwy|Highway|Pkwy|Cir|Loop|Ter|Trail)\b", name, re.I):
        return True
    # Long address with number prefix
    if re.match(r"^\d+\s+\w+", name) and len(name) > 20:
        return True
    # Contains zip code anywhere
    if re.search(r"\b\d{5}(-\d{4})?\b", name) and len(name) > 10:
        return True
    return False

async def _store_entity_embeddings(doc_id: int, extracted: dict):
    """Store embeddings for ALL entity types from the 3-pass extraction."""
    try:
        all_entities = extracted.get("all_entities", [])
        if not all_entities:
            # Backward compatibility: build from people/organizations
            for person in (extracted.get("people") or []):
                name = person.get("name") if isinstance(person, dict) else person
                if name:
                    all_entities.append({
                        "name": name, 
                        "type": "Person", 
                        "description": person.get("role", "") if isinstance(person, dict) else ""
                    })
            for org in (extracted.get("organizations") or []):
                name = org.get("name") if isinstance(org, dict) else org
                if name:
                    all_entities.append({
                        "name": name, 
                        "type": "Organization", 
                        "description": org.get("type", "") if isinstance(org, dict) else ""
                    })
        
        # Process all entities (type-agnostic)
        for entity in all_entities:
            name = entity.get("name", "")
            etype = entity.get("type", "Person").strip().title()
            desc = entity.get("description", "")
            
            if not name or not _is_valid_entity_name(name):
                continue
            if etype == "Event" and _is_date_string(name):
                continue
            
            # Find the entity in the graph (try specific type first, then any type)
            results = await graph_store.search_nodes(name, node_type=etype, limit=1)
            if not results:
                results = await graph_store.search_nodes(name, limit=1)
            
            if results:
                uuid = results[0].get("properties", {}).get("uuid", "")
                if uuid:
                    emb_content = f"{name} | {etype.lower()}"
                    if desc:
                        emb_content += f" | {desc}"
                    emb_content += f" | from doc {doc_id}"
                    
                    await embeddings_store.store_entity_embedding(
                        uuid, name, entity_type=etype, content=emb_content
                    )
                    logger.debug(f"Stored embedding for {etype} entity: {name}")

        # Store embeddings for named entities from specific doc types
        for key, etype in [("patient_name", "Person"), ("provider", "Organization"),
                           ("vendor", "Organization"), ("policyholder", "Person"),
                           ("filer_name", "Person"), ("ordering_physician", "Person"),
                           ("preparer", "Person")]:
            name = extracted.get(key)
            if name and _is_valid_entity_name(name):
                results = await graph_store.search_nodes(name, node_type=etype, limit=1)
                if results:
                    uuid = results[0].get("properties", {}).get("uuid", "")
                    if uuid:
                        content = f"{name} | {etype.lower()} | {key} from doc {doc_id}"
                        await embeddings_store.store_entity_embedding(
                            uuid, name, entity_type=etype, content=content
                        )

    except Exception as e:
        logger.warning(f"Entity embedding storage failed for doc {doc_id}: {e}")


async def _process_implied_relationships(doc_id: int, extracted: dict):
    """Process implied relationships extracted from the document."""
    implied = extracted.get("implied_relationships", [])
    if not implied or not isinstance(implied, list):
        return

    source_props = {"source_doc": doc_id, "implied": True}

    for rel in implied:
        try:
            confidence = float(rel.get("confidence", 0.5))
            if confidence < CONFIDENCE_THRESHOLD:
                logger.debug(f"Skipping low-confidence implied relationship: {rel} (conf={confidence})")
                continue

            from_name = rel.get("from_entity", "")
            to_name = rel.get("to_entity", "")
            from_type = rel.get("from_type", "Person")
            to_type = rel.get("to_type", "Person")
            rel_type = rel.get("relationship", "RELATED_TO")

            if not from_name or not to_name:
                continue

            from_uuid = await _resolve_entity(from_name, from_type, doc_id)
            to_uuid = await _resolve_entity(to_name, to_type, doc_id)

            if from_uuid and to_uuid:
                props = {**source_props, "confidence": confidence}
                await graph_store.create_relationship(
                    from_uuid, from_type, to_uuid, to_type,
                    rel_type, props
                )
                logger.debug(f"Created implied relationship: {from_name} -[{rel_type}]-> {to_name}")

        except Exception as e:
            logger.warning(f"Failed to create implied relationship: {e}")


VALID_ENTITY_TYPES = {"Person", "Organization", "Location", "System", "Product", "Document", "Event", "Condition"}

# Map entity types to Neo4j labels (avoids collision with Paperless Document nodes)
ENTITY_TYPE_TO_LABEL = {
    "Document": "DocumentRef",  # "Document" label is reserved for Paperless doc nodes
}


def _neo4j_label(entity_type: str) -> str:
    """Get Neo4j label for an entity type."""
    return ENTITY_TYPE_TO_LABEL.get(entity_type, entity_type)


async def _resolve_entity(name: str, entity_type: str, doc_id: int, doc_title: str = "") -> str:
    """Route entity resolution based on type."""
    if not _is_valid_entity_name(name):
        logger.debug(f"Skipping invalid entity name: '{name}'")
        return ""
    
    # Block date strings from ALL entity types (not just Event)
    if _is_date_string(name):
        logger.debug(f"Skipping date string entity: '{name}' ({entity_type})")
        return ""
        
    entity_type = entity_type.strip().title()
    if entity_type == "Organization":
        return await entity_resolver.resolve_organization(name, doc_id)
    elif entity_type == "Person":
        return await entity_resolver.resolve_person(name, doc_id)
    elif entity_type in VALID_ENTITY_TYPES:
        # For other types, use the generic entity creation via entity_resolver
        return await entity_resolver.resolve_generic(name, entity_type, doc_id)
    else:
        # Unknown type — default to Organization if it looks like one, else Person
        if any(w in name.lower() for w in ["inc", "llc", "corp", "dept", "department", "agency", "company", "bank", "university"]):
            return await entity_resolver.resolve_organization(name, doc_id)
        return await entity_resolver.resolve_person(name, doc_id)


async def _process_enhanced_entities(doc_id: int, doc_node_id: str, extracted: dict, title: str = ""):
    """Process enhanced entities from 3-pass extraction (all entity types)."""
    all_entities = extracted.get("all_entities", [])
    if not all_entities:
        return
        
    source_props = {"source_doc": doc_id}
    
    for entity in all_entities:
        try:
            name = entity.get("name", "")
            entity_type = entity.get("type", "Person")
            confidence = float(entity.get("confidence", 0.8))
            
            if not name or confidence < CONFIDENCE_THRESHOLD:
                continue
            
            # Skip date strings masquerading as Event entities
            if entity_type == "Event" and _is_date_string(name):
                logger.debug(f"Skipping date-as-event entity: '{name}'")
                continue
                
            # Resolve the entity and create document relationships
            entity_uuid = await _resolve_entity(name, entity_type, doc_id, doc_title=title)
            if entity_uuid:
                # Create relationship from document to entity
                label = _neo4j_label(entity_type)
                await graph_store.create_relationship(
                    doc_node_id, "Document", entity_uuid, label, 
                    "MENTIONS", {**source_props, "confidence": confidence}
                )
                logger.debug(f"Created entity relationship: Document {doc_id} -[MENTIONS]-> {label} {name}")
                
        except Exception as e:
            logger.warning(f"Failed to process enhanced entity {entity}: {e}")


async def _process_extraction(doc_id: int, doc_node_id: str, doc_type: str, extracted: dict, title: str = ""):
    """Create graph nodes and relationships from extracted data."""
    source_props = {"source_doc": doc_id}

    # Process enhanced entities from 3-pass extraction if available
    await _process_enhanced_entities(doc_id, doc_node_id, extracted, title=title)

    if doc_type == "medical_lab":
        await _process_medical(doc_id, doc_node_id, extracted, source_props)
    elif doc_type == "financial_invoice":
        await _process_financial(doc_id, doc_node_id, extracted, source_props)
    elif doc_type == "legal_contract":
        await _process_contract(doc_id, doc_node_id, extracted, source_props)
    elif doc_type == "insurance":
        await _process_insurance(doc_id, doc_node_id, extracted, source_props)
    elif doc_type == "government_tax":
        await _process_tax(doc_id, doc_node_id, extracted, source_props)
    elif doc_type == "military":
        await _process_military(doc_id, doc_node_id, extracted, source_props)
    elif doc_type == "property_home":
        await _process_property(doc_id, doc_node_id, extracted, source_props)
    else:
        await _process_generic(doc_id, doc_node_id, extracted, source_props)


async def _process_medical(doc_id, doc_node_id, data, source_props):
    patient = data.get("patient_name")
    if patient and _is_valid_entity_name(patient):
        person_uuid = await entity_resolver.resolve_person(patient, doc_id, role="patient")
        if person_uuid:
            await graph_store.create_relationship(
                doc_node_id, "Document", person_uuid, "Person", "PATIENT_OF", source_props)

    provider = data.get("provider")
    if provider and _is_valid_entity_name(provider):
        org_uuid = await entity_resolver.resolve_organization(provider, doc_id, org_type="medical")
        if org_uuid:
            await graph_store.create_relationship(
                doc_node_id, "Document", org_uuid, "Organization", "PROVIDER_FOR", source_props)

    physician = data.get("ordering_physician")
    if physician and _is_valid_entity_name(physician):
        phys_uuid = await entity_resolver.resolve_person(physician, doc_id, role="physician")
        if phys_uuid:
            await graph_store.create_relationship(
                doc_node_id, "Document", phys_uuid, "Person", "AUTHORED_BY", source_props)

    for test in (data.get("tests") or []):
        if not test.get("name"):
            continue
        test_confidence = float(test.get("confidence", 1.0))
        if test_confidence < CONFIDENCE_THRESHOLD:
            logger.debug(f"Skipping low-confidence test result: {test.get('name')} (conf={test_confidence})")
            continue
        result_uuid = await graph_store.create_node("MedicalResult", {
            "test_name": test.get("name", ""),
            "value": str(test.get("value", "")),
            "unit": test.get("unit", "") or "",
            "reference_range": test.get("reference_range", "") or "",
            "flag": test.get("flag", "") or "",
            "confidence": test_confidence,
        })
        await graph_store.create_relationship(
            doc_node_id, "Document", result_uuid, "MedicalResult", "CONTAINS_RESULT", source_props)

    # Process diagnoses as Condition entities
    for diagnosis in (data.get("diagnoses") or []):
        if not diagnosis or not _is_valid_entity_name(diagnosis):
            continue
        condition_uuid = await _resolve_entity(diagnosis, "Condition", doc_id, doc_title="")
        if condition_uuid:
            await graph_store.create_relationship(
                doc_node_id, "Document", condition_uuid, "Condition", "DIAGNOSED_WITH", source_props)
            # Link patient to condition if we have one
            if patient and _is_valid_entity_name(patient):
                patient_uuid = await entity_resolver.resolve_person(patient, doc_id, role="patient")
                if patient_uuid:
                    await graph_store.create_relationship(
                        patient_uuid, "Person", condition_uuid, "Condition", "HAS_CONDITION", source_props)


async def _process_financial(doc_id, doc_node_id, data, source_props):
    vendor = data.get("vendor")
    if vendor and _is_valid_entity_name(vendor):
        org_uuid = await entity_resolver.resolve_organization(vendor, doc_id, org_type="financial")
        if org_uuid:
            await graph_store.create_relationship(
                doc_node_id, "Document", org_uuid, "Organization", "INVOICED_BY", source_props)

    amount = data.get("total_amount")
    if amount is not None:
        fi_uuid = await graph_store.create_node("FinancialItem", {
            "type": "invoice",
            "amount": str(amount),
            "date": data.get("date", "") or "",
            "reference_number": data.get("invoice_number", "") or "",
            "currency": data.get("currency", "USD") or "USD",
            "payment_status": data.get("payment_status", "") or "",
        })
        await graph_store.create_relationship(
            doc_node_id, "Document", fi_uuid, "FinancialItem", "CONTAINS_RESULT", source_props)


async def _process_contract(doc_id, doc_node_id, data, source_props):
    """Process contract with specific relationship types (PARTY_TO, CONTRACTED_WITH)."""
    for party in (data.get("parties") or []):
        name = party.get("name")
        if not name or not _is_valid_entity_name(name):
            continue
        
        # Determine if it's a person or organization based on name patterns
        if any(w in name.lower() for w in ["inc", "llc", "corp", "company", "ltd", "agency", "dept", "department"]):
            entity_uuid = await entity_resolver.resolve_organization(name, doc_id, org_type="legal")
            entity_type = "Organization"
        else:
            entity_uuid = await entity_resolver.resolve_person(name, doc_id, role=party.get("role", "party"))
            entity_type = "Person"
        
        if entity_uuid:
            # Use specific contract relationships instead of generic MENTIONS
            role = party.get("role", "").lower()
            if "sign" in role or "execute" in role or "enter" in role:
                rel_type = "CONTRACTED_WITH"
            else:
                rel_type = "PARTY_TO"
            
            await graph_store.create_relationship(
                doc_node_id, "Document", entity_uuid, _neo4j_label(entity_type), rel_type, source_props)

    # Create contract node with metadata
    contract_uuid = await graph_store.create_node("Contract", {
        "type": data.get("contract_type", "") or "",
        "effective_date": data.get("effective_date", "") or "",
        "expiration_date": data.get("expiration_date", "") or "",
        "terms_summary": data.get("terms_summary", "") or "",
        "renewal_info": data.get("renewal_info", "") or "",
    })
    await graph_store.create_relationship(
        doc_node_id, "Document", contract_uuid, "Contract", "CONTAINS_RESULT", source_props)


async def _process_insurance(doc_id, doc_node_id, data, source_props):
    provider = data.get("provider")
    if provider and _is_valid_entity_name(provider):
        org_uuid = await entity_resolver.resolve_organization(provider, doc_id, org_type="insurance")
        if org_uuid:
            await graph_store.create_relationship(
                doc_node_id, "Document", org_uuid, "Organization", "PROVIDER_FOR", source_props)

    policyholder = data.get("policyholder")
    if policyholder and _is_valid_entity_name(policyholder):
        person_uuid = await entity_resolver.resolve_person(policyholder, doc_id, role="policyholder")
        if person_uuid:
            await graph_store.create_relationship(
                doc_node_id, "Document", person_uuid, "Person", "COVERS", source_props)

    policy_uuid = await graph_store.create_node("InsurancePolicy", {
        "policy_number": data.get("policy_number", "") or "",
        "provider": data.get("provider", "") or "",
        "coverage_type": data.get("coverage_type", "") or "",
        "premium": str(data.get("premium", "")) if data.get("premium") else "",
        "effective_date": data.get("effective_date", "") or "",
        "expiration_date": data.get("expiration_date", "") or "",
    })
    await graph_store.create_relationship(
        doc_node_id, "Document", policy_uuid, "InsurancePolicy", "CONTAINS_RESULT", source_props)


async def _process_tax(doc_id, doc_node_id, data, source_props):
    filer = data.get("filer_name")
    if filer and _is_valid_entity_name(filer):
        person_uuid = await entity_resolver.resolve_person(filer, doc_id, role="filer")
        if person_uuid:
            await graph_store.create_relationship(
                doc_node_id, "Document", person_uuid, "Person", "AUTHORED_BY", source_props)

    preparer = data.get("preparer")
    if preparer and _is_valid_entity_name(preparer):
        prep_uuid = await entity_resolver.resolve_person(preparer, doc_id, role="tax_preparer")
        if prep_uuid:
            await graph_store.create_relationship(
                doc_node_id, "Document", prep_uuid, "Person", "PREPARED_BY", source_props)

    fi_uuid = await graph_store.create_node("FinancialItem", {
        "type": data.get("form_type", "tax") or "tax",
        "amount": str(data.get("total_income", "")) if data.get("total_income") else "",
        "date": data.get("tax_year", "") or "",
        "reference_number": data.get("form_type", "") or "",
        "filing_status": data.get("filing_status", "") or "",
        "tax_owed": str(data.get("tax_owed", "")) if data.get("tax_owed") else "",
        "tax_paid": str(data.get("tax_paid", "")) if data.get("tax_paid") else "",
    })
    await graph_store.create_relationship(
        doc_node_id, "Document", fi_uuid, "FinancialItem", "CONTAINS_RESULT", source_props)


async def _process_property(doc_id, doc_node_id, data, source_props):
    address = data.get("property_address")
    if address and _is_valid_entity_name(address):
        addr_uuid = await graph_store.create_node("Address", {
            "full_address": address,
        })
        await graph_store.create_relationship(
            doc_node_id, "Document", addr_uuid, "Address", "LOCATED_AT", source_props)

    for party in (data.get("parties") or []):
        name = party.get("name")
        if not name or not _is_valid_entity_name(name):
            continue
        person_uuid = await entity_resolver.resolve_person(name, doc_id, role=party.get("role", "party"))
        if person_uuid:
            await graph_store.create_relationship(
                doc_node_id, "Document", person_uuid, "Person", "MENTIONS", source_props)



async def _process_military(doc_id, doc_node_id, data, source_props):
    """Process military documents with service-specific relationships."""
    # If 3-pass extraction provided all_entities, skip legacy processing
    if data.get("all_entities"):
        return

    service_member = data.get("service_member")
    if service_member and _is_valid_entity_name(service_member):
        person_uuid = await entity_resolver.resolve_person(service_member, doc_id, role="service_member")
        if person_uuid:
            await graph_store.create_relationship(
                doc_node_id, "Document", person_uuid, "Person", "SERVICE_RECORD_OF", source_props)

    branch = data.get("branch")
    if branch and _is_valid_entity_name(branch):
        org_uuid = await entity_resolver.resolve_organization(branch, doc_id, org_type="military_branch")
        if org_uuid:
            await graph_store.create_relationship(
                doc_node_id, "Document", org_uuid, "Organization", "BRANCH_OF_SERVICE", source_props)

    unit = data.get("unit")
    if unit and _is_valid_entity_name(unit):
        org_uuid = await entity_resolver.resolve_organization(unit, doc_id, org_type="military_unit")
        if org_uuid:
            await graph_store.create_relationship(
                doc_node_id, "Document", org_uuid, "Organization", "ASSIGNED_TO", source_props)

    base = data.get("base")
    if base and _is_valid_entity_name(base):
        base_uuid = await entity_resolver.resolve_generic(base, "Location", doc_id)
        if base_uuid:
            await graph_store.create_relationship(
                doc_node_id, "Document", base_uuid, "Location", "STATIONED_AT", source_props)

    for org in (data.get("organizations") or []):
        name = org.get("name") if isinstance(org, dict) else org
        if not name or not _is_valid_entity_name(name):
            continue
        org_uuid = await entity_resolver.resolve_organization(name, doc_id, org_type=org.get("type", "military"))
        if org_uuid:
            await graph_store.create_relationship(
                doc_node_id, "Document", org_uuid, "Organization", "MENTIONS", source_props)

    for loc in (data.get("locations") or []):
        name = loc.get("name") if isinstance(loc, dict) else loc
        if not name or not _is_valid_entity_name(name):
            continue
        if _is_full_address(name):
            continue
        loc_uuid = await entity_resolver.resolve_generic(name, "Location", doc_id)
        if loc_uuid:
            context = loc.get("context", "mentioned") if isinstance(loc, dict) else "mentioned"
            rel_type = "DEPLOYED_TO" if "deploy" in context.lower() else "STATIONED_AT" if "station" in context.lower() else "LOCATED_AT"
            await graph_store.create_relationship(
                doc_node_id, "Document", loc_uuid, "Location", rel_type, source_props)


async def _process_generic(doc_id, doc_node_id, data, source_props):
    """Process generic documents - dates stored as properties, not separate nodes."""
    # If 3-pass extraction provided all_entities, skip legacy people/org processing
    # (already handled by _process_enhanced_entities)
    if data.get("all_entities"):
        return
    
    for person in (data.get("people") or []):
        name = person.get("name") if isinstance(person, dict) else person
        if not name or not _is_valid_entity_name(name):
            continue
        if isinstance(person, dict):
            confidence = float(person.get("confidence", 1.0))
            if confidence < CONFIDENCE_THRESHOLD:
                logger.debug(f"Skipping low-confidence person: {name} (conf={confidence})")
                continue
        role = person.get("role", "") if isinstance(person, dict) else ""
        person_uuid = await entity_resolver.resolve_person(name, doc_id, role=role)
        if person_uuid:
            await graph_store.create_relationship(
                doc_node_id, "Document", person_uuid, "Person", "MENTIONS", source_props)

    for org in (data.get("organizations") or []):
        name = org.get("name") if isinstance(org, dict) else org
        if not name or not _is_valid_entity_name(name):
            continue
        if isinstance(org, dict):
            confidence = float(org.get("confidence", 1.0))
            if confidence < CONFIDENCE_THRESHOLD:
                logger.debug(f"Skipping low-confidence org: {name} (conf={confidence})")
                continue
        org_type = org.get("type", "") if isinstance(org, dict) else ""
        org_uuid = await entity_resolver.resolve_organization(name, doc_id, org_type=org_type)
        if org_uuid:
            await graph_store.create_relationship(
                doc_node_id, "Document", org_uuid, "Organization", "MENTIONS", source_props)

    # Dates are stored as properties on the document node, not as separate DateEvent nodes
    # The document node already has date properties set during creation


def _extract_date(doc: dict, extracted: dict) -> str:
    """Extract the primary date for document node properties."""
    for key in ("date", "effective_date"):
        if extracted.get(key):
            return str(extracted[key])
    created = doc.get("created")
    if created:
        return str(created)[:10]
    return ""


def _count_entities(extracted: dict) -> int:
    """Count entities extracted from the document."""
    count = 0
    for key, val in extracted.items():
        if key in ("confidence", "extraction_method", "implied_relationships", "all_entities"):
            continue
        if isinstance(val, list):
            count += len(val)
        elif isinstance(val, str) and val:
            count += 1
    return count


async def sync_documents(progress_callback=None, cancel_event=None):
    """Incremental sync - process new/modified documents."""
    last_sync = await embeddings_store.get_last_sync()
    logger.info(f"Starting sync (last sync: {last_sync})")

    start_time = time.time()
    docs = await paperless_client.get_all_documents(modified_after=last_sync)
    logger.info(f"Found {len(docs)} documents to check")

    if progress_callback:
        progress_callback("init", {"total_docs": len(docs)})

    semaphore = asyncio.Semaphore(settings.max_concurrent_docs)

    async def _process_with_semaphore(doc):
        if cancel_event and cancel_event.is_set():
            return {"doc_id": doc["id"], "status": "skipped", "reason": "cancelled"}
        async with semaphore:
            if cancel_event and cancel_event.is_set():
                return {"doc_id": doc["id"], "status": "skipped", "reason": "cancelled"}
            if progress_callback:
                progress_callback("current", {"title": doc.get("title", f"Document {doc['id']}")})
            result = await process_document(doc)
            if progress_callback:
                progress_callback("result", result)
            return result

    tasks = [_process_with_semaphore(doc) for doc in docs]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    # Convert exceptions to error results
    clean_results = []
    for i, result in enumerate(results):
        if isinstance(result, Exception):
            doc_id = docs[i]["id"] if i < len(docs) else "unknown"
            logger.error(f"Unexpected error processing doc {doc_id}: {result}")
            clean_results.append({"doc_id": doc_id, "status": "error", "error": str(result)})
        else:
            clean_results.append(result)
    results = clean_results

    now = datetime.now(timezone.utc)
    await embeddings_store.set_last_sync(now)

    elapsed = time.time() - start_time
    processed = sum(1 for r in results if r["status"] == "processed")
    skipped = sum(1 for r in results if r["status"] == "skipped")
    errors = sum(1 for r in results if r["status"] == "error")
    docs_per_minute = (processed / (elapsed / 60)) if elapsed > 0 and processed > 0 else 0
    avg_entities = 0
    if processed > 0:
        total_entities = sum(r.get("entities_extracted", 0) for r in results if r["status"] == "processed")
        avg_entities = total_entities / processed

    logger.info(
        f"Sync complete: {processed} processed, {skipped} skipped, {errors} errors "
        f"| {elapsed:.1f}s | {docs_per_minute:.1f} docs/min | {avg_entities:.1f} entities/doc avg"
    )
    return {
        "total": len(docs),
        "processed": processed,
        "skipped": skipped,
        "errors": errors,
        "elapsed_seconds": round(elapsed, 1),
        "docs_per_minute": round(docs_per_minute, 1),
        "avg_entities_per_doc": round(avg_entities, 1),
        "results": results,
    }


async def reindex_all(progress_callback=None, cancel_event=None):
    """Full reindex - clear everything and reprocess all documents."""
    logger.info("Starting full reindex")
    await graph_store.clear_all()
    await embeddings_store.clear_all()

    start_time = time.time()
    docs = await paperless_client.get_all_documents()
    logger.info(f"Reindexing {len(docs)} documents")

    if progress_callback:
        progress_callback("init", {"total_docs": len(docs)})

    semaphore = asyncio.Semaphore(settings.max_concurrent_docs)

    async def _process_with_semaphore(doc):
        if cancel_event and cancel_event.is_set():
            return {"doc_id": doc["id"], "status": "skipped", "reason": "cancelled"}
        async with semaphore:
            if cancel_event and cancel_event.is_set():
                return {"doc_id": doc["id"], "status": "skipped", "reason": "cancelled"}
            if progress_callback:
                progress_callback("current", {"title": doc.get("title", f"Document {doc['id']}")})
            result = await process_document(doc)
            if progress_callback:
                progress_callback("result", result)
            return result

    tasks = [_process_with_semaphore(doc) for doc in docs]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    # Convert exceptions to error results
    clean_results = []
    for i, result in enumerate(results):
        if isinstance(result, Exception):
            doc_id = docs[i]["id"] if i < len(docs) else "unknown"
            logger.error(f"Unexpected error processing doc {doc_id}: {result}")
            clean_results.append({"doc_id": doc_id, "status": "error", "error": str(result)})
        else:
            clean_results.append(result)
    results = clean_results

    now = datetime.now(timezone.utc)
    await embeddings_store.set_last_sync(now)

    elapsed = time.time() - start_time
    processed = sum(1 for r in results if r["status"] == "processed")
    errors = sum(1 for r in results if r["status"] == "error")

    logger.info(f"Reindex complete: {processed} processed, {errors} errors | {elapsed:.1f}s")

    # Post-reindex: build vector indexes and resolve entities
    if processed > 0 and not (cancel_event and cancel_event.is_set()):
        if progress_callback:
            progress_callback("current", {"title": "Building vector indexes..."})
        try:
            logger.info("Post-reindex: creating IVFFlat vector indexes")
            await embeddings_store.create_vector_indexes()
            logger.info("Post-reindex: vector indexes created")
        except Exception as e:
            logger.error(f"Post-reindex: failed to create indexes: {e}")

        if progress_callback:
            progress_callback("current", {"title": "Resolving duplicate entities..."})
        try:
            logger.info("Post-reindex: running entity resolution")
            report = await entity_resolver.resolve_all_entities()
            merged = report.get("total_merged", 0)
            logger.info(f"Post-reindex: entity resolution complete — {merged} entities merged")
        except Exception as e:
            logger.error(f"Post-reindex: entity resolution failed: {e}")

    return {
        "total": len(docs),
        "processed": processed,
        "errors": errors,
        "elapsed_seconds": round(elapsed, 1),
        "results": results,
    }


async def reindex_document(doc_id: int):
    """Reindex a single document."""
    logger.info(f"Reindexing document {doc_id}")
    doc = await paperless_client.get_document(doc_id)

    await embeddings_store.delete_doc_hash(doc_id)
    await graph_store.delete_document_graph(doc_id)
    await embeddings_store.delete_document_embeddings(doc_id)

    result = await process_document(doc)
    return result