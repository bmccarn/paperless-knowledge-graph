import re
import asyncio
import logging
import time
from datetime import datetime, timezone
from typing import Any
from contextvars import ContextVar
import json as _json
from app.entity_bindings import DocumentBindings, ResolvedEntity

from app.config import settings
from app.paperless import paperless_client, PaperlessClient
from app.classifier import classifier
from app.extractor import extractor
from app.entity_resolver import entity_resolver
from app.graph import graph_store, _sanitize_rel_type
from app.embeddings import embeddings_store, chunk_text
from app.cache import invalidate_on_sync

logger = logging.getLogger(__name__)

# Confidence threshold - entities below this are logged but not committed
CONFIDENCE_THRESHOLD = 0.5

# Type adjudication belongs to the extractor's source-aware verification pass.
# Never run a name/title-only alternate-meaning classifier after acceptance.
_document_bindings: ContextVar[DocumentBindings | None] = ContextVar("document_entity_bindings", default=None)


async def close_clients():
    """Kept for application shutdown compatibility; no name-only client exists."""
    return None


async def _create_relationship(from_uuid, from_label, to_uuid, to_label, rel_type, properties=None):
    """Carry exact resolved labels through legacy metadata processors too."""
    bindings = _document_bindings.get()
    rel_type = _sanitize_rel_type(rel_type)
    if bindings is not None:
        from app.relationship_support import merge_support_properties
        key = (str(from_uuid), str(to_uuid), rel_type)
        properties = merge_support_properties(bindings.relationship_support.get(key, {}), properties or {})
        bindings.relationship_support[key] = properties
    await graph_store.create_relationship(
        str(from_uuid), getattr(from_uuid, "entity_type", from_label),
        str(to_uuid), getattr(to_uuid, "entity_type", to_label), rel_type, properties)


def _coerce_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, list):
        for item in value:
            text = _coerce_text(item)
            if text:
                return text
        return ""
    if isinstance(value, dict):
        return " ".join(_coerce_text(item) for item in value.values()).strip()
    return str(value).strip()


async def _purge_document_index(doc_id: int) -> None:
    """Remove a document's derived state, leaving interrupted work retryable."""
    await entity_resolver.hydrate_review_identities()
    try:
        await asyncio.to_thread(invalidate_on_sync)
        await embeddings_store.delete_doc_hash(doc_id)
        await graph_store.delete_document_graph(doc_id)
        await embeddings_store.delete_document_embeddings(doc_id)
    finally:
        await asyncio.to_thread(invalidate_on_sync)


async def process_document(doc: dict, *, force: bool = False) -> dict:
    """Process a single Paperless document through the full pipeline."""
    doc_id = doc["id"]
    title = doc.get("title", "")
    content = doc.get("content", "") or ""
    content_hash = PaperlessClient.content_hash(content)
    fingerprint = PaperlessClient.ingestion_fingerprint(doc)

    skip_tag_ids = await paperless_client.get_skip_tag_ids()
    if paperless_client.has_any_tag(doc, skip_tag_ids):
        logger.info(f"Doc {doc_id} has a configured skip tag, removing any KG index")
        await _purge_document_index(doc_id)
        return {"doc_id": doc_id, "status": "skipped", "reason": "configured skip tag present"}

    mutation_started = False
    if force:
        # The old graph/chunks remain usable during preparation. A missing
        # completion marker makes a failed forced attempt discoverable by sync.
        await embeddings_store.delete_doc_hash(doc_id)
        await asyncio.to_thread(invalidate_on_sync)

    if not content or not content.strip():
        logger.warning(f"Doc {doc_id} has no content, recording metadata-only index")
        try:
            doc_type = "no_content"
            doc_date = _extract_date(doc, {})
            metadata_content = (
                f"Document: {title or f'Document {doc_id}'}\n"
                f"Type: {doc_type}\n"
                f"Date: {doc_date or 'unknown'}\n"
                f"Paperless ID: {doc_id}\n\n"
                "No OCR content is available for this Paperless document."
            )
            embedding = await embeddings_store.generate_embedding(metadata_content)
            if not embedding:
                raise ValueError("Metadata-only document embedding was not generated")
            await entity_resolver.hydrate_review_identities()
            mutation_started = True
            await asyncio.to_thread(invalidate_on_sync)
            await embeddings_store.delete_doc_hash(doc_id)
            await graph_store.delete_document_graph(doc_id)
            await embeddings_store.delete_document_embeddings(doc_id)
            await graph_store.create_document_node(
                paperless_id=doc_id,
                title=title,
                doc_type=doc_type,
                date=doc_date,
                content_hash=content_hash,
            )

            await embeddings_store.store_document_embedding(
                doc_id,
                metadata_content,
                chunk_index=0,
                title=title,
                doc_type=doc_type,
                embedding=embedding,
                source_kind="metadata",
            )
            await embeddings_store.set_doc_hash(doc_id, content_hash, ingestion_fingerprint=fingerprint)
            return {
                "doc_id": doc_id,
                "status": "processed",
                "doc_type": doc_type,
                "entities_extracted": 0,
                "chunks": 1,
                "confidence": 0.0,
                "reason": "metadata-only; no OCR content",
            }
        except Exception as e:
            logger.error(f"Failed to process metadata-only doc {doc_id}: {e}", exc_info=True)
            return {"doc_id": doc_id, "status": "error", "error": str(e)}
        finally:
            if mutation_started or force:
                await asyncio.to_thread(invalidate_on_sync)

    # Check if already processed with same content
    existing_hash = await embeddings_store.get_doc_hash(doc_id)
    indexed_fingerprint = (await embeddings_store.get_ingestion_fingerprints([doc_id])).get(doc_id)
    if not force and existing_hash == content_hash and indexed_fingerprint == fingerprint:
        logger.info(f"Doc {doc_id} unchanged, skipping")
        return {"doc_id": doc_id, "status": "skipped", "reason": "unchanged"}

    logger.info(f"Processing doc {doc_id}: {title}")

    binding_token = None
    try:
        # Step 1: Classify
        classification = await classifier.classify(title, content)
        doc_type = classification["doc_type"]
        logger.info(f"Doc {doc_id} classified as {doc_type} (confidence={classification['confidence']:.2f})")

        # Step 2: Extract (3-pass pipeline - no fallback)
        extracted = await extractor.extract(title, content, doc_type)
        if not isinstance(extracted, dict):
            raise ValueError("Extraction did not return a structured document")
        coverage = extracted.get("extraction_coverage") or {}
        if coverage.get("status") != "complete":
            raise ValueError("Extraction coverage is incomplete; previous index retained")

        bindings = DocumentBindings(doc_id, extracted, content, entity_resolver)

        # Log extraction confidence
        extraction_confidence = extracted.get("confidence", 1.0)
        entity_count = _count_entities(extracted)
        logger.info(f"Doc {doc_id} extracted {entity_count} fields (confidence={extraction_confidence})")

        if entity_count == 0:
            logger.warning(f"Doc {doc_id} '{title}': no entities extracted (type={doc_type}, classification_conf={classification['confidence']:.2f})")

        # Prepare all required document vectors before deleting usable state.
        # Keep the complete OCR text; boilerplate heuristics must not remove facts.
        doc_date = _extract_date(doc, extracted)
        chunks = chunk_text(content, chunk_size=4000, overlap=800)
        metadata_prefix = f"Document: {title}\nType: {doc_type}\nDate: {doc_date or 'unknown'}\n\n"
        prepared_chunks = [(i, metadata_prefix + chunk, chunk, "ocr") for i, chunk in enumerate(chunks)]
        doc_summary = await _generate_document_summary(doc_id, title, doc_type, content, extracted)
        if doc_summary:
            prepared_chunks.append((9999, doc_summary, None, "generated"))
        prepared_vectors = []
        for index, text, source_content, source_kind in prepared_chunks:
            embedding = await embeddings_store.generate_embedding(text)
            if not embedding:
                raise ValueError(f"Document embedding was not generated for chunk {index}")
            prepared_vectors.append((index, text, embedding, source_content, source_kind))

        await entity_resolver.hydrate_review_identities()
        mutation_started = True
        await asyncio.to_thread(invalidate_on_sync)
        # The completion marker commits last. A write failure is retryable even
        # when this forced document predates the corpus checkpoint.
        await embeddings_store.delete_doc_hash(doc_id)
        await graph_store.delete_document_graph(doc_id)
        await embeddings_store.delete_document_embeddings(doc_id)

        # Step 4: Create document node
        doc_node_id = await graph_store.create_document_node(
            paperless_id=doc_id, title=title, doc_type=doc_type,
            date=doc_date, content_hash=content_hash,
            extraction_metadata={
                key: extracted.get(key)
                for key in ("extraction_coverage", "metadata_evidence", "metadata_conflicts", "extraction_issues")
                if extracted.get(key) is not None
            },
        )

        # Keep one generation's evidence accumulator across every graph writer,
        # including implied relationships after the metadata processor returns.
        binding_token = _document_bindings.set(bindings)

        # Step 5: Process extracted entities based on doc type
        await _process_extraction(doc_id, doc_node_id, doc_type, extracted, title=title, bindings=bindings)

        # Step 5b: Process implied relationships
        await _process_implied_relationships(doc_id, extracted, bindings=bindings)

        for index, text, embedding, source_content, source_kind in prepared_vectors:
            await embeddings_store.store_document_embedding(
                doc_id, text, chunk_index=index, title=title, doc_type=doc_type,
                embedding=embedding, source_content=source_content, source_kind=source_kind,
            )
        logger.info(f"Doc {doc_id}: stored {len(chunks)} embedding chunks")

        # Step 6b: Store entity embeddings for resolved entities (ALL entity types)
        await _store_entity_embeddings(doc_id, extracted, bindings=bindings)

        # Step 7: Update hash
        await embeddings_store.set_doc_hash(doc_id, content_hash, ingestion_fingerprint=fingerprint)

        return {"doc_id": doc_id, "status": "processed", "doc_type": doc_type,
                "entities_extracted": entity_count,
                "chunks": len(chunks),
                "confidence": extraction_confidence}

    except Exception as e:
        logger.error(f"Failed to process doc {doc_id}: {e}", exc_info=True)
        return {"doc_id": doc_id, "status": "error", "error": str(e)}
    finally:
        if binding_token is not None:
            _document_bindings.reset(binding_token)
        if mutation_started or force:
            await asyncio.to_thread(invalidate_on_sync)


# Protected entity names - NEVER rejected by LLM validation or blocklist.
# These are known-good entities that the LLM might not recognize (pets, nicknames, etc.)
PROTECTED_ENTITY_NAMES = {
    "ggarbo", "ggarbo mccarn", "ggarbo mccam", "ggarbo mccarm",
    "mwd ggarbo", "mwd ggarbo tattoo v234",
    "ggarbo v234 12-0047", "ggarbo v234 12-0047 (canine)",
}


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
    # Junk entities seen in reindexes
    "rating decision", "builders", "owners", "trane",
    "josh", "braesael", "homeowner",
}


def _is_valid_entity_name(name: str) -> bool:
    """Validate entity name - reject generic terms and junk."""
    name = _coerce_text(name)
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
    name = _coerce_text(name)
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
    name = _coerce_text(name)
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


# --- Boilerplate Filter (Improvement D) ---
_BOILERPLATE_PATTERNS = [
    re.compile(r'(?:OMB Approved|Respondent Burden|Expiration Date).*', re.I),
    re.compile(r'SERVES THE FOLLOWING STATES.*?(?=\n(?:---|#)|$)', re.S),
    re.compile(r'\[QR [Cc]ode[^\]]*\]'),
    re.compile(r'Veterans Crisis Line.*?(?:veteranscrisisisline\.net|838255).*?\n', re.S | re.I),
    re.compile(r'(?:^|\n)\s*(?:\d+ of \d+|Page \d+)\s*(?:\n|$)', re.M),
    re.compile(r'(?:^|\n)\s*SIGN HERE\s*.*$', re.M),
    re.compile(r'(?:^|\n)\s*\d+\.\s*(?:SOCIAL SECURITY NUMBER|SEX OF APPLICANT|DATE OF BIRTH)\s*$', re.M),
]

def _filter_boilerplate(content: str) -> str:
    """Remove boilerplate sections from document content before chunking."""
    if not content:
        return content
    filtered = content
    for pattern in _BOILERPLATE_PATTERNS:
        filtered = pattern.sub('\n', filtered)
    filtered = re.sub(r'\n{4,}', '\n\n\n', filtered)
    # Remove visual separator lines (----, ===, etc.) but preserve markdown table
    # separator rows (| :--- | :--- |) which are needed for table-aware chunking
    filtered = re.sub(r'^[\s:-]+$', '', filtered, flags=re.M)  # no | in class
    original_len = len(content.strip())
    filtered_len = len(filtered.strip())
    stripped_pct = round((1 - filtered_len / original_len) * 100, 1) if original_len > 0 else 0
    if stripped_pct > 60:
        logger.warning(f'Boilerplate filter stripped {stripped_pct}% of content, using original')
        return content
    if stripped_pct > 5:
        logger.info(f'Boilerplate filter: stripped {stripped_pct}% ({original_len - filtered_len} chars)')
    return filtered.strip()


# --- Document Summary Generator (Improvement A) ---
async def _generate_document_summary(doc_id: int, title: str, doc_type: str,
                                      content: str, extracted: dict) -> str:
    """Generate a concise document summary capturing key facts for embedding."""
    from app.config import settings as _settings
    from app.retry import retry_with_backoff
    from openai import AsyncOpenAI

    client = None
    try:
        client = AsyncOpenAI(
            base_url=_settings.litellm_url,
            api_key=_settings.litellm_api_key,
        )

        extracted_facts = []
        for key, val in extracted.items():
            if key in ("confidence", "extraction_method", "implied_relationships", "all_entities"):
                continue
            if isinstance(val, str) and val:
                extracted_facts.append(f"{key}: {val}")
            elif isinstance(val, list) and val:
                items = []
                for item in val[:10]:
                    if isinstance(item, dict):
                        items.append(str({k: v for k, v in item.items() if v}))
                    else:
                        items.append(str(item))
                extracted_facts.append(f"{key}: {', '.join(items)}")

        facts_text = "\n".join(extracted_facts[:30]) if extracted_facts else "No structured data extracted."

        prompt = f"""Summarize this document in 150-200 words. Focus on KEY FACTS: names, numbers, dates, amounts, ratings, percentages, decisions, and outcomes. Be specific and precise.

If this is a government/VA/military document, explicitly state any disability ratings, combined rating percentages, effective dates, permanent/total status, and decisions made.
If this is a financial document, state amounts, parties, account numbers, and dates.
If this is a medical document, state diagnoses, test results, providers, and dates.

Do NOT include boilerplate, instructions, or form descriptions. Only summarize the actual substantive content.

Document title: {title}
Document type: {doc_type}
Extracted metadata:
{facts_text}

Document content (first 8000 chars):
{content[:24000]}

Summary:"""

        async def _call():
            response = await client.chat.completions.create(
                model=_settings.gemini_model,
                messages=[{"role": "user", "content": prompt}],
                max_tokens=1500,
            )
            text = response.choices[0].message.content
            if text:
                text = text.strip()
            return text or ""

        summary = await retry_with_backoff(_call, operation="generate_doc_summary")
        
        # If summary is suspiciously short, retry once with more explicit instruction
        if len(summary) < 200:
            logger.warning(f"Doc {doc_id}: summary too short ({len(summary)} chars), retrying with explicit prompt")
            retry_prompt = f"Write a 150-200 word factual summary of this document. Include ALL key numbers, dates, percentages, names, and decisions.\n\nTitle: {title}\nContent (first 12000 chars):\n{content[:24000]}"
            
            async def _retry_call():
                response = await client.chat.completions.create(
                    model=_settings.gemini_model,
                    messages=[{"role": "user", "content": retry_prompt}],
                    max_tokens=1500,
                )
                text = response.choices[0].message.content
                if text:
                    text = text.strip()
                return text or ""
            
            retry_summary = await retry_with_backoff(_retry_call, operation="generate_doc_summary_retry")
            if len(retry_summary) > len(summary):
                summary = retry_summary
        
        full_summary = f"DOCUMENT SUMMARY — {title} (Type: {doc_type}, Doc ID: {doc_id})\n\n{summary}"
        logger.info(f"Doc {doc_id}: generated summary ({len(summary)} chars)")
        return full_summary

    except Exception as e:
        logger.warning(f"Doc {doc_id}: summary generation failed: {e}")
        return ""
    finally:
        if client is not None:
            await client.close()


async def _store_entity_embeddings(doc_id: int, extracted: dict, *, bindings: DocumentBindings | None = None):
    """Use UUID/type bindings; fuzzy search can attach vectors to the wrong node."""
    if bindings is None:
        if extracted.get("all_entities"):
            raise ValueError("Entity embeddings require document identity bindings")
        return
    seen = set()
    for resolved in bindings.resolved.values():
        if str(resolved) in seen:
            continue
        seen.add(str(resolved))
        content = f"{resolved.name} | {resolved.entity_type.lower()} | {resolved.description} | from doc {doc_id}"
        await embeddings_store.store_entity_embedding(str(resolved), resolved.name,
            entity_type=resolved.entity_type, content=content)


async def _process_implied_relationships(doc_id: int, extracted: dict, *, bindings: DocumentBindings | None = None):
    """Bind accepted identities and accumulate support even for direct callers."""
    relationships = extracted.get("implied_relationships") or []
    if not relationships:
        return
    if bindings is None or bindings.doc_id != doc_id:
        raise ValueError("Relationships require this document's identity bindings")
    token = _document_bindings.set(bindings)
    try:
        for rel in relationships:
            left = bindings.lookup(rel.get("from_entity", ""), rel.get("from_type", ""), rel.get("from_entity_id"))
            right = bindings.lookup(rel.get("to_entity", ""), rel.get("to_type", ""), rel.get("to_entity_id"))
            if not left or not right:
                raise ValueError("Relationship endpoint is missing or ambiguous in accepted identities")
            await _create_relationship(left, left.entity_type, right, right.entity_type,
                rel.get("relationship", "RELATED_TO"), {
                    "source_doc": doc_id, "confidence": rel.get("confidence", 0),
                    "implied": bool(rel.get("inferred", True)),
                    "rationale": _coerce_text(rel.get("rationale") or rel.get("explanation")),
                    "evidence_json": _json.dumps(rel.get("evidence") or []),
                })
    finally:
        _document_bindings.reset(token)


# Canonical PascalCase map — .title() breaks multi-capital types like FinancialItem
_CANONICAL_ENTITY_TYPE = {
    "financialitem": "FinancialItem",
    "insurancepolicy": "InsurancePolicy",
    "dateevent": "DateEvent",
    "documentref": "DocumentRef",
    "medicalresult": "MedicalResult",
    "person": "Person",
    "organization": "Organization",
    "location": "Location",
    "system": "System",
    "product": "Product",
    "document": "Document",
    "event": "Event",
    "condition": "Condition",
    "contract": "Contract",
    "address": "Address",
}

def _normalize_entity_type(etype: str) -> str:
    """Normalize entity type to canonical PascalCase. Handles multi-capital types."""
    etype = _coerce_text(etype)
    if not etype:
        return etype
    cleaned = etype.strip()
    return _CANONICAL_ENTITY_TYPE.get(cleaned.lower(), cleaned.title())

VALID_ENTITY_TYPES = {"Person", "Organization", "Location", "System", "Product", "Document", "Event", "Condition", "FinancialItem", "InsurancePolicy", "Contract", "DateEvent", "Address"}

# Map entity types to Neo4j labels (avoids collision with Paperless Document nodes)
ENTITY_TYPE_TO_LABEL = {
    "Document": "DocumentRef",  # "Document" label is reserved for Paperless doc nodes
}


def _neo4j_label(entity_type: str) -> str:
    """Get Neo4j label for an entity type."""
    return ENTITY_TYPE_TO_LABEL.get(entity_type, entity_type)


async def _resolve_entity(name: str, entity_type: str, doc_id: int, doc_title: str = "", description: str = "",
                          *, allowed_types: set[str] | None = None) -> str:
    """Reuse identity without confusing a corrected type with a compatible role.

    Metadata processors specify their role's allowed endpoint types; generic
    mentions leave this unset. Independently reviewed relationships bypass these
    metadata assumptions and bind their exact accepted endpoints directly.
    """
    name, entity_type = _coerce_text(name), _normalize_entity_type(entity_type)
    bindings = _document_bindings.get()
    if bindings is not None:
        if bindings.doc_id != doc_id:
            raise ValueError("Cross-document identity binding attempted")
        found = bindings.lookup(name) if bindings.authoritative else bindings.lookup(name, entity_type)
        if found:
            if allowed_types is not None and found.entity_type not in allowed_types:
                logger.warning("Metadata role omitted: doc_id=%s allowed_types=%s accepted_type=%s",
                               doc_id, sorted(allowed_types), found.entity_type)
                return ""
            return found
        if bindings.authoritative:
            return ""  # metadata cannot bypass an omitted/ambiguous entity
    if not _is_valid_entity_name(name) or _is_date_string(name):
        return ""
    if entity_type not in VALID_ENTITY_TYPES:
        raise ValueError("Unsupported entity type; name-only inference is prohibited")
    if allowed_types is not None and entity_type not in allowed_types:
        return ""
    # Source-less compatibility callers retain their explicit type and get only
    # conservative canonical/reviewed matching, with no type model or cache.
    uuid = await entity_resolver.resolve(name, entity_type, doc_id, description=description)
    resolved = ResolvedEntity(uuid, entity_type, name, description)
    if bindings is not None:
        key = f"legacy:{entity_type}:{name.casefold()}"
        bindings.entities[key] = {"name": name, "type": entity_type}
        bindings.resolved[key] = resolved
    return resolved


async def _process_enhanced_entities(doc_id: int, doc_node_id: str, extracted: dict, title: str = "", *, bindings=None):
    if bindings is None:
        bindings = DocumentBindings(doc_id, extracted, "", entity_resolver)
        await bindings.resolve_all()
    for entity_id, entity in bindings.entities.items():
        resolved = bindings.resolved[entity_id]
        await _create_relationship(doc_node_id, "Document", resolved, resolved.entity_type,
            "MENTIONS", {"source_doc": doc_id, "confidence": entity.get("confidence", 0),
                         "evidence_json": _json.dumps(entity.get("evidence") or []),
                         "type_assessment_json": _json.dumps(entity.get("type_assessment") or {})})
    return bindings


async def _process_extraction(doc_id: int, doc_node_id: str, doc_type: str, extracted: dict,
                              title: str = "", *, bindings=None):
    """One document-local map across enhanced and typed metadata processors."""
    bindings = bindings or DocumentBindings(doc_id, extracted, "", entity_resolver)
    await bindings.resolve_all()
    token = _document_bindings.set(bindings)
    try:
        await _process_enhanced_entities(doc_id, doc_node_id, extracted, title, bindings=bindings)
        processors = {"medical_lab": _process_medical, "financial_invoice": _process_financial,
                      "legal_contract": _process_contract, "insurance": _process_insurance,
                      "government_tax": _process_tax, "military": _process_military,
                      "property_home": _process_property}
        await processors.get(doc_type, _process_generic)(doc_id, doc_node_id, extracted, {"source_doc": doc_id})
    finally:
        _document_bindings.reset(token)
    return bindings


def _metadata_field_support(data, field, source_props):
    """Carry only this field's exact current-source quotes into its role edge."""
    from app.entity_policy import verified_spans
    bindings = _document_bindings.get()
    value = data.get(field)
    if (bindings is None or not bindings.source or not isinstance(value, str)
            or source_props.get("source_doc") != bindings.doc_id):
        return source_props
    spans = []
    for evidence in (data.get("metadata_evidence") or {}).values():
        for span in verified_spans(value, evidence.get(field), bindings.source):
            exact = {key: span[key] for key in ("start", "end", "quote")}
            if exact not in spans:
                spans.append(exact)
    return {**source_props, "evidence_spans": spans} if spans else source_props


async def _process_medical(doc_id, doc_node_id, data, source_props):
    patient = data.get("patient_name")
    if patient and _is_valid_entity_name(patient):
        person_uuid = await _resolve_entity(patient, "Person", doc_id, doc_title="", allowed_types={'Person'})
        if person_uuid:
            await _create_relationship(
                doc_node_id, "Document", person_uuid, "Person", "PATIENT_OF", source_props)

    provider = data.get("provider")
    if provider and _is_valid_entity_name(provider):
        org_uuid = await _resolve_entity(provider, "Organization", doc_id, doc_title="", allowed_types={'Person', 'Organization'})
        if org_uuid:
            await _create_relationship(
                doc_node_id, "Document", org_uuid, "Organization", "PROVIDER_FOR",
                _metadata_field_support(data, "provider", source_props))

    physician = data.get("ordering_physician")
    if physician and _is_valid_entity_name(physician):
        phys_uuid = await _resolve_entity(physician, "Person", doc_id, doc_title="", allowed_types={'Person'})
        if phys_uuid:
            await _create_relationship(
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
        await _create_relationship(
            doc_node_id, "Document", result_uuid, "MedicalResult", "CONTAINS_RESULT", source_props)

    # Process diagnoses as Condition entities
    for diagnosis in (data.get("diagnoses") or []):
        if not diagnosis or not _is_valid_entity_name(diagnosis):
            continue
        condition_uuid = await _resolve_entity(diagnosis, "Condition", doc_id, doc_title="", allowed_types={'Condition'})
        if condition_uuid:
            await _create_relationship(
                doc_node_id, "Document", condition_uuid, "Condition", "DIAGNOSED_WITH", source_props)
            # Link patient to condition if we have one
            if patient and _is_valid_entity_name(patient):
                patient_uuid = await _resolve_entity(patient, "Person", doc_id, allowed_types={'Person'})
                if patient_uuid:
                    await _create_relationship(
                        patient_uuid, "Person", condition_uuid, "Condition", "HAS_CONDITION", source_props)


async def _process_financial(doc_id, doc_node_id, data, source_props):
    vendor = data.get("vendor")
    if vendor and _is_valid_entity_name(vendor):
        org_uuid = await _resolve_entity(vendor, "Organization", doc_id, doc_title="", allowed_types={'Person', 'Organization'})
        if org_uuid:
            await _create_relationship(
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
        await _create_relationship(
            doc_node_id, "Document", fi_uuid, "FinancialItem", "CONTAINS_RESULT", source_props)


async def _process_contract(doc_id, doc_node_id, data, source_props):
    """Process contract with specific relationship types (PARTY_TO, CONTRACTED_WITH)."""
    for party in (data.get("parties") or []):
        name = _coerce_text(party.get("name"))
        if not name or not _is_valid_entity_name(name):
            continue
        
        # Determine if it's a person or organization based on name patterns
        if any(w in name.lower() for w in ["inc", "llc", "corp", "company", "ltd", "agency", "dept", "department"]):
            entity_uuid = await _resolve_entity(name, "Organization", doc_id, allowed_types={'Person', 'Organization'})
            entity_type = "Organization"
        else:
            entity_uuid = await _resolve_entity(name, "Person", doc_id, allowed_types={'Person', 'Organization'})
            entity_type = "Person"
        
        if entity_uuid:
            # Use specific contract relationships instead of generic MENTIONS
            role = _coerce_text(party.get("role", "")).lower()
            if "sign" in role or "execute" in role or "enter" in role:
                rel_type = "CONTRACTED_WITH"
            else:
                rel_type = "PARTY_TO"
            
            await _create_relationship(
                doc_node_id, "Document", entity_uuid, _neo4j_label(entity_type), rel_type, source_props)

    # Create contract node with metadata
    contract_uuid = await graph_store.create_node("Contract", {
        "type": data.get("contract_type", "") or "",
        "effective_date": data.get("effective_date", "") or "",
        "expiration_date": data.get("expiration_date", "") or "",
        "terms_summary": data.get("terms_summary", "") or "",
        "renewal_info": data.get("renewal_info", "") or "",
    })
    await _create_relationship(
        doc_node_id, "Document", contract_uuid, "Contract", "CONTAINS_RESULT", source_props)


async def _process_insurance(doc_id, doc_node_id, data, source_props):
    provider = data.get("provider")
    if provider and _is_valid_entity_name(provider):
        org_uuid = await _resolve_entity(provider, "Organization", doc_id, allowed_types={'Person', 'Organization'})
        if org_uuid:
            await _create_relationship(
                doc_node_id, "Document", org_uuid, "Organization", "PROVIDER_FOR",
                _metadata_field_support(data, "provider", source_props))

    policyholder = data.get("policyholder")
    if policyholder and _is_valid_entity_name(policyholder):
        person_uuid = await _resolve_entity(policyholder, "Person", doc_id, allowed_types={'Person', 'Organization'})
        if person_uuid:
            await _create_relationship(
                doc_node_id, "Document", person_uuid, "Person", "COVERS", source_props)

    policy_uuid = await graph_store.create_node("InsurancePolicy", {
        "policy_number": data.get("policy_number", "") or "",
        "provider": data.get("provider", "") or "",
        "coverage_type": data.get("coverage_type", "") or "",
        "premium": str(data.get("premium", "")) if data.get("premium") else "",
        "effective_date": data.get("effective_date", "") or "",
        "expiration_date": data.get("expiration_date", "") or "",
    })
    await _create_relationship(
        doc_node_id, "Document", policy_uuid, "InsurancePolicy", "CONTAINS_RESULT", source_props)


async def _process_tax(doc_id, doc_node_id, data, source_props):
    filer = data.get("filer_name")
    if filer and _is_valid_entity_name(filer):
        person_uuid = await _resolve_entity(filer, "Person", doc_id, allowed_types={'Person', 'Organization'})
        if person_uuid:
            await _create_relationship(
                doc_node_id, "Document", person_uuid, "Person", "AUTHORED_BY", source_props)

    preparer = data.get("preparer")
    if preparer and _is_valid_entity_name(preparer):
        prep_uuid = await _resolve_entity(preparer, "Person", doc_id, allowed_types={'Person', 'Organization'})
        if prep_uuid:
            await _create_relationship(
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
    await _create_relationship(
        doc_node_id, "Document", fi_uuid, "FinancialItem", "CONTAINS_RESULT", source_props)


async def _process_property(doc_id, doc_node_id, data, source_props):
    address = data.get("property_address")
    if address and _is_valid_entity_name(address):
        addr_uuid = await graph_store.create_node("Address", {
            "full_address": address,
        })
        await _create_relationship(
            doc_node_id, "Document", addr_uuid, "Address", "LOCATED_AT", source_props)

    for party in (data.get("parties") or []):
        name = party.get("name")
        if not name or not _is_valid_entity_name(name):
            continue
        person_uuid = await _resolve_entity(name, "Person", doc_id)
        if person_uuid:
            await _create_relationship(
                doc_node_id, "Document", person_uuid, "Person", "MENTIONS", source_props)



async def _process_military(doc_id, doc_node_id, data, source_props):
    """Process military documents with service-specific relationships and VA rating data."""
    service_member = data.get("service_member")
    person_uuid = None
    if service_member and _is_valid_entity_name(service_member):
        person_uuid = await _resolve_entity(service_member, "Person", doc_id, allowed_types={'Person'})
        if person_uuid:
            await _create_relationship(
                doc_node_id, "Document", person_uuid, "Person", "SERVICE_RECORD_OF", source_props)

    branch = data.get("branch")
    if branch and _is_valid_entity_name(branch):
        org_uuid = await _resolve_entity(branch, "Organization", doc_id, allowed_types={'Organization'})
        if org_uuid:
            await _create_relationship(
                doc_node_id, "Document", org_uuid, "Organization", "BRANCH_OF_SERVICE", source_props)

    unit = data.get("unit")
    if unit and _is_valid_entity_name(unit):
        org_uuid = await _resolve_entity(unit, "Organization", doc_id, allowed_types={'Organization'})
        if org_uuid:
            await _create_relationship(
                doc_node_id, "Document", org_uuid, "Organization", "ASSIGNED_TO", source_props)

    base = data.get("base")
    if base and _is_valid_entity_name(base):
        base_uuid = await _resolve_entity(base, "Location", doc_id, allowed_types={'Location', 'Address', 'Organization'})
        if base_uuid:
            await _create_relationship(
                doc_node_id, "Document", base_uuid, "Location", "STATIONED_AT", source_props)

    # B: Process disability ratings as MedicalResult nodes
    for rating in (data.get("disability_ratings") or []):
        condition = rating.get("condition", "")
        percentage = rating.get("percentage", "")
        if not condition:
            continue
        result_uuid = await graph_store.create_node("MedicalResult", {
            "test_name": condition,
            "value": str(percentage) + "%" if percentage else "",
            "unit": "percent",
            "reference_range": "",
            "flag": rating.get("status", ""),
            "effective_date": rating.get("effective_date", ""),
            "confidence": 1.0,
        })
        await _create_relationship(
            doc_node_id, "Document", result_uuid, "MedicalResult", "CONTAINS_RESULT", source_props)
        # Link person to condition
        if person_uuid and condition and _is_valid_entity_name(condition):
            condition_uuid = await _resolve_entity(condition, "Condition", doc_id, allowed_types={'Condition'})
            if condition_uuid:
                await _create_relationship(
                    person_uuid, "Person", condition_uuid, "Condition", "HAS_CONDITION",
                    {**source_props, "rating": str(percentage), "effective_date": rating.get("effective_date", "")})

    # B: Process combined rating
    combined = data.get("combined_rating")
    if combined:
        combined_uuid = await graph_store.create_node("MedicalResult", {
            "test_name": "Combined VA Disability Rating",
            "value": str(combined) + "%",
            "unit": "percent",
            "effective_date": data.get("combined_rating_effective_date", ""),
            "flag": "permanent_and_total" if data.get("permanent_and_total") else "",
            "confidence": 1.0,
        })
        await _create_relationship(
            doc_node_id, "Document", combined_uuid, "MedicalResult", "CONTAINS_RESULT", source_props)
        if person_uuid:
            await _create_relationship(
                person_uuid, "Person", combined_uuid, "MedicalResult", "RATED_AT",
                {**source_props, "combined_rating": str(combined),
                 "effective_date": data.get("combined_rating_effective_date", "")})

    # B: Process conditions
    for cond in (data.get("conditions") or []):
        name = cond.get("name") if isinstance(cond, dict) else cond
        if not name or not _is_valid_entity_name(name):
            continue
        condition_uuid = await _resolve_entity(name, "Condition", doc_id, allowed_types={'Condition'})
        if condition_uuid:
            await _create_relationship(
                doc_node_id, "Document", condition_uuid, "Condition", "DIAGNOSED_WITH", source_props)
            if person_uuid:
                status = cond.get("status", "") if isinstance(cond, dict) else ""
                await _create_relationship(
                    person_uuid, "Person", condition_uuid, "Condition", "HAS_CONDITION",
                    {**source_props, "status": status})

    # B: Process benefits (DEA, CHAMPVA, etc.)
    for benefit in (data.get("benefits") or []):
        benefit_type = benefit.get("benefit_type", "")
        if not benefit_type:
            continue
        benefit_uuid = await graph_store.create_node("InsurancePolicy", {
            "policy_number": "",
            "provider": "Department of Veterans Affairs",
            "coverage_type": benefit_type,
            "effective_date": benefit.get("effective_date", ""),
            "eligibility": benefit.get("eligibility", ""),
        })
        await _create_relationship(
            doc_node_id, "Document", benefit_uuid, "InsurancePolicy", "CONTAINS_RESULT", source_props)

    for org in (data.get("organizations") or []):
        name = _coerce_text(org.get("name") if isinstance(org, dict) else org)
        if not name or not _is_valid_entity_name(name):
            continue
        org_uuid = await _resolve_entity(name, "Organization", doc_id)
        if org_uuid:
            await _create_relationship(
                doc_node_id, "Document", org_uuid, "Organization", "MENTIONS", source_props)

    for loc in (data.get("locations") or []):
        name = _coerce_text(loc.get("name") if isinstance(loc, dict) else loc)
        if not name or not _is_valid_entity_name(name):
            continue
        if _is_full_address(name):
            continue
        loc_uuid = await _resolve_entity(name, "Location", doc_id, allowed_types={'Location', 'Address', 'Organization'})
        if loc_uuid:
            context = _coerce_text(loc.get("context", "mentioned")) if isinstance(loc, dict) else "mentioned"
            rel_type = "DEPLOYED_TO" if "deploy" in context.lower() else "STATIONED_AT" if "station" in context.lower() else "LOCATED_AT"
            await _create_relationship(
                doc_node_id, "Document", loc_uuid, "Location", rel_type, source_props)


async def _process_generic(doc_id, doc_node_id, data, source_props):
    """Process generic documents - dates stored as properties, not separate nodes."""
    # If 3-pass extraction provided all_entities, skip legacy people/org processing
    # (already handled by _process_enhanced_entities)
    if data.get("all_entities"):
        return
    
    for person in (data.get("people") or []):
        name = _coerce_text(person.get("name") if isinstance(person, dict) else person)
        if not name or not _is_valid_entity_name(name):
            continue
        if isinstance(person, dict):
            confidence = float(person.get("confidence", 1.0))
            if confidence < CONFIDENCE_THRESHOLD:
                logger.debug(f"Skipping low-confidence person: {name} (conf={confidence})")
                continue
        role = person.get("role", "") if isinstance(person, dict) else ""
        person_uuid = await _resolve_entity(name, "Person", doc_id)
        if person_uuid:
            await _create_relationship(
                doc_node_id, "Document", person_uuid, "Person", "MENTIONS", source_props)

    for org in (data.get("organizations") or []):
        name = _coerce_text(org.get("name") if isinstance(org, dict) else org)
        if not name or not _is_valid_entity_name(name):
            continue
        if isinstance(org, dict):
            confidence = float(org.get("confidence", 1.0))
            if confidence < CONFIDENCE_THRESHOLD:
                logger.debug(f"Skipping low-confidence org: {name} (conf={confidence})")
                continue
        org_type = org.get("type", "") if isinstance(org, dict) else ""
        org_uuid = await _resolve_entity(name, "Organization", doc_id)
        if org_uuid:
            await _create_relationship(
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


async def _process_document_batch(docs, force_ids, progress_callback, cancel_event):
    semaphore = asyncio.Semaphore(max(1, settings.max_concurrent_docs))

    async def process(doc):
        async with semaphore:
            if cancel_event.is_set():
                result = {"doc_id": doc["id"], "status": "skipped", "reason": "cancelled"}
            else:
                if progress_callback:
                    progress_callback("current", {"title": doc.get("title", f"Document {doc['id']}")})
                try:
                    result = await process_document(doc, force=doc["id"] in force_ids)
                except Exception as exc:
                    logger.exception("Unexpected error processing document %s", doc["id"])
                    result = {"doc_id": doc["id"], "status": "error", "error": str(exc)}
            if progress_callback:
                progress_callback("result", result)
            return result

    # Cooperative cancellation stops new work; already admitted writers drain.
    # Also drain when our owning task is cancelled directly during shutdown.
    tasks = [asyncio.create_task(process(doc)) for doc in docs]
    pending = asyncio.gather(*tasks)
    try:
        return await asyncio.shield(pending)
    except asyncio.CancelledError:
        cancel_event.set()
        await pending
        raise


async def _ingest_documents(*, force=False, progress_callback=None, cancel_event=None):
    cancel_event = cancel_event or asyncio.Event()
    start_time = time.time()
    scan_started_at = datetime.now(timezone.utc)
    last_sync = await embeddings_store.get_last_sync()
    changed = [] if force else await paperless_client.get_all_documents(modified_after=last_sync)
    # This reconciliation was already required for deletion detection. Use the
    # same complete snapshot to discover interrupted replacements across stores.
    all_docs = await paperless_client.get_all_documents()
    skip_tag_ids = await paperless_client.get_skip_tag_ids()
    indexable, held = paperless_client.partition_indexable_documents(all_docs, skip_tag_ids)
    current = {int(doc["id"]): doc for doc in indexable}
    graph_ids = await graph_store.get_all_document_ids()
    embedding_ids = await embeddings_store.get_document_embedding_ids()
    hash_ids = await embeddings_store.get_document_hash_ids()
    complete_ids = graph_ids & embedding_ids & hash_ids
    missing_ids = set(current) - complete_ids
    fingerprints = await embeddings_store.get_ingestion_fingerprints()
    changed_index_ids = {doc_id for doc_id, doc in current.items()
                         if fingerprints.get(doc_id) != PaperlessClient.ingestion_fingerprint(doc)}
    changed_ids = {int(doc["id"]) for doc in changed}
    selected_ids = set(current) if force else (changed_ids | missing_ids | changed_index_ids) & set(current)
    docs = [current[doc_id] for doc_id in sorted(selected_ids)]
    force_ids = selected_ids if force else missing_ids
    deleted_ids = (graph_ids | embedding_ids | hash_ids) - set(current)

    if progress_callback:
        progress_callback("init", {"total_docs": len(docs) + len(deleted_ids)})
    results = await _process_document_batch(docs, force_ids, progress_callback, cancel_event)
    deleted_count = 0
    for doc_id in sorted(deleted_ids):
        if cancel_event.is_set():
            result = {"doc_id": doc_id, "status": "skipped", "reason": "cancelled"}
        else:
            if progress_callback:
                progress_callback("current", {"title": f"Removing stale document #{doc_id}"})
            try:
                await _purge_document_index(doc_id)
                deleted_count += 1
                result = {"doc_id": doc_id, "status": "processed", "reason": "removed stale document"}
            except Exception as exc:
                logger.exception("Failed to purge stale document %s", doc_id)
                result = {"doc_id": doc_id, "status": "error", "error": str(exc)}
        results.append(result)
        if progress_callback:
            progress_callback("result", result)

    errors = sum(result["status"] == "error" for result in results)
    processed = sum(result["status"] == "processed" for result in results) - deleted_count
    postprocess_errors = []
    if force and processed and not cancel_event.is_set():
        try:
            await embeddings_store.create_vector_indexes()
            if not cancel_event.is_set():
                resolution = await entity_resolver.resolve_all_entities()
                if not isinstance(resolution, dict):
                    raise ValueError("Entity resolution returned no completion report")
                postprocess_errors.extend(str(error) for error in resolution.get("errors", []))
        except Exception as exc:
            logger.exception("Reindex post-processing failed")
            postprocess_errors.append(str(exc))
    errors += len(postprocess_errors)
    cancelled = cancel_event.is_set()
    checkpoint_advanced = not cancelled and errors == 0
    if checkpoint_advanced:
        # A completion-time watermark loses changes made while scanning.
        await embeddings_store.set_last_sync(scan_started_at)
    elapsed = time.time() - start_time
    skipped = sum(result["status"] == "skipped" for result in results)
    entity_count = sum(result.get("entities_extracted", 0) for result in results if result["status"] == "processed")
    await asyncio.to_thread(invalidate_on_sync)
    return {
        "total": len(docs) + len(deleted_ids),
        "processed": processed,
        "skipped": skipped,
        "held": len(held),
        "errors": errors,
        "deleted": deleted_count,
        "cancelled": cancelled,
        "status": "cancelled" if cancelled else ("failed" if errors else "completed"),
        "checkpoint_advanced": checkpoint_advanced,
        "scan_started_at": scan_started_at.isoformat(),
        "previous_checkpoint": last_sync.isoformat() if last_sync else None,
        "elapsed_seconds": round(elapsed, 1),
        "docs_per_minute": round(processed / (elapsed / 60), 1) if elapsed > 0 else 0,
        "avg_entities_per_doc": round(entity_count / processed, 1) if processed else 0,
        "postprocess_errors": postprocess_errors,
        "results": results,
    }


async def sync_documents(progress_callback=None, cancel_event=None):
    """Incremental scan with complete-store reconciliation and retryable markers."""
    return await _ingest_documents(progress_callback=progress_callback, cancel_event=cancel_event)


async def reindex_all(progress_callback=None, cancel_event=None):
    """Prepare and replace every source document without clearing usable data."""
    return await _ingest_documents(force=True, progress_callback=progress_callback, cancel_event=cancel_event)


async def reindex_document(doc_id: int):
    """Force preparation before replacing a single document's usable index."""
    doc = await paperless_client.get_document(doc_id)
    return await process_document(doc, force=True)
