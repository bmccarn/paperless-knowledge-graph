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

        # Step 2: Extract
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

        if entity_count == 0 and not is_fallback:
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
        await _process_extraction(doc_id, doc_node_id, doc_type, extracted)

        # Step 5b: Process implied relationships
        await _process_implied_relationships(doc_id, extracted)

        # Step 6: Store embeddings — chunk content for granular retrieval
        chunks = chunk_text(content, chunk_size=4000, overlap=800)
        for i, chunk in enumerate(chunks):
            await embeddings_store.store_document_embedding(
                doc_id, chunk, chunk_index=i, title=title, doc_type=doc_type
            )
        logger.info(f"Doc {doc_id}: stored {len(chunks)} embedding chunks")

        # Step 6b: Store entity embeddings for resolved entities
        await _store_entity_embeddings(doc_id, extracted)

        # Step 7: Update hash
        await embeddings_store.set_doc_hash(doc_id, content_hash)

        return {"doc_id": doc_id, "status": "processed", "doc_type": doc_type,
                "entities_extracted": entity_count,
                "chunks": len(chunks),
                "confidence": extraction_confidence,
}

    except Exception as e:
        logger.error(f"Failed to process doc {doc_id}: {e}", exc_info=True)
        return {"doc_id": doc_id, "status": "error", "error": str(e)}


async def _store_entity_embeddings(doc_id: int, extracted: dict):
    """Store embeddings for ALL entity types from the 3-pass extraction."""
    try:
        all_entities = extracted.get("all_entities", [])
        if not all_entities:
            # Backward compat: build from people/organizations
            for person in (extracted.get("people") or []):
                name = person.get("name") if isinstance(person, dict) else person
                if name:
                    all_entities.append({"name": name, "type": "Person", "description": person.get("role", "") if isinstance(person, dict) else ""})
            for org in (extracted.get("organizations") or []):
                name = org.get("name") if isinstance(org, dict) else org
                if name:
                    all_entities.append({"name": name, "type": "Organization", "description": org.get("type", "") if isinstance(org, dict) else ""})

        for entity in all_entities:
            name = entity.get("name", "")
            etype = entity.get("type", "Person").strip().title()
            desc = entity.get("description", "")
            if not name or not _is_valid_entity_name(name):
                continue
            results = await graph_store.search_nodes(name, node_type=etype, limit=1)
            if not results:
                results = await graph_store.search_nodes(name, limit=1)
            if results:
                uuid = results[0].get("properties", {}).get("uuid", "")
                if uuid:
                    emb_content = f"{name} | {etype.lower()}"
                    if desc:
                        emb_content += f" | {desc}"
                    await embeddings_store.store_entity_embedding(
                        uuid, name, entity_type=etype, content=emb_content
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


VALID_ENTITY_TYPES = {"Person", "Organization", "Location", "System", "Product", "Document", "Event"}

# Blocklist of generic terms that should not become entity nodes
BLOCKED_ENTITY_NAMES = {
    "subject matter expert", "candidates", "applicant", "customer", "client",
    "employee", "employer", "vendor", "buyer", "seller", "user", "admin",
    "recipient", "sender", "owner", "tenant", "landlord", "borrower", "lender",
    "insured", "beneficiary", "claimant", "plaintiff", "defendant",
    "taxpayer", "filer", "spouse", "dependent", "subscriber", "member",
    "patient", "provider", "physician", "doctor", "nurse",
    "n/a", "unknown", "none", "null", "other", "various", "multiple",
    "not specified", "not applicable", "see above", "see below",
}


def _is_valid_entity_name(name: str) -> bool:
    """Validate entity name - reject generic terms and junk."""
    if not name or len(name.strip()) < 2:
        return False
    name_lower = name.strip().lower()
    if name_lower in BLOCKED_ENTITY_NAMES:
        return False
    # Reject single common words (not proper nouns)
    if len(name_lower.split()) == 1 and name_lower == name.strip():
        # Single word, all lowercase = probably not a proper noun
        if not any(c.isupper() for c in name.strip()):
            return False
    # Reject very short names
    if len(name.strip()) < 3:
        return False
    return True


async def _resolve_entity(name: str, entity_type: str, doc_id: int) -> str:
    """Route entity resolution based on type."""
    if not _is_valid_entity_name(name):
        logger.debug(f"Skipping invalid entity name: '{name}'")
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


async def _process_enhanced_entities(doc_id: int, doc_node_id: str, extracted: dict):
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
                
            # Resolve the entity and create document relationships
            entity_uuid = await _resolve_entity(name, entity_type, doc_id)
            if entity_uuid:
                # Create relationship from document to entity
                await graph_store.create_relationship(
                    doc_node_id, "Document", entity_uuid, entity_type, 
                    "MENTIONS", {**source_props, "confidence": confidence}
                )
                logger.debug(f"Created entity relationship: Document {doc_id} -[MENTIONS]-> {entity_type} {name}")
                
        except Exception as e:
            logger.warning(f"Failed to process enhanced entity {entity}: {e}")


async def _process_extraction(doc_id: int, doc_node_id: str, doc_type: str, extracted: dict):
    """Create graph nodes and relationships from extracted data."""
    source_props = {"source_doc": doc_id}

    # Process enhanced entities from 3-pass extraction if available
    await _process_enhanced_entities(doc_id, doc_node_id, extracted)

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
    elif doc_type == "property_home":
        await _process_property(doc_id, doc_node_id, extracted, source_props)
    else:
        await _process_generic(doc_id, doc_node_id, extracted, source_props)
async def _process_medical(doc_id, doc_node_id, data, source_props):
    patient = data.get("patient_name")
    if patient:
        person_uuid = await entity_resolver.resolve_person(patient, doc_id, role="patient")
        if person_uuid:
            await graph_store.create_relationship(
                doc_node_id, "Document", person_uuid, "Person", "PATIENT_OF", source_props)

    provider = data.get("provider")
    if provider:
        org_uuid = await entity_resolver.resolve_organization(provider, doc_id, org_type="medical")
        if org_uuid:
            await graph_store.create_relationship(
                doc_node_id, "Document", org_uuid, "Organization", "PROVIDER_FOR", source_props)

    physician = data.get("ordering_physician")
    if physician:
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


async def _process_financial(doc_id, doc_node_id, data, source_props):
    vendor = data.get("vendor")
    if vendor:
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
    for party in (data.get("parties") or []):
        name = party.get("name")
        if not name:
            continue
        org_uuid = await entity_resolver.resolve_organization(name, doc_id, org_type="legal")
        if org_uuid:
            role = party.get("role", "party")
            rel_type = "CONTRACTED_WITH" if "sign" in role.lower() or "party" in role.lower() else "PARTY_TO"
            await graph_store.create_relationship(
                doc_node_id, "Document", org_uuid, "Organization", rel_type, source_props)

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
    if provider:
        org_uuid = await entity_resolver.resolve_organization(provider, doc_id, org_type="insurance")
        if org_uuid:
            await graph_store.create_relationship(
                doc_node_id, "Document", org_uuid, "Organization", "PROVIDER_FOR", source_props)

    policyholder = data.get("policyholder")
    if policyholder:
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
    if filer:
        person_uuid = await entity_resolver.resolve_person(filer, doc_id, role="filer")
        if person_uuid:
            await graph_store.create_relationship(
                doc_node_id, "Document", person_uuid, "Person", "AUTHORED_BY", source_props)

    preparer = data.get("preparer")
    if preparer:
        prep_uuid = await entity_resolver.resolve_person(preparer, doc_id, role="tax_preparer")
        if prep_uuid:
            await graph_store.create_relationship(
                doc_node_id, "Document", prep_uuid, "Person", "MENTIONS", source_props)

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
    if address:
        addr_uuid = await graph_store.create_node("Address", {
            "full_address": address,
        })
        await graph_store.create_relationship(
            doc_node_id, "Document", addr_uuid, "Address", "LOCATED_AT", source_props)

    for party in (data.get("parties") or []):
        name = party.get("name")
        if not name:
            continue
        person_uuid = await entity_resolver.resolve_person(name, doc_id, role=party.get("role"))
        if person_uuid:
            await graph_store.create_relationship(
                doc_node_id, "Document", person_uuid, "Person", "MENTIONS", source_props)


async def _process_generic(doc_id, doc_node_id, data, source_props):
    for person in (data.get("people") or []):
        name = person.get("name") if isinstance(person, dict) else person
        if not name:
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
        if not name:
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

    # Dates are stored as properties on the document node, not as separate nodes


def _extract_date(doc: dict, extracted: dict) -> str:
    for key in ("date", "effective_date"):
        if extracted.get(key):
            return str(extracted[key])
    created = doc.get("created")
    if created:
        return str(created)[:10]
    return ""


def _count_entities(extracted: dict) -> int:
    count = 0
    for key, val in extracted.items():
        if key in ("confidence", "fallback_extraction", "implied_relationships"):
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
