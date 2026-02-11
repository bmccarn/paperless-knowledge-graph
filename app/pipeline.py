import logging
import time
from datetime import datetime, timezone

from app.paperless import paperless_client, PaperlessClient
from app.classifier import classifier
from app.extractor import extractor
from app.entity_resolver import entity_resolver
from app.graph import graph_store
from app.embeddings import embeddings_store

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
        is_fallback = extracted.get("fallback_extraction", False)
        if is_fallback:
            logger.info(f"Doc {doc_id} used fallback extraction (confidence={extraction_confidence})")

        entity_count = _count_entities(extracted)
        logger.info(f"Doc {doc_id} extracted {entity_count} fields (confidence={extraction_confidence})")

        # If primary extraction returned nothing and no fallback was tried, log it
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

        # Step 6: Store embeddings
        # Split content into chunks for better retrieval
        chunks = _chunk_content(content, max_chars=2000)
        for i, chunk in enumerate(chunks):
            await embeddings_store.store_document_embedding(doc_id, chunk, chunk_index=i)

        # Step 7: Update hash
        await embeddings_store.set_doc_hash(doc_id, content_hash)

        return {"doc_id": doc_id, "status": "processed", "doc_type": doc_type,
                "entities_extracted": entity_count,
                "confidence": extraction_confidence,
                "fallback": is_fallback}

    except Exception as e:
        logger.error(f"Failed to process doc {doc_id}: {e}", exc_info=True)
        return {"doc_id": doc_id, "status": "error", "error": str(e)}


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

            # Resolve entities
            if from_type == "Organization":
                from_uuid = await entity_resolver.resolve_organization(from_name, doc_id)
            else:
                from_uuid = await entity_resolver.resolve_person(from_name, doc_id)

            if to_type == "Organization":
                to_uuid = await entity_resolver.resolve_organization(to_name, doc_id)
            else:
                to_uuid = await entity_resolver.resolve_person(to_name, doc_id)

            if from_uuid and to_uuid:
                props = {**source_props, "confidence": confidence}
                await graph_store.create_relationship(
                    from_uuid, from_type, to_uuid, to_type,
                    rel_type, props
                )
                logger.debug(f"Created implied relationship: {from_name} -[{rel_type}]-> {to_name}")

        except Exception as e:
            logger.warning(f"Failed to create implied relationship: {e}")


async def _process_extraction(doc_id: int, doc_node_id: str, doc_type: str, extracted: dict):
    """Create graph nodes and relationships from extracted data."""
    source_props = {"source_doc": doc_id}

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
    # Patient
    patient = data.get("patient_name")
    if patient:
        person_uuid = await entity_resolver.resolve_person(patient, doc_id, role="patient")
        if person_uuid:
            await graph_store.create_relationship(
                str(doc_id), "Document", person_uuid, "Person", "PATIENT_OF", source_props)

    # Provider
    provider = data.get("provider")
    if provider:
        org_uuid = await entity_resolver.resolve_organization(provider, doc_id, org_type="medical")
        if org_uuid:
            await graph_store.create_relationship(
                str(doc_id), "Document", org_uuid, "Organization", "PROVIDER_FOR", source_props)

    # Ordering physician
    physician = data.get("ordering_physician")
    if physician:
        phys_uuid = await entity_resolver.resolve_person(physician, doc_id, role="physician")
        if phys_uuid:
            await graph_store.create_relationship(
                str(doc_id), "Document", phys_uuid, "Person", "AUTHORED_BY", source_props)

    # Test results
    for test in (data.get("tests") or []):
        if not test.get("name"):
            continue
        # Check confidence on individual test results
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
            str(doc_id), "Document", result_uuid, "MedicalResult", "CONTAINS_RESULT", source_props)


async def _process_financial(doc_id, doc_node_id, data, source_props):
    # Vendor
    vendor = data.get("vendor")
    if vendor:
        org_uuid = await entity_resolver.resolve_organization(vendor, doc_id, org_type="financial")
        if org_uuid:
            await graph_store.create_relationship(
                str(doc_id), "Document", org_uuid, "Organization", "INVOICED_BY", source_props)

    # Financial item
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
            str(doc_id), "Document", fi_uuid, "FinancialItem", "CONTAINS_RESULT", source_props)


async def _process_contract(doc_id, doc_node_id, data, source_props):
    # Parties
    for party in (data.get("parties") or []):
        name = party.get("name")
        if not name:
            continue
        org_uuid = await entity_resolver.resolve_organization(name, doc_id, org_type="legal")
        if org_uuid:
            await graph_store.create_relationship(
                str(doc_id), "Document", org_uuid, "Organization", "MENTIONS", source_props)

    # Contract node
    contract_uuid = await graph_store.create_node("Contract", {
        "type": data.get("contract_type", "") or "",
        "effective_date": data.get("effective_date", "") or "",
        "expiration_date": data.get("expiration_date", "") or "",
        "terms_summary": data.get("terms_summary", "") or "",
        "renewal_info": data.get("renewal_info", "") or "",
    })
    await graph_store.create_relationship(
        str(doc_id), "Document", contract_uuid, "Contract", "CONTAINS_RESULT", source_props)


async def _process_insurance(doc_id, doc_node_id, data, source_props):
    provider = data.get("provider")
    if provider:
        org_uuid = await entity_resolver.resolve_organization(provider, doc_id, org_type="insurance")
        if org_uuid:
            await graph_store.create_relationship(
                str(doc_id), "Document", org_uuid, "Organization", "PROVIDER_FOR", source_props)

    policyholder = data.get("policyholder")
    if policyholder:
        person_uuid = await entity_resolver.resolve_person(policyholder, doc_id, role="policyholder")
        if person_uuid:
            await graph_store.create_relationship(
                str(doc_id), "Document", person_uuid, "Person", "COVERS", source_props)

    policy_uuid = await graph_store.create_node("InsurancePolicy", {
        "policy_number": data.get("policy_number", "") or "",
        "provider": data.get("provider", "") or "",
        "coverage_type": data.get("coverage_type", "") or "",
        "premium": str(data.get("premium", "")) if data.get("premium") else "",
        "effective_date": data.get("effective_date", "") or "",
        "expiration_date": data.get("expiration_date", "") or "",
    })
    await graph_store.create_relationship(
        str(doc_id), "Document", policy_uuid, "InsurancePolicy", "CONTAINS_RESULT", source_props)


async def _process_tax(doc_id, doc_node_id, data, source_props):
    filer = data.get("filer_name")
    if filer:
        person_uuid = await entity_resolver.resolve_person(filer, doc_id, role="filer")
        if person_uuid:
            await graph_store.create_relationship(
                str(doc_id), "Document", person_uuid, "Person", "AUTHORED_BY", source_props)

    preparer = data.get("preparer")
    if preparer:
        prep_uuid = await entity_resolver.resolve_person(preparer, doc_id, role="tax_preparer")
        if prep_uuid:
            await graph_store.create_relationship(
                str(doc_id), "Document", prep_uuid, "Person", "MENTIONS", source_props)

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
        str(doc_id), "Document", fi_uuid, "FinancialItem", "CONTAINS_RESULT", source_props)


async def _process_property(doc_id, doc_node_id, data, source_props):
    # Address
    address = data.get("property_address")
    if address:
        addr_uuid = await graph_store.create_node("Address", {
            "full_address": address,
        })
        await graph_store.create_relationship(
            str(doc_id), "Document", addr_uuid, "Address", "LOCATED_AT", source_props)

    # Parties
    for party in (data.get("parties") or []):
        name = party.get("name")
        if not name:
            continue
        person_uuid = await entity_resolver.resolve_person(name, doc_id, role=party.get("role"))
        if person_uuid:
            await graph_store.create_relationship(
                str(doc_id), "Document", person_uuid, "Person", "MENTIONS", source_props)


async def _process_generic(doc_id, doc_node_id, data, source_props):
    # People
    for person in (data.get("people") or []):
        name = person.get("name") if isinstance(person, dict) else person
        if not name:
            continue
        # Check confidence if available
        if isinstance(person, dict):
            confidence = float(person.get("confidence", 1.0))
            if confidence < CONFIDENCE_THRESHOLD:
                logger.debug(f"Skipping low-confidence person: {name} (conf={confidence})")
                continue
        role = person.get("role", "") if isinstance(person, dict) else ""
        person_uuid = await entity_resolver.resolve_person(name, doc_id, role=role)
        if person_uuid:
            await graph_store.create_relationship(
                str(doc_id), "Document", person_uuid, "Person", "MENTIONS", source_props)

    # Organizations
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
                str(doc_id), "Document", org_uuid, "Organization", "MENTIONS", source_props)

    # Date events
    for date_info in (data.get("dates") or []):
        if isinstance(date_info, dict):
            if not date_info.get("date"):
                continue
            de_uuid = await graph_store.create_node("DateEvent", {
                "date": date_info["date"],
                "description": date_info.get("description", "") or "",
                "recurring": False,
            })
        elif isinstance(date_info, str):
            de_uuid = await graph_store.create_node("DateEvent", {
                "date": date_info,
                "description": "",
                "recurring": False,
            })
        else:
            continue
        await graph_store.create_relationship(
            str(doc_id), "Document", de_uuid, "DateEvent", "MENTIONS", source_props)


def _extract_date(doc: dict, extracted: dict) -> str:
    """Extract the best date from doc metadata or extraction."""
    for key in ("date", "effective_date"):
        if extracted.get(key):
            return str(extracted[key])
    created = doc.get("created")
    if created:
        return str(created)[:10]
    return ""


def _chunk_content(content: str, max_chars: int = 2000) -> list[str]:
    """Split content into chunks."""
    if not content:
        return []
    chunks = []
    for i in range(0, len(content), max_chars):
        chunk = content[i:i + max_chars]
        if chunk.strip():
            chunks.append(chunk)
    return chunks or [content[:max_chars]]


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


async def sync_documents():
    """Incremental sync - process new/modified documents."""
    last_sync = await embeddings_store.get_last_sync()
    logger.info(f"Starting sync (last sync: {last_sync})")

    start_time = time.time()
    docs = await paperless_client.get_all_documents(modified_after=last_sync)
    logger.info(f"Found {len(docs)} documents to check")

    results = []
    for doc in docs:
        result = await process_document(doc)
        results.append(result)

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


async def reindex_all():
    """Full reindex - clear everything and reprocess all documents."""
    logger.info("Starting full reindex")
    await graph_store.clear_all()
    await embeddings_store.clear_all()

    start_time = time.time()
    docs = await paperless_client.get_all_documents()
    logger.info(f"Reindexing {len(docs)} documents")

    results = []
    for doc in docs:
        result = await process_document(doc)
        results.append(result)

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

    # Force reprocess by deleting hash
    await embeddings_store.delete_doc_hash(doc_id)
    await graph_store.delete_document_graph(doc_id)
    await embeddings_store.delete_document_embeddings(doc_id)

    result = await process_document(doc)
    return result
