import asyncio
import logging
from datetime import datetime, timezone

from google import genai

from app.config import settings
from app.graph import graph_store

logger = logging.getLogger(__name__)

ENTITY_LABELS = [
    "Person", "Organization", "Address", "FinancialItem",
    "MedicalResult", "Contract", "InsurancePolicy", "DateEvent",
]

TYPE_PROMPTS = {
    "Person": "For this person, include biographical info, roles, relationships to other people and organizations, and all document mentions with dates. Note any identifiers (SSN fragments, account numbers, etc.).",
    "Organization": "For this organization, describe what they do, their relationship to the document owner, services provided, account numbers or policy numbers, and all document interactions.",
    "MedicalResult": "For this medical result, include the test name, value, units, reference range, whether it was flagged, the date it was taken, the provider, and any clinical significance.",
    "FinancialItem": "For this financial item, include amounts, dates, payment status, vendor/source, invoice numbers, and any recurring patterns.",
    "Contract": "For this contract, include parties involved, effective/expiration dates, key terms, renewal information, and obligations.",
    "InsurancePolicy": "For this insurance policy, include the provider, policy number, coverage type, premium, effective dates, and covered individuals.",
    "Address": "For this address, describe what it represents (home, business, property), who is associated with it, and any documents referencing it.",
    "DateEvent": "For this date/event, describe what happened, who was involved, and its significance.",
}


def _build_prompt(entity_name: str, entity_type: str, properties: dict,
                  relationships: list[dict], documents: list[dict]) -> str:
    type_guidance = TYPE_PROMPTS.get(entity_type, "Be comprehensive and factual.")

    props_text = "\n".join(f"  - {k}: {v}" for k, v in properties.items()
                           if k not in ("uuid", "description", "description_updated_at") and v)

    rels_text = ""
    for r in relationships:
        neighbor_name = r.get("neighbor_name") or r.get("neighbor_title") or "unknown"
        neighbor_type = r.get("neighbor_type", "")
        rel_type = r.get("rel_type", "")
        direction = r.get("direction", "")
        rel_desc = r.get("rel_description", "")
        arrow = "->" if direction == "out" else "<-"
        line = f"  - {arrow} [{rel_type}] {neighbor_type}: {neighbor_name}"
        if rel_desc:
            line += f" ({rel_desc})"
        rels_text += line + "\n"

    docs_text = ""
    for d in documents:
        docs_text += f"  - \"{d.get('title', 'Untitled')}\" (type: {d.get('doc_type', 'unknown')}, date: {d.get('date', 'unknown')})\n"

    return f"""Given the following information about "{entity_name}" ({entity_type}) extracted from personal documents, write a comprehensive narrative description that consolidates all known facts.

{type_guidance}

**Entity Properties:**
{props_text or "  (none)"}

**Relationships:**
{rels_text or "  (none)"}

**Source Documents:**
{docs_text or "  (none)"}

Write a well-organized, factual dossier-style description. Use prose paragraphs, not bullet points. Include all dates, identifiers, and specifics. If information is limited, state what is known concisely. Do not speculate or add information not present above."""


async def _get_entity_context(session, node_uuid: str) -> dict:
    """Gather all context for an entity from Neo4j."""
    result = await session.run(
        """
        MATCH (n) WHERE n.uuid = $uuid
        OPTIONAL MATCH (n)-[r]-(m)
        RETURN labels(n) AS labels, properties(n) AS props,
               collect(DISTINCT {
                   rel_type: type(r),
                   direction: CASE WHEN startNode(r) = n THEN 'out' ELSE 'in' END,
                   rel_props: properties(r),
                   neighbor_labels: labels(m),
                   neighbor_name: COALESCE(m.name, m.title, m.test_name, m.full_address, m.type, ''),
                   neighbor_title: m.title,
                   neighbor_type: HEAD([l IN labels(m) WHERE l <> 'Document'])
               }) AS relationships
        """,
        uuid=node_uuid,
    )
    record = await result.single()
    if not record:
        return None

    # Separate document relationships from entity relationships
    docs = []
    rels = []
    for r in record["relationships"]:
        if r["rel_type"] is None:
            continue
        if "Document" in (r.get("neighbor_labels") or []):
            docs.append({
                "title": r.get("neighbor_name") or r.get("neighbor_title") or "",
                "doc_type": (r.get("rel_props") or {}).get("doc_type", ""),
                "date": "",
            })
        else:
            rels.append({
                "rel_type": r["rel_type"],
                "direction": r["direction"],
                "neighbor_name": r.get("neighbor_name", ""),
                "neighbor_type": r.get("neighbor_type", ""),
                "rel_description": (r.get("rel_props") or {}).get("description", ""),
            })

    # Get document details separately for better info
    doc_result = await session.run(
        """
        MATCH (n {uuid: $uuid})-[r]-(d:Document)
        RETURN d.title AS title, d.doc_type AS doc_type, d.date AS date, d.paperless_id AS pid
        """,
        uuid=node_uuid,
    )
    docs = [dict(rec) async for rec in doc_result]

    props = record["props"]
    labels = [l for l in record["labels"] if l not in ("_Entity",)]
    entity_type = labels[0] if labels else "Unknown"
    entity_name = props.get("name") or props.get("test_name") or props.get("title") or props.get("full_address") or props.get("type") or "Unknown"

    return {
        "uuid": node_uuid,
        "name": entity_name,
        "type": entity_type,
        "properties": props,
        "relationships": rels,
        "documents": docs,
    }


async def summarize_entity(node_uuid: str) -> dict:
    """Generate and store a description for a single entity."""
    async with graph_store.driver.session() as session:
        ctx = await _get_entity_context(session, node_uuid)
        if not ctx:
            return {"uuid": node_uuid, "status": "not_found"}

        prompt = _build_prompt(ctx["name"], ctx["type"], ctx["properties"],
                               ctx["relationships"], ctx["documents"])

        client = genai.Client(api_key=settings.gemini_api_key)
        response = client.models.generate_content(
            model=settings.gemini_model,
            contents=prompt,
        )
        description = response.text.strip()

        now = datetime.now(timezone.utc).isoformat()
        await session.run(
            """
            MATCH (n {uuid: $uuid})
            SET n.description = $desc, n.description_updated_at = $ts
            """,
            uuid=node_uuid, desc=description, ts=now,
        )

        logger.info(f"Summarized {ctx['type']} '{ctx['name']}' ({node_uuid[:8]}...)")
        return {
            "uuid": node_uuid,
            "name": ctx["name"],
            "type": ctx["type"],
            "status": "summarized",
            "description_length": len(description),
        }


async def summarize_all_entities(force: bool = False) -> dict:
    """Summarize all entities, optionally only those needing updates."""
    async with graph_store.driver.session() as session:
        # Get all non-Document entities
        if force:
            query = """
                MATCH (n)
                WHERE NOT n:Document AND n.uuid IS NOT NULL
                RETURN n.uuid AS uuid, labels(n) AS labels, n.name AS name
            """
        else:
            # Only entities without descriptions or with new document mentions
            query = """
                MATCH (n)
                WHERE NOT n:Document AND n.uuid IS NOT NULL
                  AND (n.description IS NULL
                       OR EXISTS {
                           MATCH (n)-[r]-(d:Document)
                           WHERE d.processed_at > datetime(n.description_updated_at)
                       })
                RETURN n.uuid AS uuid, labels(n) AS labels, n.name AS name
            """
        result = await session.run(query)
        entities = [dict(r) async for r in result]

    total = len(entities)
    logger.info(f"Summarizing {total} entities (force={force})")

    results = []
    for i, entity in enumerate(entities):
        try:
            r = await summarize_entity(entity["uuid"])
            results.append(r)
            logger.info(f"Progress: {i+1}/{total}")
        except Exception as e:
            logger.error(f"Failed to summarize {entity['uuid']}: {e}")
            results.append({"uuid": entity["uuid"], "status": "error", "error": str(e)})

        # Rate limiting
        if i < total - 1:
            await asyncio.sleep(1.5)

    succeeded = sum(1 for r in results if r.get("status") == "summarized")
    failed = sum(1 for r in results if r.get("status") == "error")
    logger.info(f"Summarization complete: {succeeded} succeeded, {failed} failed out of {total}")

    return {
        "total": total,
        "summarized": succeeded,
        "failed": failed,
        "results": results,
    }
