import logging
import json
import re
import uuid
from collections import defaultdict
from datetime import date
from typing import Any, Optional

from neo4j import AsyncGraphDatabase
from rapidfuzz import fuzz

from app.config import settings
from app.retry import retry_db
from app.relationship_support import merge_support_properties, support_records
from app.graph_fields import EVIDENCE_TYPES, HUB_LINKS, owner_rule

logger = logging.getLogger(__name__)


def _calendar_date(value: str) -> bool:
    try:
        return date.fromisoformat(value).isoformat() == value
    except ValueError:
        return False


async def source_dates(store, document_ids: list[int]) -> dict[str, str]:
    """Calendar dates for answer sources; a failed lookup omits dates, never the answer."""
    try:
        dates = await store.get_document_dates(list(dict.fromkeys(document_ids)))
        return {str(doc_id): value[:10] for doc_id, value in dates.items() if _calendar_date(value[:10])}
    except Exception:
        logger.warning("Source date lookup failed", exc_info=True)
        return {}

# Node fields a merge unions; the drift fallback restores them three-way.
MERGED_LIST_FIELDS = ("aliases", "alias_records", "identity_hints")


class MergeDrift(Exception):
    """The graph changed after a merge, so an exact restore would lose data."""


def _endpoint_key(node: dict) -> dict:
    if node.get("uuid") is not None:
        return {"uuid": node["uuid"]}
    if node.get("paperless_id") is not None:
        return {"paperless_id": node["paperless_id"]}
    raise ValueError("A relationship endpoint has no uuid or paperless_id to journal")


def _canonical(value) -> str:
    # No default=str: a value JSON cannot hold exactly must stop the merge.
    return json.dumps(value, sort_keys=True)


async def _entity_image(tx, uuids: list[str]) -> tuple[dict, dict]:
    """Nodes and incident edges as exact JSON, plus endpoint key -> elementId."""
    result = await tx.run("""MATCH (n) WHERE n.uuid IN $uuids
                             RETURN elementId(n) AS id, labels(n) AS labels, properties(n) AS props""", uuids=uuids)
    nodes = await result.data()
    result = await tx.run("""MATCH (n)-[r]-() WHERE n.uuid IN $uuids
                             WITH DISTINCT r
                             RETURN type(r) AS type, properties(r) AS props,
                                    elementId(startNode(r)) AS start_id,
                                    {uuid: startNode(r).uuid, paperless_id: startNode(r).paperless_id} AS start,
                                    elementId(endNode(r)) AS end_id,
                                    {uuid: endNode(r).uuid, paperless_id: endNode(r).paperless_id} AS end""", uuids=uuids)
    ids = {_canonical(_endpoint_key(n["props"])): n["id"] for n in nodes}
    edges = []
    for row in await result.data():
        edge = {"type": row["type"], "start": _endpoint_key(row["start"]), "end": _endpoint_key(row["end"]),
                "props": row["props"]}
        ids[_canonical(edge["start"])], ids[_canonical(edge["end"])] = row["start_id"], row["end_id"]
        edges.append(edge)
    image = json.loads(_canonical({
        "nodes": sorted(({"labels": sorted(n["labels"]), "props": n["props"]} for n in nodes),
                        key=lambda n: str(n["props"].get("uuid"))),
        "edges": sorted(edges, key=_canonical),
    }))
    return image, ids


def _merge_risk_flags(primary: dict, duplicate: dict, edges: list[dict]) -> list[str]:
    from app.entity_policy import extract_identifiers
    skip = {"uuid", "name", "source_doc_ids", *MERGED_LIST_FIELDS}
    flags = []
    if set(primary.get("source_doc_ids") or []) & set(duplicate.get("source_doc_ids") or []):
        flags.append("shared_source_document")
    pair = {primary["uuid"], duplicate["uuid"]}
    if any({edge["start"].get("uuid"), edge["end"].get("uuid")} == pair for edge in edges):
        flags.append("direct_edge")
    # identity_hints is skipped here; it has its own flag below.
    identifiers = [extract_identifiers({k: v for k, v in node.items() if k not in skip}) for node in (primary, duplicate)]
    if any(not set(values) & set(identifiers[1][key]) for key, values in identifiers[0].items() if key in identifiers[1]):
        flags.append("conflicting_identifiers")
    hints = [set(node.get("identity_hints") or []) for node in (primary, duplicate)]
    if all(hints) and hints[0] != hints[1]:
        flags.append("different_identity_hints")
    flags += [f"scalar_clash:{key}" for key in sorted(set(primary) & set(duplicate) - skip)
              if primary[key] not in (None, "") and duplicate[key] not in (None, "") and primary[key] != duplicate[key]]
    if (duplicate.get("resolution_status") in {"isolated", "ambiguous", "quarantined"} or duplicate.get("review_anchor")) \
            and any(primary.get(key) != duplicate.get(key) for key in ("resolution_status", "review_anchor")):
        flags.append("status_carried_over")
    return flags


def _three_way_fields(before: dict, after: dict, current: dict) -> dict:
    """Undo the merge's node-field changes and keep changes made after it."""
    result = dict(current)
    for key in (set(before) | set(after)) - {"uuid", "source_doc_ids"}:
        if key in MERGED_LIST_FIELDS:
            old, merged, now = before.get(key) or [], after.get(key) or [], current.get(key) or []
            # Drop what the merge added, put back what it removed, keep later edits.
            result[key] = list(dict.fromkeys([*(v for v in now if v not in merged or v in old),
                                              *(v for v in old if v not in merged and v not in now)]))
        elif after.get(key) != before.get(key) and current.get(key) == after.get(key):
            if key in before:
                result[key] = before[key]
            else:
                result.pop(key, None)
    return result


class GraphStore:
    def __init__(self):
        self.driver = None
        self.review_store = None

    async def _preserve_alias_revocations(self, nodes):
        from app.entity_decisions import alias_revocations
        rows = [row for node in nodes for row in alias_revocations(node)]
        if rows:
            from app.embeddings import embeddings_store
            await (self.review_store or embeddings_store).preserve_alias_revocations(rows)

    async def _delete_entity_embeddings(self, entity_uuids):
        # Called inside the graph transaction, so PG commits first: a failed delete
        # rolls the graph back; a failed graph commit leaves a live entity rowless.
        if entity_uuids:
            from app.embeddings import embeddings_store
            await (self.review_store or embeddings_store).delete_entity_embeddings(entity_uuids)

    async def protect_review_anchor(self, node_uuid: str):
        """Partial legacy review anchors survive document orphan cleanup."""
        async with self.driver.session() as session:
            await session.run("MATCH (n {uuid:$uuid}) SET n.review_anchor = true", uuid=node_uuid)

    async def init(self):
        self.driver = AsyncGraphDatabase.driver(
            settings.neo4j_uri,
            auth=(settings.neo4j_user, settings.neo4j_password),
        )
        # Create constraints and indexes
        async with self.driver.session() as session:
            constraints = [
                "CREATE CONSTRAINT IF NOT EXISTS FOR (d:Document) REQUIRE d.paperless_id IS UNIQUE",
                "CREATE CONSTRAINT IF NOT EXISTS FOR (p:Person) REQUIRE p.uuid IS UNIQUE",
                "CREATE CONSTRAINT IF NOT EXISTS FOR (o:Organization) REQUIRE o.uuid IS UNIQUE",
                "CREATE CONSTRAINT IF NOT EXISTS FOR (a:Address) REQUIRE a.uuid IS UNIQUE",
                "CREATE CONSTRAINT IF NOT EXISTS FOR (f:FinancialItem) REQUIRE f.uuid IS UNIQUE",
                "CREATE CONSTRAINT IF NOT EXISTS FOR (m:MedicalResult) REQUIRE m.uuid IS UNIQUE",
                "CREATE CONSTRAINT IF NOT EXISTS FOR (c:Contract) REQUIRE c.uuid IS UNIQUE",
                "CREATE CONSTRAINT IF NOT EXISTS FOR (i:InsurancePolicy) REQUIRE i.uuid IS UNIQUE",
                "CREATE CONSTRAINT IF NOT EXISTS FOR (e:DateEvent) REQUIRE e.uuid IS UNIQUE",
                "CREATE CONSTRAINT IF NOT EXISTS FOR (dr:DocumentRef) REQUIRE dr.uuid IS UNIQUE",
                "CREATE CONSTRAINT IF NOT EXISTS FOR (cond:Condition) REQUIRE cond.uuid IS UNIQUE",
                "CREATE CONSTRAINT IF NOT EXISTS FOR (loc:Location) REQUIRE loc.uuid IS UNIQUE",
                "CREATE CONSTRAINT IF NOT EXISTS FOR (sys:System) REQUIRE sys.uuid IS UNIQUE",
                "CREATE CONSTRAINT IF NOT EXISTS FOR (ev:Event) REQUIRE ev.uuid IS UNIQUE",
                "CREATE CONSTRAINT IF NOT EXISTS FOR (prod:Product) REQUIRE prod.uuid IS UNIQUE",
            ]
            for c in constraints:
                try:
                    await session.run(c)
                except Exception as e:
                    logger.warning(f"Constraint creation: {e}")
            # Indexes for name lookups
            indexes = [
                "CREATE INDEX IF NOT EXISTS FOR (p:Person) ON (p.name)",
                "CREATE INDEX IF NOT EXISTS FOR (o:Organization) ON (o.name)",
                "CREATE INDEX IF NOT EXISTS FOR (d:Document) ON (d.doc_type)",
            ]
            for idx in indexes:
                try:
                    await session.run(idx)
                except Exception as e:
                    logger.warning(f"Index creation: {e}")
        logger.info("Graph store initialized")

    async def close(self):
        if self.driver:
            await self.driver.close()

    @staticmethod
    def new_uuid() -> str:
        return str(uuid.uuid4())

    @staticmethod
    def _first_text(value: Any) -> str:
        if value is None:
            return ""
        if isinstance(value, list):
            for item in value:
                text = GraphStore._first_text(item)
                if text:
                    return text
            return ""
        return str(value)

    @staticmethod
    def _searchable_text(value: Any) -> str:
        if value is None:
            return ""
        if isinstance(value, list):
            return " ".join(GraphStore._searchable_text(item) for item in value)
        if isinstance(value, dict):
            return " ".join(GraphStore._searchable_text(item) for item in value.values())
        return str(value)

    @staticmethod
    def _coerce_text(value: Any) -> str:
        if value is None:
            return ""
        if isinstance(value, list):
            for item in value:
                text = GraphStore._coerce_text(item)
                if text:
                    return text
            return ""
        if isinstance(value, dict):
            return GraphStore._searchable_text(value).strip()
        return str(value).strip()

    @staticmethod
    def _coerce_text_list(value: Any) -> list[str]:
        if value is None:
            return []
        if isinstance(value, list):
            texts: list[str] = []
            for item in value:
                if isinstance(item, list):
                    candidates = GraphStore._coerce_text_list(item)
                else:
                    text = GraphStore._coerce_text(item)
                    candidates = [text] if text else []
                for candidate in candidates:
                    if candidate and candidate not in texts:
                        texts.append(candidate)
            return texts
        text = GraphStore._coerce_text(value)
        return [text] if text else []

    @staticmethod
    def _sanitize_identity_properties(properties: dict | None) -> dict:
        props = dict(properties or {})
        for key in ("name", "uuid"):
            if key in props:
                props[key] = GraphStore._coerce_text(props[key])
        if "aliases" in props:
            props["aliases"] = GraphStore._coerce_text_list(props["aliases"])
        return props

    @staticmethod
    def _search_terms(query: str) -> list[str]:
        return [term for term in re.findall(r"[a-z0-9]+", query.lower()) if len(term) >= 2]

    async def create_document_node(self, paperless_id: int, title: str, doc_type: str,
                                    date: str, content_hash: str,
                                    extraction_metadata: dict | None = None) -> str:
        """Create or update a Document node. Returns the paperless_id."""
        async def _op():
            async with self.driver.session() as session:
                await session.run(
                    """
                    MERGE (d:Document {paperless_id: $pid})
                    SET d.title = $title, d.doc_type = $doc_type, d.date = $date,
                        d.content_hash = $hash, d.processed_at = datetime(),
                        d.extraction_metadata = $metadata
                    """,
                    pid=paperless_id, title=title, doc_type=doc_type,
                    date=date or "", hash=content_hash,
                    metadata=json.dumps(extraction_metadata or {}, ensure_ascii=False),
                )
        await retry_db(_op, operation='create_document_node')
        return str(paperless_id)

    async def get_entities_by_type(self, label: str) -> list[dict]:
        from app.entity_policy import ENTITY_TYPES
        if label not in ENTITY_TYPES or label == "Document":
            raise ValueError("Unsupported entity label")
        async with self.driver.session() as session:
            result = await session.run(f"MATCH (n:{label}) RETURN properties(n) AS props")
            return [{**dict(row["props"]), "entity_type": label} async for row in result]

    async def record_entity_source(self, node_uuid: str, doc_id: int, *, identity_hint: str = ""):
        async with self.driver.session() as session:
            result = await session.run("""
                MATCH (n {uuid: $uuid}) WHERE NOT n:Document
                SET n.source_doc_ids = CASE WHEN $doc IN coalesce(n.source_doc_ids, [])
                    THEN n.source_doc_ids ELSE coalesce(n.source_doc_ids, []) + $doc END,
                    n.identity_hints = CASE WHEN $hint = '' OR $hint IN coalesce(n.identity_hints, [])
                    THEN coalesce(n.identity_hints, []) ELSE coalesce(n.identity_hints, []) + $hint END
                RETURN n.uuid AS uuid""", uuid=node_uuid, doc=doc_id, hint=identity_hint)
            if await result.single() is None:
                raise ValueError("Resolved identity disappeared before source binding")

    async def add_entity_alias_record(self, node_uuid: str, record: dict):
        """Append provenance separately; raw aliases remain searchable, not trusted."""
        if record.get("status") in {"quarantined", "revoked", "untrusted"}:
            node = await self.get_node(node_uuid)
            if not node:
                raise ValueError("Alias target disappeared")
            # Fail closed before the graph write if the durable ledger is down.
            props = node["properties"]
            await self._preserve_alias_revocations([{**node, "properties": {**props, "alias_records": [record]}}])
        serialized = json.dumps(record, sort_keys=True)
        async with self.driver.session() as session:
            result = await session.run("""
                MATCH (n {uuid: $uuid}) WHERE NOT n:Document
                SET n.alias_records = CASE WHEN $record IN coalesce(n.alias_records, [])
                    THEN n.alias_records ELSE coalesce(n.alias_records, []) + $record END,
                    n.aliases = CASE WHEN $alias IN coalesce(n.aliases, [])
                    THEN n.aliases ELSE coalesce(n.aliases, []) + $alias END
                RETURN n.uuid AS uuid""", uuid=node_uuid, record=serialized, alias=record["alias"])
            if await result.single() is None:
                raise ValueError("Alias target disappeared")

    async def create_node(self, label: str, properties: dict) -> str:
        """Create a generic node with given label and properties."""
        node_uuid = self.new_uuid()
        properties = self._sanitize_identity_properties(properties)
        props = {**properties, "uuid": node_uuid}
        props_str = ", ".join(f"{k}: ${k}" for k in props)
        query = f"CREATE (n:{label} {{{props_str}}})"
        async def _op():
            async with self.driver.session() as session:
                await session.run(query, **props)
        await retry_db(_op, operation='create_node')
        return node_uuid

    async def create_relationship(self, from_uuid: str, from_label: str,
                                   to_uuid: str, to_label: str,
                                   rel_type: str, properties: dict = None):
        """Upsert one source's support without overwriting other documents."""
        props = properties or {}
        for label in (from_label, to_label):
            if not isinstance(label, str) or not re.fullmatch(r"[A-Za-z][A-Za-z0-9_]*", label):
                raise ValueError("Invalid relationship endpoint label")
        rel_type = _sanitize_rel_type(rel_type)
        async def write(tx):
            result = await tx.run(f"""
                MATCH (a:{from_label}) WHERE a.uuid = $from_uuid OR a.paperless_id = $from_pid
                MATCH (b:{to_label}) WHERE b.uuid = $to_uuid OR b.paperless_id = $to_pid
                MERGE (a)-[r:{rel_type}]->(b)
                SET r._support_lock = true
                RETURN elementId(r) AS id, properties(r) AS props
                """, from_uuid=from_uuid, from_pid=_try_int(from_uuid),
                to_uuid=to_uuid, to_pid=_try_int(to_uuid))
            row = await result.single()
            if row is None:
                raise ValueError("Relationship endpoints do not exist")
            previous = dict(row["props"])
            previous.pop("_support_lock", None)
            source = props.get("source_doc")
            if type(source) is int:
                records = support_records(previous)
                records.pop(source, None)
                previous = {"support_records": [json.dumps(r) for r in records.values()]}
            merged = merge_support_properties(previous, props)
            await tx.run("MATCH ()-[r]->() WHERE elementId(r) = $id SET r = $props",
                         id=row["id"], props=merged)
        async with self.driver.session() as session:
            await session.execute_write(write)

    async def get_document_entities(self, paperless_id: int) -> list[dict]:
        """Get all entities connected to a document."""
        async with self.driver.session() as session:
            result = await session.run(
                """
                MATCH (d:Document {paperless_id: $pid})-[r]-(n)
                WHERE NOT n:Document
                RETURN DISTINCT labels(n) AS labels, properties(n) AS props, n.uuid AS uuid
                """,
                pid=paperless_id,
            )
            entities = []
            async for r in result:
                entity = {"labels": r["labels"], "uuid": r["uuid"]}
                entity.update(r["props"])
                entities.append(entity)
            return entities

    async def get_document_entity_counts(self, document_ids: list[int]) -> dict[int, int]:
        """Count the distinct entities connected to each document, as get_document_entities lists them."""
        if not document_ids:
            return {}
        async with self.driver.session() as session:
            result = await session.run("""
                MATCH (d:Document) WHERE d.paperless_id IN $ids
                RETURN d.paperless_id AS document_id,
                       COUNT { MATCH (d)--(n) WHERE NOT n:Document RETURN DISTINCT n } AS count
                """, ids=document_ids)
            return {row["document_id"]: row["count"] for row in await result.data()}

    async def get_entity_name(self, node_uuid: str) -> Optional[dict]:
        """Name an entity for a filter chip without loading its links."""
        async with self.driver.session() as session:
            result = await session.run("""
                MATCH (n {uuid: $uuid}) WHERE NOT n:Document
                RETURN n.uuid AS uuid, n.name AS name, labels(n) AS labels LIMIT 1
                """, uuid=node_uuid)
            record = await result.single()
            return dict(record) if record else None

    async def get_document_detail_graph(self, paperless_id: int) -> dict:
        """Return a document node with extracted entities and relationships."""
        async with self.driver.session() as session:
            result = await session.run(
                """
                MATCH (d:Document {paperless_id: $pid})
                OPTIONAL MATCH (d)-[r]-(n)
                RETURN properties(d) AS document,
                       collect({
                         rel_type: type(r),
                         rel_props: properties(r),
                         direction: CASE WHEN startNode(r) = d THEN 'out' ELSE 'in' END,
                         labels: labels(n),
                         props: properties(n)
                       }) AS relationships
                """,
                pid=paperless_id,
            )
            record = await result.single()
            if not record:
                return {"document": None, "entities": [], "relationships": []}
            rels = [r for r in record["relationships"] if r.get("rel_type")]
            entities = []
            seen = set()
            for rel in rels:
                props = rel.get("props") or {}
                key = props.get("uuid") or props.get("paperless_id") or props.get("name")
                if key and key not in seen:
                    seen.add(key)
                    entities.append({"labels": rel.get("labels") or [], "properties": props})
            return {
                "document": record["document"],
                "entities": entities,
                "relationships": rels,
            }

    async def get_subgraph(self, entity_uuids: list[str], depth: int = 2) -> dict:
        """Get a connected subgraph within N hops of any of the input entities."""
        if not entity_uuids:
            return {"nodes": [], "relationships": []}

        async with self.driver.session() as session:
            result = await session.run(
                """
                MATCH (n) WHERE n.uuid IN $uuids
                CALL apoc.path.subgraphAll(n, {maxLevel: $depth})
                YIELD nodes, relationships
                WITH collect(nodes) AS all_nodes_lists, collect(relationships) AS all_rels_lists
                WITH reduce(acc = [], nl IN all_nodes_lists | acc + nl) AS all_nodes,
                     reduce(acc = [], rl IN all_rels_lists | acc + rl) AS all_rels
                UNWIND all_nodes AS n2
                WITH collect(DISTINCT {labels: labels(n2), props: properties(n2)}) AS nodes, all_rels
                UNWIND all_rels AS r2
                RETURN nodes,
                       collect(DISTINCT {type: type(r2), props: properties(r2),
                               start_uuid: coalesce(properties(startNode(r2)).uuid, toString(startNode(r2).paperless_id)),
                               end_uuid: coalesce(properties(endNode(r2)).uuid, toString(endNode(r2).paperless_id)),
                               weight: r2.weight}) AS relationships
                """,
                uuids=entity_uuids, depth=depth,
            )
            record = await result.single()
            if not record:
                return await self._get_subgraph_no_apoc(entity_uuids, depth)
            return {"nodes": record["nodes"][:50], "relationships": record["relationships"][:100]}

    async def _get_subgraph_no_apoc(self, entity_uuids: list[str], depth: int) -> dict:
        """Fallback subgraph query without APOC."""
        async with self.driver.session() as session:
            result = await session.run(
                """
                MATCH path = (start)-[*1..3]-(end)
                WHERE start.uuid IN $uuids
                UNWIND nodes(path) AS n
                UNWIND relationships(path) AS r
                WITH collect(DISTINCT {labels: labels(n), props: properties(n)}) AS nodes,
                     collect(DISTINCT {type: type(r), props: properties(r),
                             start_uuid: coalesce(properties(startNode(r)).uuid, toString(startNode(r).paperless_id)),
                             end_uuid: coalesce(properties(endNode(r)).uuid, toString(endNode(r).paperless_id)),
                             weight: r.weight}) AS rels
                RETURN nodes, rels
                """,
                uuids=entity_uuids,
            )
            record = await result.single()
            if not record:
                return {"nodes": [], "relationships": []}
            return {"nodes": record["nodes"][:50], "relationships": record["rels"][:100]}


    async def get_document_dates(self, document_ids: list[int]) -> dict[int, str]:
        """Indexed Paperless dates are retrieval hints, not event evidence."""
        if not document_ids:
            return {}
        async with self.driver.session() as session:
            result = await session.run("""
                MATCH (d:Document) WHERE d.paperless_id IN $ids
                RETURN d.paperless_id AS document_id, coalesce(toString(d.date), '') AS date
                ORDER BY document_id
                """, ids=document_ids[:500])
            return {row["document_id"]: row["date"] for row in await result.data()}

    async def get_all_document_ids(self) -> set[int]:
        """Return all paperless_id values for Document nodes in the graph."""
        async with self.driver.session() as session:
            result = await session.run(
                "MATCH (d:Document) WHERE d.paperless_id IS NOT NULL RETURN d.paperless_id AS pid"
            )
            records = await result.data()
            return {r["pid"] for r in records}

    async def delete_document_graph(self, paperless_id: int):
        """Remove this document's support and only its newly orphaned entities."""
        async def write(tx):
            result = await tx.run("""
                MATCH (a)-[r]->(b)
                WHERE r.source_doc = $pid OR $pid IN coalesce(r.source_doc_ids, [])
                   OR a.paperless_id = $pid OR b.paperless_id = $pid
                SET r._support_lock = true
                RETURN elementId(r) AS id, properties(r) AS props,
                       a.uuid AS a_uuid, b.uuid AS b_uuid,
                       a.paperless_id = $pid OR b.paperless_id = $pid AS document_edge
                """, pid=paperless_id)
            affected = set()
            rows = [dict(row) async for row in result]
            for row in rows:
                affected.update(x for x in (row["a_uuid"], row["b_uuid"]) if x)
                records = support_records(row["props"])
                records.pop(paperless_id, None)
                if records and not row["document_edge"]:
                    props = merge_support_properties({"support_records": [json.dumps(r) for r in records.values()]})
                    await tx.run("MATCH ()-[r]->() WHERE elementId(r) = $id SET r = $props", id=row["id"], props=props)
                else:
                    await tx.run("MATCH ()-[r]->() WHERE elementId(r) = $id DELETE r", id=row["id"])
            await tx.run("""MATCH (n) WHERE $pid IN coalesce(n.source_doc_ids, []) AND NOT n:Document
                             SET n.source_doc_ids = [id IN n.source_doc_ids WHERE id <> $pid]""", pid=paperless_id)
            await tx.run("MATCH (d:Document {paperless_id: $pid}) DETACH DELETE d", pid=paperless_id)
            # Promote legacy node-local negatives before UUID deletion. PG is
            # committed first; a failed graph transaction merely leaves a safe,
            # idempotent revocation. A failed PG write rolls graph cleanup back.
            result = await tx.run("""MATCH (n) WHERE n.uuid IN $affected AND NOT n:Document
                                  RETURN properties(n) AS props, labels(n) AS labels""", affected=list(affected))
            await self._preserve_alias_revocations([{"properties": row["props"], "labels": row["labels"]} async for row in result])
            result = await tx.run("""MATCH (n) WHERE n.uuid IN $affected AND NOT n:Document
                             AND NOT coalesce(n.review_anchor, false)
                             AND NOT EXISTS { (n)--() }
                             WITH n, n.uuid AS uuid DELETE n RETURN uuid""", affected=list(affected))
            await self._delete_entity_embeddings([row["uuid"] async for row in result])
        async with self.driver.session() as session:
            await session.execute_write(write)

    async def get_counts(self) -> dict:
        async with self.driver.session() as session:
            node_result = await session.run("MATCH (n) RETURN count(n) AS count")
            node_record = await node_result.single()
            rel_result = await session.run("MATCH ()-[r]->() RETURN count(r) AS count")
            rel_record = await rel_result.single()
            doc_result = await session.run("MATCH (d:Document) RETURN count(d) AS count")
            doc_record = await doc_result.single()
            nodes = node_record["count"] if node_record else 0
            docs = doc_record["count"] if doc_record else 0
            return {
                "nodes": nodes,
                "entities": nodes - docs,
                "relationships": rel_record["count"] if rel_record else 0,
                "documents": docs,
            }

    async def get_entity_review_candidates(self, ignored_pairs: set[tuple[str, str]], limit: int = 50) -> list[dict]:
        """Find likely duplicate entities for human review."""
        async with self.driver.session() as session:
            result = await session.run(
                """
                MATCH (n)
                WHERE n.uuid IS NOT NULL
                  AND NOT n:Document
                  AND (n.name IS NOT NULL OR n.title IS NOT NULL)
                RETURN n.uuid AS uuid, labels(n) AS labels,
                       coalesce(n.name, n.title) AS name,
                       properties(n) AS properties
                LIMIT 2000
                """
            )
            entities = [dict(r) async for r in result]

        by_label: dict[str, list[dict]] = defaultdict(list)
        for entity in entities:
            entity_uuid = self._first_text(entity.get("uuid"))
            entity_name = self._first_text(entity.get("name"))
            if not entity_uuid or len(entity_name) < 3:
                continue
            entity["uuid"] = entity_uuid
            entity["name"] = entity_name
            properties = entity.get("properties") or {}
            properties["uuid"] = entity_uuid
            entity["properties"] = properties
            label = (entity.get("labels") or ["Unknown"])[0]
            by_label[label].append(entity)

        candidates = []
        for label, group in by_label.items():
            for i, left in enumerate(group):
                left_name = left["name"]
                for right in group[i + 1:]:
                    pair = tuple(sorted([str(left["uuid"]), str(right["uuid"])]))
                    if pair[0] == pair[1]:
                        continue
                    if pair in ignored_pairs:
                        continue
                    right_name = right["name"]
                    score = fuzz.token_sort_ratio(left_name, right_name)
                    if score >= 82:
                        candidates.append({
                            "score": score,
                            "label": label,
                            "left": left,
                            "right": right,
                        })

        candidates.sort(key=lambda c: c["score"], reverse=True)
        return candidates[:limit]

    async def merge_entities(self, primary_uuid: str, duplicate_uuid: str, *, merge_id: str,
                             review_id: str | None = None, review_method: str | None = None) -> dict:
        """Atomically preserve canonical identity, aliases and relationship support.

        Human review policy is enforced by EntityResolver before calling this
        storage operation. Exact UUID lookup also rejects ambiguous legacy IDs.
        The journal row with before and after images commits in PG before the
        graph does, so a failed journal write leaves the graph unmerged.
        """
        from app.entity_policy import EXPLICIT_REVIEW_METHOD
        if review_id and review_method != EXPLICIT_REVIEW_METHOD:
            raise ValueError("Trusted alias writes require explicit review origin")
        if not primary_uuid or not duplicate_uuid or primary_uuid == duplicate_uuid:
            raise ValueError("Two different entity UUIDs are required")

        async def merge(tx):
            result = await tx.run(
                """MATCH (n) WHERE n.uuid IN $uuids
                   WITH n ORDER BY n.uuid
                   SET n.uuid = n.uuid
                   RETURN elementId(n) AS id, labels(n) AS labels, properties(n) AS props""",
                uuids=[primary_uuid, duplicate_uuid],
            )
            nodes = await result.data()
            if len(nodes) != 2 or {n["props"]["uuid"] for n in nodes} != {primary_uuid, duplicate_uuid}:
                raise ValueError("Entity pair not found")
            primary = next(n for n in nodes if n["props"]["uuid"] == primary_uuid)
            duplicate = next(n for n in nodes if n["props"]["uuid"] == duplicate_uuid)
            if any("Document" in n["labels"] for n in nodes):
                raise ValueError("Document nodes cannot be merged as entities")
            if set(primary["labels"]) != set(duplicate["labels"]):
                raise ValueError("Entity types must match")
            before, _ = await _entity_image(tx, [primary_uuid, duplicate_uuid])

            result = await tx.run(
                """MATCH (n) WHERE elementId(n) IN $ids
                   MATCH (n)-[r]-()
                   WITH DISTINCT r
                   RETURN elementId(r) AS id, type(r) AS type,
                          elementId(startNode(r)) AS start, elementId(endNode(r)) AS end,
                          properties(r) AS props""",
                ids=[primary["id"], duplicate["id"]],
            )
            grouped = defaultdict(list)
            for edge in await result.data():
                endpoints = tuple(primary["id"] if edge[k] == duplicate["id"] else edge[k] for k in ("start", "end"))
                grouped[(*endpoints, edge["type"])].append(edge)

            props = {**duplicate["props"], **primary["props"]}
            aliases = set()
            source_docs = set()
            for node in nodes:
                value = node["props"].get("aliases") or []
                aliases.update(value if isinstance(value, list) else [value])
                source_docs.update(v for v in node["props"].get("source_doc_ids", []) if type(v) is int)
            aliases.add(duplicate["props"].get("name"))
            aliases.discard(primary["props"].get("name"))
            props["aliases"] = sorted(alias for alias in aliases if isinstance(alias, str) and alias)
            # Preserve unknown legacy strings and records, without blessing them.
            records = [value for node in nodes for value in (node["props"].get("alias_records") or [])]
            if review_id:
                from app.entity_policy import human_alias_record, trusted_aliases
                kind = primary["props"].get("entity_type") or primary["labels"][0]
                record = human_alias_record(primary["props"]["name"], duplicate["props"]["name"], kind, review_id, review_method=review_method)
                records.append(json.dumps(record, sort_keys=True))
                for source_node in nodes:
                    for alias in trusted_aliases(source_node["props"], kind, -1, ""):
                        records.append(json.dumps(human_alias_record(primary["props"]["name"], alias, kind, review_id, review_method=review_method), sort_keys=True))
            props["alias_records"] = list(dict.fromkeys(records))
            props["identity_hints"] = sorted({value for node in nodes for value in node["props"].get("identity_hints", [])})

            for (start, end, rel_type), edges in grouped.items():
                combined = merge_support_properties(*(edge["props"] for edge in edges))
                source_docs.update(combined.get("source_doc_ids") or [])
                # Keep the existing canonical edge ID whenever possible.
                survivor = next((e for e in edges if e["start"] == start and e["end"] == end), None)
                if survivor:
                    await (await tx.run(
                        "MATCH ()-[r]->() WHERE elementId(r) = $id SET r = $props",
                        id=survivor["id"], props=combined,
                    )).consume()
                else:
                    escaped_type = rel_type.replace("`", "``")
                    await (await tx.run(
                        f"MATCH (a), (b) WHERE elementId(a) = $start AND elementId(b) = $end "
                        f"CREATE (a)-[r:`{escaped_type}`]->(b) SET r = $props",
                        start=start, end=end, props=combined,
                    )).consume()
                removed_ids = [e["id"] for e in edges if not survivor or e["id"] != survivor["id"]]
                if removed_ids:
                    await (await tx.run(
                        "MATCH ()-[r]->() WHERE elementId(r) IN $ids DELETE r", ids=removed_ids,
                    )).consume()

            props["source_doc_ids"] = sorted(source_docs)
            result = await tx.run(
                """MATCH (primary), (duplicate)
                   WHERE elementId(primary) = $primary AND elementId(duplicate) = $duplicate
                   SET primary = $props
                   DELETE duplicate
                   RETURN labels(primary) AS labels, properties(primary) AS properties""",
                primary=primary["id"], duplicate=duplicate["id"], props=props,
            )
            merged = dict(await result.single(strict=True))
            after, _ = await _entity_image(tx, [primary_uuid])
            from app.embeddings import embeddings_store
            await (self.review_store or embeddings_store).insert_merge_journal({
                "merge_id": merge_id, "review_id": review_id, "primary_uuid": primary_uuid,
                "duplicate_uuid": duplicate_uuid, "decision": "merged", "review_method": review_method or "",
                "before_image": before, "after_image": after,
                "risk_flags": _merge_risk_flags(primary["props"], duplicate["props"], before["edges"]),
            })
            await self._delete_entity_embeddings([duplicate_uuid])
            return merged

        async with self.driver.session() as session:
            return await session.execute_write(merge)

    async def unmerge_entities(self, journal: dict) -> str:
        """Write the before-image back while the survivor still equals the after-image.

        Returns "restored", or "already_restored" when a previous attempt committed.
        Raises MergeDrift, without writing, when anything differs.
        """
        primary_uuid, duplicate_uuid = journal["primary_uuid"], journal["duplicate_uuid"]
        before, after = journal["before_image"], journal.get("after_image")

        async def unmerge(tx):
            await tx.run("MATCH (n) WHERE n.uuid IN $uuids SET n.uuid = n.uuid", uuids=[primary_uuid, duplicate_uuid])
            current, ids = await _entity_image(tx, [primary_uuid, duplicate_uuid])
            if any(node["props"].get("uuid") == duplicate_uuid for node in current["nodes"]):
                if _canonical(current) == _canonical({k: before[k] for k in ("nodes", "edges")}):
                    return "already_restored"
                raise MergeDrift("The duplicate entity exists again in another state")
            if not after or _canonical(current) != _canonical(after):
                raise MergeDrift("The surviving entity changed after the merge")
            duplicate_key = _canonical({"uuid": duplicate_uuid})
            if any(_canonical(edge[end]) not in ids and _canonical(edge[end]) != duplicate_key
                   for edge in before["edges"] for end in ("start", "end")):
                raise MergeDrift("A relationship endpoint no longer exists")

            await tx.run("MATCH (n)-[r]-() WHERE n.uuid = $uuid DELETE r", uuid=primary_uuid)
            for node in before["nodes"]:
                if node["props"]["uuid"] == primary_uuid:
                    await tx.run("MATCH (n) WHERE elementId(n) = $id SET n = $props",
                                 id=ids[_canonical({"uuid": primary_uuid})], props=node["props"])
                    continue
                if not all(re.fullmatch(r"[A-Za-z][A-Za-z0-9_]*", label) for label in node["labels"]):
                    raise ValueError("Invalid journaled entity label")
                result = await tx.run(f"CREATE (n:{':'.join(node['labels'])}) SET n = $props RETURN elementId(n) AS id",
                                      props=node["props"])
                ids[duplicate_key] = (await result.single(strict=True))["id"]
            by_type = defaultdict(list)
            for edge in before["edges"]:
                by_type[edge["type"]].append({"start": ids[_canonical(edge["start"])],
                                              "end": ids[_canonical(edge["end"])], "props": edge["props"]})
            for rel_type, edges in by_type.items():
                escaped_type = rel_type.replace("`", "``")
                await (await tx.run(f"""UNWIND $edges AS e
                    MATCH (a) WHERE elementId(a) = e.start MATCH (b) WHERE elementId(b) = e.end
                    CREATE (a)-[r:`{escaped_type}`]->(b) SET r = e.props""", edges=edges)).consume()
            return "restored"

        async with self.driver.session() as session:
            return await session.execute_write(unmerge)

    async def reset_unmerged_fields(self, journal: dict) -> bool:
        """Drift fallback: undo the merge's node-field changes on the survivor.

        Fields changed after the merge keep their current value. Returns False,
        without writing, when the survivor is gone or the duplicate exists again.
        """
        primary_uuid, duplicate_uuid = journal["primary_uuid"], journal["duplicate_uuid"]
        before = next(n["props"] for n in journal["before_image"]["nodes"] if n["props"]["uuid"] == primary_uuid)
        after = next(n["props"] for n in journal["after_image"]["nodes"] if n["props"]["uuid"] == primary_uuid)

        async def write(tx):
            result = await tx.run("""MATCH (n) WHERE n.uuid IN $uuids SET n.uuid = n.uuid
                                     RETURN n.uuid AS uuid, properties(n) AS props""", uuids=[primary_uuid, duplicate_uuid])
            current = {row["uuid"]: row["props"] for row in await result.data()}
            if duplicate_uuid in current or primary_uuid not in current:
                return False
            await tx.run("MATCH (n) WHERE n.uuid = $uuid SET n = $props", uuid=primary_uuid,
                         props=_three_way_fields(before, after, current[primary_uuid]))
            return True

        async with self.driver.session() as session:
            return await session.execute_write(write)

    async def search_nodes(
        self, query: str, node_type: str = None, limit: int = 20, *,
        offset: int = 0, doc_type: str | None = None, sort: str = "relevance",
        direction: str = "desc", include_page: bool = False, entity: str = "",
        exclude_documents: bool = False, graph_fields: bool = False,
    ) -> list[dict] | dict:
        """Filter the whole graph before bounded, deterministic pagination."""
        if node_type and not re.fullmatch(r"[A-Za-z][A-Za-z0-9_]*", node_type):
            raise ValueError(f"Invalid node type: {node_type}")
        if isinstance(limit, bool) or not 1 <= limit <= 200:
            raise ValueError("Search limit must be between 1 and 200")
        if isinstance(offset, bool) or offset < 0:
            raise ValueError("Search offset must be nonnegative")
        orders = {
            "relevance": "score",
            "title": "toLower(coalesce(toStringOrNull(n.title), toStringOrNull(n.name), ''))",
            "date": "coalesce(toStringOrNull(n.date), '')",
            "doc_type": "coalesce(toStringOrNull(n.doc_type), 'unknown')",
            "paperless_id": "n.paperless_id",
        }
        documents = sort == "documents"
        graph_fields = graph_fields or documents
        if (sort not in orders and not documents) or direction not in {"asc", "desc"}:
            raise ValueError("Invalid search ordering")
        terms = self._search_terms(query)
        # Only the documents sort lists entities for an empty query.
        if not terms and not node_type and not documents:
            empty = {"results": [], "total": 0, "offset": offset, "limit": limit,
                     "has_more": False, "doc_types": {}}
            return empty if include_page else []
        label = f":{node_type}" if node_type else ""
        # Only an entity filter adds this scope, so the default query stays unchanged.
        scope = """
            OPTIONAL MATCH (e {uuid: $entity}) WHERE NOT e:Document
            WITH e, coalesce(e.source_doc_ids, []) AS entity_docs
        """ if entity else ""
        where = """WHERE e IS NOT NULL AND (n.paperless_id IN entity_docs
                                         OR EXISTS { (n)-[:MENTIONS]-(e) })""" if entity else ""
        if exclude_documents:
            where = f"{where} AND NOT n:Document" if where else "WHERE NOT n:Document"
        match = f"""{scope}
            MATCH (n{label}) {where}
            WITH n, toLower(
                coalesce(toStringOrNull(n.title), '') + ' ' +
                coalesce(toStringOrNull(n.name), '') + ' ' +
                coalesce(toStringOrNull(n.doc_type), '') + ' ' +
                coalesce(toStringOrNull(n.date), '') + ' ' +
                coalesce(toStringOrNull(n.paperless_id), '') + ' ' +
                reduce(text = '', alias IN coalesce(n.aliases, []) |
                       text + ' ' + coalesce(toStringOrNull(alias), ''))
            ) AS searchable
            WITH n, size([term IN $terms WHERE searchable CONTAINS term]) AS score
            WHERE size($terms) = 0 OR score > 0
        """
        parameters = {"terms": terms, "doc_type": doc_type or "", "limit": limit, "offset": offset,
                      "entity": entity}
        async with self.driver.session() as session:
            total = None
            facets = {}
            if include_page:
                counts = await session.run(match + """
                    WITH coalesce(toStringOrNull(n.doc_type), 'unknown') AS doc_type, count(*) AS count
                    RETURN coalesce(sum(CASE WHEN $doc_type = '' OR doc_type = $doc_type
                                             THEN count ELSE 0 END), 0) AS total,
                           collect({type: doc_type, count: count}) AS doc_types
                """, **parameters)
                record = await counts.single()
                total = int(record["total"]) if record else 0
                facets = {item["type"]: item["count"] for item in (record["doc_types"] if record else [])}
            fields = f""", {_EVIDENCE_DOC_COUNT} AS evidence_doc_count,
                CASE WHEN size($terms) = 0 OR any(term IN $terms WHERE toLower(coalesce(
                         toStringOrNull(n.name), toStringOrNull(n.title), '')) CONTAINS term) THEN null
                     ELSE head([alias IN coalesce(n.aliases, []) WHERE any(term IN $terms WHERE
                         toLower(coalesce(toStringOrNull(alias), '')) CONTAINS term) | toStringOrNull(alias)])
                END AS matched_alias""" if graph_fields else ""
            order = ("evidence_doc_count DESC, toLower(coalesce(toStringOrNull(n.name), toStringOrNull(n.title), '')) ASC"
                     if documents else f"{orders[sort]} {direction.upper()}")
            result = await session.run(match + f"""
                WITH n, score
                WHERE $doc_type = '' OR coalesce(toStringOrNull(n.doc_type), 'unknown') = $doc_type
                RETURN labels(n) AS labels, properties(n) AS props{fields}
                ORDER BY {order},
                         coalesce(toStringOrNull(n.date), '') DESC,
                         coalesce(toStringOrNull(n.uuid), toStringOrNull(n.paperless_id), elementId(n)) ASC
                SKIP $offset LIMIT $limit
            """, **parameters)
            rows = [{"labels": row["labels"], "properties": row["props"],
                     **({"evidence_doc_count": row["evidence_doc_count"], "matched_alias": row["matched_alias"]}
                        if graph_fields else {})} async for row in result]
        if not include_page:
            return rows
        return {"results": rows, "total": total, "offset": offset, "limit": limit,
                "has_more": offset + len(rows) < total, "doc_types": facets}

    async def get_node(self, node_uuid: str) -> Optional[dict]:
        async with self.driver.session() as session:
            result = await session.run(
                """
                MATCH (n) WHERE n.uuid = $uuid OR n.paperless_id = $pid
                OPTIONAL MATCH (n)-[r]-(m)
                RETURN labels(n) AS labels, properties(n) AS props,
                       collect({rel_type: type(r), direction: CASE WHEN startNode(r) = n THEN 'out' ELSE 'in' END,
                               rel_props: properties(r), neighbor_labels: labels(m),
                               neighbor_props: properties(m)}) AS relationships
                """,
                uuid=node_uuid, pid=_try_int(node_uuid),
            )
            record = await result.single()
            if not record:
                return None
            return {
                "labels": record["labels"],
                "properties": record["props"],
                "relationships": record["relationships"],
            }

    async def get_neighbors(self, node_uuid: str, depth: int = 2) -> dict:
        depth = max(1, min(int(depth), 4))
        try:
            async with self.driver.session() as session:
                result = await session.run(
                    """
                    MATCH (start) WHERE start.uuid = $uuid OR start.paperless_id = $pid
                    CALL apoc.path.subgraphAll(start, {maxLevel: $depth})
                    YIELD nodes, relationships
                    RETURN [n IN nodes | {labels: labels(n), props: properties(n)}] AS nodes,
                           [r IN relationships | {id: elementId(r), type: type(r), props: properties(r),
                            start: coalesce(startNode(r).uuid, 'doc-' + toString(startNode(r).paperless_id)),
                            end: coalesce(endNode(r).uuid, 'doc-' + toString(endNode(r).paperless_id))}] AS rels
                    """,
                    uuid=node_uuid, pid=_try_int(node_uuid), depth=depth,
                )
                record = await result.single()
                return {"nodes": record["nodes"], "relationships": record["rels"]} if record else {"nodes": [], "relationships": []}
        except Exception as error:
            # Only a missing optional procedure should trigger a different query.
            if getattr(error, "code", "") != "Neo.ClientError.Procedure.ProcedureNotFound":
                raise
            return await self._get_neighbors_no_apoc(node_uuid, depth)

    async def _get_neighbors_no_apoc(self, node_uuid: str, depth: int) -> dict:
        """Fallback neighborhood query without APOC."""
        depth = max(1, min(int(depth), 4))
        async with self.driver.session() as session:
            result = await session.run(
                f"""
                MATCH (start) WHERE start.uuid = $uuid OR start.paperless_id = $pid
                MATCH path = (start)-[*0..{depth}]-(end)
                WITH collect(DISTINCT end) AS nodes
                UNWIND nodes AS a
                OPTIONAL MATCH (a)-[r]->(b)
                WHERE b IN nodes
                WITH nodes, collect(DISTINCT r) AS relationships
                RETURN [n IN nodes | {{labels: labels(n), props: properties(n)}}] AS nodes,
                       [r IN relationships | {{id: elementId(r), type: type(r), props: properties(r),
                        start: coalesce(startNode(r).uuid, 'doc-' + toString(startNode(r).paperless_id)),
                        end: coalesce(endNode(r).uuid, 'doc-' + toString(endNode(r).paperless_id))}}] AS rels
                """,
                uuid=node_uuid, pid=_try_int(node_uuid),
            )
            record = await result.single()
            if not record:
                return {"nodes": [], "relationships": []}
            return {"nodes": record["nodes"], "relationships": record["rels"]}

    async def get_relationship_types(self) -> list[dict]:
        """Count every stored relationship type, with stable ordering for ties."""
        async with self.driver.session() as session:
            result = await session.run("""
                MATCH ()-[r]->()
                RETURN type(r) AS type, count(*) AS count
                ORDER BY count DESC, type ASC
            """)
            return [{"type": row["type"], "count": row["count"]} async for row in result]

    async def get_initial_graph(self, limit: int = 300) -> dict:
        """Get an initial graph view sampling across ALL entity types (not raw Document nodes)."""
        async with self.driver.session() as session:
            # Sample top nodes from each entity type for a diverse view
            node_result = await session.run(
                """
                MATCH (n)
                WHERE NOT n:Document
                WITH labels(n)[0] AS lbl, n
                ORDER BY COUNT { (n)--() } DESC
                WITH lbl, collect({labels: labels(n), props: properties(n)}) AS typed_nodes
                UNWIND typed_nodes[0..CASE WHEN size(typed_nodes) > 50 THEN 50 ELSE size(typed_nodes) END] AS node
                RETURN node.labels AS labels, node.props AS props
                LIMIT $limit
                """,
                limit=limit,
            )
            nodes = [{"labels": r["labels"], "props": r["props"]} async for r in node_result]

            # Get UUIDs for relationship query
            uuids = [n["props"].get("uuid") for n in nodes if n["props"].get("uuid")]

            # Get relationships between these nodes (and their connected Document nodes)
            rel_result = await session.run(
                """
                MATCH (a)-[r]-(b)
                WHERE a.uuid IN $uuids
                RETURN DISTINCT
                    labels(a) AS a_labels, properties(a) AS a_props,
                    labels(b) AS b_labels, properties(b) AS b_props,
                    elementId(r) AS rel_id, type(r) AS rel_type, properties(r) AS rel_props,
                    properties(startNode(r)).uuid AS start_uuid,
                    properties(endNode(r)).uuid AS end_uuid,
                    startNode(r).paperless_id AS start_pid,
                    endNode(r).paperless_id AS end_pid
                LIMIT 1000
                """,
                uuids=uuids,
            )

            all_nodes = {_graph_node_id(n["props"]): n for n in nodes if _graph_node_id(n["props"])}
            relationships = {}

            async for r in rel_result:
                # Add connected nodes we haven't seen
                for prefix in ["a", "b"]:
                    props = r[f"{prefix}_props"]
                    uid = _graph_node_id(props)
                    if uid and uid not in all_nodes:
                        all_nodes[uid] = {"labels": r[f"{prefix}_labels"], "props": props}

                start = _graph_node_id({"uuid": r["start_uuid"], "paperless_id": r["start_pid"]})
                end = _graph_node_id({"uuid": r["end_uuid"], "paperless_id": r["end_pid"]})
                if start in all_nodes and end in all_nodes:
                    relationships[r["rel_id"]] = {
                        "id": r["rel_id"],
                        "type": r["rel_type"],
                        "props": r["rel_props"],
                        "start": start,
                        "end": end,
                    }

            return {
                "nodes": list(all_nodes.values()),
                "relationships": list(relationships.values()),
            }

    async def get_documents_by_entity_types(self, entity_types: list[str], limit: int = 100) -> list[int]:
        """Get document IDs connected to entities of the given types.
        Returns the most recent documents first (highest paperless_id = newest).
        """
        if not entity_types:
            return []
        async with self.driver.session() as session:
            result = await session.run(
                """
                MATCH (d:Document)-[r]-(e)
                WHERE any(label IN labels(e) WHERE label IN $types)
                WITH d.paperless_id AS pid, count(DISTINCT e) AS entity_count
                ORDER BY pid DESC
                LIMIT $limit
                RETURN pid
                """,
                types=entity_types,
                limit=limit,
            )
            doc_ids = []
            async for record in result:
                pid = record["pid"]
                if pid is not None:
                    doc_ids.append(int(pid))
            return doc_ids

    async def get_recent_docs_per_organization(self, limit_per_org: int = 2) -> list[dict]:
        """For each Organization in the graph, find the most recent documents.
        Returns list of {org_name, doc_id} dicts, most recent doc per org.
        Perfect for 'what are my bills?' queries — ensures every payee is represented."""
        async with self.driver.session() as session:
            result = await session.run(
                """
                MATCH (o:Organization)-[]-(d:Document)
                WITH o.name AS org_name, d.paperless_id AS pid
                ORDER BY pid DESC
                WITH org_name, collect(pid)[0..$limit] AS doc_ids
                UNWIND doc_ids AS did
                RETURN org_name, did AS doc_id
                ORDER BY org_name
                """,
                limit=limit_per_org,
            )
            results = []
            async for record in result:
                results.append({
                    "org_name": record["org_name"],
                    "doc_id": int(record["doc_id"]),
                })
            return results

    async def get_recent_docs_per_organization_filtered(
        self, entity_types: list[str], limit_per_org: int = 2
    ) -> list[dict]:
        """For each Organization connected to entities of the specified types
        OR to Documents with relevant doc_types, find the most recent documents.
        This dual filter ensures orgs whose documents lack entity extraction
        (e.g., Starlink invoices without FinancialItem entities) still get included
        if their documents are typed as financial_invoice, insurance, etc.
        Query-agnostic: works for any entity type combination."""
        if not entity_types:
            return await self.get_recent_docs_per_organization(limit_per_org)

        # Map entity types to relevant document doc_types for fallback matching
        doc_type_map = {
            "FinancialItem": ["financial_invoice", "financial_statement", "tax_document"],
            "InsurancePolicy": ["insurance"],
            "Contract": ["contract", "legal"],
            "MedicalResult": ["medical", "health"],
            "Condition": ["medical", "health"],
            "Product": ["product", "manual", "warranty"],
            "System": ["product", "manual"],
            "DateEvent": [],
            "Event": [],
            "Location": [],
            "Address": ["property_home"],
            "Person": [],
            "Organization": [],
            "DocumentRef": [],
        }
        relevant_doc_types = set()
        for etype in entity_types:
            for dt in doc_type_map.get(etype, []):
                relevant_doc_types.add(dt)

        async with self.driver.session() as session:
            result = await session.run(
                """
                // Strategy 1: Orgs connected to entities of the specified types
                OPTIONAL MATCH (o1:Organization)-[]-(e)
                WHERE any(label IN labels(e) WHERE label IN $types)
                WITH collect(DISTINCT o1) AS entity_orgs

                // Strategy 2: Orgs connected to documents with relevant doc_types
                OPTIONAL MATCH (o2:Organization)-[]-(d2:Document)
                WHERE d2.doc_type IN $doc_types
                WITH entity_orgs, collect(DISTINCT o2) AS doctype_orgs

                // Union both sets
                WITH [o IN entity_orgs + doctype_orgs WHERE o IS NOT NULL] AS all_orgs
                UNWIND all_orgs AS o
                WITH DISTINCT o

                // Get most recent docs for each qualifying org
                MATCH (o)-[]-(d:Document)
                WITH o.name AS org_name, d.paperless_id AS pid
                ORDER BY pid DESC
                WITH org_name, collect(pid)[0..$limit] AS doc_ids
                UNWIND doc_ids AS did
                RETURN org_name, did AS doc_id
                ORDER BY org_name
                """,
                types=entity_types,
                doc_types=list(relevant_doc_types),
                limit=limit_per_org,
            )
            results = []
            async for record in result:
                results.append({
                    "org_name": record["org_name"],
                    "doc_id": int(record["doc_id"]),
                })
            return results

    async def check_health(self) -> dict:
        """Check Neo4j connectivity and return health info."""
        try:
            async with self.driver.session() as session:
                result = await session.run("RETURN 1 AS ok")
                record = await result.single()
                if record and record["ok"] == 1:
                    return {"status": "healthy"}
        except Exception as e:
            return {"status": "unhealthy", "error": str(e)}
        return {"status": "unhealthy", "error": "unexpected"}

    async def get_owner(self) -> tuple[str, Optional[str]]:
        """Return (owner_status, owner uuid) for OWNER_NAME; see app.graph_fields.owner_rule."""
        if not settings.owner_name.strip():
            return "unset", None
        async with self.driver.session() as session:
            result = await session.run(
                "MATCH (p:Person) RETURN p.uuid AS uuid, p.name AS name, p.aliases AS aliases")
            persons = [dict(row) async for row in result]
        return owner_rule(settings.owner_name, persons)

    async def get_node_fields(self, uuids: list[str], owner: Optional[str] = None) -> dict[str, dict]:
        """Derived node fields by uuid. Evidence links (document mentions) are not counted as links."""
        async with self.driver.session() as session:
            result = await session.run(f"""
                MATCH (n) WHERE n.uuid IN $uuids
                RETURN n.uuid AS uuid, {_EVIDENCE_DOC_COUNT} AS evidence_doc_count,
                       COUNT {{ (n)-[r]-() WHERE NOT type(r) IN $evidence }} AS link_count
                """, uuids=uuids, evidence=EVIDENCE_TYPES)
            return {row["uuid"]: {"evidence_doc_count": row["evidence_doc_count"],
                                  "link_count": row["link_count"],
                                  "is_hub": row["link_count"] > HUB_LINKS,
                                  "is_owner": row["uuid"] == owner}
                    async for row in result}


# The entity's stored source documents, or else the distinct Document nodes linked to it.
_EVIDENCE_DOC_COUNT = """CASE WHEN size(coalesce(n.source_doc_ids, [])) > 0 THEN size(n.source_doc_ids)
    ELSE COUNT { MATCH (n)--(d:Document) RETURN DISTINCT d } END"""


def _sanitize_rel_type(rel_type: str) -> str:
    """Sanitize relationship type for Neo4j: uppercase, underscores, no special chars."""
    import re
    sanitized = rel_type.strip().replace(' ', '_').replace('-', '_')
    sanitized = re.sub(r'[^A-Za-z0-9_]', '', sanitized)
    sanitized = sanitized.upper()
    sanitized = re.sub(r'_+', '_', sanitized).strip('_')
    return sanitized or 'RELATED_TO'


def _graph_node_id(props: dict) -> Optional[str]:
    node_uuid = props.get("uuid")
    if isinstance(node_uuid, str) and node_uuid.strip():
        return node_uuid
    pid = props.get("paperless_id")
    if isinstance(pid, bool) or not isinstance(pid, (str, int)):
        return None
    if str(pid).isdigit() and int(pid) > 0:
        return f"doc-{int(pid)}"
    return None


def _try_int(val: str) -> int:
    try:
        if isinstance(val, str) and val.startswith("doc-"):
            val = val[4:]
        return int(val)
    except (ValueError, TypeError):
        return -1



graph_store = GraphStore()
