import logging
import json
import re
import uuid
from collections import defaultdict
from typing import Any, Optional

from neo4j import AsyncGraphDatabase
from rapidfuzz import fuzz

from app.config import settings
from app.retry import retry_db
from app.relationship_support import merge_support_properties, support_records

logger = logging.getLogger(__name__)


class GraphStore:
    def __init__(self):
        self.driver = None

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

    async def _find_canonical(self, name: str, label: str) -> Optional[dict]:
        from app.entity_policy import name_key
        matches = [node for node in await self.get_entities_by_type(label)
                   if name_key(node.get("name", ""), label) == name_key(name, label)]
        return matches[0] if len(matches) == 1 else None

    async def find_person(self, name: str) -> Optional[dict]:
        """Unique canonical lookup only; alias authorization belongs to resolver."""
        return await self._find_canonical(name, "Person")

    async def get_all_persons(self) -> list[dict]:
        return await self.get_entities_by_type("Person")

    async def get_all_organizations(self) -> list[dict]:
        return await self.get_entities_by_type("Organization")

    async def find_organization(self, name: str) -> Optional[dict]:
        return await self._find_canonical(name, "Organization")

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

    async def create_person(self, name: str, aliases: list[str] = None, role: str = None,
                            description: str = None, source_doc_ids: list[int] = None) -> str:
        node_uuid = self.new_uuid()
        name = self._coerce_text(name)
        aliases = self._coerce_text_list(aliases)
        role = self._coerce_text(role)
        description = self._coerce_text(description)
        async with self.driver.session() as session:
            await session.run(
                """
                CREATE (p:Person {uuid: $uuid, name: $name, aliases: $aliases, role: $role,
                                  description: $description, entity_type: 'Person', source_doc_ids: $source_doc_ids})
                """,
                uuid=node_uuid, name=name, aliases=aliases, role=role,
                description=description,
                source_doc_ids=sorted(set(source_doc_ids or [])),
            )
        return node_uuid

    async def add_person_alias(self, node_uuid: str, alias: str):
        alias = self._coerce_text(alias)
        if not alias:
            return
        async with self.driver.session() as session:
            await session.run(
                """
                MATCH (p:Person {uuid: $uuid})
                SET p.aliases = CASE
                    WHEN NOT $alias IN coalesce(p.aliases, []) THEN coalesce(p.aliases, []) + $alias
                    ELSE coalesce(p.aliases, [])
                END
                """,
                uuid=node_uuid, alias=alias,
            )

    async def create_organization(self, name: str, org_type: str = None,
                                   aliases: list[str] = None, description: str = None,
                                   source_doc_ids: list[int] = None) -> str:
        node_uuid = self.new_uuid()
        name = self._coerce_text(name)
        org_type = self._coerce_text(org_type)
        aliases = self._coerce_text_list(aliases)
        description = self._coerce_text(description)
        async with self.driver.session() as session:
            await session.run(
                """
                CREATE (o:Organization {uuid: $uuid, name: $name, type: $type, aliases: $aliases,
                                        description: $description, entity_type: 'Organization', source_doc_ids: $source_doc_ids})
                """,
                uuid=node_uuid, name=name, type=org_type, aliases=aliases,
                description=description,
                source_doc_ids=sorted(set(source_doc_ids or [])),
            )
        return node_uuid

    async def add_org_alias(self, node_uuid: str, alias: str):
        alias = self._coerce_text(alias)
        if not alias:
            return
        async with self.driver.session() as session:
            await session.run(
                """
                MATCH (o:Organization {uuid: $uuid})
                SET o.aliases = CASE
                    WHEN NOT $alias IN coalesce(o.aliases, []) THEN coalesce(o.aliases, []) + $alias
                    ELSE coalesce(o.aliases, [])
                END
                """,
                uuid=node_uuid, alias=alias,
            )

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
        rel_type = _sanitize_rel_type(rel_type)
        async def write(tx):
            result = await tx.run(f"""
                MATCH (a) WHERE a.uuid = $from_uuid OR a.paperless_id = $from_pid
                MATCH (b) WHERE b.uuid = $to_uuid OR b.paperless_id = $to_pid
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
            await tx.run("""MATCH (n) WHERE n.uuid IN $affected AND NOT n:Document
                             AND NOT EXISTS { (n)--() } DELETE n""", affected=list(affected))
        async with self.driver.session() as session:
            await session.execute_write(write)

    async def clear_all(self):
        async with self.driver.session() as session:
            await session.run("MATCH (n) DETACH DELETE n")

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

    async def merge_entities(self, primary_uuid: str, duplicate_uuid: str, *, review_id: str | None = None) -> dict:
        """Atomically preserve canonical identity, aliases and relationship support.

        Human review policy is enforced by EntityResolver before calling this
        storage operation. Exact UUID lookup also rejects ambiguous legacy IDs.
        """
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
                from app.entity_policy import human_alias_record
                kind = primary["props"].get("entity_type") or primary["labels"][0]
                record = human_alias_record(primary["props"]["name"], duplicate["props"]["name"], kind, review_id)
                records.append(json.dumps(record, sort_keys=True))
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
            return dict(await result.single(strict=True))

        async with self.driver.session() as session:
            return await session.execute_write(merge)

    async def search_nodes(
        self, query: str, node_type: str = None, limit: int = 20, *,
        offset: int = 0, doc_type: str | None = None, sort: str = "relevance",
        direction: str = "desc", include_page: bool = False,
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
        if sort not in orders or direction not in {"asc", "desc"}:
            raise ValueError("Invalid search ordering")
        terms = self._search_terms(query)
        if not terms and not node_type:
            empty = {"results": [], "total": 0, "offset": offset, "limit": limit,
                     "has_more": False, "doc_types": {}}
            return empty if include_page else []
        label = f":{node_type}" if node_type else ""
        match = f"""
            MATCH (n{label})
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
        parameters = {"terms": terms, "doc_type": doc_type or "", "limit": limit, "offset": offset}
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
            result = await session.run(match + f"""
                WITH n, score
                WHERE $doc_type = '' OR coalesce(toStringOrNull(n.doc_type), 'unknown') = $doc_type
                RETURN labels(n) AS labels, properties(n) AS props
                ORDER BY {orders[sort]} {direction.upper()},
                         coalesce(toStringOrNull(n.date), '') DESC,
                         coalesce(toStringOrNull(n.uuid), toStringOrNull(n.paperless_id), elementId(n)) ASC
                SKIP $offset LIMIT $limit
            """, **parameters)
            rows = [{"labels": row["labels"], "properties": row["props"]} async for row in result]
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
