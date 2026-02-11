import logging
import uuid
from typing import Any, Optional

from neo4j import AsyncGraphDatabase

from app.config import settings
from app.retry import retry_db

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

    async def create_document_node(self, paperless_id: int, title: str, doc_type: str,
                                    date: str, content_hash: str) -> str:
        """Create or update a Document node. Returns the paperless_id."""
        async def _op():
            async with self.driver.session() as session:
                await session.run(
                    """
                    MERGE (d:Document {paperless_id: $pid})
                    SET d.title = $title, d.doc_type = $doc_type, d.date = $date,
                        d.content_hash = $hash, d.processed_at = datetime()
                    """,
                    pid=paperless_id, title=title, doc_type=doc_type,
                    date=date or "", hash=content_hash,
                )
        await retry_db(_op, operation='create_document_node')
        return str(paperless_id)

    async def find_person(self, name: str) -> Optional[dict]:
        """Find a person by name or alias."""
        async with self.driver.session() as session:
            result = await session.run(
                """
                MATCH (p:Person)
                WHERE toLower(p.name) = toLower($name)
                   OR any(a IN p.aliases WHERE toLower(a) = toLower($name))
                RETURN p.uuid AS uuid, p.name AS name, p.aliases AS aliases
                LIMIT 1
                """,
                name=name,
            )
            record = await result.single()
            return dict(record) if record else None

    async def get_all_persons(self) -> list[dict]:
        async with self.driver.session() as session:
            result = await session.run(
                "MATCH (p:Person) RETURN p.uuid AS uuid, p.name AS name, p.aliases AS aliases"
            )
            return [dict(r) async for r in result]

    async def get_all_organizations(self) -> list[dict]:
        async with self.driver.session() as session:
            result = await session.run(
                "MATCH (o:Organization) RETURN o.uuid AS uuid, o.name AS name, o.aliases AS aliases, o.type AS type"
            )
            return [dict(r) async for r in result]

    async def find_organization(self, name: str) -> Optional[dict]:
        async with self.driver.session() as session:
            result = await session.run(
                """
                MATCH (o:Organization)
                WHERE toLower(o.name) = toLower($name)
                   OR any(a IN o.aliases WHERE toLower(a) = toLower($name))
                RETURN o.uuid AS uuid, o.name AS name, o.aliases AS aliases, o.type AS type
                LIMIT 1
                """,
                name=name,
            )
            record = await result.single()
            return dict(record) if record else None

    async def create_person(self, name: str, aliases: list[str] = None, role: str = None) -> str:
        node_uuid = self.new_uuid()
        async with self.driver.session() as session:
            await session.run(
                """
                CREATE (p:Person {uuid: $uuid, name: $name, aliases: $aliases, role: $role})
                """,
                uuid=node_uuid, name=name, aliases=aliases or [], role=role or "",
            )
        return node_uuid

    async def add_person_alias(self, node_uuid: str, alias: str):
        async with self.driver.session() as session:
            await session.run(
                """
                MATCH (p:Person {uuid: $uuid})
                SET p.aliases = CASE
                    WHEN NOT $alias IN p.aliases THEN p.aliases + $alias
                    ELSE p.aliases
                END
                """,
                uuid=node_uuid, alias=alias,
            )

    async def create_organization(self, name: str, org_type: str = None,
                                   aliases: list[str] = None) -> str:
        node_uuid = self.new_uuid()
        async with self.driver.session() as session:
            await session.run(
                """
                CREATE (o:Organization {uuid: $uuid, name: $name, type: $type, aliases: $aliases})
                """,
                uuid=node_uuid, name=name, type=org_type or "", aliases=aliases or [],
            )
        return node_uuid

    async def add_org_alias(self, node_uuid: str, alias: str):
        async with self.driver.session() as session:
            await session.run(
                """
                MATCH (o:Organization {uuid: $uuid})
                SET o.aliases = CASE
                    WHEN NOT $alias IN o.aliases THEN o.aliases + $alias
                    ELSE o.aliases
                END
                """,
                uuid=node_uuid, alias=alias,
            )

    async def create_node(self, label: str, properties: dict) -> str:
        """Create a generic node with given label and properties."""
        node_uuid = self.new_uuid()
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
        """Create a relationship between two nodes. Increments weight on duplicate."""
        props = properties or {}
        # Use MERGE to avoid duplicates and track weight
        query = f"""
            MATCH (a) WHERE a.uuid = $from_uuid OR a.paperless_id = $from_pid
            MATCH (b) WHERE b.uuid = $to_uuid OR b.paperless_id = $to_pid
            MERGE (a)-[r:{rel_type}]->(b)
            ON CREATE SET r = $props, r.weight = 1
            ON MATCH SET r.weight = coalesce(r.weight, 1) + 1, r += $props
        """
        async def _op():
            async with self.driver.session() as session:
                await session.run(
                    query,
                    from_uuid=from_uuid, from_pid=_try_int(from_uuid),
                    to_uuid=to_uuid, to_pid=_try_int(to_uuid),
                    props=props,
                )
        await retry_db(_op, operation='create_relationship')

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

    async def delete_document_graph(self, paperless_id: int):
        """Remove all nodes and relationships sourced from a document."""
        async with self.driver.session() as session:
            # Delete relationships with source_doc
            await session.run(
                """
                MATCH ()-[r]->()
                WHERE r.source_doc = $pid
                DELETE r
                """,
                pid=paperless_id,
            )
            # Delete the document node
            await session.run(
                "MATCH (d:Document {paperless_id: $pid}) DETACH DELETE d",
                pid=paperless_id,
            )
            # Clean up orphan nodes (no relationships)
            await session.run(
                """
                MATCH (n)
                WHERE NOT n:Document AND NOT EXISTS { (n)--() }
                DELETE n
                """
            )

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

    async def search_nodes(self, query: str, node_type: str = None, limit: int = 20) -> list[dict]:
        type_filter = f":{node_type}" if node_type else ""
        async with self.driver.session() as session:
            result = await session.run(
                f"""
                MATCH (n{type_filter})
                WHERE toLower(n.name) CONTAINS toLower($q)
                   OR toLower(n.title) CONTAINS toLower($q)
                   OR any(a IN n.aliases WHERE toLower(a) CONTAINS toLower($q))
                RETURN labels(n) AS labels, properties(n) AS props
                LIMIT $limit
                """,
                q=query, limit=limit,
            )
            return [{"labels": r["labels"], "properties": r["props"]} async for r in result]

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
        async with self.driver.session() as session:
            result = await session.run(
                f"""
                MATCH (start) WHERE start.uuid = $uuid OR start.paperless_id = $pid
                CALL apoc.path.subgraphAll(start, {{maxLevel: $depth}})
                YIELD nodes, relationships
                RETURN [n IN nodes | {{labels: labels(n), props: properties(n)}}] AS nodes,
                       [r IN relationships | {{type: type(r), props: properties(r),
                        start: properties(startNode(r)).uuid, end: properties(endNode(r)).uuid}}] AS rels
                """,
                uuid=node_uuid, pid=_try_int(node_uuid), depth=depth,
            )
            record = await result.single()
            if not record:
                # Fallback without APOC
                return await self._get_neighbors_no_apoc(node_uuid, depth)
            return {"nodes": record["nodes"], "relationships": record["rels"]}

    async def _get_neighbors_no_apoc(self, node_uuid: str, depth: int) -> dict:
        """Fallback neighborhood query without APOC."""
        async with self.driver.session() as session:
            result = await session.run(
                """
                MATCH path = (start)-[*1..3]-(end)
                WHERE start.uuid = $uuid OR start.paperless_id = $pid
                UNWIND nodes(path) AS n
                UNWIND relationships(path) AS r
                WITH collect(DISTINCT {labels: labels(n), props: properties(n)}) AS nodes,
                     collect(DISTINCT {type: type(r), props: properties(r),
                             start_uuid: properties(startNode(r)).uuid,
                             end_uuid: properties(endNode(r)).uuid}) AS rels
                RETURN nodes, rels
                """,
                uuid=node_uuid, pid=_try_int(node_uuid),
            )
            record = await result.single()
            if not record:
                return {"nodes": [], "relationships": []}
            return {"nodes": record["nodes"], "relationships": record["rels"]}

    async def get_initial_graph(self, limit: int = 300) -> dict:
        """Get an initial graph view with Person/Organization nodes and their connections."""
        async with self.driver.session() as session:
            # Get Person and Organization nodes
            node_result = await session.run(
                """
                MATCH (n)
                WHERE n:Person OR n:Organization
                RETURN labels(n) AS labels, properties(n) AS props
                ORDER BY COUNT { (n)--() } DESC
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
                    type(r) AS rel_type, properties(r) AS rel_props,
                    properties(startNode(r)).uuid AS start_uuid,
                    properties(endNode(r)).uuid AS end_uuid,
                    startNode(r).paperless_id AS start_pid,
                    endNode(r).paperless_id AS end_pid
                LIMIT 1000
                """,
                uuids=uuids,
            )

            all_nodes = {n["props"].get("uuid"): n for n in nodes}
            relationships = []

            async for r in rel_result:
                # Add connected nodes we haven't seen
                for prefix in ["a", "b"]:
                    props = r[f"{prefix}_props"]
                    uid = props.get("uuid") or f"doc-{props.get('paperless_id', '')}"
                    if uid and uid not in all_nodes:
                        all_nodes[uid] = {"labels": r[f"{prefix}_labels"], "props": props}

                start = r["start_uuid"] or f"doc-{r['start_pid']}"
                end = r["end_uuid"] or f"doc-{r['end_pid']}"
                if start and end:
                    relationships.append({
                        "type": r["rel_type"],
                        "props": r["rel_props"],
                        "start": start,
                        "end": end,
                    })

            return {
                "nodes": list(all_nodes.values()),
                "relationships": relationships,
            }

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


def _try_int(val: str) -> int:
    try:
        if isinstance(val, str) and val.startswith("doc-"):
            val = val[4:]
        return int(val)
    except (ValueError, TypeError):
        return -1



graph_store = GraphStore()
