import logging
import uuid
from typing import Any, Optional

from neo4j import AsyncGraphDatabase

from app.config import settings

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
        async with self.driver.session() as session:
            await session.run(query, **props)
        return node_uuid

    async def create_relationship(self, from_uuid: str, from_label: str,
                                   to_uuid: str, to_label: str,
                                   rel_type: str, properties: dict = None):
        """Create a relationship between two nodes."""
        props = properties or {}
        props_str = ""
        if props:
            props_str = " {" + ", ".join(f"{k}: ${k}" for k in props) + "}"
        # Use uuid matching, try multiple label combinations
        query = f"""
            MATCH (a {{{("uuid" if from_label != "Document" else "uuid")}: $from_uuid}})
            MATCH (b {{{("uuid" if to_label != "Document" else "uuid")}: $to_uuid}})
            MERGE (a)-[r:{rel_type}]->(b)
            SET r += $props
        """
        # Simpler approach: match by uuid across all labels
        query = f"""
            MATCH (a) WHERE a.uuid = $from_uuid OR a.paperless_id = $from_pid
            MATCH (b) WHERE b.uuid = $to_uuid OR b.paperless_id = $to_pid
            MERGE (a)-[r:{rel_type}]->(b)
            SET r += $props
        """
        async with self.driver.session() as session:
            await session.run(
                query,
                from_uuid=from_uuid, from_pid=_try_int(from_uuid),
                to_uuid=to_uuid, to_pid=_try_int(to_uuid),
                props=props,
            )

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
            return {
                "nodes": node_record["count"] if node_record else 0,
                "relationships": rel_record["count"] if rel_record else 0,
                "documents": doc_record["count"] if doc_record else 0,
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


def _try_int(val: str) -> int:
    try:
        return int(val)
    except (ValueError, TypeError):
        return -1


graph_store = GraphStore()
