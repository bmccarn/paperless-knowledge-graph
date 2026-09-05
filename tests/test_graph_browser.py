"""Exercise the actual graph browsing methods against disposable Neo4j 5.

Imports the real application module with synthetic client settings and uses a
small HTTP driver adapter. Cypher runs in Neo4j; it is not simulated.
See docs/audits/graph-validation.md.
"""
import json
import os
import unittest
from urllib.request import Request, urlopen
from urllib.parse import urlparse

from tests.runtime import configure_test_environment

configure_test_environment()

from app.graph import GraphStore

TEST_URL = os.environ.get("NEO4J_TEST_URL", "")


class QueryError(Exception):
    def __init__(self, error):
        self.code = error["code"]
        super().__init__(error["message"])


class Result:
    def __init__(self, rows):
        self.rows = rows

    async def single(self):
        return self.rows[0] if self.rows else None

    def __aiter__(self):
        async def rows():
            for row in self.rows:
                yield row
        return rows()


class HttpDriver:
    def __init__(self, missing_apoc=False, injected_error=None):
        self.missing_apoc = missing_apoc
        self.injected_error = injected_error

    def execute(self, query, parameters=None):
        request = Request(TEST_URL, data=json.dumps({"statements": [{"statement": query, "parameters": parameters or {}}]}).encode(), headers={"Content-Type": "application/json"})
        with urlopen(request, timeout=30) as response:
            result = json.load(response)
        if result["errors"]:
            raise QueryError(result["errors"][0])
        result = result["results"][0]
        return [dict(zip(result["columns"], row["row"])) for row in result["data"]]

    def session(self):
        return self

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        pass

    async def run(self, query, **parameters):
        if "apoc.path.subgraphAll" in query:
            if self.injected_error:
                raise QueryError(self.injected_error)
            if self.missing_apoc:
                # Execute an actually missing procedure so the real server emits its error.
                query = query.replace("apoc.path.subgraphAll", "audit_missing.subgraphAll")
        return Result(self.execute(query, parameters))


def node_ids(graph):
    return {node["props"].get("uuid") or f"doc-{node['props']['paperless_id']}" for node in graph["nodes"]}


@unittest.skipUnless(TEST_URL, "Set NEO4J_TEST_URL to a disposable local Neo4j HTTP transaction endpoint")
class GraphBrowserTests(unittest.IsolatedAsyncioTestCase):
    @classmethod
    def setUpClass(cls):
        if urlparse(TEST_URL).hostname not in {"127.0.0.1", "localhost"}:
            raise ValueError("Graph tests require a disposable localhost database")
        cls.driver = HttpDriver()
        if cls.driver.execute("MATCH (n) RETURN count(n) AS count")[0]["count"]:
            raise ValueError("Refusing to seed a nonempty database; use the documented disposable container")
        cls.driver.execute("""
            CREATE (p:Person {uuid: 'audit-person', name: 'Alex Example'}),
                   (o:Organization {uuid: 'audit-org', name: 'Example Utility'}),
                   (d:Document {paperless_id: 100001, title: 'January statement'}),
                   (e:Document {paperless_id: 100002, title: 'February statement'}),
                   (:Document {paperless_id: 100003, title: 'Isolated document'}),
                   (p)-[:CUSTOMER_OF {source_doc: 100001, implied: true}]->(o),
                   (d)-[:INVOICED_BY {source_doc: 100001}]->(o),
                   (e)-[:INVOICED_BY {source_doc: 100002}]->(o)
        """)

    def store(self, **kwargs):
        store = GraphStore()
        store.driver = HttpDriver(**kwargs)
        return store

    @classmethod
    def tearDownClass(cls):
        cls.driver.execute("MATCH (n) WHERE n.uuid IN ['audit-person', 'audit-org'] OR n.paperless_id IN [100001, 100002, 100003] DETACH DELETE n")

    async def test_document_neighbors_preserve_identity_and_sources(self):
        graph = await self.store().get_neighbors("doc-100001", 1)
        self.assertEqual(node_ids(graph), {"doc-100001", "audit-org"})
        self.assertEqual(len(graph["relationships"]), 1)
        edge = graph["relationships"][0]
        self.assertEqual((edge["start"], edge["end"]), ("doc-100001", "audit-org"))
        self.assertEqual(edge["props"]["source_doc"], 100001)
        self.assertTrue(edge["id"])

    async def test_initial_and_expansion_share_stable_unique_edge_ids(self):
        store = self.store()
        initial = await store.get_initial_graph(100)
        expanded = await store.get_neighbors("audit-org", 1)
        initial_ids = [edge["id"] for edge in initial["relationships"]]
        self.assertEqual(len(initial_ids), 3)
        self.assertEqual(len(set(initial_ids)), 3)
        self.assertEqual(set(initial_ids), {edge["id"] for edge in expanded["relationships"]})

    async def test_no_apoc_matches_one_hop_contract(self):
        graph = await self.store(missing_apoc=True).get_neighbors("doc-100001", 1)
        self.assertEqual(node_ids(graph), {"doc-100001", "audit-org"})
        self.assertEqual(len(graph["relationships"]), 1)
        self.assertEqual(graph["relationships"][0]["start"], "doc-100001")

    async def test_no_apoc_retains_isolated_node(self):
        graph = await self.store(missing_apoc=True).get_neighbors("doc-100003", 1)
        self.assertEqual(node_ids(graph), {"doc-100003"})
        self.assertEqual(graph["relationships"], [])

    async def test_unrelated_database_errors_are_not_hidden(self):
        with self.assertRaises(QueryError):
            await self.store(injected_error={"code": "Neo.ClientError.Security.Unauthorized", "message": "Synthetic authorization error"}).get_neighbors("audit-org", 1)

    async def test_node_details_report_real_edge_direction(self):
        detail = await self.store().get_node("audit-person")
        self.assertEqual(detail["relationships"][0]["direction"], "out")
        detail = await self.store().get_node("doc-100001")
        self.assertEqual(detail["properties"]["paperless_id"], 100001)


if __name__ == "__main__":
    unittest.main()
