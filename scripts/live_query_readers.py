"""Read-only datastore attachment for an admitted, isolated evaluation process."""
from contextlib import AsyncExitStack, asynccontextmanager
import copy
import json

from scripts.eval_source_audit import write_private
from scripts.live_query_capture import CapturedClient
from scripts.live_query_evaluation import sha256


GRAPH_READS = frozenset({
    'get_all_document_ids', 'get_document_dates', 'get_document_entities', 'get_documents_by_entity_types',
    'get_recent_docs_per_organization', 'get_recent_docs_per_organization_filtered',
    'get_subgraph', 'search_nodes', 'get_node',
})
VECTOR_READS = frozenset({
    'get_document_embedding_ids', 'get_document_hash_ids', 'get_ingestion_fingerprints',
    'entity_keyword_search', 'entity_vector_search', 'get_chunks_for_documents',
    'get_incomplete_document_ids', 'get_open_feedback_document_ids',
    'historical_document_candidates', 'keyword_search', 'vector_search',
    'vector_search_by_doc_ids', 'get_doc_hash', 'acquisition_document_page',
})


class ReadMethods:
    def __init__(self, store, allowed):
        self._store, self._allowed = store, allowed
        self.denied = []

    def __getattr__(self, name):
        if name not in self._allowed:
            self.denied.append(name)
            raise AttributeError('Datastore operation not admitted for evaluation')
        return getattr(self._store, name)


class ReadSessions:
    def __init__(self, driver):
        self._driver = driver

    def session(self, **kwargs):
        if kwargs.pop('default_access_mode', 'READ') != 'READ':
            raise ValueError('Only read sessions are admitted')
        return self._driver.session(default_access_mode='READ', **kwargs)


class CapturedDocuments:
    """Retain actual retrieval originals; never preload rubric documents."""
    def __init__(self, client, directory):
        self._client, self.directory = client, directory
        self.documents = {}
        self.hashes = {}
        self.changed = False
        self._seen = set()

    async def get_skip_tag_ids(self):
        ids = await self._client.get_skip_tag_ids()
        path = self.directory / 'skip-tag-ids.json'
        value = sorted(ids)
        if path.exists():
            if json.loads(path.read_text()) != value:
                self.changed = True
                raise ValueError('Eligibility tags changed during retrieval')
        else:
            write_private(path, value)
        return set(ids)

    async def get_document(self, document_id):
        if type(document_id) is not int or document_id <= 0:
            self.changed = True
            raise ValueError('Original document identity must be a positive integer')
        document = await self._client.get_document(document_id)
        encoded = json.dumps(document, sort_keys=True, ensure_ascii=False).encode()
        digest = sha256(encoded)
        name = f'document-{len(self.hashes):04d}.json'
        if digest not in self._seen:
            write_private(self.directory / name, document)
            self.hashes[name] = sha256((self.directory / name).read_bytes())
            self._seen.add(digest)
        if (type(document.get('id')) is not int or document['id'] <= 0 or document['id'] != document_id
                or document_id in self.documents and self.documents[document_id] != digest):
            self.changed = True
            raise ValueError('Original document identity or snapshot changed during retrieval')
        self.documents[document_id] = digest
        return copy.deepcopy(document)


@asynccontextmanager
async def attach_readers(capture, originals_directory):
    """Caller must validate admission and routes before entering this context.

    No application lifespan or store init method is invoked. This function refuses
    already initialized stores, so it cannot attach to the serving process.
    """
    import asyncpg
    from neo4j import AsyncGraphDatabase
    from app import query
    from app.cache import TTLCache
    from app.config import settings

    graph, vectors = query.graph_store, query.embeddings_store
    if graph.driver is not None or vectors.pool is not None:
        raise ValueError('Live evaluation requires an isolated uninitialized process')
    original_client = vectors.openai
    original_retries = original_client.max_retries
    saved = {name: getattr(query, name) for name in (
        'graph_store', 'embeddings_store', 'paperless_client',
        'query_cache', 'vector_cache', 'graph_cache',
    )}
    async with AsyncExitStack() as stack:
        def restore():
            for name, value in saved.items():
                setattr(query, name, value)
            graph.driver = None
            vectors.pool = None
            vectors.openai = original_client
            original_client.max_retries = original_retries

        stack.callback(restore)
        stack.push_async_callback(original_client.close)
        pool = await asyncpg.create_pool(
            host=settings.postgres_host, port=settings.postgres_port,
            database=settings.postgres_db, user=settings.postgres_user,
            password=settings.postgres_password, min_size=1, max_size=4,
            server_settings={'default_transaction_read_only': 'on'},
        )
        stack.push_async_callback(pool.close)
        async with pool.acquire() as connection:
            if await connection.fetchval('SHOW default_transaction_read_only') != 'on':
                raise ValueError('PostgreSQL evaluation session is not read-only')
        driver = AsyncGraphDatabase.driver(settings.neo4j_uri,
                                           auth=(settings.neo4j_user, settings.neo4j_password))
        stack.push_async_callback(driver.close)
        await driver.verify_connectivity()
        graph.driver = ReadSessions(driver)
        vectors.pool = pool
        original_client.max_retries = 0
        vectors.openai = CapturedClient(original_client, capture, label='retrieval', bypass_cache=True)
        originals_directory.mkdir(mode=0o700)
        documents = CapturedDocuments(saved['paperless_client'], originals_directory)
        graph_reads, vector_reads = ReadMethods(graph, GRAPH_READS), ReadMethods(vectors, VECTOR_READS)
        query.graph_store, query.embeddings_store = graph_reads, vector_reads
        query.paperless_client = documents
        query.query_cache, query.vector_cache, query.graph_cache = (TTLCache() for _ in range(3))
        yield {'documents': documents, 'graph_reads': graph_reads, 'vector_reads': vector_reads}
