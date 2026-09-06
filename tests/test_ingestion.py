"""Ingestion behavior through real modules and controlled dependency adapters."""
import asyncio
import copy
from datetime import datetime, timezone
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from tests.runtime import configure_test_environment

configure_test_environment()

from app import pipeline
from app.paperless import PaperlessClient
from tests.test_extraction_recovery import SDKWire, SOURCE, STAGES


class PaperlessFixture:
    def __init__(self, documents):
        self.documents = {doc['id']: copy.deepcopy(doc) for doc in documents}
        self.fetches = []

    async def get_all_documents(self, modified_after=None, page_size=100):
        self.fetches.append(modified_after)
        return [copy.deepcopy(doc) for doc in self.documents.values()
                if modified_after is None or datetime.fromisoformat(doc['modified']) > modified_after]

    async def get_document(self, doc_id):
        return copy.deepcopy(self.documents[doc_id])

    async def get_skip_tag_ids(self):
        return {99}

    has_any_tag = staticmethod(PaperlessClient.has_any_tag)
    partition_indexable_documents = PaperlessClient.partition_indexable_documents


class GraphFixture:
    def __init__(self):
        self.documents = {}
        self.fail_delete = False
        self.fail_create = False
        self.deleted = []
        self.relationships = []

    async def get_all_document_ids(self):
        return set(self.documents)

    async def delete_document_graph(self, doc_id):
        self.deleted.append(doc_id)
        if self.fail_delete:
            raise RuntimeError('graph delete failed')
        self.documents.pop(doc_id, None)

    async def create_document_node(self, **document):
        if self.fail_create:
            raise RuntimeError('graph create failed')
        self.documents[document['paperless_id']] = copy.deepcopy(document)
        return str(document['paperless_id'])

    async def create_relationship(self, *args):
        self.relationships.append(args)

    async def search_nodes(self, *args, **kwargs):
        return []


class EmbeddingsFixture:
    def __init__(self):
        self.hashes = {}
        self.fingerprints = {}
        self.chunks = {}
        self.last_sync = datetime(2026, 1, 1, tzinfo=timezone.utc)
        self.fail_generate = False
        self.fail_store = False
        self.generated = []
        self.writes = []
        self.during_store = None

    async def get_last_sync(self):
        return self.last_sync

    async def set_last_sync(self, value):
        self.last_sync = value

    async def get_document_embedding_ids(self):
        return {doc_id for doc_id, _ in self.chunks}

    async def get_document_hash_ids(self):
        return set(self.hashes)

    async def get_doc_hash(self, doc_id):
        return self.hashes.get(doc_id)

    async def delete_doc_hash(self, doc_id):
        self.hashes.pop(doc_id, None)
        self.fingerprints.pop(doc_id, None)
        self.writes.append(('delete_hash', doc_id))

    async def get_ingestion_fingerprints(self, doc_ids=None):
        return {doc_id: self.fingerprints.get(doc_id) for doc_id in self.hashes
                if doc_ids is None or doc_id in doc_ids}

    async def set_doc_hash(self, doc_id, value, *, ingestion_fingerprint=None):
        self.hashes[doc_id] = value
        self.fingerprints[doc_id] = ingestion_fingerprint
        self.writes.append(('commit_hash', doc_id))

    async def delete_document_embeddings(self, doc_id):
        self.chunks = {key: value for key, value in self.chunks.items() if key[0] != doc_id}

    async def generate_embedding(self, text):
        self.generated.append(text)
        return [] if self.fail_generate else [0.1, 0.2, 0.3]

    async def store_document_embedding(self, doc_id, content, chunk_index=0, **kwargs):
        if self.during_store:
            self.during_store()
        if self.fail_store:
            raise RuntimeError('chunk write failed')
        assert kwargs.get('embedding'), 'vectors must be prepared before replacement'
        self.chunks[doc_id, chunk_index] = content
        self.writes.append(('chunk', doc_id))

    async def create_vector_indexes(self):
        pass


class ClassifierFixture:
    fail = False

    async def classify(self, title, content):
        if self.fail:
            raise RuntimeError('classification failed')
        return {'doc_type': 'generic', 'confidence': 0.9}


class ExtractionFixture:
    def __init__(self):
        self.fail = False
        self.coverage = 'complete'
        self.started = None
        self.release = None
        self.seen = []

    async def extract(self, title, content, doc_type):
        self.seen.append(content)
        if self.started is not None:
            self.started.set()
            await self.release.wait()
        if self.fail:
            raise RuntimeError('extraction failed')
        return {'confidence': 0.9, 'all_entities': [], 'people': [], 'organizations': [],
                'extraction_coverage': {'status': self.coverage, 'total_characters': len(content),
                                        'covered_characters': len(content)},
                'metadata_evidence': {}, 'metadata_conflicts': [], 'extraction_issues': []}


class ResolverFixture:
    def __init__(self):
        self.hydrated = 0

    async def hydrate_review_identities(self):
        self.hydrated += 1

    async def resolve_all_entities(self):
        return {'total_merged': 0}


class SummaryModelFixture:
    def __init__(self, **kwargs):
        self.chat = SimpleNamespace(completions=self)

    async def create(self, **kwargs):
        return SimpleNamespace(choices=[SimpleNamespace(message=SimpleNamespace(content='Synthetic summary. ' * 20))])

    async def close(self):
        pass


def document(doc_id=1, content='Source statement amount $25.', modified='2026-02-01T00:00:00+00:00'):
    return {'id': doc_id, 'title': f'Document {doc_id}', 'content': content,
            'modified': modified, 'created': '2026-01-01', 'tags': []}


class IngestionTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.paperless = PaperlessFixture([document()])
        self.graph = GraphFixture()
        self.embeddings = EmbeddingsFixture()
        self.classifier = ClassifierFixture()
        self.extractor = ExtractionFixture()
        self.resolver = ResolverFixture()
        self.invalidations = []
        self.cached_answers = {}

        def invalidate():
            self.invalidations.append(True)
            self.cached_answers.clear()

        self.patches = [
            patch.object(pipeline, 'paperless_client', self.paperless),
            patch.object(pipeline, 'graph_store', self.graph),
            patch.object(pipeline, 'embeddings_store', self.embeddings),
            patch.object(pipeline, 'classifier', self.classifier),
            patch.object(pipeline, 'extractor', self.extractor),
            patch.object(pipeline, 'entity_resolver', self.resolver),
            patch.object(pipeline, 'invalidate_on_sync', invalidate),
            patch.object(pipeline.settings, 'max_concurrent_docs', 1),
            patch('openai.AsyncOpenAI', SummaryModelFixture),
        ]
        for item in self.patches:
            item.start()
        self.addCleanup(lambda: [item.stop() for item in reversed(self.patches)])

    def existing(self, doc_id=1):
        self.graph.documents[doc_id] = {'paperless_id': doc_id, 'title': 'Existing usable document'}
        self.embeddings.chunks[doc_id, 0] = 'Existing usable chunk'
        self.embeddings.hashes[doc_id] = PaperlessClient.content_hash(self.paperless.documents[doc_id]['content'])

    async def test_failed_document_retries_without_paperless_change(self):
        previous = self.embeddings.last_sync
        self.extractor.fail = True
        result = await pipeline.sync_documents()
        self.assertEqual(result['status'], 'failed')
        self.assertEqual(self.embeddings.last_sync, previous)
        self.extractor.fail = False
        result = await pipeline.sync_documents()
        self.assertEqual(result['processed'], 1)
        self.assertTrue(result['checkpoint_advanced'])
        self.assertEqual(self.embeddings.last_sync.isoformat(), result['scan_started_at'])
        self.assertIn(1, self.embeddings.hashes)

    async def test_large_legacy_document_finishes_backfill_and_is_not_reprocessed(self):
        from app.extractor import EntityExtractor
        from tests.test_extraction import CompletionClient
        tail = "Premium: 125.00 USD."
        source = ("Blank filler. " * 100000)[:1_211_567 - len(tail)] + tail
        self.paperless.documents[1] = document(content=source)
        self.existing()
        self.embeddings.last_sync = datetime(2026, 8, 1, tzinfo=timezone.utc)
        client = CompletionClient()
        with patch.object(pipeline, "extractor", EntityExtractor(client)):
            result = await pipeline.sync_documents()
            self.assertEqual(result["errors"], 0)
            self.assertTrue(result["checkpoint_advanced"])
            self.assertEqual(result["processed"], 1)
            coverage = self.graph.documents[1]["extraction_metadata"]["extraction_coverage"]
            self.assertEqual(coverage["status"], "complete")
            self.assertEqual(coverage["covered_characters"], len(source))
            self.assertEqual(coverage["windows"][-1]["end"], len(source))
            self.assertTrue(any(tail in chunk for chunk in self.embeddings.chunks.values()))
            provenance = self.graph.documents[1]["extraction_metadata"]["metadata_evidence"]
            self.assertTrue(any(span["start"] > 359_200 and source[span["start"]:span["end"]] == span["quote"]
                                for window in provenance.values() for span in window.get("premium", [])))
            self.assertEqual(self.embeddings.fingerprints[1],
                             PaperlessClient.ingestion_fingerprint(self.paperless.documents[1]))
            self.assertEqual((await pipeline.sync_documents())["processed"], 0)

    async def test_returned_resolution_errors_fail_reindex_without_advancing_checkpoint(self):
        previous = self.embeddings.last_sync
        async def failed_resolution():
            return {"total_merged": 0, "errors": ["Synthetic merge failure"]}
        self.resolver.resolve_all_entities = failed_resolution
        result = await pipeline.reindex_all()
        self.assertEqual(result["status"], "failed")
        self.assertEqual(result["errors"], 1)
        self.assertFalse(result["checkpoint_advanced"])
        self.assertEqual(self.embeddings.last_sync, previous)
        self.assertIn("Synthetic merge failure", result["postprocess_errors"][0])

    async def test_metadata_corrections_refresh_index_without_changing_ocr_hash(self):
        await pipeline.sync_documents()
        original_hash = self.embeddings.hashes[1]
        self.paperless.documents[1].update(title="Corrected title", created="2026-03-01")
        # Reconciliation must find metadata drift even behind a prior checkpoint.
        result = await pipeline.sync_documents()
        self.assertEqual(result["processed"], 1)
        self.assertEqual(self.graph.documents[1]["title"], "Corrected title")
        self.assertEqual(self.graph.documents[1]["date"], "2026-03-01")
        self.assertIn("Corrected title", self.embeddings.chunks[1, 0])
        self.assertEqual(self.embeddings.hashes[1], original_hash)
        self.assertEqual((await pipeline.sync_documents())["processed"], 0)

    async def test_legacy_completion_without_metadata_fingerprint_is_reconciled(self):
        self.existing()
        self.embeddings.last_sync = datetime(2026, 8, 1, tzinfo=timezone.utc)
        self.assertEqual((await pipeline.sync_documents())["processed"], 1)

    async def test_primary_model_change_reconciles_once_without_changing_ocr_identity(self):
        from app.config import settings
        with patch.object(settings, "gemini_model", "gemini-3.5-flash"):
            await pipeline.sync_documents()
        previous_hash = self.embeddings.hashes[1]
        previous_fingerprint = self.embeddings.fingerprints[1]
        with patch.object(settings, "gemini_model", "gemini-3.8-flash"):
            result = await pipeline.sync_documents()
            self.assertEqual(result["errors"], 0)
            self.assertEqual(result["processed"], 1)
            self.assertEqual(self.embeddings.hashes[1], previous_hash)
            self.assertNotEqual(self.embeddings.fingerprints[1], previous_fingerprint)
            self.assertEqual((await pipeline.sync_documents())["processed"], 0)

    async def test_freshness_exposes_metadata_drift_behind_checkpoint(self):
        import httpx
        import app.main as main
        from unittest.mock import AsyncMock
        await pipeline.sync_documents()
        self.paperless.documents[1]["title"] = "Corrected after index"
        with patch.object(main, "paperless_client", self.paperless), \
             patch.object(main, "embeddings_store", self.embeddings), \
             patch.object(main.graph_store, "get_all_document_ids", AsyncMock(return_value={1})), \
             patch.object(main.graph_store, "get_counts", AsyncMock(return_value={"documents": 1})), \
             patch.object(main, "_freshness_cache", None):
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=main.app), base_url="http://test") as client:
                response = await client.get("/freshness", params={"force": "true"})
        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.json()["stale"])
        self.assertEqual(response.json()["drift"]["changed_since_index"][0]["id"], 1)

    async def test_scan_watermark_retains_changes_made_during_processing(self):
        self.extractor.started, self.extractor.release = asyncio.Event(), asyncio.Event()
        running = asyncio.create_task(pipeline.sync_documents())
        await self.extractor.started.wait()
        changed = 'Source statement amount $35.'
        self.paperless.documents[1].update(content=changed, modified=datetime.now(timezone.utc).isoformat())
        self.extractor.release.set()
        first = await running
        self.assertEqual(self.embeddings.last_sync.isoformat(), first['scan_started_at'])
        self.extractor.started = None
        second = await pipeline.sync_documents()
        self.assertEqual(second['processed'], 1)
        self.assertEqual(self.embeddings.hashes[1], PaperlessClient.content_hash(changed))

    async def test_missing_derived_state_recovers_old_documents(self):
        for missing in ('graph', 'chunks', 'hash'):
            with self.subTest(missing=missing):
                self.existing()
                self.embeddings.last_sync = datetime(2026, 8, 1, tzinfo=timezone.utc)
                if missing == 'graph': self.graph.documents.clear()
                if missing == 'chunks': self.embeddings.chunks.clear()
                if missing == 'hash': self.embeddings.hashes.clear()
                result = await pipeline.sync_documents()
                self.assertEqual(result['processed'], 1)
                self.assertIn(1, self.graph.documents)
                self.assertIn((1, 0), self.embeddings.chunks)
                self.assertIn(1, self.embeddings.hashes)

    async def test_preparation_failures_preserve_existing_graph_and_chunks(self):
        for stage in ('classify', 'extract', 'coverage', 'vector'):
            with self.subTest(stage=stage):
                self.existing()
                self.classifier.fail = stage == 'classify'
                self.extractor.fail = stage == 'extract'
                self.extractor.coverage = 'partial' if stage == 'coverage' else 'complete'
                self.embeddings.fail_generate = stage == 'vector'
                result = await pipeline.reindex_document(1)
                self.assertEqual(result['status'], 'error')
                self.assertEqual(self.graph.documents[1]['title'], 'Existing usable document')
                self.assertEqual(self.embeddings.chunks[1, 0], 'Existing usable chunk')
                self.assertNotIn(1, self.embeddings.hashes)
                self.assertEqual(self.graph.deleted, [])

    async def test_failed_targeted_preparation_retries_in_later_sync(self):
        self.existing()
        self.embeddings.last_sync = datetime(2026, 8, 1, tzinfo=timezone.utc)
        self.extractor.fail = True
        self.assertEqual((await pipeline.reindex_document(1))['status'], 'error')
        self.extractor.fail = False
        self.assertEqual((await pipeline.sync_documents())['processed'], 1)

    async def test_persistent_adaptive_truncation_never_commits_or_replaces_old_index(self):
        from app.extractor import EntityExtractor
        from tests.test_extraction import CompletionClient
        self.existing()
        self.paperless.documents[1] = document(content="Alice Example " * 40)
        previous_checkpoint = self.embeddings.last_sync
        client = CompletionClient()
        client.finish_reason = "length"
        with patch.object(pipeline, "extractor", EntityExtractor(
                client, window_characters=600, overlap_characters=40, minimum_split_characters=180)):
            result = await pipeline.reindex_document(1)
        self.assertEqual(result["status"], "error")
        self.assertEqual(self.graph.documents[1]["title"], "Existing usable document")
        self.assertEqual(self.embeddings.chunks[1, 0], "Existing usable chunk")
        self.assertNotIn(1, self.embeddings.hashes)
        self.assertNotIn(1, self.embeddings.fingerprints)
        self.assertEqual(self.graph.deleted, [])
        self.assertEqual(self.embeddings.last_sync, previous_checkpoint)
        self.assertLessEqual(len(client.calls), 15)

    async def test_bad_pass_envelope_never_replaces_old_index_or_commits_hash(self):
        from app.extractor import EntityExtractor
        from tests.test_extraction import CompletionClient

        for stage in STAGES:
            for bad in ({}, [], {"metadata": {}, "evidence": ["bad-row"]}):
                with self.subTest(stage=stage, bad=bad):
                    self.paperless.documents[1] = document(content=SOURCE)
                    self.existing()
                    self.embeddings.fingerprints[1] = "previous-completion"
                    previous_checkpoint = self.embeddings.last_sync
                    self.embeddings.writes.clear()
                    wire = SDKWire(CompletionClient({stage: bad}), cached=True)
                    async with wire.client() as client:
                        with patch.object(pipeline, "extractor", EntityExtractor(client)):
                            result = await pipeline.reindex_document(1)
                    self.assertEqual(result["status"], "error")
                    self.assertEqual(self.graph.documents[1]["title"], "Existing usable document")
                    self.assertEqual(self.embeddings.chunks[1, 0], "Existing usable chunk")
                    # Forced reindex deliberately removes the old completion marker;
                    # failed preparation must never commit a replacement marker.
                    self.assertNotIn(1, self.embeddings.hashes)
                    self.assertNotIn(1, self.embeddings.fingerprints)
                    self.assertEqual(self.graph.deleted, [])
                    self.assertEqual(self.embeddings.last_sync, previous_checkpoint)
                    self.assertEqual(self.embeddings.writes, [("delete_hash", 1)])
                    self.assertEqual(wire.counts()[stage], 3)
                    self.assertEqual(len(wire.requests), STAGES.index(stage) + 3)

    async def test_partial_storage_write_is_uncommitted_and_retry_converges(self):
        for store in ('graph', 'chunks'):
            with self.subTest(store=store):
                self.existing()
                self.graph.fail_create = store == 'graph'
                self.embeddings.fail_store = store == 'chunks'
                self.embeddings.last_sync = datetime(2026, 8, 1, tzinfo=timezone.utc)
                result = await pipeline.reindex_document(1)
                self.assertEqual(result['status'], 'error')
                self.assertNotIn(1, self.embeddings.hashes)
                self.assertTrue(self.invalidations)
                self.graph.fail_create = self.embeddings.fail_store = False
                self.assertEqual((await pipeline.sync_documents())['processed'], 1)
                self.assertEqual(self.embeddings.writes[-1], ('commit_hash', 1))

    async def test_answers_cached_during_replacement_are_invalidated_on_success_and_failure(self):
        self.embeddings.during_store = lambda: self.cached_answers.update(question='intermediate answer')
        for fail in (False, True):
            with self.subTest(fail=fail):
                self.existing()
                self.embeddings.fail_store = fail
                result = await pipeline.reindex_document(1)
                self.assertEqual(result['status'], 'error' if fail else 'processed')
                self.assertEqual(self.cached_answers, {})

    async def test_deletion_failure_retains_checkpoint_and_retries_orphan_stores(self):
        self.existing()
        self.paperless.documents.clear()
        previous = self.embeddings.last_sync
        self.graph.fail_delete = True
        result = await pipeline.sync_documents()
        self.assertEqual(result['status'], 'failed')
        self.assertEqual(self.embeddings.last_sync, previous)
        self.graph.fail_delete = False
        self.graph.documents.clear()  # simulate graph deletion committed separately
        result = await pipeline.sync_documents()
        self.assertEqual(result['deleted'], 1)
        self.assertEqual(self.embeddings.chunks, {})
        self.assertEqual(self.embeddings.hashes, {})

    async def test_full_reindex_does_not_clear_old_data_before_preparation(self):
        self.existing()
        self.extractor.fail = True
        result = await pipeline.reindex_all()
        self.assertEqual(result['status'], 'failed')
        self.assertEqual(self.graph.documents[1]['title'], 'Existing usable document')
        self.assertEqual(self.graph.deleted, [])

    async def test_skip_tag_purges_all_derived_state(self):
        self.existing()
        self.paperless.documents[1]['tags'] = [99]
        result = await pipeline.sync_documents()
        self.assertEqual(result['deleted'], 1)
        self.assertFalse(self.graph.documents or self.embeddings.chunks or self.embeddings.hashes)
        self.assertGreater(self.resolver.hydrated, 0)

    async def test_coverage_and_complete_source_text_are_persisted(self):
        self.paperless.documents[1]['content'] = 'Instructional heading\n' + 'Some boilerplate. ' * 300 + '\nOnly tail fact: $41.'
        result = await pipeline.reindex_document(1)
        self.assertEqual(result['status'], 'processed')
        metadata = self.graph.documents[1]['extraction_metadata']
        self.assertEqual(metadata['extraction_coverage']['status'], 'complete')
        self.assertIn('Only tail fact: $41.', '\n'.join(self.embeddings.chunks.values()))
        self.assertTrue(self.invalidations)

    async def test_metadata_only_vector_failure_preserves_old_index(self):
        self.existing()
        self.paperless.documents[1]['content'] = ''
        self.embeddings.fail_generate = True
        result = await pipeline.reindex_document(1)
        self.assertEqual(result['status'], 'error')
        self.assertEqual(self.graph.documents[1]['title'], 'Existing usable document')
        self.assertEqual(self.embeddings.chunks[1, 0], 'Existing usable chunk')
        self.embeddings.fail_generate = False
        result = await pipeline.reindex_document(1)
        self.assertEqual(result['status'], 'processed')
        self.assertEqual(result['doc_type'], 'no_content')
        self.assertIn('No OCR content', self.embeddings.chunks[1, 0])

    async def test_already_cancelled_reindex_preserves_existing_indexes(self):
        self.existing()
        cancel = asyncio.Event()
        cancel.set()
        result = await pipeline.reindex_all(cancel_event=cancel)
        self.assertEqual(result['status'], 'cancelled')
        self.assertEqual(self.graph.documents[1]['title'], 'Existing usable document')
        self.assertEqual(self.graph.deleted, [])
        self.assertIn(1, self.embeddings.hashes)

    async def test_owner_cancellation_also_drains_admitted_writer(self):
        self.extractor.started, self.extractor.release = asyncio.Event(), asyncio.Event()
        running = asyncio.create_task(pipeline.sync_documents())
        await self.extractor.started.wait()
        running.cancel()
        await asyncio.sleep(0)
        self.assertFalse(running.done())
        self.extractor.release.set()
        with self.assertRaises(asyncio.CancelledError):
            await running
        self.assertIn(1, self.embeddings.hashes)

    async def test_cancellation_drains_active_document_and_retries_pending_document(self):
        self.paperless.documents[2] = document(2)
        self.extractor.started, self.extractor.release = asyncio.Event(), asyncio.Event()
        cancel = asyncio.Event()
        old = self.embeddings.last_sync
        running = asyncio.create_task(pipeline.sync_documents(cancel_event=cancel))
        await self.extractor.started.wait()
        cancel.set()
        await asyncio.sleep(0)
        self.assertFalse(running.done())
        self.extractor.release.set()
        result = await running
        self.assertEqual(result['status'], 'cancelled')
        self.assertEqual(result['skipped'], 1)
        self.assertEqual(self.embeddings.last_sync, old)
        self.assertIn(1, self.embeddings.hashes)
        self.assertNotIn(2, self.embeddings.hashes)
        self.extractor.started = None
        retry = await pipeline.sync_documents()
        self.assertEqual(retry['processed'], 1)
        self.assertIn(2, self.embeddings.hashes)

    async def test_cancelled_task_keeps_admission_until_writers_finish(self):
        from app import main
        main._tasks.clear()
        main._cancel_events.clear()
        self.extractor.started, self.extractor.release = asyncio.Event(), asyncio.Event()
        with patch.object(main, 'invalidate_on_sync', lambda: None):
            task_id = await main._run_sync_task()
            await self.extractor.started.wait()
            self.assertEqual((await main.cancel_task(task_id))['status'], 'cancelling')
            for start in (main._run_sync_task, main.reindex):
                with self.assertRaises(main.HTTPException) as error:
                    await start()
                self.assertEqual(error.exception.status_code, 409)
            with self.assertRaises(main.HTTPException):
                await main._run_reindex_documents_task([1], task_type='test', message='test')
            self.assertEqual(main._tasks[task_id]['status'], 'cancelling')
            self.extractor.release.set()
            for _ in range(100):
                if main._tasks[task_id]['status'] not in {'running', 'cancelling'}: break
                await asyncio.sleep(0)
            self.assertEqual(main._tasks[task_id]['status'], 'cancelled')
            with self.assertRaises(main.HTTPException):
                await main.cancel_task(task_id)

    async def test_partial_repair_does_not_advance_corpus_checkpoint(self):
        from app import main
        main._tasks.clear()
        main._cancel_events.clear()
        previous = self.embeddings.last_sync
        with patch.object(main, 'invalidate_on_sync', lambda: None), patch.object(main, 'embeddings_store', self.embeddings):
            task_id, _ = await main._run_reindex_documents_task([1], task_type='test', message='test', update_last_sync=True)
            for _ in range(100):
                if main._tasks[task_id]['status'] != 'running': break
                await asyncio.sleep(0)
            self.assertEqual(main._tasks[task_id]['status'], 'completed')
            self.assertEqual(self.embeddings.last_sync, previous)

    async def test_background_sync_and_reindex_report_errors_as_failed(self):
        from app import main
        self.extractor.fail = True
        with patch.object(main, 'invalidate_on_sync', lambda: None):
            for start in (main._run_sync_task, main.reindex):
                main._tasks.clear()
                main._cancel_events.clear()
                started = await start()
                task_id = started if isinstance(started, str) else started.task_id
                for _ in range(100):
                    if main._tasks[task_id]['status'] != 'running': break
                    await asyncio.sleep(0)
                self.assertEqual(main._tasks[task_id]['status'], 'failed')
                self.assertEqual(main._tasks[task_id]['result']['errors'], 1)


if __name__ == '__main__':
    unittest.main()
