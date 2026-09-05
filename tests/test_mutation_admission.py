import asyncio
import unittest
from unittest.mock import AsyncMock, patch
import httpx

from tests.runtime import configure_test_environment
configure_test_environment()
import app.main as main


class MutationAdmissionTests(unittest.IsolatedAsyncioTestCase):
    async def test_cancel_draining_task_blocks_delete_and_manual_merge(self):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=main.app), base_url="http://test") as client:
            with patch.object(main, "_tasks", {"sync": {"status": "cancelling"}}), \
                 patch.object(main.graph_store, "delete_document_graph", AsyncMock()) as delete:
                result = await client.delete("/document/101")
                self.assertEqual(result.status_code, 409)
                result = await client.post("/entity-review/merge", json={"primary_uuid": "a", "duplicate_uuid": "b"})
                self.assertEqual(result.status_code, 409)
                delete.assert_not_awaited()

    async def test_inflight_http_mutation_holds_ingestion_admission_and_partial_failure_invalidates(self):
        started, release = asyncio.Event(), asyncio.Event()
        async def fail_graph(doc_id):
            started.set()
            await release.wait()
            raise RuntimeError("synthetic partial failure")
        with patch.object(main, "_tasks", {}), patch.object(main, "_schedule_task_cleanup"), \
             patch.object(main.entity_resolver, "hydrate_review_identities", AsyncMock()), \
             patch.object(main.embeddings_store, "delete_doc_hash", AsyncMock()) as clear_hash, \
             patch.object(main.graph_store, "delete_document_graph", fail_graph), \
             patch.object(main, "invalidate_on_sync_async", AsyncMock()) as invalidate:
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=main.app), base_url="http://test") as client:
                pending = asyncio.create_task(client.delete("/document/101"))
                await started.wait()
                sync = await client.post("/sync")
                self.assertEqual(sync.status_code, 409)
                clear_hash.assert_awaited_once_with(101)
                release.set()
                failed = await pending
                self.assertEqual(failed.status_code, 500)
                self.assertEqual(invalidate.await_count, 2)
                self.assertTrue(all(t["status"] == "failed" for t in main._tasks.values()))


if __name__ == "__main__":
    unittest.main()
