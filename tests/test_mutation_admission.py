import asyncio
import unittest
from unittest.mock import AsyncMock, patch
from contextlib import ExitStack
import httpx

from tests.runtime import configure_test_environment
configure_test_environment()
import app.main as main


class MutationAdmissionTests(unittest.IsolatedAsyncioTestCase):
    async def test_lifespan_drains_periodic_steward_before_closing_dependencies(self):
        started, drained = asyncio.Event(), asyncio.Event()
        workers = []
        async def steward(**kwargs):
            workers.append(asyncio.current_task())
            started.set()
            try:
                await asyncio.Event().wait()
            finally:
                await asyncio.sleep(0)
                drained.set()
        async def periodic():
            await main._run_entity_steward_task(reason="periodic")
            await asyncio.Event().wait()
        async def close():
            self.assertTrue(drained.is_set(), "Dependencies closed before Steward drained")
        try:
            with ExitStack() as stack:
                stack.enter_context(patch.object(main, "_tasks", {}))
                stack.enter_context(patch.object(main, "_schedule_task_cleanup"))
                stack.enter_context(patch.object(main, "_auto_sync_loop", AsyncMock()))
                stack.enter_context(patch.object(main, "_entity_steward_loop", periodic))
                stack.enter_context(patch.object(main.entity_steward, "run_once", steward))
                for dependency in (main.graph_store, main.embeddings_store, main.conversations):
                    stack.enter_context(patch.object(dependency, "init", AsyncMock()))
                for dependency in (main.query_engine, main.extractor, main.classifier,
                                   main.strands_orchestrator, main.graph_store,
                                   main.embeddings_store, main.conversations):
                    stack.enter_context(patch.object(dependency, "close", close))
                stack.enter_context(patch.object(main, "close_pipeline_clients", close))
                async with main.lifespan(main.app):
                    await asyncio.wait_for(started.wait(), 1)
                self.assertTrue(all(worker.done() for worker in workers))
                self.assertEqual([task["status"] for task in main._tasks.values()], ["cancelled"])
        finally:
            for worker in workers:
                worker.cancel()
            await asyncio.gather(*workers, return_exceptions=True)

    async def test_post_sync_steward_is_visible_and_holds_mutation_admission_until_drained(self):
        started, release, drained = asyncio.Event(), asyncio.Event(), asyncio.Event()
        async def steward(**kwargs):
            started.set()
            try:
                await release.wait()
                return {"status": "completed", "reviewed_count": 1}
            finally:
                drained.set()
        with patch.object(main, "_tasks", {}), patch.object(main, "_schedule_task_cleanup"), \
                patch.object(main, "sync_documents", AsyncMock(return_value={"processed": 1, "errors": 0})), \
                patch.object(main, "invalidate_on_sync", lambda: None), \
                patch.object(main.entity_steward, "run_once", steward):
            await main._run_sync_task()
            await asyncio.wait_for(started.wait(), timeout=1)
            try:
                active = [t for t in main._tasks.values() if t["status"] == "running"]
                self.assertEqual([t["type"] for t in active], ["entity_steward"])
                with self.assertRaises(main.HTTPException):
                    await main._run_sync_task()
                with self.assertRaises(main.HTTPException):
                    async with main._graph_mutation("synthetic-repair"):
                        self.fail("Mutation admitted before Steward drained")
            finally:
                release.set()
                await asyncio.wait_for(drained.wait(), timeout=1)
                await asyncio.sleep(0)
            self.assertTrue(all(t["status"] == "completed" for t in main._tasks.values()))

    async def test_steward_cannot_start_while_ingestion_or_repair_is_running(self):
        for task_type in ("sync", "reindex", "repair"):
            with patch.object(main, "_tasks", {"existing": {"type": task_type, "status": "running"}}):
                with self.assertRaises(main.HTTPException):
                    await main._run_entity_steward_task(reason="periodic")

    async def test_cancel_draining_task_blocks_delete_and_manual_merge(self):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=main.app), base_url="http://test") as client:
            with patch.object(main, "_tasks", {"sync": {"status": "cancelling"}}), \
                 patch.object(main.graph_store, "delete_document_graph", AsyncMock()) as delete:
                result = await client.delete("/document/101")
                self.assertEqual(result.status_code, 409)
                result = await client.post("/entity-review/merge", json={"primary_uuid": "a", "duplicate_uuid": "b"})
                self.assertEqual(result.status_code, 409)
                with patch.object(main.entity_resolver, "record_decision", AsyncMock()) as decide:
                    result = await client.post("/entity-review/split", json={"left_uuid": "a", "right_uuid": "b"})
                    self.assertEqual(result.status_code, 409)
                    decide.assert_not_awaited()
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
