"""Feedback lifecycle through the full ASGI application with owned storage replaced."""

from datetime import datetime, timedelta, timezone
import unittest
from unittest.mock import AsyncMock, patch

import httpx

from tests.runtime import configure_test_environment

configure_test_environment()

from app import main


class FeedbackStore:
    def __init__(self):
        self.rows = []
        self.processed_at = datetime.now(timezone.utc) - timedelta(days=1)

    async def add_document_feedback(self, doc_id, reason, note):
        row = {"id": len(self.rows) + 1, "document_id": doc_id, "reason": reason, "note": note,
               "status": "open", "created_at": datetime.now(timezone.utc), "resolved_at": None}
        self.rows.append(row)
        return dict(row)

    async def get_document_feedback(self, doc_id):
        return [dict(row) for row in self.rows if row["document_id"] == doc_id]

    async def get_document_processing_status(self, doc_id):
        return {"processed": True, "content_hash": "synthetic-hash", "processed_at": self.processed_at.isoformat(),
                "chunk_count": 1, "feedback_count": len(self.rows), "open_feedback_count": sum(row["status"] == "open" for row in self.rows)}

    async def resolve_document_feedback(self, doc_id, feedback_id, resolution, note, content_hash=None):
        for row in self.rows:
            if row["document_id"] == doc_id and row["id"] == feedback_id and row["status"] == "open":
                row.update(status="resolved", resolution=resolution, resolution_note=note,
                           resolved_at=datetime.now(timezone.utc), resolved_content_hash=content_hash)
                return dict(row)
        return None


class FeedbackRoutesTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.store = FeedbackStore()
        self.storage_patch = patch.object(main, "embeddings_store", self.store)
        self.storage_patch.start()
        self.invalidate_patch = patch.object(main, "invalidate_on_sync_async", new=AsyncMock(), create=True)
        self.invalidate = self.invalidate_patch.start()
        self.client = httpx.AsyncClient(transport=httpx.ASGITransport(app=main.app), base_url="http://synthetic.local")

    async def asyncTearDown(self):
        await self.client.aclose()
        self.invalidate_patch.stop()
        self.storage_patch.stop()

    async def create_report(self):
        response = await self.client.post("/document/101/feedback", json={"reason": "extraction_wrong", "note": "Wrong premium."})
        self.assertEqual(response.status_code, 200, response.text)
        return response.json()["feedback"]

    async def test_flag_is_open_visible_and_invalidates_cached_answers(self):
        report = await self.create_report()
        self.assertEqual(report["status"], "open")
        response = await self.client.get("/document/101/feedback")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["feedback"][0]["id"], report["id"])
        self.invalidate.assert_awaited_once()

    async def test_reindex_resolution_requires_completed_processing_after_report(self):
        report = await self.create_report()
        path = f"/document/101/feedback/{report['id']}/resolve"
        payload = {"resolution": "reindexed_and_reviewed", "note": "Checked corrected premium against OCR."}
        failed = await self.client.post(path, json=payload)
        self.assertEqual(failed.status_code, 409)
        self.assertEqual(self.store.rows[0]["status"], "open")
        self.store.processed_at = datetime.now(timezone.utc) + timedelta(seconds=1)
        resolved = await self.client.post(path, json=payload)
        self.assertEqual(resolved.status_code, 200, resolved.text)
        self.assertEqual(resolved.json()["feedback"]["status"], "resolved")
        self.assertEqual(resolved.json()["feedback"]["resolved_content_hash"], "synthetic-hash")
        self.assertEqual(self.invalidate.await_count, 2)

    async def test_dismissal_records_review_without_claiming_reindex(self):
        report = await self.create_report()
        response = await self.client.post(f"/document/101/feedback/{report['id']}/resolve", json={"resolution": "dismissed_after_review", "note": "Original extraction matches the document."})
        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(response.json()["feedback"]["resolution"], "dismissed_after_review")

    async def test_resolution_needs_note_and_cannot_target_another_document_or_repeat(self):
        report = await self.create_report()
        payload = {"resolution": "dismissed_after_review", "note": "Checked the source."}
        path = f"/document/101/feedback/{report['id']}/resolve"
        empty = await self.client.post(path, json={**payload, "note": "  "})
        self.assertEqual(empty.status_code, 422)
        foreign = await self.client.post(f"/document/102/feedback/{report['id']}/resolve", json=payload)
        self.assertEqual(foreign.status_code, 404)
        self.assertEqual((await self.client.post(path, json=payload)).status_code, 200)
        self.assertEqual((await self.client.post(path, json=payload)).status_code, 409)


if __name__ == "__main__":
    unittest.main()
