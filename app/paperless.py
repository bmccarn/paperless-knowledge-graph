import hashlib
import logging
from datetime import datetime

import httpx

from app.config import settings

logger = logging.getLogger(__name__)


class PaperlessClient:
    def __init__(self):
        self.base_url = settings.paperless_url.rstrip("/")
        self.headers = {"Authorization": f"Token {settings.paperless_token}"}

    async def _get(self, path: str, params: dict | None = None) -> dict:
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.get(
                f"{self.base_url}{path}",
                headers=self.headers,
                params=params,
            )
            resp.raise_for_status()
            return resp.json()

    async def get_documents(
        self, modified_after: datetime | None = None, page: int = 1, page_size: int = 50
    ) -> dict:
        """Fetch documents from Paperless-ngx, ordered by most recently modified."""
        params = {"ordering": "-modified", "page": page, "page_size": page_size}
        if modified_after:
            params["modified__gt"] = modified_after.isoformat()
        return await self._get("/api/documents/", params=params)

    async def get_document(self, doc_id: int) -> dict:
        """Fetch a single document by ID."""
        return await self._get(f"/api/documents/{doc_id}/")

    async def get_all_documents(self, modified_after: datetime | None = None) -> list[dict]:
        """Fetch all documents, paginating automatically."""
        all_docs = []
        page = 1
        while True:
            data = await self.get_documents(modified_after=modified_after, page=page)
            results = data.get("results", [])
            all_docs.extend(results)
            if not data.get("next"):
                break
            page += 1
        return all_docs

    @staticmethod
    def content_hash(content: str) -> str:
        """Generate SHA-256 hash of document content."""
        return hashlib.sha256(content.encode("utf-8")).hexdigest()


paperless_client = PaperlessClient()
