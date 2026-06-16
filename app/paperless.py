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
        self._skip_tag_ids: set[int] | None = None

    async def _get(self, path: str, params: dict | None = None) -> dict:
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.get(
                f"{self.base_url}{path}",
                headers=self.headers,
                params=params,
            )
            resp.raise_for_status()
            return resp.json()

    @staticmethod
    def _configured_skip_tag_names() -> set[str]:
        return {
            name.strip().lower()
            for name in settings.paperless_skip_tag_names.split(",")
            if name.strip()
        }

    async def get_documents(
        self, modified_after: datetime | None = None, page: int = 1, page_size: int = 50
    ) -> dict:
        """Fetch documents from Paperless-ngx, ordered by most recently modified."""
        params = {"ordering": "-modified", "page": page, "page_size": page_size}
        if modified_after:
            params["modified__gt"] = modified_after.isoformat()
        return await self._get("/api/documents/", params=params)

    async def get_document_summary(self) -> dict:
        """Return Paperless document count and most recent document metadata."""
        data = await self.get_documents(page=1, page_size=1)
        latest = (data.get("results") or [None])[0]
        return {
            "count": data.get("count", 0),
            "latest": latest,
            "latest_modified": latest.get("modified") if latest else None,
            "latest_id": latest.get("id") if latest else None,
            "latest_title": latest.get("title") if latest else None,
        }

    async def get_document(self, doc_id: int) -> dict:
        """Fetch a single document by ID."""
        return await self._get(f"/api/documents/{doc_id}/")

    async def get_tags(self, page: int = 1, page_size: int = 100) -> dict:
        """Fetch Paperless tags."""
        return await self._get("/api/tags/", params={"page": page, "page_size": page_size})

    async def get_all_tags(self, page_size: int = 100) -> list[dict]:
        """Fetch all Paperless tags, paginating automatically."""
        all_tags = []
        page = 1
        while True:
            data = await self.get_tags(page=page, page_size=page_size)
            results = data.get("results", [])
            all_tags.extend(results)
            if not data.get("next"):
                break
            page += 1
        return all_tags

    async def get_skip_tag_ids(self) -> set[int]:
        """Resolve configured hold/skip tag names to Paperless tag IDs."""
        if self._skip_tag_ids is not None:
            return set(self._skip_tag_ids)

        configured_names = self._configured_skip_tag_names()
        if not configured_names:
            self._skip_tag_ids = set()
            return set()

        tags = await self.get_all_tags()
        resolved = set()
        for tag in tags:
            tag_name = str(tag.get("name") or "").strip().lower()
            tag_slug = str(tag.get("slug") or "").strip().lower()
            if tag_name in configured_names or tag_slug in configured_names:
                try:
                    resolved.add(int(tag["id"]))
                except (KeyError, TypeError, ValueError):
                    continue

        missing = configured_names - {
            str(tag.get("name") or "").strip().lower()
            for tag in tags
        } - {
            str(tag.get("slug") or "").strip().lower()
            for tag in tags
        }
        if missing:
            logger.warning("Configured Paperless skip tags not found: %s", ", ".join(sorted(missing)))

        self._skip_tag_ids = resolved
        return set(resolved)

    @staticmethod
    def has_any_tag(doc: dict, tag_ids: set[int]) -> bool:
        if not tag_ids:
            return False
        doc_tag_ids = set()
        for tag_id in doc.get("tags") or []:
            try:
                doc_tag_ids.add(int(tag_id))
            except (TypeError, ValueError):
                continue
        return bool(doc_tag_ids & tag_ids)

    def partition_indexable_documents(self, docs: list[dict], skip_tag_ids: set[int]) -> tuple[list[dict], list[dict]]:
        """Split Paperless docs into KG-indexable docs and docs held for review."""
        indexable = []
        held = []
        for doc in docs:
            if self.has_any_tag(doc, skip_tag_ids):
                held.append(doc)
            else:
                indexable.append(doc)
        return indexable, held

    async def get_all_documents(
        self, modified_after: datetime | None = None, page_size: int = 50
    ) -> list[dict]:
        """Fetch all documents, paginating automatically."""
        all_docs = []
        page = 1
        while True:
            data = await self.get_documents(
                modified_after=modified_after,
                page=page,
                page_size=page_size,
            )
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
