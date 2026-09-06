"""Release model routing and identity contracts; no real provider calls."""

import hashlib
import json
import os
from pathlib import Path
import unittest
from unittest.mock import AsyncMock, patch

from tests.runtime import configure_test_environment

configure_test_environment()

from app.config import Settings, settings
from app.paperless import PaperlessClient


class ModelIdentityTests(unittest.TestCase):
    def test_release_defaults_and_sample_environments_agree(self):
        with patch.dict(os.environ, {}, clear=True):
            defaults = Settings(_env_file=None)
        self.assertEqual(defaults.gemini_model, "gemini-3.8-flash")
        self.assertEqual(defaults.fallback_model, "gpt-5.4-mini")
        self.assertEqual(defaults.embedding_model, "text-embedding-3-large")
        self.assertEqual(defaults.strands_model, "")
        root = Path(__file__).resolve().parents[1]
        for path in (root / ".env.example", root / "examples/kg-local.env.example"):
            values = dict(line.split("=", 1) for line in path.read_text().splitlines()
                          if line and not line.startswith("#") and "=" in line)
            self.assertEqual(values["GEMINI_MODEL"], defaults.gemini_model)
            self.assertEqual(values["FALLBACK_MODEL"], defaults.fallback_model)
            self.assertEqual(values["EMBEDDING_MODEL"], defaults.embedding_model)
            self.assertEqual(values["STRANDS_MODEL"], "")

    def test_model_upgrade_changes_fingerprint_not_source_identity(self):
        document = {"id": 101, "title": "Synthetic record", "content": "Premium: 125 USD", "tags": [3, 1]}
        original_hash = PaperlessClient.content_hash(document["content"])
        with patch.object(settings, "gemini_model", "gemini-3.5-flash"):
            old_model = PaperlessClient.ingestion_fingerprint(document)
        with patch.object(settings, "gemini_model", "gemini-3.8-flash"):
            current = PaperlessClient.ingestion_fingerprint(document)
            self.assertEqual(current, PaperlessClient.ingestion_fingerprint(dict(document, tags=[1, 3])))
        self.assertNotEqual(old_model, current)
        self.assertEqual(original_hash, PaperlessClient.content_hash(document["content"]))
        # PR #10 completions had source-origin-v2 but no model identity.
        fields = {key: document.get(key) for key in (
            "title", "created", "document_type", "correspondent", "storage_path",
            "archive_serial_number", "custom_fields")}
        fields.update(tags=[1, 3], content_hash=original_hash, index_policy="source-origin-v2")
        legacy = hashlib.sha256(json.dumps(fields, sort_keys=True, ensure_ascii=False).encode()).hexdigest()
        self.assertNotEqual(legacy, current)


class ModelRoutingTests(unittest.IsolatedAsyncioTestCase):
    async def test_primary_selectors_and_strands_inheritance_use_the_configured_route(self):
        from app.classifier import DocumentClassifier
        from app.extractor import EntityExtractor
        from app.query import QueryEngine
        from app import strands_orchestrator as strands_module

        with patch.object(settings, "gemini_model", "gemini-3.8-flash"), \
             patch.object(settings, "strands_model", ""), \
             patch("app.classifier.AsyncOpenAI"), patch("app.query.AsyncOpenAI"), \
             patch.object(strands_module, "LiteLLMModel", create=True) as model:
            self.assertEqual(DocumentClassifier().model, "gemini-3.8-flash")
            self.assertEqual(EntityExtractor(client=object()).model, "gemini-3.8-flash")
            self.assertEqual(QueryEngine()._active_model(), "gemini-3.8-flash")
            strands_module.strands_orchestrator._model(max_tokens=6000)
            self.assertEqual(model.call_args.kwargs["model_id"], "gemini-3.8-flash")
            self.assertEqual(model.call_args.kwargs["params"], {"max_tokens": 6000})
            with patch.object(settings, "strands_model", "explicit-synthetic-override"):
                strands_module.strands_orchestrator._model(max_tokens=6000)
                self.assertEqual(model.call_args.kwargs["model_id"], "explicit-synthetic-override")

    async def test_model_selection_metadata_keeps_exact_route_and_filters_embeddings(self):
        from app import main

        routes = {"data": [{"id": "gemini-3.8-flash"}, {"id": "gpt-5.4-mini"},
                           {"id": "text-embedding-3-large"}]}
        with patch.object(settings, "gemini_model", "gemini-3.8-flash"), \
             patch("httpx.AsyncClient") as client:
            response = client.return_value.__aenter__.return_value.get
            response.return_value.json = lambda: routes
            response.return_value.raise_for_status = lambda: None
            result = await main.list_models()
        self.assertEqual(result["default"], "gemini-3.8-flash")
        self.assertEqual(result["models"], [{"id": route, "name": route}
                                          for route in ("gemini-3.8-flash", "gpt-5.4-mini")])
        with patch.object(settings, "gemini_model", "gemini-3.8-flash"), \
             patch("httpx.AsyncClient") as client:
            client.return_value.__aenter__.return_value.get = AsyncMock(side_effect=RuntimeError("synthetic offline proxy"))
            result = await main.list_models()
        self.assertEqual(result, {"default": "gemini-3.8-flash",
                                  "models": [{"id": "gemini-3.8-flash", "name": "gemini-3.8-flash"}]})
