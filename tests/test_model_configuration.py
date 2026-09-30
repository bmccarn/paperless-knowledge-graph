"""Release model routing and identity contracts; no real provider calls."""

import hashlib
import json
import os
from pathlib import Path
import unittest
from unittest.mock import AsyncMock, patch

from tests.runtime import configure_test_environment

configure_test_environment()

from app.config import Settings, normalize_base_url, settings
from app.paperless import PaperlessClient


class ModelIdentityTests(unittest.TestCase):
    def test_review_reconciliation_upgrade_invalidates_processing_without_revoking_alias_policy(self):
        from app import extraction_evidence
        from app.entity_policy import RESOLUTION_POLICY
        document = {"id": 101, "title": "Synthetic", "content": "Synthetic source", "tags": []}
        before = PaperlessClient.ingestion_fingerprint(document)
        with patch.object(extraction_evidence, "RECONCILIATION_VERSION", "future-review-version"):
            after = PaperlessClient.ingestion_fingerprint(document)
        self.assertNotEqual(before, after)
        self.assertEqual(RESOLUTION_POLICY, "evidence-identity-v2")

    def test_release_defaults_and_sample_environments_agree(self):
        with patch.dict(os.environ, {}, clear=True):
            defaults = Settings(_env_file=None)
        self.assertEqual(defaults.llm_model, "gemini-3.8-flash")
        self.assertEqual(defaults.fallback_model, "gpt-5.4-mini")
        self.assertEqual(defaults.embedding_model, "text-embedding-3-large")
        self.assertEqual(defaults.strands_model, "")
        self.assertEqual(defaults.embedding_dimensions, 3072)
        root = Path(__file__).resolve().parents[1]
        for path in (root / ".env.example", root / "examples/kg-local.env.example"):
            values = dict(line.split("=", 1) for line in path.read_text().splitlines()
                          if line and not line.startswith("#") and "=" in line)
            self.assertEqual(values["LLM_MODEL"], defaults.llm_model)
            self.assertEqual(values["FALLBACK_MODEL"], defaults.fallback_model)
            self.assertEqual(values["EMBEDDING_MODEL"], defaults.embedding_model)
            self.assertEqual(values["STRANDS_MODEL"], "")
            self.assertEqual(int(values["EMBEDDING_DIMENSIONS"]), defaults.embedding_dimensions)

    def test_model_upgrade_changes_fingerprint_not_source_identity(self):
        document = {"id": 101, "title": "Synthetic record", "content": "Premium: 125 USD", "tags": [3, 1]}
        original_hash = PaperlessClient.content_hash(document["content"])
        with patch.object(settings, "llm_model", "gemini-3.5-flash"):
            old_model = PaperlessClient.ingestion_fingerprint(document)
        with patch.object(settings, "llm_model", "gemini-3.8-flash"):
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


class EndpointConfigurationTests(unittest.TestCase):
    @staticmethod
    def settings_from(env):
        with patch.dict(os.environ, env, clear=True):
            return Settings(_env_file=None)

    def test_base_url_with_or_without_version_resolves_to_one_api_root(self):
        cases = {
            "http://litellm:4000": "http://litellm:4000/v1",
            "http://litellm:4000/v1/": "http://litellm:4000/v1",
            "https://openrouter.ai/api": "https://openrouter.ai/api/v1",
            "https://openrouter.ai/api/v1": "https://openrouter.ai/api/v1",
            "https://router.requesty.ai/v1": "https://router.requesty.ai/v1",
            # Versioned roots that do not end in /v1 must not gain a second version.
            "https://generativelanguage.googleapis.com/v1beta/openai/": "https://generativelanguage.googleapis.com/v1beta/openai",
        }
        for configured, expected in cases.items():
            with self.subTest(configured=configured):
                self.assertEqual(normalize_base_url(configured), expected)

    def test_legacy_litellm_install_keeps_its_endpoint_key_and_model(self):
        legacy = self.settings_from({"LITELLM_URL": "http://litellm:4000", "LITELLM_API_KEY": "legacy-key",
                                     "GEMINI_MODEL": "legacy-route"})
        self.assertEqual((legacy.llm_endpoint.base_url, legacy.llm_endpoint.api_key, legacy.llm_endpoint.litellm),
                         ("http://litellm:4000/v1", "legacy-key", True))
        self.assertEqual(legacy.llm_endpoint.litellm_root, "http://litellm:4000")
        self.assertEqual(legacy.embedding_endpoint, legacy.llm_endpoint)
        self.assertEqual(legacy.llm_model, "legacy-route")

    def test_neutral_settings_win_and_never_send_a_key_to_another_endpoint(self):
        configured = self.settings_from({
            "LITELLM_URL": "http://litellm:4000", "LITELLM_API_KEY": "litellm-key",
            "LLM_BASE_URL": "https://openrouter.ai/api/v1", "LLM_API_KEY": "",
            "GEMINI_MODEL": "legacy-route", "LLM_MODEL": "google/gemini-3.8-flash",
            "EMBEDDING_BASE_URL": "https://api.openai.com", "EMBEDDING_API_KEY": "",
        })
        self.assertEqual(configured.llm_model, "google/gemini-3.8-flash")
        self.assertEqual((configured.llm_endpoint.base_url, configured.llm_endpoint.litellm),
                         ("https://openrouter.ai/api/v1", False))
        self.assertEqual(configured.embedding_endpoint.base_url, "https://api.openai.com/v1")
        # An unset key for a new destination stays empty rather than borrowing a proxy credential.
        self.assertEqual(configured.llm_endpoint.api_key, "")
        self.assertEqual(configured.embedding_endpoint.api_key, "")
        inherited = self.settings_from({"LLM_BASE_URL": "https://openrouter.ai/api", "LLM_API_KEY": "router-key"})
        self.assertEqual(inherited.embedding_endpoint, inherited.llm_endpoint)
        self.assertEqual(inherited.embedding_endpoint.api_key, "router-key")

    def test_endpoint_url_with_query_string_is_rejected_at_load(self):
        from pydantic import ValidationError
        with self.assertRaisesRegex(ValidationError, "query string"):
            self.settings_from({"LLM_BASE_URL": "https://resource.openai.azure.com/openai/v1?api-version=preview"})

    def test_keyless_local_endpoints_construct_clients_without_an_auth_header(self):
        from app import embeddings as embeddings_module
        from app.classifier import DocumentClassifier
        local = self.settings_from({"LLM_BASE_URL": "http://localhost:11434", "EMBEDDING_BASE_URL": "http://localhost:8081"})
        self.assertEqual(local.llm_endpoint.headers, {})
        with patch.dict(os.environ, {}, clear=True), patch.object(embeddings_module, "settings", local), \
             patch("app.classifier.settings", local):
            # The OpenAI SDK refuses empty credentials, which would stop the backend at import.
            store = embeddings_module.EmbeddingsStore()
            classifier = DocumentClassifier()
        self.assertEqual(str(store.openai.base_url), "http://localhost:8081/v1/")
        self.assertEqual(str(classifier.client.base_url), "http://localhost:11434/v1/")


class ModelRoutingTests(unittest.IsolatedAsyncioTestCase):
    async def test_primary_selectors_and_strands_inheritance_use_the_configured_route(self):
        from app.classifier import DocumentClassifier
        from app.extractor import EntityExtractor
        from app.query import QueryEngine
        from app import strands_orchestrator as strands_module

        with patch.object(settings, "llm_model", "gemini-3.8-flash"), \
             patch.object(settings, "strands_model", ""), \
             patch("app.classifier.AsyncOpenAI"), patch("app.query.AsyncOpenAI"), \
             patch.object(strands_module, "OpenAIModel", create=True) as model:
            self.assertEqual(DocumentClassifier().model, "gemini-3.8-flash")
            self.assertEqual(EntityExtractor(client=object()).model, "gemini-3.8-flash")
            self.assertEqual(QueryEngine()._active_model(), "gemini-3.8-flash")
            strands_module.strands_orchestrator._model()
            self.assertEqual(model.call_args.kwargs["model_id"], "gemini-3.8-flash")
            with patch.object(settings, "strands_model", "explicit-synthetic-override"):
                strands_module.strands_orchestrator._model()
                self.assertEqual(model.call_args.kwargs["model_id"], "explicit-synthetic-override")

    async def test_model_selection_metadata_keeps_exact_route_and_filters_embeddings(self):
        from app import main

        routes = {"data": [{"id": "gemini-3.8-flash"}, {"id": "gpt-5.4-mini"},
                           {"id": "text-embedding-3-large"}]}
        with patch.object(settings, "llm_model", "gemini-3.8-flash"), \
             patch("httpx.AsyncClient") as client:
            response = client.return_value.__aenter__.return_value.get
            response.return_value.json = lambda: routes
            response.return_value.raise_for_status = lambda: None
            result = await main.list_models()
        self.assertEqual(result["default"], "gemini-3.8-flash")
        self.assertEqual(result["models"], [{"id": route, "name": route}
                                          for route in ("gemini-3.8-flash", "gpt-5.4-mini")])
        with patch.object(settings, "llm_model", "gemini-3.8-flash"), \
             patch("httpx.AsyncClient") as client:
            client.return_value.__aenter__.return_value.get = AsyncMock(side_effect=RuntimeError("synthetic offline proxy"))
            result = await main.list_models()
        self.assertEqual(result["default"], "gemini-3.8-flash")
        self.assertEqual(result["models"], [{"id": "gemini-3.8-flash", "name": "gemini-3.8-flash"}])
        self.assertIn("synthetic offline proxy", result["error"])

    async def test_model_listing_uses_litellm_management_route_only_for_litellm(self):
        from app import main

        class Response:
            def __init__(self, url):
                self.url = url

            def raise_for_status(self):
                if not self.url.endswith("/model/info"):
                    raise RuntimeError("synthetic list failure")

            def json(self):
                return {"data": [{"model_name": "info-route"}]}

        for env, expected_urls, listed in (
            ({"LITELLM_URL": "http://litellm:4000/v1/"},
             ["http://litellm:4000/v1/models", "http://litellm:4000/model/info"], ["info-route"]),
            ({"LLM_BASE_URL": "https://openrouter.ai/api/"}, ["https://openrouter.ai/api/v1/models"], ["configured-route"]),
        ):
            with self.subTest(env=env):
                with patch.dict(os.environ, {**env, "LLM_MODEL": "configured-route"}, clear=True):
                    configured = Settings(_env_file=None)
                requested = []

                async def get(url, **_):
                    requested.append(url)
                    return Response(url)
                with patch("app.config.settings", configured), patch("httpx.AsyncClient") as client:
                    client.return_value.__aenter__.return_value.get = get
                    result = await main.list_models()
                self.assertEqual(requested, expected_urls)
                self.assertEqual([model["id"] for model in result["models"]], listed)
                self.assertEqual("error" in result, listed == ["configured-route"])

    async def test_model_listing_error_never_exposes_url_credentials(self):
        import httpx
        from app import main

        with patch.dict(os.environ, {"LLM_BASE_URL": "https://user:s3cret@gateway.example/v1"}, clear=True):
            configured = Settings(_env_file=None)
        request = httpx.Request("GET", "https://user:s3cret@gateway.example/v1/models")
        failure = httpx.HTTPStatusError("Server error for url https://user:s3cret@gateway.example/v1/models",
                                        request=request, response=httpx.Response(502, request=request))
        for side_effect, reported in ((failure, "HTTP 502"), (httpx.ConnectError(
                "cannot reach https://user:s3cret@gateway.example/v1/models"), "cannot reach")):
            with self.subTest(error=type(side_effect).__name__), patch("app.config.settings", configured), \
                 patch("httpx.AsyncClient") as client, self.assertLogs("app.main", "ERROR") as logs:
                client.return_value.__aenter__.return_value.get = AsyncMock(side_effect=side_effect)
                result = await main.list_models()
            self.assertIn(reported, result["error"])
            self.assertNotIn("s3cret", result["error"])
            self.assertNotIn("s3cret", "\n".join(logs.output))


class EmbeddingDimensionTests(unittest.IsolatedAsyncioTestCase):
    async def test_vector_of_the_wrong_size_is_rejected_before_storage(self):
        from types import SimpleNamespace
        from app.embeddings import EMBEDDING_DIMENSIONS, EmbeddingsStore, SearchUnavailable

        store = EmbeddingsStore()
        try:
            for size, accepted in ((EMBEDDING_DIMENSIONS, True), (768, False)):
                store.openai.embeddings.create = AsyncMock(return_value=SimpleNamespace(
                    data=[SimpleNamespace(embedding=[0.1] * size)]))
                with self.subTest(size=size):
                    self.assertEqual(len(await store.generate_embedding("synthetic")), size if accepted else 0)
                    if not accepted:
                        with self.assertRaises(SearchUnavailable):
                            await store.generate_embedding("synthetic", strict=True)
        finally:
            await store.close()

    async def test_dimensions_beyond_the_hnsw_limit_skip_optional_indexes(self):
        from app import embeddings as embeddings_module
        store = embeddings_module.EmbeddingsStore()
        store.pool = None  # Any index DDL would fail on the missing pool.
        try:
            with patch.object(embeddings_module, "EMBEDDING_DIMENSIONS", 4096):
                result = await store.create_vector_indexes()
            self.assertEqual(result["optional_candidate_index"], None)
            self.assertEqual(result["default_search"], "exact")
        finally:
            await store.close()
