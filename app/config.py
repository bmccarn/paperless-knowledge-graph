import os
import re
from dataclasses import dataclass
from typing import Literal
from urllib.parse import urlsplit, urlunsplit

from pydantic import AliasChoices, Field, field_validator
from pydantic_settings import BaseSettings


# A path segment such as v1, v1beta or v2 marks an already-versioned API root.
_API_VERSION_SEGMENT = re.compile(r"v\d+[a-z0-9]*", re.IGNORECASE)


def normalize_base_url(url: str) -> str:
    """Return the OpenAI-compatible API root the SDK appends routes to.

    A bare host such as ``http://litellm:4000`` gains ``/v1``. A URL whose path
    already contains a version segment (``/v1``, ``/api/v1``, ``/v1beta/openai``)
    is kept, so the same endpoint written with or without ``/v1`` resolves once.
    """
    parts = urlsplit(url.strip())
    if parts.query or parts.fragment:
        # The SDK and raw requests append routes to the base URL, which would land inside a query.
        raise ValueError(f"model endpoint URL must not include a query string or fragment: {parts.path or '/'}")
    path = parts.path.rstrip("/")
    if not any(_API_VERSION_SEGMENT.fullmatch(segment) for segment in path.split("/")):
        path += "/v1"
    return urlunsplit((parts.scheme, parts.netloc, path, parts.query, parts.fragment))


@dataclass(frozen=True)
class ModelEndpoint:
    """One OpenAI-compatible destination; the key always travels with its own URL."""
    base_url: str
    api_key: str
    litellm: bool

    @property
    def litellm_root(self) -> str:
        """LiteLLM serves management routes such as /model/info beside /v1."""
        return re.sub(r"/v1$", "", self.base_url)

    @property
    def sdk_api_key(self) -> str:
        # The OpenAI SDK refuses an empty key; keyless local servers ignore this placeholder.
        return self.api_key or "unused"

    @property
    def headers(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {self.api_key}"} if self.api_key else {}


class Settings(BaseSettings):
    paperless_url: str = "http://localhost:8000"
    paperless_token: str = ""
    paperless_external_url: str = ""
    paperless_skip_tag_names: str = "needs-review"

    gemini_api_key: str = ""
    llm_model: str = Field("gemini-3.8-flash", validation_alias=AliasChoices("LLM_MODEL", "GEMINI_MODEL"))
    fallback_model: str = "gpt-5.4-mini"

    # Any OpenAI-compatible endpoint. Empty falls back to the legacy LiteLLM pair.
    llm_base_url: str = ""
    llm_api_key: str = ""
    litellm_url: str = "http://localhost:4000"
    litellm_api_key: str = ""
    # Empty inherits the chat endpoint and key.
    embedding_base_url: str = ""
    embedding_api_key: str = ""
    embedding_model: str = "text-embedding-3-large"
    embedding_dimensions: int = Field(default=3072, ge=1, le=16000)
    strands_enabled: bool = True
    question_pipeline_enabled: bool = False
    strands_model: str = ""
    strands_call_timeout_seconds: float = 45
    strands_max_concurrent_calls: int = Field(default=4, ge=1, le=16)
    stream_verification_timeout_seconds: float = 60
    answer_audit_timeout_seconds: float = 60
    source_date_order: Literal["mdy", "dmy", "reject_ambiguous"] = "mdy"

    neo4j_uri: str = "bolt://neo4j:7687"
    neo4j_user: str = "neo4j"
    neo4j_password: str = ""

    postgres_host: str = "pgvector"
    postgres_port: int = 5432
    postgres_db: str = "knowledge_graph"
    postgres_user: str = "kguser"
    postgres_password: str = ""

    redis_url: str = "redis://localhost:6379"

    owner_name: str = ""
    owner_context: str = ""

    max_concurrent_docs: int = 10
    auto_sync_interval_minutes: int = 0
    entity_steward_interval_minutes: int = 360
    entity_steward_candidate_limit: int = 40

    model_config = {"env_file": os.environ.get("KG_ENV_FILE", ".env") or None, "extra": "ignore",
                    "populate_by_name": True}

    @field_validator("llm_base_url", "litellm_url", "embedding_base_url")
    @classmethod
    def _endpoint_url_is_usable(cls, value: str) -> str:
        if value:
            normalize_base_url(value)
        return value

    @property
    def llm_endpoint(self) -> ModelEndpoint:
        if self.llm_base_url:
            return ModelEndpoint(normalize_base_url(self.llm_base_url), self.llm_api_key, litellm=False)
        return ModelEndpoint(normalize_base_url(self.litellm_url), self.litellm_api_key, litellm=True)

    @property
    def embedding_endpoint(self) -> ModelEndpoint:
        if self.embedding_base_url:
            return ModelEndpoint(normalize_base_url(self.embedding_base_url), self.embedding_api_key, litellm=False)
        return self.llm_endpoint

    @property
    def postgres_dsn(self) -> str:
        return (
            f"postgresql://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )

    @property
    def effective_paperless_external_url(self) -> str:
        return self.paperless_external_url or self.paperless_url


settings = Settings()
