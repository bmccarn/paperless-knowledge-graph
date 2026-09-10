"""Explicit local-only configuration before importing application modules."""
import os


def configure_test_environment():
    os.environ.update({
        "KG_ENV_FILE": "",
        "PAPERLESS_URL": "http://127.0.0.1:1",
        "PAPERLESS_TOKEN": "synthetic-test-token",
        "PAPERLESS_EXTERNAL_URL": "http://127.0.0.1:1",
        "LITELLM_URL": "http://127.0.0.1:1",
        "LITELLM_API_KEY": "synthetic-test-key",
        "GEMINI_API_KEY": "synthetic-test-key",
        "NEO4J_URI": "bolt://127.0.0.1:1",
        "NEO4J_PASSWORD": "synthetic-test-password",
        "POSTGRES_HOST": "127.0.0.1",
        "POSTGRES_PORT": "1",
        "POSTGRES_PASSWORD": "synthetic-test-password",
        "REDIS_URL": "memory://",
        "STRANDS_ENABLED": "false",
        "QUESTION_PIPELINE_ENABLED": "false",
        "LITELLM_LOCAL_MODEL_COST_MAP": "True",
        "OWNER_NAME": "Synthetic document owner",
        "OWNER_CONTEXT": "Synthetic test corpus only",
        "AUTO_SYNC_INTERVAL_MINUTES": "0",
        "ENTITY_STEWARD_INTERVAL_MINUTES": "0",
    })
