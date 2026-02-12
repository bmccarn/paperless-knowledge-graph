from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    paperless_url: str = "http://localhost:8000"
    paperless_token: str = ""
    paperless_external_url: str = ""

    gemini_api_key: str = ""
    gemini_model: str = "gemini-2.5-flash"

    litellm_url: str = "http://localhost:4000"
    litellm_api_key: str = ""
    embedding_model: str = "text-embedding-3-large"

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

    model_config = {"env_file": ".env", "extra": "ignore"}

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
