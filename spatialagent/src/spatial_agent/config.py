from pydantic_settings import BaseSettings
from pydantic import Field


class Settings(BaseSettings):
    model_config = {"env_prefix": "SA_"}

    # MCP connection
    mcp_endpoint: str = Field(default="http://mcp-server:8082/mcp")

    # Lakehouse API (for notify endpoint)
    lakehouse_api: str = Field(default="http://lakehouse-api:8000")

    # LLM serving
    llm_backend: str = Field(default="vllm", description="'vllm', 'ollama', or 'bedrock'")
    vllm_base_url: str = Field(default="http://localhost:8000/v1")
    ollama_base_url: str = Field(default="http://localhost:11434")
    bedrock_region: str = Field(default="us-east-1")
    bedrock_model_id: str = Field(default="us.anthropic.claude-sonnet-4-20250514")

    # Model selection
    primary_model: str = Field(default="devstral-small-2")
    mid_model: str = Field(default="ministral-3-14b-instruct")
    fast_model: str = Field(default="duckdb-nsql-7b")
    active_model: str = Field(default="", description="Override: force specific model")

    # Session
    scratch_prefix: str = Field(default="_scratch_")
    max_retry: int = Field(default=3, ge=1, le=5)
    query_timeout: int = Field(default=60, ge=10, le=300)

    # Server
    host: str = Field(default="0.0.0.0")
    port: int = Field(default=8090)


settings = Settings()
