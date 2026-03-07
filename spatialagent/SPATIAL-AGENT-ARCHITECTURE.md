# Spatial Lakehouse Agent — Architecture & Implementation Guide

## Purpose

New repo (`spatial-lakehouse-agent`) providing natural-language spatial query
capabilities over the Iceberg lakehouse. Users type questions in the webmap
chat panel, the agent translates to spatial SQL, executes via MCP tools, and
pushes results to the map in real time.

Runs as a standalone container (port 8090) joining the lakehouse docker-compose
network. **No direct database access** — all data interaction goes through the
MCP server's 19 typed tools via Streamable HTTP.

---

## Architecture

```
User (webmap chat panel)
  │
  │  POST /api/agent/chat  {session_id, message}
  │  (proxied by nginx → spatial-agent:8090)
  ▼
┌─────────────────────────────────────────────────┐
│  spatial-lakehouse-agent (port 8090)            │
│                                                 │
│  ┌──────────┐  ┌──────────┐  ┌───────────┐     │
│  │  Router   │─▶│  Planner │─▶│  Executor │     │
│  │  (intent) │  │  (NL→SQL)│  │  (MCP)    │     │
│  └──────────┘  └──────────┘  └───────────┘     │
│       │                            │            │
│       │  fast analytics path       │            │
│       ▼                            ▼            │
│  ┌────────────┐   ┌───────────────────┐         │
│  │ DuckDB-NSQL│   │ Devstral/Ministral│         │
│  │ (optional) │   │ (via vLLM/Ollama) │         │
│  └────────────┘   └───────────────────┘         │
└───────────────────────┬─────────────────────────┘
                        │ MCP tool calls (Streamable HTTP)
                        ▼
               ┌─────────────────┐
               │  MCP Server     │
               │  (port 8082)    │
               │  19 spatial     │
               │  tools          │
               └────────┬────────┘
                        │ DuckDB + Iceberg
                        ▼
               ┌─────────────────┐
               │  Lakehouse      │
               │  (Garage S3 +   │
               │   LakeKeeper)   │
               └────────┬────────┘
                        │ POST /api/agent/notify/{session_id}
                        ▼
               ┌─────────────────┐
               │  Lakehouse API  │── WS push ──▶ Webmap
               │  (port 8000)    │              (layer appears)
               └─────────────────┘
```

---

## Key Decisions

| # | Decision | Rationale |
|---|----------|-----------|
| 1 | Separate repo | Different release cadence, hardware reqs, optional deployment |
| 2 | vLLM production, Ollama dev | vLLM: better tool-call parsing, throughput. Ollama: simpler local |
| 3 | Devstral Small 2 (24B) primary | 68% SWE-Bench, 256K context, Apache 2.0, agentic tool-use |
| 4 | Ministral 3 14B mid-tier | For 16GB GPU environments |
| 5 | DuckDB-NSQL-7B specialist | Fast analytics routing, 5GB VRAM |
| 6 | MCP client over Streamable HTTP | Standard transport, calls port 8082 |
| 7 | Session-scoped scratch | `_scratch_{session_id[:8]}` namespace per tab |
| 8 | Hardware-based model routing | VRAM ≥20GB→Devstral, 10-19→Ministral, <10→NSQL |
| 9 | SQL validation + retry | Pre-parse + post-exec retry (max 3×) with error feedback |
| 10 | Streaming responses | SSE to chat panel for incremental status |
| 11 | No Chinese-origin models | Excludes Qwen, DeepSeek, XiYan per policy |
| 12 | Air-gapped deployment | Weights + images via internal registry |
| 13 | Anonymous UUID sessions | `crypto.randomUUID()` from webmap, no auth v1 |

---

## Project Structure

```
spatial-lakehouse-agent/
├── CLAUDE.md
├── README.md
├── pyproject.toml
├── Dockerfile
├── Dockerfile.dev
├── docker-compose.agent.yml
├── .env.example
│
├── src/
│   └── spatial_agent/
│       ├── __init__.py
│       ├── config.py            # Pydantic settings (SA_ prefix)
│       ├── server.py            # FastAPI app (port 8090)
│       ├── session.py           # Session manager (UUID → state)
│       ├── router/
│       │   ├── __init__.py
│       │   └── intent.py        # Intent classification
│       ├── planner/
│       │   ├── __init__.py
│       │   ├── schema.py        # Schema context builder (calls MCP discovery tools)
│       │   ├── prompts.py       # System prompts with schema injection
│       │   └── sql_gen.py       # LLM call → SQL extraction → validation
│       ├── executor/
│       │   ├── __init__.py
│       │   ├── mcp_client.py    # MCP Streamable HTTP client wrapper
│       │   ├── tool_picker.py   # Maps intent → optimal MCP tool
│       │   └── retry.py         # Error → re-prompt loop (max 3×)
│       ├── models/
│       │   ├── __init__.py
│       │   ├── registry.py      # Discover available models by VRAM
│       │   └── llm.py           # Unified interface (vLLM / Ollama backends)
│       └── notify/
│           ├── __init__.py
│           └── lakehouse.py     # POST notify to lakehouse API
│
├── prompts/
│   ├── system_spatial.txt
│   ├── system_analytics.txt
│   ├── schema_context.txt
│   └── error_correction.txt
│
├── tests/
│   ├── conftest.py
│   ├── test_intent.py
│   ├── test_sql_gen.py
│   ├── test_mcp_client.py
│   ├── test_retry.py
│   └── test_session.py
│
└── scripts/
    ├── download_models.sh
    └── health_check.py
```

---

## Component Specifications

### 1. `config.py`

```python
from pydantic_settings import BaseSettings
from pydantic import Field

class Settings(BaseSettings):
    model_config = {"env_prefix": "SA_"}

    # MCP connection
    mcp_endpoint: str = Field(default="http://mcp-server:8082/mcp")

    # Lakehouse API (for notify endpoint)
    lakehouse_api: str = Field(default="http://lakehouse-api:8000")

    # LLM serving
    llm_backend: str = Field(default="vllm", description="'vllm' or 'ollama'")
    vllm_base_url: str = Field(default="http://localhost:8000/v1")
    ollama_base_url: str = Field(default="http://localhost:11434")

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
```

### 2. `server.py` — FastAPI Application

Three endpoints:

| Method | Path | Purpose |
|--------|------|---------|
| POST | `/api/agent/chat` | Main chat endpoint (SSE streaming) |
| GET | `/api/agent/health` | Health check |
| GET | `/api/agent/models` | Available models + active selection |

**POST `/api/agent/chat`** — request body:

```json
{"session_id": "uuid-string", "message": "show buildings within 500m of the river"}
```

Response: `text/event-stream` (SSE) with typed chunks:

```
data: {"type": "status", "content": "Discovering schema..."}
data: {"type": "status", "content": "Generating SQL..."}
data: {"type": "sql", "content": "SELECT b.* FROM ..."}
data: {"type": "status", "content": "Executing query..."}
data: {"type": "result", "content": "Found 847 buildings. Layer added to map."}
data: {"type": "done"}
```

The `sql` event lets power users inspect what was generated. The actual map
layer arrives via the WebSocket push from the lakehouse API, not this stream.

Use `StreamingResponse` with `media_type="text/event-stream"`. Async generator
yields events as the pipeline progresses. Conversational queries respond
directly with `result` event, no SQL/MCP execution.

### 3. `router/intent.py` — Intent Classification

| Intent | Model | Action |
|--------|-------|--------|
| `spatial` — geometry, location, proximity | Primary (Devstral) | Full NL→SQL with spatial MCP tools |
| `analytics` — tabular aggregation, no geometry | Fast (NSQL) or Primary | Simpler SQL, `query`/`multi_table_query` tools |
| `conversational` — greetings, help, clarification | Primary | Direct text, no SQL |

**v1:** Keyword + regex heuristic. Spatial keywords: `near`, `within`,
`buffer`, `intersect`, `distance`, `polygon`, `boundary`, `adjacent`,
`surrounding`, `closest`, `overlap`, `contains`, `crosses`, `touches`,
`lat`, `lon`, `coordinate`, `radius`, `bbox`, `envelope`. No LLM call,
deterministic, fast.

**v2 upgrade:** Single LLM classification call.

### 4. `planner/schema.py` — Schema Context Builder

Before SQL generation, discover tables and columns via MCP:

1. Call `list_tables()` → namespaces and tables
2. For likely-relevant tables (keyword match against table names), call
   `describe_table(table)` → columns and types
3. For geometry tables, call `get_bbox(table)` → spatial extent
4. Cache in session state (TTL 5 min)
5. Format into prompt-injectable context string:

```
Available tables:
  default.buildings (id INT, name VARCHAR, height FLOAT, geom GEOMETRY)
    → geometry: geom | bbox: [-77.5, 38.8, -77.0, 39.0]
  default.roads (id INT, name VARCHAR, type VARCHAR, geom GEOMETRY)
    → geometry: geom | bbox: [-77.5, 38.8, -77.0, 39.0]
```

Only include tables matching the query's likely intent.

### 5. `planner/sql_gen.py` — SQL Generation

System prompt (`prompts/system_spatial.txt`):

```
You are a spatial SQL expert. Generate DuckDB SQL queries against an Iceberg
lakehouse with the DuckDB spatial extension.

Rules:
- Tables referenced as: lakehouse.{namespace}.{table_name}
- DuckDB spatial functions: ST_Intersects, ST_DWithin, ST_Buffer, ST_Distance,
  ST_Contains, ST_Area, ST_Length, ST_Centroid, ST_Extent, ST_GeomFromText,
  ST_Point, ST_Transform, ST_Union, ST_Intersection
- Geometry columns typically named "geom" or "geometry"
- Always include geometry column in SELECT for map visualization
- Use ST_DWithin(a, b, distance_meters) for proximity queries
- Coordinates are EPSG:4326 (lon/lat)
- Return ONLY the SQL query, no explanation

{schema_context}

User query: {user_message}
```

**SQL extraction** from LLM output:

1. Look for ` ```sql ... ``` ` fences → extract content
2. If no fences, find lines starting with `SELECT`, `WITH`
3. Take first complete statement
4. Strip non-SQL prefix/suffix

**Pre-execution validation:**

1. Must be SELECT or WITH...SELECT
2. Table references match known tables
3. No obvious syntax errors (unmatched parens, missing FROM)
4. On failure → retry loop

### 6. `executor/mcp_client.py` — MCP Client

```python
import json
from mcp import ClientSession
from mcp.client.streamable_http import streamablehttp_client

class MCPClient:
    def __init__(self, endpoint: str):
        self.endpoint = endpoint

    async def call_tool(self, tool_name: str, arguments: dict) -> dict:
        async with streamablehttp_client(self.endpoint) as (read, write, _):
            async with ClientSession(read, write) as session:
                await session.initialize()
                result = await session.call_tool(tool_name, arguments)
                text = next(
                    (c.text for c in result.content if hasattr(c, 'text')), '{}'
                )
                return json.loads(text)

    async def list_tools(self) -> list:
        async with streamablehttp_client(self.endpoint) as (read, write, _):
            async with ClientSession(read, write) as session:
                await session.initialize()
                return (await session.list_tools()).tools
```

v1: fresh connection per call (same Docker network, sub-ms). v2: pool if needed.

### 7. `executor/tool_picker.py`

**v1 strategy:** Use `materialize_result` for everything map-visible. Typed
spatial tools become v2 optimizations.

```python
def pick_tool(sql: str, should_materialize: bool, session_id: str):
    if should_materialize:
        return "materialize_result", {
            "sql": sql,
            "result_name": generate_result_name(sql),
            "namespace": f"_scratch_{session_id.replace('-','')[:8]}",
            "overwrite": True,
        }
    else:
        return "query", {"sql": sql, "limit": 100}
```

### 8. `executor/retry.py`

Error correction prompt template:

```
The SQL you generated failed:
Error: {error_message}
Original query: {failed_sql}
Original user request: {user_message}
{schema_context}
Fix the SQL. Return ONLY the corrected query.
```

Loop up to `max_retry` (3). Emit SSE status events: "Retrying (attempt 2/3)..."

### 9. `notify/lakehouse.py`

```python
import httpx

async def notify_lakehouse(
    lakehouse_api: str, session_id: str,
    namespace: str, table: str,
    row_count: int, description: str = "",
):
    async with httpx.AsyncClient() as client:
        await client.post(
            f"{lakehouse_api}/api/agent/notify/{session_id}",
            json={"namespace": namespace, "table": table,
                  "row_count": row_count, "description": description},
            timeout=10.0,
        )
```

### 10. `session.py`

```python
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List

@dataclass
class SessionState:
    session_id: str
    created_at: datetime = field(default_factory=datetime.utcnow)
    schema_cache: dict = field(default_factory=dict)
    schema_cache_ts: float = 0.0
    history: List[dict] = field(default_factory=list)
    scratch_namespace: str = ""

    def __post_init__(self):
        self.scratch_namespace = f"_scratch_{self.session_id.replace('-','')[:8]}"

class SessionManager:
    def __init__(self):
        self._sessions: Dict[str, SessionState] = {}

    def get_or_create(self, session_id: str) -> SessionState:
        if session_id not in self._sessions:
            self._sessions[session_id] = SessionState(session_id=session_id)
        return self._sessions[session_id]

    def remove(self, session_id: str):
        self._sessions.pop(session_id, None)
```

Last N exchanges (default 5) kept for multi-turn context.

---

## MCP Tools Available (19 tools on port 8082)

Called via Streamable HTTP:

**Catalog Discovery (7):**
`list_namespaces`, `list_tables`, `describe_table`, `search_tables`,
`table_stats`, `table_snapshots`, `health_check`

**Query Execution (4):**
`query`, `multi_table_query`, `sample_data`, `time_travel_query`

**Spatial Analysis (5):**
`spatial_filter`, `spatial_join`, `nearest_features`, `aggregate_within`,
`buffer_analysis`, `get_bbox`

**Data Output (2):**
`export_geojson`, `materialize_result`

---

## Docker & Deployment

### `Dockerfile`

```dockerfile
FROM python:3.12-slim
RUN groupadd -r agent && useradd -r -g agent agent
WORKDIR /app
COPY pyproject.toml .
RUN pip install --no-cache-dir -e "."
COPY src/ src/
COPY prompts/ prompts/
COPY scripts/ scripts/
RUN chown -R agent:agent /app
USER agent
EXPOSE 8090
HEALTHCHECK --interval=30s --timeout=5s --retries=3 --start-period=15s \
    CMD ["python", "scripts/health_check.py"]
CMD ["uvicorn", "spatial_agent.server:app", "--host", "0.0.0.0", "--port", "8090"]
```

Agent does NOT run the LLM. Calls external vLLM or Ollama via HTTP.

### `Dockerfile.dev`

```dockerfile
FROM python:3.12-slim
WORKDIR /app
COPY pyproject.toml .
RUN pip install --no-cache-dir -e ".[dev]"
COPY . .
ENV SA_LLM_BACKEND=ollama
ENV SA_OLLAMA_BASE_URL=http://host.docker.internal:11434
EXPOSE 8090
CMD ["uvicorn", "spatial_agent.server:app", "--host", "0.0.0.0", "--port", "8090", "--reload"]
```

### `docker-compose.agent.yml` (overlay)

```yaml
# From lakehouse repo root:
#   docker compose -f docker-compose.yml \
#     -f ../spatial-lakehouse-agent/docker-compose.agent.yml up -d

services:
  spatial-agent:
    build:
      context: ../spatial-lakehouse-agent
      dockerfile: Dockerfile
    container_name: spatial-agent
    ports:
      - "8090:8090"
    environment:
      SA_MCP_ENDPOINT: "http://mcp-server:8082/mcp"
      SA_LAKEHOUSE_API: "http://lakehouse-api:8000"
      SA_LLM_BACKEND: "${SA_LLM_BACKEND:-vllm}"
      SA_VLLM_BASE_URL: "${SA_VLLM_BASE_URL:-http://vllm:8000/v1}"
      SA_OLLAMA_BASE_URL: "${SA_OLLAMA_BASE_URL:-http://host.docker.internal:11434}"
      SA_PRIMARY_MODEL: "${SA_PRIMARY_MODEL:-devstral-small-2}"
      SA_ACTIVE_MODEL: "${SA_ACTIVE_MODEL:-}"
    depends_on:
      mcp-server:
        condition: service_healthy
      api:
        condition: service_healthy
    networks:
      - lakehouse
    mem_limit: 2g
    restart: unless-stopped

networks:
  lakehouse:
    external: true
    name: iceberg-geospatial-api-server-main_lakehouse
```

### `pyproject.toml`

```toml
[project]
name = "spatial-lakehouse-agent"
version = "0.1.0"
requires-python = ">=3.11"
dependencies = [
    "fastapi>=0.115",
    "uvicorn[standard]>=0.34",
    "httpx>=0.27",
    "pydantic>=2.0",
    "pydantic-settings>=2.0",
    "mcp>=1.0",
    "openai>=1.0",
    "sse-starlette>=2.0",
]

[project.optional-dependencies]
dev = [
    "pytest>=8.0",
    "pytest-asyncio>=0.23",
    "respx>=0.21",
]
```

### Air-Gapped: `scripts/download_models.sh`

```bash
#!/bin/bash
MODELS_DIR="${1:-./models}"
mkdir -p "$MODELS_DIR"

# Primary: Devstral Small 2
huggingface-cli download mistralai/Devstral-Small-2-GGUF \
    --local-dir "$MODELS_DIR/devstral-small-2"

# Mid-tier: Ministral 3 14B Instruct
huggingface-cli download mistralai/Ministral-3-14B-Instruct-GGUF \
    --local-dir "$MODELS_DIR/ministral-3-14b"

# Specialist: DuckDB-NSQL-7B
huggingface-cli download motherduck/DuckDB-NSQL-7B-GGUF \
    --local-dir "$MODELS_DIR/duckdb-nsql-7b"

echo "Models downloaded to $MODELS_DIR"
```

---

## End-to-End Flow Example

User: **"Show me all buildings within 500m of the river centerline"**

1. **Router** → `spatial` (keywords: "within", "500m", "buildings")
2. **Schema** → `search_tables("building")`, `search_tables("river")`, then
   `describe_table` for matches
3. **SQL gen** → Devstral with schema context produces:
   ```sql
   SELECT b.* FROM lakehouse.default.buildings b,
     lakehouse.default.rivers r
   WHERE ST_DWithin(b.geom, r.geom, 500)
   ```
4. **Validate** → SELECT ✓, tables exist ✓, syntax ✓
5. **Tool pick** → `materialize_result(sql, "buildings_near_river", "_scratch_a1b2c3d4")`
6. **MCP call** → returns `{status: "ok", row_count: 847}`
7. **Notify** → POST to lakehouse API `/api/agent/notify/{session_id}`
8. **Lakehouse API** → computes bbox, pushes `layer_ready` via WebSocket
9. **Webmap** → loads layer, flies to bbox
10. **SSE** → "Found 847 buildings within 500m of the river. Layer added to map."

---

## Testing Strategy

**Unit tests (mock LLM + mock MCP):**

- `test_intent.py` — classification against keyword list
- `test_sql_gen.py` — extraction from fenced/unfenced LLM output, validation
- `test_retry.py` — error feedback loop with mocked MCP errors
- `test_session.py` — session lifecycle, schema caching

**Integration tests (mock LLM, real MCP):**

- `test_mcp_client.py` — call real MCP `health_check`, `list_tables`
  (requires MCP server running; skip in CI if unavailable)

**End-to-end (real LLM + real MCP + real lakehouse):**

- Manual or scripted via `curl` against running stack
- Test matrix: spatial filter, spatial join, buffer, aggregation,
  conversational query, malformed query (retry), multi-turn context
