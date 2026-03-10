# Spatial Lakehouse Agent

Part of the [LakehouseAI](../README.md) monorepo. Natural-language spatial query agent for the Iceberg lakehouse. Users type questions in the webmap chat panel, and the agent classifies intent, routes discovery queries directly to MCP tools, generates spatial SQL for analytical queries, and pushes results to the map in real time. Runs as a standalone container (port 8090) joining the lakehouse docker-compose network.

## Architecture

```
User (webmap chat panel)
  │
  │  POST /api/agent/chat  {session_id, message}
  │  (proxied by nginx → spatial-agent:8090)
  ▼
┌─────────────────────────────────────────────────────┐
│  spatial-lakehouse-agent (port 8090)                │
│                                                     │
│  ┌──────────┐                                       │
│  │  Router   │─── intent classifier ──┐             │
│  │  (intent) │                        │             │
│  └──────────┘                        │             │
│       │                              │             │
│       ├─ meta ──▶ ┌──────────────┐   │             │
│       │           │ Tool Router  │   │             │
│       │           │ (regex match)│   │             │
│       │           └──────┬───────┘   │             │
│       │                  │           │             │
│       │           ┌──────▼───────┐   │             │
│       │           │ LLM Search   │   │             │
│       │           │ (fallback)   │   │             │
│       │           └──────────────┘   │             │
│       │                              │             │
│       ├─ spatial ─▶ ┌────────────┐   │             │
│       │             │  Planner   │   │             │
│       ├─ analytics ▶│  (NL→SQL)  │   │             │
│       │             └─────┬──────┘   │             │
│       │                   ▼          │             │
│       │             ┌────────────┐   │             │
│       │             │  Executor  │───┘             │
│       │             │  (MCP)     │                 │
│       │             └────────────┘                 │
│       │                   │                        │
│       └─ conversational ──┘ (direct reply)         │
│                                                     │
│  LLM: Devstral / Ministral / DuckDB-NSQL           │
│       (via vLLM or Ollama)                          │
└───────────────────────┬─────────────────────────────┘
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

## Query Processing Pipeline

The agent classifies each query into one of four intents and routes accordingly:

| Intent | Route | LLM Used? | Description |
|--------|-------|-----------|-------------|
| **meta** | Tool Router → MCP tools | Only for fuzzy search | Discovery queries ("what tables exist?", "describe buildings schema") |
| **spatial** | Planner → SQL → MCP `materialize_result` | Yes | Spatial queries that produce map layers |
| **analytics** | Planner → SQL → MCP `query` | Yes | Aggregation, counts, statistics — results shown as tables |
| **conversational** | Direct reply | No | Greetings, help requests |

### Meta Query Routing (Discovery)

Discovery queries bypass the LLM SQL pipeline entirely. A rule-based tool router matches natural-language patterns to MCP catalog tools:

| Pattern | MCP Tool | Example |
|---------|----------|---------|
| List tables/datasets | `list_tables` | "What datasets are available?" |
| List namespaces | `list_namespaces` | "What namespaces exist?" |
| Describe table schema | `describe_table` | "Describe the buildings schema" |
| Tables with geometry | `search_tables` | "Which tables have geometry columns?" |
| Tables with specific columns | `search_tables` | "Are there tables with a timestamp column?" |
| Find tables by topic | `search_tables` | "Find tables related to transportation" |

When the tool router has no confident match, or for semantic search queries, the agent falls back to **LLM fuzzy search** — using the LLM to semantically match against the cached table catalog.

## Quick Start

```bash
# From the lakehouse/ directory:
docker compose --profile agent up -d
```

## Dev Setup

1. Start Ollama on host:
```bash
ollama serve
ollama pull devstral-small:latest
```

2. Run with dev Dockerfile:
```bash
docker build -f Dockerfile.dev -t spatial-agent-dev .
docker run -p 8090:8090 spatial-agent-dev
```

Or run directly:
```bash
pip install -e ".[dev]"
SA_LLM_BACKEND=ollama SA_OLLAMA_BASE_URL=http://localhost:11434 \
  uvicorn spatial_agent.server:app --port 8090 --reload
```

## Project Structure

```
spatialagent/
├── pyproject.toml
├── Dockerfile / Dockerfile.dev
├── docker-compose.agent.yml
├── .env.example
├── prompts/
│   ├── system_spatial.txt          # Spatial SQL generation prompt
│   ├── system_analytics.txt        # Analytics SQL generation prompt
│   └── error_correction.txt        # SQL retry prompt
├── src/spatial_agent/
│   ├── server.py                   # FastAPI app, SSE streaming, query orchestration
│   ├── config.py                   # Pydantic settings (SA_ env prefix)
│   ├── session.py                  # Session state + schema cache (5-min TTL)
│   ├── router/
│   │   ├── intent.py               # Intent classifier (spatial/analytics/meta/conversational)
│   │   ├── tool_router.py          # Rule-based MCP tool routing for meta queries
│   │   └── llm_search.py           # LLM-powered fuzzy table/column search
│   ├── planner/
│   │   ├── schema.py               # SchemaBuilder — discovers tables, columns, bbox via MCP
│   │   ├── prompts.py              # Prompt builders for spatial/analytics/error SQL
│   │   └── sql_gen.py              # SQL extraction + validation from LLM responses
│   ├── executor/
│   │   ├── mcp_client.py           # MCP Streamable HTTP client
│   │   ├── tool_picker.py          # Maps SQL to MCP tool (query vs materialize_result)
│   │   └── retry.py                # SQL generation retry loop with error feedback
│   ├── models/
│   │   ├── llm.py                  # LLMClient (vLLM + Ollama backends)
│   │   └── registry.py             # Model detection + selection by intent
│   └── notify/
│       └── lakehouse.py            # POST to lakehouse API after materialization
└── tests/
    ├── test_intent.py              # Intent classification tests
    ├── test_tool_router.py         # Tool router pattern matching + formatting tests
    ├── test_server.py
    ├── test_schema.py
    ├── test_sql_gen.py
    ├── test_retry.py
    ├── test_mcp_client.py
    ├── test_llm.py
    ├── test_registry.py
    ├── test_session.py
    └── test_notify.py
```

## Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `SA_MCP_ENDPOINT` | `http://mcp-server:8082/mcp` | MCP server Streamable HTTP endpoint |
| `SA_LAKEHOUSE_API` | `http://lakehouse-api:8000` | Lakehouse API base URL |
| `SA_LLM_BACKEND` | `vllm` | LLM backend: `vllm` or `ollama` |
| `SA_VLLM_BASE_URL` | `http://localhost:8000/v1` | vLLM OpenAI-compatible endpoint |
| `SA_OLLAMA_BASE_URL` | `http://localhost:11434` | Ollama API endpoint |
| `SA_PRIMARY_MODEL` | `devstral-small-2` | Primary model (spatial queries) |
| `SA_MID_MODEL` | `ministral-3-14b-instruct` | Mid-tier model (16GB GPU) |
| `SA_FAST_MODEL` | `duckdb-nsql-7b` | Fast analytics specialist |
| `SA_ACTIVE_MODEL` | *(empty)* | Override: force specific model |
| `SA_SCRATCH_PREFIX` | `_scratch_` | Session scratch namespace prefix |
| `SA_MAX_RETRY` | `3` | Max SQL retry attempts (1-5) |
| `SA_QUERY_TIMEOUT` | `60` | LLM call timeout in seconds (10-300) |
| `SA_HOST` | `0.0.0.0` | Server bind host |
| `SA_PORT` | `8090` | Server bind port |

## MCP Tools

19 tools available on the MCP server (port 8082):

- **Catalog Discovery (7):** `list_namespaces`, `list_tables`, `describe_table`, `search_tables`, `table_stats`, `table_snapshots`, `health_check`
- **Query Execution (4):** `query`, `multi_table_query`, `sample_data`, `time_travel_query`
- **Spatial Analysis (6):** `spatial_filter`, `spatial_join`, `nearest_features`, `aggregate_within`, `buffer_analysis`, `get_bbox`
- **Data Output (2):** `export_geojson`, `materialize_result`

## Testing

```bash
pip install -e ".[dev]"
pytest tests/ -v
```

## Air-Gapped Deployment

Download model weights for offline use:

```bash
./scripts/download_models.sh ./models
```

Then configure vLLM or Ollama to serve from the local `./models` directory.
