# Terminus Spatial Agent

Part of the [Terminus GIS](../README.md) monorepo. Natural-language spatial query agent for the Iceberg lakehouse. Users type questions in the webmap chat panel, and the agent classifies intent, routes discovery queries directly to MCP tools, generates spatial SQL for analytical queries, and pushes results to the map in real time. Runs as a standalone container (port 8090) with a containerized Ollama LLM.

Supports three LLM backends: **Ollama** (default, containerized), **vLLM**, and **AWS Bedrock**.

## Architecture

```
User (webmap chat panel)
  │
  │  POST /api/agent/chat  {session_id, message, active_layers}
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
| **spatial** | Planner → SQL → MCP `materialize_result` | Yes | Spatial queries that produce map layers (proximity, spatial joins, buffers, aggregation) |
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

### Spatial SQL Generation

For spatial and analytics intents, the agent generates DuckDB SQL via few-shot prompting. The system prompt includes chain-of-thought examples covering:

| Category | Examples |
|----------|----------|
| **Proximity / Nearest** | K-nearest neighbor, closest X to named Y, distance between features |
| **Spatial Joins** | Point-in-polygon (ST_Contains), line-polygon intersection (ST_Intersects), distance-based joins (ST_DWithin) |
| **Buffer Analysis** | Meter-accurate buffers via CRS transform, dissolved buffers (ST_Union_Agg) |
| **Spatial Aggregation** | COUNT/SUM/AVG with spatial GROUP BY, filtered aggregation |

Key prompt rules ensure correct SQL:
- Geometry columns are WKB BLOBs — must wrap with `ST_GeomFromWKB()` in spatial functions
- Distances in EPSG:4326 are in degrees — convert meters via `/ 111320.0`
- Column names come from schema context, not from examples
- VARCHAR columns use simple equality; STRUCT/JSON columns use `::JSON->>'key'`

### Schema Context

The `SchemaBuilder` discovers available tables and columns via MCP `list_tables` + `describe_table`, then injects this context into the LLM prompt. Scratch tables (from previous agent sessions) are automatically filtered out to keep the prompt focused.

**Categorical value sampling:** For columns named `class`, `subtype`, `basic_category`, `type`, or `category`, the builder samples up to 15 distinct values and includes them in the schema context (e.g. `→ class values: apartments, school, hospital`). This helps the LLM distinguish between tables with similar names but different semantics.

**Namespace filtering:** When the webmap sends `active_layers` (the currently visible layer keys), the builder extracts namespaces and preferentially includes only tables from those namespaces. This prevents the LLM from picking tables in the wrong namespace when multiple datasets are loaded.

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
    ├── fixtures/
    │   ├── canonical_questions.json  # 52 NL2Spatial test cases
    │   └── paraphrases.json          # 208 controlled paraphrases
    ├── eval_nl2spatial.py            # Integration eval harness (live Docker)
    ├── test_intent.py                # Intent classification (86 tests)
    ├── test_tool_router.py           # Tool routing + formatting (66 tests)
    ├── test_paraphrases.py           # Paraphrase robustness (268 tests)
    ├── test_retry.py                 # Retry loop + error hints (16 tests)
    ├── test_sql_gen.py               # SQL extraction + validation (17 tests)
    ├── test_schema.py                # Schema builder (4 tests)
    ├── test_server.py                # FastAPI endpoints (8 tests)
    ├── test_mcp_client.py            # MCP client (5 tests)
    ├── test_llm.py                   # LLM backends
    ├── test_registry.py              # Model detection
    ├── test_session.py               # Session state
    └── test_notify.py                # Lakehouse notifications
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

The agent has a two-tier test suite: fast unit tests (mocked, no Docker) and integration eval tests (live against Docker services).

### Unit Tests (482 tests)

```bash
pip install -e ".[dev]"
pytest tests/ -m "not live" -v
```

| Suite | Tests | What It Covers |
|-------|-------|----------------|
| `test_intent.py` | 86 | Intent classification across all 52 canonical questions + Tier 2/3 edge cases |
| `test_tool_router.py` | 66 | Rule-based routing for 14 MCP tools: pattern matching, parameter extraction, result formatting |
| `test_paraphrases.py` | 268 | Robustness: 208 intent tests + 60 routing tests across 4 paraphrases per canonical question |
| `test_retry.py` | 16 | Retry loop + 10 spatial error hint pattern tests (WKB mismatch, GROUP BY, column not found, etc.) |
| `test_sql_gen.py` | 17 | SQL extraction from LLM output, validation, DDL stripping |
| `test_schema.py` | 4 | Schema builder caching, context generation |
| `test_server.py` | 8 | FastAPI endpoint routing, SSE streaming |
| `test_mcp_client.py` | 5 | MCP tool calling, error handling |
| Others | 12 | LLM client, model registry, session management, notifications |

### Integration Eval (52 questions, live)

Runs all 52 canonical questions against live Docker services and generates a scoring report.

```bash
# Requires running Docker stack
pytest tests/eval_nl2spatial.py -m live -v

# Single question
pytest tests/eval_nl2spatial.py -m live -v -k Q40

# Report generated at tests/eval_report.md
```

Scores each question on: intent classification, tool selection, SQL pattern matching, and execution success. Generates `tests/eval_report.md` with results by tier, category, and per-query detail.

**Last run: 87% execution accuracy** (41/47 scored).

| Tier | Description | Pass Rate |
|------|-------------|-----------|
| Tier 1 | Basic — discovery, preview, spatial filter | 80% |
| Tier 2 | Intermediate — proximity, spatial joins, buffers, aggregation | 87% |
| Tier 3 | Advanced — temporal, export, materialization, ambiguous | 94% |

### Test Fixtures

- `tests/fixtures/canonical_questions.json` — 52 canonical NL questions with expected intents, tools, params, and SQL patterns
- `tests/fixtures/paraphrases.json` — 208 controlled paraphrases (4 per question) for robustness testing

### NL2Spatial Training Methodology

The test corpus follows the NL2GeoSQL evaluation methodology (GeoSQL-Eval, SpatialQueryQA) with three difficulty tiers:

- **Tier 1 (Q1-Q19):** Single-step operations — schema discovery, data preview, simple spatial filtering
- **Tier 2 (Q20-Q35):** Multi-parameter — proximity/nearest neighbor, spatial joins, buffer analysis, spatial aggregation
- **Tier 3 (Q36-Q52):** Multi-step reasoning — temporal queries, export/materialization, compound spatial, ambiguous/edge cases

Each question tests: intent classification accuracy, tool selection (for meta queries), SQL pattern correctness (for spatial/analytics), and end-to-end execution success.

## Air-Gapped Deployment

Download model weights for offline use:

```bash
./scripts/download_models.sh ./models
```

Then configure vLLM or Ollama to serve from the local `./models` directory.
