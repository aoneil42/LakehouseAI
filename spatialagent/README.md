# Spatial Lakehouse Agent

Natural-language spatial query agent for the Iceberg lakehouse. Users type questions in the webmap chat panel, the agent translates to spatial SQL, executes via MCP tools, and pushes results to the map in real time. Runs as a standalone container (port 8090) joining the lakehouse docker-compose network — all data interaction goes through the MCP server's 19 typed tools via Streamable HTTP.

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

## Quick Start

```bash
# From lakehouse repo root:
docker compose -f docker-compose.yml \
  -f ../spatial-lakehouse-agent/docker-compose.agent.yml up -d
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
- **Spatial Analysis (5):** `spatial_filter`, `spatial_join`, `nearest_features`, `aggregate_within`, `buffer_analysis`, `get_bbox`
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
