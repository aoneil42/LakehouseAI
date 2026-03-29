# Terminus GIS

Cloud Native GIS Suite | AI & Analytics

Upload geospatial data, serve it through three API standards (OGC, Esri, GeoParquet), visualize on a full-featured deck.gl webmap with GIS tools, and query with plain English via an AI spatial agent.

## Components

| Directory | Description |
|-----------|-------------|
| [`lakehouse/`](lakehouse/) | Core platform — containerized stack with Iceberg storage, DuckDB, three API surfaces, and a deck.gl webmap with GIS tools (identify, measure, attribute table, symbology, time slider, export) |
| [`icebergmcp/`](icebergmcp/) | MCP server — 19 tools for LLM agents to discover, query, and analyze spatial data |
| [`spatialagent/`](spatialagent/) | Spatial agent — routes discovery queries to MCP tools, generates spatial SQL for analytical queries, pushes results to the map |

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Webmap (deck.gl v9)                       │
│   MapLibre + GeoArrow + identify/measure/symbology/time    │
└──────────┬──────────────────────────────────┬───────────────┘
           │ REST/GeoParquet                  │ WebSocket
           ▼                                  ▼
┌─────────────────────┐            ┌─────────────────────┐
│   Terminus APIs     │            │   Spatial Agent     │
│  FastAPI · pygeoapi │            │   NL → SQL → MCP    │
│  Esri GeoServices   │            │   (port 8090)       │
│  (port 80)          │            └──────────┬──────────┘
└──────────┬──────────┘                       │ MCP tools
           │                       ┌──────────▼──────────┐
           │                       │   MCP Server        │
           │                       │   19 spatial tools   │
           │                       │   (port 8082)       │
           │                       └──────────┬──────────┘
           └──────────────┬───────────────────┘
                          ▼
               ┌─────────────────────┐
               │  Apache Iceberg     │
               │  LakeKeeper + S3    │
               │  (Garage or AWS)    │
               └─────────────────────┘

  Optional services (activated via --profile):
  ┌────────────────┐  ┌────────────────┐  ┌────────────────┐
  │ Ollama (agent) │  │ TileServer     │  │ SedonaSpark    │
  │ local LLM      │  │ (disconnected) │  │ (heavy)        │
  │ port 11434     │  │ port 8070      │  │ port 8888      │
  └────────────────┘  └────────────────┘  └────────────────┘
```

## Quick Start

```bash
git clone https://github.com/aoneil42/LakehouseAI.git
cd LakehouseAI/lakehouse

# Start the core stack
docker compose up -d

# One-time bootstrap (generates secrets, creates S3 bucket + Iceberg warehouse)
chmod +x bootstrap.sh
./bootstrap.sh

# Open the webmap
open http://localhost
```

See [`lakehouse/README.md`](lakehouse/) for full setup details, API documentation, and data upload instructions.

## Prerequisites

- Docker Desktop (8GB+ RAM; 16GB+ if using Sedona)
- Git
- `curl` (for bootstrap)

## Deployment Profiles

```bash
docker compose up -d                                          # Core stack
docker compose --profile agent up -d                          # + AI agent + Ollama
docker compose --profile heavy up -d                          # + SedonaSpark
docker compose --profile disconnected up -d                   # + bundled tileserver
docker compose --profile agent --profile disconnected up -d   # Full air-gapped
```

**First-run model pull** (after starting with `--profile agent`):
```bash
./scripts/pull-models.sh     # pulls devstral-small-2 into the Ollama container
```

**Environment overrides:**
- `SA_LLM_BACKEND=bedrock` — use AWS Bedrock instead of local Ollama
- `BASEMAP_CONFIG=basemaps-esri.json` — Esri portal basemaps
- `BASEMAP_CONFIG=basemaps-disconnected.json` — bundled offline tiles

See [`spatialagent/README.md`](spatialagent/) for agent configuration and model options.

## MCP Server (AI Tool Access)

Connect any MCP-compatible client (Claude Desktop, Cursor, Claude Code) to query the lakehouse:

```bash
# Claude Code
claude mcp add terminus-mcp --transport http --url http://localhost:8082/mcp
```

See [`icebergmcp/README.md`](icebergmcp/) for all 19 tools and configuration.

## Testing

Each component has its own test suite. The spatial agent includes an NL2Spatial evaluation harness based on research benchmarks (GeoSQL-Eval, SpatialQueryQA).

```bash
# MCP server — in-memory DuckDB, no Docker needed
cd icebergmcp && pytest -v

# Spatial agent — unit tests (482 tests, mocked)
cd spatialagent && pytest tests/ -m "not live" -v

# Spatial agent — integration eval (52 questions against live Docker)
cd spatialagent && pytest tests/eval_nl2spatial.py -m live -v
```

| Component | Tests | Coverage |
|-----------|-------|----------|
| MCP Server (`icebergmcp/`) | 21 | All 19 tools: spatial queries, joins, buffers, export, materialization |
| Spatial Agent (`spatialagent/`) | 482 | Intent classification, tool routing, SQL generation, error correction, paraphrase robustness |
| Integration Eval | 52 | End-to-end NL queries across 3 difficulty tiers (87% execution accuracy) |

See [`spatialagent/README.md`](spatialagent/) for the full NL2Spatial testing methodology.

## AI Integration

See [`AI-INTEGRATION.md`](AI-INTEGRATION.md) for a detailed explanation of how this stack implements AI — from the Model Context Protocol (MCP) tool layer through the natural-language agent to the webmap chat interface.

## License

Apache-2.0
