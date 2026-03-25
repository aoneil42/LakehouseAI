# LakehouseAI

A geospatial data lakehouse with natural-language spatial queries. Upload geospatial data, serve it through three API standards (OGC, Esri, GeoParquet), visualize on a full-featured deck.gl webmap with GIS tools, and query with plain English via an AI agent.

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
│   Lakehouse APIs    │            │   Spatial Agent     │
│  FastAPI · pygeoapi │            │   NL → SQL → MCP    │
│  Esri GeoServices   │            │   (port 8090)       │
│  (port 80)          │            └──────────┬──────────┘
└──────────┬──────────┘                       │ MCP tools
           │                                  ▼
           │                       ┌─────────────────────┐
           │                       │   MCP Server        │
           │                       │   19 spatial tools   │
           │                       │   (port 8082)       │
           │                       └──────────┬──────────┘
           │                                  │
           └──────────────┬───────────────────┘
                          ▼
               ┌─────────────────────┐
               │  Apache Iceberg     │
               │  LakeKeeper + S3    │
               │  (Garage or AWS)    │
               └─────────────────────┘
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

## Optional: AI Agent

To enable natural-language spatial queries in the webmap chat panel:

1. Install [Ollama](https://ollama.ai) and pull a model:
   ```bash
   ollama pull devstral-small:latest
   ```

2. Start the agent alongside the lakehouse:
   ```bash
   docker compose -f lakehouse/docker-compose.yml \
     -f spatialagent/docker-compose.agent.yml up -d
   ```

See [`spatialagent/README.md`](spatialagent/) for configuration and model options.

## MCP Server (AI Tool Access)

Connect any MCP-compatible client (Claude Desktop, Cursor, Claude Code) to query the lakehouse:

```bash
# Claude Code
claude mcp add spatial-lakehouse --transport http --url http://localhost:8082/mcp
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
