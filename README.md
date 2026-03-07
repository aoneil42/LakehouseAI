# LakehouseAI

A geospatial data lakehouse with natural-language spatial queries. Upload geospatial data, serve it through three API standards (OGC, Esri, GeoParquet), visualize on a deck.gl webmap, and query with plain English via an AI agent.

## Components

| Directory | Description |
|-----------|-------------|
| [`lakehouse/`](lakehouse/) | Core platform — containerized stack with Iceberg storage, DuckDB, three API surfaces, and a deck.gl webmap |
| [`icebergmcp/`](icebergmcp/) | MCP server — 18 tools for LLM agents to discover, query, and analyze spatial data |
| [`spatialagent/`](spatialagent/) | Spatial agent — translates natural language to spatial SQL, executes via MCP, pushes results to the map |

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Webmap (deck.gl v9)                       │
│         MapLibre + GeoArrow pipeline + chat panel           │
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
           │                       │   18 spatial tools   │
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

See [`icebergmcp/README.md`](icebergmcp/) for all 18 tools and configuration.

## License

Apache-2.0
