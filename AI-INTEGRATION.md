# AI Integration Architecture

How LakehouseAI implements AI — from the Model Context Protocol tool layer through the spatial agent to the webmap chat interface.

## Overview

LakehouseAI uses a three-layer AI architecture: a **tool layer** (MCP server) that exposes structured geospatial operations, an **agent layer** that translates natural language to tool calls, and a **presentation layer** (webmap chat panel) that streams results to the user.

```
User types: "Find buildings within 500m of the Louvre"
                    │
                    ▼
        ┌───────────────────┐
        │   Webmap Chat     │  ← Presentation layer
        │   (deck.gl + SSE) │     Streams events to user
        └────────┬──────────┘
                 │ POST /api/agent/chat
                 ▼
        ┌───────────────────┐
        │   Spatial Agent   │  ← Agent layer
        │   Intent → SQL    │     NL understanding + SQL generation
        │   (port 8090)     │
        └────────┬──────────┘
                 │ MCP tool calls (Streamable HTTP)
                 ▼
        ┌───────────────────┐
        │   MCP Server      │  ← Tool layer
        │   19 spatial tools│     Structured access to the lakehouse
        │   (port 8082)     │
        └────────┬──────────┘
                 │ DuckDB SQL
                 ▼
        ┌───────────────────┐
        │   Apache Iceberg  │  ← Storage layer
        │   LakeKeeper + S3 │     Data lives here
        └───────────────────┘
```

## Layer 1: MCP Tool Layer (icebergmcp)

The MCP server is the AI-facing interface to the lakehouse. It exposes 19 typed tools over the Model Context Protocol using Streamable HTTP transport. Any MCP client (Claude Desktop, Cursor, Claude Code, or the spatial agent) can connect and use these tools.

**Why MCP instead of direct SQL access?**

- **Safety**: Tools validate all inputs, enforce read-only queries, and prevent SQL injection. The LLM never has raw database access.
- **Abstraction**: Spatial operations like "find nearest K features" are one tool call instead of complex SQL with ST_Distance + ORDER BY + LIMIT + CRS transforms.
- **Consistency**: Geometry is stored as WKB BLOBs in Iceberg (which doesn't support native GEOMETRY). Tools handle the WKB wrapping/unwrapping transparently.
- **Discoverability**: The LLM can call `list_tables`, `describe_table`, and `search_tables` to understand the data before querying it.

**Implementation**: FastMCP (Python) with a single DuckDB in-process connection. DuckDB v1.5 natively ATTACHes to the Iceberg REST catalog, so the MCP server sees all Iceberg tables as regular DuckDB tables with full spatial function support.

### Tool Categories

| Category | Tools | Purpose |
|----------|-------|---------|
| Catalog Discovery | `list_namespaces`, `list_tables`, `describe_table`, `search_tables`, `table_stats`, `table_snapshots` | Let the LLM understand what data exists |
| Spatial Queries | `query`, `spatial_filter`, `nearest_features`, `get_bbox`, `time_travel_query`, `multi_table_query` | Execute spatial operations |
| Spatial Analysis | `spatial_join`, `aggregate_within`, `buffer_analysis` | Complex multi-table spatial operations |
| Data Output | `export_geojson`, `materialize_result` | Return results as GeoJSON or persist as new Iceberg tables |

## Layer 2: Spatial Agent (spatialagent)

The agent translates natural language questions into MCP tool calls. It runs as a FastAPI service (port 8090) and returns results as a Server-Sent Events (SSE) stream.

### Query Processing Pipeline

```
"Find buildings within 500m of the Louvre"
        │
   ┌────▼─────┐
   │  Intent   │  Keyword + regex classifier
   │ Classify  │  → spatial / analytics / meta / conversational
   └────┬──────┘
        │ spatial
   ┌────▼─────┐
   │  Schema   │  Calls list_tables + describe_table + table_stats via MCP
   │  Builder  │  Injects table schemas, geometry types, row counts,
   └────┬──────┘  and categorical value samples into LLM context
        │
   ┌────▼─────┐
   │  LLM SQL  │  Few-shot prompt with spatial examples
   │  Generate │  LLM: Devstral / Ministral / DuckDB-NSQL (via Ollama)
   └────┬──────┘
        │ "SELECT b.*, b.geometry FROM lakehouse.paris.buildings b
        │  WHERE ST_DWithin(ST_GeomFromWKB(b.geometry), ..., 500.0/111320.0)"
   ┌────▼─────┐
   │  Execute  │  Calls materialize_result MCP tool
   │  + Retry  │  On error: appends spatial-specific fix hints, retries up to 3x
   └────┬──────┘
        │ success
   ┌────▼─────┐
   │  Notify   │  POST to lakehouse API → WebSocket push to webmap
   │  Webmap   │  "Found 683 features. Layer added to map."
   └──────────┘
```

### Intent Classification

The classifier uses keyword matching and regex patterns (no LLM call) to sort queries into four categories:

| Intent | How It's Detected | What Happens |
|--------|-------------------|--------------|
| **meta** | Discovery keywords (table, schema, column, snapshot, export, geojson) | Rule-based tool router maps directly to MCP tool — no LLM needed |
| **spatial** | Spatial keywords (near, within, buffer, zone, meters, km) or materialization signals (save, create layer) | LLM generates DuckDB spatial SQL, result materialized as map layer |
| **analytics** | Data analysis terms (count, average, compare) without spatial keywords | LLM generates SQL, result returned as table |
| **conversational** | Greetings, help requests, short non-data messages | Hardcoded response |

### Schema-Aware Context

Before SQL generation, the `SchemaBuilder` fetches table metadata via MCP and injects it into the LLM prompt:

```
Available tables:
  lakehouse.paris.buildings (geometry BLOB, id VARCHAR, class VARCHAR, height DOUBLE, ...)
    → rows: 5,686
    → geometry: geometry [MULTIPOLYGON]
    → class values: public, government, school, college, retail, residential, ...
    → subtype values: civic, medical, commercial, industrial, education, ...
  lakehouse.paris.places (geometry BLOB, id VARCHAR, basic_category VARCHAR, ...)
    → rows: 12,295
    → geometry: geometry [POINT]
    → basic_category values: hotel, hospital, restaurant, library, museum, ...
```

This gives the LLM: column names and types, row scale, geometry type (Point vs Polygon — determines which spatial operations make sense), and categorical value samples (for correct WHERE clauses).

### Error Recovery

When SQL execution fails, the retry loop detects the error pattern and appends a targeted fix hint:

| Error Pattern | Hint Appended |
|--------------|---------------|
| `ST_Intersects(BLOB, BLOB)` | "Wrap geometry with ST_GeomFromWKB() before spatial functions" |
| `column "type" not found` | "Check schema context for correct column names" |
| `must appear in GROUP BY` | "List all non-aggregated columns explicitly" |
| `Ambiguous reference` | "Use table aliases in JOINs" |
| `Subquery returned more than 1 row` | "Add LIMIT 1 to subquery" |
| `ST_Transform: Invalid CRS` | "Use EPSG:4326 and EPSG:3857" |

The LLM sees both the error message and the fix hint, then generates a corrected query. Up to 3 retry attempts.

### LLM Configuration

The agent supports multiple LLM backends and models:

| Backend | Models | Use Case |
|---------|--------|----------|
| **Ollama** (default) | `devstral-small-2`, `ministral-3-14b-instruct` | Local deployment, no GPU required |
| **vLLM** | Any OpenAI-compatible model | GPU deployment, higher throughput |

The agent auto-detects available models and selects based on intent (spatial queries get the primary model, analytics can use a faster/smaller model).

## Layer 3: Webmap Chat Interface (lakehouse/webmap)

The webmap embeds a chat panel that connects to the spatial agent:

1. **User types a question** in the chat panel
2. **POST to `/api/agent/chat`** with `session_id`, `message`, and `active_layers` (currently visible map layers)
3. **SSE stream** returns events: `status` (progress), `sql` (generated query), `result` (answer), `error`, `done`
4. **WebSocket listener** on `/ws/agent/{sessionId}` receives `layer_ready` events when spatial queries materialize
5. **Map auto-loads** the new layer with bbox, row count, and description

The `active_layers` parameter is key: it tells the agent which namespaces the user is looking at, so the schema builder filters to only relevant tables. This prevents the LLM from accidentally querying the wrong dataset when multiple are loaded.

## How MCP Enables External AI Clients

The MCP server is a standalone service. Beyond the built-in spatial agent, any MCP client can connect:

```bash
# Claude Code
claude mcp add spatial-lakehouse --transport http --url http://localhost:8082/mcp

# Claude Desktop (settings.json)
{
  "mcpServers": {
    "spatial-lakehouse": {
      "transport": "http",
      "url": "http://localhost:8082/mcp"
    }
  }
}
```

This gives Claude (or any MCP-compatible LLM) direct access to all 19 spatial tools — it can explore the catalog, run spatial queries, create buffer zones, and export GeoJSON without the spatial agent intermediary.

## Testing the AI Stack

The AI integration is tested at three levels:

| Level | What | How |
|-------|------|-----|
| **MCP tools** | All 19 tools execute correctly | Unit tests with in-memory DuckDB (21 tests) |
| **Agent logic** | Intent classification, tool routing, SQL generation, error correction | Unit tests with mocked MCP (482 tests, including 208 paraphrase robustness tests) |
| **End-to-end** | NL question → agent → MCP → DuckDB → result | Integration eval harness against live Docker (52 canonical questions, 87% execution accuracy) |

The test corpus follows the NL2GeoSQL evaluation methodology with 52 canonical questions across three difficulty tiers, plus 208 controlled paraphrases for robustness testing.
