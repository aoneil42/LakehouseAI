# Terminus Core

The core Terminus GIS platform — a containerized geospatial data lakehouse with three API interfaces (Esri GeoServices, OGC API Features, GeoParquet), a deck.gl webmap, and Apache Iceberg table storage. Supports local dev, AWS, and fully disconnected (air-gapped) deployments.

## Architecture

```
                         ┌──────────────────────────────────────┐
                         │           nginx (port 80)            │
                         │    reverse proxy + URL rewriting     │
                         └──┬───────┬───────┬───────┬──────────┘
                            │       │       │       │
                   /ogc/    │  /api/ │ /esri/ │  /   │
                            │       │       │       │
              ┌─────────────▼┐  ┌───▼───┐  ┌▼──────▼──────────┐
              │   pygeoapi   │  │FastAPI│  │  Esri GeoServices │
              │  OGC API     │  │DuckDB │  │  FeatureServer    │
              │  Features    │  │       │  │  PBF + JSON       │
              │  port 5000   │  │  8000 │  │  port 8001        │
              └──────┬───────┘  └───┬───┘  └────────┬──────────┘
                     │              │               │
                     └──────────────┼───────────────┘
                                    │
                          ┌─────────▼──────────┐
                          │    LakeKeeper      │
                          │  Iceberg REST      │──── PostgreSQL
                          │  Catalog (8181)    │     (metadata)
                          └─────────┬──────────┘
                                    │ S3 API
                          ┌─────────▼──────────┐
                          │      Garage        │
                          │  S3-compatible     │
                          │  object storage    │
                          └────────────────────┘
                          (swap to real S3 later)

  ┌────────────────────┐    ┌────────────────────┐
  │  DuckDB container  │    │  SedonaSpark       │
  │  interactive SQL   │    │  (--profile heavy) │
  │  + spatial + ice   │    │  ETL, batch jobs   │
  └────────────────────┘    └────────────────────┘

  ┌────────────────────────────────────────────────┐
  │              Webmap (deck.gl v9)               │
  │  MapLibre v5 + GeoArrow + full GIS toolset    │
  │  identify, measure, symbology, time slider    │
  └────────────────────────────────────────────────┘
```

### Webmap Rendering Pipeline

The webmap uses a **zero-copy GeoArrow pipeline** — no GeoJSON conversion in the rendering path:

1. **Fetch**: The API serves features as GeoParquet. `@geoarrow/geoparquet-wasm` decodes them in-browser into **Apache Arrow tables** via WebAssembly.
2. **Detect**: Geometry type (Point, LineString, Polygon) is auto-detected from GeoArrow extension metadata on the Arrow table's geometry column.
3. **Render**: `@geoarrow/deck.gl-layers` renders directly from Arrow columnar memory to the GPU:
   - `GeoArrowScatterplotLayer` — points
   - `GeoArrowPathLayer` — lines
   - `GeoArrowSolidPolygonLayer` — polygons

Data stays in Arrow columnar format from network fetch through to GPU upload, avoiding the serialization overhead of GeoJSON.

### Webmap Features

The webmap is a full-featured GIS viewer with interactive tools:

| Feature | Description |
|---------|-------------|
| **Catalog browser** | Hierarchical namespace/table tree with search, layer toggles, and zoom-to-extent |
| **Active layers** | Drag-to-reorder layer list with per-layer opacity, visibility, symbology, and attribute table access |
| **Identify / box select** | Click features for popup details, or drag a box to select multiple features |
| **Attribute table** | Spreadsheet-style data table with column sorting, timestamp formatting, and feature count |
| **Symbology** | Customize fill color, stroke color, stroke width, point radius, and opacity per layer |
| **Measure** | Interactive distance (polyline) and area (polygon) measurement tools |
| **Time slider** | Filter temporal datasets with play/pause/step controls, adjustable window size (hourly → yearly), and playback speed. All timestamps treated as UTC |
| **Screenshot** | Export the current map view as a PNG image |
| **Data export** | Export layer data as GeoJSON or CSV |
| **Search** | Geocode addresses via Nominatim, or jump to coordinates (lat/lon or lon/lat) |
| **Basemap picker** | Switch between configured basemap styles |
| **URL state** | Shareable permalinks preserving zoom, center, and active layers |
| **Reset map** | One-click removal of all layers and return to initial view |

### Services

| Service | Port | Description |
|---------|------|-------------|
| **nginx** (webmap) | 80 | Reverse proxy, webmap UI, URL rewriting |
| **FastAPI + DuckDB** | /api/ | Feature queries, upload, namespace management |
| **pygeoapi** | /ogc/ | OGC API Features (auto-discovers Iceberg tables) |
| **Esri GeoServices** | /esri/ | ArcGIS-compatible FeatureServer (PBF + JSON) |
| **LakeKeeper** | 8181 | Iceberg REST Catalog + UI |
| **Garage** | 3900 | S3-compatible object storage |
| **PostgreSQL** | 5432 | LakeKeeper catalog metadata |
| **DuckDB** | - | Interactive SQL (exec into container) |
| **MCP Server** | 8082 | AI agent access via Model Context Protocol |
| **Spatial Agent** | 8090 | NL spatial queries (optional, `--profile agent`) |
| **Ollama** | 11434 | Local LLM inference (optional, `--profile agent`) |
| **TileServer** | 8070 | Bundled basemap tiles (optional, `--profile disconnected`) |
| **SedonaSpark** | 8888 | JupyterLab (optional, `--profile heavy`) |

## Prerequisites

- Docker Desktop (with 8GB+ RAM allocated; 16GB+ if using Sedona)
- Git
- `curl` (for bootstrap)

## Quick Start

```bash
# Clone
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

The bootstrap script:
1. Generates cryptographic secrets for Garage (RPC, admin, metrics tokens)
2. Assigns Garage cluster layout (single-node, 10GB)
3. Creates the `lakehouse` S3 bucket and access key
4. Patches all config files with the generated credentials
5. Writes `.env` with `GARAGE_KEY_ID`, `GARAGE_SECRET_KEY`, `GARAGE_ADMIN_TOKEN`
6. Bootstraps LakeKeeper (accepts ToS, creates the `lakehouse` warehouse)

## Setting Up `.env`

The `.env` file is **auto-generated by `bootstrap.sh`** and excluded from git. It contains:

```bash
# Generated by bootstrap.sh
GARAGE_KEY_ID=GKxxxxxxxxxxxxxxxxxxxxxxxx      # Garage S3 access key ID
GARAGE_SECRET_KEY=xxxxxxxxxxxxxxxxxxxxxxxx...  # Garage S3 secret (64-char hex)
GARAGE_ADMIN_TOKEN=xxxxxxxxxxxxxxxxxxxxxxxx... # Garage admin API token (64-char hex)

# pygeoapi URL rewriting (nginx handles this automatically)
# "auto" works for local, LAN, and EC2 deployments
PYGEOAPI_SERVER_URL=auto
```

**If you need to recreate `.env` manually** (e.g., cloning onto a new machine with existing Garage data):

```bash
# Get your Garage key ID and secret from the admin API
curl -s -H "Authorization: Bearer $ADMIN_TOKEN" http://localhost:3903/v1/key | jq

# Create .env
cat > .env << 'EOF'
GARAGE_KEY_ID=<your-key-id>
GARAGE_SECRET_KEY=<your-secret-key>
GARAGE_ADMIN_TOKEN=<your-admin-token>
PYGEOAPI_SERVER_URL=auto
EOF
```

**For EC2 deployment**: No changes needed. `PYGEOAPI_SERVER_URL=auto` combined with nginx's `sub_filter` dynamically rewrites all OGC API links to match the request hostname.

## Uploading Data

### Web UI

Navigate to **http://localhost/api/upload** for a drag-and-drop upload form. Accepts GeoJSON and GeoParquet files. The form includes combobox dropdowns for existing namespaces and tables to prevent typos and simplify appending to existing datasets. Upload includes a two-phase flow: preview with validation, then confirm to commit.

### API

```bash
# Upload a GeoJSON file to namespace "mydata", table "cities"
curl -X POST "http://localhost/api/upload?namespace=mydata&table_name=cities" \
  -F "files=@cities.geojson"

# Append to existing table
curl -X POST "http://localhost/api/upload?namespace=mydata&table_name=cities&append=true" \
  -F "files=@more_cities.geojson"
```

### SedonaSpark (batch)

```bash
docker compose --profile heavy up sedona -d
# Open JupyterLab at http://localhost:8888
```

```python
from sedona.spark import SedonaContext
sedona = SedonaContext.builder().getOrCreate()

sedona.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.mydata")
sedona.sql("""
  CREATE TABLE lakehouse.mydata.buildings (
    id STRING, name STRING, geometry BINARY
  ) USING iceberg
""")
```

## API Endpoints

### Feature API (FastAPI + DuckDB)

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/namespaces` | List Iceberg namespaces |
| GET | `/api/tables/{namespace}` | List tables in a namespace |
| GET | `/api/features/{namespace}/{layer}` | Query features (GeoJSON) |
| GET | `/api/bbox/{namespace}` | Bounding box for a namespace |
| GET | `/api/bbox/{namespace}/{table}` | Bounding box for a table |
| GET | `/api/schema/{namespace}/{table}` | Table schema with temporal column metadata |
| GET | `/api/geoparquet/{namespace}/{layer}` | Features as GeoParquet (bbox/limit/simplify/time filter) |
| GET | `/api/upload` | Upload form (combobox namespace/table selection) |
| POST | `/api/upload` | Upload GeoJSON/GeoParquet (two-phase: preview → confirm) |
| GET | `/api/docs` | Swagger UI |
| GET | `/api/health` | Health check |
| WS | `/ws/agent/{session_id}` | WebSocket for agent layer_ready push events |
| POST | `/api/agent/notify/{session_id}` | Agent calls this after materializing a result |

### OGC API Features (pygeoapi)

| Path | Description |
|------|-------------|
| `/ogc/` | Landing page |
| `/ogc/collections` | List all collections (auto-discovered from catalog) |
| `/ogc/collections/{id}/items` | Query features |
| `/ogc/conformance` | Supported conformance classes |
| `/ogc/openapi` | OpenAPI spec |

Collections are **auto-discovered** from LakeKeeper every 30 seconds. Adding or removing Iceberg tables automatically updates the OGC API without restarts.

### Esri GeoServices REST

| Path | Description |
|------|-------------|
| `/esri/rest/services` | Service catalog |
| `/esri/rest/services/{namespace}/FeatureServer` | FeatureServer metadata |
| `/esri/rest/services/{namespace}/FeatureServer/{id}` | Layer metadata |
| `/esri/rest/services/{namespace}/FeatureServer/{id}/query` | Feature query (PBF/JSON) |

Compatible with ArcGIS Pro, ArcGIS Online, and any Esri REST client. Supports PBF (Protocol Buffers) for high-performance feature transfer.

## DuckDB Interactive Queries

```bash
docker compose exec duckdb /duckdb -init /config/init.sql
```

```sql
-- List all Iceberg tables
SHOW ALL TABLES;

-- Spatial query
SELECT id, name, ST_AsText(ST_GeomFromWKB(geometry))
FROM lakehouse.colorado.points
WHERE ST_Within(
  ST_GeomFromWKB(geometry),
  ST_GeomFromText('POLYGON((-105.5 39.5, -104.5 39.5, -104.5 40.5, -105.5 40.5, -105.5 39.5))')
);
```

## MCP Server (AI Agent Access)

The stack includes an MCP (Model Context Protocol) server that lets LLM agents
query the lakehouse using natural language. The server runs DuckDB with spatial
and Iceberg extensions, connected to the same catalog and storage as all other
services.

### Connecting from Claude Desktop

Add to your Claude Desktop MCP config (`~/Library/Application Support/Claude/claude_desktop_config.json` on macOS):

```json
{
  "mcpServers": {
    "terminus-mcp": {
      "type": "streamable-http",
      "url": "http://localhost:8082/mcp"
    }
  }
}
```

### Connecting from Cursor

Go to Settings > MCP > Add new global MCP server, then add:

```json
{
  "mcpServers": {
    "terminus-mcp": {
      "type": "streamable-http",
      "url": "http://localhost:8082/mcp"
    }
  }
}
```

### Connecting from Claude Code

```bash
claude mcp add terminus-mcp --transport http --url http://localhost:8082/mcp
```

### Example Queries (via AI agent)

Once connected, you can ask your AI assistant things like:

- "What namespaces are in the lakehouse?"
- "Show me the schema of the buildings table in the colorado namespace"
- "Find all features within 1km of downtown Denver"
- "How many points are in each namespace?"
- "Run a spatial join between buildings and parcels"

The agent translates these to DuckDB spatial SQL and executes them against
your Iceberg tables.

### Direct MCP Testing

```bash
# Test the MCP server is running
curl http://localhost:8082/mcp

# Use the MCP inspector for interactive testing
npx @modelcontextprotocol/inspector
# Then connect to http://localhost:8082/mcp
```

## File Structure

```
lakehouse/
├── docker-compose.yml           # All service definitions
├── bootstrap.sh                 # One-time setup (secrets, bucket, warehouse)
├── .env                         # Auto-generated credentials (git-ignored)
├── garage.toml                  # Garage S3 config
├── create-warehouse.json        # LakeKeeper warehouse definition
├── duckdb-init.sql              # DuckDB startup: extensions + catalog
├── sedona-defaults.conf         # Spark/Sedona config (Iceberg + S3)
├── api/                         # FastAPI + DuckDB feature service
│   ├── Dockerfile               # Non-root user + HEALTHCHECK
│   ├── .dockerignore
│   ├── main.py                  # Routes, upload, WebSocket, agent notify
│   └── requirements.txt
├── iceberg-geo-api/             # Esri GeoServices + OGC API
│   ├── Dockerfile.geoservices
│   ├── Dockerfile.pygeoapi
│   ├── docker/
│   │   └── pygeoapi-entrypoint.sh   # Catalog watcher + auto-discovery
│   ├── config/
│   │   ├── catalog.yml              # PyIceberg catalog config
│   │   └── pygeoapi-config.yml      # OGC API base config
│   ├── src/iceberg_geo/
│   │   ├── query/                   # Shared query engine (DuckDB + Iceberg)
│   │   ├── geoservices/             # Esri FeatureServer (PBF + JSON)
│   │   └── pygeoapi_provider/       # OGC API provider plugin
│   └── tests/
├── webmap/                      # deck.gl + MapLibre frontend
│   ├── Dockerfile
│   ├── nginx.conf               # Reverse proxy + WebSocket upgrade + agent proxy
│   ├── src/
│   │   ├── main.ts              # App entry, dataset loading, agent integration
│   │   ├── map.ts               # MapLibre + deck.gl setup
│   │   ├── layers.ts            # deck.gl layer definitions (GeoArrow)
│   │   ├── queries.ts           # API queries (GeoParquet fetch)
│   │   ├── ui.ts                # Catalog tree, layer toggles, active layers
│   │   ├── attribute-table.ts   # Spreadsheet-style attribute table
│   │   ├── symbology.ts         # Layer symbology panel (fill, stroke, opacity)
│   │   ├── measure.ts           # Distance and area measurement tool
│   │   ├── time-slider.ts       # Temporal data filtering (play/pause/step)
│   │   ├── screenshot.ts        # Map screenshot export
│   │   ├── export.ts            # GeoJSON/CSV data export
│   │   ├── url-state.ts         # Permalink state (zoom, center, layers)
│   │   ├── agent-ws.ts          # WebSocket client for agent events
│   │   └── chat-panel.ts        # Chat panel UI component
│   └── public/data/             # Static Parquet files (git-ignored)
└── notebooks/                   # Jupyter notebooks
    ├── colorado_sample_data.ipynb
    └── query_cookbook.ipynb
```

## Accessing from Other Machines (LAN / ArcPro)

All services are accessible via the host machine's LAN IP on port 80:

```
http://<lan-ip>/          # Webmap
http://<lan-ip>/api/docs  # Swagger UI
http://<lan-ip>/ogc/      # OGC API (for ArcPro, QGIS)
http://<lan-ip>/esri/     # Esri GeoServices (for ArcPro)
http://<lan-ip>/api/upload # Upload form
```

**ArcPro connections:**
- **OGC API Features**: Add OGC API server → `http://<lan-ip>/ogc/`
- **Esri FeatureServer**: Add ArcGIS Server → `http://<lan-ip>/esri/rest/services`

nginx dynamically rewrites URLs in OGC API responses to match the requesting hostname, so the same deployment works from localhost, LAN, and EC2 without config changes.

## Migration to Production S3

When ready to point at real AWS S3:

1. Update `create-warehouse.json` with your S3 bucket, region, and IAM credentials
2. Recreate the warehouse in LakeKeeper (UI or API)
3. Update `duckdb-init.sql` S3 secrets
4. Update `sedona-defaults.conf` S3 endpoint + keys
5. Update `.env` with AWS credentials
6. Remove the `garage` service from `docker-compose.yml`

Your Iceberg tables, schemas, and SQL all stay the same.

## Resource Usage

| Profile | RAM | Services |
|---------|-----|----------|
| Core (default) | ~4GB | Garage, LakeKeeper, Postgres, DuckDB, MCP, API, pygeoapi, geoservices, webmap |
| Agent (`--profile agent`) | +16GB | Adds Spatial Agent + Ollama (devstral-small-2) |
| Disconnected (`--profile disconnected`) | +1GB | Adds TileServer-GL for offline basemaps |
| Heavy (`--profile heavy`) | +8-10GB | Adds SedonaSpark + JupyterLab |

Memory limits are set per-service in `docker-compose.yml` and can be tuned.

## Known Limitations

- **Iceberg v3 native geometry**: Neither DuckDB nor SedonaSpark fully supports Iceberg v3 `GEOMETRY` columns end-to-end yet. Geometry is stored as WKB in binary columns (industry standard as of early 2026).
- **DuckDB Iceberg writes**: Write support is new (v1.3+). Use SedonaSpark or PyIceberg for heavy writes and table creation.
- **Table maintenance**: Compaction, snapshot expiry, and orphan file cleanup require SedonaSpark or PyIceberg.
- **Single-node Garage**: Replication factor is 1 (dev mode). Not for production data durability.

## Agent Integration

The webmap includes a chat panel for natural-language spatial queries. Users type
questions like "show me all parcels within 500m of the river" and the agent
translates them to spatial SQL, executes via MCP tools, and pushes results to
the map in real time.

### Requirements

- The spatial agent container (see [`spatialagent/`](../spatialagent/)) — activate with `--profile agent`

### Architecture

```
┌─────────────┐     POST /api/agent/chat      ┌───────────────┐
│   Webmap     │ ──────────────────────────────▶│  Agent (8090) │
│  Chat Panel  │                                │  spatialagent/ │
└──────┬───────┘                                └───────┬───────┘
       │ ws://.../ws/agent/{session_id}                 │
       │                                                │ MCP tools
       ▼                                                ▼
┌──────────────┐   POST /api/agent/notify/{id}  ┌──────────────┐
│   FastAPI     │◀─────────────────────────────── │  MCP Server  │
│   (8000)     │                                 │  (8082)      │
└──────────────┘                                 └──────────────┘
       │ ws push LayerReadyEvent
       ▼
   Webmap loads layer via existing REST pipeline
```

1. User types a question in the chat panel
2. Chat panel POSTs to `/api/agent/chat` with `{session_id, message, active_layers}` → nginx proxies to agent container (8090)
3. Agent uses `active_layers` to filter schema context to the relevant namespace, then generates SQL
4. Agent calls MCP `materialize_result` with scratch namespace (GEOMETRY columns auto-converted to WKB)
5. Agent POSTs to `/api/agent/notify/{session_id}` on the lakehouse API
6. Lakehouse API computes bbox, pushes `layer_ready` event via WebSocket
7. Webmap receives event, loads layer via existing GeoParquet pipeline
8. Map flies to result extent

### Running Without the Agent

When the agent container is not running, the webmap still functions normally:
- The chat toggle button appears in the sidebar
- Clicking it opens the chat panel
- The WebSocket status dot shows red ("Agent unavailable")
- Sending a message returns "Agent is not running" (nginx 502)
- The map continues to function normally

### Running With the Agent

The agent lives in [`spatialagent/`](../spatialagent/) and joins the lakehouse
Docker network via a compose overlay:

```bash
# Start the lakehouse with AI agent
docker compose --profile agent up -d

# First run: pull the LLM model into the Ollama container
./scripts/pull-models.sh
```

The `agent` profile starts both the spatial agent (port 8090) and a containerized
Ollama instance (port 11434). nginx proxies `/api/agent/` to the agent container.

To use **AWS Bedrock** instead of local Ollama:
```bash
SA_LLM_BACKEND=bedrock docker compose --profile agent up -d
```

### Session Isolation

Each browser tab generates a unique session UUID via `crypto.randomUUID()`. The
agent creates scratch tables in `lakehouse._scratch_{session_id[:8]}`. When the
WebSocket disconnects (tab close or panel close), the API drops the scratch
namespace automatically via `DROP SCHEMA IF EXISTS ... CASCADE`.

### Nginx Proxy Routes (Agent)

| Location | Target | Purpose |
|----------|--------|---------|
| `/ws/` | `lakehouse-api:8000` | WebSocket upgrade for agent push events |
| `/api/agent/` | `spatial-agent:8090` | Agent chat API (502 when absent) |
