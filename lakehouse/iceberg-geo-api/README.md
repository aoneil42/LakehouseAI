# Iceberg Geospatial API Server

Part of the [LakehouseAI](../../README.md) monorepo. A Python service that serves geospatial data stored in Apache Iceberg tables through two API surfaces:

1. **OGC API Features** (via pygeoapi) — standards-based geospatial API serving GeoJSON, GeoArrow/Arrow IPC, and HTML
2. **Esri GeoServices REST** (via FastAPI) — `/FeatureServer` endpoints serving Esri PBF (protobuf) and Esri JSON for ArcGIS clients

Both API surfaces share a common **Iceberg Query Service** module that handles all data access through PyIceberg + DuckDB with its spatial extension.

```
                    ┌──────────────┐
                    │   pygeoapi   │ ← OGC API Features
                    │   :5000      │   (GeoJSON, GeoArrow, HTML)
                    └──────┬───────┘
                           │
                    ┌──────▼───────┐
                    │  Iceberg     │ ← PyIceberg + DuckDB spatial
                    │  Query       │   (shared Python module)
                    │  Service     │
                    └──────▲───────┘
                           │
                    ┌──────┴───────┐
                    │   FastAPI    │ ← Esri GeoServices
                    │   :8001      │   (PBF + Esri JSON + GeoJSON)
                    │  /rest/      │
                    └──────────────┘
```

## Architecture Principles

- **One query engine.** All spatial filtering, attribute queries, pagination, and aggregation happen in DuckDB against Arrow tables produced by PyIceberg. No query logic is duplicated between API surfaces.
- **One language.** Everything is Python.
- **Binary-first serialization for large data.** PBF (Esri clients) and GeoArrow/Arrow IPC (analytical clients) are first-class output formats alongside JSON.
- **Shared module, separate processes.** The Iceberg Query Service is a Python package imported by both pygeoapi and the FastAPI app. They run as separate containers for independent scaling and failure isolation.

## Project Structure

```
iceberg-geo-api/
├── Dockerfile.pygeoapi
├── Dockerfile.geoservices
├── pyproject.toml
│
├── src/
│   └── iceberg_geo/
│       ├── query/                    # Shared Iceberg Query Service
│       │   ├── catalog.py            # PyIceberg catalog connection management
│       │   ├── engine.py             # Core DuckDB spatial query engine
│       │   ├── models.py             # Pydantic models (QueryParams, QueryResult, FeatureSchema)
│       │   └── geometry.py           # WKB/GeoJSON conversion, coordinate transforms
│       │
│       ├── pygeoapi_provider/        # pygeoapi provider plugin
│       │   └── iceberg.py            # BaseProvider implementation
│       │
│       ├── geoservices/              # FastAPI GeoServices app
│       │   ├── app.py                # FastAPI application
│       │   ├── routes/
│       │   │   └── feature_server.py # /FeatureServer routes + query handler
│       │   ├── serializers/
│       │   │   ├── esri_json.py      # Arrow → Esri JSON
│       │   │   ├── esri_pbf.py       # Arrow → Esri PBF (protobuf)
│       │   │   └── geojson.py        # Arrow → GeoJSON
│       │   ├── metadata.py           # Service/layer metadata from Iceberg schema
│       │   └── proto/
│       │       ├── FeatureCollection.proto
│       │       └── FeatureCollection_pb2.py
│       │
│       └── formatters/
│           └── geoarrow.py           # GeoArrow/Arrow IPC formatter for pygeoapi
│
├── config/
│   ├── pygeoapi-config.yml
│   └── catalog.yml
│
├── tests/
│   ├── conftest.py                   # Shared fixtures with in-memory Iceberg tables
│   ├── test_query_engine.py
│   ├── test_pygeoapi_provider.py
│   ├── test_geoservices_query.py
│   ├── test_esri_pbf_encoder.py
│   └── test_geoarrow_formatter.py
│
└── scripts/
    ├── generate_proto.sh
    ├── seed_test_data.py
    └── healthcheck.py
```

## Quick Start

### Local Development (no containers)

```bash
# Install all dependencies
pip install -e ".[pygeoapi,geoservices,dev]"

# Start a local Iceberg REST catalog
docker run -p 8181:8181 apache/iceberg-rest:1.9.0

# Seed test data
python scripts/seed_test_data.py

# Run pygeoapi (terminal 1)
ICEBERG_CATALOG_CONFIG=config/catalog.yml pygeoapi serve --server-config config/pygeoapi-config.yml

# Run GeoServices endpoint (terminal 2)
ICEBERG_CATALOG_CONFIG=config/catalog.yml uvicorn iceberg_geo.geoservices.app:app --port 8001 --reload
```

### Docker Compose (via parent stack)

Both API services are orchestrated by the parent [`lakehouse/docker-compose.yml`](../docker-compose.yml). From the `lakehouse/` directory:

```bash
docker compose up -d
```

| Service | Port | Description |
|---------|------|-------------|
| pygeoapi | /ogc/ (via nginx) | OGC API Features |
| geoservices | /esri/ (via nginx) | Esri GeoServices REST |

### Test Endpoints

```bash
# OGC API Features
curl http://localhost:5000/                                                    # Landing page
curl http://localhost:5000/collections                                         # List collections
curl "http://localhost:5000/collections/example-parcels/items?limit=5"         # Query features

# Esri GeoServices
curl http://localhost:8001/rest/info                                           # Service info
curl http://localhost:8001/rest/services/default/FeatureServer                 # Layer list
curl "http://localhost:8001/rest/services/default/FeatureServer/0/query?where=1=1&outFields=*&f=json&resultRecordCount=5"
curl "http://localhost:8001/rest/services/default/FeatureServer/0/query?where=1=1&outFields=*&f=pbf&resultRecordCount=5"
curl "http://localhost:8001/rest/services/default/FeatureServer/0/query?where=1=1&outFields=*&f=geojson&resultRecordCount=5"
```

## Running Tests

```bash
# Run all tests
pytest tests/ -v

# Run specific test suites
pytest tests/test_query_engine.py -v          # Query engine (bbox, WHERE, pagination, sanitization)
pytest tests/test_pygeoapi_provider.py -v     # pygeoapi provider (query, get, filters)
pytest tests/test_geoservices_query.py -v     # FastAPI GeoServices endpoints
pytest tests/test_esri_pbf_encoder.py -v      # PBF encoding (quantization, delta-encoding)
pytest tests/test_geoarrow_formatter.py -v    # GeoArrow IPC output
```

## Configuration

### Iceberg Catalog (`config/catalog.yml`)

Configure the PyIceberg catalog connection. Supports REST, SQL, Glue, Hive, and DynamoDB catalog types:

```yaml
# REST catalog (default for Docker Compose)
catalog:
    name: default
    type: rest
    uri: http://rest-catalog:8181

# SQLite catalog (local development)
catalog:
    name: default
    type: sql
    uri: sqlite:////data/warehouse/catalog.db
    warehouse: file:///data/warehouse

# AWS Glue catalog
catalog:
    name: default
    type: glue
    warehouse: s3://your-bucket/warehouse
```

### pygeoapi Collections (`config/pygeoapi-config.yml`)

Each Iceberg table is exposed as a pygeoapi collection with the custom provider:

```yaml
resources:
    my-dataset:
        type: collection
        title: My Dataset
        providers:
            - type: feature
              name: iceberg_geo.pygeoapi_provider.iceberg.IcebergProvider
              data: namespace.table_name
              id_field: objectid
              options:
                  geometry_column: geometry
```

## GeoServices Query Parameters

The `/FeatureServer/{layer_id}/query` endpoint supports these parameters:

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `where` | string | `1=1` | SQL WHERE clause for attribute filtering |
| `geometry` | string | — | Spatial filter (Esri JSON envelope/polygon or bbox string) |
| `geometryType` | string | `esriGeometryEnvelope` | Type of geometry parameter |
| `spatialRel` | string | `esriSpatialRelIntersects` | Spatial relationship |
| `outFields` | string | `*` | Comma-separated field names |
| `outSR` | int | — | Output spatial reference (EPSG code) |
| `returnGeometry` | bool | `true` | Include geometry in response |
| `returnCountOnly` | bool | `false` | Return only the count |
| `returnIdsOnly` | bool | `false` | Return only object IDs |
| `resultOffset` | int | `0` | Pagination offset |
| `resultRecordCount` | int | — | Max features to return |
| `orderByFields` | string | — | Sort fields (e.g., `name ASC, id DESC`) |
| `f` | string | `json` | Output format: `json`, `geojson`, or `pbf` |

## Security

WHERE clause input is sanitized with a conservative allowlist approach:

- **Forbidden keywords** rejected: `DROP`, `DELETE`, `INSERT`, `UPDATE`, `CREATE`, `ALTER`, `EXEC`, `UNION`, `TRUNCATE`, etc.
- **Forbidden patterns** rejected: `;`, `--`, `/*`, `*/`
- **Subqueries** rejected: any `SELECT` in the WHERE clause
- **ORDER BY** validated: only alphanumeric column names with `ASC`/`DESC`

## Dependencies

| Package | Purpose |
|---------|---------|
| PyIceberg | Apache Iceberg table access |
| DuckDB | SQL query engine with spatial extension |
| Shapely | Geometry operations and WKB decoding |
| pyproj | Coordinate reference system transforms |
| pygeoapi | OGC API Features server |
| FastAPI | Esri GeoServices REST API |
| protobuf | Esri PBF serialization |
| PyArrow | Arrow columnar data and IPC |

## License

Apache-2.0
