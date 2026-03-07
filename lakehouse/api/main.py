"""FastAPI backend for live spatial queries against the Iceberg lakehouse."""

from __future__ import annotations

import collections
import contextlib
import json
import os
import re
import tempfile
import threading
import time
import uuid

import urllib.parse
import urllib.request

import duckdb
from fastapi import FastAPI, File, Form, Query, Response, UploadFile
from fastapi.responses import HTMLResponse, JSONResponse

app = FastAPI(docs_url="/api/docs", openapi_url="/api/openapi.json")

CATALOG_URL = os.environ.get("CATALOG_URL", "http://lakekeeper:8181/catalog")

_pool: DuckDBPool | None = None
_catalog_prefix: str | None = None

_VALID_NAME = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")
_VALID_NS_PATH = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*(\.[a-zA-Z_][a-zA-Z0-9_]*)*$")

# Schema cache: keyed by "namespace.layer.metadata_loc" → (timestamp, cols_info)
_schema_cache: dict[str, tuple[float, list]] = {}
_SCHEMA_CACHE_TTL = 60  # seconds

# Upload preview cache: preview_id → {path, columns, duckdb_types, sample, num_rows, expires}
_preview_cache: dict[str, dict] = {}
_PREVIEW_TTL = 900  # 15 minutes
_preview_lock = threading.Lock()

ALLOWED_TYPES = ("VARCHAR", "INTEGER", "BIGINT", "DOUBLE", "FLOAT", "BOOLEAN", "DATE", "TIMESTAMP")

# Map exotic DuckDB types to user-facing types
_TYPE_NORMALIZATION: dict[str, str] = {
    "TINYINT": "INTEGER", "SMALLINT": "INTEGER", "INT": "INTEGER",
    "HUGEINT": "BIGINT", "UTINYINT": "INTEGER", "USMALLINT": "INTEGER",
    "UINTEGER": "BIGINT", "UBIGINT": "BIGINT",
    "REAL": "FLOAT", "DECIMAL": "DOUBLE", "INT64": "BIGINT", "INT32": "INTEGER",
    "FLOAT64": "DOUBLE", "FLOAT32": "FLOAT",
}


def _normalize_duckdb_type(t: str) -> str:
    """Normalize a DuckDB type to the user-facing type list."""
    upper = t.upper()
    return _TYPE_NORMALIZATION.get(upper, upper)


def _reap_expired_previews():
    """Remove expired preview cache entries and their temp files."""
    now = time.monotonic()
    with _preview_lock:
        expired = [k for k, v in _preview_cache.items() if v["expires"] < now]
        for k in expired:
            path = _preview_cache.pop(k, {}).get("path")
            if path and os.path.exists(path):
                os.unlink(path)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _get_catalog_prefix() -> str:
    """Discover the warehouse prefix from LakeKeeper's /v1/config endpoint."""
    global _catalog_prefix
    if _catalog_prefix:
        return _catalog_prefix
    url = f"{CATALOG_URL}/v1/config?warehouse=lakehouse"
    req = urllib.request.Request(url, headers={"Authorization": "Bearer dummy"})
    with urllib.request.urlopen(req) as resp:
        data = json.loads(resp.read())
    _catalog_prefix = data["defaults"]["prefix"]
    return _catalog_prefix


def _encode_namespace(namespace: str) -> str:
    """Encode a dotted namespace path for the Iceberg REST catalog API.

    The Iceberg REST spec uses the Unit Separator character (0x1F) to join
    multi-part namespace identifiers in URLs.  For example,
    ``colorado.water`` becomes ``colorado%1Fwater``.
    """
    return urllib.parse.quote(namespace.replace(".", "\x1f"), safe="")


def _get_metadata_location(namespace: str, layer: str) -> str:
    """Query LakeKeeper REST API for the latest metadata location of a table."""
    prefix = _get_catalog_prefix()
    ns_encoded = _encode_namespace(namespace)
    url = f"{CATALOG_URL}/v1/{prefix}/namespaces/{ns_encoded}/tables/{layer}"
    req = urllib.request.Request(url, headers={"Authorization": "Bearer dummy"})
    with urllib.request.urlopen(req) as resp:
        data = json.loads(resp.read())
    return data["metadata-location"]


def _init_connection() -> duckdb.DuckDBPyConnection:
    conn = duckdb.connect()

    # Air-gap support: load extensions from a mounted volume if configured,
    # otherwise download from the DuckDB extension repository (default).
    ext_dir = os.environ.get("DUCKDB_EXTENSION_DIR")
    if ext_dir:
        conn.execute(f"SET extension_directory = '{ext_dir}'")
        conn.execute("SET autoinstall_known_extensions = false")
        conn.execute("SET autoload_known_extensions = false")
        for ext in ("httpfs", "iceberg", "spatial"):
            conn.execute(f"LOAD {ext}")
    else:
        conn.execute("SET home_directory = '/tmp'")
        for ext in ("httpfs", "iceberg", "spatial"):
            conn.execute(f"INSTALL {ext}; LOAD {ext};")

    key_id = os.environ["GARAGE_KEY_ID"]
    secret = os.environ["GARAGE_SECRET_KEY"]

    conn.execute(
        f"""
        CREATE SECRET garage_s3 (
            TYPE S3,
            KEY_ID '{key_id}',
            SECRET '{secret}',
            REGION 'garage',
            ENDPOINT 'garage:3900',
            URL_STYLE 'path',
            USE_SSL false
        )
        """
    )

    return conn


class DuckDBPool:
    """Thread-safe pool of DuckDB connections.

    Each connection is fully initialized (extensions, S3 secret).
    Callers acquire a connection via the context manager and release
    it automatically when the block exits.
    """

    def __init__(self, size: int = 4):
        self._sem = threading.Semaphore(size)
        self._conns: collections.deque[duckdb.DuckDBPyConnection] = (
            collections.deque()
        )
        for _ in range(size):
            self._conns.append(_init_connection())

    @contextlib.contextmanager
    def acquire(self):
        self._sem.acquire()
        conn = self._conns.popleft()
        try:
            yield conn
        finally:
            self._conns.append(conn)
            self._sem.release()


@app.on_event("startup")
def startup() -> None:
    global _pool
    size = int(os.environ.get("DUCKDB_POOL_SIZE", "4"))
    _pool = DuckDBPool(size)


# ---------------------------------------------------------------------------
# Namespace / table discovery
# ---------------------------------------------------------------------------


@app.get("/api/namespaces")
def list_namespaces() -> list[str]:
    """List available Iceberg namespaces as dotted paths."""
    prefix = _get_catalog_prefix()
    url = f"{CATALOG_URL}/v1/{prefix}/namespaces"
    req = urllib.request.Request(url, headers={"Authorization": "Bearer dummy"})
    with urllib.request.urlopen(req) as resp:
        data = json.loads(resp.read())
    return [
        ".".join(ns) if isinstance(ns, list) else ns
        for ns in data["namespaces"]
    ]


@app.get("/api/namespaces/tree")
def list_namespaces_tree() -> list[list[str]]:
    """List all namespaces as path arrays (supports nested namespaces).

    Returns e.g. ``[["colorado"], ["colorado", "water"]]``.
    """
    prefix = _get_catalog_prefix()
    url = f"{CATALOG_URL}/v1/{prefix}/namespaces"
    req = urllib.request.Request(url, headers={"Authorization": "Bearer dummy"})
    with urllib.request.urlopen(req) as resp:
        data = json.loads(resp.read())
    return [
        ns if isinstance(ns, list) else [ns]
        for ns in data["namespaces"]
    ]


@app.get("/api/tables/{namespace}")
def list_tables(namespace: str) -> list[str]:
    """List tables in a namespace (supports dotted paths like 'colorado.water')."""
    if not _VALID_NS_PATH.match(namespace):
        return JSONResponse(
            status_code=400, content={"error": "Invalid namespace name"}
        )
    prefix = _get_catalog_prefix()
    ns_encoded = _encode_namespace(namespace)
    url = f"{CATALOG_URL}/v1/{prefix}/namespaces/{ns_encoded}/tables"
    req = urllib.request.Request(url, headers={"Authorization": "Bearer dummy"})
    with urllib.request.urlopen(req) as resp:
        data = json.loads(resp.read())
    return [
        ident.get("name", ident[-1] if isinstance(ident, list) else str(ident))
        for ident in data.get("identifiers", [])
    ]


# ---------------------------------------------------------------------------
# Bounding box
# ---------------------------------------------------------------------------


def _compute_bbox(source: str) -> tuple[float, float, float, float] | None:
    """Compute bounding box for a table source using MIN/MAX.

    Note: ST_Extent() is buggy with iceberg_scan in DuckDB — it returns a
    single-point bbox instead of the full extent.  The MIN/MAX approach on
    individual geometries works correctly.
    """
    sql = (
        f"SELECT MIN(ST_XMin(g)), MIN(ST_YMin(g)), "
        f"MAX(ST_XMax(g)), MAX(ST_YMax(g)) "
        f"FROM (SELECT ST_GeomFromWKB(geometry) AS g FROM {source})"
    )
    with _pool.acquire() as conn:  # type: ignore[union-attr]
        row = conn.execute(sql).fetchone()
    if row and row[0] is not None:
        return (row[0], row[1], row[2], row[3])
    return None


@app.get("/api/bbox/{namespace}")
def get_bbox(namespace: str) -> dict:
    """Get the aggregate bounding box for all geometry in a namespace."""
    if not _VALID_NS_PATH.match(namespace):
        return JSONResponse(
            status_code=400, content={"error": "Invalid namespace name"}
        )

    tables = list_tables(namespace)
    extents: list[tuple[float, float, float, float]] = []
    for table_name in tables:
        try:
            metadata_loc = _get_metadata_location(namespace, table_name)
        except Exception:
            continue

        source = f"iceberg_scan('{metadata_loc}')"
        try:
            ext = _compute_bbox(source)
            if ext:
                extents.append(ext)
        except Exception:
            continue

    if not extents:
        return JSONResponse(
            status_code=404,
            content={"error": f"No data found for namespace {namespace}"},
        )

    bbox = [
        min(e[0] for e in extents),
        min(e[1] for e in extents),
        max(e[2] for e in extents),
        max(e[3] for e in extents),
    ]
    return {"bbox": bbox}


@app.get("/api/bbox/{namespace}/{table_name}")
def get_table_bbox(namespace: str, table_name: str) -> dict:
    """Get the bounding box for a single table."""
    if not _VALID_NS_PATH.match(namespace):
        return JSONResponse(
            status_code=400, content={"error": "Invalid namespace name"}
        )
    if not _VALID_NAME.match(table_name):
        return JSONResponse(
            status_code=400, content={"error": "Invalid table name"}
        )

    try:
        metadata_loc = _get_metadata_location(namespace, table_name)
    except Exception as e:
        return JSONResponse(
            status_code=502,
            content={"error": f"Catalog lookup failed: {e}"},
        )

    source = f"iceberg_scan('{metadata_loc}')"
    try:
        ext = _compute_bbox(source)
        if ext:
            return {"bbox": list(ext)}
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"error": f"Extent query failed: {e}"},
        )

    return JSONResponse(
        status_code=404,
        content={"error": f"No data found for {namespace}.{table_name}"},
    )


# ---------------------------------------------------------------------------
# Feature queries
# ---------------------------------------------------------------------------


@app.get("/api/features/{namespace}/{layer}")
def get_features(
    namespace: str,
    layer: str,
    bbox: str | None = Query(default=None, description="minx,miny,maxx,maxy"),
    limit: int | None = Query(default=None, ge=1),
    simplify: float | None = Query(
        default=None, ge=0, description="Simplification tolerance in degrees"
    ),
    mode: str | None = Query(
        default=None, description="Query mode: 'aggregate' for grid-binned centroids"
    ),
    resolution: float | None = Query(
        default=None, gt=0, description="Grid cell size in degrees (for mode=aggregate)"
    ),
) -> Response:
    if not _VALID_NS_PATH.match(namespace):
        return JSONResponse(
            status_code=400, content={"error": "Invalid namespace name"}
        )
    if not _VALID_NAME.match(layer):
        return JSONResponse(
            status_code=400, content={"error": "Invalid layer name"}
        )

    # Discover the latest metadata location from LakeKeeper
    try:
        metadata_loc = _get_metadata_location(namespace, layer)
    except Exception as e:
        return JSONResponse(
            status_code=502,
            content={"error": f"Catalog lookup failed: {e}"},
        )

    source = f"iceberg_scan('{metadata_loc}')"

    # Introspect columns (cached for 60s per table version).
    # Detects geometry type and flattens STRUCTs — GeoArrow deck.gl layers
    # don't handle nested structs well.
    cache_key = f"{namespace}.{layer}.{metadata_loc}"
    cached = _schema_cache.get(cache_key)
    if cached and (time.monotonic() - cached[0]) < _SCHEMA_CACHE_TTL:
        cols_info = cached[1]
    else:
        with _pool.acquire() as conn:  # type: ignore[union-attr]
            cols_info = conn.execute(
                f"SELECT column_name, column_type "
                f"FROM (DESCRIBE SELECT * FROM {source} LIMIT 0)"
            ).fetchall()
        _schema_cache[cache_key] = (time.monotonic(), cols_info)

    col_map: dict[str, str] = {c[0]: c[1] for c in cols_info}
    geom_col_type = col_map.get("geometry", "BLOB").upper()

    # Build geometry expression based on actual column type.
    # Output must be DuckDB GEOMETRY type for COPY TO PARQUET to produce
    # valid GeoParquet with proper extension metadata.
    # ST_Dump flattens Multi* types (MultiPolygon → Polygon, etc.) so that
    # GeoArrow deck.gl layers render correctly (they don't handle Multi*).
    if "GEOMETRY" in geom_col_type:
        geom_from = "ST_GeomFromWKB(ST_AsWKB(geometry))"
        geom_base = "geometry"
    else:
        geom_from = "ST_GeomFromWKB(geometry)"
        geom_base = "ST_GeomFromWKB(geometry)"

    # Optionally simplify geometry (Douglas-Peucker) for low-zoom rendering
    if simplify and simplify > 0:
        geom_inner = f"ST_Simplify({geom_base}, {simplify})"
    else:
        geom_inner = geom_base

    geom_expr = f"UNNEST(ST_Dump({geom_inner})).geom AS geometry"

    # Build column list, flattening any STRUCT columns into their fields.
    # DuckDB's "col.*" expands a STRUCT into its child columns.
    # GeoArrow / deck.gl layers don't handle nested Arrow structs.
    select_parts = [geom_expr]
    for cname, ctype in cols_info:
        if cname == "geometry":
            continue
        if ctype.upper().startswith("STRUCT"):
            select_parts.append(f"{cname}.*")
        else:
            select_parts.append(cname)

    conditions: list[str] = []

    if bbox:
        parts = bbox.split(",")
        if len(parts) != 4:
            return JSONResponse(
                status_code=400,
                content={"error": "bbox must be minx,miny,maxx,maxy"},
            )
        minx, miny, maxx, maxy = (float(p) for p in parts)
        conditions.append(
            f"ST_Intersects({geom_from}, "
            f"ST_MakeEnvelope({minx}, {miny}, {maxx}, {maxy}))"
        )

    where = f" WHERE {' AND '.join(conditions)}" if conditions else ""
    limit_clause = f" LIMIT {limit}" if limit else ""

    # ---- Aggregation mode: grid-binned centroids with counts ----
    if mode == "aggregate":
        res = resolution or 0.1
        centroid = f"ST_Centroid({geom_from})"
        agg_sql = (
            f"SELECT "
            f"  ST_Point("
            f"    (FLOOR(ST_X({centroid}) / {res}) + 0.5) * {res},"
            f"    (FLOOR(ST_Y({centroid}) / {res}) + 0.5) * {res}"
            f"  ) AS geometry,"
            f"  COUNT(*) AS feature_count"
            f" FROM {source}{where}"
            f" GROUP BY"
            f"  FLOOR(ST_X({centroid}) / {res}),"
            f"  FLOOR(ST_Y({centroid}) / {res})"
            f" ORDER BY feature_count DESC"
            f"{limit_clause}"
        )
        fd, tmppath = tempfile.mkstemp(suffix=".parquet")
        os.close(fd)
        try:
            with _pool.acquire() as conn:  # type: ignore[union-attr]
                conn.execute(
                    f"COPY ({agg_sql}) TO '{tmppath}' (FORMAT PARQUET)"
                )
            with open(tmppath, "rb") as f:
                data = f.read()
        finally:
            os.unlink(tmppath)
        return Response(
            content=data,
            media_type="application/x-parquet",
            headers={"X-Aggregation-Mode": "true", "X-Resolution": str(res)},
        )

    # Total count (before LIMIT) for truncation detection
    total_count: int | None = None
    if limit:
        count_sql = f"SELECT COUNT(*) FROM {source}{where}"
        with _pool.acquire() as conn:  # type: ignore[union-attr]
            row = conn.execute(count_sql).fetchone()
        total_count = row[0] if row else 0

    # Put geometry first (matches the column order readGeoParquet expects),
    # and convert WKB binary → DuckDB GEOMETRY so COPY TO writes GeoParquet.
    sql = f"SELECT {', '.join(select_parts)} FROM {source}{where}{limit_clause}"

    # Use DuckDB's native Parquet writer (produces correct GeoParquet encoding
    # with proper GeoArrow extension metadata that readGeoParquet WASM needs).
    fd, tmppath = tempfile.mkstemp(suffix=".parquet")
    os.close(fd)
    try:
        with _pool.acquire() as conn:  # type: ignore[union-attr]
            conn.execute(
                f"COPY ({sql}) TO '{tmppath}' (FORMAT PARQUET)"
            )
        with open(tmppath, "rb") as f:
            data = f.read()
    finally:
        os.unlink(tmppath)

    headers: dict[str, str] = {}
    if total_count is not None:
        headers["X-Total-Count"] = str(total_count)
        headers["X-Truncated"] = str(limit is not None and total_count > limit).lower()

    return Response(
        content=data,
        media_type="application/x-parquet",
        headers=headers or None,
    )


# ---------------------------------------------------------------------------
# Upload — ingest GeoJSON or GeoParquet into the Iceberg lakehouse
# ---------------------------------------------------------------------------


_pyiceberg_catalog = None
_pyiceberg_lock = threading.Lock()


def _get_pyiceberg_catalog():
    """Lazy-init a PyIceberg REST catalog connection."""
    global _pyiceberg_catalog
    if _pyiceberg_catalog is not None:
        return _pyiceberg_catalog

    from pyiceberg.catalog import load_catalog

    _pyiceberg_catalog = load_catalog(
        "rest",
        **{
            "uri": CATALOG_URL,
            "warehouse": "lakehouse",
            "token": "dummy",
            "s3.access-key-id": os.environ["GARAGE_KEY_ID"],
            "s3.secret-access-key": os.environ["GARAGE_SECRET_KEY"],
            "s3.endpoint": "http://garage:3900",
            "s3.region": "garage",
            "s3.path-style-access": "true",
            "s3.remote-signing-enabled": "false",
        },
    )
    return _pyiceberg_catalog


def _detect_geom_column_geoparquet(tmp_path: str) -> tuple[str, str]:
    """
    Detect geometry column name and encoding from GeoParquet metadata.

    Returns (column_name, encoding) — encoding is typically "WKB".
    """
    import pyarrow.parquet as pq

    pf = pq.ParquetFile(tmp_path)
    raw_meta = pf.schema_arrow.metadata or {}
    geo_meta = json.loads(raw_meta.get(b"geo", b"{}"))
    geom_col = geo_meta.get("primary_column", "geometry")

    column_meta = geo_meta.get("columns", {}).get(geom_col, {})
    encoding = column_meta.get("encoding", "WKB")

    return geom_col, encoding


# ---------------------------------------------------------------------------
# Upload form — HTML UI for file uploads
# ---------------------------------------------------------------------------

_UPLOAD_FORM_HTML = """\
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Upload to Lakehouse</title>
<style>
  * { margin: 0; padding: 0; box-sizing: border-box; }
  body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
         background: #f5f5f5; display: flex; justify-content: center; padding: 40px 16px; }
  .card { background: #fff; border-radius: 10px; box-shadow: 0 2px 12px rgba(0,0,0,0.1);
          padding: 32px; max-width: 720px; width: 100%; }
  h1 { font-size: 20px; color: #333; margin-bottom: 4px; }
  .subtitle { font-size: 13px; color: #888; margin-bottom: 24px; }
  label { display: block; font-size: 13px; font-weight: 600; color: #555; margin-bottom: 4px; }
  input[type="text"], select { width: 100%; padding: 8px 10px; border: 1px solid #ccc;
    border-radius: 6px; font-size: 14px; margin-bottom: 16px; }
  input[type="text"]:focus, select:focus { outline: none; border-color: #1e90ff;
    box-shadow: 0 0 0 2px rgba(30,144,255,0.15); }
  .file-area { border: 2px dashed #ccc; border-radius: 8px; padding: 24px; text-align: center;
    margin-bottom: 16px; cursor: pointer; transition: border-color 0.2s; }
  .file-area:hover, .file-area.dragover { border-color: #1e90ff; background: #f0f8ff; }
  .file-area input { display: none; }
  .file-area p { color: #888; font-size: 14px; }
  .file-area .selected { color: #333; font-weight: 500; }
  .check-row { display: flex; align-items: center; gap: 8px; margin-bottom: 20px; }
  .check-row input { cursor: pointer; }
  .check-row label { margin: 0; font-weight: 400; cursor: pointer; }
  button { width: 100%; padding: 10px; background: #1e90ff; color: #fff; border: none;
    border-radius: 6px; font-size: 15px; font-weight: 600; cursor: pointer;
    transition: background 0.2s; }
  button:hover { background: #0b7dda; }
  button:disabled { background: #aaa; cursor: not-allowed; }
  .btn-row { display: flex; gap: 8px; }
  .btn-row button { flex: 1; }
  .btn-secondary { background: #888; }
  .btn-secondary:hover { background: #666; }
  .result { margin-top: 16px; padding: 12px; border-radius: 6px; font-size: 13px;
    white-space: pre-wrap; word-break: break-all; }
  .result.ok { background: #e8f5e9; color: #2e7d32; border: 1px solid #a5d6a7; }
  .result.err { background: #fbe9e7; color: #c62828; border: 1px solid #ef9a9a; }
  .result.warn { background: #fff3e0; color: #e65100; border: 1px solid #ffcc80; }
  .home-link { display: inline-block; margin-top: 16px; font-size: 13px; color: #1e90ff; }
  .schema-table { width: 100%; border-collapse: collapse; font-size: 13px; margin-bottom: 16px; }
  .schema-table th { text-align: left; padding: 6px 8px; background: #f0f0f0;
    border-bottom: 2px solid #ddd; font-size: 11px; color: #666; text-transform: uppercase; }
  .schema-table td { padding: 6px 8px; border-bottom: 1px solid #eee; }
  .schema-table tr:hover { background: #f8f8f8; }
  .schema-table select { padding: 3px 6px; border: 1px solid #ccc; border-radius: 4px; font-size: 13px; }
  .sample-val { color: #666; font-size: 11px; font-family: monospace; max-width: 200px;
    overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
  .geom-label { color: #999; font-style: italic; font-size: 12px; }
</style>
</head>
<body>
<div class="card">

  <!-- Phase 1: File selection -->
  <div id="phase1">
    <h1>Upload to Lakehouse</h1>
    <p class="subtitle">Ingest GeoJSON or GeoParquet into the Iceberg catalog</p>

    <label for="namespace">Namespace</label>
    <input type="text" id="namespace" placeholder="e.g. colorado" required />

    <label for="table_name">Table name</label>
    <input type="text" id="table_name" placeholder="e.g. buildings" required />

    <div class="file-area" id="drop-zone">
      <input type="file" id="files" multiple accept=".geojson,.json,.parquet,.geoparquet" />
      <p id="file-label">Drop files here or <u>browse</u><br>
         <span style="font-size:12px;color:#aaa">.geojson &middot; .parquet &middot; .geoparquet</span></p>
    </div>

    <div class="check-row">
      <input type="checkbox" id="append" />
      <label for="append">Append to existing table</label>
    </div>

    <button id="preview-btn" type="button">Preview Schema</button>
    <div id="preview-result"></div>
    <a class="home-link" href="/">&larr; Back to map</a>
  </div>

  <!-- Phase 2: Schema preview + type editing -->
  <div id="phase2" style="display:none">
    <h1>Schema Preview</h1>
    <p class="subtitle" id="preview-subtitle"></p>

    <table class="schema-table">
      <thead>
        <tr><th>Column</th><th>Inferred</th><th>Target Type</th><th>Sample Values</th></tr>
      </thead>
      <tbody id="schema-tbody"></tbody>
    </table>

    <div class="btn-row">
      <button id="back-btn" type="button" class="btn-secondary">Back</button>
      <button id="commit-btn" type="button">Commit Upload</button>
    </div>
    <div id="commit-result"></div>
    <a class="home-link" href="/">&larr; Back to map</a>
  </div>

</div>
<script>
const dropZone = document.getElementById("drop-zone");
const fileInput = document.getElementById("files");
const fileLabel = document.getElementById("file-label");

// State
let previewId = null;
let previewTypes = {};

dropZone.addEventListener("click", () => fileInput.click());
dropZone.addEventListener("dragover", e => { e.preventDefault(); dropZone.classList.add("dragover"); });
dropZone.addEventListener("dragleave", () => dropZone.classList.remove("dragover"));
dropZone.addEventListener("drop", e => {
  e.preventDefault(); dropZone.classList.remove("dragover");
  fileInput.files = e.dataTransfer.files;
  showSelected();
});
fileInput.addEventListener("change", showSelected);

function showSelected() {
  const n = fileInput.files.length;
  if (n === 0) { fileLabel.innerHTML = 'Drop files here or <u>browse</u>'; return; }
  const names = Array.from(fileInput.files).map(f => f.name).join(", ");
  fileLabel.innerHTML = `<span class="selected">${n} file${n>1?"s":""}: ${names}</span>`;
}

// Phase 1: Preview
document.getElementById("preview-btn").addEventListener("click", async () => {
  const ns = document.getElementById("namespace").value.trim();
  const tn = document.getElementById("table_name").value.trim();
  const ap = document.getElementById("append").checked;
  const fl = fileInput.files;
  const res = document.getElementById("preview-result");
  const btn = document.getElementById("preview-btn");

  if (!ns || !tn) { res.className="result err"; res.textContent="Namespace and table name required."; return; }
  if (!fl.length) { res.className="result err"; res.textContent="Select at least one file."; return; }

  const form = new FormData();
  for (const f of fl) form.append("files", f);

  const params = new URLSearchParams({ namespace: ns, table_name: tn, append: String(ap) });
  btn.disabled = true; btn.textContent = "Reading files...";
  res.className = ""; res.textContent = "";

  try {
    const resp = await fetch(`/api/upload/preview?${params}`, { method: "POST", body: form });
    const data = await resp.json();
    if (!resp.ok) {
      res.className = "result err";
      res.textContent = data.error || JSON.stringify(data);
      return;
    }

    previewId = data.preview_id;
    previewTypes = data.duckdb_types;
    buildSchemaTable(data.columns, data.duckdb_types, data.sample, data.editable_types);
    document.getElementById("preview-subtitle").textContent =
      `${data.num_rows.toLocaleString()} rows in ${ns}.${tn}`;

    document.getElementById("phase1").style.display = "none";
    document.getElementById("phase2").style.display = "block";
    document.getElementById("commit-result").className = "";
    document.getElementById("commit-result").textContent = "";
  } catch (e) {
    res.className = "result err";
    res.textContent = "Request failed: " + e.message;
  } finally {
    btn.disabled = false; btn.textContent = "Preview Schema";
  }
});

function buildSchemaTable(columns, types, sample, editableTypes) {
  const tbody = document.getElementById("schema-tbody");
  tbody.innerHTML = "";

  columns.forEach(col => {
    const tr = document.createElement("tr");

    // Column name
    const tdName = document.createElement("td");
    tdName.textContent = col;
    tdName.style.fontWeight = "600";
    tr.appendChild(tdName);

    // Inferred type
    const tdInferred = document.createElement("td");
    tdInferred.textContent = types[col] || "UNKNOWN";
    tdInferred.style.fontFamily = "monospace";
    tdInferred.style.fontSize = "12px";
    tr.appendChild(tdInferred);

    // Target type
    const tdTarget = document.createElement("td");
    if (col === "geometry") {
      tdTarget.innerHTML = '<span class="geom-label">binary (locked)</span>';
    } else {
      const sel = document.createElement("select");
      sel.dataset.col = col;
      sel.className = "type-select";
      const inferred = types[col] || "VARCHAR";
      // Put inferred type first if it's in the list
      const ordered = editableTypes.includes(inferred)
        ? [inferred, ...editableTypes.filter(t => t !== inferred)]
        : editableTypes;
      ordered.forEach(t => {
        const opt = document.createElement("option");
        opt.value = t; opt.textContent = t;
        if (t === inferred) opt.selected = true;
        sel.appendChild(opt);
      });
      tdTarget.appendChild(sel);
    }
    tr.appendChild(tdTarget);

    // Sample values
    const tdSample = document.createElement("td");
    tdSample.className = "sample-val";
    const vals = (sample[col] || []).slice(0, 3).map(v =>
      v === null ? "null" : String(v).substring(0, 40)
    );
    tdSample.textContent = vals.join(" | ");
    tr.appendChild(tdSample);

    tbody.appendChild(tr);
  });
}

// Phase 2: Commit
document.getElementById("commit-btn").addEventListener("click", async () => {
  const ns = document.getElementById("namespace").value.trim();
  const tn = document.getElementById("table_name").value.trim();
  const ap = document.getElementById("append").checked;
  const res = document.getElementById("commit-result");
  const btn = document.getElementById("commit-btn");

  // Collect type overrides
  const overrides = {};
  document.querySelectorAll(".type-select").forEach(sel => {
    const col = sel.dataset.col;
    if (sel.value !== previewTypes[col]) {
      overrides[col] = sel.value;
    }
  });

  btn.disabled = true; btn.textContent = "Committing...";
  res.className = ""; res.textContent = "";

  try {
    const resp = await fetch("/api/upload/commit", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        preview_id: previewId,
        namespace: ns,
        table_name: tn,
        append: ap,
        type_overrides: overrides,
      }),
    });
    const data = await resp.json();
    if (resp.ok) {
      let msg = `${data.created ? "Created" : "Appended to"} ${ns}.${tn}\\n`
        + `${data.rows.toLocaleString()} rows from ${data.files_processed} file(s)\\n`
        + `Columns: ${data.columns.join(", ")}`;
      if (data.tables) {
        msg += "\\n\\nSplit by geometry type:";
        data.tables.forEach(t => { msg += `\\n  ${t.table}: ${t.rows} ${t.geometry_type} rows`; });
      }
      res.className = "result ok";
      res.textContent = msg;
      if (data.null_warnings) {
        const warns = Object.entries(data.null_warnings)
          .map(([col, n]) => `${n} values in "${col}" could not be converted (set to NULL)`)
          .join("\\n");
        const warnDiv = document.createElement("div");
        warnDiv.className = "result warn";
        warnDiv.style.marginTop = "8px";
        warnDiv.textContent = warns;
        res.after(warnDiv);
      }
    } else {
      res.className = "result err";
      res.textContent = data.error || JSON.stringify(data);
      if (resp.status === 410) {
        // Preview expired — go back to phase 1
        setTimeout(() => {
          document.getElementById("phase1").style.display = "block";
          document.getElementById("phase2").style.display = "none";
        }, 2000);
      }
    }
  } catch (e) {
    res.className = "result err";
    res.textContent = "Request failed: " + e.message;
  } finally {
    btn.disabled = false; btn.textContent = "Commit Upload";
  }
});

// Back button
document.getElementById("back-btn").addEventListener("click", () => {
  document.getElementById("phase1").style.display = "block";
  document.getElementById("phase2").style.display = "none";
});
</script>
</body>
</html>
"""


@app.get("/api/upload", response_class=HTMLResponse, include_in_schema=False)
def upload_form():
    """Serve the upload UI form."""
    return _UPLOAD_FORM_HTML


@app.post("/api/upload")
async def upload_dataset(
    namespace: str = Query(..., description="Iceberg namespace (created if missing)"),
    table_name: str = Query(..., description="Table name within the namespace"),
    append: bool = Query(default=False, description="Append to existing table"),
    files: list[UploadFile] = File(
        ..., description="GeoJSON or GeoParquet files to upload"
    ),
) -> dict:
    """
    Upload one or more GeoJSON / GeoParquet files into an Iceberg table.

    Auto-detects file format, schema, and geometry column.
    Creates the namespace and table if they do not already exist.
    """
    # --- Validate names ---
    if not _VALID_NS_PATH.match(namespace):
        return JSONResponse(
            status_code=400, content={"error": "Invalid namespace name"}
        )
    if not _VALID_NAME.match(table_name):
        return JSONResponse(
            status_code=400, content={"error": "Invalid table name"}
        )

    if not files:
        return JSONResponse(
            status_code=400, content={"error": "No files provided"}
        )

    # --- Read all uploaded files into Arrow tables ---
    import pyarrow as pa

    arrow_tables: list[pa.Table] = []

    for upload_file in files:
        filename = (upload_file.filename or "").lower()
        if filename.endswith((".geojson", ".json")):
            fmt = "geojson"
        elif filename.endswith((".parquet", ".geoparquet")):
            fmt = "geoparquet"
        else:
            return JSONResponse(
                status_code=400,
                content={
                    "error": (
                        f"Unsupported file: {upload_file.filename}. "
                        "Upload .geojson or .parquet/.geoparquet files."
                    )
                },
            )

        suffix = ".geojson" if fmt == "geojson" else ".parquet"
        fd, tmp_path = tempfile.mkstemp(suffix=suffix)
        try:
            with os.fdopen(fd, "wb") as tmp:
                while chunk := await upload_file.read(1024 * 1024):
                    tmp.write(chunk)
            table, _types = _read_upload(tmp_path, fmt)
            arrow_tables.append(table)
        finally:
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)

    # Concatenate all tables (they must share the same schema)
    if len(arrow_tables) == 1:
        combined = arrow_tables[0]
    else:
        try:
            combined = pa.concat_tables(arrow_tables, promote_options="default")
        except Exception as e:
            return JSONResponse(
                status_code=400,
                content={
                    "error": (
                        f"Schema mismatch across uploaded files: {e}. "
                        "All files must share the same schema."
                    )
                },
            )

    return _write_to_iceberg(combined, namespace, table_name, append, len(files))


# ---------------------------------------------------------------------------
# Upload preview + commit (two-step flow with schema editing)
# ---------------------------------------------------------------------------


async def _read_uploaded_files(files: list[UploadFile]):
    """Read uploaded files into a combined Arrow table + DuckDB types.

    Returns (combined_table, duckdb_types) or a JSONResponse on error.
    """
    import pyarrow as pa

    arrow_tables: list[pa.Table] = []
    duckdb_types: dict[str, str] = {}

    for upload_file in files:
        filename = (upload_file.filename or "").lower()
        if filename.endswith((".geojson", ".json")):
            fmt = "geojson"
        elif filename.endswith((".parquet", ".geoparquet")):
            fmt = "geoparquet"
        else:
            return JSONResponse(
                status_code=400,
                content={
                    "error": (
                        f"Unsupported file: {upload_file.filename}. "
                        "Upload .geojson or .parquet/.geoparquet files."
                    )
                },
            )

        suffix = ".geojson" if fmt == "geojson" else ".parquet"
        fd, tmp_path = tempfile.mkstemp(suffix=suffix)
        try:
            with os.fdopen(fd, "wb") as tmp:
                while chunk := await upload_file.read(1024 * 1024):
                    tmp.write(chunk)
            table, types = _read_upload(tmp_path, fmt)
            arrow_tables.append(table)
            if not duckdb_types:
                duckdb_types = types
        finally:
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)

    if len(arrow_tables) == 1:
        combined = arrow_tables[0]
    else:
        import pyarrow as pa
        try:
            combined = pa.concat_tables(arrow_tables, promote_options="default")
        except Exception as e:
            return JSONResponse(
                status_code=400,
                content={"error": f"Schema mismatch across uploaded files: {e}"},
            )

    return combined, duckdb_types


@app.post("/api/upload/preview")
async def upload_preview(
    namespace: str = Query(..., description="Iceberg namespace"),
    table_name: str = Query(..., description="Table name"),
    append: bool = Query(default=False),
    files: list[UploadFile] = File(...),
) -> dict:
    """Upload files and return a schema preview with sample data for type editing."""
    import pyarrow.parquet as pq

    if not _VALID_NS_PATH.match(namespace):
        return JSONResponse(status_code=400, content={"error": "Invalid namespace name"})
    if not _VALID_NAME.match(table_name):
        return JSONResponse(status_code=400, content={"error": "Invalid table name"})
    if not files:
        return JSONResponse(status_code=400, content={"error": "No files provided"})

    _reap_expired_previews()

    result = await _read_uploaded_files(files)
    if isinstance(result, JSONResponse):
        return result
    combined, duckdb_types = result

    # Cache the full table as Parquet
    preview_id = str(uuid.uuid4())
    fd, cache_path = tempfile.mkstemp(suffix=".parquet", prefix=f"preview_{preview_id[:8]}_")
    os.close(fd)
    pq.write_table(combined, cache_path)

    # Extract sample (first 5 rows)
    sample_table = combined.slice(0, min(5, combined.num_rows))
    sample: dict[str, list] = {}
    for col_name in [f.name for f in sample_table.schema]:
        vals = sample_table.column(col_name).to_pylist()
        if col_name == "geometry":
            sample[col_name] = ["(WKB binary)"] * len(vals)
        else:
            sample[col_name] = [
                None if v is None else str(v)[:80] for v in vals
            ]

    # Normalize types for dropdown defaults
    normalized_types = {
        col: _normalize_duckdb_type(t) for col, t in duckdb_types.items()
    }

    columns = [f.name for f in combined.schema]

    with _preview_lock:
        _preview_cache[preview_id] = {
            "path": cache_path,
            "columns": columns,
            "duckdb_types": normalized_types,
            "namespace": namespace,
            "table_name": table_name,
            "append": append,
            "num_rows": combined.num_rows,
            "num_files": len(files),
            "expires": time.monotonic() + _PREVIEW_TTL,
        }

    return {
        "preview_id": preview_id,
        "columns": columns,
        "duckdb_types": normalized_types,
        "sample": sample,
        "num_rows": combined.num_rows,
        "editable_types": list(ALLOWED_TYPES),
    }


from pydantic import BaseModel


class CommitRequest(BaseModel):
    preview_id: str
    namespace: str
    table_name: str
    append: bool = False
    type_overrides: dict[str, str] = {}


@app.post("/api/upload/commit")
def upload_commit(req: CommitRequest):
    """Commit a previewed upload, optionally casting column types."""
    _reap_expired_previews()

    with _preview_lock:
        entry = _preview_cache.get(req.preview_id)

    if not entry or entry["expires"] < time.monotonic():
        return JSONResponse(
            status_code=410,
            content={"error": "Preview expired or not found. Please re-upload."},
        )

    if not _VALID_NS_PATH.match(req.namespace):
        return JSONResponse(status_code=400, content={"error": "Invalid namespace name"})
    if not _VALID_NAME.match(req.table_name):
        return JSONResponse(status_code=400, content={"error": "Invalid table name"})

    # Validate type overrides
    for col, target_type in req.type_overrides.items():
        if col == "geometry":
            return JSONResponse(status_code=400, content={"error": "Cannot change geometry column type"})
        if col not in entry["columns"]:
            return JSONResponse(status_code=400, content={"error": f"Unknown column: {col}"})
        if target_type.upper() not in ALLOWED_TYPES:
            return JSONResponse(status_code=400, content={"error": f"Unsupported type: {target_type}"})

    cache_path = entry["path"]

    # Apply type casts if any overrides provided
    if req.type_overrides:
        with _pool.acquire() as conn:  # type: ignore[union-attr]
            conn.execute("DROP TABLE IF EXISTS __preview")
            conn.execute(f"CREATE TEMP TABLE __preview AS SELECT * FROM read_parquet('{cache_path}')")

            select_parts = []
            for col in entry["columns"]:
                if col == "geometry":
                    select_parts.append('"geometry"')
                elif col in req.type_overrides:
                    target = req.type_overrides[col].upper()
                    select_parts.append(f'TRY_CAST("{col}" AS {target}) AS "{col}"')
                else:
                    select_parts.append(f'"{col}"')

            combined = conn.execute(
                f"SELECT {', '.join(select_parts)} FROM __preview"
            ).fetch_arrow_table()
            conn.execute("DROP TABLE IF EXISTS __preview")
    else:
        import pyarrow.parquet as pq
        combined = pq.read_table(cache_path)

    # Clean up cache entry
    with _preview_lock:
        removed = _preview_cache.pop(req.preview_id, None)
    if removed and os.path.exists(removed["path"]):
        os.unlink(removed["path"])

    # Check for null warnings from TRY_CAST
    null_warnings: dict[str, int] = {}
    if req.type_overrides:
        for col in req.type_overrides:
            null_count = combined.column(col).null_count
            if null_count > 0:
                null_warnings[col] = null_count

    result = _write_to_iceberg(
        combined, req.namespace, req.table_name, req.append, entry.get("num_files", 1)
    )

    if isinstance(result, JSONResponse):
        return result

    if null_warnings:
        result["null_warnings"] = null_warnings

    return result


# ---------------------------------------------------------------------------
# Shared Iceberg write logic
# ---------------------------------------------------------------------------

_FAMILY_SUFFIX = {"point": "_points", "line": "_lines", "polygon": "_polygons", "other": "_other"}


def _write_to_iceberg(
    combined, namespace: str, table_name: str, append: bool, num_files: int = 1
):
    """Split by geometry type and write to Iceberg. Returns response dict or JSONResponse on error."""
    splits = _split_by_geometry_type(combined)
    mixed = len(splits) > 1
    tables_written: list[dict] = []

    catalog = _get_pyiceberg_catalog()

    try:
        catalog.create_namespace(namespace)
    except Exception:
        pass

    for family, split_table in splits.items():
        suffix = _FAMILY_SUFFIX.get(family, f"_{family}") if mixed else ""
        split_name = f"{table_name}{suffix}"

        if not _VALID_NAME.match(split_name):
            continue

        table_id = f"{namespace}.{split_name}"
        created = False
        try:
            ice_table = catalog.create_table(table_id, schema=split_table.schema)
            created = True
        except Exception as e:
            msg = str(e).lower()
            if "already exists" in msg or "alreadyexists" in msg:
                if not append:
                    return JSONResponse(
                        status_code=409,
                        content={
                            "error": (
                                f"Table {table_id} already exists. "
                                "Set append=true to add data to it."
                            )
                        },
                    )
                ice_table = catalog.load_table(table_id)
            else:
                return JSONResponse(
                    status_code=500,
                    content={"error": f"Failed to create table: {e}"},
                )

        if ice_table.io.properties.get("s3.remote-signing-enabled") == "true":
            ice_table.io.properties["s3.remote-signing-enabled"] = "false"
            ice_table.io.properties.pop("s3.signer", None)
            ice_table.io.properties.pop("s3.signer.endpoint", None)
            ice_table.io.properties.pop("s3.signer.uri", None)

        ice_table.append(split_table)
        tables_written.append({
            "table": split_name,
            "geometry_type": family,
            "rows": split_table.num_rows,
            "created": created,
        })

    global _catalog_prefix
    _catalog_prefix = None

    total_rows = sum(t["rows"] for t in tables_written)

    return {
        "status": "ok",
        "namespace": namespace,
        "table": table_name,
        "created": tables_written[0]["created"] if tables_written else False,
        "rows": total_rows,
        "files_processed": num_files,
        "columns": [f.name for f in combined.schema],
        "schema": {f.name: str(f.type) for f in combined.schema},
        **({"tables": tables_written, "mixed_geometry_types": True} if mixed else {}),
    }


def _read_upload(tmp_path: str, fmt: str):
    """
    Read an uploaded GeoJSON or GeoParquet into a normalised Arrow table.

    Returns (arrow_table, duckdb_types_dict) where:
      - arrow_table has ``geometry`` column as WKB binary (first column)
      - duckdb_types_dict maps column name → DuckDB type string
    """
    if fmt == "geojson":
        return _read_geojson(tmp_path)
    else:
        return _read_geoparquet(tmp_path)


# WKB geometry type → family mapping
_WKB_FAMILY = {
    1: "point", 2: "line", 3: "polygon",
    4: "point", 5: "line", 6: "polygon",  # Multi variants
    7: "other",  # GeometryCollection
}


def _split_by_geometry_type(table):
    """Split an Arrow table by WKB geometry type family.

    Returns a dict mapping family name → Arrow table.
    If all geometries are the same family, returns a single entry.
    """
    import pyarrow as pa

    geom_col = table.column("geometry")
    families: dict[str, list[int]] = {}

    for i in range(table.num_rows):
        wkb = geom_col[i].as_py()
        if wkb and len(wkb) >= 5:
            le = wkb[0] == 1
            geom_type = int.from_bytes(wkb[1:5], "little" if le else "big")
            family = _WKB_FAMILY.get(geom_type, "other")
        else:
            family = "other"
        families.setdefault(family, []).append(i)

    if len(families) <= 1:
        return {list(families.keys())[0] if families else "other": table}

    return {
        family: table.take(indices)
        for family, indices in families.items()
        if indices
    }


def _read_geojson(tmp_path: str):
    """Read GeoJSON via DuckDB ST_Read → (Arrow table, DuckDB types dict)."""
    with _pool.acquire() as conn:  # type: ignore[union-attr]
        conn.execute("DROP TABLE IF EXISTS __upload")
        conn.execute(
            f"CREATE TEMP TABLE __upload AS "
            f"SELECT * FROM ST_Read('{tmp_path}')"
        )
        # Capture DuckDB-inferred column types
        raw_types = conn.execute(
            "SELECT column_name, data_type FROM information_schema.columns "
            "WHERE table_name = '__upload' ORDER BY ordinal_position"
        ).fetchall()
        duckdb_types: dict[str, str] = {"geometry": "BLOB"}
        for cname, ctype in raw_types:
            if cname != "geom":
                duckdb_types[cname] = ctype

        # ST_Read produces a 'geom' column of type GEOMETRY
        arrow_table = conn.execute(
            "SELECT ST_AsWKB(geom) AS geometry, "
            "* EXCLUDE (geom) FROM __upload"
        ).fetch_arrow_table()
        conn.execute("DROP TABLE IF EXISTS __upload")
    return arrow_table, duckdb_types


def _read_geoparquet(tmp_path: str):
    """Read GeoParquet via DuckDB → (Arrow table, DuckDB types dict)."""
    geom_col, encoding = _detect_geom_column_geoparquet(tmp_path)

    with _pool.acquire() as conn:  # type: ignore[union-attr]
        conn.execute("DROP TABLE IF EXISTS __upload")
        conn.execute(
            f"CREATE TEMP TABLE __upload AS "
            f"SELECT * FROM read_parquet('{tmp_path}')"
        )

        # Build the SELECT: convert geometry to WKB, keep everything else
        cols = conn.execute(
            "SELECT column_name, data_type FROM information_schema.columns "
            "WHERE table_name = '__upload' ORDER BY ordinal_position"
        ).fetchall()
        col_names = [c[0] for c in cols]
        col_types = {c[0]: c[1] for c in cols}

        # Build DuckDB types dict for preview
        duckdb_types: dict[str, str] = {}
        if geom_col in col_names:
            dtype = col_types[geom_col].upper()

            if dtype == "GEOMETRY":
                geom_expr = f"ST_AsWKB({geom_col}) AS geometry"
            elif encoding.upper() == "WKT" or dtype == "VARCHAR":
                geom_expr = (
                    f"ST_AsWKB(ST_GeomFromText({geom_col})) AS geometry"
                )
            else:
                geom_expr = (
                    f"ST_AsWKB(ST_GeomFromWKB({geom_col})) AS geometry"
                )

            other_cols = [c for c in col_names if c != geom_col]
            select = ", ".join([geom_expr] + other_cols)
            duckdb_types["geometry"] = "BLOB"
            for c in other_cols:
                duckdb_types[c] = col_types[c]
        else:
            select = "*"
            for c in col_names:
                duckdb_types[c] = col_types[c]

        arrow_table = conn.execute(
            f"SELECT {select} FROM __upload"
        ).fetch_arrow_table()
        conn.execute("DROP TABLE IF EXISTS __upload")

    return arrow_table, duckdb_types


# ---------------------------------------------------------------------------
# Health check
# ---------------------------------------------------------------------------


@app.get("/api/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


# ---------------------------------------------------------------------------
# Tier 3 Agent Integration (additive — no-op when agent is not connected)
# ---------------------------------------------------------------------------

from fastapi import WebSocket, WebSocketDisconnect
from pydantic import BaseModel
from typing import Dict, List
import asyncio


class LayerNotification(BaseModel):
    """Payload sent by the agent after materializing a result."""
    namespace: str
    table: str
    row_count: int
    description: str = ""


class ConnectionManager:
    """Track WebSocket connections per agent session.

    Cleanup of scratch namespaces runs asynchronously with a 30-second
    grace period so that brief reconnections (page reload, network blip)
    don't drop materialized data.
    """

    def __init__(self):
        self.active: Dict[str, List[WebSocket]] = {}
        self._cleanup_tasks: Dict[str, asyncio.Task] = {}

    async def connect(self, session_id: str, ws: WebSocket):
        await ws.accept()
        # Cancel any pending cleanup for this session (reconnection)
        task = self._cleanup_tasks.pop(session_id, None)
        if task:
            task.cancel()
        self.active.setdefault(session_id, []).append(ws)

    def disconnect(self, session_id: str, ws: WebSocket):
        conns = self.active.get(session_id, [])
        if ws in conns:
            conns.remove(ws)
        if not conns and session_id in self.active:
            del self.active[session_id]
            # Schedule async cleanup with grace period
            self._cleanup_tasks[session_id] = asyncio.create_task(
                self._delayed_cleanup(session_id)
            )

    async def send_to_session(self, session_id: str, data: dict):
        for ws in self.active.get(session_id, []):
            try:
                await ws.send_json(data)
            except Exception:
                pass  # stale connection; will be cleaned on next disconnect

    async def _delayed_cleanup(self, session_id: str):
        """Drop scratch namespace after a 30-second grace period."""
        await asyncio.sleep(30)
        short_id = session_id.replace("-", "")[:8]
        scratch_ns = f"_scratch_{short_id}"
        try:
            def _drop():
                with _pool.acquire() as conn:  # type: ignore[union-attr]
                    conn.execute(
                        f"DROP SCHEMA IF EXISTS lakehouse.{scratch_ns} CASCADE"
                    )
            await asyncio.to_thread(_drop)
        except Exception:
            pass  # scratch namespace may not exist; that's fine
        self._cleanup_tasks.pop(session_id, None)


_ws_manager = ConnectionManager()


@app.websocket("/ws/agent/{session_id}")
async def agent_websocket(websocket: WebSocket, session_id: str):
    """
    WebSocket for push-based layer notifications from the agent.

    The webmap connects here on chat panel open. The agent triggers
    layer_ready events via POST /api/agent/notify/{session_id}, which
    this endpoint relays to all connected webmap clients for that session.
    """
    await _ws_manager.connect(session_id, websocket)
    try:
        while True:
            # Keep-alive: read pings from client, ignore payload
            await websocket.receive_text()
    except WebSocketDisconnect:
        _ws_manager.disconnect(session_id, websocket)


@app.post("/api/agent/notify/{session_id}")
async def agent_notify(session_id: str, payload: LayerNotification):
    """
    Called by the agent after materialize_result completes.

    Computes the bbox of the new table (off the event loop via to_thread)
    and pushes a layer_ready event to all webmap clients for this session.
    """
    bbox = None
    try:
        qualified = f"lakehouse.{payload.namespace}.{payload.table}"

        def _compute_bbox():
            with _pool.acquire() as conn:  # type: ignore[union-attr]
                result = conn.execute(f"""
                    SELECT
                        ST_XMin(ST_Extent(geom)) as xmin,
                        ST_YMin(ST_Extent(geom)) as ymin,
                        ST_XMax(ST_Extent(geom)) as xmax,
                        ST_YMax(ST_Extent(geom)) as ymax
                    FROM iceberg_scan('{qualified}')
                """).fetchone()
            if result and result[0] is not None:
                return [result[0], result[1], result[2], result[3]]
            return None

        bbox = await asyncio.to_thread(_compute_bbox)
    except Exception:
        pass  # bbox computation failed; layer_ready still fires without bbox

    event = {
        "type": "layer_ready",
        "namespace": payload.namespace,
        "table": payload.table,
        "row_count": payload.row_count,
        "bbox": bbox,
        "description": payload.description,
    }
    await _ws_manager.send_to_session(session_id, event)
    return {"status": "notified", "session_id": session_id}
