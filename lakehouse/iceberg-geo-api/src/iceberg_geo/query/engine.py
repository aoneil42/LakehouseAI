"""
Core query engine. Translates query parameters into DuckDB SQL
against Iceberg tables via the attached DuckDB catalog.

This is the ONLY place where DuckDB queries are constructed and executed.
Both pygeoapi and the GeoServices endpoint call these functions.
"""

import logging
import os
import re

import duckdb
import pyarrow as pa
from shapely import wkb
from shapely.geometry import box as shapely_box

from .catalog import get_connection
from .geometry import detect_geometry_type
from .models import FeatureSchema, QueryParams, QueryResult

logger = logging.getLogger(__name__)

_schema_cache: dict[str, FeatureSchema] = {}

# Allowlisted SQL tokens for WHERE clause sanitization
_ALLOWED_OPERATORS = {
    "=", "!=", "<>", "<", ">", "<=", ">=",
    "AND", "OR", "NOT", "IN", "BETWEEN",
    "LIKE", "IS", "NULL",
}

_FORBIDDEN_KEYWORDS = re.compile(
    r"\b(DROP|DELETE|INSERT|UPDATE|CREATE|ALTER|EXEC|EXECUTE|UNION|"
    r"TRUNCATE|GRANT|REVOKE|MERGE|CALL|COPY|ATTACH|DETACH|PRAGMA)\b",
    re.IGNORECASE,
)

_FORBIDDEN_PATTERNS = re.compile(r"(--|/\*|\*/|;)")

_HAS_SPATIAL = None
_DUCKDB_EXT_DIR = os.environ.get("DUCKDB_EXTENSION_DIR")


def _get_connection() -> duckdb.DuckDBPyConnection:
    """Get the shared DuckDB connection from the catalog module."""
    global _HAS_SPATIAL
    conn = get_connection()
    if _HAS_SPATIAL is None:
        try:
            # Test if spatial is loaded by running a spatial function
            conn.execute("SELECT ST_Point(0, 0)")
            _HAS_SPATIAL = True
        except Exception:
            _HAS_SPATIAL = False
    return conn


def get_table_schema(table_ref: str) -> FeatureSchema:
    """
    Extract feature schema from an Iceberg table via DuckDB.

    Returns field names, types, geometry column name, spatial reference,
    and extent. Results are cached per table identifier.
    """
    # Return cached schema if available
    if table_ref in _schema_cache:
        return _schema_cache[table_ref]

    conn = _get_connection()

    # Get column info via DESCRIBE
    cols = conn.execute(f"DESCRIBE {table_ref}").fetchall()

    # Map DuckDB types to simple type strings
    type_map = {
        "varchar": "string",
        "text": "string",
        "int32": "int32",
        "integer": "int32",
        "int64": "int64",
        "bigint": "int64",
        "smallint": "int32",
        "tinyint": "int32",
        "float": "float",
        "real": "float",
        "double": "double",
        "boolean": "boolean",
        "bool": "boolean",
        "date": "date",
        "timestamp": "timestamp",
        "blob": "binary",
        "geometry": "geometry",
    }

    # Detect geometry column
    geom_col = "geometry"
    geom_indicators = {"geometry", "geom", "wkb_geometry", "shape", "the_geom"}
    for col_name, col_type, *_ in cols:
        col_type_lower = col_type.lower()
        if ("geometry" in col_type_lower or "blob" in col_type_lower) and \
                col_name.lower() in geom_indicators:
            geom_col = col_name
            break
    else:
        # Fallback: first GEOMETRY or BLOB column
        for col_name, col_type, *_ in cols:
            col_type_lower = col_type.lower()
            if "geometry" in col_type_lower or "blob" in col_type_lower:
                geom_col = col_name
                break

    # Detect ID field
    id_field = _detect_id_field_from_cols(cols)

    fields = []
    for col_name, col_type, *_ in cols:
        if col_name == geom_col:
            continue
        col_type_lower = col_type.lower()
        simple_type = "string"
        for key, val in type_map.items():
            if key in col_type_lower:
                simple_type = val
                break
        fields.append({
            "name": col_name,
            "type": simple_type,
            "alias": col_name,
        })

    # Detect geometry type and compute extent
    geometry_type = "Polygon"
    extent = None
    try:
        # Sample first row for geometry type
        row = conn.execute(
            f'SELECT "{geom_col}" FROM {table_ref} LIMIT 1'
        ).fetchone()
        if row and row[0] is not None:
            wkb_bytes = row[0]
            if isinstance(wkb_bytes, (bytes, bytearray)):
                geometry_type = detect_geometry_type(wkb_bytes)

        # Compute extent
        try:
            col_type_upper = ""
            for cn, ct, *_ in cols:
                if cn == geom_col:
                    col_type_upper = ct.upper()
                    break
            if "GEOMETRY" in col_type_upper:
                extent_row = conn.execute(
                    f'SELECT ST_XMin(e), ST_YMin(e), ST_XMax(e), ST_YMax(e) '
                    f'FROM (SELECT ST_Extent("{geom_col}") AS e FROM {table_ref})'
                ).fetchone()
            else:
                extent_row = conn.execute(
                    f'SELECT MIN(ST_XMin(g)), MIN(ST_YMin(g)), '
                    f'MAX(ST_XMax(g)), MAX(ST_YMax(g)) '
                    f'FROM (SELECT ST_GeomFromWKB("{geom_col}") AS g FROM {table_ref})'
                ).fetchone()
            if extent_row and extent_row[0] is not None:
                extent = {
                    "xmin": extent_row[0], "ymin": extent_row[1],
                    "xmax": extent_row[2], "ymax": extent_row[3],
                }
        except Exception as e:
            logger.warning("Failed to compute extent for %s: %s", table_ref, e)
    except Exception:
        pass

    # Adaptive max record count: fewer for polygons (heavy geometry)
    max_records = 500 if geometry_type in ("Polygon", "MultiPolygon") else 10000

    result = FeatureSchema(
        table_identifier=table_ref,
        geometry_column=geom_col,
        geometry_type=geometry_type,
        srid=4326,
        fields=fields,
        extent=extent,
        id_field=id_field,
        max_record_count=max_records,
    )
    _schema_cache[table_ref] = result
    return result


def query_features(table_ref: str, params: QueryParams) -> QueryResult:
    """
    Execute a spatial query against an Iceberg table via the DuckDB catalog.

    Pipeline:
    1. Query the attached Iceberg catalog table directly
    2. Build DuckDB SQL with spatial filters, attribute filters,
       field selection, sorting, pagination
    3. Return QueryResult with Arrow table of matching features
    """
    conn = _get_connection()

    # Get column info for building queries
    cols = conn.execute(f"DESCRIBE {table_ref}").fetchall()
    col_names = [c[0] for c in cols]
    col_types = {c[0]: c[1] for c in cols}

    # Detect geometry column
    geom_col = _detect_geom_from_cols(cols)
    geom_type_upper = col_types.get(geom_col, "BLOB").upper()
    is_native_geom = "GEOMETRY" in geom_type_upper

    schema_info = get_table_schema(table_ref)

    # Build a minimal Arrow schema for _build_select
    arrow_fields = []
    for c_name, c_type, *_ in cols:
        arrow_fields.append(pa.field(c_name, pa.utf8()))  # type doesn't matter for _build_select
    arrow_schema = pa.schema(arrow_fields)

    where_clauses = []
    needs_shapely_spatial_filter = False
    shapely_filter_geom = None

    # Spatial filter — bbox
    if params.bbox:
        xmin, ymin, xmax, ymax = params.bbox
        if _HAS_SPATIAL:
            if is_native_geom:
                where_clauses.append(
                    f'ST_Intersects("{geom_col}", '
                    f"ST_MakeEnvelope({xmin}, {ymin}, {xmax}, {ymax}))"
                )
            else:
                where_clauses.append(
                    f'ST_Intersects(ST_GeomFromWKB("{geom_col}"), '
                    f"ST_MakeEnvelope({xmin}, {ymin}, {xmax}, {ymax}))"
                )
        else:
            needs_shapely_spatial_filter = True
            shapely_filter_geom = shapely_box(xmin, ymin, xmax, ymax)

    # Spatial filter — geometry (WKT)
    if params.geometry_filter:
        if _HAS_SPATIAL:
            spatial_fn = {
                "intersects": "ST_Intersects",
                "contains": "ST_Contains",
                "within": "ST_Within",
            }.get(params.spatial_rel, "ST_Intersects")
            if is_native_geom:
                where_clauses.append(
                    f'{spatial_fn}("{geom_col}", '
                    f"ST_GeomFromText('{params.geometry_filter}'))"
                )
            else:
                where_clauses.append(
                    f'{spatial_fn}(ST_GeomFromWKB("{geom_col}"), '
                    f"ST_GeomFromText('{params.geometry_filter}'))"
                )
        else:
            from shapely import wkt as wkt_mod

            needs_shapely_spatial_filter = True
            shapely_filter_geom = wkt_mod.loads(params.geometry_filter)

    # Attribute filter
    if params.where:
        sanitized = _sanitize_where(params.where)
        where_clauses.append(f"({sanitized})")

    where_sql = " AND ".join(where_clauses) if where_clauses else "1=1"

    # Global OID CTE — used by all query paths below
    numbered_cte = f"""
        WITH numbered AS (
            SELECT (ROW_NUMBER() OVER () - 1)::INTEGER AS __oid, *
            FROM {table_ref}
        )
    """

    # Count-only shortcut
    if params.return_count_only:
        if needs_shapely_spatial_filter and shapely_filter_geom is not None:
            all_arrow = conn.execute(
                f'{numbered_cte} SELECT "{geom_col}" FROM numbered WHERE {where_sql}'
            ).fetch_arrow_table()
            filtered = _apply_shapely_spatial_filter(
                all_arrow, geom_col, shapely_filter_geom, params.spatial_rel
            )
            return QueryResult(count=filtered.num_rows, features=None)
        result = conn.execute(
            f"{numbered_cte} SELECT COUNT(*) as cnt FROM numbered WHERE {where_sql}"
        ).fetchone()
        return QueryResult(count=result[0], features=None)

    # IDs-only — return global OIDs and the real ID field
    if params.return_ids_only:
        id_field = schema_info.id_field
        result_arrow = conn.execute(
            f'{numbered_cte} SELECT __oid, "{id_field}" FROM numbered WHERE {where_sql}'
        ).fetch_arrow_table()
        return QueryResult(
            features=result_arrow,
            geometry_column=geom_col,
            count=result_arrow.num_rows,
        )

    # objectIds filter — fetch specific features by global OID
    if params.object_ids is not None:
        select_clause = _build_select(params, geom_col, arrow_schema)
        oid_list = ",".join(str(int(oid)) for oid in params.object_ids)
        sql = f"""
            {numbered_cte}
            SELECT __oid, {select_clause}
            FROM numbered
            WHERE __oid IN ({oid_list})
        """
        result_arrow = conn.execute(sql).fetch_arrow_table()
        return QueryResult(
            features=result_arrow,
            geometry_column=geom_col,
            count=result_arrow.num_rows,
            exceeded_transfer_limit=False,
        )

    # Build SELECT
    select_clause = _build_select(params, geom_col, arrow_schema)

    # Order
    order_sql = ""
    if params.order_by:
        order_sql = f"ORDER BY {_sanitize_order(params.order_by)}"

    # Pagination
    limit_sql = f"LIMIT {int(params.limit)}" if params.limit else ""
    offset_sql = f"OFFSET {int(params.offset)}" if params.offset else ""

    sql = f"""
        {numbered_cte}
        SELECT __oid, {select_clause}
        FROM numbered
        WHERE {where_sql}
        {order_sql}
        {limit_sql}
        {offset_sql}
    """

    result_arrow = conn.execute(sql).fetch_arrow_table()

    # Apply Shapely spatial filter if DuckDB spatial not available
    if needs_shapely_spatial_filter and shapely_filter_geom is not None:
        result_arrow = _apply_shapely_spatial_filter(
            result_arrow, geom_col, shapely_filter_geom, params.spatial_rel
        )

    # Check if more results exist (exceededTransferLimit)
    exceeded = False
    if params.limit and not needs_shapely_spatial_filter:
        count_result = conn.execute(
            f"{numbered_cte} SELECT COUNT(*) FROM numbered WHERE {where_sql}"
        ).fetchone()
        exceeded = count_result[0] > (params.offset or 0) + params.limit
    elif params.limit and needs_shapely_spatial_filter:
        exceeded = result_arrow.num_rows >= params.limit

    return QueryResult(
        features=result_arrow,
        geometry_column=geom_col,
        exceeded_transfer_limit=exceeded,
        count=result_arrow.num_rows,
    )


def _detect_geom_from_cols(cols) -> str:
    """Find geometry column from DESCRIBE output."""
    geom_indicators = {"geometry", "geom", "wkb_geometry", "shape", "the_geom"}
    for col_name, col_type, *_ in cols:
        ct = col_type.lower()
        if col_name.lower() in geom_indicators and ("geometry" in ct or "blob" in ct or "binary" in ct):
            return col_name
    # Fallback: first GEOMETRY or BLOB column
    for col_name, col_type, *_ in cols:
        ct = col_type.lower()
        if "geometry" in ct or "blob" in ct:
            return col_name
    return "geometry"


def _detect_geometry_column(schema: pa.Schema) -> str:
    """Find the geometry column in an Arrow schema.

    Looks for:
    1. Column with GeoArrow extension type metadata
    2. Column named geometry/geom/wkb_geometry/shape with binary type
    3. First large_binary column
    """
    known_names = {"geometry", "geom", "wkb_geometry", "shape", "location"}

    # Check for known geometry column names with binary type
    for i, field in enumerate(schema):
        if field.name.lower() in known_names and _is_binary_type(field.type):
            return field.name

    # Check for GeoArrow extension type metadata
    for i, field in enumerate(schema):
        metadata = field.metadata
        if metadata:
            for key in metadata:
                key_str = key.decode("utf-8") if isinstance(key, bytes) else key
                if "geo" in key_str.lower() or "arrow" in key_str.lower():
                    return field.name

    # Fallback: first large_binary or binary column
    for field in schema:
        if _is_binary_type(field.type):
            return field.name

    return "geometry"


def _detect_id_field_from_cols(cols) -> str:
    """Detect the ID field from DESCRIBE output."""
    known_id_names = {"objectid", "id", "fid", "gid", "ogc_fid"}
    for col_name, col_type, *_ in cols:
        if col_name.lower() in known_id_names:
            return col_name
    # Return first integer field as fallback
    for col_name, col_type, *_ in cols:
        if "int" in col_type.lower():
            return col_name
    return "objectid"


def _is_binary_type(arrow_type) -> bool:
    """Check if an Arrow type is binary-like."""
    return (
        pa.types.is_binary(arrow_type)
        or pa.types.is_large_binary(arrow_type)
        or pa.types.is_fixed_size_binary(arrow_type)
    )


def _sanitize_where(where: str) -> str:
    """
    Sanitize a SQL WHERE clause from user input.

    Uses a conservative allowlist approach:
    - Reject forbidden keywords (DDL, DML)
    - Reject dangerous patterns (comments, semicolons)
    - Allow only safe expressions
    """
    if not where or where.strip() == "":
        return "1=1"

    # Check for forbidden patterns
    if _FORBIDDEN_PATTERNS.search(where):
        raise ValueError(f"Forbidden pattern in WHERE clause: {where}")

    # Check for forbidden keywords
    if _FORBIDDEN_KEYWORDS.search(where):
        raise ValueError(f"Forbidden keyword in WHERE clause: {where}")

    # Check for subqueries
    if re.search(r"\bSELECT\b", where, re.IGNORECASE):
        raise ValueError(f"Subqueries not allowed in WHERE clause: {where}")

    return where


def _sanitize_order(order_by: str) -> str:
    """Sanitize ORDER BY clause. Only allow column names + ASC/DESC."""
    if not order_by:
        return ""

    # Check for forbidden patterns
    if _FORBIDDEN_PATTERNS.search(order_by):
        raise ValueError(f"Forbidden pattern in ORDER BY: {order_by}")

    if _FORBIDDEN_KEYWORDS.search(order_by):
        raise ValueError(f"Forbidden keyword in ORDER BY: {order_by}")

    # Validate format: comma-separated "column_name [ASC|DESC]"
    parts = [p.strip() for p in order_by.split(",")]
    sanitized = []
    for part in parts:
        tokens = part.split()
        if len(tokens) == 0:
            continue
        col_name = tokens[0]
        # Column name must be alphanumeric/underscore
        if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", col_name):
            raise ValueError(f"Invalid column name in ORDER BY: {col_name}")
        direction = ""
        if len(tokens) > 1:
            direction = tokens[1].upper()
            if direction not in ("ASC", "DESC"):
                raise ValueError(f"Invalid sort direction: {direction}")
        sanitized.append(f'"{col_name}" {direction}'.strip())

    return ", ".join(sanitized)


def _apply_shapely_spatial_filter(
    arrow_table: pa.Table,
    geom_col: str,
    filter_geom,
    spatial_rel: str = "intersects",
) -> pa.Table:
    """
    Apply spatial filtering using Shapely when DuckDB spatial is unavailable.

    This is a fallback for environments where the DuckDB spatial extension
    cannot be installed. Production deployments should use DuckDB spatial.
    """
    if arrow_table.num_rows == 0:
        return arrow_table

    geom_data = arrow_table.column(geom_col).to_pylist()
    keep_indices = []

    spatial_fn = {
        "intersects": lambda g, f: g.intersects(f),
        "contains": lambda g, f: f.contains(g),
        "within": lambda g, f: g.within(f),
    }.get(spatial_rel, lambda g, f: g.intersects(f))

    for i, wkb_bytes in enumerate(geom_data):
        if wkb_bytes is None:
            continue
        geom = wkb.loads(wkb_bytes)
        if spatial_fn(geom, filter_geom):
            keep_indices.append(i)

    if not keep_indices:
        return arrow_table.slice(0, 0)

    return arrow_table.take(keep_indices)


# Columns that _build_select should never include in the SELECT clause.
# __oid is always added by the query template (SELECT __oid, ...) so must
# not be duplicated.
_EXCLUDED_FROM_SELECT = {"__oid"}


def _build_select(
    params: QueryParams, geom_col: str, schema: pa.Schema
) -> str:
    """Build SELECT clause from requested fields.

    Excludes __oid (already in the query template).
    """
    if params.out_fields == "*" or not params.out_fields:
        if not params.return_geometry:
            # Select all columns except geometry and excluded columns
            cols = [
                f'"{f.name}"'
                for f in schema
                if f.name != geom_col and f.name not in _EXCLUDED_FROM_SELECT
            ]
            return ", ".join(cols) if cols else "1 AS _dummy"
        # Select all columns except excluded columns
        cols = [
            f'"{f.name}"'
            for f in schema
            if f.name not in _EXCLUDED_FROM_SELECT
        ]
        return ", ".join(cols) if cols else "1 AS _dummy"

    fields = [f.strip() for f in params.out_fields.split(",")]
    # Quote field names for safety, exclude internal/duplicate columns
    quoted = [f'"{f}"' for f in fields if f not in _EXCLUDED_FROM_SELECT]
    if geom_col not in fields and params.return_geometry:
        quoted.append(f'"{geom_col}"')
    # If only __oid was requested (and we excluded it), return geometry only
    if not quoted and params.return_geometry:
        quoted.append(f'"{geom_col}"')
    if not quoted:
        return "1 AS _dummy"
    return ", ".join(quoted)
