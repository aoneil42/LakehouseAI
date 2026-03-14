"""
Serialize QueryResult -> GeoJSON FeatureCollection.

Used when f=geojson is requested from the GeoServices endpoint.

Uses DuckDB's ST_AsGeoJSON() for geometry conversion instead of
Shapely, avoiding the WKB → Python object → GeoJSON round-trip.
"""

import json
import uuid

from iceberg_geo.query.catalog import get_connection
from iceberg_geo.query.models import QueryResult


def _geojson_expr(geom_col: str, reg_name: str, conn) -> str:
    """Build ST_AsGeoJSON expression, casting BLOB to GEOMETRY if needed."""
    col_type = conn.execute(
        f"SELECT typeof(\"{geom_col}\") FROM {reg_name} LIMIT 1"
    ).fetchone()[0].upper()
    if "GEOMETRY" in col_type:
        return f'ST_AsGeoJSON("{geom_col}")'
    return f'ST_AsGeoJSON(ST_GeomFromWKB("{geom_col}"))'


def serialize(result: QueryResult) -> dict:
    """Convert QueryResult to GeoJSON FeatureCollection."""

    if result.features is None or result.features.num_rows == 0:
        return {
            "type": "FeatureCollection",
            "features": [],
        }

    geom_col = result.geometry_column
    col_names = result.features.column_names
    non_geom_cols = [c for c in col_names if c != geom_col]

    # Register the Arrow table in DuckDB and use ST_AsGeoJSON() in C++
    conn = get_connection()
    reg_name = f"__geojson_{uuid.uuid4().hex[:8]}"
    conn.register(reg_name, result.features)
    try:
        geojson_sql = _geojson_expr(geom_col, reg_name, conn)
        non_geom_select = ", ".join(f'"{c}"' for c in non_geom_cols)
        if non_geom_select:
            sql = (
                f'SELECT {non_geom_select}, '
                f'{geojson_sql} AS __geojson '
                f'FROM {reg_name}'
            )
        else:
            sql = (
                f'SELECT {geojson_sql} AS __geojson '
                f'FROM {reg_name}'
            )
        rows = conn.execute(sql).fetchall()
        desc = conn.execute(sql).description
    finally:
        conn.unregister(reg_name)

    # Map column names from description
    col_map = [d[0] for d in desc]

    features = []
    for row in rows:
        row_dict = dict(zip(col_map, row))
        geojson_str = row_dict.pop("__geojson", None)
        geometry = json.loads(geojson_str) if geojson_str else None
        properties = {k: _to_json_safe(v) for k, v in row_dict.items()}

        features.append(
            {
                "type": "Feature",
                "geometry": geometry,
                "properties": properties,
            }
        )

    return {
        "type": "FeatureCollection",
        "features": features,
    }


def _to_json_safe(val):
    """Convert values to JSON-serializable Python types."""
    if val is None:
        return None
    if isinstance(val, (bytes, bytearray)):
        return None
    if hasattr(val, "as_py"):
        return val.as_py()
    return val
