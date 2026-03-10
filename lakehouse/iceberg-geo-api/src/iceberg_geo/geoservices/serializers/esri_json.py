"""
Serialize QueryResult -> Esri JSON response.

Esri JSON is the native JSON format for ArcGIS Feature Services.
It differs from GeoJSON in geometry representation:
- Polygons use {"rings": [[[x,y],...], ...]}
- Polylines use {"paths": [[[x,y],...], ...]}
- Points use {"x": val, "y": val}
- SpatialReference is an object: {"wkid": 4326}

Uses DuckDB's ST_AsGeoJSON() for geometry conversion, then transforms
the GeoJSON geometry dict to Esri format — avoids Shapely round-trip.
"""

import json
import uuid

from iceberg_geo.query.catalog import get_connection
from iceberg_geo.query.geometry import ESRI_GEOMETRY_TYPE_MAP
from iceberg_geo.query.models import FeatureSchema, QueryResult


def _geojson_expr(geom_col: str, reg_name: str, conn) -> str:
    """Build ST_AsGeoJSON expression, casting BLOB to GEOMETRY if needed."""
    col_type = conn.execute(
        f"SELECT typeof(\"{geom_col}\") FROM {reg_name} LIMIT 1"
    ).fetchone()[0].upper()
    if "GEOMETRY" in col_type:
        return f'ST_AsGeoJSON("{geom_col}")'
    return f'ST_AsGeoJSON(ST_GeomFromWKB("{geom_col}"))'


def serialize(result: QueryResult, schema: FeatureSchema) -> dict:
    """Convert QueryResult to Esri JSON FeatureSet response."""

    if result.features is None:
        return {"count": result.count}

    # IDs-only response
    if "__oid" in result.features.column_names and len(result.features.column_names) == 1:
        oids = result.features.column("__oid").to_pylist()
        return {
            "objectIdFieldName": "__oid",
            "objectIds": oids,
        }

    esri_geom_type = ESRI_GEOMETRY_TYPE_MAP.get(
        schema.geometry_type, "esriGeometryPolygon"
    )

    fields = [
        {"name": "__oid", "type": "esriFieldTypeOID", "alias": "OID"},
    ] + _build_field_definitions(schema)

    geom_col = result.geometry_column
    col_names = result.features.column_names
    non_geom_cols = [c for c in col_names if c != geom_col]
    has_geometry = geom_col in col_names

    # Use DuckDB ST_AsGeoJSON() instead of Shapely wkb.loads()
    conn = get_connection()
    reg_name = f"__esri_{uuid.uuid4().hex[:8]}"
    conn.register(reg_name, result.features)
    try:
        non_geom_select = ", ".join(f'"{c}"' for c in non_geom_cols)
        if has_geometry:
            geojson_sql = _geojson_expr(geom_col, reg_name, conn)
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
        else:
            sql = f'SELECT {non_geom_select} FROM {reg_name}'
        rows = conn.execute(sql).fetchall()
        desc = conn.execute(sql).description
    finally:
        conn.unregister(reg_name)

    col_map = [d[0] for d in desc]

    features = []
    for row in rows:
        row_dict = dict(zip(col_map, row))
        geojson_str = row_dict.pop("__geojson", None)
        geometry = None
        if geojson_str:
            gj = json.loads(geojson_str)
            geometry = _geojson_to_esri(gj)

        attributes = {k: _to_esri_value(v) for k, v in row_dict.items()}
        features.append(
            {
                "attributes": attributes,
                "geometry": geometry,
            }
        )

    return {
        "objectIdFieldName": "__oid",
        "geometryType": esri_geom_type,
        "spatialReference": {"wkid": schema.srid},
        "fields": fields,
        "features": features,
        "exceededTransferLimit": result.exceeded_transfer_limit,
    }


def _geojson_to_esri(gj: dict) -> dict:
    """Convert a GeoJSON geometry dict to Esri JSON geometry."""
    t = gj.get("type")
    c = gj.get("coordinates")
    if t == "Point":
        return {"x": c[0], "y": c[1]}
    elif t == "MultiPoint":
        return {"points": c}
    elif t == "LineString":
        return {"paths": [c]}
    elif t == "MultiLineString":
        return {"paths": c}
    elif t == "Polygon":
        return {"rings": c}
    elif t == "MultiPolygon":
        # Flatten: each polygon's rings become top-level rings
        return {"rings": [ring for polygon in c for ring in polygon]}
    return None


def _build_field_definitions(schema: FeatureSchema) -> list[dict]:
    """Build Esri field definition array from schema."""
    type_map = {
        "string": "esriFieldTypeString",
        "int32": "esriFieldTypeInteger",
        "int64": "esriFieldTypeInteger",
        "float": "esriFieldTypeSingle",
        "double": "esriFieldTypeDouble",
        "boolean": "esriFieldTypeSmallInteger",
        "date": "esriFieldTypeDate",
        "timestamp": "esriFieldTypeDate",
    }
    fields = []
    for f in schema.fields:
        esri_type = type_map.get(f["type"], "esriFieldTypeString")
        fields.append(
            {
                "name": f["name"],
                "type": esri_type,
                "alias": f.get("alias", f["name"]),
            }
        )
    return fields


def _to_esri_value(val):
    """Convert Python value to Esri JSON safe value."""
    if val is None:
        return None
    if isinstance(val, (bytes, bytearray)):
        return None
    if hasattr(val, "as_py"):
        return val.as_py()
    return val
