"""
pygeoapi BaseProvider implementation for Apache Iceberg tables.

Registered in pygeoapi-config.yml as:
    provider:
        name: iceberg_geo.pygeoapi_provider.iceberg.IcebergProvider

This provider translates pygeoapi's query interface into calls
to the shared Iceberg Query Service.
"""

import json
import logging
import uuid

from pygeoapi.provider.base import BaseProvider, ProviderQueryError

from iceberg_geo.query.catalog import get_connection, get_table
from iceberg_geo.query.engine import get_table_schema, query_features
from iceberg_geo.query.models import QueryParams

LOGGER = logging.getLogger(__name__)


def _geojson_expr(geom_col: str, reg_name: str, conn) -> str:
    """Build ST_AsGeoJSON expression, casting BLOB to GEOMETRY if needed."""
    col_type = conn.execute(
        f"SELECT typeof(\"{geom_col}\") FROM {reg_name} LIMIT 1"
    ).fetchone()[0].upper()
    if "GEOMETRY" in col_type:
        return f'ST_AsGeoJSON("{geom_col}")'
    return f'ST_AsGeoJSON(ST_GeomFromWKB("{geom_col}"))'


class IcebergProvider(BaseProvider):
    """pygeoapi provider for Apache Iceberg tables with geometry columns."""

    def __init__(self, provider_def):
        """
        Initialize the Iceberg provider.

        provider_def comes from pygeoapi-config.yml and should include:
            data: "namespace.table_name"
            id_field: "objectid"
            options:
                geometry_column: "geometry"
        """
        super().__init__(provider_def)

        table_id = provider_def["data"]
        parts = table_id.split(".")
        self.namespace = parts[0]
        self.table_name = parts[1]
        self.table = get_table(self.namespace, self.table_name)

        options = provider_def.get("options", {})
        self.geometry_column = options.get("geometry_column", "geometry")

        self._schema = get_table_schema(self.table)
        self._fields = {
            f["name"]: {"type": f["type"]}
            for f in self._schema.fields
            if f["name"] != self.geometry_column
        }

    def get_fields(self):
        """Return field definitions for the collection."""
        return self.fields

    def query(
        self,
        offset=0,
        limit=10,
        resulttype="results",
        bbox=None,
        datetime_=None,
        properties=None,
        sortby=None,
        select_properties=None,
        skip_geometry=False,
        q=None,
        filterq=None,
        crs_transform_spec=None,
        **kwargs,
    ):
        """
        Execute a query against the Iceberg table.

        Translates pygeoapi query params to shared QueryParams model,
        calls the query engine, and converts results to GeoJSON
        FeatureCollection.
        """
        params = QueryParams(
            bbox=tuple(bbox) if bbox else None,
            limit=limit,
            offset=offset,
            return_geometry=not skip_geometry,
            return_count_only=(resulttype == "hits"),
        )

        # Property filters -> simple WHERE clause
        if properties:
            where_parts = []
            for prop in properties:
                name, value = prop["property"], prop["value"]
                # Use quoted values safely
                safe_value = str(value).replace("'", "''")
                where_parts.append(f"{name} = '{safe_value}'")
            params.where = " AND ".join(where_parts)

        # Sort
        if sortby:
            order_parts = []
            for s in sortby:
                direction = "ASC" if s.get("order", "A") == "A" else "DESC"
                order_parts.append(f"{s['property']} {direction}")
            params.order_by = ", ".join(order_parts)

        # Field selection
        if select_properties:
            params.out_fields = ",".join(select_properties)

        # CQL2 filter (Phase 2)
        if filterq:
            params.cql2_filter = filterq

        # Execute
        result = query_features(self.table, params)

        if resulttype == "hits":
            return self._format_hits(result)

        return self._format_feature_collection(result, skip_geometry)

    def get(self, identifier, **kwargs):
        """Retrieve a single feature by ID."""
        params = QueryParams(
            where=f"{self._schema.id_field} = '{identifier}'",
            limit=1,
        )
        result = query_features(self.table, params)
        if result.count == 0:
            raise ProviderQueryError(f"Feature {identifier} not found")

        features = self._arrow_to_geojson_features(
            result.features, result.geometry_column
        )
        return features[0]

    def _format_feature_collection(self, result, skip_geometry=False):
        """Convert QueryResult to pygeoapi-expected dict."""
        features = self._arrow_to_geojson_features(
            result.features, result.geometry_column, skip_geometry
        )
        return {
            "type": "FeatureCollection",
            "features": features,
            "numberMatched": result.count,
            "numberReturned": len(features),
        }

    def _format_hits(self, result):
        """Return count-only response."""
        return {"numberMatched": result.count}

    def _arrow_to_geojson_features(
        self, arrow_table, geom_col, skip_geometry=False
    ):
        """
        Convert Arrow table to list of GeoJSON Feature dicts.

        Uses DuckDB's ST_AsGeoJSON() for geometry conversion instead of
        Shapely, avoiding the WKB → Python object → GeoJSON round-trip.
        """
        if arrow_table is None or arrow_table.num_rows == 0:
            return []

        id_field = self._schema.id_field
        col_names = arrow_table.column_names
        non_geom_cols = [c for c in col_names if c != geom_col]

        conn = get_connection()
        reg_name = f"__pygeo_{uuid.uuid4().hex[:8]}"
        conn.register(reg_name, arrow_table)
        try:
            non_geom_select = ", ".join(f'"{c}"' for c in non_geom_cols)
            if not skip_geometry and geom_col in col_names:
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
        for i, row in enumerate(rows):
            row_dict = dict(zip(col_map, row))
            geojson_str = row_dict.pop("__geojson", None)
            geometry = json.loads(geojson_str) if geojson_str else None
            properties = {k: _to_json_safe(v) for k, v in row_dict.items()}

            # Get ID from the id_field if present
            fid = properties.get(id_field, i)

            feature = {
                "type": "Feature",
                "id": str(fid),
                "geometry": geometry,
                "properties": properties,
            }
            features.append(feature)

        return features


def _to_json_safe(val):
    """Convert pyarrow scalar values to JSON-serializable Python types."""
    if val is None:
        return None
    if isinstance(val, (bytes, bytearray)):
        return None
    if hasattr(val, "as_py"):
        return val.as_py()
    return val
