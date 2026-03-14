"""
Serialize QueryResult -> Esri FeatureCollection PBF (Protocol Buffers).

This is the performance-critical serializer for Iceberg-scale data.
The ArcGIS JS API requests PBF by default (f=pbf).

Key differences from JSON encoding:
1. Coordinates are quantized to integers with a Transform
2. Coordinates are delta-encoded (each = current - previous)
3. Geometry rings/paths use a flat coords array + lengths array
4. Attributes use protobuf Value oneof types (not JSON strings)

Performance notes:
- Uses column-based Arrow access instead of to_pydict() to avoid
  materializing all data (especially large WKB blobs) at once
- Direct WKB parsing with struct.unpack() for coordinate extraction
  when no simplification is needed (avoids Shapely object allocation)
- Supports geometry simplification via max_allowable_offset to
  dramatically reduce coordinate counts for polygon-heavy layers
- Field type lookup is pre-computed as a dict (O(1) per access)

References:
- Spec: https://github.com/Esri/arcgis-pbf/tree/main/proto/FeatureCollection
"""

import logging
import struct
from typing import Optional

from iceberg_geo.query.models import FeatureSchema, QueryResult

logger = logging.getLogger(__name__)

try:
    from ..proto import FeatureCollection_pb2 as pb

    HAS_PROTO = True
except ImportError:
    HAS_PROTO = False
    pb = None

# Quantization resolution — controls coordinate precision
QUANTIZE_RESOLUTION = 1e8

# WKB type constants (little-endian)
_WKB_POINT = 1
_WKB_LINESTRING = 2
_WKB_POLYGON = 3
_WKB_MULTIPOINT = 4
_WKB_MULTILINESTRING = 5
_WKB_MULTIPOLYGON = 6


def serialize(
    result: QueryResult,
    schema: FeatureSchema,
    max_allowable_offset: Optional[float] = None,
) -> bytes:
    """
    Serialize a QueryResult to Esri FeatureCollection PBF bytes.

    Args:
        result: Query result with Arrow table
        schema: Feature schema with field definitions
        max_allowable_offset: Geometry simplification tolerance in map units.
            ArcPro sends this based on display resolution — higher values
            mean coarser simplification (fewer vertices).

    Returns raw protobuf bytes suitable for Response(content=...,
    media_type="application/x-protobuf").
    """
    if not HAS_PROTO:
        raise ImportError(
            "Protobuf classes not generated. Run scripts/generate_proto.sh first."
        )

    if result.features is None or result.features.num_rows == 0:
        return _serialize_empty(result, schema)

    fc = pb.FeatureCollectionPBuffer()
    query_result = fc.queryResult
    feature_result = query_result.featureResult

    # Spatial reference
    feature_result.spatialReference.wkid = schema.srid
    feature_result.spatialReference.lastestWkid = schema.srid

    # Object ID field name — critical for ArcPro
    feature_result.objectIdFieldName = "__oid"

    # Geometry type
    geom_type_map = {
        "Point": pb.FeatureCollectionPBuffer.esriGeometryTypePoint,
        "MultiPoint": pb.FeatureCollectionPBuffer.esriGeometryTypeMultipoint,
        "LineString": pb.FeatureCollectionPBuffer.esriGeometryTypePolyline,
        "MultiLineString": pb.FeatureCollectionPBuffer.esriGeometryTypePolyline,
        "Polygon": pb.FeatureCollectionPBuffer.esriGeometryTypePolygon,
        "MultiPolygon": pb.FeatureCollectionPBuffer.esriGeometryTypePolygon,
    }
    feature_result.geometryType = geom_type_map.get(
        schema.geometry_type,
        pb.FeatureCollectionPBuffer.esriGeometryTypePolygon,
    )

    # --- Column-based access (avoids to_pydict() materializing everything) ---
    geom_col = result.geometry_column
    num_rows = result.features.num_rows
    result_columns = set(result.features.column_names)
    has_geometry = geom_col in result_columns

    # Determine which attribute fields are actually in the result.
    # PBF field count MUST exactly match attribute count per feature.
    # __oid is always first, then only schema fields present in the result.
    field_type_map = {f["name"]: f["type"] for f in schema.fields}
    present_field_names = [
        f["name"] for f in schema.fields if f["name"] in result_columns
    ]

    # Build PBF field definitions: __oid first, then present fields
    _build_fields_for_result(feature_result, schema, present_field_names)

    # --- Geometry processing (skipped when returnGeometry=false) ---
    use_simplify = bool(max_allowable_offset and max_allowable_offset > 0)
    parsed_geoms = None
    x_min = 0.0
    y_min = 0.0
    x_scale = 1.0
    y_scale = 1.0

    if has_geometry:
        geom_data = result.features.column(geom_col).to_pylist()

        if use_simplify:
            # Shapely path — needed for simplification
            parsed_geoms = _parse_geometries_shapely(
                geom_data, max_allowable_offset
            )
        else:
            # Fast path — direct WKB parsing, no Shapely
            parsed_geoms = _parse_geometries_wkb(geom_data)

        # Compute bounds from parsed geometries
        g_x_min = float("inf")
        g_y_min = float("inf")
        g_x_max = float("-inf")
        g_y_max = float("-inf")
        has_any_geom = False

        for pg in parsed_geoms:
            if pg is not None:
                bounds = pg["bounds"]
                if bounds[0] < g_x_min:
                    g_x_min = bounds[0]
                if bounds[1] < g_y_min:
                    g_y_min = bounds[1]
                if bounds[2] > g_x_max:
                    g_x_max = bounds[2]
                if bounds[3] > g_y_max:
                    g_y_max = bounds[3]
                has_any_geom = True

        if not has_any_geom:
            return _serialize_empty(result, schema)

        x_min = g_x_min
        y_min = g_y_min
        x_range = g_x_max - g_x_min if g_x_max != g_x_min else 1.0
        y_range = g_y_max - g_y_min if g_y_max != g_y_min else 1.0
        x_scale = x_range / QUANTIZE_RESOLUTION
        y_scale = y_range / QUANTIZE_RESOLUTION

        transform = feature_result.transform
        transform.quantizeOriginPostion = (
            pb.FeatureCollectionPBuffer.lowerLeft
        )
        transform.scale.xScale = x_scale
        transform.scale.yScale = y_scale
        transform.translate.xTranslate = x_min
        transform.translate.yTranslate = y_min

    # Pre-fetch __oid column (always present in query results)
    try:
        oid_data = result.features.column("__oid").to_pylist()
    except KeyError:
        # Fallback: generate sequential OIDs
        oid_data = list(range(num_rows))

    # Pre-fetch attribute columns as Python lists (one to_pylist() per column)
    attr_columns = {}
    for field_name in present_field_names:
        attr_columns[field_name] = result.features.column(field_name).to_pylist()

    # Encode features
    # CRITICAL: attribute count per feature MUST equal PBF field count.
    # Fields = [__oid] + present_field_names
    for i in range(num_rows):
        feature = feature_result.features.add()

        # __oid must be the first attribute (matches first field definition)
        _set_value(
            feature.attributes.add(),
            oid_data[i],
            "esriFieldTypeOID",
        )

        # Remaining attributes (only fields present in result, in order)
        for field_name in present_field_names:
            _set_value(
                feature.attributes.add(),
                attr_columns[field_name][i],
                field_type_map.get(field_name, "string"),
            )

        # Geometry (only when returnGeometry=true)
        if parsed_geoms is not None:
            pg = parsed_geoms[i]
            if pg is not None:
                _encode_geometry_from_coord_arrays(
                    feature.geometry,
                    pg["coord_arrays"],
                    x_min, y_min, x_scale, y_scale,
                )

    feature_result.exceededTransferLimit = result.exceeded_transfer_limit

    return fc.SerializeToString()


def _parse_geometries_wkb(geom_data: list) -> list:
    """Parse WKB bytes directly with struct.unpack — no Shapely.

    Returns list of dicts with 'coord_arrays' and 'bounds', or None for
    null geometries.
    """
    results = []
    for wkb_bytes in geom_data:
        if wkb_bytes:
            try:
                coord_arrays, bounds = _parse_wkb(wkb_bytes)
                results.append({"coord_arrays": coord_arrays, "bounds": bounds})
            except (struct.error, IndexError):
                # Fallback: skip unparseable geometry
                results.append(None)
        else:
            results.append(None)
    return results


def _parse_geometries_shapely(geom_data: list, max_offset: float) -> list:
    """Parse with Shapely — used when simplification is needed.

    Returns list of dicts with 'coord_arrays' and 'bounds', or None.
    """
    from shapely import wkb

    results = []
    for wkb_bytes in geom_data:
        if wkb_bytes:
            geom = wkb.loads(wkb_bytes)
            if max_offset and max_offset > 0:
                geom = geom.simplify(max_offset, preserve_topology=True)
                if geom.is_empty:
                    results.append(None)
                    continue
            coord_arrays = _extract_coord_arrays(geom)
            bounds = geom.bounds  # (minx, miny, maxx, maxy)
            results.append({"coord_arrays": coord_arrays, "bounds": bounds})
        else:
            results.append(None)
    return results


def _parse_wkb(data: bytes):
    """Parse WKB bytes into coord_arrays and bounds.

    Returns (coord_arrays, (xmin, ymin, xmax, ymax)).
    coord_arrays is a list of rings/paths, each a list of (x, y) tuples.
    """
    if isinstance(data, memoryview):
        data = bytes(data)

    xmin = float("inf")
    ymin = float("inf")
    xmax = float("-inf")
    ymax = float("-inf")

    def update_bounds(x, y):
        nonlocal xmin, ymin, xmax, ymax
        if x < xmin:
            xmin = x
        if y < ymin:
            ymin = y
        if x > xmax:
            xmax = x
        if y > ymax:
            ymax = y

    def read_point(buf, offset):
        x, y = struct.unpack_from("<dd", buf, offset)
        update_bounds(x, y)
        return (x, y), offset + 16

    def read_ring(buf, offset):
        (n_points,) = struct.unpack_from("<I", buf, offset)
        offset += 4
        coords = []
        for _ in range(n_points):
            pt, offset = read_point(buf, offset)
            coords.append(pt)
        return coords, offset

    def read_geom(buf, offset):
        """Read a geometry at the given offset. Returns (coord_arrays, offset)."""
        # Byte order
        _bo = buf[offset]
        offset += 1
        # Type
        (wkb_type,) = struct.unpack_from("<I", buf, offset)
        offset += 4
        # Mask off SRID/Z/M flags
        base_type = wkb_type & 0xFF

        if base_type == _WKB_POINT:
            pt, offset = read_point(buf, offset)
            return [[pt]], offset

        elif base_type == _WKB_LINESTRING:
            ring, offset = read_ring(buf, offset)
            return [ring], offset

        elif base_type == _WKB_POLYGON:
            (n_rings,) = struct.unpack_from("<I", buf, offset)
            offset += 4
            rings = []
            for _ in range(n_rings):
                ring, offset = read_ring(buf, offset)
                rings.append(ring)
            return rings, offset

        elif base_type == _WKB_MULTIPOINT:
            (n_geoms,) = struct.unpack_from("<I", buf, offset)
            offset += 4
            all_arrays = []
            for _ in range(n_geoms):
                arrays, offset = read_geom(buf, offset)
                all_arrays.extend(arrays)
            return all_arrays, offset

        elif base_type == _WKB_MULTILINESTRING:
            (n_geoms,) = struct.unpack_from("<I", buf, offset)
            offset += 4
            all_arrays = []
            for _ in range(n_geoms):
                arrays, offset = read_geom(buf, offset)
                all_arrays.extend(arrays)
            return all_arrays, offset

        elif base_type == _WKB_MULTIPOLYGON:
            (n_geoms,) = struct.unpack_from("<I", buf, offset)
            offset += 4
            all_arrays = []
            for _ in range(n_geoms):
                arrays, offset = read_geom(buf, offset)
                all_arrays.extend(arrays)
            return all_arrays, offset

        else:
            raise ValueError(f"Unsupported WKB type: {wkb_type}")

    coord_arrays, _ = read_geom(data, 0)
    return coord_arrays, (xmin, ymin, xmax, ymax)


def _encode_geometry_from_coord_arrays(
    pb_geom,
    coord_arrays: list,
    x_translate: float,
    y_translate: float,
    x_scale: float,
    y_scale: float,
):
    """Encode coord_arrays into an Esri PBF Geometry message.

    coord_arrays: list of rings/paths, each a list of (x, y) tuples.
    """
    all_delta_coords = []
    lengths = []

    for ring_coords in coord_arrays:
        lengths.append(len(ring_coords))
        prev_x, prev_y = 0, 0

        for wx, wy in ring_coords:
            qx = round((wx - x_translate) / x_scale)
            qy = round((wy - y_translate) / y_scale)
            dx = qx - prev_x
            dy = qy - prev_y
            all_delta_coords.extend([dx, dy])
            prev_x, prev_y = qx, qy

    pb_geom.lengths.extend(lengths)
    pb_geom.coords.extend(all_delta_coords)


def _extract_coord_arrays(geom):
    """Extract coordinate arrays from a Shapely geometry.

    Returns list of rings/paths, where each is a list of (x, y) tuples.
    Used by the Shapely simplification path.
    """
    geom_type = geom.geom_type

    if geom_type == "Point":
        return [[(geom.x, geom.y)]]
    elif geom_type == "MultiPoint":
        return [[(p.x, p.y)] for p in geom.geoms]
    elif geom_type == "LineString":
        return [list(geom.coords)]
    elif geom_type == "MultiLineString":
        return [list(line.coords) for line in geom.geoms]
    elif geom_type == "Polygon":
        rings = [list(geom.exterior.coords)]
        for interior in geom.interiors:
            rings.append(list(interior.coords))
        return rings
    elif geom_type == "MultiPolygon":
        rings = []
        for poly in geom.geoms:
            rings.append(list(poly.exterior.coords))
            for interior in poly.interiors:
                rings.append(list(interior.coords))
        return rings
    return []


def _set_value(pb_value, python_val, field_type: str):
    """Set a protobuf Value message from a Python value."""
    if python_val is None:
        return

    if hasattr(python_val, "as_py"):
        python_val = python_val.as_py()

    if python_val is None:
        return

    # Handle non-scalar values (dicts, lists) — serialize to string
    if isinstance(python_val, (dict, list)):
        pb_value.string_value = str(python_val)
        return

    try:
        if field_type in ("string", "esriFieldTypeString"):
            pb_value.string_value = str(python_val)
        elif field_type in ("int32", "esriFieldTypeSmallInteger"):
            pb_value.sint_value = int(python_val)
        elif field_type in ("int64", "esriFieldTypeInteger", "esriFieldTypeOID"):
            pb_value.int64_value = int(python_val)
        elif field_type in ("float", "esriFieldTypeSingle"):
            pb_value.float_value = float(python_val)
        elif field_type in ("double", "esriFieldTypeDouble"):
            pb_value.double_value = float(python_val)
        elif field_type in ("boolean",):
            pb_value.bool_value = bool(python_val)
        else:
            pb_value.string_value = str(python_val)
    except (TypeError, ValueError):
        # Fall back to string for any unconvertible values
        pb_value.string_value = str(python_val)


def _build_fields_for_result(
    feature_result, schema: FeatureSchema, present_field_names: list[str]
):
    """Add field definitions matching ONLY the fields present in the result.

    CRITICAL: PBF field count MUST exactly match attribute count per feature.
    When ArcPro sends outFields=__oid, we must only declare __oid in fields,
    not all schema fields. Otherwise ArcPro crashes (field/attribute mismatch).

    Fields = [__oid] + [only schema fields present in result_columns]
    """
    type_map = {
        "string": pb.FeatureCollectionPBuffer.esriFieldTypeString,
        "int32": pb.FeatureCollectionPBuffer.esriFieldTypeSmallInteger,
        "int64": pb.FeatureCollectionPBuffer.esriFieldTypeInteger,
        "float": pb.FeatureCollectionPBuffer.esriFieldTypeSingle,
        "double": pb.FeatureCollectionPBuffer.esriFieldTypeDouble,
        "date": pb.FeatureCollectionPBuffer.esriFieldTypeDate,
        "timestamp": pb.FeatureCollectionPBuffer.esriFieldTypeDate,
    }

    # __oid must be the first field — ArcPro uses it for feature identity
    oid_field = feature_result.fields.add()
    oid_field.name = "__oid"
    oid_field.alias = "OID"
    oid_field.fieldType = pb.FeatureCollectionPBuffer.esriFieldTypeOID

    # Only add fields that are present in the result Arrow table
    present_set = set(present_field_names)
    for f in schema.fields:
        if f["name"] not in present_set:
            continue
        field = feature_result.fields.add()
        field.name = f["name"]
        field.alias = f.get("alias", f["name"])
        field.fieldType = type_map.get(
            f["type"],
            pb.FeatureCollectionPBuffer.esriFieldTypeString,
        )


def _build_fields(feature_result, schema: FeatureSchema):
    """Add ALL field definitions to the PBF FeatureResult.

    Used by _serialize_empty where we don't have a result to match against.
    Always includes __oid as the first field (type OID) since ArcPro
    requires an objectIdField for feature identification and pagination.
    """
    type_map = {
        "string": pb.FeatureCollectionPBuffer.esriFieldTypeString,
        "int32": pb.FeatureCollectionPBuffer.esriFieldTypeSmallInteger,
        "int64": pb.FeatureCollectionPBuffer.esriFieldTypeInteger,
        "float": pb.FeatureCollectionPBuffer.esriFieldTypeSingle,
        "double": pb.FeatureCollectionPBuffer.esriFieldTypeDouble,
        "date": pb.FeatureCollectionPBuffer.esriFieldTypeDate,
        "timestamp": pb.FeatureCollectionPBuffer.esriFieldTypeDate,
    }

    # __oid must be the first field — ArcPro uses it for feature identity
    oid_field = feature_result.fields.add()
    oid_field.name = "__oid"
    oid_field.alias = "OID"
    oid_field.fieldType = pb.FeatureCollectionPBuffer.esriFieldTypeOID

    for f in schema.fields:
        field = feature_result.fields.add()
        field.name = f["name"]
        field.alias = f.get("alias", f["name"])
        field.fieldType = type_map.get(
            f["type"],
            pb.FeatureCollectionPBuffer.esriFieldTypeString,
        )


def _get_field_type(schema: FeatureSchema, field_name: str) -> str:
    """Look up field type from schema."""
    for f in schema.fields:
        if f["name"] == field_name:
            return f["type"]
    return "string"


def _serialize_empty(result: QueryResult, schema: FeatureSchema) -> bytes:
    """Serialize an empty result set."""
    fc = pb.FeatureCollectionPBuffer()
    query_result = fc.queryResult

    if result.features is None and result.count > 0:
        # Count-only result
        query_result.countResult.count = result.count
        return fc.SerializeToString()

    feature_result = query_result.featureResult
    feature_result.spatialReference.wkid = schema.srid
    _build_fields(feature_result, schema)
    feature_result.exceededTransferLimit = False
    return fc.SerializeToString()
