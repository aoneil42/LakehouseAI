"""Tests for the rule-based discovery tool router."""

import pytest

from spatial_agent.router.tool_router import match, format_result, ToolRoute


@pytest.fixture
def known_tables():
    """Simulated table cache matching SchemaBuilder format."""
    return [
        {"namespace": "default", "name": "buildings", "full_name": "lakehouse.default.buildings"},
        {"namespace": "default", "name": "roads", "full_name": "lakehouse.default.roads"},
        {"namespace": "default", "name": "parcels", "full_name": "lakehouse.default.parcels"},
        {"namespace": "default", "name": "census_tracts", "full_name": "lakehouse.default.census_tracts"},
        {"namespace": "default", "name": "zones", "full_name": "lakehouse.default.zones"},
    ]


# ── Q1: list_tables ──────────────────────────────────────────────


@pytest.mark.parametrize("msg", [
    "What datasets are available?",
    "What tables are available?",
    "List all tables",
    "Show me the available data",
    "What data do we have?",
])
def test_list_tables(msg, known_tables):
    route = match(msg, known_tables)
    assert route is not None
    assert route.tool_name == "list_tables"
    assert route.arguments == {}


# ── Q2: list_namespaces ──────────────────────────────────────────


@pytest.mark.parametrize("msg", [
    "What namespaces exist in the catalog?",
    "List all schemas",
    "Show me the namespaces",
    "What schemas are available?",
])
def test_list_namespaces(msg, known_tables):
    route = match(msg, known_tables)
    assert route is not None
    assert route.tool_name == "list_namespaces"
    assert route.arguments == {}


# ── Q3/Q7: describe_table ────────────────────────────────────────


@pytest.mark.parametrize("msg,expected_table", [
    ("Describe the schema of the buildings table", "default.buildings"),
    ("What columns does the parcels table have?", "default.parcels"),
    ("Show me the columns of roads", "default.roads"),
    ("What fields are in the census_tracts table?", "default.census_tracts"),
])
def test_describe_table(msg, expected_table, known_tables):
    route = match(msg, known_tables)
    assert route is not None
    assert route.tool_name == "describe_table"
    assert route.arguments == {"table": expected_table}


# ── Q4: search_tables geometry_only ──────────────────────────────


@pytest.mark.parametrize("msg", [
    "Which tables have geometry columns?",
    "What tables are spatial?",
    "Show me tables with geometry",
    "List datasets that have geometry",
])
def test_geometry_only(msg, known_tables):
    route = match(msg, known_tables)
    assert route is not None
    assert route.tool_name == "search_tables"
    assert route.arguments == {"geometry_only": True}


# ── Q5: search_tables column_pattern ─────────────────────────────


@pytest.mark.parametrize("msg,expected_col", [
    ("Are there any tables with a population column?", "population"),
    ("Which tables have a height field?", "height"),
    ("Find tables containing a name column", "name"),
    # Implicit column search (no "column" word)
    ("which tables have timestamps?", "timestamps"),
    ("what tables have population?", "population"),
])
def test_column_search(msg, expected_col, known_tables):
    route = match(msg, known_tables)
    assert route is not None
    assert route.tool_name == "search_tables"
    assert route.arguments == {"column_pattern": expected_col}


# ── Q6: search_tables pattern ────────────────────────────────────


@pytest.mark.parametrize("msg,expected_pattern", [
    ("Find tables related to roads or transportation", "roads"),
    ("Search for tables about census", "census"),
])
def test_table_search(msg, expected_pattern, known_tables):
    route = match(msg, known_tables)
    assert route is not None
    assert route.tool_name == "search_tables"
    assert route.arguments == {"pattern": expected_pattern}


# ── Q8/Q11: sample_data ─────────────────────────────────────────


@pytest.mark.parametrize("msg,expected_args", [
    ("Show me a sample of the buildings data", {"table": "default.buildings"}),
    ("Preview the first 5 rows of roads", {"table": "default.roads", "n": 5}),
    ("Give me a sample of the parcels table", {"table": "default.parcels"}),
    (
        "Preview the first 10 rows of the buildings data without the geometry",
        {"table": "default.buildings", "n": 10, "include_geometry": False},
    ),
])
def test_sample_data(msg, expected_args, known_tables):
    route = match(msg, known_tables)
    assert route is not None
    assert route.tool_name == "sample_data"
    assert route.arguments == expected_args


# ── Q9/Q10/Q12: table_stats ────────────────────────────────────


@pytest.mark.parametrize("msg,expected_table", [
    ("How many records are in the zones table?", "default.zones"),
    ("Summarize the buildings dataset", "default.buildings"),
    ("Row count for parcels", "default.parcels"),
])
def test_table_stats(msg, expected_table, known_tables):
    route = match(msg, known_tables)
    assert route is not None
    assert route.tool_name == "table_stats"
    assert route.arguments == {"table": expected_table}
    assert route.format_hint == ""


# ── Q12: geometry_types ─────────────────────────────────────────


@pytest.mark.parametrize("msg,expected_table", [
    ("What types of geometries are in the roads table?", "default.roads"),
    ("What kind of geometries are in the buildings dataset?", "default.buildings"),
])
def test_geometry_types(msg, expected_table, known_tables):
    route = match(msg, known_tables)
    assert route is not None
    assert route.tool_name == "table_stats"
    assert route.arguments == {"table": expected_table}
    assert route.format_hint == "geometry_types"


# ── Q13/Q14: get_bbox ──────────────────────────────────────────


@pytest.mark.parametrize("msg,expected_table", [
    ("What geographic area does the parcels table cover?", "default.parcels"),
    ("Give me the bounding box of buildings", "default.buildings"),
    ("What is the spatial extent of the zones dataset?", "default.zones"),
])
def test_get_bbox(msg, expected_table, known_tables):
    route = match(msg, known_tables)
    assert route is not None
    assert route.tool_name == "get_bbox"
    assert route.arguments == {"table": expected_table}


# ── Fallback ─────────────────────────────────────────────────────


def test_no_match_returns_none(known_tables):
    route = match("Tell me something interesting", known_tables)
    assert route is None


def test_describe_unknown_table_falls_back(known_tables):
    """Table name mentioned via 'the X table' pattern, not in known list."""
    route = match("Describe the schema of the foobar table", known_tables)
    assert route is not None
    assert route.tool_name == "describe_table"
    assert route.arguments == {"table": "default.foobar"}


def test_describe_no_table_returns_none():
    """No table name extractable at all."""
    route = match("Describe the schema", [])
    assert route is None


# ── Result Formatting ────────────────────────────────────────────


def test_format_list_namespaces():
    result = {"namespaces": ["default", "_scratch_abc123"]}
    text = format_result("list_namespaces", result)
    assert "2 namespace(s)" in text
    assert "default" in text


def test_format_list_tables():
    result = {
        "row_count": 2,
        "rows": [
            {"schema_name": "default", "table_name": "buildings", "column_count": 5},
            {"schema_name": "default", "table_name": "roads", "column_count": 4},
        ],
    }
    text = format_result("list_tables", result)
    assert "2 table(s)" in text
    assert "buildings" in text
    assert "roads" in text


def test_format_describe_table():
    result = {
        "row_count": 3,
        "rows": [
            {"column_name": "id", "column_type": "INTEGER", "is_geometry": False},
            {"column_name": "name", "column_type": "VARCHAR", "is_geometry": False},
            {"column_name": "geom", "column_type": "GEOMETRY", "is_geometry": True},
        ],
    }
    text = format_result("describe_table", result)
    assert "3 columns" in text
    assert "(geometry)" in text


def test_format_search_tables():
    result = {
        "row_count": 1,
        "rows": [
            {
                "schema_name": "default",
                "table_name": "buildings",
                "has_geometry": True,
                "column_names": ["id", "name", "geometry"],
            },
        ],
    }
    text = format_result("search_tables", result)
    assert "1 matching" in text
    assert "[spatial]" in text


def test_format_error():
    result = {"error": True, "message": "Table not found"}
    text = format_result("describe_table", result)
    assert "Error" in text
    assert "Table not found" in text


def test_format_empty_tables():
    result = {"rows": []}
    text = format_result("list_tables", result)
    assert "No tables found" in text


def test_format_sample_data():
    result = {
        "rows": [
            {"id": 1, "name": "test", "geometry": b"\x01\x02\x00"},
        ],
    }
    text = format_result("sample_data", result)
    assert "(geometry)" in text
    assert "test" in text
    assert "1 row(s)" in text


def test_format_table_stats():
    result = {
        "row_count": 1000,
        "column_count": 8,
        "columns": [
            {"name": "id", "type": "INTEGER"},
            {"name": "geometry", "type": "BLOB"},
        ],
        "spatial": {
            "total_rows": 1000,
            "non_null_geom": 995,
            "null_geom": 5,
            "min_lon": 2.33,
            "min_lat": 48.85,
            "max_lon": 2.34,
            "max_lat": 48.86,
        },
        "geometry_types": [
            {"geom_type": "POLYGON", "cnt": 900},
            {"geom_type": "MULTIPOLYGON", "cnt": 95},
        ],
    }
    text = format_result("table_stats", result)
    assert "1,000" in text
    assert "POLYGON" in text
    assert "Bounding box:" in text
    assert "48.85" in text
    assert "995" in text
    assert "5" in text  # null count


def test_format_geometry_types_hint():
    result = {
        "row_count": 5000,
        "column_count": 10,
        "geometry_types": [
            {"geom_type": "POINT", "cnt": 4500},
            {"geom_type": "MULTIPOINT", "cnt": 500},
        ],
        "spatial": {"min_lon": 1.0, "min_lat": 2.0, "max_lon": 3.0, "max_lat": 4.0,
                     "non_null_geom": 5000, "null_geom": 0},
    }
    text = format_result("table_stats", result, format_hint="geometry_types")
    assert "POINT" in text
    assert "4,500" in text
    # Should NOT include full stats
    assert "Row count" not in text
    assert "Column count" not in text
    assert "Bounding box" not in text


def test_format_get_bbox():
    result = {
        "min_lon": -105.5,
        "min_lat": 39.5,
        "max_lon": -104.5,
        "max_lat": 40.5,
    }
    text = format_result("get_bbox", result)
    assert "[-105.5, 39.5, -104.5, 40.5]" in text


# ── Tier 3: table_snapshots ──────────────────────────────────────


@pytest.mark.parametrize("msg,expected_table", [
    ("What snapshots exist for the buildings table?", "default.buildings"),
    ("Show the version history of roads", "default.roads"),
    ("List all snapshots for parcels", "default.parcels"),
    ("What snapshots are available for buildings?", "default.buildings"),
])
def test_table_snapshots(msg, expected_table, known_tables):
    route = match(msg, known_tables)
    assert route is not None
    assert route.tool_name == "table_snapshots"
    assert route.arguments == {"table": expected_table}


# ── Tier 3: time_travel_query ────────────────────────────────────


@pytest.mark.parametrize("msg,expected_table,expected_key,expected_val", [
    (
        "Show buildings as it was at snapshot 12345",
        "default.buildings", "snapshot_id", 12345,
    ),
    (
        "What did the buildings table look like on 2025-06-15?",
        "default.buildings", "timestamp", "2025-06-15 00:00:00",
    ),
    (
        "What did the roads table look like on March 1, 2026?",
        "default.roads", "timestamp", "2026-03-01 00:00:00",
    ),
])
def test_time_travel_query(msg, expected_table, expected_key, expected_val, known_tables):
    route = match(msg, known_tables)
    assert route is not None
    assert route.tool_name == "time_travel_query"
    assert route.arguments["table"] == expected_table
    assert route.arguments[expected_key] == expected_val
    assert route.arguments.get("sql_select") == "SELECT *"


def test_time_travel_earliest_falls_back_to_snapshots(known_tables):
    """'as it was at the earliest' needs snapshots first."""
    route = match("Show buildings data as it was at the earliest snapshot", known_tables)
    assert route is not None
    assert route.tool_name == "table_snapshots"


# ── Tier 3: export_geojson ──────────────────────────────────────


@pytest.mark.parametrize("msg,expected_table", [
    ("Export buildings as GeoJSON", "default.buildings"),
    ("Download the roads data as geojson", "default.roads"),
    ("Export the parcels table as GeoJSON", "default.parcels"),
])
def test_export_geojson(msg, expected_table, known_tables):
    route = match(msg, known_tables)
    assert route is not None
    assert route.tool_name == "export_geojson"
    assert route.arguments["table"] == expected_table
    assert route.format_hint == "export"


# ── Tier 3: formatting for new tools ────────────────────────────


def test_format_table_snapshots():
    result = {
        "snapshots": [
            {"snapshot_id": 123, "timestamp": "2025-06-15T00:00:00Z"},
            {"snapshot_id": 456, "timestamp": "2025-07-01T00:00:00Z"},
        ],
    }
    text = format_result("table_snapshots", result)
    assert "2 snapshot(s)" in text
    assert "123" in text
    assert "456" in text


def test_format_table_snapshots_empty():
    result = {"snapshots": []}
    text = format_result("table_snapshots", result)
    assert "No snapshots" in text


def test_format_export_geojson():
    result = {
        "type": "FeatureCollection",
        "features": [{"type": "Feature"}, {"type": "Feature"}],
        "metadata": {"feature_count": 42, "truncated": False},
    }
    text = format_result("export_geojson", result)
    assert "42 features" in text
    assert "GeoJSON" in text


def test_format_export_geojson_truncated():
    result = {
        "type": "FeatureCollection",
        "features": [],
        "metadata": {"feature_count": 500, "truncated": True},
    }
    text = format_result("export_geojson", result)
    assert "500 features" in text
    assert "truncated" in text


# ── Tier 2: spatial aggregation should NOT match meta ────────────


@pytest.mark.parametrize("msg", [
    "How many buildings are in each zone?",
    "What is the average building height per zone?",
    "Count buildings taller than 20m in each zone",
])
def test_spatial_aggregation_not_meta(msg, known_tables):
    """Spatial aggregation queries should not route through meta tool router."""
    route = match(msg, known_tables)
    assert route is None or route.tool_name not in ("table_stats", "sample_data")


# ── tool_picker result naming ────────────────────────────────────

from spatial_agent.executor.tool_picker import generate_result_name


def test_result_name_dissolved():
    sql = "SELECT ST_Union_Agg(ST_Buffer(ST_Transform(...))) FROM lakehouse.default.places WHERE type = 'medical'"
    assert generate_result_name(sql) == "places_dissolved"


def test_result_name_aggregated():
    sql = "SELECT z.id, COUNT(b.*) FROM lakehouse.default.zones z LEFT JOIN lakehouse.default.buildings b ON ST_Contains(...) GROUP BY z.id"
    assert generate_result_name(sql) == "zones_aggregated"


def test_result_name_distance():
    sql = "SELECT ST_Distance(ST_GeomFromWKB(a.geometry), ST_GeomFromWKB(b.geometry)) FROM lakehouse.default.buildings a"
    assert generate_result_name(sql) == "buildings_distance"
