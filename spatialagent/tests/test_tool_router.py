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
