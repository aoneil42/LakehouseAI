import time
from unittest.mock import AsyncMock

import pytest

from spatial_agent.planner.schema import SchemaBuilder, SCHEMA_CACHE_TTL
from spatial_agent.session import SessionState


@pytest.fixture
def mock_mcp():
    mcp = AsyncMock()

    async def call_tool(name, args):
        if name == "list_tables":
            return {
                "rows": [
                    {"schema_name": "default", "table_name": "buildings"},
                    {"schema_name": "default", "table_name": "roads"},
                    {"schema_name": "default", "table_name": "rivers"},
                ]
            }
        elif name == "describe_table":
            table = args.get("table", "")
            if "buildings" in table:
                return {
                    "rows": [
                        {"column_name": "id", "column_type": "INTEGER"},
                        {"column_name": "name", "column_type": "VARCHAR"},
                        {"column_name": "geom", "column_type": "GEOMETRY", "is_geometry": True},
                    ]
                }
            elif "roads" in table:
                return {
                    "rows": [
                        {"column_name": "id", "column_type": "INTEGER"},
                        {"column_name": "type", "column_type": "VARCHAR"},
                        {"column_name": "geom", "column_type": "GEOMETRY", "is_geometry": True},
                    ]
                }
            return {"rows": [{"column_name": "id", "column_type": "INTEGER"}]}
        elif name == "get_bbox":
            return {"bbox": [-77.5, 38.8, -77.0, 39.0]}
        return {}

    mcp.call_tool = AsyncMock(side_effect=call_tool)
    return mcp


@pytest.fixture
def session():
    return SessionState(session_id="test-1234")


@pytest.mark.asyncio
async def test_build_context_matches_tables(mock_mcp, session):
    builder = SchemaBuilder(mock_mcp)
    ctx = await builder.build_context("show me buildings", session)
    assert "buildings" in ctx
    assert "GEOMETRY" in ctx


@pytest.mark.asyncio
async def test_build_context_excludes_unrelated(mock_mcp, session):
    builder = SchemaBuilder(mock_mcp)
    ctx = await builder.build_context("show me buildings", session)
    assert "roads" not in ctx


@pytest.mark.asyncio
async def test_cache_hit(mock_mcp, session):
    builder = SchemaBuilder(mock_mcp)
    await builder.build_context("show me buildings", session)
    call_count_1 = mock_mcp.call_tool.call_count

    await builder.build_context("show me buildings again", session)
    call_count_2 = mock_mcp.call_tool.call_count

    # list_tables should not be called again (cached)
    list_calls_1 = sum(
        1 for c in mock_mcp.call_tool.call_args_list[:call_count_1]
        if c[0][0] == "list_tables"
    )
    list_calls_2 = sum(
        1 for c in mock_mcp.call_tool.call_args_list[call_count_1:]
        if c[0][0] == "list_tables"
    )
    assert list_calls_1 == 1
    assert list_calls_2 == 0


@pytest.mark.asyncio
async def test_cache_miss_after_ttl(mock_mcp, session):
    builder = SchemaBuilder(mock_mcp)
    await builder.build_context("show me buildings", session)

    # Expire the cache
    session.schema_cache_ts -= SCHEMA_CACHE_TTL + 1

    await builder.build_context("show me buildings", session)

    list_calls = sum(
        1 for c in mock_mcp.call_tool.call_args_list
        if c[0][0] == "list_tables"
    )
    assert list_calls == 2
