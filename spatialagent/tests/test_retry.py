import pytest

from spatial_agent.executor.retry import retry_loop, MaxRetriesExceeded, _get_error_hint


@pytest.mark.asyncio
async def test_success_first_try():
    async def gen_fn(msg, ctx, **kw):
        return "SELECT 1"

    async def exec_fn(sql):
        return {"status": "ok", "row_count": 1}

    events = []
    async for event in retry_loop(gen_fn, exec_fn, "test", "ctx", max_retry=3):
        events.append(event)

    types = [e["type"] for e in events]
    assert "sql" in types
    assert "result_data" in types
    assert "status" not in [e.get("content", "") for e in events if "Retrying" in e.get("content", "")]


@pytest.mark.asyncio
async def test_success_second_try():
    attempts = {"count": 0}

    async def gen_fn(msg, ctx, **kw):
        return "SELECT 1"

    async def exec_fn(sql):
        attempts["count"] += 1
        if attempts["count"] == 1:
            return {"error": "table not found"}
        return {"status": "ok", "row_count": 5}

    events = []
    async for event in retry_loop(gen_fn, exec_fn, "test", "ctx", max_retry=3):
        events.append(event)

    types = [e["type"] for e in events]
    assert "result_data" in types
    retry_statuses = [e for e in events if "Retrying" in e.get("content", "")]
    assert len(retry_statuses) == 1


@pytest.mark.asyncio
async def test_all_retries_exhausted():
    async def gen_fn(msg, ctx, **kw):
        return "SELECT bad"

    async def exec_fn(sql):
        return {"error": "syntax error"}

    with pytest.raises(MaxRetriesExceeded):
        async for _ in retry_loop(gen_fn, exec_fn, "test", "ctx", max_retry=2):
            pass


@pytest.mark.asyncio
async def test_error_message_in_retry():
    calls = []

    async def gen_fn(msg, ctx, **kw):
        calls.append(kw)
        return "SELECT 1"

    async def exec_fn(sql):
        if len(calls) < 2:
            return {"error": "column xyz not found"}
        return {"status": "ok", "row_count": 1}

    events = []
    async for event in retry_loop(gen_fn, exec_fn, "test", "ctx", max_retry=3):
        events.append(event)

    assert len(calls) >= 2
    assert "column xyz not found" in calls[1].get("error", "")


# ── Error hint tests ───────────────────────────────────────────


@pytest.mark.parametrize("error_msg,expected_keyword", [
    (
        "No function matches the given name and argument types 'ST_Intersects(BLOB, BLOB)'",
        "ST_GeomFromWKB",
    ),
    (
        "No function matches the given name and argument types 'ST_AsGeoJSON(BLOB)'",
        "ST_GeomFromWKB",
    ),
    (
        "No function matches the given name and argument types 'ST_Buffer(BLOB, INTEGER)'",
        "ST_GeomFromWKB",
    ),
    (
        'Referenced column "type" not found in FROM clause',
        "column name",
    ),
    (
        'Referenced column "geom" not found in FROM clause',
        "column name",
    ),
    (
        'column "z.class" must appear in the GROUP BY clause',
        "GROUP BY",
    ),
    (
        'Ambiguous reference to column name "id"',
        "aliases",
    ),
    (
        "Subquery returned more than 1 row",
        "LIMIT 1",
    ),
    (
        "ST_Transform: Invalid source CRS: EPSG:1234",
        "EPSG:4326",
    ),
    (
        "Parser Error: syntax error at or near SELECT",
        "syntax",
    ),
])
def test_error_hint_matching(error_msg, expected_keyword):
    """Each known error pattern should produce a hint containing the expected keyword."""
    hint = _get_error_hint(error_msg)
    assert hint, f"No hint generated for: {error_msg}"
    assert expected_keyword.lower() in hint.lower(), (
        f"Hint for {error_msg!r:.60} should contain {expected_keyword!r}, got: {hint!r:.100}"
    )


def test_unknown_error_no_hint():
    """Unknown errors should not produce a hint."""
    hint = _get_error_hint("Something completely unexpected happened")
    assert hint == ""


@pytest.mark.asyncio
async def test_hint_appended_on_retry():
    """The error hint should be appended to the error passed to generate_fn on retry."""
    calls = []

    async def gen_fn(msg, ctx, **kw):
        calls.append(kw)
        return "SELECT 1"

    async def exec_fn(sql):
        if len(calls) < 2:
            return {"error": "No function matches the given name and argument types 'ST_DWithin(BLOB, BLOB, DOUBLE)'"}
        return {"status": "ok", "row_count": 1}

    events = []
    async for event in retry_loop(gen_fn, exec_fn, "test", "ctx", max_retry=3):
        events.append(event)

    assert len(calls) >= 2
    error_passed = calls[1].get("error", "")
    assert "ST_GeomFromWKB" in error_passed, (
        f"Hint not appended to error: {error_passed!r:.200}"
    )
