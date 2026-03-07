import pytest

from spatial_agent.executor.retry import retry_loop, MaxRetriesExceeded


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
    assert calls[1].get("error") == "column xyz not found"
