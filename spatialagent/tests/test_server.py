import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import httpx

from spatial_agent.server import app


@pytest.fixture
def mock_deps():
    """Patch external dependencies for server tests."""
    with patch("spatial_agent.server.detect_available_models", new_callable=AsyncMock) as mock_detect, \
         patch("spatial_agent.server.llm_client") as mock_llm, \
         patch("spatial_agent.server.mcp_client") as mock_mcp, \
         patch("spatial_agent.server.notify_lakehouse", new_callable=AsyncMock) as mock_notify:

        mock_detect.return_value = ["devstral-small-2"]
        mock_llm.generate = AsyncMock(
            return_value="```sql\nSELECT * FROM lakehouse.default.buildings;\n```"
        )
        mock_mcp.call_tool = AsyncMock(
            return_value={"status": "ok", "row_count": 42}
        )

        # Schema builder mock
        with patch("spatial_agent.server.schema_builder") as mock_schema:
            mock_schema.build_context = AsyncMock(
                return_value="Available tables:\n  default.buildings (id INT, geom GEOMETRY)"
            )
            yield {
                "detect": mock_detect,
                "llm": mock_llm,
                "mcp": mock_mcp,
                "notify": mock_notify,
                "schema": mock_schema,
            }


def _parse_sse(text: str) -> list[dict]:
    events = []
    for line in text.strip().split("\n"):
        line = line.strip()
        if line.startswith("data: "):
            events.append(json.loads(line[6:]))
    return events


@pytest.mark.asyncio
async def test_health(mock_deps):
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/api/agent/health")
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "ok"


@pytest.mark.asyncio
async def test_models(mock_deps):
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/api/agent/models")
        assert resp.status_code == 200
        data = resp.json()
        assert "available" in data
        assert "active" in data


@pytest.mark.asyncio
async def test_chat_spatial(mock_deps):
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.post(
            "/api/agent/chat",
            json={"session_id": "test-001", "message": "buildings near the river"},
        )
        assert resp.status_code == 200
        events = _parse_sse(resp.text)
        types = [e["type"] for e in events]
        assert "status" in types
        assert "sql" in types
        assert "result" in types
        assert "done" in types


@pytest.mark.asyncio
async def test_chat_conversational(mock_deps):
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.post(
            "/api/agent/chat",
            json={"session_id": "test-002", "message": "hello"},
        )
        assert resp.status_code == 200
        events = _parse_sse(resp.text)
        types = [e["type"] for e in events]
        assert "sql" not in types
        assert "result" in types
        assert "done" in types


@pytest.mark.asyncio
async def test_chat_mcp_error(mock_deps):
    mock_deps["mcp"].call_tool = AsyncMock(return_value={"error": "table not found"})

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.post(
            "/api/agent/chat",
            json={"session_id": "test-003", "message": "buildings near river"},
        )
        assert resp.status_code == 200
        events = _parse_sse(resp.text)
        types = [e["type"] for e in events]
        assert "error" in types
        assert "done" in types
