import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from spatial_agent.executor.mcp_client import MCPClient


@pytest.fixture
def mcp_client():
    return MCPClient("http://localhost:8082/mcp")


def _make_mock_session(tool_result_text='{"status": "ok", "row_count": 42}'):
    mock_session = AsyncMock()
    mock_session.initialize = AsyncMock()

    content_item = MagicMock()
    content_item.text = tool_result_text
    mock_result = MagicMock()
    mock_result.content = [content_item]
    mock_session.call_tool = AsyncMock(return_value=mock_result)

    tool = MagicMock()
    tool.name = "query"
    tools_result = MagicMock()
    tools_result.tools = [tool]
    mock_session.list_tools = AsyncMock(return_value=tools_result)

    return mock_session


@pytest.mark.asyncio
async def test_call_tool_success(mcp_client):
    mock_session = _make_mock_session()

    with patch("spatial_agent.executor.mcp_client.streamablehttp_client") as mock_http, \
         patch("spatial_agent.executor.mcp_client.ClientSession") as mock_cs:
        ctx = AsyncMock()
        ctx.__aenter__ = AsyncMock(return_value=(AsyncMock(), AsyncMock(), AsyncMock()))
        ctx.__aexit__ = AsyncMock(return_value=False)
        mock_http.return_value = ctx

        session_ctx = AsyncMock()
        session_ctx.__aenter__ = AsyncMock(return_value=mock_session)
        session_ctx.__aexit__ = AsyncMock(return_value=False)
        mock_cs.return_value = session_ctx

        result = await mcp_client.call_tool("query", {"sql": "SELECT 1"})
        assert result == {"status": "ok", "row_count": 42}


@pytest.mark.asyncio
async def test_call_tool_connection_error(mcp_client):
    with patch("spatial_agent.executor.mcp_client.streamablehttp_client") as mock_http:
        mock_http.side_effect = ConnectionRefusedError("Connection refused")
        result = await mcp_client.call_tool("query", {"sql": "SELECT 1"})
        assert "error" in result


@pytest.mark.asyncio
async def test_call_tool_malformed_json(mcp_client):
    mock_session = _make_mock_session(tool_result_text="not valid json")

    with patch("spatial_agent.executor.mcp_client.streamablehttp_client") as mock_http, \
         patch("spatial_agent.executor.mcp_client.ClientSession") as mock_cs:
        ctx = AsyncMock()
        ctx.__aenter__ = AsyncMock(return_value=(AsyncMock(), AsyncMock(), AsyncMock()))
        ctx.__aexit__ = AsyncMock(return_value=False)
        mock_http.return_value = ctx

        session_ctx = AsyncMock()
        session_ctx.__aenter__ = AsyncMock(return_value=mock_session)
        session_ctx.__aexit__ = AsyncMock(return_value=False)
        mock_cs.return_value = session_ctx

        result = await mcp_client.call_tool("query", {"sql": "SELECT 1"})
        assert "error" in result
        assert "Malformed JSON" in result["error"]


@pytest.mark.asyncio
async def test_list_tools_success(mcp_client):
    mock_session = _make_mock_session()

    with patch("spatial_agent.executor.mcp_client.streamablehttp_client") as mock_http, \
         patch("spatial_agent.executor.mcp_client.ClientSession") as mock_cs:
        ctx = AsyncMock()
        ctx.__aenter__ = AsyncMock(return_value=(AsyncMock(), AsyncMock(), AsyncMock()))
        ctx.__aexit__ = AsyncMock(return_value=False)
        mock_http.return_value = ctx

        session_ctx = AsyncMock()
        session_ctx.__aenter__ = AsyncMock(return_value=mock_session)
        session_ctx.__aexit__ = AsyncMock(return_value=False)
        mock_cs.return_value = session_ctx

        tools = await mcp_client.list_tools()
        assert len(tools) == 1
        assert tools[0].name == "query"
