from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from spatial_agent.notify.lakehouse import notify_lakehouse


@pytest.mark.asyncio
async def test_notify_success():
    mock_resp = MagicMock()
    mock_resp.raise_for_status = MagicMock()

    mock_client = AsyncMock()
    mock_client.post = AsyncMock(return_value=mock_resp)
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=False)

    with patch("spatial_agent.notify.lakehouse.httpx.AsyncClient", return_value=mock_client):
        await notify_lakehouse(
            "http://localhost:8000",
            "session-123",
            "_scratch_abc12345",
            "buildings_nearby",
            847,
            "Buildings within 500m of river",
        )

        mock_client.post.assert_called_once()
        call_args = mock_client.post.call_args
        assert "/api/agent/notify/session-123" in call_args[0][0]
        payload = call_args[1]["json"]
        assert payload["namespace"] == "_scratch_abc12345"
        assert payload["table"] == "buildings_nearby"
        assert payload["row_count"] == 847


@pytest.mark.asyncio
async def test_notify_failure_does_not_raise():
    mock_client = AsyncMock()
    mock_client.post = AsyncMock(side_effect=ConnectionError("refused"))
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=False)

    with patch("spatial_agent.notify.lakehouse.httpx.AsyncClient", return_value=mock_client):
        # Should not raise
        await notify_lakehouse(
            "http://localhost:8000", "session-123",
            "_scratch_abc", "tbl", 0,
        )
