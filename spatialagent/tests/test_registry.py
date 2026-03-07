from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from spatial_agent.models.registry import detect_available_models, select_model


@pytest.mark.asyncio
async def test_detect_vllm_models():
    mock_resp = MagicMock()
    mock_resp.json.return_value = {
        "data": [{"id": "devstral-small-2"}, {"id": "ministral-3-14b-instruct"}]
    }
    mock_resp.raise_for_status = MagicMock()

    mock_client = AsyncMock()
    mock_client.get = AsyncMock(return_value=mock_resp)
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=False)

    with patch("spatial_agent.models.registry.httpx.AsyncClient", return_value=mock_client):
        models = await detect_available_models("vllm", "http://localhost:8000/v1")
        assert "devstral-small-2" in models


@pytest.mark.asyncio
async def test_detect_ollama_models():
    mock_resp = MagicMock()
    mock_resp.json.return_value = {
        "models": [{"name": "devstral:latest"}, {"name": "duckdb-nsql:7b"}]
    }
    mock_resp.raise_for_status = MagicMock()

    mock_client = AsyncMock()
    mock_client.get = AsyncMock(return_value=mock_resp)
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=False)

    with patch("spatial_agent.models.registry.httpx.AsyncClient", return_value=mock_client):
        models = await detect_available_models("ollama", "http://localhost:11434")
        assert "devstral:latest" in models


@pytest.mark.asyncio
async def test_detect_connection_failure():
    with patch("spatial_agent.models.registry.httpx.AsyncClient") as mock_cls:
        mock_client = AsyncMock()
        mock_client.get = AsyncMock(side_effect=ConnectionError("refused"))
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)
        mock_cls.return_value = mock_client

        models = await detect_available_models("vllm", "http://localhost:8000/v1")
        assert models == []


def test_select_model_spatial():
    settings = MagicMock()
    settings.active_model = ""
    settings.primary_model = "devstral"
    settings.mid_model = "ministral"
    settings.fast_model = "nsql"

    available = ["devstral-small-2", "ministral-3-14b", "duckdb-nsql-7b"]
    result = select_model("spatial", available, settings)
    assert "devstral" in result


def test_select_model_analytics():
    settings = MagicMock()
    settings.active_model = ""
    settings.primary_model = "devstral"
    settings.mid_model = "ministral"
    settings.fast_model = "nsql"

    available = ["devstral-small-2", "duckdb-nsql-7b"]
    result = select_model("analytics", available, settings)
    assert "nsql" in result


def test_select_model_override():
    settings = MagicMock()
    settings.active_model = "my-custom-model"

    result = select_model("spatial", ["devstral"], settings)
    assert result == "my-custom-model"


def test_select_model_no_available():
    settings = MagicMock()
    settings.active_model = ""

    with pytest.raises(RuntimeError):
        select_model("spatial", [], settings)
