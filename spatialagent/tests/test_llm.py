from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from spatial_agent.models.llm import LLMClient


@pytest.mark.asyncio
async def test_vllm_generate():
    client = LLMClient("vllm", "http://localhost:8000/v1", "http://localhost:11434")

    mock_choice = MagicMock()
    mock_choice.message.content = "SELECT * FROM buildings;"
    mock_response = MagicMock()
    mock_response.choices = [mock_choice]

    mock_openai = AsyncMock()
    mock_openai.chat.completions.create = AsyncMock(return_value=mock_response)
    mock_openai.close = AsyncMock()

    with patch("spatial_agent.models.llm.AsyncOpenAI", return_value=mock_openai):
        result = await client.generate(
            [{"role": "user", "content": "test"}], "devstral"
        )
        assert result == "SELECT * FROM buildings;"


@pytest.mark.asyncio
async def test_ollama_generate():
    client = LLMClient("ollama", "http://localhost:8000/v1", "http://localhost:11434")

    mock_resp = MagicMock()
    mock_resp.json.return_value = {
        "message": {"content": "SELECT count(*) FROM roads;"}
    }
    mock_resp.raise_for_status = MagicMock()

    mock_http = AsyncMock()
    mock_http.post = AsyncMock(return_value=mock_resp)
    mock_http.__aenter__ = AsyncMock(return_value=mock_http)
    mock_http.__aexit__ = AsyncMock(return_value=False)

    with patch("spatial_agent.models.llm.httpx.AsyncClient", return_value=mock_http):
        result = await client.generate(
            [{"role": "user", "content": "test"}], "devstral"
        )
        assert result == "SELECT count(*) FROM roads;"


@pytest.mark.asyncio
async def test_unknown_backend():
    client = LLMClient("unknown", "", "")
    with pytest.raises(ValueError, match="Unknown LLM backend"):
        await client.generate([{"role": "user", "content": "test"}], "model")
