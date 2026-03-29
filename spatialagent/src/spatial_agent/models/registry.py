import logging

import httpx

logger = logging.getLogger(__name__)


async def detect_available_models(
    backend: str, base_url: str, settings=None
) -> list[str]:
    try:
        if backend == "bedrock":
            return _detect_bedrock_models(settings)
        async with httpx.AsyncClient(timeout=10) as client:
            if backend == "vllm":
                resp = await client.get(f"{base_url}/models")
                resp.raise_for_status()
                data = resp.json()
                return [m["id"] for m in data.get("data", [])]
            elif backend == "ollama":
                resp = await client.get(f"{base_url}/api/tags")
                resp.raise_for_status()
                data = resp.json()
                return [m["name"] for m in data.get("models", [])]
    except Exception as e:
        logger.warning("Failed to detect models (%s): %s", backend, e)
    return []


def _detect_bedrock_models(settings) -> list[str]:
    """Return the configured Bedrock model ID as the available model list."""
    if settings and settings.bedrock_model_id:
        return [settings.bedrock_model_id]
    return ["us.anthropic.claude-sonnet-4-20250514"]


def select_model(intent: str, available: list[str], settings) -> str:
    if settings.active_model:
        return settings.active_model

    if not available:
        raise RuntimeError("No LLM models available")

    def _find(name: str) -> str | None:
        for m in available:
            if name in m:
                return m
        return None

    if intent == "spatial":
        return (
            _find(settings.primary_model)
            or _find(settings.mid_model)
            or available[0]
        )
    elif intent == "analytics":
        return (
            _find(settings.fast_model)
            or _find(settings.primary_model)
            or available[0]
        )
    else:  # conversational
        return _find(settings.primary_model) or available[0]
