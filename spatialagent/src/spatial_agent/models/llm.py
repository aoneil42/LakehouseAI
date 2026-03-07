import logging

import httpx
from openai import AsyncOpenAI

logger = logging.getLogger(__name__)


class LLMClient:
    def __init__(self, backend: str, vllm_url: str, ollama_url: str, timeout: int = 60):
        self.backend = backend
        self.vllm_url = vllm_url
        self.ollama_url = ollama_url
        self.timeout = timeout

    async def generate(self, messages: list[dict], model: str) -> str:
        if self.backend == "vllm":
            return await self._vllm_generate(messages, model)
        elif self.backend == "ollama":
            return await self._ollama_generate(messages, model)
        else:
            raise ValueError(f"Unknown LLM backend: {self.backend}")

    async def _vllm_generate(self, messages: list[dict], model: str) -> str:
        client = AsyncOpenAI(
            base_url=self.vllm_url,
            api_key="not-needed",
        )
        try:
            response = await client.chat.completions.create(
                model=model,
                messages=messages,
                timeout=self.timeout,
            )
            return response.choices[0].message.content or ""
        finally:
            await client.close()

    async def _ollama_generate(self, messages: list[dict], model: str) -> str:
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            resp = await client.post(
                f"{self.ollama_url}/api/chat",
                json={"model": model, "messages": messages, "stream": False},
            )
            resp.raise_for_status()
            data = resp.json()
            return data.get("message", {}).get("content", "")
