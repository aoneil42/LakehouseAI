import logging

import httpx
from openai import AsyncOpenAI

logger = logging.getLogger(__name__)


class LLMClient:
    def __init__(
        self,
        backend: str,
        vllm_url: str,
        ollama_url: str,
        timeout: int = 60,
        bedrock_region: str = "us-east-1",
        bedrock_model_id: str = "",
    ):
        self.backend = backend
        self.vllm_url = vllm_url
        self.ollama_url = ollama_url
        self.timeout = timeout
        self.bedrock_region = bedrock_region
        self.bedrock_model_id = bedrock_model_id

    async def generate(self, messages: list[dict], model: str) -> str:
        if self.backend == "vllm":
            return await self._vllm_generate(messages, model)
        elif self.backend == "ollama":
            return await self._ollama_generate(messages, model)
        elif self.backend == "bedrock":
            return await self._bedrock_generate(messages, model)
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

    async def _bedrock_generate(self, messages: list[dict], model: str) -> str:
        import asyncio
        import boto3

        model_id = model or self.bedrock_model_id

        # Convert to Bedrock converse format
        bedrock_messages = []
        system_text = ""
        for msg in messages:
            role = msg["role"]
            content = msg.get("content", "")
            if role == "system":
                system_text += content + "\n"
            else:
                bedrock_messages.append({
                    "role": "user" if role == "user" else "assistant",
                    "content": [{"text": content}],
                })

        client = boto3.client("bedrock-runtime", region_name=self.bedrock_region)

        kwargs = {
            "modelId": model_id,
            "messages": bedrock_messages,
        }
        if system_text.strip():
            kwargs["system"] = [{"text": system_text.strip()}]

        # Run synchronous boto3 call in executor to avoid blocking
        loop = asyncio.get_event_loop()
        response = await loop.run_in_executor(None, lambda: client.converse(**kwargs))

        output = response.get("output", {})
        msg = output.get("message", {})
        parts = msg.get("content", [])
        return "".join(p.get("text", "") for p in parts)
