import json
import logging

from mcp import ClientSession
from mcp.client.streamable_http import streamablehttp_client

logger = logging.getLogger(__name__)


class MCPClient:
    def __init__(self, endpoint: str):
        self.endpoint = endpoint

    async def call_tool(self, tool_name: str, arguments: dict) -> dict:
        try:
            async with streamablehttp_client(self.endpoint) as (read, write, _):
                async with ClientSession(read, write) as session:
                    await session.initialize()
                    result = await session.call_tool(tool_name, arguments)
                    text = next(
                        (c.text for c in result.content if hasattr(c, "text")), "{}"
                    )
                    return json.loads(text)
        except json.JSONDecodeError as e:
            logger.error("Malformed JSON from MCP tool %s: %s", tool_name, e)
            return {"error": f"Malformed JSON response: {e}"}
        except Exception as e:
            logger.error("MCP call_tool error (%s): %s", tool_name, e)
            return {"error": f"MCP connection error: {e}"}

    async def list_tools(self) -> list:
        try:
            async with streamablehttp_client(self.endpoint) as (read, write, _):
                async with ClientSession(read, write) as session:
                    await session.initialize()
                    return (await session.list_tools()).tools
        except Exception as e:
            logger.error("MCP list_tools error: %s", e)
            return []
