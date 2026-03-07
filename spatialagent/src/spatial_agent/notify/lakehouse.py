import logging

import httpx

logger = logging.getLogger(__name__)


async def notify_lakehouse(
    lakehouse_api: str,
    session_id: str,
    namespace: str,
    table: str,
    row_count: int,
    description: str = "",
) -> None:
    try:
        async with httpx.AsyncClient() as client:
            await client.post(
                f"{lakehouse_api}/api/agent/notify/{session_id}",
                json={
                    "namespace": namespace,
                    "table": table,
                    "row_count": row_count,
                    "description": description,
                },
                timeout=10.0,
            )
    except Exception as e:
        logger.warning("Failed to notify lakehouse: %s", e)
