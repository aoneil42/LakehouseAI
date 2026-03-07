import logging
from typing import AsyncGenerator

logger = logging.getLogger(__name__)


class MaxRetriesExceeded(Exception):
    pass


def _extract_error(result: dict) -> str | None:
    """Extract error message from MCP result. Returns None if no error."""
    if result.get("error") is True:
        return result.get("message", "Unknown MCP error")
    if isinstance(result.get("error"), str) and result["error"]:
        return result["error"]
    return None


async def retry_loop(
    generate_fn,
    execute_fn,
    user_message: str,
    schema_context: str,
    max_retry: int = 3,
) -> AsyncGenerator[dict, None]:
    last_error = None
    last_sql = None

    for attempt in range(1, max_retry + 1):
        try:
            if last_error and last_sql:
                yield {"type": "status", "content": f"Retrying (attempt {attempt}/{max_retry})..."}
                sql = await generate_fn(
                    user_message, schema_context, error=last_error, failed_sql=last_sql
                )
            else:
                sql = await generate_fn(user_message, schema_context)

            yield {"type": "sql", "content": sql}
            yield {"type": "status", "content": "Executing query..."}

            result = await execute_fn(sql)

            error_msg = _extract_error(result)
            if error_msg:
                last_error = error_msg
                last_sql = sql
                logger.warning("MCP execution error (attempt %d): %s", attempt, last_error)
                continue

            yield {"type": "result_data", "data": result, "sql": sql}
            return

        except Exception as e:
            last_error = str(e)
            last_sql = last_sql or ""
            logger.warning("Generation/execution error (attempt %d): %s", attempt, e)
            continue

    raise MaxRetriesExceeded(
        f"Failed after {max_retry} attempts. Last error: {last_error}"
    )
