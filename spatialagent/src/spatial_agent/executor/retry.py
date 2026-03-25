import logging
import re
from typing import AsyncGenerator

logger = logging.getLogger(__name__)


class MaxRetriesExceeded(Exception):
    pass


# ── Spatial error pattern → fix hint mapping ───────────────────
# Each entry: (regex pattern on error message, hint to append)

_ERROR_HINTS = [
    # WKB/GEOMETRY type mismatch — most common error
    (
        re.compile(
            r"No function matches.*ST_\w+\(BLOB",
            re.IGNORECASE,
        ),
        "FIX: Geometry columns are stored as WKB BLOB, not native GEOMETRY. "
        "Wrap every geometry column with ST_GeomFromWKB() before passing to "
        "spatial functions. Example: ST_Intersects(ST_GeomFromWKB(a.geometry), "
        "ST_GeomFromWKB(b.geometry)). Do NOT wrap geometry in SELECT — keep "
        "the raw BLOB column for map visualization.",
    ),
    # Column not found — wrong column name
    (
        re.compile(
            r'Referenced column "(\w+)" not found',
            re.IGNORECASE,
        ),
        "FIX: The column name does not exist in this table. Check the schema "
        "context above for the correct column names. Common mistakes: "
        "'type' should be 'class' or 'subtype', 'geom' should be 'geometry', "
        "'name' should be 'names'.",
    ),
    # GROUP BY mismatch — column must appear in GROUP BY
    (
        re.compile(
            r'must appear in the GROUP BY clause|not in aggregate',
            re.IGNORECASE,
        ),
        "FIX: When using GROUP BY, all non-aggregated columns in SELECT must "
        "be in the GROUP BY clause. Either add the missing column to GROUP BY "
        "or wrap it in an aggregate function. Avoid SELECT * with GROUP BY — "
        "list columns explicitly.",
    ),
    # Ambiguous column reference — needs table alias
    (
        re.compile(
            r'Ambiguous reference to column',
            re.IGNORECASE,
        ),
        "FIX: Use table aliases to disambiguate. Example: SELECT b.id, z.id "
        "FROM buildings b JOIN zones z ON ...",
    ),
    # Subquery returned more than 1 row
    (
        re.compile(
            r'Subquery returned more than 1 row',
            re.IGNORECASE,
        ),
        "FIX: The subquery returns multiple rows but is used where a single "
        "value is expected. Add LIMIT 1 to the subquery, or use a JOIN instead.",
    ),
    # ST_Transform / CRS errors
    (
        re.compile(
            r'ST_Transform|Invalid.*CRS|Invalid.*SRID|Unknown.*spatial.*reference',
            re.IGNORECASE,
        ),
        "FIX: Use standard CRS identifiers. For WGS84: 'EPSG:4326'. "
        "For Web Mercator (meters): 'EPSG:3857'. Transform pattern: "
        "ST_Transform(ST_GeomFromWKB(geom), 'EPSG:4326', 'EPSG:3857').",
    ),
    # Empty result on proximity (row_count = 0)
    (
        re.compile(
            r'0 rows|empty result|no results',
            re.IGNORECASE,
        ),
        "FIX: The query returned no results. If this is a distance/proximity "
        "query, the distance threshold may be too small. Remember coordinates "
        "are in EPSG:4326 (degrees). Convert meters to degrees: "
        "meters / 111320.0. Example: 500m = 500.0/111320.0 degrees.",
    ),
    # Parser error — often from malformed SQL
    (
        re.compile(
            r'Parser Error.*syntax error',
            re.IGNORECASE,
        ),
        "FIX: Check SQL syntax. Common issues: missing commas between "
        "SELECT columns, unmatched parentheses, missing JOIN keyword, "
        "or stray semicolons. DuckDB uses standard SQL syntax.",
    ),
]


def _get_error_hint(error_msg: str) -> str:
    """Match an error message against known spatial patterns and return a fix hint."""
    for pattern, hint in _ERROR_HINTS:
        if pattern.search(error_msg):
            return f"\n\n{hint}"
    return ""


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
                hint = _get_error_hint(error_msg)
                last_error = error_msg + hint
                last_sql = sql
                logger.warning("MCP execution error (attempt %d): %s", attempt, error_msg)
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
