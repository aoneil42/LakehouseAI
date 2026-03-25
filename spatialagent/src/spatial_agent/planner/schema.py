import logging
import time

from ..config import settings
from ..session import SessionState

logger = logging.getLogger(__name__)

SCHEMA_CACHE_TTL = 300  # 5 minutes

# Columns likely to hold categorical values that help the LLM pick the right table.
SAMPLE_COLS = {"class", "subtype", "basic_category", "type", "category"}


def _is_error(result: dict) -> bool:
    """Check if MCP result is an error (error can be bool True or a string)."""
    return result.get("error") is True or (
        isinstance(result.get("error"), str) and result["error"]
    )


class SchemaBuilder:
    def __init__(self, mcp_client):
        self.mcp = mcp_client

    async def build_context(
        self,
        message: str,
        session: SessionState,
        active_namespaces: list[str] | None = None,
    ) -> str:
        now = time.time()
        all_tables = await self._get_tables(session, now)
        # Exclude scratch/temp tables from LLM context
        tables = [
            t for t in all_tables
            if not t["namespace"].startswith(settings.scratch_prefix)
        ]
        if not tables:
            return "No tables available."

        # If the webmap has active layers, prefer those namespaces
        if active_namespaces:
            ns_tables = [
                t for t in tables if t["namespace"] in active_namespaces
            ]
            if ns_tables:
                matched = ns_tables
            else:
                matched = self._match_tables(message, tables)
                if not matched:
                    matched = tables
        else:
            matched = self._match_tables(message, tables)
            if not matched:
                matched = tables  # fall back to all non-scratch tables

        lines = ["Available tables:"]
        for table_info in matched:
            full_name = table_info["full_name"]
            desc = await self._describe(full_name, session, now)
            if desc:
                # MCP returns rows with column_name/column_type keys
                columns = desc.get("rows", desc.get("columns", []))
                cols = ", ".join(
                    f"{c.get('column_name', c.get('name', '?'))} "
                    f"{c.get('column_type', c.get('type', '?'))}"
                    for c in columns
                )
                line = f"  {full_name} ({cols})"

                # Fetch table stats for row count + geometry types
                stats = await self._get_stats(full_name, session, now)
                if stats:
                    row_count = stats.get("row_count")
                    if row_count is not None:
                        line += f"\n    → rows: {row_count:,}"

                geom_cols = [
                    c.get("column_name", c.get("name", ""))
                    for c in columns
                    if c.get("is_geometry")
                    or "geom" in c.get("column_type", c.get("type", "")).lower()
                ]
                if geom_cols:
                    # Geometry type from stats (e.g. POLYGON, POINT, LINESTRING)
                    geom_type_str = ""
                    if stats:
                        geom_types = stats.get("geometry_types", [])
                        if geom_types:
                            type_strs = [
                                gt.get("geom_type", "?") for gt in geom_types
                            ]
                            geom_type_str = f" [{', '.join(type_strs)}]"
                    bbox = await self._get_bbox(full_name)
                    bbox_str = f" | bbox: {bbox}" if bbox else ""
                    line += (
                        f"\n    → geometry: {', '.join(geom_cols)}"
                        f"{geom_type_str}{bbox_str}"
                    )
                # Sample categorical columns to help LLM pick the right table
                for c in columns:
                    col_name = c.get("column_name", c.get("name", ""))
                    if col_name.lower() in SAMPLE_COLS:
                        values = await self._sample_values(
                            full_name, col_name, session, now
                        )
                        if values:
                            line += f"\n    → {col_name} values: {', '.join(values)}"
                lines.append(line)
            else:
                lines.append(f"  {full_name}")

        return "\n".join(lines)

    async def _get_tables(self, session: SessionState, now: float) -> list:
        cache_key = "_tables"
        if (
            cache_key in session.schema_cache
            and now - session.schema_cache_ts < SCHEMA_CACHE_TTL
        ):
            return session.schema_cache[cache_key]

        result = await self.mcp.call_tool("list_tables", {})
        if _is_error(result):
            logger.warning("Failed to list tables: %s", result.get("message", result))
            return session.schema_cache.get(cache_key, [])

        tables = []
        # spatial-lakehouse-mcp format: rows with schema_name + table_name
        for row in result.get("rows", []):
            schema = row.get("schema_name", "default")
            name = row.get("table_name", "")
            tables.append({
                "namespace": schema,
                "name": name,
                "full_name": f"lakehouse.{schema}.{name}",
            })

        session.schema_cache[cache_key] = tables
        session.schema_cache_ts = now
        return tables

    async def _describe(self, full_name: str, session: SessionState, now: float) -> dict:
        cache_key = f"_desc_{full_name}"
        if (
            cache_key in session.schema_cache
            and now - session.schema_cache_ts < SCHEMA_CACHE_TTL
        ):
            return session.schema_cache[cache_key]

        result = await self.mcp.call_tool("describe_table", {"table": full_name})
        if not _is_error(result):
            session.schema_cache[cache_key] = result
            return result
        return {}

    async def _get_stats(
        self, full_name: str, session: SessionState, now: float
    ) -> dict:
        """Fetch table stats (row count, geometry types) with caching."""
        cache_key = f"_stats_{full_name}"
        if (
            cache_key in session.schema_cache
            and now - session.schema_cache_ts < SCHEMA_CACHE_TTL
        ):
            return session.schema_cache[cache_key]

        result = await self.mcp.call_tool("table_stats", {"table": full_name})
        if not _is_error(result):
            session.schema_cache[cache_key] = result
            return result
        return {}

    async def _get_bbox(self, full_name: str) -> list | None:
        result = await self.mcp.call_tool("get_bbox", {"table": full_name})
        if not _is_error(result):
            return result.get("bbox")
        return None

    async def _sample_values(
        self, full_name: str, col_name: str, session: SessionState, now: float
    ) -> list[str]:
        """Return up to 15 distinct values for a categorical column."""
        cache_key = f"_sample_{full_name}.{col_name}"
        if (
            cache_key in session.schema_cache
            and now - session.schema_cache_ts < SCHEMA_CACHE_TTL
        ):
            return session.schema_cache[cache_key]

        sql = (
            f'SELECT DISTINCT "{col_name}" FROM {full_name} '
            f'WHERE "{col_name}" IS NOT NULL LIMIT 15'
        )
        result = await self.mcp.call_tool("query", {"sql": sql})
        values: list[str] = []
        if not _is_error(result):
            for row in result.get("rows", []):
                v = row.get(col_name)
                if v is not None:
                    values.append(str(v))
        session.schema_cache[cache_key] = values
        return values

    def _match_tables(self, message: str, tables: list) -> list:
        words = set(message.lower().split())
        matched = []
        for t in tables:
            name_parts = set(t["name"].lower().replace("_", " ").split())
            ns_parts = set(t["namespace"].lower().replace("_", " ").split())
            if words & (name_parts | ns_parts):
                matched.append(t)
        return matched
