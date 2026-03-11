import logging
import time

from ..config import settings
from ..session import SessionState

logger = logging.getLogger(__name__)

SCHEMA_CACHE_TTL = 300  # 5 minutes


def _is_error(result: dict) -> bool:
    """Check if MCP result is an error (error can be bool True or a string)."""
    return result.get("error") is True or (
        isinstance(result.get("error"), str) and result["error"]
    )


class SchemaBuilder:
    def __init__(self, mcp_client):
        self.mcp = mcp_client

    async def build_context(self, message: str, session: SessionState) -> str:
        now = time.time()
        all_tables = await self._get_tables(session, now)
        # Exclude scratch/temp tables from LLM context
        tables = [
            t for t in all_tables
            if not t["namespace"].startswith(settings.scratch_prefix)
        ]
        if not tables:
            return "No tables available."

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
                geom_cols = [
                    c.get("column_name", c.get("name", ""))
                    for c in columns
                    if c.get("is_geometry")
                    or "geom" in c.get("column_type", c.get("type", "")).lower()
                ]
                if geom_cols:
                    bbox = await self._get_bbox(full_name)
                    bbox_str = f" | bbox: {bbox}" if bbox else ""
                    line += f"\n    → geometry: {', '.join(geom_cols)}{bbox_str}"
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

    async def _get_bbox(self, full_name: str) -> list | None:
        result = await self.mcp.call_tool("get_bbox", {"table": full_name})
        if not _is_error(result):
            return result.get("bbox")
        return None

    def _match_tables(self, message: str, tables: list) -> list:
        words = set(message.lower().split())
        matched = []
        for t in tables:
            name_parts = set(t["name"].lower().replace("_", " ").split())
            ns_parts = set(t["namespace"].lower().replace("_", " ").split())
            if words & (name_parts | ns_parts):
                matched.append(t)
        return matched
