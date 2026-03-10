"""Rule-based tool router for discovery/metadata questions.

Maps natural-language meta queries to specific MCP catalog tools,
bypassing the LLM entirely for predictable discovery patterns.
"""

import re
import logging
from dataclasses import dataclass
from typing import Optional

logger = logging.getLogger(__name__)


@dataclass
class ToolRoute:
    """A resolved MCP tool call with extracted parameters."""
    tool_name: str
    arguments: dict


# ── Pattern Definitions ──────────────────────────────────────────
# Ordered from most specific to least specific.

# Q2: "What namespaces/schemas exist?"
_LIST_NAMESPACES = re.compile(
    r"(?i)\b(what|which|list|show)\b.*\b(namespace|schema|catalog)s?\b"
)

# Q4: "Which tables have geometry columns?"
_GEOMETRY_ONLY = re.compile(
    r"(?i)\b(which|what|find|list|show)\b.*\b(table|dataset|layer)s?\b.*"
    r"\b(geometry|geom|spatial|geographic)\b"
)

# Q3/Q7: "Describe the schema of X" / "What columns does X have?"
_DESCRIBE_TABLE = re.compile(
    r"(?i)("
    r"\b(describe|explain)\b.*\b(schema|table|structure)\b"
    r"|\b(what|which|list|show)\b.*\b(column|field|attribute)s?\b"
    r"|\b(column|field|attribute)s?\b.*\b(of|for|in)\b"
    r")"
)

# Q5a: "Are there any tables with a <column> column?" (explicit "column" word)
_COLUMN_SEARCH = re.compile(
    r"(?i)\b(table|dataset|layer)s?\b.*"
    r"\b(with|having|contain(?:s|ing)?|has|have)\b.*"
    r"\b(column|field|attribute)\b"
)

# Q5b: "Which tables have timestamps?" (implied column search — no "column" word)
_IMPLICIT_COLUMN_SEARCH = re.compile(
    r"(?i)\b(which|what|are there|do)\b.*\b(table|dataset|layer)s?\b.*"
    r"\b(have|has|with|contain(?:s|ing)?)\b\s+(\w+)"
)

# Q6: "Find tables related to <topic>"
_TABLE_SEARCH = re.compile(
    r"(?i)\b(find|search|look\s+for|locate)\b.*\b(table|dataset|layer)s?\b.*"
    r"\b(related|about|for|named|called|matching|like)\b"
)

# Q1: "What datasets/tables are available?" (broadest — checked last)
_LIST_TABLES = re.compile(
    r"(?i)("
    r"\b(what|which|list|show|display)\b.*\b(dataset|table|layer|data)s?\b"
    r"|\bwhat('s| is)\b.*(available|in the (database|catalog|lakehouse))"
    r")"
)


def match(message: str, known_tables: list[dict]) -> Optional[ToolRoute]:
    """Match a user message to an MCP catalog tool.

    Args:
        message: The user's natural-language query.
        known_tables: Table dicts from SchemaBuilder cache, each with
                      keys: namespace, name, full_name.

    Returns:
        A ToolRoute if a confident match is found, or None to fall
        back to the default meta behavior.
    """
    # Most specific first

    if _LIST_NAMESPACES.search(message):
        return ToolRoute("list_namespaces", {})

    if _GEOMETRY_ONLY.search(message):
        return ToolRoute("search_tables", {"geometry_only": True})

    if _DESCRIBE_TABLE.search(message):
        table_ref = _extract_table_ref(message, known_tables)
        if table_ref:
            return ToolRoute("describe_table", {"table": table_ref})
        # No table name found — fall through to other patterns

    if _COLUMN_SEARCH.search(message):
        column = _extract_column_name(message)
        if column:
            return ToolRoute("search_tables", {"column_pattern": column})

    # Implicit column search: "which tables have timestamps?"
    m = _IMPLICIT_COLUMN_SEARCH.search(message)
    if m:
        candidate = m.group(4).rstrip("?").lower()
        if candidate not in _STOP_WORDS:
            return ToolRoute("search_tables", {"column_pattern": candidate})

    if _TABLE_SEARCH.search(message):
        pattern = _extract_search_pattern(message)
        if pattern:
            return ToolRoute("search_tables", {"pattern": pattern})

    if _LIST_TABLES.search(message):
        return ToolRoute("list_tables", {})

    return None


# ── Parameter Extraction ─────────────────────────────────────────

_THE_TABLE_RE = re.compile(r"(?i)\bthe\s+(\w+)\s+table\b")
_OF_TABLE_RE = re.compile(r"(?i)\b(?:of|for|in)\s+(?:the\s+)?(\w+)\b(?:\s+table)?\s*$")
_QUOTED_RE = re.compile(r"""['"`](\w+)['"`]""")

# Words that are not table names
_STOP_WORDS = {
    "the", "a", "an", "all", "any", "some", "every", "each",
    "table", "tables", "dataset", "datasets", "layer", "layers",
    "schema", "column", "columns", "field", "fields", "attribute",
    "describe", "show", "list", "what", "which", "find", "search",
    "does", "do", "is", "are", "have", "has", "with", "in", "of", "for",
    "me", "my", "this", "that", "data", "available", "exist",
    "geometry", "geom", "spatial",
}


def _extract_table_ref(
    message: str, known_tables: list[dict]
) -> Optional[str]:
    """Extract and resolve a table name from the message.

    Returns "namespace.table" format as expected by MCP tools.
    """
    msg_lower = message.lower()

    # 0. Check for explicit "namespace.table" reference in the message
    explicit = re.search(r"\b(\w+)\.(\w+)\b", message)
    if explicit:
        ns, tbl = explicit.group(1), explicit.group(2)
        # Verify it's a known table (not e.g. "e.g.")
        for t in known_tables:
            if t["namespace"].lower() == ns.lower() and t["name"].lower() == tbl.lower():
                return f"{t['namespace']}.{t['name']}"

    # 1. Match against known table names (most reliable)
    for t in known_tables:
        name_lower = t["name"].lower()
        if re.search(rf"\b{re.escape(name_lower)}\b", msg_lower):
            return f"{t['namespace']}.{t['name']}"

    # 2. Try quoted/backticked name
    quoted = _QUOTED_RE.search(message)
    if quoted:
        candidate = quoted.group(1)
        return f"default.{candidate}"

    # 3. "the X table" pattern
    m = _THE_TABLE_RE.search(message)
    if m:
        candidate = m.group(1).lower()
        if candidate not in _STOP_WORDS:
            return f"default.{m.group(1)}"

    # 4. "schema/columns of X" pattern
    m = _OF_TABLE_RE.search(message.strip().rstrip("?"))
    if m:
        candidate = m.group(1).lower()
        if candidate not in _STOP_WORDS:
            return f"default.{m.group(1)}"

    return None


def _extract_column_name(message: str) -> Optional[str]:
    """Extract a column name from "tables with a X column" patterns."""
    # "with a <word> column/field"
    m = re.search(
        r"(?i)\b(?:with|having|contain(?:ing)?|has|have)\s+"
        r"(?:a\s+)?['\"]?(\w+)['\"]?\s+(?:column|field|attribute)\b",
        message,
    )
    if m and m.group(1).lower() not in _STOP_WORDS:
        return m.group(1)

    # "column/field named/called <word>"
    m = re.search(
        r"(?i)\b(?:column|field|attribute)\s+(?:named|called)\s+"
        r"['\"]?(\w+)['\"]?",
        message,
    )
    if m:
        return m.group(1)

    return None


def _extract_search_pattern(message: str) -> Optional[str]:
    """Extract search keyword from "tables related to X" patterns."""
    # Anchor after the table/dataset/layer mention to avoid matching
    # "search for" as "for <table>" instead of "about <topic>"
    m = re.search(
        r"(?i)\b(?:table|dataset|layer)s?\b.*"
        r"\b(?:related\s+to|about|for|named|called|matching|like)\s+"
        r"['\"]?(\w+)['\"]?",
        message,
    )
    if m and m.group(1).lower() not in _STOP_WORDS:
        return m.group(1)

    return None


# ── Result Formatting ────────────────────────────────────────────


def format_result(tool_name: str, result: dict) -> str:
    """Format MCP tool result as human-readable text."""
    if result.get("error"):
        msg = result.get("message", result.get("error", "Unknown error"))
        return f"Error: {msg}"

    formatter = _FORMATTERS.get(tool_name, _format_generic)
    return formatter(result)


def _format_list_namespaces(result: dict) -> str:
    namespaces = result.get("namespaces", [])
    if not namespaces:
        return "No namespaces found in the catalog."
    lines = [f"**{len(namespaces)} namespace(s) in the catalog:**"]
    for ns in namespaces:
        lines.append(f"  - `{ns}`")
    return "\n".join(lines)


def _format_list_tables(result: dict) -> str:
    rows = result.get("rows", [])
    if not rows:
        return "No tables found in the catalog."
    lines = [f"**{len(rows)} table(s) available:**"]
    for row in rows:
        schema = row.get("schema_name", "default")
        name = row.get("table_name", "unknown")
        col_count = row.get("column_count", "?")
        lines.append(f"  - `{schema}.{name}` ({col_count} columns)")
    return "\n".join(lines)


def _format_describe_table(result: dict) -> str:
    rows = result.get("rows", [])
    if not rows:
        return "No column information available for this table."
    lines = [f"**Table schema ({len(rows)} columns):**"]
    for row in rows:
        col_name = row.get("column_name", "?")
        col_type = row.get("column_type", "?")
        is_geom = row.get("is_geometry", False)
        geom_marker = " (geometry)" if is_geom else ""
        lines.append(f"  - `{col_name}` {col_type}{geom_marker}")
    return "\n".join(lines)


def _format_search_tables(result: dict) -> str:
    rows = result.get("rows", [])
    if not rows:
        return "No matching tables found."
    lines = [f"**{len(rows)} matching table(s):**"]
    for row in rows:
        schema = row.get("schema_name", "default")
        name = row.get("table_name", "unknown")
        has_geom = row.get("has_geometry", False)
        geom_tag = " [spatial]" if has_geom else ""
        col_names = row.get("column_names", [])
        col_preview = ", ".join(col_names[:6])
        if len(col_names) > 6:
            col_preview += f", ... ({len(col_names)} total)"
        lines.append(f"  - `{schema}.{name}`{geom_tag}: {col_preview}")
    return "\n".join(lines)


def _format_generic(result: dict) -> str:
    row_count = result.get("row_count", 0)
    return f"Query returned {row_count} result(s)."


_FORMATTERS = {
    "list_namespaces": _format_list_namespaces,
    "list_tables": _format_list_tables,
    "describe_table": _format_describe_table,
    "search_tables": _format_search_tables,
}
