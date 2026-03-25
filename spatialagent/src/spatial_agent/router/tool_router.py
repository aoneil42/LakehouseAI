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
    format_hint: str = ""


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

# Q8/Q11: "Show me a sample" / "Preview the first 5 rows"
_SAMPLE_DATA = re.compile(
    r"(?i)("
    r"\b(show|give|display)\b.*\b(sample|preview)\b"
    r"|\b(sample|preview)\b.*\b(data|rows?|records?|table|dataset|layer)\b"
    r"|\bfirst\s+\d+\s+(rows?|records?|features?)\b"
    r")"
)

# Extract row count: "first 5 rows", "sample of 20", "10 rows"
_SAMPLE_N_RE = re.compile(r"(?i)(?:first|top|sample\s+(?:of\s+)?)?(\d+)\s*(?:rows?|records?|features?|entries)")

# Detect "without geometry" / "exclude geometry"
_NO_GEOM_RE = re.compile(r"(?i)\b(without|no|exclude|excluding|skip|omit)\b.*\b(geom|geometry|spatial)\b")

# Q12: "What types of geometries are in X?" (geometry types only — not full stats)
_GEOMETRY_TYPES = re.compile(
    r"(?i)\b(what|which)\b.*\b(type|kind)s?\b.*\bgeometr"
)

# Q9/Q10: "How many records" / "Summarize" / "Statistics"
_TABLE_STATS = re.compile(
    r"(?i)("
    r"\b(how many|count|number of)\b.*\b(record|row|feature|point|line|polygon|entri)s?\b"
    r"|\b(summarize|summary|statistics|stats)\b"
    r"|\brow\s+count\b"
    r")"
)

# Q13/Q14: "Bounding box" / "Geographic area" / "Spatial extent" / "Coverage"
_GET_BBOX = re.compile(
    r"(?i)("
    r"\b(bounding\s+box|bbox)\b"
    r"|\b(geographic|spatial)\s+(area|extent|coverage|bounds)\b"
    r"|\b(what|which)\b.*\b(area|extent|coverage|region)\b.*\b(cover|span|encompass)\b"
    r"|\b(what|which|where)\b.*\b(area|extent|region)\b.*\b(does|do)\b"
    r"|\b(area|extent|coverage|bounds)\b.*\b(of|for)\b.*\b(the|all)\b"
    r")"
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

# Temporal: "What snapshots exist for X?" / "Show history of X"
_TABLE_SNAPSHOTS = re.compile(
    r"(?i)("
    r"\b(what|which|list|show)\b.*\b(snapshots?|versions?|history)\b"
    r"|\b(snapshots?|versions?)\b.*\b(exist|available|for|of)\b"
    r")"
)

# Time travel: "Show X as it was at snapshot Y" / "X as of timestamp"
_TIME_TRAVEL = re.compile(
    r"(?i)("
    r"\b(as\s+it\s+was|as\s+of|look(?:ed)?\s+like|at\s+snapshot|at\s+the\s+earliest)\b"
    r"|\b(time\s+travel|historical|previous\s+version)\b"
    r"|\b(data|table)\b.*\b(at|on|from)\b.*\b(\d{4}[-/]\d{2}|\bsnapshot\s+\d+)\b"
    r"|\b(what\s+did)\b.*\b(look\s+like|as\s+of)\b.*\b(on|at|in)\b"
    r")"
)

# Export: "Export X as GeoJSON"
_EXPORT_GEOJSON = re.compile(
    r"(?i)\b(export|download)\b.*\b(geojson|geo\s*json)\b"
)

# Q1: "What datasets/tables are available?" (broadest — checked last)
_LIST_TABLES = re.compile(
    r"(?i)("
    r"\b(what|which|list|show|display)\b.*\b(dataset|table|layer|data)s?\b"
    r"|\bwhat('s| is)\b.*(available|in the (database|catalog|lakehouse))"
    r")"
)


def match(message: str, known_tables: list[dict],
          active_namespaces: list[str] | None = None) -> Optional[ToolRoute]:
    """Match a user message to an MCP catalog tool.

    Args:
        message: The user's natural-language query.
        known_tables: Table dicts from SchemaBuilder cache, each with
                      keys: namespace, name, full_name.
        active_namespaces: Optional list of active namespace names from
                          the webmap layer selection.

    Returns:
        A ToolRoute if a confident match is found, or None to fall
        back to the default meta behavior.
    """
    # Most specific first

    if _LIST_NAMESPACES.search(message):
        return ToolRoute("list_namespaces", {})

    if _GEOMETRY_ONLY.search(message):
        return ToolRoute("search_tables", {"geometry_only": True})

    # Q13/Q14: Bounding box / geographic extent
    if _GET_BBOX.search(message):
        table_ref = _extract_table_ref(message, known_tables)
        if table_ref:
            where = _extract_where_clause(message)
            args = {"table": table_ref}
            if where:
                args["where"] = where
            return ToolRoute("get_bbox", args)
        return None  # Matched pattern but no table — LLM fallback

    # Q12: Geometry types — return only geometry type info
    if _GEOMETRY_TYPES.search(message):
        table_ref = _extract_table_ref(message, known_tables)
        if table_ref:
            return ToolRoute("table_stats", {"table": table_ref},
                             format_hint="geometry_types")
        # Namespace-level: "geometries in paris datasets"
        ns = _extract_namespace_ref(message, known_tables)
        if ns:
            tables = [
                f"{t['namespace']}.{t['name']}"
                for t in known_tables if t["namespace"] == ns
            ]
            return ToolRoute("table_stats_multi", {"tables": tables},
                             format_hint="geometry_types")
        return None

    # Q9/Q10: Table stats / row count / summarize
    if _TABLE_STATS.search(message):
        table_ref = _extract_table_ref(message, known_tables)
        if table_ref:
            return ToolRoute("table_stats", {"table": table_ref})
        return None  # Matched pattern but no table — LLM fallback

    # Q8/Q11: Sample data / preview
    if _SAMPLE_DATA.search(message):
        table_ref = _extract_table_ref(message, known_tables)
        if table_ref:
            args = {"table": table_ref}
            n = _extract_sample_n(message)
            if n:
                args["n"] = n
            if _NO_GEOM_RE.search(message):
                args["include_geometry"] = False
            return ToolRoute("sample_data", args)
        return None  # Matched pattern but no table — LLM fallback

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

    # Temporal: snapshots and time travel (before list_tables to avoid false match)
    if _TIME_TRAVEL.search(message):
        table_ref = _extract_table_ref(message, known_tables)
        if table_ref:
            snapshot_id = _extract_snapshot_id(message)
            timestamp = _extract_timestamp(message)
            args = {"table": table_ref, "sql_select": "SELECT *"}
            if snapshot_id:
                args["snapshot_id"] = snapshot_id
            elif timestamp:
                args["timestamp"] = timestamp
            else:
                # "as it was at the earliest" — need snapshots first
                return ToolRoute("table_snapshots", {"table": table_ref})
            return ToolRoute("time_travel_query", args)
        return None

    if _TABLE_SNAPSHOTS.search(message):
        table_ref = _extract_table_ref(message, known_tables)
        if table_ref:
            return ToolRoute("table_snapshots", {"table": table_ref})
        return None

    # Export GeoJSON
    if _EXPORT_GEOJSON.search(message):
        table_ref = _extract_table_ref(message, known_tables)
        if not table_ref:
            # Infer table from feature type keywords
            table_ref = _infer_table_from_features(
                message, known_tables, active_namespaces
            )
        if table_ref:
            args = {"table": table_ref}
            where = _extract_where_clause(message)
            if where:
                args["where"] = where
            else:
                # Try to extract feature type filter: "just the schools" → "school"
                feature_filter = _extract_feature_type_filter(message)
                if feature_filter:
                    args["where"] = feature_filter
            # Detect column selection hints
            columns = _extract_export_columns(message)
            if columns:
                args["columns"] = columns
            return ToolRoute("export_geojson", args,
                             format_hint="export")
        return None

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


def _extract_namespace_ref(
    message: str, known_tables: list[dict]
) -> Optional[str]:
    """Extract a namespace reference from the message.

    Returns the namespace name if mentioned, or None.
    """
    msg_lower = message.lower()
    namespaces = {t["namespace"].lower(): t["namespace"] for t in known_tables}
    for ns_lower, ns in namespaces.items():
        if ns_lower.startswith("_"):
            continue  # Skip scratch namespaces
        if re.search(rf"\b{re.escape(ns_lower)}\b", msg_lower):
            return ns
    return None


def _extract_sample_n(message: str) -> Optional[int]:
    """Extract sample row count from message."""
    m = _SAMPLE_N_RE.search(message)
    if m:
        n = int(m.group(1))
        return max(1, min(n, 100))
    return None


def _extract_where_clause(message: str) -> Optional[str]:
    """Extract a simple WHERE filter like 'type = residential'."""
    # "all residential buildings" → type = 'residential'
    # "where type = 'residential'" → type = 'residential'
    m = re.search(
        r"(?i)\bwhere\s+(.+?)(?:\s*$)",
        message.rstrip("?"),
    )
    if m:
        return m.group(1).strip()
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


def _extract_snapshot_id(message: str) -> Optional[int]:
    """Extract snapshot ID from 'at snapshot 12345'."""
    m = re.search(r"(?i)\bsnapshot\s+(\d+)\b", message)
    if m:
        return int(m.group(1))
    return None


def _extract_timestamp(message: str) -> Optional[str]:
    """Extract a date/timestamp from the message for time travel."""
    # "on March 1, 2026" / "on 2025-06-15"
    # ISO format
    m = re.search(r"\b(\d{4}-\d{2}-\d{2})\b", message)
    if m:
        return f"{m.group(1)} 00:00:00"
    # "March 1, 2026" / "June 15, 2025"
    m = re.search(
        r"(?i)\b(january|february|march|april|may|june|july|august|"
        r"september|october|november|december)\s+(\d{1,2}),?\s+(\d{4})\b",
        message,
    )
    if m:
        month_names = {
            "january": "01", "february": "02", "march": "03", "april": "04",
            "may": "05", "june": "06", "july": "07", "august": "08",
            "september": "09", "october": "10", "november": "11", "december": "12",
        }
        month = month_names[m.group(1).lower()]
        day = m.group(2).zfill(2)
        year = m.group(3)
        return f"{year}-{month}-{day} 00:00:00"
    return None


def _infer_table_from_features(
    message: str, known_tables: list[dict],
    active_namespaces: list[str] | None = None,
) -> Optional[str]:
    """Infer a table reference from feature-type keywords.

    Maps common feature types to likely tables (buildings, places, etc.)
    when no explicit table name is mentioned.
    """
    msg_lower = message.lower()
    # Feature types that typically live in a buildings table
    _BUILDING_FEATURES = {
        "school", "hospital", "church", "house", "apartment",
        "office", "warehouse", "garage", "shed", "commercial",
        "industrial", "residential", "kindergarten",
    }
    # Feature types that typically live in a places/POI table
    _PLACE_FEATURES = {
        "restaurant", "cafe", "hotel", "shop", "store", "bar",
        "pharmacy", "bank", "supermarket", "museum", "park",
    }
    # Build lookup: prefer active namespaces > non-scratch > scratch/test
    active_ns = set(ns.lower() for ns in (active_namespaces or []))
    tables_by_name: dict[str, dict] = {}
    for t in known_tables:
        key = t["name"].lower()
        ns = t["namespace"].lower()
        existing = tables_by_name.get(key)
        if not existing:
            tables_by_name[key] = t
        elif ns in active_ns and existing["namespace"].lower() not in active_ns:
            tables_by_name[key] = t  # Active namespace wins
        elif not ns.startswith("_") and existing["namespace"].lower().startswith("_"):
            tables_by_name[key] = t  # Non-scratch wins over scratch

    for feat in _BUILDING_FEATURES:
        if re.search(rf"\b{feat}s?\b", msg_lower):
            t = tables_by_name.get("buildings")
            if t:
                return f"{t['namespace']}.{t['name']}"
    for feat in _PLACE_FEATURES:
        if re.search(rf"\b{feat}s?\b", msg_lower):
            t = tables_by_name.get("places")
            if t:
                return f"{t['namespace']}.{t['name']}"
    return None


def _extract_feature_type_filter(message: str) -> Optional[str]:
    """Extract a feature type filter from natural language.

    Maps "just the schools" → "class = 'school'" or
    "only hospitals" → "class = 'hospital'"
    """
    m = re.search(
        r"(?i)\b(?:just|only|the)\s+(?:the\s+)?(\w+)\b",
        message,
    )
    if not m:
        return None
    candidate = m.group(1).lower().rstrip("s")  # "schools" → "school"
    if candidate in _STOP_WORDS or candidate in {"geojson", "export", "all"}:
        return None
    # Return as a hint — categorical column + value
    # Try common patterns: class, subtype, basic_category
    return f"class = '{candidate}' OR subtype = '{candidate}'"


def _extract_export_columns(message: str) -> Optional[str]:
    """Extract column names for export from 'with their names' patterns."""
    m = re.search(r"(?i)\bwith\s+(?:their\s+)?(\w+(?:\s*,\s*\w+)*)\b", message)
    if m:
        candidate = m.group(1).lower()
        # Filter out common non-column words
        if candidate in {"names", "name"}:
            return "names"
        if candidate not in _STOP_WORDS:
            return candidate
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


def format_result(tool_name: str, result: dict, format_hint: str = "") -> str:
    """Format MCP tool result as human-readable text."""
    if result.get("error"):
        msg = result.get("message", result.get("error", "Unknown error"))
        return f"Error: {msg}"

    if format_hint == "geometry_types":
        return _format_geometry_types(result)

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


def _format_sample_data(result: dict) -> str:
    rows = result.get("rows", [])
    if not rows:
        return "No data found in this table."
    keys = list(rows[0].keys())
    # Truncate long values (especially WKB geometry bytes)
    def _truncate(val, max_len=60):
        s = str(val)
        if isinstance(val, (bytes, bytearray)) or (
            isinstance(val, str) and s.startswith("b'\\x")
        ):
            return "(geometry)"
        if len(s) > max_len:
            return s[:max_len] + "..."
        return s
    lines = []
    lines.append("| " + " | ".join(keys) + " |")
    lines.append("| " + " | ".join("---" for _ in keys) + " |")
    for row in rows:
        vals = [_truncate(row.get(k, "")) for k in keys]
        lines.append("| " + " | ".join(vals) + " |")
    lines.append(f"\n*Showing {len(rows)} row(s)*")
    return "\n".join(lines)


def _format_table_stats(result: dict) -> str:
    if "row_count" not in result:
        return _format_generic(result)

    lines = []
    lines.append("**Table Statistics:**")
    lines.append(f"  - Row count: {result.get('row_count', '?'):,}")
    lines.append(f"  - Column count: {result.get('column_count', '?')}")

    # Geometry types — top-level list of {"geom_type": ..., "cnt": ...}
    geom_types = result.get("geometry_types", [])
    if geom_types:
        type_strs = [
            f"{gt.get('geom_type', '?')} ({gt.get('cnt', '?')})"
            for gt in geom_types
        ]
        lines.append(f"  - Geometry types: {', '.join(type_strs)}")

    # Spatial extent — top-level "spatial" dict with flat keys
    spatial = result.get("spatial", {})
    if spatial:
        null_count = spatial.get("null_geom", 0)
        non_null = spatial.get("non_null_geom", 0)
        if non_null or null_count:
            lines.append(
                f"  - Geometries: {non_null:,} valid, {null_count:,} null"
            )
        min_lon = spatial.get("min_lon")
        if min_lon is not None:
            lines.append(
                f"  - Bounding box: [{spatial.get('min_lon')}, "
                f"{spatial.get('min_lat')}, "
                f"{spatial.get('max_lon')}, "
                f"{spatial.get('max_lat')}]"
            )

    return "\n".join(lines)


def _format_get_bbox(result: dict) -> str:
    bbox = result.get("bbox") or result
    min_lon = bbox.get("min_lon", bbox.get("xmin", "?"))
    min_lat = bbox.get("min_lat", bbox.get("ymin", "?"))
    max_lon = bbox.get("max_lon", bbox.get("xmax", "?"))
    max_lat = bbox.get("max_lat", bbox.get("ymax", "?"))
    if min_lon == "?":
        return "Could not determine bounding box for this table."
    lines = [
        f"**Bounding box:** [{min_lon}, {min_lat}, {max_lon}, {max_lat}]",
    ]
    row_count = bbox.get("feature_count", bbox.get("row_count"))
    if row_count:
        lines.append(f"  - Feature count: {row_count:,}")
    return "\n".join(lines)


def _format_geometry_types(result: dict) -> str:
    """Format only geometry type info from a table_stats result."""
    geom_types = result.get("geometry_types", [])
    if not geom_types:
        return "No geometry types found in this table."
    type_strs = [
        f"{gt.get('geom_type', '?')} ({gt.get('cnt', '?'):,})"
        for gt in geom_types
    ]
    return f"**Geometry types:** {', '.join(type_strs)}"


def _format_geometry_types_multi(results: list[tuple[str, dict]]) -> str:
    """Format geometry types aggregated across multiple tables."""
    lines = ["**Geometry types by table:**"]
    for table_name, result in results:
        geom_types = result.get("geometry_types", [])
        if geom_types:
            type_strs = [
                f"{gt.get('geom_type', '?')} ({gt.get('cnt', '?'):,})"
                for gt in geom_types
            ]
            lines.append(f"  - `{table_name}`: {', '.join(type_strs)}")
        else:
            lines.append(f"  - `{table_name}`: no geometry")
    return "\n".join(lines)


def _format_table_snapshots(result: dict) -> str:
    snapshots = result.get("snapshots", result.get("rows", []))
    if not snapshots:
        return "No snapshots found for this table."
    lines = [f"**{len(snapshots)} snapshot(s):**"]
    for snap in snapshots:
        snap_id = snap.get("snapshot_id", snap.get("id", "?"))
        ts = snap.get("timestamp", snap.get("committed_at", "?"))
        parent = snap.get("parent_id", "")
        parent_str = f" (parent: {parent})" if parent else ""
        lines.append(f"  - Snapshot `{snap_id}` at {ts}{parent_str}")
    return "\n".join(lines)


def _format_time_travel(result: dict) -> str:
    rows = result.get("rows", [])
    row_count = result.get("row_count", len(rows))
    if not rows:
        return f"Time-travel query returned {row_count} row(s)."
    if row_count <= 20:
        return _format_sample_data(result)
    return f"Time-travel query returned {row_count} row(s)."


def _format_export_geojson(result: dict) -> str:
    # GeoJSON FeatureCollection: {type, features, metadata}
    metadata = result.get("metadata", {})
    if isinstance(metadata, str):
        import json as _json
        try:
            metadata = _json.loads(metadata)
        except Exception:
            metadata = {}
    feature_count = metadata.get("feature_count", 0)
    features = result.get("features", [])
    if not feature_count:
        feature_count = len(features) if isinstance(features, list) else 0
    if feature_count:
        truncated = metadata.get("truncated", False)
        note = " (truncated)" if truncated else ""
        return f"Exported {feature_count} features as GeoJSON{note}."
    if result.get("error"):
        return f"Export error: {result.get('message', result.get('error'))}"
    return "Export returned 0 features."


def _format_generic(result: dict) -> str:
    row_count = result.get("row_count", 0)
    return f"Query returned {row_count} result(s)."


_FORMATTERS = {
    "list_namespaces": _format_list_namespaces,
    "list_tables": _format_list_tables,
    "describe_table": _format_describe_table,
    "search_tables": _format_search_tables,
    "sample_data": _format_sample_data,
    "table_stats": _format_table_stats,
    "get_bbox": _format_get_bbox,
    "table_snapshots": _format_table_snapshots,
    "time_travel_query": _format_time_travel,
    "export_geojson": _format_export_geojson,
}
