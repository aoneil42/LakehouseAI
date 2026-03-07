import re


def generate_result_name(sql: str) -> str:
    # Extract the main table name from SQL
    match = re.search(r"(?i)\bFROM\s+\S*\.(\w+)", sql)
    table = match.group(1) if match else "result"

    # Detect operation hints
    if re.search(r"(?i)\bST_DWithin\b", sql):
        return f"{table}_nearby"
    if re.search(r"(?i)\bST_Buffer\b", sql):
        return f"{table}_buffered"
    if re.search(r"(?i)\bST_Intersects\b", sql):
        return f"{table}_intersected"
    if re.search(r"(?i)\bST_Contains\b", sql):
        return f"{table}_contained"
    if re.search(r"(?i)\bJOIN\b", sql):
        return f"{table}_joined"

    return f"{table}_query"


def pick_tool(
    sql: str, should_materialize: bool, session_id: str
) -> tuple[str, dict]:
    if should_materialize:
        return "materialize_result", {
            "sql": sql,
            "result_name": generate_result_name(sql),
            "namespace": f"_scratch_{session_id.replace('-', '')[:8]}",
            "overwrite": True,
        }
    else:
        return "query", {"sql": sql, "limit": 100}
