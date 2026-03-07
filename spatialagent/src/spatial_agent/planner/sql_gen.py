import re


class ExtractionError(Exception):
    pass


class ValidationError(Exception):
    pass


def extract_sql(llm_response: str) -> str:
    # Try fenced SQL blocks first
    match = re.search(r"```sql\s*\n(.*?)```", llm_response, re.DOTALL | re.IGNORECASE)
    if match:
        return match.group(1).strip()

    # Try generic fenced blocks
    match = re.search(r"```\s*\n(.*?)```", llm_response, re.DOTALL)
    if match:
        candidate = match.group(1).strip()
        if re.match(r"(?i)(SELECT|WITH)\b", candidate):
            return candidate

    # Fall back to lines starting with SELECT or WITH
    lines = llm_response.strip().splitlines()
    sql_lines = []
    capturing = False
    for line in lines:
        stripped = line.strip()
        if not capturing and re.match(r"(?i)(SELECT|WITH)\b", stripped):
            capturing = True
        if capturing:
            sql_lines.append(line)

    if sql_lines:
        result = "\n".join(sql_lines).strip().rstrip(";") + ";"
        return result

    raise ExtractionError("No SQL found in LLM response")


_DISALLOWED = re.compile(
    r"\b(INSERT|UPDATE|DELETE|DROP|ALTER|TRUNCATE|GRANT|REVOKE)\b",
    re.IGNORECASE,
)


def validate_sql(sql: str, known_tables: list[str] | None = None) -> None:
    stripped = sql.strip().rstrip(";").strip()

    if not re.match(r"(?i)(SELECT|WITH)\b", stripped):
        raise ValidationError("Query must start with SELECT or WITH")

    if _DISALLOWED.search(stripped):
        raise ValidationError("Query contains disallowed statement type")

    # Check balanced parentheses
    depth = 0
    for ch in stripped:
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        if depth < 0:
            raise ValidationError("Unmatched closing parenthesis")
    if depth != 0:
        raise ValidationError("Unmatched opening parenthesis")


async def generate_sql(message: str, schema_context: str, llm_client, model: str) -> str:
    from .prompts import build_spatial_prompt

    messages = build_spatial_prompt(schema_context, message)
    response = await llm_client.generate(messages, model)
    sql = extract_sql(response)
    validate_sql(sql)
    return sql
