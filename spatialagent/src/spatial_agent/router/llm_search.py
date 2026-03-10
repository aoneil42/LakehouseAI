"""LLM-powered fuzzy search for table/column discovery.

Used when regex-based pattern matching is insufficient — the LLM
does semantic matching against the known table catalog.
"""

import json
import logging
import re
from typing import Optional

logger = logging.getLogger(__name__)

_TABLE_SEARCH_PROMPT = """\
You are a catalog search assistant. Given a list of tables in a spatial \
data lakehouse, identify which tables are relevant to the user's question.

Tables in the catalog:
{catalog}

User question: {question}

Return ONLY a JSON array of matching table references (e.g., \
["paris.transportation", "paris.roads"]). If no tables match, return []. \
No explanation."""

_COLUMN_SEARCH_PROMPT = """\
You are a catalog search assistant. Given tables with their columns, \
identify which tables have columns relevant to the user's question.

Tables and columns:
{catalog}

User question: {question}

Return ONLY a JSON object where keys are matching table references and \
values are the relevant column names (e.g., \
{{"paris.places": ["created_at", "updated_at"]}}). \
If no tables match, return {{}}. No explanation."""


def _build_table_catalog(known_tables: list[dict]) -> str:
    """Build a compact table listing for the prompt."""
    lines = []
    for t in known_tables:
        lines.append(f"- {t['namespace']}.{t['name']}")
    return "\n".join(lines)


def _build_column_catalog(
    known_tables: list[dict], schema_cache: dict
) -> str:
    """Build table + column listing from cached descriptions."""
    lines = []
    for t in known_tables:
        full_name = t["full_name"]
        cache_key = f"_desc_{full_name}"
        desc = schema_cache.get(cache_key, {})
        columns = desc.get("columns") or desc.get("rows", [])
        if columns:
            col_names = [c.get("name") or c.get("column_name", "?") for c in columns]
            cols_str = ", ".join(col_names)
            lines.append(f"- {t['namespace']}.{t['name']}: {cols_str}")
        else:
            lines.append(f"- {t['namespace']}.{t['name']}: (columns unknown)")
    return "\n".join(lines)


def _parse_table_list(response: str) -> list[str]:
    """Extract a JSON array of table refs from LLM response."""
    # Try to find JSON array in response
    m = re.search(r"\[.*?\]", response, re.DOTALL)
    if m:
        try:
            result = json.loads(m.group())
            if isinstance(result, list):
                return [str(t) for t in result]
        except json.JSONDecodeError:
            pass
    return []


def _parse_column_matches(response: str) -> dict:
    """Extract a JSON object of table→columns from LLM response."""
    m = re.search(r"\{.*\}", response, re.DOTALL)
    if m:
        try:
            result = json.loads(m.group())
            if isinstance(result, dict):
                return result
        except json.JSONDecodeError:
            pass
    return {}


async def fuzzy_table_search(
    question: str,
    known_tables: list[dict],
    llm_client,
    model: str,
) -> str:
    """Use LLM to find tables matching the user's question.

    Returns formatted markdown result string.
    """
    catalog = _build_table_catalog(known_tables)
    prompt = _TABLE_SEARCH_PROMPT.format(catalog=catalog, question=question)

    try:
        response = await llm_client.generate(
            [{"role": "user", "content": prompt}], model
        )
        matches = _parse_table_list(response)
    except Exception as e:
        logger.warning("LLM search failed: %s", e)
        return "Search failed — could not reach the language model."

    if not matches:
        return "No matching tables found."

    lines = [f"**{len(matches)} matching table(s):**"]
    for ref in matches:
        lines.append(f"  - `{ref}`")
    return "\n".join(lines)


async def fuzzy_column_search(
    question: str,
    known_tables: list[dict],
    schema_cache: dict,
    llm_client,
    model: str,
) -> str:
    """Use LLM to find tables with columns matching the user's question.

    Returns formatted markdown result string.
    """
    catalog = _build_column_catalog(known_tables, schema_cache)
    prompt = _COLUMN_SEARCH_PROMPT.format(catalog=catalog, question=question)

    try:
        response = await llm_client.generate(
            [{"role": "user", "content": prompt}], model
        )
        matches = _parse_column_matches(response)
    except Exception as e:
        logger.warning("LLM column search failed: %s", e)
        return "Search failed — could not reach the language model."

    if not matches:
        return "No tables with matching columns found."

    lines = [f"**{len(matches)} table(s) with matching columns:**"]
    for table_ref, cols in matches.items():
        if isinstance(cols, list):
            cols_str = ", ".join(f"`{c}`" for c in cols)
        else:
            cols_str = str(cols)
        lines.append(f"  - `{table_ref}`: {cols_str}")
    return "\n".join(lines)
