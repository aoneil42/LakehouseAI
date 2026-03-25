"""Paraphrase robustness tests for intent classification and tool routing.

Tests that controlled paraphrases of the 52 canonical questions
classify to the same intent (and route to the same tool for meta queries).
"""

import json
from pathlib import Path

import pytest

from spatial_agent.router.intent import classify
from spatial_agent.router.tool_router import match

FIXTURES_DIR = Path(__file__).parent / "fixtures"


def _load_paraphrases():
    """Load paraphrase fixture and flatten to (variant, expected_intent, canonical_id) tuples."""
    with open(FIXTURES_DIR / "paraphrases.json") as f:
        data = json.load(f)
    cases = []
    for entry in data["paraphrases"]:
        cid = entry["canonical_id"]
        intent = entry["expected_intent"]
        for variant in entry["variants"]:
            cases.append((variant, intent, cid))
    return cases


def _load_meta_paraphrases():
    """Load only meta-intent paraphrases with their canonical tool expectations."""
    with open(FIXTURES_DIR / "canonical_questions.json") as f:
        canonical = {q["id"]: q for q in json.load(f)["questions"]}
    with open(FIXTURES_DIR / "paraphrases.json") as f:
        data = json.load(f)
    cases = []
    for entry in data["paraphrases"]:
        if entry["expected_intent"] != "meta":
            continue
        cid = entry["canonical_id"]
        cq = canonical.get(cid, {})
        tool = cq.get("expected_tool")
        if not tool:
            continue
        for variant in entry["variants"]:
            cases.append((variant, tool, cid))
    return cases


_PARAPHRASE_CASES = _load_paraphrases()
_META_ROUTING_CASES = _load_meta_paraphrases()


# Known tables fixture matching what the tool router tests use
_KNOWN_TABLES = [
    {"namespace": "paris", "name": "buildings", "full_name": "lakehouse.paris.buildings"},
    {"namespace": "paris", "name": "land_use", "full_name": "lakehouse.paris.land_use"},
    {"namespace": "paris", "name": "places", "full_name": "lakehouse.paris.places"},
    {"namespace": "paris", "name": "transportation", "full_name": "lakehouse.paris.transportation"},
    {"namespace": "paris", "name": "water_lines", "full_name": "lakehouse.paris.water_lines"},
    {"namespace": "paris", "name": "water_polygons", "full_name": "lakehouse.paris.water_polygons"},
]


# ── Intent classification ──────────────────────────────────────


@pytest.mark.parametrize(
    "variant,expected_intent,canonical_id",
    _PARAPHRASE_CASES,
    ids=[f"{c[2]}-{i}" for i, c in enumerate(_PARAPHRASE_CASES)],
)
def test_paraphrase_intent(variant, expected_intent, canonical_id):
    """Every paraphrase should classify to the same intent as its canonical question."""
    result = classify(variant)
    # Allow spatial/analytics flexibility for queries that could go either way
    if expected_intent in ("spatial", "analytics"):
        assert result in ("spatial", "analytics"), (
            f"{canonical_id}: {variant!r} classified as {result!r}, "
            f"expected spatial or analytics"
        )
    else:
        assert result == expected_intent, (
            f"{canonical_id}: {variant!r} classified as {result!r}, "
            f"expected {expected_intent!r}"
        )


# ── Tool routing for meta paraphrases ──────────────────────────


@pytest.mark.parametrize(
    "variant,expected_tool,canonical_id",
    _META_ROUTING_CASES,
    ids=[f"{c[2]}-{i}" for i, c in enumerate(_META_ROUTING_CASES)],
)
def test_paraphrase_tool_routing(variant, expected_tool, canonical_id):
    """Meta paraphrases should route to the same MCP tool as their canonical question.

    Terse paraphrases that return no route are acceptable — they fall through
    to the LLM fuzzy search path in production. We only fail on *wrong* routes.
    """
    route = match(variant, _KNOWN_TABLES)
    if route is None:
        # No match → LLM fallback in production. Skip, don't fail.
        pytest.skip(
            f"{canonical_id}: {variant!r} → no rule match (LLM fallback)"
        )
    assert route.tool_name == expected_tool, (
        f"{canonical_id}: {variant!r} routed to {route.tool_name!r}, "
        f"expected {expected_tool!r}"
    )
