"""NL2Spatial integration evaluation harness.

Runs canonical questions against the live spatial agent and scores results.

Usage:
    pytest tests/eval_nl2spatial.py -m live -v
    pytest tests/eval_nl2spatial.py -m live -v -k Q40   # single question
"""

import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import httpx
import pytest

AGENT_URL = "http://localhost:8090/api/agent/chat"
ACTIVE_LAYERS = [
    "paris/buildings",
    "paris/land_use",
    "paris/water_polygons",
    "paris/water_lines",
    "paris/places",
    "paris/transportation",
]


# ── Data classes ────────────────────────────────────────────────


@dataclass
class NLTestCase:
    id: str
    question: str
    tier: int
    difficulty: str
    category: str
    expected_intent: str
    expected_tool: Optional[str] = None
    expected_params: Optional[dict] = None
    sql_patterns: Optional[list[str]] = None
    paris_variant: Optional[str] = None


@dataclass
class EvalScore:
    test_id: str
    tier: int
    category: str
    intent_correct: bool = False
    tool_correct: Optional[bool] = None
    sql_pattern_match: Optional[bool] = None
    executes_ok: bool = False
    error_message: Optional[str] = None
    raw_events: list = field(default_factory=list)

    @property
    def passed(self) -> bool:
        checks = [self.intent_correct, self.executes_ok]
        if self.tool_correct is not None:
            checks.append(self.tool_correct)
        if self.sql_pattern_match is not None:
            checks.append(self.sql_pattern_match)
        return all(checks)


# ── SSE stream parsing ─────────────────────────────────────────


async def send_query(question: str, session_id: str) -> list[dict]:
    """Send a query to the agent and return parsed SSE events."""
    events = []
    async with httpx.AsyncClient(timeout=180.0) as client:
        async with client.stream(
            "POST",
            AGENT_URL,
            json={
                "session_id": session_id,
                "message": question,
                "active_layers": ACTIVE_LAYERS,
            },
        ) as response:
            async for line in response.aiter_lines():
                if line.startswith("data: "):
                    try:
                        event = json.loads(line[6:])
                        events.append(event)
                    except json.JSONDecodeError:
                        continue
    return events


# ── Event analysis helpers ─────────────────────────────────────


def infer_intent(events: list[dict]) -> str:
    """Infer the classified intent from SSE status events."""
    for e in events:
        if e.get("type") != "status":
            continue
        content = e.get("content", "")
        if "Generating SQL" in content:
            return "spatial_or_analytics"
        if "Calling" in content or "Searching catalog" in content:
            return "meta"
    # Check for conversational response
    for e in events:
        if e.get("type") == "result" and "Hello!" in e.get("content", ""):
            return "conversational"
    # Check for SQL events (spatial/analytics path)
    for e in events:
        if e.get("type") == "sql":
            return "spatial_or_analytics"
    return "unknown"


def extract_tool(events: list[dict]) -> Optional[str]:
    """Extract which MCP tool was called from status events."""
    for e in events:
        if e.get("type") == "status":
            m = re.search(r"Calling (\w+)\.\.\.", e.get("content", ""))
            if m:
                return m.group(1)
            # LLM fuzzy search path emits "Searching catalog..." for search_tables
            if "Searching catalog" in e.get("content", ""):
                return "search_tables"
    return None


def extract_sql(events: list[dict]) -> Optional[str]:
    """Extract the last generated SQL from events."""
    sql = None
    for e in events:
        if e.get("type") == "sql":
            sql = e.get("content")
    return sql


def check_execution(events: list[dict]) -> tuple[bool, Optional[str]]:
    """Check if the query executed successfully (has result, no error)."""
    for e in events:
        if e.get("type") == "error":
            return False, e.get("content", "Unknown error")
    for e in events:
        if e.get("type") == "result":
            return True, None
    return False, "No result event received"


# ── Scoring logic ──────────────────────────────────────────────


def score_case(case: NLTestCase, events: list[dict]) -> EvalScore:
    """Score a single test case against observed events."""
    score = EvalScore(
        test_id=case.id,
        tier=case.tier,
        category=case.category,
        raw_events=events,
    )

    # Intent check
    inferred = infer_intent(events)
    if case.expected_intent in ("spatial", "analytics"):
        score.intent_correct = inferred == "spatial_or_analytics"
    elif case.expected_intent == "meta":
        score.intent_correct = inferred == "meta"
    elif case.expected_intent == "conversational":
        score.intent_correct = inferred == "conversational"

    # Tool check (meta queries only)
    if case.expected_tool:
        actual_tool = extract_tool(events)
        score.tool_correct = actual_tool == case.expected_tool

    # SQL pattern check (spatial/analytics queries)
    if case.sql_patterns:
        sql = extract_sql(events)
        if sql:
            score.sql_pattern_match = all(
                re.search(pat, sql, re.IGNORECASE)
                for pat in case.sql_patterns
            )
        else:
            score.sql_pattern_match = False

    # Execution check
    ok, err = check_execution(events)
    score.executes_ok = ok
    score.error_message = err

    return score


# ── Report generation ──────────────────────────────────────────


def generate_report(scores: list[EvalScore]) -> str:
    """Generate a markdown evaluation report."""
    from collections import defaultdict

    by_tier = defaultdict(list)
    by_category = defaultdict(list)
    for s in scores:
        by_tier[s.tier].append(s)
        by_category[s.category].append(s)

    lines = ["# NL2Spatial Evaluation Report\n"]

    total = len(scores)
    passed = sum(1 for s in scores if s.passed)
    lines.append(f"**Overall: {passed}/{total} passed ({100*passed/total:.0f}%)**\n")

    # By tier
    lines.append("## Results by Tier\n")
    lines.append("| Tier | Total | Passed | Failed | Rate |")
    lines.append("|------|-------|--------|--------|------|")
    for tier in sorted(by_tier.keys()):
        tier_scores = by_tier[tier]
        p = sum(1 for s in tier_scores if s.passed)
        f = len(tier_scores) - p
        rate = f"{100*p/len(tier_scores):.0f}%"
        lines.append(f"| {tier} | {len(tier_scores)} | {p} | {f} | {rate} |")

    # By category
    lines.append("\n## Results by Category\n")
    lines.append("| Category | Total | Passed | Failed | Rate |")
    lines.append("|----------|-------|--------|--------|------|")
    for cat in sorted(by_category.keys()):
        cat_scores = by_category[cat]
        p = sum(1 for s in cat_scores if s.passed)
        f = len(cat_scores) - p
        rate = f"{100*p/len(cat_scores):.0f}%"
        lines.append(f"| {cat} | {len(cat_scores)} | {p} | {f} | {rate} |")

    # Per-query detail
    lines.append("\n## Per-Query Detail\n")
    lines.append("| ID | Tier | Category | Intent | Tool | SQL | Exec | Status |")
    lines.append("|----|------|----------|--------|------|-----|------|--------|")
    for s in scores:
        status = "PASS" if s.passed else "FAIL"
        intent = "ok" if s.intent_correct else "MISS"
        tool = "ok" if s.tool_correct else ("MISS" if s.tool_correct is False else "-")
        sql = "ok" if s.sql_pattern_match else (
            "MISS" if s.sql_pattern_match is False else "-"
        )
        exec_ = "ok" if s.executes_ok else "FAIL"
        lines.append(
            f"| {s.test_id} | {s.tier} | {s.category} | {intent} | "
            f"{tool} | {sql} | {exec_} | {status} |"
        )

    return "\n".join(lines)


# ── Fixtures ───────────────────────────────────────────────────


@pytest.fixture(scope="session")
def test_cases() -> list[NLTestCase]:
    fixture_path = Path(__file__).parent / "fixtures" / "canonical_questions.json"
    with open(fixture_path) as f:
        data = json.load(f)
    return [NLTestCase(**q) for q in data["questions"]]


# Shared accumulator for scores across all test invocations
_scores: list[EvalScore] = []


@pytest.fixture(scope="session", autouse=True)
def write_report():
    """Write eval report after all tests complete."""
    yield
    if not _scores:
        return
    report = generate_report(_scores)
    report_path = Path(__file__).parent / "eval_report.md"
    report_path.write_text(report)
    print(f"\n\nEval report written to {report_path}")


# ── Test function ──────────────────────────────────────────────


def _case_ids():
    fixture_path = Path(__file__).parent / "fixtures" / "canonical_questions.json"
    with open(fixture_path) as f:
        data = json.load(f)
    return [q["id"] for q in data["questions"]]


@pytest.mark.live
@pytest.mark.parametrize("case_id", _case_ids())
async def test_canonical(case_id: str, test_cases: list[NLTestCase]):
    case = next((c for c in test_cases if c.id == case_id), None)
    assert case is not None, f"No test case for {case_id}"

    question = case.paris_variant or case.question
    events = await send_query(question, session_id=f"eval-{case.id}")
    score = score_case(case, events)
    _scores.append(score)

    # Assertions for pytest output
    assert score.executes_ok, f"{case.id} execution failed: {score.error_message}"
    assert score.intent_correct, f"{case.id} intent mismatch (inferred: {infer_intent(events)})"
    if score.tool_correct is not None:
        assert score.tool_correct, (
            f"{case.id} tool mismatch: expected {case.expected_tool}, "
            f"got {extract_tool(events)}"
        )
    if score.sql_pattern_match is not None:
        assert score.sql_pattern_match, (
            f"{case.id} SQL pattern miss: patterns={case.sql_patterns}, "
            f"sql={extract_sql(events)!r:.200}"
        )
