import re
from typing import Literal

SPATIAL_KEYWORDS = {
    "near", "within", "buffer", "intersect", "distance",
    "polygon", "boundary", "adjacent", "surrounding", "closest",
    "overlap", "contains", "crosses", "touches", "lat", "lon",
    "coordinate", "radius", "bbox", "envelope", "geometry",
    "spatial", "geom", "proximity", "meters", "kilometers",
    "miles", "feet",
}

_META_PATTERNS = re.compile(
    r"(?i)("
    r"\b(what|which|list|show|describe|tell me about|find|search)\b"
    r".*(table|schema|column|database|catalog|namespace|dataset|layer)"
    r"|\b(are there|is there|do we have)\b.*\b(table|dataset|layer|column)s?\b"
    r")",
)

_GREETING_PATTERNS = re.compile(
    r"^(hi|hello|hey|howdy|greetings|good\s+(morning|afternoon|evening))\b",
    re.IGNORECASE,
)

_HELP_PATTERNS = re.compile(
    r"^(help|what can you do|how do(es)? (this|it) work)\s*\??$",
    re.IGNORECASE,
)


def classify(message: str) -> Literal["spatial", "analytics", "conversational", "meta"]:
    msg_lower = message.lower()

    # Meta: questions about tables/schema/columns (answer from schema context)
    if _META_PATTERNS.search(message):
        return "meta"

    # Check spatial keywords with word boundaries
    for kw in SPATIAL_KEYWORDS:
        if re.search(rf"\b{re.escape(kw)}\b", msg_lower):
            return "spatial"

    # Conversational: greetings, help, or very short non-data messages
    stripped = message.strip()
    if _GREETING_PATTERNS.search(stripped):
        return "conversational"
    if _HELP_PATTERNS.search(stripped):
        return "conversational"
    words = stripped.split()
    if len(words) < 4 and not any(
        term in msg_lower
        for term in ("table", "column", "count", "average", "sum", "select", "row")
    ):
        return "conversational"

    return "analytics"
