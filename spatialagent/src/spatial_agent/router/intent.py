import re
from typing import Literal

SPATIAL_KEYWORDS = {
    "near", "within", "buffer", "intersect", "distance",
    "polygon", "boundary", "adjacent", "surrounding", "closest",
    "overlap", "contains", "contain", "crosses", "touches",
    "inside", "lat", "lon", "longitude", "latitude", "map",
    "coordinate", "radius", "bbox", "envelope", "geometry",
    "spatial", "geom", "proximity", "meters", "kilometers",
    "miles", "feet",
    # Tier 2: proximity, spatial join/aggregation, buffer
    "nearest", "farthest",
    "zone", "zones",
    "setback", "neighboring",
}

_META_PATTERNS = re.compile(
    r"(?i)("
    r"\b(what|which|list|show|describe|tell me about|find|search)\b"
    r".*(table|schema|column|database|catalog|namespace|dataset|layer)"
    r"|\b(are there|is there|do we have)\b.*\b(table|dataset|layer|column)s?\b"
    r"|\b(sample|preview)\b.*\b(data|rows?|records?|table|dataset|layer)\b"
    r"|\b(show|give)\b.*\b(sample|preview)\b"
    r"|\b(how many)\b.*\b(record|row|feature|entri)s?\b"
    r"|\b(summarize|summary|statistics|stats)\b"
    r"|\b(bounding\s*box|bbox)\b.*\b(of|for)\b"
    r"|\b(geographic\s+area|spatial\s+extent)\b"
    r"|\b(what|which)\b.*(area|extent|coverage|region).*(cover|span|encompass)"
    r"|\b(what|which)\b.*(type|kind)s?\s+of\s+(geometr|geom)"
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

# Compound spatial patterns that single keywords miss:
# - "how far" (distance queries)
# - "join/match X with/to Y" (spatial joins)
# - "in each zone/tract/..." (spatial aggregation)
_SPATIAL_PATTERNS = re.compile(
    r"(?i)("
    r"\bhow\s+far\b"
    r"|\b(join|match)\b.*\b(with|to)\b"
    r"|\b(in|per|within|inside)\s+each\b"
    r".*\b(zone|tract|district|region|area|polygon|boundary|block|neighborhood)\b"
    r"|\b(per)\s+(zone|tract|district|region|area|polygon|boundary|block|neighborhood)\b"
    r")",
)


def classify(message: str) -> Literal["spatial", "analytics", "conversational", "meta"]:
    msg_lower = message.lower()

    # Meta: questions about tables/schema/columns/stats/preview (answer from schema context)
    # Must check before spatial keywords so "bounding box" and "geometry types" route here
    if _META_PATTERNS.search(message):
        return "meta"

    # Check spatial keywords with word boundaries
    for kw in SPATIAL_KEYWORDS:
        if re.search(rf"\b{re.escape(kw)}\b", msg_lower):
            return "spatial"

    # Compound spatial patterns (proximity, spatial joins, spatial aggregation)
    if _SPATIAL_PATTERNS.search(message):
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
