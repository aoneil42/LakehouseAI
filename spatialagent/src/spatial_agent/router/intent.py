import re
from typing import Literal

SPATIAL_KEYWORDS = {
    "near", "within", "buffer", "intersect", "distance",
    "polygon", "boundary", "adjacent", "surrounding", "closest",
    "overlap", "contains", "contain", "crosses", "touches",
    "inside", "outside", "lat", "lon", "longitude", "latitude", "map",
    "coordinate", "coords", "radius", "bbox", "envelope", "geometry",
    "spatial", "geom", "proximity",
    # Units
    "meters", "meter", "kilometers", "kilometer", "km",
    "miles", "mile", "feet", "yard",
    # Proximity, spatial join/aggregation, buffer
    "nearest", "farthest",
    "zone", "zones",
    "setback", "neighboring", "corridor",
    # Spatial concepts (NOT area/extent/coverage — those are in META patterns)
    "centroid", "footprint",
    "north", "south", "east", "west",
    # Materialization signals
    "layer", "scratch",
}

_META_PATTERNS = re.compile(
    r"(?i)("
    r"\b(what|which|list|show|describe|tell me about|find|search|enumerate)\b"
    r".*(table|schema|column|database|catalog|namespace|dataset|layer)"
    r"|\b(are there|is there|do we have|any)\b.*\b(table|dataset|layer|column|data)s?\b"
    r"|\b(what|which)\s+(data|do we have)\b"
    r"|\b(list|show)\s+(all\s+)?data\s*$"
    r"|\b(got\s+any|have\s+any)\b.*\b(data|table|dataset)\b"
    r"|\b(search|look)\s+for\b.*\b(data|table|dataset)\b"
    r"|\b(sample|preview)\b.*\b(data|rows?|records?|table|dataset|layer|building|place|land)"
    r"|\b(show|display|give)\b.*\b(first|top)\s+\d+\s+(rows?|records?)\b.*\b(of|from)\b"
    r"|\b(sample|preview)\b.*\b\d+\b"
    r"|\b(skip|exclude|without|no)\b.*\b(geom|geometry)\b"
    r"|\b(show|give)\b.*\b(sample|preview)\b"
    r"|\b(how many)\b.*\b(record|row|feature|entri|table|dataset|layer)s?\b"
    r"|\brow\s+count\b"
    r"|\b(summarize|summary|statistics|stats)\b"
    r"|\b(bounding\s*box|bbox)\b.*\b(of|for)\b"
    r"|\b(geographic\s+area|spatial\s+extent)\b"
    r"|\b(extent|coverage)\b.*\b(of|for)\b"
    r"|\b(what|which)\b.*(area|extent|coverage|region).*(cover|span|encompass)"
    r"|\b(what|which|what's the)\b.*(type|kind)s?\s+of\s+(geometr|geom)"
    r"|\b(geometry\s+types?)\b.*\b(for|of|in)\b"
    r"|\b\w+\s+(geometry\s+types?|bounding\s+box|extent)\s*$"
    # Temporal: snapshots, time travel, history
    r"|\b(snapshot|snapshots|version|history)\b.*\b(exist|available|for|of|list|show|what)\b"
    r"|\b(what|which|list|show|any)\b.*\b(snapshot|snapshots|version|history)\b"
    r"|\b(as\s+it\s+was|as\s+of|look(?:ed)?\s+like|at\s+snapshot)\b"
    r"|\b(time\s+travel|historical|previous\s+version)\b"
    r"|\b(what\s+did|what\s+was)\b.*\b(look|on|at|from)\b"
    r"|\b(what\s+changed|diff|changes?\s+since)\b"
    r"|\b(data\s+on)\b.*\b\d{4}\b"
    r"|\b(at\s+(?:the\s+)?(?:earliest|latest|first|last))\b.*\b(snapshot|version)\b"
    # Export: export/download as GeoJSON/CSV
    r"|\b(export|download)\b.*\b(as|to)?\s*(geojson|geo\s*json|csv|shapefile)\b"
    r"|\b(export|download)\b.*\b(all|the|just)\b"
    r"|\b(geojson)\b.*\b(export|of)\b"
    r"|\b\w+\s+(?:as|to)\s+geojson\b"
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
    # Materialization: save/create as layer/table for map
    r"|\b(save|create|materialize)\b.*\b(layer|scratch|webmap|for the map)\b"
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
        for term in (
            "table", "column", "count", "average", "sum", "select", "row",
            "data", "list", "preview", "sample", "describe", "stats",
            "bbox", "extent", "geojson", "export", "snapshot", "version",
        )
    ):
        return "conversational"

    return "analytics"
