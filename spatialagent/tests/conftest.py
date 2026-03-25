import json
from pathlib import Path

import pytest


@pytest.fixture(scope="session")
def canonical_questions():
    """Load the canonical NL2Spatial question corpus."""
    fixture_path = Path(__file__).parent / "fixtures" / "canonical_questions.json"
    with open(fixture_path) as f:
        data = json.load(f)
    return data["questions"]


@pytest.fixture
def sample_tables():
    return {
        "namespaces": [
            {
                "namespace": "default",
                "tables": ["buildings", "roads", "rivers"],
            }
        ]
    }


@pytest.fixture
def sample_describe():
    return {
        "columns": [
            {"name": "id", "type": "INTEGER"},
            {"name": "name", "type": "VARCHAR"},
            {"name": "height", "type": "FLOAT"},
            {"name": "geom", "type": "GEOMETRY"},
        ]
    }


@pytest.fixture
def sample_bbox():
    return {"bbox": [-77.5, 38.8, -77.0, 39.0]}
