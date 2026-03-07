import pytest


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
