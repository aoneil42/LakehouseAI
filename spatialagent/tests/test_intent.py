import pytest

from spatial_agent.router.intent import classify


@pytest.mark.parametrize("msg,expected", [
    ("buildings near the river", "spatial"),
    ("buffer roads by 100m", "spatial"),
    ("show me parcels within the city boundary", "spatial"),
    ("what is the distance between these points", "spatial"),
    ("find the closest hospital", "spatial"),
    ("polygon overlay analysis", "spatial"),
    ("show lat lon coordinates", "spatial"),
    ("features within 500 meters", "spatial"),
    ("count rows in census table", "analytics"),
    ("average population by tract", "analytics"),
    ("show me the top 10 records", "analytics"),
    # Discovery / metadata (Section 3.1)
    ("What datasets are available?", "meta"),
    ("What namespaces exist in the catalog?", "meta"),
    ("Describe the schema of the buildings table", "meta"),
    ("Which tables have geometry columns?", "meta"),
    ("what tables have a height column", "meta"),
    ("Are there any tables with a population column?", "meta"),
    ("Find tables related to roads or transportation", "meta"),
    ("What columns does the parcels table have?", "meta"),
    ("hello, what can you do?", "conversational"),
    ("hi", "conversational"),
    ("help", "conversational"),
    ("hey there", "conversational"),
    ("good morning", "conversational"),
])
def test_classify(msg, expected):
    assert classify(msg) == expected
