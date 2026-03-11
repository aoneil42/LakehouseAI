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
    # Discovery / preview / stats (Section 3.2)
    ("Show me a sample of the buildings data", "meta"),
    ("Preview the first 5 rows of census data", "meta"),
    ("How many records are in the zones table?", "meta"),
    ("Summarize the buildings dataset", "meta"),
    ("What types of geometries are in the roads table?", "meta"),
    ("What geographic area does the parcels dataset cover?", "meta"),
    ("Give me the bounding box of buildings", "meta"),
    ("are there any time enabled layers?", "meta"),
    ("summarize paris.places", "meta"),
    ("summarize the buildings", "meta"),
    ("what type of geometries are in paris datasets", "meta"),
    # Section 3.3: spatial filtering (should NOT route to meta)
    ("What features are inside this bounding box: 2.33, 48.84, 2.36, 48.87?", "spatial"),
    ("Which zones contain the point at longitude 2.35, latitude 48.86?", "spatial"),
    ("Find all features within a 1km radius of coordinates 48.86N, 2.35E", "spatial"),
    # Tier 2 — Section 3.4: Proximity / Nearest Neighbor (Q20-Q24)
    ("What are the 3 closest hospitals to the stadium?", "spatial"),
    ("Find the 5 nearest buildings to latitude 39.74, longitude -104.98", "spatial"),
    ("Which schools are within 2 miles of the library?", "spatial"),
    ("List buildings within 800m of downtown, sorted by distance", "spatial"),
    ("How far is the hospital from the school?", "spatial"),
    # Tier 2 — Section 3.5: Spatial Joins (Q25-Q28)
    ("Which buildings are inside the Downtown zone?", "spatial"),
    ("Join buildings with zones to show which zone each building falls in", "spatial"),
    ("Find all roads that cross through residential zones", "spatial"),
    ("Match each fire station to buildings within 500 meters of it", "spatial"),
    # Tier 2 — Section 3.6: Buffer Analysis (Q29-Q31)
    ("Create a 200 meter buffer around all schools", "spatial"),
    ("Show a merged buffer zone of 1km around all hospitals", "spatial"),
    ("Generate a 500m setback zone around each road segment", "spatial"),
    # Tier 2 — Section 3.7: Spatial Aggregation (Q32-Q35)
    ("How many buildings are in each zone?", "spatial"),
    ("What is the average building height per zone?", "spatial"),
    ("Sum the population in each census tract", "spatial"),
    ("Count buildings taller than 20m in each zone", "spatial"),
    # Edge case: "zones table" in meta context should still be meta
    ("How many records are in the zones table?", "meta"),
    ("hello, what can you do?", "conversational"),
    ("hi", "conversational"),
    ("help", "conversational"),
    ("hey there", "conversational"),
    ("good morning", "conversational"),
])
def test_classify(msg, expected):
    assert classify(msg) == expected
