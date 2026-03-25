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
    # Tier 3 — Section 3.8: Multi-step spatial (Q36-Q38)
    ("Which buildings are within 500m of a hospital?", "spatial"),
    ("Find the total area of land_use zones that contain at least 3 buildings", "spatial"),
    ("Create a heat map layer of building density by land_use zone and save it for the webmap", "spatial"),
    # Tier 3 — Section 3.9: Temporal (Q39-Q42)
    ("Compare the current buildings dataset to last week's snapshot — what changed?", "meta"),
    ("What snapshots exist for the buildings table?", "meta"),
    ("Show me the buildings data as it was at the earliest snapshot", "meta"),
    ("What did the buildings table look like on March 1, 2026?", "meta"),
    # Tier 3 — Section 3.10: Export (Q43-Q44)
    ("Export all buildings as GeoJSON", "meta"),
    ("Export just the schools with their names as GeoJSON", "meta"),
    # Tier 3 — Section 3.10: Materialization (Q45-Q46)
    ("Save all hospitals from places as a new layer called nearby_hospitals", "spatial"),
    ("Create a scratch table with all buildings in residential land_use zones for the webmap", "spatial"),
    # Tier 3 — Section 3.11: Ambiguous & edge cases (Q47-Q52)
    ("Show me everything near the center", "spatial"),
    ("buildings close to land_use zones", "spatial"),
    ("Find features within 500 of the point", "spatial"),
    ("What's in the database?", "meta"),
    ("Can you make a map of all the schools with a 1km service area?", "spatial"),
    ("Are there more buildings in residential areas or commercial areas?", "analytics"),
    # Phase 1 keyword expansion — new spatial keywords
    ("find the centroid of each building", "spatial"),
    ("buildings on the north side of the river", "spatial"),
    ("create a 500m corridor around the road", "spatial"),
    ("what is the footprint of the building?", "spatial"),
    ("features outside the boundary", "spatial"),
    ("convert coordinates to meters", "spatial"),
    ("show me data within 2 km", "spatial"),
    ("find buildings within 1 yard of the road", "spatial"),
    # Phase 1 — materialization patterns (save/create as layer)
    ("save the results as a layer", "spatial"),
    ("create a scratch table for the map", "spatial"),
    ("materialize this as a new layer", "spatial"),
    # Phase 1 — meta patterns (how many tables)
    ("how many tables are there?", "meta"),
    ("how many datasets do we have?", "meta"),
    ("how many layers are available?", "meta"),
    # Conversational
    ("hello, what can you do?", "conversational"),
    ("hi", "conversational"),
    ("help", "conversational"),
    ("hey there", "conversational"),
    ("good morning", "conversational"),
])
def test_classify(msg, expected):
    assert classify(msg) == expected
