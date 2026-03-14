"""
Shared test fixtures.

Creates DuckDB tables with sample geometry data for testing all components.
"""

import random

import duckdb
import pyarrow as pa
import pytest
from shapely import wkb as wkb_mod
from shapely.geometry import Point, box


@pytest.fixture(scope="session")
def test_connection():
    """Create a DuckDB connection with spatial extension and test tables."""
    conn = duckdb.connect(database=":memory:")
    conn.execute("INSTALL spatial; LOAD spatial;")
    conn.execute("SET geometry_always_xy = true")
    conn.execute("CREATE SCHEMA test")

    _create_points_table(conn)
    _create_polygons_table(conn)

    return conn


@pytest.fixture(autouse=True)
def setup_catalog(test_connection):
    """Set the test connection as the global connection for all tests."""
    from iceberg_geo.query.catalog import set_catalog, reset_catalog

    set_catalog(test_connection)
    yield
    reset_catalog()


@pytest.fixture
def points_table():
    """Return the qualified name for the test points table."""
    return "test.sensor_points"


@pytest.fixture
def polygons_table():
    """Return the qualified name for the test polygons table."""
    return "test.parcels"


@pytest.fixture
def sample_query_params():
    """Default query params for testing."""
    from iceberg_geo.query.models import QueryParams

    return QueryParams(limit=10)


def _create_points_table(conn):
    """Create test.sensor_points with 100 random points."""
    random.seed(42)

    n = 100
    geometries = []
    for _ in range(n):
        pt = Point(
            random.uniform(-120, -70),
            random.uniform(25, 50),
        )
        geometries.append(wkb_mod.dumps(pt))

    table = pa.table(
        {
            "objectid": pa.array(range(n), type=pa.int64()),
            "sensor_id": pa.array([f"S{i:04d}" for i in range(n)]),
            "temperature": pa.array(
                [random.uniform(-10, 45) for _ in range(n)],
                type=pa.float64(),
            ),
            "geometry": pa.array(geometries, type=pa.large_binary()),
        }
    )

    conn.register("_tmp_points", table)
    conn.execute("""
        CREATE TABLE test.sensor_points AS
        SELECT objectid, sensor_id, temperature,
               ST_GeomFromWKB(geometry) AS geometry
        FROM _tmp_points
    """)
    conn.unregister("_tmp_points")


def _create_polygons_table(conn):
    """Create test.parcels with 50 rectangular polygons."""
    random.seed(43)

    n = 50
    geometries = []
    for _ in range(n):
        x = random.uniform(-120, -70)
        y = random.uniform(25, 50)
        size = random.uniform(0.01, 0.1)
        poly = box(x, y, x + size, y + size)
        geometries.append(wkb_mod.dumps(poly))

    table = pa.table(
        {
            "objectid": pa.array(range(n), type=pa.int64()),
            "parcel_id": pa.array([f"P{i:06d}" for i in range(n)]),
            "area_sqm": pa.array(
                [random.uniform(100, 50000) for _ in range(n)],
                type=pa.float64(),
            ),
            "zoning": pa.array(
                [random.choice(["R1", "R2", "C1", "C2", "I1"]) for _ in range(n)]
            ),
            "geometry": pa.array(geometries, type=pa.large_binary()),
        }
    )

    conn.register("_tmp_parcels", table)
    conn.execute("""
        CREATE TABLE test.parcels AS
        SELECT objectid, parcel_id, area_sqm, zoning,
               ST_GeomFromWKB(geometry) AS geometry
        FROM _tmp_parcels
    """)
    conn.unregister("_tmp_parcels")
