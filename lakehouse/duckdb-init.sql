-- DuckDB initialization: spatial lakehouse connection
-- Run with:  /duckdb -init /config/init.sql

INSTALL httpfs;
LOAD httpfs;
INSTALL iceberg;
LOAD iceberg;
INSTALL spatial;
LOAD spatial;

SET geometry_always_xy = true;

-- S3 credentials for direct file access (GeoParquet on Garage)
-- Values are templated at runtime by duckdb-entrypoint.sh
CREATE SECRET garage_s3 (
    TYPE S3,
    KEY_ID '${GARAGE_KEY_ID}',
    SECRET '${GARAGE_SECRET_KEY}',
    REGION 'garage',
    ENDPOINT 'garage:3900',
    URL_STYLE 'path',
    USE_SSL false
);

-- Iceberg REST catalog secret
-- LakeKeeper runs with AUTHZ_BACKEND=allowall in dev, so any token value works.
CREATE SECRET lakekeeper_secret (
    TYPE ICEBERG,
    TOKEN 'dummy'
);

-- Attach the LakeKeeper catalog
ATTACH 'lakehouse' AS lakehouse (
    TYPE ICEBERG,
    ENDPOINT 'http://lakekeeper:8181/catalog',
    SECRET lakekeeper_secret
);

.print '──────────────────────────────────────────'
.print '  DuckDB + Spatial + Iceberg ready'
.print '  Catalog:  lakehouse  (via LakeKeeper)'
.print '  Storage:  Garage S3  (http://garage:3900)'
.print '──────────────────────────────────────────'
.print ''
.print '  Try: SHOW ALL TABLES;'
.print '  Try: SELECT ST_Point(0,0);'
.print ''
