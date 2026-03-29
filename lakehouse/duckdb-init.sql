-- DuckDB initialization: spatial lakehouse connection
-- Run with:  /duckdb -init /config/init.sql

INSTALL httpfs;
LOAD httpfs;
INSTALL iceberg;
LOAD iceberg;
INSTALL spatial;
LOAD spatial;

SET geometry_always_xy = true;

-- S3 credentials for direct file access (GeoParquet on S3 or Garage)
-- Values are templated at runtime by duckdb-entrypoint.sh
CREATE SECRET s3_creds (
    TYPE S3,
    KEY_ID '${S3_ACCESS_KEY_ID}',
    SECRET '${S3_SECRET_ACCESS_KEY}',
    REGION '${S3_REGION}',
    ENDPOINT '${S3_ENDPOINT}',
    URL_STYLE '${S3_URL_STYLE}',
    USE_SSL ${S3_USE_SSL}
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
.print '  Storage:  S3  (${S3_ENDPOINT})'
.print '──────────────────────────────────────────'
.print ''
.print '  Try: SHOW ALL TABLES;'
.print '  Try: SELECT ST_Point(0,0);'
.print ''
