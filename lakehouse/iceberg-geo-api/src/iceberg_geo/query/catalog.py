"""
Manage DuckDB connection with attached Iceberg catalog.

Reads config from ICEBERG_CATALOG_CONFIG env var or passed dict.
Supports ${ENV_VAR} interpolation in YAML string values so that
secrets (e.g. S3 credentials) can be injected via environment
variables rather than hard-coded in the config file.
"""

import logging
import os
import re
import threading

import duckdb
import yaml

logger = logging.getLogger(__name__)

_conn = None
_conn_lock = threading.Lock()
_catalog_prefix = "lakehouse"  # Set to None in test mode

_ENV_RE = re.compile(r"\$\{(\w+)\}")


def _resolve_env_vars(value):
    """Replace ${VAR} placeholders with environment variable values."""
    if isinstance(value, str):
        return _ENV_RE.sub(
            lambda m: os.environ.get(m.group(1), m.group(0)), value
        )
    return value


def _load_catalog_config() -> dict:
    """Load catalog config from YAML file."""
    config_path = os.environ.get(
        "ICEBERG_CATALOG_CONFIG", "config/catalog.yml"
    )
    with open(config_path) as f:
        config = yaml.safe_load(f)
    return {
        k: _resolve_env_vars(v) for k, v in config["catalog"].items()
    }


def get_connection() -> duckdb.DuckDBPyConnection:
    """Get or create the shared DuckDB connection with Iceberg catalog attached.

    Uses lazy initialization with double-checked locking.
    """
    global _conn
    if _conn is not None:
        return _conn

    with _conn_lock:
        if _conn is not None:
            return _conn

        logger.info("Initializing DuckDB connection...")
        conn = duckdb.connect(database=":memory:")

        # Load extensions
        ext_dir = os.environ.get("DUCKDB_EXTENSION_DIR")
        if ext_dir:
            conn.execute(f"SET extension_directory = '{ext_dir}'")
            conn.execute("SET autoinstall_known_extensions = false")
            conn.execute("SET autoload_known_extensions = false")
            for ext in ("httpfs", "iceberg", "spatial"):
                conn.execute(f"LOAD {ext}")
                logger.info(f"Loaded extension: {ext}")
        else:
            for ext in ("httpfs", "iceberg", "spatial"):
                conn.execute(f"INSTALL {ext}; LOAD {ext};")
                logger.info(f"Loaded extension: {ext}")

        conn.execute("SET geometry_always_xy = true")

        # Read catalog config for S3 and catalog endpoint
        config = _load_catalog_config()

        # Create S3 secret for Garage
        s3_key = config.get("s3.access-key-id", "")
        s3_secret = config.get("s3.secret-access-key", "")
        s3_endpoint = config.get("s3.endpoint", "http://garage:3900")
        s3_region = config.get("s3.region", "garage")
        use_ssl = "true" if s3_endpoint.startswith("https") else "false"

        if s3_key:
            # Strip protocol from endpoint for DuckDB
            endpoint = s3_endpoint.replace("http://", "").replace("https://", "")
            conn.execute(f"""
                CREATE SECRET garage_s3 (
                    TYPE S3,
                    KEY_ID '{s3_key}',
                    SECRET '{s3_secret}',
                    REGION '{s3_region}',
                    ENDPOINT '{endpoint}',
                    URL_STYLE 'path',
                    USE_SSL {use_ssl}
                )
            """)
            logger.info(f"Created S3 secret for endpoint {endpoint}")

        # Create Iceberg catalog secret
        token = config.get("token", "dummy")
        conn.execute(f"""
            CREATE SECRET iceberg_secret (
                TYPE ICEBERG,
                TOKEN '{token}'
            )
        """)

        # Attach the Iceberg catalog
        catalog_uri = config.get("uri", "http://lakekeeper:8181/catalog")
        warehouse = config.get("warehouse", "lakehouse")
        conn.execute(f"""
            ATTACH '{warehouse}' AS lakehouse (
                TYPE ICEBERG,
                ENDPOINT '{catalog_uri}',
                SECRET iceberg_secret,
                ACCESS_DELEGATION_MODE none
            )
        """)
        logger.info(f"Attached catalog '{warehouse}' from {catalog_uri}")

        _conn = conn
        return _conn


def set_catalog(conn):
    """Override the connection instance (used for testing)."""
    global _conn, _catalog_prefix
    _conn = conn
    _catalog_prefix = None  # Test connections don't have lakehouse catalog


def reset_catalog():
    """Reset the singleton connection (used for testing)."""
    global _conn, _catalog_prefix
    _conn = None
    _catalog_prefix = "lakehouse"


def get_table(namespace: str, table_name: str) -> str:
    """Return the qualified table reference for DuckDB queries."""
    get_connection()
    if _catalog_prefix:
        return f"{_catalog_prefix}.{namespace}.{table_name}"
    return f"{namespace}.{table_name}"


def list_namespaces() -> list[str]:
    """List available namespaces in the catalog."""
    conn = get_connection()
    db_filter = f"database_name = '{_catalog_prefix}'" if _catalog_prefix else "1=1"
    rows = conn.execute(
        f"SELECT DISTINCT schema_name FROM duckdb_tables() WHERE {db_filter}"
    ).fetchall()
    return [r[0] for r in rows]


def list_tables(namespace: str) -> list[str]:
    """List available tables in a namespace."""
    conn = get_connection()
    db_filter = f"database_name = '{_catalog_prefix}' AND " if _catalog_prefix else ""
    rows = conn.execute(
        f"SELECT table_name FROM duckdb_tables() "
        f"WHERE {db_filter}schema_name = '{namespace}'"
    ).fetchall()
    return [r[0] for r in rows]
