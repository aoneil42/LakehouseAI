#!/bin/sh
# Template duckdb-init.sql with Garage credentials at runtime.
# Uses sed instead of envsubst for compatibility with minimal images.
sed \
  -e "s|\${GARAGE_KEY_ID}|${GARAGE_KEY_ID}|g" \
  -e "s|\${GARAGE_SECRET_KEY}|${GARAGE_SECRET_KEY}|g" \
  /config/init.sql.template > /config/init.sql

# Keep container alive for interactive exec
exec tail -f /dev/null
