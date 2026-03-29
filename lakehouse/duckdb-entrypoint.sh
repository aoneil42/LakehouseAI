#!/bin/sh
# Template duckdb-init.sql with S3 credentials at runtime.
# Supports both Garage (local dev) and AWS S3 (production).
# Uses sed instead of envsubst for compatibility with minimal images.
sed \
  -e "s|\${S3_ACCESS_KEY_ID}|${S3_ACCESS_KEY_ID}|g" \
  -e "s|\${S3_SECRET_ACCESS_KEY}|${S3_SECRET_ACCESS_KEY}|g" \
  -e "s|\${S3_ENDPOINT}|${S3_ENDPOINT}|g" \
  -e "s|\${S3_REGION}|${S3_REGION}|g" \
  -e "s|\${S3_URL_STYLE}|${S3_URL_STYLE}|g" \
  -e "s|\${S3_USE_SSL}|${S3_USE_SSL}|g" \
  /config/init.sql.template > /config/init.sql

# Keep container alive for interactive exec
exec tail -f /dev/null
