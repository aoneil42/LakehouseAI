#!/usr/bin/env bash
set -euo pipefail

###############################################################################
# bootstrap.sh — Idempotent setup for the Terminus GIS stack
#
# Prerequisites: docker compose up -d  (core stack)
#
# Safe to re-run: each step checks whether it has already been completed.
#
# Modes:
#   Local dev (Garage):  docker compose --profile local up -d && ./bootstrap.sh
#   AWS S3 (production): Set S3_* env vars in .env, then ./bootstrap.sh
#
# The script auto-detects which mode to use based on the S3_ENDPOINT value.
###############################################################################

DC="docker compose"

# ── Detect storage mode ──────────────────────────────────────────────
# If .env exists, source it for variable defaults
if [ -f .env ]; then
  set -a
  # shellcheck disable=SC1091
  source .env
  set +a
fi

S3_ENDPOINT="${S3_ENDPOINT:-garage:3900}"
S3_REGION="${S3_REGION:-garage}"
S3_BUCKET="${S3_BUCKET:-lakehouse}"
S3_URL_STYLE="${S3_URL_STYLE:-path}"
S3_USE_SSL="${S3_USE_SSL:-false}"
S3_PATH_STYLE="${S3_PATH_STYLE:-true}"
S3_FLAVOR="${S3_FLAVOR:-minio}"

# Determine if we're using Garage (local dev) or real S3
if echo "$S3_ENDPOINT" | grep -qi "garage"; then
  MODE="garage"
else
  MODE="s3"
  S3_URL_STYLE="${S3_URL_STYLE:-vhost}"
  S3_USE_SSL="${S3_USE_SSL:-true}"
  S3_PATH_STYLE="${S3_PATH_STYLE:-false}"
  S3_FLAVOR="${S3_FLAVOR:-aws}"
fi

echo ""
echo "╔══════════════════════════════════════════════╗"
echo "║   Terminus GIS — Bootstrap                   ║"
echo "╚══════════════════════════════════════════════╝"
echo ""
echo "  Storage mode: ${MODE}"
echo "  S3 endpoint:  ${S3_ENDPOINT}"
echo ""

# ── Helper: wait for a service to be reachable ────────────────────────
wait_for() {
  local url="$1" label="$2" max="${3:-30}"
  local i=0
  echo "   Waiting for ${label}..."
  while ! curl -sf "$url" > /dev/null 2>&1; do
    i=$((i + 1))
    if [ "$i" -ge "$max" ]; then
      echo "   ✗ ${label} not ready after ${max}s"
      exit 1
    fi
    sleep 1
  done
  echo "   ✓ ${label} is ready."
}

# ══════════════════════════════════════════════════════════════════════
# GARAGE MODE (local dev only)
# ══════════════════════════════════════════════════════════════════════
if [ "$MODE" = "garage" ]; then
  GARAGE="docker exec garage /garage"

  # ── 1. Generate Garage secrets (idempotent) ─────────────────────────
  if grep -q "PLACEHOLDER_RPC_SECRET" garage.toml 2>/dev/null; then
    echo "① Generating Garage secrets..."
    RPC_SECRET=$(openssl rand -hex 32)
    ADMIN_TOKEN=$(openssl rand -hex 32)
    METRICS_TOKEN=$(openssl rand -hex 32)

    sed -i.bak \
      -e "s|PLACEHOLDER_RPC_SECRET|${RPC_SECRET}|" \
      -e "s|PLACEHOLDER_ADMIN_TOKEN|${ADMIN_TOKEN}|" \
      -e "s|PLACEHOLDER_METRICS_TOKEN|${METRICS_TOKEN}|" \
      garage.toml
    rm -f garage.toml.bak
    echo "   ✓ garage.toml updated with real secrets."

    echo "② Restarting Garage with real config..."
    $DC restart garage
    wait_for "http://localhost:3903/health" "Garage admin API" 30
  else
    echo "① Garage secrets already configured — skipping."
  fi

  # ── 2. Assign layout (idempotent) ───────────────────────────────────
  echo "③ Configuring Garage cluster layout..."
  NODE_ID=$($GARAGE node id 2>/dev/null | head -n1 | cut -d@ -f1)
  if [ -z "$NODE_ID" ]; then
    echo "   ✗ Failed to get Garage node ID. Is Garage running?"
    exit 1
  fi
  echo "   Node: ${NODE_ID:0:16}…"

  $GARAGE layout assign -z dc1 -c 10G "$NODE_ID" 2>/dev/null || true
  LAYOUT_VER=$($GARAGE layout show 2>&1 | sed 's/\x1b\[[0-9;]*m//g' | grep -oE "layout version: [0-9]+" | awk '{print $NF}' || echo "0")
  if [ "$LAYOUT_VER" = "0" ] || [ -z "$LAYOUT_VER" ]; then
    $GARAGE layout apply --version 1 2>/dev/null
    echo "   ✓ Layout applied (10GB single-node)."
  else
    echo "   ✓ Layout already applied (version ${LAYOUT_VER})."
  fi

  # ── 3. Create bucket + key (idempotent) ─────────────────────────────
  echo "④ Creating bucket 'lakehouse'..."
  $GARAGE bucket create lakehouse 2>/dev/null || echo "   (bucket already exists)"

  if [ -f .env ] && grep -q "GARAGE_KEY_ID=" .env 2>/dev/null; then
    echo "   ✓ Credentials found in .env — reusing."
    source .env
    KEY_ID="${GARAGE_KEY_ID}"
    SECRET_KEY="${GARAGE_SECRET_KEY}"
    ADMIN_TOKEN="${GARAGE_ADMIN_TOKEN:-}"
  else
    echo "   Creating access key..."
    KEY_OUTPUT=$($GARAGE key create lakehouse-key 2>&1)

    KEY_ID=$(echo "$KEY_OUTPUT" | grep -i "Key ID" | awk '{print $NF}')
    SECRET_KEY=$(echo "$KEY_OUTPUT" | grep -i "Secret key" | awk '{print $NF}')

    if [ -z "$KEY_ID" ]; then
      KEY_ID=$(echo "$KEY_OUTPUT" | grep -iE "access.key|key.id" | awk '{print $NF}')
    fi
    if [ -z "$SECRET_KEY" ]; then
      SECRET_KEY=$(echo "$KEY_OUTPUT" | grep -iE "secret.key|secret.access" | awk '{print $NF}')
    fi

    if [ -z "$KEY_ID" ] || [ -z "$SECRET_KEY" ]; then
      echo "   ✗ Could not parse key output. Set GARAGE_KEY_ID and GARAGE_SECRET_KEY in .env."
      exit 1
    fi

    $GARAGE bucket allow --read --write --owner lakehouse --key lakehouse-key 2>/dev/null
    echo "   ✓ Key created and granted permissions."
  fi

  echo "   Key ID:     ${KEY_ID}"
  echo "   Secret:     ${SECRET_KEY:0:8}…"

  # ── 4. Save .env ────────────────────────────────────────────────────
  echo "⑤ Saving credentials to .env..."
  if [ -z "${ADMIN_TOKEN:-}" ] && [ -f .env ]; then
    ADMIN_TOKEN=$(grep "^GARAGE_ADMIN_TOKEN=" .env 2>/dev/null | cut -d= -f2 || echo "")
  fi

  cat > .env <<EOF
# Generated by bootstrap.sh — $(date -Iseconds 2>/dev/null || date)
# Storage mode: Garage (local dev)
GARAGE_KEY_ID=${KEY_ID}
GARAGE_SECRET_KEY=${SECRET_KEY}
GARAGE_ADMIN_TOKEN=${ADMIN_TOKEN:-}

# S3 variables (default to Garage for local dev)
S3_ACCESS_KEY_ID=${KEY_ID}
S3_SECRET_ACCESS_KEY=${SECRET_KEY}
S3_ENDPOINT=garage:3900
S3_REGION=garage
S3_BUCKET=lakehouse
S3_URL_STYLE=path
S3_USE_SSL=false
S3_PATH_STYLE=true
S3_FLAVOR=minio

PYGEOAPI_SERVER_URL=auto
EOF
  echo "   ✓ .env saved."

  # Update runtime vars for warehouse creation
  S3_ACCESS_KEY_ID="${KEY_ID}"
  S3_SECRET_ACCESS_KEY="${SECRET_KEY}"

else
  # ══════════════════════════════════════════════════════════════════════
  # AWS S3 MODE
  # ══════════════════════════════════════════════════════════════════════
  echo "① Skipping Garage setup (using AWS S3)."

  S3_ACCESS_KEY_ID="${S3_ACCESS_KEY_ID:?S3_ACCESS_KEY_ID must be set in .env}"
  S3_SECRET_ACCESS_KEY="${S3_SECRET_ACCESS_KEY:?S3_SECRET_ACCESS_KEY must be set in .env}"

  echo "   Bucket:     ${S3_BUCKET}"
  echo "   Region:     ${S3_REGION}"
  echo "   Endpoint:   ${S3_ENDPOINT}"
fi

# ══════════════════════════════════════════════════════════════════════
# COMMON: Bootstrap LakeKeeper (both modes)
# ══════════════════════════════════════════════════════════════════════
echo "⑥ Bootstrapping LakeKeeper..."
$DC restart lakekeeper
wait_for "http://localhost:8181/health" "LakeKeeper" 30

# Accept terms of use
BOOT_HTTP=$(curl -s -o /dev/null -w "%{http_code}" \
  -X POST http://localhost:8181/management/v1/bootstrap \
  -H "Content-Type: application/json" \
  --data '{"accept-terms-of-use": true}' 2>/dev/null) || true

case "$BOOT_HTTP" in
  200|204) echo "   ✓ Terms of use accepted." ;;
  409|422) echo "   ✓ Already bootstrapped." ;;
  *)
    echo "   ⚠ Bootstrap returned HTTP ${BOOT_HTTP}. Retrying..."
    sleep 3
    BOOT_HTTP=$(curl -s -o /dev/null -w "%{http_code}" \
      -X POST http://localhost:8181/management/v1/bootstrap \
      -H "Content-Type: application/json" \
      --data '{"accept-terms-of-use": true}' 2>/dev/null) || true
    case "$BOOT_HTTP" in
      200|204) echo "   ✓ Terms of use accepted (retry)." ;;
      409|422) echo "   ✓ Already bootstrapped." ;;
      *) echo "   ✗ Bootstrap failed (HTTP ${BOOT_HTTP}). Check LakeKeeper logs." ;;
    esac
    ;;
esac

# Create warehouse — template create-warehouse.json with env vars
echo "   Creating warehouse..."
WAREHOUSE_JSON=$(sed \
  -e "s|\${S3_ACCESS_KEY_ID}|${S3_ACCESS_KEY_ID}|g" \
  -e "s|\${S3_SECRET_ACCESS_KEY}|${S3_SECRET_ACCESS_KEY}|g" \
  -e "s|\${S3_BUCKET}|${S3_BUCKET}|g" \
  -e "s|\${S3_REGION}|${S3_REGION}|g" \
  -e "s|\${S3_ENDPOINT}|${S3_ENDPOINT}|g" \
  -e "s|\${S3_PATH_STYLE}|${S3_PATH_STYLE}|g" \
  -e "s|\${S3_FLAVOR}|${S3_FLAVOR}|g" \
  create-warehouse.json)

HTTP_CODE=$(curl -sf -o /dev/null -w "%{http_code}" \
  -X POST http://localhost:8181/management/v1/warehouse \
  -H "Content-Type: application/json" \
  --data "$WAREHOUSE_JSON" 2>/dev/null) || true

case "$HTTP_CODE" in
  200|201) echo "   ✓ Warehouse 'lakehouse' created." ;;
  409)     echo "   ✓ Warehouse 'lakehouse' already exists." ;;
  *)       echo "   ⚠ Warehouse creation returned HTTP ${HTTP_CODE}." ;;
esac

# ── Restart services that depend on credentials ──────────────────────
echo "⑦ Restarting services with credentials..."
$DC restart duckdb mcp-server api pygeoapi geoservices
sleep 2
echo "   ✓ Services restarted."

# ── Done ─────────────────────────────────────────────────────────────
echo ""
echo "╔══════════════════════════════════════════════╗"
echo "║   ✅  Bootstrap complete!                    ║"
echo "╚══════════════════════════════════════════════╝"
echo ""
echo "  Storage: ${MODE} (${S3_ENDPOINT})"
echo "  LakeKeeper UI: http://localhost:8181"
echo ""
echo "  DuckDB (interactive):"
echo "    docker compose exec duckdb /duckdb -init /config/init.sql"
echo ""
