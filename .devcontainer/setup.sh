#!/usr/bin/env bash
set -euo pipefail

###############################################################################
# .devcontainer/setup.sh — One-time Codespace setup
#
# Runs during postCreateCommand (first creation only).
# Builds all custom Docker images so subsequent starts are fast.
###############################################################################

echo ""
echo "╔══════════════════════════════════════════════╗"
echo "║   LakehouseAI — Codespace Setup              ║"
echo "╚══════════════════════════════════════════════╝"
echo ""

# Wait for Docker daemon (docker-in-docker takes a moment)
echo "⏳ Waiting for Docker daemon..."
for i in $(seq 1 30); do
  if docker info >/dev/null 2>&1; then
    echo "   ✓ Docker is ready."
    break
  fi
  if [ "$i" -eq 30 ]; then
    echo "   ✗ Docker not available after 30s."
    exit 1
  fi
  sleep 1
done

# Build all custom images (cached for future starts)
echo ""
echo "🔨 Building Docker images (this takes a few minutes on first run)..."
cd lakehouse
docker compose build
echo "   ✓ All images built."

# Make bootstrap executable
chmod +x bootstrap.sh

echo ""
echo "✅ Setup complete. The stack will start automatically."
echo ""