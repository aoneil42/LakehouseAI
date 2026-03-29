#!/bin/bash
# pull-models.sh — Pull default LLM models into the containerized Ollama.
# Run after: docker compose --profile agent up -d
#
# For air-gapped deployment, pre-pull on a connected machine then export:
#   docker compose exec ollama ollama pull devstral-small-2
#   docker compose down
#   docker volume export lakehouse_ollama-models > ollama-models.tar
#
# On the target machine:
#   docker volume create lakehouse_ollama-models
#   docker volume import lakehouse_ollama-models < ollama-models.tar

set -euo pipefail

COMPOSE_DIR="$(cd "$(dirname "$0")/.." && pwd)"

echo "Waiting for Ollama to be ready..."
until docker compose -f "$COMPOSE_DIR/docker-compose.yml" exec ollama curl -sf http://localhost:11434/api/tags > /dev/null 2>&1; do
  sleep 2
done

echo "Pulling devstral-small-2..."
docker compose -f "$COMPOSE_DIR/docker-compose.yml" exec ollama ollama pull devstral-small-2

echo "Available models:"
docker compose -f "$COMPOSE_DIR/docker-compose.yml" exec ollama ollama list
