#!/bin/bash
MODELS_DIR="${1:-./models}"
mkdir -p "$MODELS_DIR"

# Primary: Devstral Small 2
huggingface-cli download mistralai/Devstral-Small-2-GGUF \
    --local-dir "$MODELS_DIR/devstral-small-2"

# Mid-tier: Ministral 3 14B Instruct
huggingface-cli download mistralai/Ministral-3-14B-Instruct-GGUF \
    --local-dir "$MODELS_DIR/ministral-3-14b"

# Specialist: DuckDB-NSQL-7B
huggingface-cli download motherduck/DuckDB-NSQL-7B-GGUF \
    --local-dir "$MODELS_DIR/duckdb-nsql-7b"

echo "Models downloaded to $MODELS_DIR"
