#!/usr/bin/env bash
# ==============================================================================
# Auto_all.sh - Automated Master Execution Script for Crawler 766
# Runs all DVCQG crawler pipelines sequentially via tools/auto_sync.py
# ==============================================================================
set -eo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PYTHON_CMD="python3"
if ! command -v python3 &> /dev/null; then
    PYTHON_CMD="python"
fi

exec "$PYTHON_CMD" "${SCRIPT_DIR}/auto_sync.py" --run-once --push "$@"
