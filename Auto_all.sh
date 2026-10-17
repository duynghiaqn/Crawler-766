#!/usr/bin/env bash
# ==============================================================================
# Auto_all.sh - Root Wrapper Script for Crawler 766
# Calls tools/Auto_all.sh with all provided arguments.
# ==============================================================================
set -eo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
exec "${SCRIPT_DIR}/tools/Auto_all.sh" "$@"
