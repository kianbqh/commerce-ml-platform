#!/usr/bin/env bash
# Instacart Online Grocery Shopping Dataset 2017 (public mirror on Hugging Face).
set -euo pipefail
cd "$(dirname "$0")/.."
exec "${PYTHON:-python3}" scripts/download_data.py
