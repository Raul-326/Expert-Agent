#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
APP_FILE="${APP_FILE:-panel_app.py}"
HOST="${HOST:-0.0.0.0}"
PORT="${PORT:-8501}"

cd "$ROOT_DIR"

if [[ -f ".venv/bin/activate" ]]; then
  source ".venv/bin/activate"
fi

exec streamlit run "$APP_FILE" \
  --server.address "$HOST" \
  --server.port "$PORT" \
  --server.headless true \
  --browser.gatherUsageStats false
