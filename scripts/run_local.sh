#!/usr/bin/env bash
set -euo pipefail

# запускать из корня проекта: ./scripts/run_local.sh

# грузим .env (если есть)
if [ -f .env ]; then
  set -a
  source .env
  set +a
fi

HOST="${HOST:-127.0.0.1}"
PORT="${PORT:-8000}"

python -m short_pump api --host "$HOST" --port "$PORT" --reload