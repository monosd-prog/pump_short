#!/usr/bin/env bash
#
# Unified deploy script for VPS: updates pump_short repo, optionally runs smokes, restarts services.
# Usage: bash scripts/deploy_all.sh [--smoke]
# With --smoke: run lightweight smoke checks (strategy only).
#
set -euo pipefail

STRATEGY_ROOT="${STRATEGY_ROOT:-/root/pump_short}"
SERVICES="pump-short.service pump-short-live-auto.service pump-short-report-bot.service"
RUN_SMOKE=false

for arg in "$@"; do
    case "$arg" in
        --smoke) RUN_SMOKE=true ;;
    esac
done

echo "=============================================="
echo "  DEPLOY pump_short  $(date -u '+%Y-%m-%d %H:%M:%S UTC')"
echo "=============================================="

# --- 1. Update strategy repo ---
echo ""
echo "[1/3] Updating strategy repo: $STRATEGY_ROOT"
if ! git -C "$STRATEGY_ROOT" pull; then
    echo "ERROR: strategy repo pull failed"
    exit 1
fi

# --- 2. Optional smoke checks (strategy only) ---
if "$RUN_SMOKE"; then
    echo ""
    echo "[2/3] Running smoke checks..."
    if [[ -f "$STRATEGY_ROOT/venv/bin/python3" ]] && [[ -f "$STRATEGY_ROOT/scripts/smoke_dataset_exec_mode.py" ]]; then
        echo "  Strategy smoke: smoke_dataset_exec_mode.py"
        if ! (cd "$STRATEGY_ROOT" && PYTHONPATH="$STRATEGY_ROOT" ./venv/bin/python3 scripts/smoke_dataset_exec_mode.py); then
            echo "ERROR: strategy smoke failed"
            exit 1
        fi
    fi
else
    echo ""
    echo "[2/3] Skipping smoke (use --smoke to enable)"
fi

# --- 3. Restart services ---
echo ""
echo "[3/3] Restarting services: $SERVICES"
for svc in $SERVICES; do
    if systemctl is-enabled "$svc" &>/dev/null; then
        echo "  Restarting $svc..."
        if ! sudo systemctl restart "$svc"; then
            echo "ERROR: restart $svc failed"
            exit 1
        fi
    else
        echo "  Skipping $svc (not enabled)"
    fi
done

echo ""
echo "Service status"
for svc in $SERVICES; do
    if systemctl is-enabled "$svc" &>/dev/null; then
        echo "--- $svc ---"
        systemctl status --no-pager -l "$svc" 2>/dev/null | head -5 || true
    fi
done

echo ""
echo "=============================================="
echo "  DEPLOY COMPLETE  $(date -u '+%Y-%m-%d %H:%M:%S UTC')"
echo "=============================================="
