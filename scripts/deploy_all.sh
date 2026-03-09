#!/usr/bin/env bash
#
# Unified deploy script for VPS: updates strategy + analytics repos, optionally runs smokes, restarts services.
# Usage: bash scripts/deploy_all.sh [--smoke]
# With --smoke: run lightweight smoke checks (strategy: exec_mode smoke, analytics: report smoke).
#
set -euo pipefail

STRATEGY_ROOT="${STRATEGY_ROOT:-/root/pump_short}"
ANALYTICS_ROOT="${ANALYTICS_ROOT:-/opt/pump_short_analysis}"
SERVICES="pump-short.service pump-short-live-auto.service"
RUN_SMOKE=false

for arg in "$@"; do
    case "$arg" in
        --smoke) RUN_SMOKE=true ;;
    esac
done

echo "=============================================="
echo "  DEPLOY ALL  $(date -u '+%Y-%m-%d %H:%M:%S UTC')"
echo "=============================================="

# --- 1. Update strategy repo ---
echo ""
echo "[1/5] Updating strategy repo: $STRATEGY_ROOT"
if ! git -C "$STRATEGY_ROOT" pull; then
    echo "ERROR: strategy repo pull failed"
    exit 1
fi

# --- 2. Update analytics repo ---
echo ""
echo "[2/5] Updating analytics repo: $ANALYTICS_ROOT"
if ! git -C "$ANALYTICS_ROOT" pull; then
    echo "ERROR: analytics repo pull failed"
    exit 1
fi

# --- 3. Optional smoke checks ---
if "$RUN_SMOKE"; then
    echo ""
    echo "[3/5] Running smoke checks..."
    if [[ -f "$STRATEGY_ROOT/venv/bin/python3" ]] && [[ -f "$STRATEGY_ROOT/scripts/smoke_dataset_exec_mode.py" ]]; then
        echo "  Strategy smoke: smoke_dataset_exec_mode.py"
        if ! (cd "$STRATEGY_ROOT" && PYTHONPATH="$STRATEGY_ROOT" ./venv/bin/python3 scripts/smoke_dataset_exec_mode.py); then
            echo "ERROR: strategy smoke failed"
            exit 1
        fi
    fi
    if [[ -f "$ANALYTICS_ROOT/venv/bin/python3" ]] || [[ -f "$STRATEGY_ROOT/venv/bin/python3" ]]; then
        PY="${ANALYTICS_ROOT}/venv/bin/python3"
        [[ -x "$PY" ]] || PY="${STRATEGY_ROOT}/venv/bin/python3"
        echo "  Analytics smoke: smoke_fast0_report.py"
        if ! (cd "$ANALYTICS_ROOT" && PYTHONPATH="$ANALYTICS_ROOT" "$PY" scripts/smoke_fast0_report.py --root "$STRATEGY_ROOT/datasets"); then
            echo "ERROR: analytics smoke failed"
            exit 1
        fi
    fi
else
    echo ""
    echo "[3/5] Skipping smoke (use --smoke to enable)"
fi

# --- 4. Restart services ---
echo ""
echo "[4/5] Restarting services: $SERVICES"
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

# --- 5. Status ---
echo ""
echo "[5/5] Service status"
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
