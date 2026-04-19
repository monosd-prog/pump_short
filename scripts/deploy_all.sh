#!/usr/bin/env bash
#
# Unified deploy script for VPS: updates pump_short repo, optionally runs smokes, restarts services.
# Usage: bash scripts/deploy_all.sh [--smoke]
# With --smoke: run lightweight smoke checks (strategy only).
#
set -euo pipefail

STRATEGY_ROOT="${STRATEGY_ROOT:-/root/pump_short}"
# Long-running daemons (reload code / connections on restart)
LONG_RUN_SERVICES="pump-short.service pump-short-report-bot.service"
# Timer units: restart reschedules triggers; oneshot *.service units are started by these timers
TIMER_UNITS="pump-trading-runner.timer pump-short-live-auto.timer"
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

# --- 3. Reload systemd and restart all pump units ---
echo ""
echo "[3/3] systemctl daemon-reload + restart services + timers"
if ! sudo systemctl daemon-reload; then
    echo "ERROR: daemon-reload failed"
    exit 1
fi

echo "  Long-running services: $LONG_RUN_SERVICES"
for svc in $LONG_RUN_SERVICES; do
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

echo "  Timers: $TIMER_UNITS"
for t in $TIMER_UNITS; do
    if systemctl is-enabled "$t" &>/dev/null; then
        echo "  Restarting $t..."
        if ! sudo systemctl restart "$t"; then
            echo "ERROR: restart $t failed"
            exit 1
        fi
    else
        echo "  Skipping $t (not enabled)"
    fi
done

echo ""
echo "Service status"
for svc in $LONG_RUN_SERVICES; do
    if systemctl is-enabled "$svc" &>/dev/null; then
        echo "--- $svc ---"
        systemctl status --no-pager -l "$svc" 2>/dev/null | head -5 || true
    fi
done

echo ""
echo "Timer status"
for t in $TIMER_UNITS; do
    if systemctl is-enabled "$t" &>/dev/null; then
        echo "--- $t ---"
        systemctl status --no-pager -l "$t" 2>/dev/null | head -6 || true
    fi
done

echo ""
echo "=============================================="
echo "  DEPLOY COMPLETE  $(date -u '+%Y-%m-%d %H:%M:%S UTC')"
echo "=============================================="
