#!/bin/bash
# Runbook: Pump Short Live Auto Watchdog

set -e

echo "=== WATCHDOG VERIFICATION RUNBOOK ==="
echo ""

# 1. Verify watchdog is enabled
echo "1. Check watchdog timer status:"
systemctl status watchdog-live-auto.timer --no-pager | head -10

echo ""
echo "2. Verify watchdog service installed:"
ls -l /etc/systemd/system/watchdog-live-auto.* || echo "Missing!"

echo ""
echo "3. Recent watchdog health checks (last 5):"
journalctl -u watchdog-live-auto.service -n 5 --no-pager | tail -10

echo ""
echo "4. Live auto timer current status:"
systemctl status pump-short-live-auto.timer --no-pager | head -8

echo ""
echo "5. Check for alerts (last 24h):"
journalctl -u watchdog-live-auto.service --since "24 hours ago" -o short \
  | grep -i "unhealthy\|alert\|recovery\|failed" || echo "No alerts found (healthy!)"

echo ""
echo "=== RECOVERY PROCEDURES ==="
echo ""
echo "If watchdog detects stuck timer:"
echo "  Auto-action: Restarts pump-short-live-auto.timer (WATCHDOG_AUTO_RECOVER=1)"
echo "  Evidence: Check journalctl for WATCHDOG_AUTO_RECOVERY_SUCCESS"
echo ""
echo "To disable auto-recovery (manual only):"
echo "  sudo systemctl edit watchdog-live-auto.service"
echo "  Change: WATCHDOG_AUTO_RECOVER=1 → WATCHDOG_AUTO_RECOVER=0"
echo "  Then: sudo systemctl daemon-reload"
echo ""
echo "To manually restart the timer:"
echo "  sudo systemctl restart pump-short-live-auto.timer"
echo ""
echo "To manually check health now:"
echo "  python3 /root/pump_short/scripts/watchdog_live_auto.py"
echo ""
echo "=== ALERT THRESHOLDS ==="
echo "  Timer Unhealthy if:"
echo "    - State != active"
echo "    - SubState = 'elapsed' (no scheduled runs)"
echo "    - Trigger: n/a (timer sleeping)"
echo "    - Service not run for > 120 seconds"
echo ""
echo "  Watchdog checks every 30 seconds"
echo ""
echo "=== LOG MARKERS ==="
echo "  LIVE_AUTO_WATCHDOG_ALERT       → Timer unhealthy"
echo "  WATCHDOG_AUTO_RECOVERY_ATTEMPT → Attempting restart"
echo "  WATCHDOG_AUTO_RECOVERY_SUCCESS → Restart succeeded"
echo "  LIVE_AUTO_WATCHDOG_RECOVERED   → Returned to healthy"
