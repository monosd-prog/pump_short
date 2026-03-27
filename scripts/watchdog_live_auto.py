#!/usr/bin/env python3
"""
Watchdog for pump-short-live-auto timer health.
Detects and alerts on stuck/unhealthy timer states.
Optional auto-recovery if configured.

Run every 30 seconds via systemd timer or cron.
Usage: python3 watchdog_live_auto.py
Environment:
  WATCHDOG_AUTO_RECOVER=1|0 (default: 1)
  WATCHDOG_STALE_THRESHOLD_SEC=120 (default: 120s = 2 min)
"""
import json
import logging
import os
import re
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("watchdog_live_auto")

# Config
AUTO_RECOVER = os.getenv("WATCHDOG_AUTO_RECOVER", "1").strip().lower() in ("1", "true", "yes")
STALE_THRESHOLD_SEC = int(os.getenv("WATCHDOG_STALE_THRESHOLD_SEC", "120"))
STATE_FILE = Path("/tmp/watchdog_live_auto.state.json")

# Timer/service names
TIMER_NAME = "pump-short-live-auto.timer"
SERVICE_NAME = "pump-short-live-auto.service"


def run_cmd(cmd: str) -> str:
    """Run shell command, return stdout. Raise on non-zero exit."""
    try:
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=5)
        if result.returncode != 0:
            raise RuntimeError(f"Command failed: {cmd}\n{result.stderr}")
        return result.stdout.strip()
    except subprocess.TimeoutExpired:
        raise RuntimeError(f"Command timed out: {cmd}")


def get_timer_status() -> dict:
    """Get timer status via systemctl show."""
    try:
        # Use simpler statusoutput parsing
        output = run_cmd(f"systemctl status {TIMER_NAME} 2>/dev/null | head -20")
        state = {}
        
        # Parse "Active: active (running)" or "Active: active (elapsed)"
        active_match = re.search(r'Active:\s+(\S+)\s+\((\S+)\)', output)
        if active_match:
            state['State'] = active_match.group(1)  # "active"
            state['SubState'] = active_match.group(2)  # "running", "elapsed", etc
        
        # Detect "Trigger: n/a" pattern (bad)
        if 'Trigger: n/a' in output:
            state['NextElapseUSecRealtime'] = ''
        else:
            state['NextElapseUSecRealtime'] = 'set'
        
        return state
    except Exception as e:
        logger.error("WATCHDOG_TIMER_QUERY_FAILED | error=%s", str(e))
        return {}


def get_last_service_run_age() -> int:
    """Get age of last successful pump-short-live-auto.service run in seconds."""
    try:
        # Get last successful run
        output = run_cmd(
            f"journalctl -u {SERVICE_NAME} --no-pager -n 1 -o short-iso 2>/dev/null | head -1"
        )
        if not output:
            return 9999  # No runs found
        
        # Parse timestamp from journalctl output
        # Format: "2026-03-27T14:32:09+0000 system..."
        parts = output.split()
        if len(parts) < 2:
            return 9999
        
        ts_str = parts[0]  # e.g., "2026-03-27T14:32:09+0000"
        try:
            last_run = datetime.fromisoformat(ts_str.replace("+0000", "+00:00"))
            now = datetime.now(timezone.utc)
            age_sec = int((now - last_run).total_seconds())
            return age_sec
        except ValueError:
            return 9999
    except Exception as e:
        logger.warning("WATCHDOG_SERVICE_AGE_QUERY_FAILED | error=%s", str(e))
        return 9999


def is_healthy() -> tuple[bool, str]:
    """
    Check health. Return (is_healthy, reason).
    Healthy if:
      - Timer State = active
      - Timer SubState != elapsed (should be "running" or "waiting")
      - NextElapseUSecRealtime is set (not empty/n/a)
      - Last service run < STALE_THRESHOLD_SEC
    """
    state = get_timer_status()
    
    if not state:
        return False, "timer_status_query_failed"
    
    timer_state = state.get("State", "unknown")
    if timer_state != "active":
        return False, f"timer_state={timer_state}"
    
    # Check for stuck "elapsed" substate (bad) vs "running"/"waiting" (good)
    substate = state.get("SubState", "unknown")
    if substate == "elapsed":
        return False, "timer_substate_elapsed"
    
    next_elapse = state.get("NextElapseUSecRealtime", "")
    if not next_elapse or next_elapse == "":
        return False, "timer_next_elapse_empty"
    
    age = get_last_service_run_age()
    if age > STALE_THRESHOLD_SEC:
        return False, f"last_run_stale_age_sec={age}"
    
    return True, "ok"


def maybe_recover(reason: str) -> None:
    """Attempt auto-recovery if configured."""
    if not AUTO_RECOVER:
        logger.warning(
            "WATCHDOG_AUTO_RECOVER_DISABLED | reason=%s | manual_recovery_needed",
            reason,
        )
        return
    
    try:
        logger.info("WATCHDOG_AUTO_RECOVERY_ATTEMPT | timer=%s | reason=%s", TIMER_NAME, reason)
        run_cmd(f"systemctl restart {TIMER_NAME}")
        time.sleep(1)
        
        # Verify
        is_ok, check_reason = is_healthy()
        if is_ok:
            logger.info("WATCHDOG_AUTO_RECOVERY_SUCCESS | timer=%s", TIMER_NAME)
        else:
            logger.error(
                "WATCHDOG_AUTO_RECOVERY_FAILED_VERIFICATION | timer=%s | check_reason=%s",
                TIMER_NAME, check_reason,
            )
    except Exception as e:
        logger.error("WATCHDOG_AUTO_RECOVERY_ERROR | error=%s", str(e))


def update_state(is_healthy: bool, reason: str) -> None:
    """Track state to avoid spam alerts."""
    state = {}
    try:
        if STATE_FILE.exists():
            with open(STATE_FILE) as f:
                state = json.load(f)
    except Exception:
        pass
    
    now = datetime.now(timezone.utc).isoformat()
    last_health = state.get("is_healthy")
    last_reason = state.get("reason")
    
    state["is_healthy"] = is_healthy
    state["reason"] = reason
    state["timestamp"] = now
    
    # Transition detection
    if last_health is True and is_healthy is False:
        logger.error(
            "LIVE_AUTO_WATCHDOG_ALERT | status=UNHEALTHY | reason=%s | was_healthy_before",
            reason,
        )
    elif last_health is False and is_healthy is True:
        logger.info("LIVE_AUTO_WATCHDOG_RECOVERED | status=HEALTHY | reason=%s", reason)
    
    with open(STATE_FILE, "w") as f:
        json.dump(state, f)


def main():
    """Main watchdog loop (one-shot when run as script)."""
    try:
        is_ok, reason = is_healthy()
        
        if is_ok:
            update_state(True, "ok")
            logger.debug("WATCHDOG_HEALTHY | reason=%s", reason)
        else:
            update_state(False, reason)
            logger.error("WATCHDOG_UNHEALTHY | reason=%s", reason)
            maybe_recover(reason)
    
    except Exception as e:
        logger.error("WATCHDOG_FATAL_ERROR | error=%s", str(e), exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
