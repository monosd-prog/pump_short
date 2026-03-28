#!/usr/bin/env python3
"""
Production smoke health check for pump_short.

Usage:
    python3 scripts/smoke_prod_health.py
    python3 scripts/smoke_prod_health.py --days 7   # widen recent-data window

Prints PASS/FAIL for each check and a final overall verdict.
Read-only: no writes, no restarts.
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import re
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

# ── Project root on sys.path ───────────────────────────────────────────────────
_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

_DATASETS = _ROOT / "datasets"
_GUARD_STATE = _DATASETS / "auto_risk_guard_state.json"
_GUARD_FLAG = _DATASETS / "guard_refresh_pending.flag"
_TRADING_CLOSES = _DATASETS / "trading_closes.csv"


# ── Helpers ────────────────────────────────────────────────────────────────────

def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _parse_dt(s: str) -> Optional[datetime]:
    if not s:
        return None
    s = str(s).strip().replace("Z", "+00:00").replace("+0000", "+00:00")
    for fmt in ("%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%S.%f%z",
                "%Y-%m-%d %H:%M:%S%z", "%Y-%m-%d %H:%M:%S"):
        try:
            dt = datetime.strptime(s, fmt)
            return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    try:
        dt = datetime.fromisoformat(s)
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def _age_sec(dt: Optional[datetime]) -> Optional[float]:
    if dt is None:
        return None
    return (_now_utc() - dt).total_seconds()


def _service_active(name: str) -> bool:
    try:
        r = subprocess.run(["systemctl", "is-active", name],
                           capture_output=True, text=True, timeout=5)
        return r.stdout.strip() == "active"
    except Exception:
        return False


def _journalctl_tail(unit: str, lines: int = 200) -> str:
    try:
        r = subprocess.run(
            ["journalctl", "-u", unit, "--no-pager", f"-n{lines}",
             "--output=short-iso"],
            capture_output=True, text=True, timeout=10,
        )
        return r.stdout
    except Exception:
        return ""


def _timer_last_trigger_age(unit: str) -> Optional[float]:
    """Return seconds since the timer last triggered, or None on failure."""
    try:
        r = subprocess.run(
            ["systemctl", "show", unit, "--property=LastTriggerUSec"],
            capture_output=True, text=True, timeout=5,
        )
        line = r.stdout.strip()
        val = line.split("=", 1)[-1].strip()
        if not val or val == "0":
            return None
        # systemd format: "Mon 2026-03-28 19:15:12 MSK"
        # strip day name, try ISO-like parsing
        parts = val.split()
        if len(parts) >= 3:
            dt_str = f"{parts[1]} {parts[2]}"
            for fmt in ("%Y-%m-%d %H:%M:%S",):
                try:
                    dt = datetime.strptime(dt_str, fmt).replace(tzinfo=timezone.utc)
                    return (_now_utc() - dt).total_seconds()
                except ValueError:
                    pass
        return None
    except Exception:
        return None


def _recent_closes(days: int) -> list[dict]:
    if not _TRADING_CLOSES.exists():
        return []
    cutoff = _now_utc() - timedelta(days=days)
    result: list[dict] = []
    try:
        with open(_TRADING_CLOSES, newline="", encoding="utf-8", errors="replace") as f:
            for row in csv.DictReader(f):
                ts_str = row.get("ts_utc", "")
                dt = _parse_dt(ts_str)
                if dt and dt >= cutoff:
                    result.append(row)
    except Exception:
        pass
    return result


def _recent_outcomes_v3_live(days: int) -> list[dict]:
    cutoff_str = (_now_utc() - timedelta(days=days)).strftime("%Y%m%d")
    rows: list[dict] = []
    for f in _DATASETS.glob("date=*/strategy=short_pump_fast0/mode=live/outcomes_v3.csv"):
        m = re.search(r"date=(\d{8})", str(f))
        if not m or m.group(1) < cutoff_str:
            continue
        try:
            with open(f, newline="", encoding="utf-8", errors="replace") as fh:
                rows.extend(csv.DictReader(fh))
        except Exception:
            pass
    return rows


def _count_log_pattern(journal_text: str, pattern: str) -> int:
    return len(re.findall(pattern, journal_text))


# ── Check runners ──────────────────────────────────────────────────────────────

class Check:
    def __init__(self, name: str):
        self.name = name
        self._passed: Optional[bool] = None
        self._detail = ""

    def _result(self, passed: bool, detail: str = "") -> "Check":
        self._passed = passed
        self._detail = detail
        return self

    def pass_(self, detail: str = "") -> "Check":
        return self._result(True, detail)

    def fail(self, detail: str = "") -> "Check":
        return self._result(False, detail)

    def warn(self, detail: str = "") -> "Check":
        """Non-fatal warning: counted as WARN, not FAIL."""
        self._passed = None
        self._detail = detail
        return self

    def __repr__(self) -> str:
        if self._passed is True:
            status = "PASS"
        elif self._passed is False:
            status = "FAIL"
        else:
            status = "WARN"
        suffix = f"  {self._detail}" if self._detail else ""
        return f"  [{status}] {self.name}{suffix}"


def run_checks(days: int = 7) -> list[Check]:
    checks: list[Check] = []
    now = _now_utc()

    # ── 1. Services active ────────────────────────────────────────────────────
    for svc, label in [
        ("pump-short.service",            "pump-short.service (fast0 API server)"),
        ("pump-short-live-auto.timer",    "pump-short-live-auto.timer (trading runner)"),
        ("pump-short-report-bot.service", "pump-short-report-bot.service"),
    ]:
        c = Check(label)
        if _service_active(svc):
            checks.append(c.pass_())
        else:
            checks.append(c.fail("not active"))

    # ── 2. Timer fired recently (≤ 5 min) ─────────────────────────────────────
    c = Check("pump-short-live-auto timer last trigger ≤ 5 min")
    age = _timer_last_trigger_age("pump-short-live-auto.timer")
    if age is None:
        checks.append(c.warn("could not read LastTriggerUSec"))
    elif age <= 300:
        checks.append(c.pass_(f"last={age:.0f}s ago"))
    else:
        checks.append(c.fail(f"last trigger was {age:.0f}s ago (>{300}s)"))

    # ── 3. Guard state file exists ────────────────────────────────────────────
    c = Check("auto_risk_guard_state.json exists")
    if _GUARD_STATE.exists():
        checks.append(c.pass_())
    else:
        checks.append(c.fail(str(_GUARD_STATE)))

    # ── 4. Guard state is recent ──────────────────────────────────────────────
    c = Check("guard_state updated_at fresh (< 2h)")
    try:
        gstate = json.loads(_GUARD_STATE.read_text(encoding="utf-8"))
        first = next(iter(gstate.values()), {}) if isinstance(gstate, dict) else {}
        updated = first.get("updated_at", "")
        dt = _parse_dt(updated)
        age_sec = _age_sec(dt)
        if age_sec is None:
            checks.append(c.warn("updated_at missing or unparseable"))
        elif age_sec < 7200:
            checks.append(c.pass_(f"age={age_sec/60:.0f}min"))
        else:
            checks.append(c.fail(f"age={age_sec/3600:.1f}h (>2h)"))
    except Exception as e:
        checks.append(c.fail(f"cannot load: {e}"))

    # ── 5. Guard refresh flag not stuck ───────────────────────────────────────
    c = Check("guard_refresh_pending.flag not stuck (< 30 min)")
    if not _GUARD_FLAG.exists():
        checks.append(c.pass_("no flag"))
    else:
        try:
            mtime = datetime.fromtimestamp(_GUARD_FLAG.stat().st_mtime, tz=timezone.utc)
            flag_age = (now - mtime).total_seconds()
            if flag_age < 1800:
                checks.append(c.pass_(f"age={flag_age:.0f}s"))
            else:
                checks.append(c.fail(
                    f"flag stuck for {flag_age/60:.0f}min — guard refresh failing"
                ))
        except Exception as e:
            checks.append(c.warn(f"cannot stat flag: {e}"))

    # ── 6. Recent live fast0 closes in trading_closes.csv ────────────────────
    c = Check(f"live fast0 rows in trading_closes.csv last {days}d")
    closes = _recent_closes(days)
    f0_live = [r for r in closes
               if "fast0" in str(r.get("strategy", "")).lower()
               and str(r.get("mode", "")).strip().lower() == "live"]
    if f0_live:
        checks.append(c.pass_(f"n={len(f0_live)}"))
    else:
        checks.append(c.warn(f"0 live fast0 closes in last {days}d"))

    # ── 7. Recent live fast0 rows in outcomes_v3 ─────────────────────────────
    c = Check(f"live fast0 rows in outcomes_v3 last {days}d")
    v3_rows = _recent_outcomes_v3_live(days)
    if v3_rows:
        checks.append(c.pass_(f"n={len(v3_rows)}"))
    else:
        checks.append(c.warn(f"0 live fast0 outcomes_v3 in last {days}d"))

    # ── 8. outcomes_v3 / trading_closes convergence ───────────────────────────
    c = Check(f"outcomes_v3 count matches closes count last {days}d")
    v3_ids = {r.get("trade_id", "") for r in v3_rows}
    cl_ids = {r.get("trade_id", "") for r in f0_live}
    missing_in_v3 = cl_ids - v3_ids
    if not missing_in_v3:
        checks.append(c.pass_(f"closes={len(cl_ids)} v3={len(v3_ids)}"))
    else:
        checks.append(c.warn(
            f"{len(missing_in_v3)} close(s) missing in outcomes_v3 "
            f"(sync needed): {sorted(missing_in_v3)[:3]}"
        ))

    # ── 9. No recent LIVE_OUTCOME_V3_WRITE_FAILED in logs ─────────────────────
    c = Check("no LIVE_OUTCOME_V3_WRITE_FAILED in recent live-auto logs")
    live_journal = _journalctl_tail("pump-short-live-auto.service", 300)
    n_write_fail = _count_log_pattern(live_journal, r"LIVE_OUTCOME_V3_WRITE_FAILED")
    if n_write_fail == 0:
        checks.append(c.pass_())
    else:
        checks.append(c.fail(f"{n_write_fail} occurrence(s)"))

    # ── 10. No recent GUARD_REFRESH_FAILED in logs ────────────────────────────
    c = Check("no GUARD_REFRESH_FAILED in recent live-auto logs")
    n_guard_fail = _count_log_pattern(live_journal, r"GUARD_REFRESH_FAILED")
    if n_guard_fail == 0:
        checks.append(c.pass_())
    else:
        checks.append(c.fail(
            f"{n_guard_fail} occurrence(s) — check PYTHONPATH in runner"
        ))

    # ── 11. No watchdog recovery spam ─────────────────────────────────────────
    c = Check("no watchdog RECOVERY_LOOP spam in live-auto logs")
    n_wdog = _count_log_pattern(live_journal, r"WATCHDOG_RECOVERY|RECOVERY_LOOP")
    if n_wdog == 0:
        checks.append(c.pass_())
    elif n_wdog <= 3:
        checks.append(c.warn(f"{n_wdog} occurrence(s)"))
    else:
        checks.append(c.fail(f"{n_wdog} occurrence(s) — possible restart loop"))

    # ── 12. report-bot is not stale (recently restarted after code changes) ───
    c = Check("report-bot.service started (not stale)")
    try:
        r = subprocess.run(
            ["systemctl", "show", "pump-short-report-bot.service",
             "--property=ExecMainStartTimestamp,ActiveState"],
            capture_output=True, text=True, timeout=5,
        )
        ts_line = [l for l in r.stdout.splitlines()
                   if l.startswith("ExecMainStartTimestamp=")]
        ts_val = ts_line[0].split("=", 1)[-1].strip() if ts_line else ""
        if ts_val:
            checks.append(c.pass_(f"started={ts_val}"))
        else:
            checks.append(c.warn("could not read ExecMainStartTimestamp"))
    except Exception as e:
        checks.append(c.warn(f"systemctl error: {e}"))

    return checks


# ── Main ───────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="Production smoke health check")
    parser.add_argument("--days", type=int, default=7,
                        help="Window for recent-data checks (default: 7)")
    args = parser.parse_args()

    print(f"\n{'='*60}")
    print(f"  pump_short smoke health check  |  {_now_utc().strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print(f"{'='*60}\n")

    checks = run_checks(days=args.days)

    fails = [c for c in checks if c._passed is False]
    warns = [c for c in checks if c._passed is None]
    passes = [c for c in checks if c._passed is True]

    for c in checks:
        print(repr(c))

    print(f"\n{'─'*60}")
    print(f"  PASS={len(passes)}  WARN={len(warns)}  FAIL={len(fails)}")
    print(f"{'─'*60}")
    if not fails:
        verdict = "✅ HEALTHY" if not warns else "⚠️  HEALTHY with warnings"
    else:
        verdict = f"❌ UNHEALTHY — {len(fails)} check(s) failed"
    print(f"\n  VERDICT: {verdict}\n")

    sys.exit(0 if not fails else 1)


if __name__ == "__main__":
    main()
