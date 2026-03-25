"""
issue_detector.py — MODULE 2
Detect issues from RawData + StateReport.

MVP iteration 1 — 6 detectors:
  1. _detect_service_down
  2. _detect_journal_errors
  3. _detect_no_signal
  4. _detect_runner_hang
  5. _detect_stuck_positions
  6. _detect_state_corrupt

Each detector is wrapped in a try/except so one failing detector
does not break the pipeline. All detectors return list[Issue].
"""
from __future__ import annotations

import re
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from scripts.debug_modules.log_reader import RawData
from scripts.debug_modules.state_checker import StateReport


# ---------------------------------------------------------------------------
# Issue dataclass
# ---------------------------------------------------------------------------

@dataclass
class Issue:
    issue_id: str                       # "ISSUE-1", "ISSUE-2", ...
    type: str                           # see ISSUE_TYPES
    severity: str                       # CRITICAL | HIGH | MEDIUM | LOW
    source: str                         # where found: "journalctl" / "trading_state.json" / etc.
    description: str                    # human-readable summary
    evidence: str                       # raw excerpt / value
    component: str                      # runtime | watcher | fast0 | runner | outcome | liquidations | systemd
    ts: Optional[str] = None            # timestamp if known
    suggested_fix: str = ""             # one-liner action
    check_cmd: str = ""                 # shell command to verify
    fix_cmd: str = ""                   # shell command to fix (empty = no safe auto-fix)
    fix_risk: str = ""                  # risk note


# Issue type constants
class IssueType:
    SERVICE_DOWN    = "SERVICE_DOWN"
    JOURNAL_ERROR   = "JOURNAL_ERROR"
    NO_SIGNAL       = "NO_SIGNAL"
    RUNNER_HANG     = "RUNNER_HANG"
    STUCK_POSITION  = "STUCK_POSITION"
    STATE_CORRUPT   = "STATE_CORRUPT"


# Severity ordering (lower = more critical)
_SEVERITY_ORDER = {"CRITICAL": 0, "HIGH": 1, "MEDIUM": 2, "LOW": 3}


# Journal patterns that indicate real errors
_JOURNAL_ERROR_PATTERNS: List[re.Pattern] = [
    re.compile(r"\bERROR\b", re.IGNORECASE),
    re.compile(r"\bEXCEPTION\b", re.IGNORECASE),
    re.compile(r"\bTraceback\b", re.IGNORECASE),
    re.compile(r"\braise\s+\w+", re.IGNORECASE),
    re.compile(r"\bCRITICAL\b", re.IGNORECASE),
    re.compile(r"\bFATAL\b", re.IGNORECASE),
]

# Patterns that should NOT be flagged as errors (false-positive filter)
_JOURNAL_NOISE_PATTERNS: List[re.Pattern] = [
    re.compile(r"loglevel", re.IGNORECASE),
    re.compile(r"--log-level", re.IGNORECASE),
    re.compile(r"error_code.*0\b", re.IGNORECASE),
    re.compile(r"no error", re.IGNORECASE),
]

# Max journal error lines to surface in a single ISSUE
_MAX_ERROR_LINES_PER_ISSUE = 5
# Deduplicate journal errors with the same "key" (first 80 chars of message)
_MAX_UNIQUE_ERROR_KEYS = 10


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _is_error_line(line: str) -> bool:
    """Return True if the line matches an error pattern and is not noise."""
    if any(p.search(line) for p in _JOURNAL_NOISE_PATTERNS):
        return False
    return any(p.search(line) for p in _JOURNAL_ERROR_PATTERNS)


def _truncate(s: str, n: int = 200) -> str:
    return s[:n] + ("…" if len(s) > n else "")


def _format_age(minutes: Optional[float]) -> str:
    if minutes is None:
        return "unknown age"
    if minutes < 60:
        return f"{minutes:.1f} min ago"
    return f"{minutes / 60:.1f} h ago"


# Threshold for "fresh log file" liveness check (hours).
# If the newest log file is within this window, watcher is considered alive.
_LIVENESS_LOG_FRESH_HOURS = 0.5  # 30 minutes


def _is_system_alive(raw: RawData, state: StateReport) -> tuple[bool, str]:
    """
    Determine whether the system shows credible liveness signals.

    Strategy: require BOTH primary signals to conclude "alive".
    A single signal alone may be stale or misleading.

    Primary signals (each independently strong):
      1. service_active=True   — systemd reports active/running
      2. fresh log files       — watcher wrote a log file within 30 min

    Secondary / weak signal (corroborating only, not sufficient alone):
      3. journal_lines present — journalctl returned output (may be old)

    Decision logic:
      - Both primaries present                     → alive=True
      - One primary + journal lines present        → alive=True  (borderline)
      - One primary only, no journal confirmation  → alive=False
      - No primaries                               → alive=False
    """
    reasons: List[str] = []
    primary_count = 0

    # Primary 1: systemd service active
    if state.get("service_active"):
        primary_count += 1
        reasons.append("service active (systemd)")

    # Primary 2: fresh log files written by watcher
    hours_since = state.get("hours_since_last_log")
    if hours_since is not None and hours_since <= _LIVENESS_LOG_FRESH_HOURS:
        primary_count += 1
        reasons.append(f"log files fresh ({hours_since:.2f}h ago)")

    # Secondary: journalctl has any output (weak corroboration only)
    has_journal = bool(raw.get("journal_lines"))

    if primary_count >= 2:
        alive = True
    elif primary_count == 1 and has_journal:
        alive = True
        reasons.append("journal output present (secondary)")
    else:
        alive = False
        if not reasons:
            reasons.append("no liveness signals detected")

    return alive, ", ".join(reasons)


# ---------------------------------------------------------------------------
# Detectors
# ---------------------------------------------------------------------------

def _detect_service_down(raw: RawData, state: StateReport) -> List[Issue]:
    """CRITICAL if pump-short.service is not active/running."""
    issues: List[Issue] = []
    try:
        if not state["service_active"]:
            status_line = state.get("service_status_line", "unknown")
            issues.append(Issue(
                issue_id="",  # assigned by caller
                type=IssueType.SERVICE_DOWN,
                severity="CRITICAL",
                source="systemctl status pump-short.service",
                description=(
                    "pump-short.service is NOT active/running. "
                    f"Current state: {status_line}"
                ),
                evidence=status_line,
                component="systemd",
                suggested_fix="Check service status and logs before any action. Do NOT restart without reviewing the cause.",
                check_cmd="systemctl status pump-short.service --no-pager",
                fix_cmd="",  # no auto-fix — could affect live positions
                fix_risk="Restarting without reviewing may close live positions or lose queue state.",
            ))
    except Exception as exc:
        issues.append(_make_detector_error("_detect_service_down", exc))
    return issues


def _detect_journal_errors(raw: RawData, state: StateReport) -> List[Issue]:
    """HIGH for each unique ERROR/EXCEPTION cluster found in journalctl."""
    issues: List[Issue] = []
    try:
        journal_lines: List[str] = raw.get("journal_lines", [])

        if not journal_lines:
            journal_error = raw.get("journal_error")
            if journal_error:
                issues.append(Issue(
                    issue_id="",
                    type=IssueType.JOURNAL_ERROR,
                    severity="HIGH",
                    source="journalctl",
                    description=f"journalctl unavailable or returned no output: {journal_error}",
                    evidence=journal_error,
                    component="systemd",
                    suggested_fix="Check journalctl permissions or run as root.",
                    check_cmd="journalctl -u pump-short.service -n 20 --no-pager",
                    fix_risk="",
                ))
            return issues

        # Collect error lines, deduplicate by 80-char key
        error_lines: List[str] = []
        seen_keys: set = set()
        for line in journal_lines:
            if not _is_error_line(line):
                continue
            key = line.strip()[:80]
            if key in seen_keys:
                continue
            seen_keys.add(key)
            error_lines.append(line.strip())
            if len(seen_keys) >= _MAX_UNIQUE_ERROR_KEYS:
                break

        if not error_lines:
            return issues

        # Group into one ISSUE per distinct error cluster (up to _MAX_ERROR_LINES_PER_ISSUE shown)
        # For MVP: one aggregated ISSUE for all journal errors
        shown = error_lines[:_MAX_ERROR_LINES_PER_ISSUE]
        total = len(error_lines)
        evidence = "\n".join(shown)
        if total > _MAX_ERROR_LINES_PER_ISSUE:
            evidence += f"\n… (+{total - _MAX_ERROR_LINES_PER_ISSUE} more unique error lines)"

        # Infer component from content
        component = _infer_component_from_lines(shown)

        issues.append(Issue(
            issue_id="",
            type=IssueType.JOURNAL_ERROR,
            severity="HIGH",
            source="journalctl -u pump-short.service",
            description=(
                f"Found {total} unique error line(s) in journalctl for pump-short.service."
            ),
            evidence=evidence,
            component=component,
            suggested_fix="Review full journal for root cause. Do not change code until cause is confirmed.",
            check_cmd=(
                'journalctl -u pump-short.service -n 200 --no-pager '
                '| grep -E "ERROR|EXCEPTION|Traceback|CRITICAL|FATAL"'
            ),
            fix_risk="Fix depends on specific error type. Review before acting.",
        ))
    except Exception as exc:
        issues.append(_make_detector_error("_detect_journal_errors", exc))
    return issues


def _detect_no_signal(raw: RawData, state: StateReport) -> List[Issue]:
    """MEDIUM if service is active but no log files newer than threshold."""
    issues: List[Issue] = []
    try:
        # Only fire if service appears active (otherwise SERVICE_DOWN covers it)
        if not state.get("service_active", False):
            return issues

        threshold_hours = float(state.get("no_signal_threshold_hours", 2.0))
        hours_since = state.get("hours_since_last_log")

        if hours_since is None:
            # No log files at all — might be fresh deployment or logs dir missing
            logs_dir = raw.get("logs_dir")
            if logs_dir and not logs_dir.is_dir():
                evidence = f"logs/ directory not found: {logs_dir}"
            else:
                evidence = "No *.csv files found in logs/"
            issues.append(Issue(
                issue_id="",
                type=IssueType.NO_SIGNAL,
                severity="MEDIUM",
                source="logs/",
                description=(
                    "Service is active but no log files found in logs/. "
                    "No pump events have been processed, or logs/ path is wrong."
                ),
                evidence=evidence,
                component="runtime",
                suggested_fix=(
                    "Check if the webhook endpoint is receiving pump events. "
                    "Verify STRATEGIES and pump_pct filters in .env."
                ),
                check_cmd=(
                    "ls -lth logs/ | head -20\n"
                    "journalctl -u pump-short.service --since '2h ago' --no-pager "
                    "| grep -c WATCH_START"
                ),
                fix_risk="Changing filters may affect signal frequency.",
            ))
            return issues

        if hours_since >= threshold_hours:
            last_file = state.get("last_log_file_name", "unknown")
            issues.append(Issue(
                issue_id="",
                type=IssueType.NO_SIGNAL,
                severity="MEDIUM",
                source=f"logs/{last_file}",
                description=(
                    f"Service is active but last log file is {hours_since:.1f}h old "
                    f"(threshold: {threshold_hours}h). "
                    "No new pump events have generated log activity."
                ),
                evidence=f"Newest log file: {last_file} — {hours_since:.1f}h ago",
                component="runtime",
                suggested_fix=(
                    "Check if the webhook is receiving pump events. "
                    "Verify exchange connectivity and pump_pct filter thresholds."
                ),
                check_cmd=(
                    f"ls -lth logs/ | head -20\n"
                    f"journalctl -u pump-short.service --since '{int(threshold_hours)}h ago' "
                    f"--no-pager | grep -c WATCH_START"
                ),
                fix_risk="Low. Read-only check.",
            ))
    except Exception as exc:
        issues.append(_make_detector_error("_detect_no_signal", exc))
    return issues


def _detect_runner_hang(raw: RawData, state: StateReport) -> List[Issue]:
    """
    Detect runner hang conditions.

    .processing file: CRITICAL always when stuck (> 5 min).
      Rationale: runner should never hold .processing for more than a few seconds
      under normal operation. Any value > 5 min is genuinely anomalous.

    .lock file: context-aware severity.
      trading_runner.lock is a long-lived process lock, NOT a per-signal lock.
      It persists for the lifetime of the runner and is refreshed on each timer
      invocation. A stale lock (> 30 min) is only concerning when the system
      shows NO liveness signals. If the service is active and logs are fresh,
      the stale lock is a LOW informational note, not a CRITICAL alert.

    Severity matrix:
      processing_stuck=True                       -> CRITICAL (always)
      lock_stale=True  + system NOT alive         -> CRITICAL
      lock_stale=True  + system alive             -> LOW (informational)
      lock_stale=False + processing_stuck=False   -> no issue
    """
    issues: List[Issue] = []
    try:
        datasets_dir = raw.get("datasets_dir")
        ds_str = str(datasets_dir) if datasets_dir else "datasets/"

        # ----------------------------------------------------------------
        # Branch A: .processing stuck — CRITICAL unconditionally.
        # This file is only present while the runner is actively processing
        # one signal. > 5 min always indicates a genuine hang.
        # ----------------------------------------------------------------
        if state.get("processing_stuck"):
            age_min = state.get("processing_age_minutes")
            age_str = _format_age(age_min)
            issues.append(Issue(
                issue_id="",
                type=IssueType.RUNNER_HANG,
                severity="CRITICAL",
                source=f"{ds_str}/signals_queue.processing",
                description=(
                    f"signals_queue.processing exists and is {age_str}. "
                    "The runner appears to be stuck processing a signal."
                ),
                evidence=f"signals_queue.processing age: {age_str}",
                component="runner",
                suggested_fix=(
                    "Verify the runner process is still running. "
                    "If confirmed dead, inspect the .processing file before removing."
                ),
                check_cmd=(
                    f"ls -lh {ds_str}/signals_queue.processing\n"
                    f"cat {ds_str}/signals_queue.processing\n"
                    f"ps aux | grep runner"
                ),
                fix_cmd=(
                    f"# Only if runner is confirmed dead and no live order is pending:\n"
                    f"# mv {ds_str}/signals_queue.processing "
                    f"{ds_str}/signals_queue.processing.bak"
                ),
                fix_risk="If runner is mid-order, removing .processing may cause duplicate orders.",
            ))

        # ----------------------------------------------------------------
        # Branch B: .lock stale — severity depends on system liveness.
        # Use lock_stale (preferred); fall back to lock_stuck for compat.
        # ----------------------------------------------------------------
        lock_is_stale = state.get("lock_stale") or state.get("lock_stuck", False)
        if lock_is_stale:
            age_min = state.get("lock_age_minutes")
            age_str = _format_age(age_min)
            alive, alive_reason = _is_system_alive(raw, state)

            if alive:
                # Lock is old but the system is demonstrably running.
                # This is the normal persistent-lock pattern — LOW only.
                issues.append(Issue(
                    issue_id="",
                    type=IssueType.RUNNER_HANG,
                    severity="LOW",
                    source=f"{ds_str}/trading_runner.lock",
                    description=(
                        f"trading_runner.lock is {age_str} old, "
                        f"but system liveness signals are present: {alive_reason}. "
                        "This is expected for a persistent lock held by a running process."
                    ),
                    evidence=(
                        f"lock age: {age_str}\n"
                        f"liveness: {alive_reason}"
                    ),
                    component="runner",
                    suggested_fix=(
                        "No action required. "
                        "Verify with 'ps aux | grep runner' if you want to confirm the process."
                    ),
                    check_cmd=(
                        f"ls -lh {ds_str}/trading_runner.lock\n"
                        f"ps aux | grep runner"
                    ),
                    fix_risk="Lock is held by a live process. Do not remove.",
                ))
            else:
                # Lock is stale AND no liveness signals — genuine hang.
                issues.append(Issue(
                    issue_id="",
                    type=IssueType.RUNNER_HANG,
                    severity="CRITICAL",
                    source=f"{ds_str}/trading_runner.lock",
                    description=(
                        f"trading_runner.lock is {age_str} old "
                        f"and NO system liveness signals detected ({alive_reason}). "
                        "Runner process appears to be dead or stuck."
                    ),
                    evidence=(
                        f"lock age: {age_str}\n"
                        f"liveness check: {alive_reason}"
                    ),
                    component="runner",
                    suggested_fix=(
                        "Check if runner process is alive. "
                        "If the lock is stale (no runner process), it can be removed."
                    ),
                    check_cmd=(
                        f"ls -lh {ds_str}/trading_runner.lock\n"
                        f"cat {ds_str}/trading_runner.lock\n"
                        f"ps aux | grep runner\n"
                        f"systemctl status pump-short.service --no-pager"
                    ),
                    fix_cmd=(
                        f"# Only after confirming NO runner process is active:\n"
                        f"# rm {ds_str}/trading_runner.lock"
                    ),
                    fix_risk=(
                        "Removing an active lock may cause concurrent runner "
                        "execution and duplicate orders."
                    ),
                ))

    except Exception as exc:
        issues.append(_make_detector_error("_detect_runner_hang", exc))
    return issues


def _detect_stuck_positions(raw: RawData, state: StateReport) -> List[Issue]:
    """HIGH for each live open position older than 24 hours."""
    issues: List[Issue] = []
    try:
        stuck = state.get("stuck_positions", [])
        datasets_dir = raw.get("datasets_dir")
        ds_str = str(datasets_dir) if datasets_dir else "datasets/"

        for pos in stuck:
            symbol = pos.get("symbol", "?")
            strategy = pos.get("strategy", "?")
            run_id = pos.get("run_id", "?")
            age_h = pos.get("age_hours")
            ts = pos.get("ts", "?")
            pos_id = pos.get("pos_id", "?")
            has_order = pos.get("order_id_present", False)
            age_str = f"{age_h:.1f}h" if age_h is not None else "unknown age"

            issues.append(Issue(
                issue_id="",
                type=IssueType.STUCK_POSITION,
                severity="HIGH",
                source="datasets/trading_state.json → open_positions",
                description=(
                    f"Live open position for {symbol} ({strategy}) is {age_str} old. "
                    f"Expected to be closed within normal outcome window. "
                    f"order_id present: {has_order}."
                ),
                evidence=(
                    f"pos_id: {pos_id}\n"
                    f"symbol: {symbol} | strategy: {strategy} | run_id: {run_id}\n"
                    f"opened_ts: {ts} | age: {age_str} | order_id: {has_order}"
                ),
                component="outcome",
                ts=ts,
                suggested_fix=(
                    "Check the position on Bybit. "
                    "If already closed on exchange but not in state.json, "
                    "outcome worker may have failed. "
                    "Do NOT manually edit state.json without confirming Bybit status."
                ),
                check_cmd=(
                    f"cat {ds_str}/trading_state.json | python3 -m json.tool "
                    f"| grep -A 20 \"{symbol}\"\n"
                    f"cat {ds_str}/trading_closes.csv | grep {symbol} | tail -5"
                ),
                fix_risk=(
                    "Manually removing a position from state.json may cause "
                    "orphan outcome records or missing PnL accounting."
                ),
            ))
    except Exception as exc:
        issues.append(_make_detector_error("_detect_stuck_positions", exc))
    return issues


def _detect_state_corrupt(raw: RawData, state: StateReport) -> List[Issue]:
    """HIGH if trading_state.json cannot be parsed or is missing."""
    issues: List[Issue] = []
    try:
        parse_error = raw.get("state_parse_error")
        if not parse_error:
            return issues

        state_path = raw.get("state_path", "datasets/trading_state.json")
        is_missing = "not found" in str(parse_error).lower()
        severity = "MEDIUM" if is_missing else "HIGH"
        description = (
            f"trading_state.json not found at {state_path}. "
            "This is normal on a fresh deployment, but unexpected in production."
            if is_missing else
            f"trading_state.json failed to parse: {parse_error}. "
            "Open positions and signal history are unreadable."
        )

        issues.append(Issue(
            issue_id="",
            type=IssueType.STATE_CORRUPT,
            severity=severity,
            source=state_path,
            description=description,
            evidence=str(parse_error),
            component="runner",
            suggested_fix=(
                "Inspect the file for corruption. "
                "If corrupt, restore from the most recent backup before any trading resumes."
            ),
            check_cmd=(
                f"cat {state_path} | python3 -m json.tool\n"
                f"ls -lh {state_path}"
            ),
            fix_risk=(
                "Do NOT overwrite state.json while the service is running. "
                "A corrupt state can cause duplicate orders or lost position tracking."
            ),
        ))
    except Exception as exc:
        issues.append(_make_detector_error("_detect_state_corrupt", exc))
    return issues


# ---------------------------------------------------------------------------
# Internal utilities
# ---------------------------------------------------------------------------

def _infer_component_from_lines(lines: List[str]) -> str:
    """Guess which component generated the error from log line content."""
    text = " ".join(lines).lower()
    if "fast0" in text or "fast0_sampler" in text:
        return "fast0"
    if "liquidation" in text or "liq_ws" in text or "websocket" in text:
        return "liquidations"
    if "runner" in text or "trading.runner" in text:
        return "runner"
    if "outcome" in text or "close_from" in text:
        return "outcome"
    if "watcher" in text or "run_watch" in text:
        return "watcher"
    if "runtime" in text or "server" in text:
        return "runtime"
    if "bybit" in text or "broker" in text:
        return "runner"
    return "unknown"


def _make_detector_error(detector_name: str, exc: Exception) -> Issue:
    """Create a LOW-severity issue for a detector that crashed."""
    return Issue(
        issue_id="",
        type="DETECTOR_ERROR",
        severity="LOW",
        source=f"auto_debug/{detector_name}",
        description=f"Detector {detector_name} failed internally: {exc}",
        evidence=str(exc),
        component="auto_debug",
        suggested_fix="This is a bug in the diagnostic tool, not the trading bot.",
    )


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def detect(
    raw: RawData,
    state: StateReport,
) -> List[Issue]:
    """
    Run all MVP detectors and return a sorted list of Issues.
    Detector failures are captured as LOW-severity issues, not exceptions.
    """
    all_issues: List[Issue] = []

    detectors = [
        _detect_service_down,
        _detect_journal_errors,
        _detect_no_signal,
        _detect_runner_hang,
        _detect_stuck_positions,
        _detect_state_corrupt,
    ]

    for detector in detectors:
        try:
            found = detector(raw, state)
            all_issues.extend(found)
        except Exception as exc:
            all_issues.append(_make_detector_error(detector.__name__, exc))

    # Assign stable issue IDs sorted by severity
    all_issues.sort(key=lambda i: (_SEVERITY_ORDER.get(i.severity, 99), i.type))
    for idx, issue in enumerate(all_issues, start=1):
        issue.issue_id = f"ISSUE-{idx}"

    return all_issues
