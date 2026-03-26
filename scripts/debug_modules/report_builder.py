"""
report_builder.py — MODULE 4
Format Issues + StateReport into a human-readable diagnosis report.

Output: plain text string, printed to stdout.
No file writes. No imports from short_pump/ or trading/.
"""
from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from scripts.debug_modules.issue_detector import Issue
from scripts.debug_modules.log_reader import RawData
from scripts.debug_modules.state_checker import StateReport


# ---------------------------------------------------------------------------
# Severity display helpers
# ---------------------------------------------------------------------------

_SEVERITY_ICON = {
    "CRITICAL": "🔴 CRITICAL",
    "HIGH":     "🟠 HIGH",
    "MEDIUM":   "🟡 MEDIUM",
    "LOW":      "🔵 LOW",
}

_SEP_THICK = "=" * 72
_SEP_THIN  = "-" * 72
_SEP_MID   = "·" * 72

# Root cause descriptions keyed by issue type
_ROOT_CAUSE_HINTS: Dict[str, str] = {
    "SERVICE_DOWN": (
        "pump-short.service is not running. This prevents all pump event processing. "
        "Possible causes: process crashed (check journalctl for last exit), "
        "systemd dependency failure, or manual stop."
    ),
    "JOURNAL_ERROR": (
        "Unhandled exceptions or errors in the bot process. "
        "Possible causes: API timeout/auth error, unexpected data format, "
        "missing environment variable, or bug triggered by a specific market event."
    ),
    "NO_SIGNAL": (
        "No new pump events have been processed recently. "
        "Possible causes: no pumps passed the exchange/pump_pct filter, "
        "webhook endpoint not receiving events, or network connectivity issue."
    ),
    "RUNNER_HANG": (
        "The trading runner is stuck on a lock or processing file. "
        "Possible causes: runner crashed mid-execution leaving a stale lock, "
        "deadlock between runner and outcome worker, or slow Bybit API call blocking the runner."
    ),
    "STUCK_POSITION": (
        "A live position remains open in state.json longer than expected. "
        "Possible causes: outcome worker failed to resolve Bybit closed-pnl, "
        "position_idx missing so live outcome resolver returned UNKNOWN_LIVE, "
        "or state.json was not updated after exchange close."
    ),
    "STATE_CORRUPT": (
        "trading_state.json is unreadable. "
        "Possible causes: disk full during write (partial JSON), concurrent write without atomic rename, "
        "or manual file corruption."
    ),
}

# Standard check commands — always included at the end
_STANDARD_CHECK_COMMANDS = """\
# 1. Service status
systemctl status pump-short.service --no-pager

# 2. Last 200 lines with errors only
journalctl -u pump-short.service -n 200 --no-pager \\
  | grep -E "ERROR|EXCEPTION|Traceback|CRITICAL|FATAL"

# 3. Open positions in state
cat datasets/trading_state.json | python3 -m json.tool \\
  | grep -A 10 "open_positions"

# 4. Check for stale lock file
find datasets/ -name "*.lock" -mmin +2 -ls
find datasets/ -name "*.processing" -mmin +5 -ls

# 5. Signal queue depth
wc -l datasets/signals_queue.jsonl 2>/dev/null || echo "queue empty/missing"
cat datasets/signals_queue.jsonl | tail -5

# 6. Newest log files
ls -lth logs/ | head -20

# 7. Recent trades
tail -5 datasets/trading_trades.csv 2>/dev/null || echo "no trades file"

# 8. Recent closes
tail -5 datasets/trading_closes.csv 2>/dev/null || echo "no closes file"

# 9. Full journal since 2h ago
journalctl -u pump-short.service --since "2h ago" --no-pager | tail -100"""


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _ts_now_utc() -> str:
    return datetime.now(tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def _epoch_to_str(epoch: Optional[float]) -> str:
    if epoch is None:
        return "unknown"
    try:
        dt = datetime.fromtimestamp(epoch, tz=timezone.utc)
        return dt.strftime("%Y-%m-%d %H:%M:%S UTC")
    except Exception:
        return "unknown"


def _indent(text: str, prefix: str = "  ") -> str:
    return "\n".join(prefix + line for line in text.splitlines())


def _section(title: str) -> str:
    return f"\n{_SEP_THIN}\n## {title}\n{_SEP_THIN}"


def _subsection(title: str) -> str:
    return f"\n### {title}"


# ---------------------------------------------------------------------------
# Section builders
# ---------------------------------------------------------------------------

def _build_header(raw: RawData, state: StateReport, issues: List[Issue]) -> str:
    lines = [
        _SEP_THICK,
        "DIAGNOSIS REPORT — TASK_B54_AUTO_DEBUG_PIPELINE",
        _SEP_THICK,
    ]
    return "\n".join(lines)


def _build_summary(raw: RawData, state: StateReport, issues: List[Issue]) -> str:
    collection_ts = raw.get("collection_ts", time.time())
    ts_str = _epoch_to_str(collection_ts)

    svc_line = state.get("service_status_line", "unknown")
    svc_ok = state.get("service_active", False)
    svc_display = f"{'✅ active' if svc_ok else '❌ NOT active'} — {svc_line}"

    last_sig = state.get("last_signal_ts") or "unknown"
    last_sym = state.get("last_signal_symbol") or ""
    last_src = state.get("last_signal_source") or ""
    hours_log = state.get("hours_since_last_log")
    hours_str = f"{hours_log:.1f}h ago" if hours_log is not None else "unknown"

    open_cnt = state.get("open_positions_count", 0)
    queue_depth = state.get("queue_depth", 0)

    n_issues = len(issues)
    n_critical = sum(1 for i in issues if i.severity == "CRITICAL")
    n_high = sum(1 for i in issues if i.severity == "HIGH")
    n_medium = sum(1 for i in issues if i.severity == "MEDIUM")
    n_low = sum(1 for i in issues if i.severity == "LOW")

    state_valid = state.get("state_valid", False)
    state_str = "✅ valid" if state_valid else "❌ unreadable / missing"

    lines = [
        _section("Summary"),
        f"  Analysis time   : {ts_str}",
        f"  Project root    : {raw.get('project_root', 'unknown')}",
        "",
        f"  Service status  : {svc_display}",
        f"  trading_state   : {state_str}",
        f"  Open positions  : {open_cnt}",
        f"  Queue depth     : {queue_depth} signal(s)",
        f"  Last activity   : {last_sig}"
        + (f"  [{last_sym}]" if last_sym else "")
        + (f"  via {last_src}" if last_src else ""),
        f"  Last log file   : {hours_str}",
        "",
        f"  Issues found    : {n_issues}  "
        f"(CRITICAL={n_critical} HIGH={n_high} MEDIUM={n_medium} LOW={n_low})",
    ]

    if n_issues == 0:
        lines.append("\n  ✅ No issues detected. System appears healthy.")
    elif n_critical > 0:
        lines.append("\n  ⚠️  CRITICAL issues require immediate attention.")
    elif n_high > 0:
        lines.append("\n  ⚠️  HIGH severity issues detected — review recommended.")

    return "\n".join(lines)


def _build_issues(issues: List[Issue]) -> str:
    if not issues:
        return _section("Issues Found") + "\n  ✅ No issues found."

    lines = [_section("Issues Found")]
    for issue in issues:
        sev_icon = _SEVERITY_ICON.get(issue.severity, issue.severity)
        lines.append(_subsection(f"{issue.issue_id}: {issue.type}"))
        lines.append(f"  Severity  : {sev_icon}")
        lines.append(f"  Component : {issue.component}")
        lines.append(f"  Source    : {issue.source}")
        if issue.ts:
            lines.append(f"  Timestamp : {issue.ts}")
        lines.append(f"  Description:")
        lines.append(_indent(issue.description, "    "))
        lines.append(f"  Evidence:")
        lines.append(_indent(issue.evidence, "    "))

    return "\n".join(lines)


def _build_root_cause(issues: List[Issue]) -> str:
    if not issues:
        return _section("Root Cause Analysis") + "\n  ✅ No root causes identified — system healthy."

    # Only emit root cause for CRITICAL and HIGH
    significant = [i for i in issues if i.severity in ("CRITICAL", "HIGH")]
    if not significant:
        significant = issues  # fall back to all if only MEDIUM/LOW

    lines = [_section("Root Cause Analysis")]
    for issue in significant:
        hint = _ROOT_CAUSE_HINTS.get(issue.type, "No specific root cause hint for this issue type.")
        lines.append(_subsection(f"Root Cause for {issue.issue_id} ({issue.type})"))
        lines.append(f"  Hypothesis:")
        lines.append(_indent(hint, "    "))
        lines.append(f"  Evidence in logs:")
        lines.append(_indent(issue.evidence[:300], "    "))
        lines.append(f"  Related component : {issue.component}")

    return "\n".join(lines)


def _build_suggested_fixes(issues: List[Issue]) -> str:
    if not issues:
        return _section("Suggested Fixes") + "\n  ✅ No fixes needed."

    lines = [_section("Suggested Fixes")]
    for issue in issues:
        sev_icon = _SEVERITY_ICON.get(issue.severity, issue.severity)
        lines.append(_subsection(f"Fix for {issue.issue_id} ({issue.type}) — {sev_icon}"))
        if issue.suggested_fix:
            lines.append(f"  Action:")
            lines.append(_indent(issue.suggested_fix, "    "))
        if issue.check_cmd:
            lines.append(f"  Verification command(s):")
            lines.append("    ```bash")
            for cmd_line in issue.check_cmd.splitlines():
                lines.append(f"    {cmd_line}")
            lines.append("    ```")
        if issue.fix_cmd:
            lines.append(f"  Fix command (use only after verification):")
            lines.append("    ```bash")
            for cmd_line in issue.fix_cmd.splitlines():
                lines.append(f"    {cmd_line}")
            lines.append("    ```")
        if issue.fix_risk:
            lines.append(f"  ⚠️  Risk: {issue.fix_risk}")

    return "\n".join(lines)


def _build_state_detail(state: StateReport) -> str:
    lines = [_section("State Detail")]

    # Build a set of stuck pos_ids for O(1) lookup
    stuck_ids = {
        p.get("pos_id", "")
        for p in state.get("stuck_positions", [])
        if isinstance(p, dict)
    }

    # Open positions table
    detail: List[Dict[str, Any]] = state.get("open_positions_detail", [])
    if detail:
        lines.append(f"\n  Open positions ({len(detail)} total):")
        for pos in detail:
            age_h = pos.get("age_hours")
            age_str = f"{age_h:.1f}h" if age_h is not None else "?"
            is_stuck = pos.get("pos_id", "") in stuck_ids
            
            flag = " ⚠️ STUCK" if is_stuck else ""
            
            # Only compute/show confidence for stuck positions
            confidence_label = ""
            if is_stuck:
                in_closes = pos.get("in_closes", False)
                has_order = pos.get("order_id_present", False)
                low_signals = []
                if in_closes:
                    low_signals.append("in_closes")
                if not has_order:
                    low_signals.append("no_order_id")
                confidence = "MEDIUM" if len(low_signals) >= 2 else "HIGH"
                confidence_label = f" [{confidence}]"
            
            lines.append(
                f"    {pos.get('strategy','?'):25s}  {pos.get('symbol','?'):15s}  "
                f"mode={pos.get('mode','?'):5s}  age={age_str:8s}  "
                f"order_id={'✅' if pos.get('order_id_present') else '❌'}{confidence_label}{flag}"
            )
    else:
        lines.append("\n  Open positions: none")

    # Queue
    queue_depth = state.get("queue_depth", 0)
    lines.append(f"\n  Signal queue depth : {queue_depth}")

    # Lock / processing
    lock_age = state.get("lock_age_minutes")
    proc_age = state.get("processing_age_minutes")
    lock_label = "exists" if (state.get("lock_stuck") or lock_age is not None) else "clean"
    proc_label = "exists" if (state.get("processing_stuck") or proc_age is not None) else "clean"
    lines.append(
        f"  Lock file          : {lock_label}"
        + (f" ({lock_age:.1f} min old)" if lock_age is not None else "")
    )
    lines.append(
        f"  Processing file    : {proc_label}"
        + (f" ({proc_age:.1f} min old)" if proc_age is not None else "")
    )

    return "\n".join(lines)


def _build_check_commands() -> str:
    lines = [_section("Check Commands (Standard Set)")]
    lines.append("")
    lines.append("  ```bash")
    for cmd_line in _STANDARD_CHECK_COMMANDS.splitlines():
        lines.append(f"  {cmd_line}")
    lines.append("  ```")
    return "\n".join(lines)


def _build_footer(issues: List[Issue]) -> str:
    n = len(issues)
    n_critical = sum(1 for i in issues if i.severity == "CRITICAL")
    lines = [
        "",
        _SEP_THICK,
        f"END OF REPORT  |  {_ts_now_utc()}  |  {n} issue(s) found",
    ]
    if n_critical > 0:
        lines.append("⚠️  CRITICAL issues present — do not ignore.")
    lines.append(
        "NOTE: This tool is read-only. No code, config, or data was modified."
    )
    lines.append(_SEP_THICK)
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def build(
    raw: RawData,
    state: StateReport,
    issues: List[Issue],
    include_state_detail: bool = True,
) -> str:
    """
    Assemble the full diagnosis report as a plain-text string.
    Never raises.
    """
    sections = [
        _build_header(raw, state, issues),
        _build_summary(raw, state, issues),
        _build_issues(issues),
        _build_root_cause(issues),
        _build_suggested_fixes(issues),
    ]
    if include_state_detail:
        sections.append(_build_state_detail(state))
    sections.append(_build_check_commands())
    sections.append(_build_footer(issues))
    return "\n".join(sections)
