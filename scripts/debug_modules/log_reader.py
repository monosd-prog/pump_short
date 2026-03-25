"""
log_reader.py — MODULE 1
Collect raw data from all diagnostic sources.

Read-only. No imports from short_pump/ or trading/.
All errors are captured and surfaced in the result dict, never raised.
"""
from __future__ import annotations

import csv
import json
import os
import subprocess
import time
from pathlib import Path
from typing import Any, Dict, List, Optional


# ---------------------------------------------------------------------------
# Public types
# ---------------------------------------------------------------------------

RawData = Dict[str, Any]
"""
Keys:
  project_root        : Path
  datasets_dir        : Path
  logs_dir            : Path
  collection_ts       : float          — time.time() at collection start
  service_status      : str            — stdout of systemctl status
  service_status_error: str | None
  journal_lines       : list[str]      — last N lines from journalctl
  journal_error       : str | None
  log_files           : list[dict]     — [{path, mtime, size_kb, symbol, log_type}]
  state_json          : dict | None
  state_parse_error   : str | None
  state_path          : str
  queue_lines         : list[str]
  queue_path          : str
  processing_exists   : bool
  processing_mtime    : float | None
  lock_exists         : bool
  lock_mtime          : float | None
  trades_rows         : list[dict]
  closes_rows         : list[dict]
"""

_LOG_TYPE_MAP = {
    "_5m.csv": "5m",
    "_1m.csv": "1m",
    "_fast.csv": "fast",
    "_summary.csv": "summary",
}

_JOURNAL_LINES = 500


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _resolve_project_root() -> Path:
    """Resolve project root: walk up from this file looking for datasets/."""
    here = Path(__file__).resolve()
    # scripts/debug_modules/log_reader.py → project root is two levels up
    candidate = here.parent.parent.parent
    if (candidate / "datasets").is_dir():
        return candidate
    # Fallback: check cwd
    cwd = Path(os.getcwd())
    if (cwd / "datasets").is_dir():
        return cwd
    # VPS fallback
    vps = Path("/root/pump_short")
    if vps.exists():
        return vps
    return cwd


def _run_cmd(args: List[str], timeout: int = 15) -> tuple[str, Optional[str]]:
    """Run a shell command. Returns (stdout, error_str_or_None)."""
    try:
        result = subprocess.run(
            args,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
        stderr = result.stderr.strip() if result.stderr else None
        return result.stdout, stderr
    except FileNotFoundError:
        return "", f"command not found: {args[0]}"
    except subprocess.TimeoutExpired:
        return "", f"timeout after {timeout}s: {' '.join(args)}"
    except Exception as exc:
        return "", f"unexpected error: {exc}"


def _parse_log_file_meta(path: Path) -> Dict[str, Any]:
    """Extract symbol and log_type from a logs/*.csv filename."""
    name = path.name  # e.g. 20260304_083130_SIRENUSDT_fast.csv
    symbol = ""
    log_type = "unknown"
    for suffix, ltype in _LOG_TYPE_MAP.items():
        if name.endswith(suffix):
            log_type = ltype
            # strip run_id prefix (YYYYMMDD_HHMMSS_) + suffix
            core = name[: -len(suffix)]  # e.g. 20260304_083130_SIRENUSDT
            parts = core.split("_")
            # run_id is parts[0] (date) + parts[1] (time) → symbol is the rest
            if len(parts) >= 3:
                symbol = "_".join(parts[2:])
            break
    try:
        stat = path.stat()
        mtime = stat.st_mtime
        size_kb = round(stat.st_size / 1024, 1)
    except OSError:
        mtime = 0.0
        size_kb = 0.0
    return {
        "path": str(path),
        "name": name,
        "mtime": mtime,
        "size_kb": size_kb,
        "symbol": symbol,
        "log_type": log_type,
    }


def _read_csv_tail(path: Path, n: int = 50) -> List[Dict[str, Any]]:
    """Read last N rows of a CSV file as list of dicts. Returns [] on error."""
    if not path.exists():
        return []
    try:
        rows = []
        with path.open(encoding="utf-8", errors="replace") as fh:
            reader = csv.DictReader(fh)
            for row in reader:
                rows.append(dict(row))
        return rows[-n:]
    except Exception:
        return []


def _read_jsonl(path: Path, max_lines: int = 200) -> List[str]:
    """Read a JSONL file, return raw lines (stripped). Returns [] on error."""
    if not path.exists():
        return []
    try:
        lines = path.read_text(encoding="utf-8", errors="replace").strip().splitlines()
        return [ln for ln in lines if ln.strip()][-max_lines:]
    except Exception:
        return []


def _file_mtime(path: Path) -> Optional[float]:
    """Return mtime of path or None if it doesn't exist."""
    try:
        return path.stat().st_mtime if path.exists() else None
    except OSError:
        return None


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def collect(
    project_root: Optional[Path] = None,
    journal_lines: int = _JOURNAL_LINES,
) -> RawData:
    """
    Collect all raw diagnostic data. Never raises.
    All collection errors are stored in the returned dict.
    """
    if project_root is None:
        project_root = _resolve_project_root()

    datasets_dir = project_root / "datasets"
    logs_dir = project_root / "logs"
    collection_ts = time.time()

    data: RawData = {
        "project_root": project_root,
        "datasets_dir": datasets_dir,
        "logs_dir": logs_dir,
        "collection_ts": collection_ts,
        # service
        "service_status": "",
        "service_status_error": None,
        # journalctl
        "journal_lines": [],
        "journal_error": None,
        # logs/
        "log_files": [],
        # state
        "state_json": None,
        "state_parse_error": None,
        "state_path": str(datasets_dir / "trading_state.json"),
        # queue
        "queue_lines": [],
        "queue_path": str(datasets_dir / "signals_queue.jsonl"),
        "processing_exists": False,
        "processing_mtime": None,
        # lock
        "lock_exists": False,
        "lock_mtime": None,
        # trades / closes
        "trades_rows": [],
        "closes_rows": [],
    }

    # 1. systemctl status
    try:
        stdout, err = _run_cmd(["systemctl", "status", "pump-short.service", "--no-pager"])
        data["service_status"] = stdout.strip()
        if err:
            data["service_status_error"] = err
    except Exception as exc:
        data["service_status_error"] = str(exc)

    # 2. journalctl
    try:
        stdout, err = _run_cmd(
            [
                "journalctl",
                "-u", "pump-short.service",
                f"-n", str(journal_lines),
                "--no-pager",
                "--output=short-iso",
            ],
            timeout=20,
        )
        if err and "No entries" not in (err or "") and stdout.strip() == "":
            data["journal_error"] = err
        lines = [ln for ln in stdout.splitlines() if ln.strip()]
        data["journal_lines"] = lines
    except Exception as exc:
        data["journal_error"] = str(exc)

    # 3. logs/*.csv — metadata only (no full read)
    try:
        if logs_dir.is_dir():
            log_files = []
            for p in sorted(logs_dir.glob("*.csv"), key=lambda f: f.stat().st_mtime if f.exists() else 0, reverse=True):
                log_files.append(_parse_log_file_meta(p))
            data["log_files"] = log_files
    except Exception as exc:
        data["log_files"] = []
        data["log_files_error"] = str(exc)

    # 4. trading_state.json
    state_path = datasets_dir / "trading_state.json"
    data["state_path"] = str(state_path)
    try:
        if state_path.exists():
            raw = state_path.read_text(encoding="utf-8", errors="replace")
            data["state_json"] = json.loads(raw)
        else:
            data["state_parse_error"] = f"file not found: {state_path}"
    except json.JSONDecodeError as exc:
        data["state_parse_error"] = f"JSON parse error: {exc}"
    except Exception as exc:
        data["state_parse_error"] = str(exc)

    # 5. signals_queue.jsonl
    queue_path = datasets_dir / "signals_queue.jsonl"
    data["queue_path"] = str(queue_path)
    try:
        data["queue_lines"] = _read_jsonl(queue_path)
    except Exception as exc:
        data["queue_lines"] = []
        data["queue_error"] = str(exc)

    # 6. signals_queue.processing
    processing_path = datasets_dir / "signals_queue.processing"
    try:
        data["processing_exists"] = processing_path.exists()
        data["processing_mtime"] = _file_mtime(processing_path)
    except Exception:
        data["processing_exists"] = False
        data["processing_mtime"] = None

    # 7. trading_runner.lock
    lock_path = datasets_dir / "trading_runner.lock"
    try:
        data["lock_exists"] = lock_path.exists()
        data["lock_mtime"] = _file_mtime(lock_path)
    except Exception:
        data["lock_exists"] = False
        data["lock_mtime"] = None

    # 8. trading_trades.csv (last 50 rows)
    try:
        data["trades_rows"] = _read_csv_tail(datasets_dir / "trading_trades.csv", n=50)
    except Exception as exc:
        data["trades_rows"] = []
        data["trades_error"] = str(exc)

    # 9. trading_closes.csv (last 50 rows)
    try:
        closes_paths = list(datasets_dir.glob("trading_closes*.csv"))
        if closes_paths:
            # prefer the most recently modified
            closes_paths.sort(key=lambda p: p.stat().st_mtime, reverse=True)
            data["closes_rows"] = _read_csv_tail(closes_paths[0], n=50)
        else:
            data["closes_rows"] = []
    except Exception as exc:
        data["closes_rows"] = []
        data["closes_error"] = str(exc)

    return data
