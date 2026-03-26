"""
Backfill missing risk_profile for legacy paper outcome rows.

Root cause: Feb 26 – Mar 6 2026, track_outcome() did not populate risk_profile
in summary; build_outcome_row() wrote empty string. Not an active writer bug.

Usage:
  python3 scripts/backfill_risk_profile_paper.py          # dry-run (default)
  python3 scripts/backfill_risk_profile_paper.py --apply  # write changes

Backups:  <file>.bak.YYYYMMDD_HHMMSS  (one per affected CSV, before first write)
Audit:    reports/backfill_risk_profile_paper_audit_YYYYMMDD_HHMMSS.jsonl
"""

from __future__ import annotations

import argparse
import csv
import io
import json
import os
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

# ---------------------------------------------------------------------------
# Hardcoded candidates (52 rows confirmed by RCA scan)
# Composite key: file + run_id + symbol + event_id
# ---------------------------------------------------------------------------
CANDIDATES: List[Dict[str, str]] = [
    # --- short_pump_fast0 (3 rows) --- reason: legacy_no_bucket_assigned_default
    {"file": "datasets/date=20260226/strategy=short_pump_fast0/mode=paper/outcomes_v3.csv", "run_id": "20260226_232255", "symbol": "POWERUSDT", "event_id": "20260226_232255_fast0_3_b381678b", "risk_profile": "fast0_base_1R", "reason": "legacy_no_bucket_assigned_default"},
    {"file": "datasets/date=20260226/strategy=short_pump_fast0/mode=paper/outcomes_v3.csv", "run_id": "20260226_232515", "symbol": "ARPAUSDT", "event_id": "20260226_232515_fast0_8_87b3843d", "risk_profile": "fast0_base_1R", "reason": "legacy_no_bucket_assigned_default"},
    {"file": "datasets/date=20260226/strategy=short_pump_fast0/mode=paper/outcomes_v3.csv", "run_id": "20260226_233340", "symbol": "SKLUSDT", "event_id": "20260226_233340_fast0_5_78f2200c", "risk_profile": "fast0_base_1R", "reason": "legacy_no_bucket_assigned_default"},
    # --- short_pump (49 rows) --- reason: legacy_single_profile_period
    {"file": "datasets/date=20260227/strategy=short_pump/mode=paper/outcomes_v3.csv", "run_id": "20260227_121015", "symbol": "ROBOUSDT", "event_id": "20260227_121015_entry_fast", "risk_profile": "short_pump_active_1R", "reason": "legacy_single_profile_period"},
    {"file": "datasets/date=20260227/strategy=short_pump/mode=paper/outcomes_v3.csv", "run_id": "20260227_140703", "symbol": "AIXBTUSDT", "event_id": "20260227_140703_entry_fast", "risk_profile": "short_pump_active_1R", "reason": "legacy_single_profile_period"},
    {"file": "datasets/date=20260227/strategy=short_pump/mode=paper/outcomes_v3.csv", "run_id": "20260227_135315", "symbol": "HOLOUSDT", "event_id": "20260227_135315_entry_fast", "risk_profile": "short_pump_active_1R", "reason": "legacy_single_profile_period"},
    {"file": "datasets/date=20260227/strategy=short_pump/mode=paper/outcomes_v3.csv", "run_id": "20260227_141355", "symbol": "SAHARAUSDT", "event_id": "20260227_141355_entry_fast", "risk_profile": "short_pump_active_1R", "reason": "legacy_single_profile_period"},
    {"file": "datasets/date=20260227/strategy=short_pump/mode=paper/outcomes_v3.csv", "run_id": "20260227_152455", "symbol": "SOLAYERUSDT", "event_id": "20260227_152455_entry_fast", "risk_profile": "short_pump_active_1R", "reason": "legacy_single_profile_period"},
    {"file": "datasets/date=20260227/strategy=short_pump/mode=paper/outcomes_v3.csv", "run_id": "20260227_172425", "symbol": "LYNUSDT", "event_id": "20260227_172425_entry_fast", "risk_profile": "short_pump_active_1R", "reason": "legacy_single_profile_period"},
    {"file": "datasets/date=20260227/strategy=short_pump/mode=paper/outcomes_v3.csv", "run_id": "20260227_191810", "symbol": "PIPPINUSDT", "event_id": "20260227_191810_entry_fast", "risk_profile": "short_pump_active_1R", "reason": "legacy_single_profile_period"},
    {"file": "datasets/date=20260227/strategy=short_pump/mode=paper/outcomes_v3.csv", "run_id": "20260227_193455", "symbol": "ROBOUSDT", "event_id": "20260227_193455_entry_fast", "risk_profile": "short_pump_active_1R", "reason": "legacy_single_profile_period"},
    {"file": "datasets/date=20260227/strategy=short_pump/mode=paper/outcomes_v3.csv", "run_id": "20260227_210821", "symbol": "CAMPUSDT", "event_id": "20260227_210821_entry_fast", "risk_profile": "short_pump_active_1R", "reason": "legacy_single_profile_period"},
    {"file": "datasets/date=20260227/strategy=short_pump/mode=paper/outcomes_v3.csv", "run_id": "20260227_210905", "symbol": "PORTALUSDT", "event_id": "20260227_210905_entry_fast", "risk_profile": "short_pump_active_1R", "reason": "legacy_single_profile_period"},
    {"file": "datasets/date=20260228/strategy=short_pump/mode=paper/outcomes_v3.csv", "run_id": "20260228_023105", "symbol": "SIGNUSDT", "event_id": "20260228_023105_entry_fast", "risk_profile": "short_pump_active_1R", "reason": "legacy_single_profile_period"},
    {"file": "datasets/date=20260228/strategy=short_pump/mode=paper/outcomes_v3.csv", "run_id": "20260228_030935", "symbol": "ATHUSDT", "event_id": "20260228_030935_entry_fast", "risk_profile": "short_pump_active_1R", "reason": "legacy_single_profile_period"},
    {"file": "datasets/date=20260228/strategy=short_pump/mode=paper/outcomes_v3.csv", "run_id": "20260228_112135", "symbol": "POWERUSDT", "event_id": "20260228_112135_entry_fast", "risk_profile": "short_pump_active_1R", "reason": "legacy_single_profile_period"},
    {"file": "datasets/date=20260228/strategy=short_pump/mode=paper/outcomes_v3.csv", "run_id": "20260228_135405", "symbol": "SIRENUSDT", "event_id": "20260228_135405_entry_fast", "risk_profile": "short_pump_active_1R", "reason": "legacy_single_profile_period"},
    {"file": "datasets/date=20260228/strategy=short_pump/mode=paper/outcomes_v3.csv", "run_id": "20260228_165830", "symbol": "POWERUSDT", "event_id": "20260228_165830_entry_fast", "risk_profile": "short_pump_active_1R", "reason": "legacy_single_profile_period"},
    {"file": "datasets/date=20260228/strategy=short_pump/mode=paper/outcomes_v3.csv", "run_id": "20260301_011150", "symbol": "FIOUSDT", "event_id": "20260301_011150_entry_fast", "risk_profile": "short_pump_active_1R", "reason": "legacy_single_profile_period"},
    {"file": "datasets/date=20260301/strategy=short_pump/mode=paper/outcomes_v3.csv", "run_id": "20260301_052750", "symbol": "HPOS10IUSDT", "event_id": "20260301_052750_entry_fast", "risk_profile": "short_pump_active_1R", "reason": "legacy_single_profile_period"},
    {"file": "datasets/date=20260301/strategy=short_pump/mode=paper/outcomes_v3.csv", "run_id": "20260301_114445", "symbol": "SAHARAUSDT", "event_id": "20260301_114445_entry_fast", "risk_profile": "short_pump_active_1R", "reason": "legacy_single_profile_period"},
    {"file": "datasets/date=20260301/strategy=short_pump/mode=paper/outcomes_v3.csv", "run_id": "20260301_230100", "symbol": "ALICEUSDT", "event_id": "20260301_230100_entry_fast", "risk_profile": "short_pump_active_1R", "reason": "legacy_single_profile_period"},
    {"file": "datasets/date=20260302/strategy=short_pump/mode=paper/outcomes_v3.csv", "run_id": "20260302_023520", "symbol": "FIOUSDT", "event_id": "20260302_023520_entry_fast", "risk_profile": "short_pump_active_1R", "reason": "legacy_single_profile_period"},
    {"file": "datasets/date=20260302/strategy=short_pump/mode=paper/outcomes_v3.csv", "run_id": "20260302_030015", "symbol": "IMXUSDT", "event_id": "20260302_030015_entry_fast", "risk_profile": "short_pump_active_1R", "reason": "legacy_single_profile_period"},
    {"file": "datasets/date=20260302/strategy=short_pump/mode=paper/outcomes_v3.csv", "run_id": "20260302_034450", "symbol": "BSUUSDT", "event_id": "20260302_034450_entry_fast", "risk_profile": "short_pump_active_1R", "reason": "legacy_single_profile_period"},
    {"file": "datasets/date=20260302/strategy=short_pump/mode=paper/outcomes_v3.csv", "run_id": "20260302_105800", "symbol": "HIPPOUSDT", "event_id": "20260302_105800_entry_fast", "risk_profile": "short_pump_active_1R", "reason": "legacy_single_profile_period"},
    {"file": "datasets/date=20260302/strategy=short_pump/mode=paper/outcomes_v3.csv", "run_id": "20260302_115715", "symbol": "ROBOUSDT", "event_id": "20260302_115715_entry_fast", "risk_profile": "short_pump_active_1R", "reason": "legacy_single_profile_period"},
    {"file": "datasets/date=20260302/strategy=short_pump/mode=paper/outcomes_v3.csv", "run_id": "20260302_113535", "symbol": "PHAUSDT", "event_id": "20260302_113535_entry_fast", "risk_profile": "short_pump_active_1R", "reason": "legacy_single_profile_period"},
    {"file": "datasets/date=20260302/strategy=short_pump/mode=paper/outcomes_v3.csv", "run_id": "20260302_120030", "symbol": "ARCUSDT", "event_id": "20260302_120030_entry_fast", "risk_profile": "short_pump_active_1R", "reason": "legacy_single_profile_period"},
    {"file": "datasets/date=20260302/strategy=short_pump/mode=paper/outcomes_v3.csv", "run_id": "20260302_155255", "symbol": "ARCUSDT", "event_id": "20260302_155255_entry_fast", "risk_profile": "short_pump_active_1R", "reason": "legacy_single_profile_period"},
    {"file": "datasets/date=20260302/strategy=short_pump/mode=paper/outcomes_v3.csv", "run_id": "20260302_182430", "symbol": "GRASSUSDT", "event_id": "20260302_182430_entry_fast", "risk_profile": "short_pump_active_1R", "reason": "legacy_single_profile_period"},
    {"file": "datasets/date=20260302/strategy=short_pump/mode=paper/outcomes_v3.csv", "run_id": "20260302_205010", "symbol": "NILUSDT", "event_id": "20260302_205010_entry_fast", "risk_profile": "short_pump_active_1R", "reason": "legacy_single_profile_period"},
    {"file": "datasets/date=20260302/strategy=short_pump/mode=paper/outcomes_v3.csv", "run_id": "20260302_230050", "symbol": "POWERUSDT", "event_id": "20260302_230050_entry_fast", "risk_profile": "short_pump_active_1R", "reason": "legacy_single_profile_period"},
    {"file": "datasets/date=20260302/strategy=short_pump/mode=paper/outcomes_v3.csv", "run_id": "20260303_015945", "symbol": "BOBAUSDT", "event_id": "20260303_015945_entry_fast", "risk_profile": "short_pump_active_1R", "reason": "legacy_single_profile_period"},
    {"file": "datasets/date=20260303/strategy=short_pump/mode=paper/outcomes_v3.csv", "run_id": "20260303_034725", "symbol": "KITEUSDT", "event_id": "20260303_034725_entry_fast", "risk_profile": "short_pump_active_1R", "reason": "legacy_single_profile_period"},
    {"file": "datasets/date=20260303/strategy=short_pump/mode=paper/outcomes_v3.csv", "run_id": "20260303_051722", "symbol": "4USDT", "event_id": "20260303_051722_entry_fast", "risk_profile": "short_pump_active_1R", "reason": "legacy_single_profile_period"},
    {"file": "datasets/date=20260303/strategy=short_pump/mode=paper/outcomes_v3.csv", "run_id": "20260303_095800", "symbol": "BOBAUSDT", "event_id": "20260303_095800_entry_fast", "risk_profile": "short_pump_active_1R", "reason": "legacy_single_profile_period"},
    {"file": "datasets/date=20260303/strategy=short_pump/mode=paper/outcomes_v3.csv", "run_id": "20260303_111320", "symbol": "ARCUSDT", "event_id": "20260303_111320_entry_fast", "risk_profile": "short_pump_active_1R", "reason": "legacy_single_profile_period"},
    {"file": "datasets/date=20260303/strategy=short_pump/mode=paper/outcomes_v3.csv", "run_id": "20260303_162905", "symbol": "TAIUSDT", "event_id": "20260303_162905_entry_fast", "risk_profile": "short_pump_active_1R", "reason": "legacy_single_profile_period"},
    {"file": "datasets/date=20260303/strategy=short_pump/mode=paper/outcomes_v3.csv", "run_id": "20260303_173055", "symbol": "RIVERUSDT", "event_id": "20260303_173055_entry_fast", "risk_profile": "short_pump_active_1R", "reason": "legacy_single_profile_period"},
    {"file": "datasets/date=20260303/strategy=short_pump/mode=paper/outcomes_v3.csv", "run_id": "20260303_180016", "symbol": "PYRUSDT", "event_id": "20260303_180016_entry_fast", "risk_profile": "short_pump_active_1R", "reason": "legacy_single_profile_period"},
    {"file": "datasets/date=20260304/strategy=short_pump/mode=paper/outcomes_v3.csv", "run_id": "20260304_083130", "symbol": "SIRENUSDT", "event_id": "20260304_083130_entry_fast", "risk_profile": "short_pump_active_1R", "reason": "legacy_single_profile_period"},
    {"file": "datasets/date=20260304/strategy=short_pump/mode=paper/outcomes_v3.csv", "run_id": "20260304_094301", "symbol": "SIRENUSDT", "event_id": "20260304_094301_entry_fast", "risk_profile": "short_pump_active_1R", "reason": "legacy_single_profile_period"},
    {"file": "datasets/date=20260304/strategy=short_pump/mode=paper/outcomes_v3.csv", "run_id": "20260304_224755", "symbol": "BARDUSDT", "event_id": "20260304_224755_entry_fast", "risk_profile": "short_pump_active_1R", "reason": "legacy_single_profile_period"},
    {"file": "datasets/date=20260304/strategy=short_pump/mode=paper/outcomes_v3.csv", "run_id": "20260305_000330", "symbol": "BARDUSDT", "event_id": "20260305_000330_entry_fast", "risk_profile": "short_pump_active_1R", "reason": "legacy_single_profile_period"},
    {"file": "datasets/date=20260304/strategy=short_pump/mode=paper/outcomes_v3.csv", "run_id": "20260305_010705", "symbol": "AGIUSDT", "event_id": "20260305_010705_entry_fast", "risk_profile": "short_pump_active_1R", "reason": "legacy_single_profile_period"},
    {"file": "datasets/date=20260305/strategy=short_pump/mode=paper/outcomes_v3.csv", "run_id": "20260305_103350", "symbol": "PHAUSDT", "event_id": "20260305_103350_entry_fast", "risk_profile": "short_pump_active_1R", "reason": "legacy_single_profile_period"},
    {"file": "datasets/date=20260305/strategy=short_pump/mode=paper/outcomes_v3.csv", "run_id": "20260305_142155", "symbol": "SIRENUSDT", "event_id": "20260305_142155_entry_fast", "risk_profile": "short_pump_active_1R", "reason": "legacy_single_profile_period"},
    {"file": "datasets/date=20260305/strategy=short_pump/mode=paper/outcomes_v3.csv", "run_id": "20260305_140500", "symbol": "AGLDUSDT", "event_id": "20260305_140500_entry_fast", "risk_profile": "short_pump_active_1R", "reason": "legacy_single_profile_period"},
    {"file": "datasets/date=20260305/strategy=short_pump/mode=paper/outcomes_v3.csv", "run_id": "20260305_202600", "symbol": "MYXUSDT", "event_id": "20260305_202600_entry_fast", "risk_profile": "short_pump_active_1R", "reason": "legacy_single_profile_period"},
    {"file": "datasets/date=20260305/strategy=short_pump/mode=paper/outcomes_v3.csv", "run_id": "20260306_005550", "symbol": "XCNUSDT", "event_id": "20260306_005550_entry_fast", "risk_profile": "short_pump_active_1R", "reason": "legacy_single_profile_period"},
    {"file": "datasets/date=20260306/strategy=short_pump/mode=paper/outcomes_v3.csv", "run_id": "20260306_092035", "symbol": "JELLYJELLYUSDT", "event_id": "20260306_092035_entry_fast", "risk_profile": "short_pump_active_1R", "reason": "legacy_single_profile_period"},
]

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _now_ts() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")


def _match_key(row: Dict[str, Any], cand: Dict[str, str]) -> bool:
    return (
        row.get("run_id", "") == cand["run_id"]
        and row.get("symbol", "") == cand["symbol"]
        and row.get("event_id", "") == cand["event_id"]
    )


def _group_by_file(candidates: List[Dict[str, str]]) -> Dict[str, List[Dict[str, str]]]:
    groups: Dict[str, List[Dict[str, str]]] = {}
    for c in candidates:
        groups.setdefault(c["file"], []).append(c)
    return groups


def _backup_file(path: Path, ts: str) -> Path:
    bak = path.with_suffix(f".csv.bak.{ts}")
    shutil.copy2(str(path), str(bak))
    return bak


def _rewrite_csv(path: Path, updates: Dict[tuple, str]) -> tuple[int, int]:
    """
    Rewrite CSV at path, applying risk_profile updates for matching (run_id, symbol, event_id) keys.
    If risk_profile column is absent from the schema, it is appended after mfe_pct (before details_json)
    to match the v3 schema column order.
    Returns (rows_updated, rows_skipped_already_filled).
    """
    with open(path, newline="", encoding="utf-8") as f:
        content = f.read()

    reader = csv.DictReader(io.StringIO(content))
    fieldnames: List[str] = list(reader.fieldnames or [])
    if not fieldnames:
        raise ValueError(f"No fieldnames in {path}")

    # Add risk_profile to schema if missing — insert before details_json if present,
    # otherwise append. Matches the v3 schema column order.
    if "risk_profile" not in fieldnames:
        insert_before = "details_json"
        if insert_before in fieldnames:
            idx = fieldnames.index(insert_before)
            fieldnames.insert(idx, "risk_profile")
        else:
            fieldnames.append("risk_profile")

    rows_updated = 0
    rows_skipped = 0
    out_rows: List[Dict[str, Any]] = []

    for row in csv.DictReader(io.StringIO(content)):
        key = (row.get("run_id", ""), row.get("symbol", ""), row.get("event_id", ""))
        existing_rp = (row.get("risk_profile") or "").strip()
        if key in updates:
            if existing_rp:
                rows_skipped += 1
            else:
                row["risk_profile"] = updates[key]
                rows_updated += 1
        elif not existing_rp:
            row["risk_profile"] = ""
        out_rows.append(row)

    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(out_rows)

    return rows_updated, rows_skipped


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--apply", action="store_true", help="Write changes (default: dry-run)")
    args = parser.parse_args()

    dry_run = not args.apply
    mode_label = "DRY-RUN" if dry_run else "APPLY"
    ts = _now_ts()

    # Resolve root: script may be run from any directory
    script_dir = Path(__file__).resolve().parent
    root = script_dir.parent

    print(f"[{mode_label}] backfill_risk_profile_paper.py  ts={ts}")
    print(f"  candidates: {len(CANDIDATES)}")
    print()

    groups = _group_by_file(CANDIDATES)

    total_updated = 0
    total_skipped_filled = 0
    total_not_found = 0
    backups_created: List[str] = []
    audit_entries: List[Dict[str, Any]] = []

    for rel_file, file_cands in sorted(groups.items()):
        abs_file = root / rel_file
        if not abs_file.exists():
            print(f"  WARN: file not found: {rel_file}")
            for c in file_cands:
                total_not_found += 1
                print(f"    SKIP (no file): run_id={c['run_id']} symbol={c['symbol']}")
            continue

        # Pre-scan: find matching rows and their current risk_profile
        with open(abs_file, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            all_rows = list(reader)

        matches: List[Dict[str, Any]] = []
        for row in all_rows:
            for c in file_cands:
                if _match_key(row, c):
                    matches.append({"row": row, "cand": c})
                    break

        print(f"  {rel_file}")
        print(f"    candidates={len(file_cands)}  matched={len(matches)}")

        if not matches:
            print(f"    WARN: no rows matched — skipping file")
            for c in file_cands:
                total_not_found += 1
            continue

        updates: Dict[tuple, str] = {}
        for m in matches:
            row = m["row"]
            c = m["cand"]
            existing_rp = (row.get("risk_profile") or "").strip()
            key = (c["run_id"], c["symbol"], c["event_id"])
            if existing_rp:
                total_skipped_filled += 1
                print(f"    SKIP (already filled): run_id={c['run_id']} symbol={c['symbol']} existing='{existing_rp}'")
                continue

            updates[key] = c["risk_profile"]
            audit_entries.append({
                "file": rel_file,
                "run_id": c["run_id"],
                "symbol": c["symbol"],
                "event_id": c["event_id"],
                "old_risk_profile": existing_rp,
                "new_risk_profile": c["risk_profile"],
                "reason": c["reason"],
                "backfill_ts": ts,
                "dry_run": dry_run,
            })
            action = "WOULD UPDATE" if dry_run else "UPDATE"
            print(f"    {action}: run_id={c['run_id']} symbol={c['symbol']} → risk_profile={c['risk_profile']}")

        if not updates or dry_run:
            total_updated += len(updates)
            continue

        # Write backup before touching the file
        try:
            bak = _backup_file(abs_file, ts)
            backups_created.append(str(bak.relative_to(root)))
            print(f"    backup: {bak.relative_to(root)}")
        except Exception as e:
            print(f"    ERROR: backup failed for {rel_file}: {e}", file=sys.stderr)
            sys.exit(1)

        n_updated, n_skipped = _rewrite_csv(abs_file, updates)
        total_updated += n_updated
        total_skipped_filled += n_skipped
        print(f"    wrote: {n_updated} rows updated, {n_skipped} already-filled skipped")

    # Write audit JSONL
    audit_dir = root / "reports"
    audit_dir.mkdir(parents=True, exist_ok=True)
    audit_path = audit_dir / f"backfill_risk_profile_paper_audit_{ts}.jsonl"
    with open(audit_path, "w", encoding="utf-8") as af:
        for entry in audit_entries:
            af.write(json.dumps(entry, ensure_ascii=False) + "\n")

    print()
    print(f"[{mode_label}] Summary")
    print(f"  rows updated:          {total_updated}")
    print(f"  rows skipped (filled): {total_skipped_filled}")
    print(f"  rows not found:        {total_not_found}")
    print(f"  backups created:       {len(backups_created)}")
    for b in backups_created:
        print(f"    {b}")
    print(f"  audit file:            reports/{audit_path.name}")

    if dry_run:
        print()
        print("  [DRY-RUN] No files were modified. Re-run with --apply to write changes.")


if __name__ == "__main__":
    main()
