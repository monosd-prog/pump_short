from __future__ import annotations

from pathlib import Path
import os
import csv
from typing import Iterable

import pandas as pd

from analytics.utils import dprint, DEBUG_ENABLED
from common.dataset_schema import EVENT_FIELDS_V2, EVENT_FIELDS_V3


DEBUG_LOAD = True

OUTCOME_V2_COLS = [
    "schema_version",
    "trade_id",
    "event_id",
    "run_id",
    "symbol",
    "strategy",
    "mode",
    "side",
    "outcome_time_utc",
    "outcome",
    "pnl_pct",
    "hold_seconds",
    "mae_pct",
    "mfe_pct",
    "details_json",
]

# v3 schema may include source_mode after mode; required columns for validation
OUTCOME_V3_REQUIRED_COLS = [
    "schema_version", "trade_id", "event_id", "run_id", "symbol", "strategy",
    "mode", "side", "outcome_time_utc", "outcome", "pnl_pct", "hold_seconds",
    "mae_pct", "mfe_pct", "details_json",
]

# v3 without source_mode (15 cols) - legacy
OUTCOME_V3_NAMES_LEGACY = OUTCOME_V2_COLS
# v3 with source_mode (16 cols) - insert source_mode after mode
OUTCOME_V3_NAMES_WITH_SOURCE_MODE = [
    "schema_version", "trade_id", "event_id", "run_id", "symbol", "strategy",
    "mode", "source_mode", "side", "outcome_time_utc", "outcome", "pnl_pct",
    "hold_seconds", "mae_pct", "mfe_pct", "details_json",
]


def _debug(message: str) -> None:
    dprint(DEBUG_ENABLED and DEBUG_LOAD, message)


def _debug_env(message: str) -> None:
    if os.getenv("DEBUG_LOAD") == "1":
        print(message)


def _iter_roots(base_dir: Path, verbose: bool = False) -> list[Path]:
    """
    Determine the datasets root directory.
    
    Logic:
    - If base_dir contains date=* directories, use base_dir
    - Else if base_dir/datasets contains date=* directories, use base_dir/datasets
    - Else return empty list and print warning
    
    Returns:
        List of valid dataset root paths (usually 1, but can be multiple for compatibility)
    """
    base_dir = Path(base_dir).resolve()
    candidates = [
        ("direct", base_dir),
        ("datasets_subdir", base_dir / "datasets"),
    ]
    
    found_roots = []
    checked_paths = []
    
    for label, candidate in candidates:
        checked_paths.append(str(candidate))
        if not candidate.exists():
            if verbose:
                print(f"  [ROOT CHECK] {label}: {candidate} - does not exist")
            continue
        
        # Check for nested datasets (avoid /datasets/datasets)
        if "/datasets/datasets" in str(candidate):
            if verbose:
                print(f"  [ROOT CHECK] {label}: {candidate} - skipped (nested datasets)")
            continue
        
        # Check if this directory contains date=* folders
        date_dirs = list(candidate.glob("date=*"))
        date_dirs = [d for d in date_dirs if d.is_dir()]
        
        if verbose:
            print(f"  [ROOT CHECK] {label}: {candidate} - found {len(date_dirs)} date=* directories")
        
        if date_dirs:
            found_roots.append(candidate)
            if verbose:
                print(f"  [ROOT CHECK] ✓ Using root: {candidate}")
    
    if not found_roots:
        print(
            f"WARNING: No datasets root found. "
            f"Checked paths: {checked_paths}. "
            f"Expected structure: <root>/date=YYYYMMDD/strategy=*/mode=*/outcomes_v*.csv"
        )
    
    return found_roots


def _drop_v2_header_rows(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    mask = (
        (df["schema_version"].astype(str) == "schema_version")
        | (df["trade_id"].astype(str) == "trade_id")
        | (df["pnl_pct"].astype(str) == "pnl_pct")
    )
    if mask.any():
        return df[~mask].reset_index(drop=True)
    return df


def _parse_path_meta(path: Path) -> dict:
    meta = {"date": None, "strategy": None, "mode": None}
    for part in path.parts:
        if part.startswith("date="):
            meta["date"] = part.split("=", 1)[1]
        elif part.startswith("strategy="):
            meta["strategy"] = part.split("=", 1)[1]
        elif part.startswith("mode="):
            meta["mode"] = part.split("=", 1)[1]
    return meta


def _filter_test_runs(df: pd.DataFrame, include_test: bool) -> pd.DataFrame:
    if include_test or df.empty:
        return df
    # test_* runs are for smoke/synthetic generation only.
    # Support alternative test indicators
    test_col = None
    for candidate in ["run_id", "runId", "run_uuid", "test_flag"]:
        if candidate in df.columns:
            test_col = candidate
            break

    if test_col is None:
        return df

    mask = ~df[test_col].astype(str).str.startswith("test_")
    # Also check for explicit test flags
    if "is_test" in df.columns:
        mask = mask & (pd.to_numeric(df["is_test"], errors="coerce") != 1)
    return df[mask]


def _infer_schema_version_from_source(df: pd.DataFrame) -> pd.DataFrame:
    """Ensure schema_version column exists; infer from source_file if missing or null."""
    if df.empty:
        return df

    def _version_from_name(name):
        if pd.isna(name):
            return 1
        n = str(name).lower()
        if "v3" in n or "outcomes_v3" in n:
            return 3
        if "v2" in n or "outcomes_v2" in n:
            return 2
        return 1

    df = df.copy()
    if "source_file" in df.columns:
        inferred = df["source_file"].apply(_version_from_name)
    else:
        inferred = pd.Series(1, index=df.index)

    if "schema_version" in df.columns:
        sv = pd.to_numeric(df["schema_version"], errors="coerce")
        df["schema_version"] = sv.fillna(inferred).astype(int)
    else:
        df["schema_version"] = inferred.astype(int)
    return df


def _dedupe_outcomes_version_aware(
    df: pd.DataFrame,
    debug: bool = False,
) -> tuple[pd.DataFrame, dict]:
    """
    Robust de-duplication preferring highest schema_version.
    Keys: trade_id (primary) -> event_id (fallback) -> (run_id, symbol, outcome_time_utc, outcome, side).
    Returns (deduped_df, stats_dict with rows_before, rows_after, n_dupes_dropped, dates_with_multiple_versions).
    """
    stats: dict = {
        "rows_before": len(df),
        "rows_after": len(df),
        "n_dupes_dropped": 0,
        "dates_with_multiple_versions": [],
    }
    if df.empty:
        return df, stats

    df = _infer_schema_version_from_source(df)
    if "outcome_time_utc" in df.columns:
        df = df.copy()
        df["_outcome_time_parsed"] = pd.to_datetime(df["outcome_time_utc"], errors="coerce")
    else:
        df = df.copy()
        df["_outcome_time_parsed"] = pd.NaT

    # Sort so highest schema_version wins, then newest outcome_time
    sort_cols = ["schema_version", "_outcome_time_parsed"]
    df = df.sort_values(sort_cols, ascending=[False, False])

    # Detect dates with multiple source versions (for debug)
    if "date" in df.columns and "source_file" in df.columns:
        multi = df.groupby("date")["source_file"].nunique()
        stats["dates_with_multiple_versions"] = sorted(multi[multi > 1].index.tolist())

    # Build dedupe key: primary -> fallback -> final
    subset_cols = None
    if "trade_id" in df.columns and df["trade_id"].astype(str).str.len().gt(0).any():
        subset_cols = ["trade_id"]
    elif "event_id" in df.columns and df["event_id"].astype(str).str.len().gt(0).any():
        subset_cols = ["event_id"]
    else:
        fallback = [
            c for c in ["run_id", "symbol", "outcome_time_utc", "outcome", "side"]
            if c in df.columns
        ]
        if fallback:
            subset_cols = fallback

    if subset_cols is None:
        df = df.drop(columns=["_outcome_time_parsed"], errors="ignore")
        return df, stats

    rows_before = len(df)
    deduped = df.drop_duplicates(subset=subset_cols, keep="first")
    deduped = deduped.drop(columns=["_outcome_time_parsed"], errors="ignore")
    rows_after = len(deduped)
    stats["rows_before"] = rows_before
    stats["rows_after"] = rows_after
    stats["n_dupes_dropped"] = rows_before - rows_after

    if debug and stats["n_dupes_dropped"] > 0:
        _debug(
            f"[DEBUG_LOAD] dedupe_version_aware: before={rows_before} after={rows_after} "
            f"dropped={stats['n_dupes_dropped']} key={subset_cols} "
            f"dates_with_multiple_versions={stats['dates_with_multiple_versions']}"
        )
    return deduped, stats


def dedupe_outcomes(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    key = None
    # Try alternative names for trade_id
    for candidate in [
        "trade_id",
        "tradeId",
        "trade_uuid",
        "position_id",
        "event_id",
        "run_id",
    ]:
        if candidate in df.columns:
            key = candidate
            break

    if key is None:
        return df

    before = len(df)

    if "outcome_time_utc" in df.columns:
        time_col = pd.to_datetime(df["outcome_time_utc"], errors="coerce")
        has_time = time_col.notna().any()
    else:
        time_col = None
        has_time = False

    if has_time:
        sort_df = df.assign(_outcome_time=time_col)
        sort_cols = [key, "_outcome_time"]
        deduped = (
            sort_df.sort_values(sort_cols, ascending=[True, True])
            .drop_duplicates(subset=[key], keep="last")
            .drop(columns=["_outcome_time"])
        )
    else:
        sort_cols = [key]
        if "source_file" in df.columns:
            sort_cols.append("source_file")
        sort_df = df.reset_index().rename(columns={"index": "_row_index"})
        sort_cols.append("_row_index")
        deduped = (
            sort_df.sort_values(sort_cols, ascending=True)
            .drop_duplicates(subset=[key], keep="last")
            .drop(columns=["_row_index"])
        )

    after = len(deduped)
    dup_keys = before - after
    _debug(
        f"[DEBUG_LOAD] dedupe outcomes: before={before} after={after} key={key} dup_keys={dup_keys}"
    )
    return deduped


DEFAULT_DATASETS_DIR = (
    Path(__file__).resolve().parents[1] / "data" / "datasets"
)


def _date_dirs(base_dir: Path) -> Iterable[Path]:
    if not base_dir.exists():
        _debug(f"[DEBUG_LOAD] base_dir: {base_dir}")
        _debug("[DEBUG_LOAD] searching pattern: date=*")
        _debug("[DEBUG_LOAD] found files: 0")
        return []
    _debug(f"[DEBUG_LOAD] base_dir: {base_dir}")
    _debug("[DEBUG_LOAD] searching pattern: date=*")
    matches = list(base_dir.glob("date=*"))
    _debug(f"[DEBUG_LOAD] found files: {len(matches)}")
    return sorted(p for p in matches if p.is_dir())


def pick_best_file(dir_path: Path, candidates_in_priority_order: list[str]) -> Path | None:
    """Pick the first existing file from candidates in the given directory. One file per dir."""
    for name in candidates_in_priority_order:
        path = dir_path / name
        if path.exists():
            return path
    return None


def get_datasets_date_range(base_dir: Path | str, days: int | None) -> tuple[str, str] | None:
    """
    Return (min_date, max_date) from date=* folders within the window.
    Does NOT filter by strategy — uses full datasets coverage (same as fast0).
    Used for report header so short_pump shows full period within --days.
    """
    from datetime import datetime, timezone, timedelta
    base_dir = Path(base_dir)
    roots = _iter_roots(base_dir)
    if not roots:
        return None
    root = roots[0]
    cutoff_date: str | None = None
    if days is not None and days > 0:
        cutoff = datetime.now(timezone.utc).date()
        cutoff = cutoff - timedelta(days=days)
        cutoff_date = cutoff.strftime("%Y%m%d")
    date_strs: list[str] = []
    for date_dir in _date_dirs(root):
        if not date_dir.name.startswith("date="):
            continue
        date_str = date_dir.name.split("=", 1)[1]
        if cutoff_date is not None and date_str < cutoff_date:
            continue
        date_strs.append(date_str)
    if not date_strs:
        return None
    return (min(date_strs), max(date_strs))


def collect_paths(
    root: Path,
    strategy: str,
    mode: str,
    days: int | None,
    filename_candidates: list[str],
) -> list[Path]:
    """
    Collect one path per date under root/date=YYYYMMDD/strategy=X/mode=Y,
    picking the best available filename from candidates. Deduped per date.
    If days is set, only include date dirs with date >= (today - days).
    """
    from datetime import datetime, timezone, timedelta
    out: list[Path] = []
    date_dirs = list(_date_dirs(root))
    cutoff_date: str | None = None
    if days is not None and days > 0:
        cutoff = datetime.now(timezone.utc).date()
        cutoff = cutoff - timedelta(days=days)
        cutoff_date = cutoff.strftime("%Y%m%d")
    seen_dates: set[str] = set()
    for date_dir in date_dirs:
        part = date_dir.name
        if part.startswith("date="):
            date_str = part.split("=", 1)[1]
            if cutoff_date is not None and date_str < cutoff_date:
                continue
            if date_str in seen_dates:
                continue
            target_dir = date_dir / f"strategy={strategy}" / f"mode={mode}"
            if not target_dir.exists():
                continue
            best = pick_best_file(target_dir, filename_candidates)
            if best is not None:
                seen_dates.add(date_str)
                out.append(best)
    return out


def _read_csv_outcomes_v2_v3(path: Path) -> pd.DataFrame:
    """
    Read outcomes_v2.csv / outcomes_v3.csv with schema-tolerant parsing.
    Supports both:
      a) without source_mode (15 cols)
      b) with source_mode after mode (16 cols)
    Uses header=0 when possible; falls back to names by column count.
    """
    df_first = pd.read_csv(path, header=0, nrows=1, low_memory=False)
    cols = list(df_first.columns)
    col_strs = [str(c).strip() for c in cols]

    def _looks_like_header() -> bool:
        if not col_strs:
            return False
        if "schema_version" in col_strs:
            return True
        if "trade_id" in col_strs and "event_id" in col_strs:
            return True
        if any(c.startswith("{") for c in col_strs):
            return False
        return False

    def _has_source_mode() -> bool:
        return "source_mode" in col_strs

    if _looks_like_header():
        df = pd.read_csv(path, header=0, low_memory=False)
        df = _drop_v2_header_rows(df)
        if not _has_source_mode():
            df["source_mode"] = "unknown"
    else:
        df_full = pd.read_csv(path, header=None, nrows=1, low_memory=False)
        n_cols = len(df_full.columns)
        if n_cols == 16:
            names = OUTCOME_V3_NAMES_WITH_SOURCE_MODE
        elif n_cols == 15:
            names = OUTCOME_V3_NAMES_LEGACY
        else:
            print(
                f"[DEBUG_LOAD] skip outcomes file (unexpected column count): path={path} "
                f"n_cols={n_cols} expected=15_or_16"
            )
            return pd.DataFrame()
        df = pd.read_csv(path, header=None, names=names, low_memory=False)
        if "source_mode" not in df.columns:
            df["source_mode"] = "unknown"
        df = _drop_v2_header_rows(df)

    missing = [c for c in OUTCOME_V3_REQUIRED_COLS if c not in df.columns]
    if missing:
        print(
            f"[DEBUG_LOAD] skip outcomes file (missing required columns): path={path} "
            f"missing={missing} df.columns={list(df.columns)}"
        )
        return pd.DataFrame()

    return df


def _read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    if os.path.getsize(path) == 0:
        _debug(f"[DEBUG_LOAD] skip empty csv: {path}")
        is_v2_or_v3 = path.name in ("outcomes_v2.csv", "outcomes_v3.csv")
        return pd.DataFrame(columns=OUTCOME_V2_COLS if is_v2_or_v3 else None)
    try:
        if path.name in ("outcomes_v2.csv", "outcomes_v3.csv"):
            df = _read_csv_outcomes_v2_v3(path)
        elif path.name in ("events_v2.csv", "events_v3.csv"):
            expected = EVENT_FIELDS_V3 if path.name == "events_v3.csv" else EVENT_FIELDS_V2
            with open(path, "r", newline="", encoding="utf-8", errors="replace") as f:
                reader = csv.reader(f)
                header = next(reader, [])
                if header == list(expected) and len(header) == len(expected):
                    # Header is already aligned with current schema.
                    df = pd.read_csv(path, low_memory=False)
                else:
                    expected_len = len(expected)
                    header_len = len(header)
                    # Map legacy header columns into the expected field order.
                    legacy_to_expected_idx = {
                        col: expected.index(col) for col in header if col in expected
                    }
                    out_rows = []
                    for row in reader:
                        if not row:
                            continue
                        if len(row) == expected_len:
                            # Row written using current DictWriter fieldnames => expected ordering.
                            out_rows.append(row)
                            continue
                        if len(row) == header_len:
                            row_out = [None] * expected_len
                            for i, val in enumerate(row):
                                col = header[i] if i < header_len else None
                                if col is None:
                                    continue
                                idx = legacy_to_expected_idx.get(col)
                                if idx is not None:
                                    row_out[idx] = val
                            out_rows.append(row_out)
                            continue
                        # Best-effort: pad/truncate to expected_len by position.
                        row_out = [None] * expected_len
                        for i, val in enumerate(row[:expected_len]):
                            row_out[i] = val
                        out_rows.append(row_out)
                    df = pd.DataFrame(out_rows, columns=list(expected))
        else:
            df = pd.read_csv(path, low_memory=False)
    except (pd.errors.EmptyDataError, Exception) as e:
        _debug(f"[DEBUG_LOAD] skip csv {path}: {e}")
        is_v2_or_v3 = path.name in ("outcomes_v2.csv", "outcomes_v3.csv")
        return pd.DataFrame(columns=OUTCOME_V2_COLS if is_v2_or_v3 else None)

    return df


OUTCOMES_CANDIDATES = ["outcomes_v3.csv", "outcomes_v2.csv", "outcomes.csv"]


# ---------------------------------------------------------------------------
# SMOKE VALIDATION (20260220): verify no duplicate trades after load_outcomes
# ---------------------------------------------------------------------------
# from analysis.load import load_outcomes
# df, n = load_outcomes(days=30, verbose=True, return_file_count=True)
# day = df[df["date"] == "20260220"].copy()
# core = day[day["outcome"].astype(str).str.upper().isin(["TP", "SL"])]
# # Should be 0: no identical duplicates by trade_id (or event_id fallback)
# sub = ["trade_id"] if "trade_id" in core.columns else ["event_id"]
# assert core.duplicated(subset=sub).sum() == 0, f"duplicates: {core.duplicated(subset=sub).sum()}"
# ---------------------------------------------------------------------------
#
# ---------------------------------------------------------------------------
# SMOKE VALIDATION (20260302): v3 schema with source_mode - date=20260302 row
# ---------------------------------------------------------------------------
# PYTHONPATH=. python3 -c "
# from analysis.load import load_outcomes
# df, n = load_outcomes(base_dir='/root/pump_short/datasets', strategy='short_pump_fast0', mode='live', days=30, verbose=True, return_file_count=True)
# row = df[df['date'] == '20260302'].iloc[0]
# assert row['strategy'] == 'short_pump_fast0', row
# assert row['symbol'] == 'ROBOUSDT', row
# assert row['run_id'] == '20260302_191710', row
# assert row['outcome'] == 'TIMEOUT', row
# assert float(row['hold_seconds']) > 0, row
# print('OK: date=20260302 row validated')
# "
# ---------------------------------------------------------------------------


def load_outcomes(
    base_dir: Path | str = DEFAULT_DATASETS_DIR,
    strategy: str = "short_pump",
    mode: str = "live",
    days: int | None = None,
    prefer_v2: bool = True,
    include_test: bool = False,
    verbose: bool = False,
    return_file_count: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, int]:
    base_dir = Path(base_dir)
    _debug(f"[DEBUG_LOAD] base_dir: {base_dir}")
    
    if verbose:
        print(f"\n[LOAD OUTCOMES]")
        print(f"  base_dir: {base_dir}")
        print(f"  strategy: {strategy}")
        print(f"  mode: {mode}")
        print(f"  days: {days}")
        print(f"  include_test: {include_test}")
    
    v3_frames: list[pd.DataFrame] = []
    v2_frames: list[pd.DataFrame] = []
    legacy_frames: list[pd.DataFrame] = []

    roots = _iter_roots(base_dir, verbose=verbose)
    
    if verbose:
        print(f"  Roots found: {len(roots)}")
        for root in roots:
            print(f"    - {root}")
    
    if not roots:
        return (pd.DataFrame(), 0) if return_file_count else pd.DataFrame()
    
    # Strategy- and version-aware: one file per date (v3 > v2 > outcomes.csv)
    found_paths: list[Path] = []
    seen_dates: set[str] = set()
    for root in roots:
        paths = collect_paths(root, strategy, mode, days, OUTCOMES_CANDIDATES)
        for p in paths:
            meta = _parse_path_meta(p)
            date_str = meta.get("date") or "unknown"
            if date_str in seen_dates:
                continue
            seen_dates.add(date_str)
            found_paths.append(p)
    
    for selected_path in found_paths:
        selected_file = selected_path.name
        if verbose and len(found_paths) <= 3:
            print(f"    Sample file: {selected_path}")
        df = _read_csv(selected_path)
        if df.empty:
            _debug_env(f"[DEBUG_LOAD] skip empty outcomes file={selected_path}")
            continue
        meta = _parse_path_meta(selected_path)
        if "date" not in df.columns:
            df["date"] = meta["date"] if meta["date"] is not None else "unknown"
        if "strategy" not in df.columns and meta["strategy"] is not None:
            df["strategy"] = meta["strategy"]
        if "mode" not in df.columns and meta["mode"] is not None:
            df["mode"] = meta["mode"]
        df["source_file"] = selected_file
        if selected_file == "outcomes_v3.csv":
            v3_frames.append(df)
            _debug_env(
                f"[DEBUG_LOAD] include outcomes_v3 file={selected_path} rows={len(df)} "
                f"meta_date={meta['date']} meta_strategy={meta['strategy']} meta_mode={meta['mode']}"
            )
        elif selected_file == "outcomes_v2.csv":
            v2_frames.append(df)
            _debug_env(
                f"[DEBUG_LOAD] include outcomes_v2 file={selected_path} rows={len(df)} "
                f"meta_date={meta['date']} meta_strategy={meta['strategy']} meta_mode={meta['mode']}"
            )
        else:
            legacy_frames.append(df)
            _debug_env(
                f"[DEBUG_LOAD] include outcomes    file={selected_path} rows={len(df)} "
                f"meta_date={meta['date']} meta_strategy={meta['strategy']} meta_mode={meta['mode']}"
            )
    
    if verbose:
        print(f"  Outcomes files found: {len(found_paths)}")
        print(f"  Files by version: v3={len(v3_frames)}, v2={len(v2_frames)}, legacy={len(legacy_frames)}")
    
    if roots:
        _debug_env(f"[DEBUG_LOAD] roots scanned: {roots} found={len(found_paths)}")

    # Mix all versions: one file per date was picked, concat all
    all_frames = v3_frames + v2_frames + legacy_frames
    if not all_frames:
        _debug_env("[DEBUG_LOAD] load_outcomes files=0 -> empty")
        return (pd.DataFrame(), 0) if return_file_count else pd.DataFrame()
    df = pd.concat(all_frames, ignore_index=True)
    source_type = "v3/v2/plain mix"
    _debug_env(f"[DEBUG_LOAD] load_outcomes source={source_type} files={len(all_frames)}")

    before_filters = len(df)
    df = _filter_test_runs(df, include_test)
    after_test_filter = len(df)

    # Robust de-duplication: prefer highest schema_version; keys: trade_id -> event_id -> (run_id, symbol, outcome_time_utc, outcome, side)
    show_dedup_stats = return_file_count or verbose or (os.getenv("DEBUG_LOAD") == "1")
    df, dedupe_stats = _dedupe_outcomes_version_aware(df, debug=True)
    if show_dedup_stats:
        print(
            f"[DEDUP OUTCOMES] rows_before={dedupe_stats['rows_before']} "
            f"rows_after={dedupe_stats['rows_after']} n_dupes_dropped={dedupe_stats['n_dupes_dropped']}"
        )
        if dedupe_stats["dates_with_multiple_versions"]:
            print(f"[DEDUP OUTCOMES] dates_with_multiple_versions={dedupe_stats['dates_with_multiple_versions']}")

    after_dedup = len(df)
    # Final safety: enforce single row per event_id for analytics (drop later duplicates).
    if "event_id" in df.columns:
        before_event_dedup = len(df)
        df = df.drop_duplicates(subset=["event_id"], keep="first")
        after_event_dedup = len(df)
        if (after_event_dedup < before_event_dedup) and (return_file_count or verbose or os.getenv("DEBUG_LOAD") == "1"):
            print(
                f"[DEDUP OUTCOMES EVENT_ID] rows_before={before_event_dedup} rows_after={after_event_dedup} "
                f"n_dupes_dropped={before_event_dedup - after_event_dedup}"
            )

    if after_dedup == 0 and before_filters > 0:
        # WARNING: filters removed all rows
        entry_ok_col = None
        for candidate in ["entry_ok", "entryOk", "is_entry", "entry_flag"]:
            if candidate in df.columns:
                entry_ok_col = candidate
                break
        run_id_col = None
        for candidate in ["run_id", "runId", "run_uuid"]:
            if candidate in df.columns:
                run_id_col = candidate
                break
        print(
            f"WARNING: All rows filtered out. "
            f"Before filters: {before_filters}, "
            f"After test filter: {after_test_filter}, "
            f"After dedup: {after_dedup}. "
            f"Filters used: entry_ok mapping={entry_ok_col}, "
            f"run_id mapping={run_id_col}, include_test={include_test}"
        )

    _debug_env(f"[DEBUG_LOAD] load_outcomes source={source_type} rows_total={after_dedup}")

    if "outcome" in df.columns:
        outcome = df["outcome"].astype(str)
        df = df[df["outcome"].notna() & (outcome.str.len() > 0)]

    # dedupe_outcomes already applied via _dedupe_outcomes_version_aware above

    out = df.reset_index(drop=True)
    if return_file_count:
        return (out, len(found_paths))
    return out


def load_csv_all_dates(
    filename: str,
    base_dir: Path | str = DEFAULT_DATASETS_DIR,
    strategy: str = "short_pump",
    mode: str = "live",
    include_test: bool = False,
) -> pd.DataFrame:
    base_dir = Path(base_dir)
    _debug(f"[DEBUG_LOAD] base_dir: {base_dir}")
    frames: list[pd.DataFrame] = []

    roots = _iter_roots(base_dir)
    
    if not roots:
        return pd.DataFrame()
    
    found_by_root: dict[str, int] = {str(root): 0 for root in roots}
    for root in roots:
        for date_dir in _date_dirs(root):
            path = (
                date_dir
                / f"strategy={strategy}"
                / f"mode={mode}"
                / filename
            )
            _debug(f"[DEBUG_LOAD] searching pattern: {path}")
            _debug(f"[DEBUG_LOAD] found files: {1 if path.exists() else 0}")
            if not path.exists():
                continue
            found_by_root[str(root)] += 1
            df = _read_csv(path)
            if df.empty:
                continue
            meta = _parse_path_meta(path)
            if "date" not in df.columns:
                df["date"] = meta["date"] if meta["date"] is not None else "unknown"
            if "strategy" not in df.columns and meta["strategy"] is not None:
                df["strategy"] = meta["strategy"]
            if "mode" not in df.columns and meta["mode"] is not None:
                df["mode"] = meta["mode"]
            df["source_file"] = filename
            frames.append(df)
    if roots:
        _debug_env(f"[DEBUG_LOAD] roots scanned: {roots} found={found_by_root}")

    if not frames:
        return pd.DataFrame()

    df = pd.concat(frames, ignore_index=True)
    df = _filter_test_runs(df, include_test)
    return df


EVENTS_CANDIDATES = ["events_v3.csv", "events_v2.csv", "events.csv"]
TRADES_CANDIDATES = ["trades_v3.csv", "trades_v2.csv", "trades.csv"]


def load_trades_v3(
    data_dir: Path | str = DEFAULT_DATASETS_DIR,
    strategy: str = "short_pump",
    mode: str = "live",
    days: int | None = None,
    include_test: bool = False,
    verbose: bool = False,
    return_file_count: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, int]:
    """
    Load trades_v3 (or v2/legacy) from datasets/date=*/strategy=*/mode=*/.
    Used by factor_report to join outcomes → trades → events so trade-level factors are preserved.
    """
    data_dir = Path(data_dir)
    _debug(f"[DEBUG_LOAD] load_trades base_dir: {data_dir}")
    frames: list[pd.DataFrame] = []
    roots = _iter_roots(data_dir, verbose=verbose)
    if not roots:
        return (pd.DataFrame(), 0) if return_file_count else pd.DataFrame()
    found_paths: list[Path] = []
    seen_dates: set[str] = set()
    for root in roots:
        paths = collect_paths(root, strategy, mode, days, TRADES_CANDIDATES)
        for p in paths:
            meta = _parse_path_meta(p)
            date_str = meta.get("date") or "unknown"
            if date_str in seen_dates:
                continue
            seen_dates.add(date_str)
            found_paths.append(p)
    for selected_path in found_paths:
        df = _read_csv(selected_path)
        if df.empty:
            continue
        meta = _parse_path_meta(selected_path)
        if "date" not in df.columns:
            df["date"] = meta["date"] or "unknown"
        if "strategy" not in df.columns and meta["strategy"] is not None:
            df["strategy"] = meta["strategy"]
        if "mode" not in df.columns and meta["mode"] is not None:
            df["mode"] = meta["mode"]
        df["source_file"] = selected_path.name
        frames.append(df)
    if not frames:
        return (pd.DataFrame(), 0) if return_file_count else pd.DataFrame()
    df = pd.concat(frames, ignore_index=True)
    df = _filter_test_runs(df, include_test)
    if return_file_count:
        return (df, len(found_paths))
    return df


def load_events_v2(
    data_dir: Path | str = DEFAULT_DATASETS_DIR,
    strategy: str = "short_pump",
    mode: str = "live",
    days: int | None = None,
    include_test: bool = False,
    verbose: bool = False,
    raw: bool = False,
    only_entry_ok: bool = False,
    return_file_count: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, int]:
    data_dir = Path(data_dir)
    _debug(f"[DEBUG_LOAD] base_dir: {data_dir}")
    
    if verbose:
        print(f"\n[LOAD EVENTS]")
        print(f"  data_dir: {data_dir}")
        print(f"  strategy: {strategy}")
        print(f"  mode: {mode}")
        print(f"  days: {days}")
        print(f"  include_test: {include_test}")
    
    frames: list[pd.DataFrame] = []

    roots = _iter_roots(data_dir, verbose=verbose)
    
    if not roots:
        return (pd.DataFrame(), 0) if return_file_count else pd.DataFrame()
    
    found_paths: list[Path] = []
    seen_dates: set[str] = set()
    for root in roots:
        paths = collect_paths(root, strategy, mode, days, EVENTS_CANDIDATES)
        for p in paths:
            meta = _parse_path_meta(p)
            date_str = meta.get("date") or "unknown"
            if date_str in seen_dates:
                continue
            seen_dates.add(date_str)
            found_paths.append(p)
    
    for selected_path in found_paths:
        selected_file = selected_path.name
        if verbose and len(found_paths) <= 3:
            print(f"    Sample file: {selected_path}")
        df = _read_csv(selected_path)
        if df.empty:
            _debug_env(f"[DEBUG_LOAD] skip empty events file={selected_path}")
            continue
        meta = _parse_path_meta(selected_path)
        # Always trust path meta for date/strategy/mode to ensure analysis
        # aligns with directory layout, even if CSV has inconsistent values.
        df["date"] = meta["date"] if meta["date"] is not None else df.get("date", "unknown")
        if meta["strategy"] is not None:
            df["strategy"] = meta["strategy"]
        elif "strategy" not in df.columns:
            df["strategy"] = "unknown"
        if meta["mode"] is not None:
            df["mode"] = meta["mode"]
        elif "mode" not in df.columns:
            df["mode"] = "unknown"
        df["source_file"] = selected_file
        frames.append(df)
    
    if verbose or os.getenv("DEBUG_LOAD") == "1":
        print(f"  Events files found: {len(found_paths)}")
        if found_paths:
            print(f"    Sample files:")
            for i, p in enumerate(found_paths[:5], 1):
                print(f"      {i}. {p}")
    
    if roots:
        _debug_env(f"[DEBUG_LOAD] roots scanned: {roots} found={len(found_paths)}")

    if not frames:
        if verbose or os.getenv("DEBUG_LOAD") == "1":
            print(f"[LOAD EVENTS] files=0 -> empty (roots={roots}, found_paths={len(found_paths)})")
        return (pd.DataFrame(), 0) if return_file_count else pd.DataFrame()

    df = pd.concat(frames, ignore_index=True)
    rows_before_filter = len(df)

    # Coerce entry_ok to numeric early (handles string "0"/"1" in CSV)
    entry_ok_col = None
    for candidate in ["entry_ok", "entryOk", "is_entry", "entry_flag"]:
        if candidate in df.columns:
            entry_ok_col = candidate
            break
    if entry_ok_col is not None:
        df = df.copy()
        df[entry_ok_col] = pd.to_numeric(df[entry_ok_col], errors="coerce").fillna(0).astype(int)

    # Ensure strategy/mode from path if missing; then filter by requested strategy/mode
    if "strategy" not in df.columns:
        df["strategy"] = "unknown"
    if "mode" not in df.columns:
        df["mode"] = "unknown"
    strategy_mask = df["strategy"].astype(str).str.strip() == str(strategy).strip()
    mode_mask = df["mode"].astype(str).str.strip() == str(mode).strip()
    df = df[strategy_mask & mode_mask].reset_index(drop=True)
    rows_after_filter = len(df)

    df = _filter_test_runs(df, include_test)
    rows_after_test = len(df)

    show_debug = verbose or os.getenv("DEBUG_LOAD") == "1"
    if show_debug:
        print(
            f"[LOAD EVENTS] rows_before_filter={rows_before_filter} "
            f"rows_after_strategy_mode={rows_after_filter} rows_after_test={rows_after_test}"
        )
        if rows_after_test > 0:
            vc_s = df["strategy"].value_counts().head(3).to_dict() if "strategy" in df.columns else {}
            vc_m = df["mode"].value_counts().head(3).to_dict() if "mode" in df.columns else {}
            print(f"[LOAD EVENTS] strategy value_counts: {vc_s}  mode value_counts: {vc_m}")

    # When raw=True, return all events (no entry_ok filter) for ENTRY QUALITY stats
    # When only_entry_ok=False (default), return all events for scanned-events KPI
    apply_entry_filter = only_entry_ok and not raw
    if not apply_entry_filter:
        if verbose or os.getenv("DEBUG_LOAD") == "1":
            print(
                f"[LOAD EVENTS] returning all events: rows={len(df)} "
                f"(raw={raw} only_entry_ok={only_entry_ok})"
            )
            if "entry_ok" in df.columns:
                vc = df["entry_ok"].value_counts().head(5).to_dict()
                print(f"[LOAD EVENTS] entry_ok value_counts (top): {vc}")
        if return_file_count:
            return (df, len(found_paths))
        return df

    # Apply entry_ok filter only when only_entry_ok=True
    if entry_ok_col is None:
        entry_ok_col = next(
            (c for c in ["entry_ok", "entryOk", "is_entry", "entry_flag"] if c in df.columns),
            None,
        )
    if entry_ok_col:
        entry_ok = df[entry_ok_col] == 1
    else:
        entry_ok = pd.Series([True] * len(df), index=df.index)

    if strategy in ("long_pullback", "short_pump", "short_pump_filtered", "short_pump_fast0", "short_pump_fast0_filtered"):
        # For these strategies, entry_ok is the single source of truth for "entry" events.
        # short_pump: ARMED is readiness-only (entry_ok=False) and must not be inferred by stage/event_id.
        mask = entry_ok
    else:
        has_stage = "stage" in df.columns
        has_event_id = "event_id" in df.columns
        if has_stage:
            stage_ok = pd.to_numeric(df["stage"], errors="coerce") == 4
        else:
            stage_ok = pd.Series([False] * len(df), index=df.index)
        if has_event_id:
            event_id_ok = df["event_id"].astype(str).str.contains("entry", case=False, na=False)
        else:
            event_id_ok = pd.Series([False] * len(df), index=df.index)
        if not (has_stage or has_event_id):
            mask = entry_ok
        else:
            mask = entry_ok & (stage_ok | event_id_ok)

    df = df[mask].reset_index(drop=True)
    if verbose or os.getenv("DEBUG_LOAD") == "1":
        print(
            f"[LOAD EVENTS] returning entry_ok-only: rows={len(df)} "
            f"(only_entry_ok=True)"
        )
        if "entry_ok" in df.columns and len(df) > 0:
            vc = df["entry_ok"].value_counts().head(5).to_dict()
            print(f"[LOAD EVENTS] entry_ok value_counts (top): {vc}")
    if return_file_count:
        return (df, len(found_paths))
    return df


# ---------------------------------------------------------------------------
# Smoke: verify dedup stats in output (rows_before/rows_after/n_dupes_dropped)
# DEBUG_LOAD=1 PYTHONPATH=. python3 - <<'PY'
# from analysis.load import load_outcomes, DEFAULT_DATASETS_DIR
# df, n = load_outcomes(base_dir=DEFAULT_DATASETS_DIR, days=30, verbose=True, return_file_count=True)
# PY
# ---------------------------------------------------------------------------
#
# ---------------------------------------------------------------------------
# Smoke: load_events_v2 for short_pump_fast0 (events_v3 root discovery)
# DEBUG_LOAD=1 PYTHONPATH=. python3 - <<'PY'
# from analysis.load import load_events_v2
# ev = load_events_v2(data_dir="/root/pump_short/datasets", strategy="short_pump_fast0", mode="live", days=30, verbose=True)
# print("rows", len(ev), "symbols", ev["symbol"].nunique() if "symbol" in ev.columns else None)
# PY
# ---------------------------------------------------------------------------
