from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict

import pandas as pd

from analytics.load import (
    get_datasets_date_range,
    load_events_v2,
    load_outcomes,
)
from analytics.auto_risk_guard_metrics import (
    GuardModeMetrics,
    build_guard_metrics_by_mode,
)


def _sort_outcomes_for_series(df: pd.DataFrame) -> pd.DataFrame:
    """
    Stable sort for rolling EV/WR calculations.
    Prefer outcome timestamp, then opened timestamp, then run_id as a deterministic fallback.
    """
    if df is None or df.empty:
        return pd.DataFrame() if df is None else df
    out = df.copy()
    out["_outcome_sort"] = pd.to_datetime(out.get("outcome_time_utc"), errors="coerce", utc=True)
    out["_opened_sort"] = pd.to_datetime(out.get("opened_ts"), errors="coerce", utc=True)
    sort_cols = ["_outcome_sort", "_opened_sort"]
    ascending = [True, True]
    if "run_id" in out.columns:
        sort_cols.append("run_id")
        ascending.append(True)
    out = out.sort_values(sort_cols, ascending=ascending, kind="stable", na_position="last")
    return out.drop(columns=["_outcome_sort", "_opened_sort"], errors="ignore").reset_index(drop=True)


def _ensure_stage_column(df: pd.DataFrame) -> pd.DataFrame:
    """Keep enrichment-compatible columns present even before event join."""
    if df is None or df.empty:
        return pd.DataFrame() if df is None else df
    out = df.copy()
    for col in ("stage", "dist_to_peak_pct", "liq_long_usd_30s", "context_score"):
        if col not in out.columns:
            out[col] = pd.NA
    return out


def _enrich_core_with_events(
    df_core: pd.DataFrame,
    events_raw: pd.DataFrame | None,
    *,
    debug: bool = False,
) -> pd.DataFrame:
    """
    Enrich outcomes with entry-event fields required by guard/report masks.
    Primary join is exact `event_id` (entry events share the same event_id as outcome rows).
    Fallback is `(run_id, symbol, strategy)` using the latest `entry_ok=1`/highest-stage row.
    """
    if df_core is None or df_core.empty or events_raw is None or events_raw.empty:
        return df_core

    event_cols = [
        c
        for c in ("event_id", "run_id", "symbol", "strategy", "stage", "dist_to_peak_pct", "liq_long_usd_30s", "context_score", "entry_ok", "time_utc", "wall_time_utc")
        if c in events_raw.columns
    ]
    if not event_cols:
        return df_core

    events = events_raw[event_cols].copy()
    if "entry_ok" in events.columns:
        entry_ok_num = pd.to_numeric(events["entry_ok"], errors="coerce").fillna(0)
        events = events[entry_ok_num >= 1].copy()
    if "stage" in events.columns:
        events["_stage_num"] = pd.to_numeric(events["stage"], errors="coerce")
    else:
        events["_stage_num"] = pd.NA
    events["_time_sort"] = pd.to_datetime(
        events["time_utc"] if "time_utc" in events.columns else events.get("wall_time_utc"),
        errors="coerce",
        utc=True,
    )

    enrich_cols = [c for c in ("stage", "dist_to_peak_pct", "liq_long_usd_30s", "context_score") if c in events.columns]

    def _coalesce_event_columns(df: pd.DataFrame) -> pd.DataFrame:
        out = df.copy()
        for col in enrich_cols:
            evt_col = f"{col}_evt"
            if evt_col not in out.columns:
                continue
            if col in out.columns:
                out[col] = out[col].combine_first(out[evt_col])
            else:
                out[col] = out[evt_col]
            out = out.drop(columns=[evt_col], errors="ignore")
        return out

    # Primary: exact event_id match.
    if "event_id" in df_core.columns and "event_id" in events.columns:
        by_event = (
            events.sort_values(["_time_sort", "_stage_num"], ascending=[False, False], kind="stable", na_position="last")
            .drop_duplicates(subset=["event_id"], keep="first")
        )
        merged = df_core.merge(
            by_event[["event_id", *enrich_cols]],
            on="event_id",
            how="left",
            suffixes=("", "_evt"),
        )
        merged = _coalesce_event_columns(merged)
        coverage = float(merged["stage"].notna().mean()) if "stage" in merged.columns and len(merged) else 0.0
        if coverage >= 0.8:
            return merged
        if debug:
            print(f"[guard_update] low event_id enrichment coverage={coverage:.2%}; trying run_id+symbol fallback")

    # Fallback: latest qualifying entry per (run_id, symbol, strategy).
    join_keys = [c for c in ("run_id", "symbol", "strategy") if c in df_core.columns and c in events.columns]
    if len(join_keys) < 2:
        return df_core
    by_lineage = (
        events.sort_values(["_time_sort", "_stage_num"], ascending=[False, False], kind="stable", na_position="last")
        .drop_duplicates(subset=join_keys, keep="first")
    )
    merged = df_core.merge(
        by_lineage[[*join_keys, *enrich_cols]],
        on=join_keys,
        how="left",
        suffixes=("", "_evt"),
    )
    return _coalesce_event_columns(merged)


def _load_enriched_dfs(base_dir: Path, days: int) -> tuple[pd.DataFrame | None, pd.DataFrame | None, pd.DataFrame | None, tuple[str, str]]:
    """Load and enrich outcomes for short_pump, short_pump_filtered and fast0 strategies (same as daily_tg_report)."""
    # Outcomes
    all_outcomes: list[pd.DataFrame] = []
    for strategy in ("short_pump", "short_pump_filtered", "short_pump_fast0", "short_pump_fast0_filtered"):
        result = load_outcomes(
            base_dir=base_dir,
            strategy=strategy,
            mode="live",
            days=days,
            include_test=False,
            return_file_count=False,
        )
        df_part = result[0] if isinstance(result, tuple) else result
        if not df_part.empty:
            all_outcomes.append(df_part)
    df = pd.concat(all_outcomes, ignore_index=True) if all_outcomes else pd.DataFrame()

    # Events for enrichment
    all_events: list[pd.DataFrame] = []
    for strategy in ("short_pump", "short_pump_filtered", "short_pump_fast0", "short_pump_fast0_filtered"):
        result = load_events_v2(
            data_dir=base_dir,
            strategy=strategy,
            mode="live",
            days=days,
            raw=True,
            return_file_count=False,
        )
        ev = result[0] if isinstance(result, tuple) else result
        if not ev.empty:
            all_events.append(ev)
    events_raw = pd.concat(all_events, ignore_index=True) if all_events else pd.DataFrame()

    if df.empty:
        date_range = ("unknown", "unknown")
        return None, None, None, date_range

    ds_range = get_datasets_date_range(base_dir, days=days)
    date_range = ds_range if ds_range is not None else ("unknown", "unknown")

    df_sorted = _sort_outcomes_for_series(df)
    has_strat = "strategy" in df_sorted.columns
    df_sp = (
        df_sorted[df_sorted["strategy"].astype(str) == "short_pump"]
        if has_strat
        else df_sorted
    )
    df_spf = (
        df_sorted[df_sorted["strategy"].astype(str) == "short_pump_filtered"]
        if has_strat
        else pd.DataFrame()
    )
    df_f0 = (
        df_sorted[df_sorted["strategy"].astype(str).isin(["short_pump_fast0", "short_pump_fast0_filtered"])]
        if has_strat
        else pd.DataFrame()
    )
    all_ev = events_raw if not events_raw.empty else pd.DataFrame()
    has_ev_strat = "strategy" in all_ev.columns
    ev_sp = (
        all_ev[all_ev["strategy"].astype(str) == "short_pump"]
        if has_ev_strat
        else all_ev
    )
    ev_spf = (
        all_ev[all_ev["strategy"].astype(str) == "short_pump_filtered"]
        if has_ev_strat
        else pd.DataFrame()
    )
    ev_f0 = (
        all_ev[all_ev["strategy"].astype(str).isin(["short_pump_fast0", "short_pump_fast0_filtered"])]
        if has_ev_strat
        else pd.DataFrame()
    )
    df_sp_e = _ensure_stage_column(df_sp.copy()) if not df_sp.empty else None
    df_spf_e = _ensure_stage_column(df_spf.copy()) if not df_spf.empty else None
    df_f0_e = _ensure_stage_column(df_f0.copy()) if not df_f0.empty else None
    if df_sp_e is not None and not df_sp_e.empty:
        df_sp_e = _enrich_core_with_events(df_sp_e, ev_sp if not ev_sp.empty else None, debug=False)
    if df_spf_e is not None and not df_spf_e.empty:
        df_spf_e = _enrich_core_with_events(df_spf_e, ev_spf if not ev_spf.empty else None, debug=False)
    if df_f0_e is not None and not df_f0_e.empty:
        df_f0_e = _enrich_core_with_events(df_f0_e, ev_f0 if not ev_f0.empty else None, debug=False)

    return (
        df_sp_e if df_sp_e is not None and not df_sp_e.empty else None,
        df_spf_e if df_spf_e is not None and not df_spf_e.empty else None,
        df_f0_e if df_f0_e is not None and not df_f0_e.empty else None,
        date_range,
    )


def _import_guard_from_pump_short(pump_short_root: Path):
    """Dynamically import GuardMetrics and update_guard_state_from_metrics from pump_short project."""
    if not pump_short_root.exists():
        raise RuntimeError(f"pump_short root does not exist: {pump_short_root}")
    sys.path.insert(0, str(pump_short_root))
    try:
        from trading.auto_risk_guard import GuardMetrics, update_guard_state_from_metrics  # type: ignore
        from trading.config import AUTO_RISK_GUARD_STATE_PATH  # type: ignore
    except Exception as e:  # pragma: no cover - environment/Layout specific
        raise RuntimeError(
            f"Failed to import trading.auto_risk_guard from {pump_short_root}: {e}"
        ) from e
    return GuardMetrics, update_guard_state_from_metrics, AUTO_RISK_GUARD_STATE_PATH


def main() -> None:
    parser = argparse.ArgumentParser(description="Update Auto Risk Guard state from live outcomes (short_pump + short_pump_filtered + FAST0).")
    parser.add_argument("--data-dir", default="/root/pump_short/datasets", help="Datasets root directory for outcomes/events.")
    parser.add_argument("--days", type=int, default=30, help="Days window for metrics (default: 30).")
    parser.add_argument(
        "--pump-short-root",
        default="/root/pump_short",
        help="Root directory of pump_short project (for importing trading.auto_risk_guard).",
    )
    parser.add_argument("--rolling", type=int, default=20, help="Rolling window N for EV(20)/consistency (default: 20).")
    args = parser.parse_args()

    base_dir = Path(args.data_dir)
    df_sp_e, df_spf_e, df_f0_e, _ = _load_enriched_dfs(base_dir, days=args.days)

    metrics_local: Dict[str, GuardModeMetrics] = build_guard_metrics_by_mode(
        df_sp_e,
        df_spf_e,
        df_f0_e,
        rolling_n=args.rolling,
        tg_dist_min=float(sys.argv and 0 or 3.5),  # actual tg_dist_min is handled inside filter_active_trades env
    )

    pump_short_root = Path(args.pump_short_root)
    GuardMetricsLive, update_guard_state_from_metrics, guard_state_path = _import_guard_from_pump_short(pump_short_root)

    # Build metrics dict for guard (convert local dataclass -> live GuardMetrics).
    metrics_for_guard: Dict[str, Any] = {}
    for mode_name, m in metrics_local.items():
        metrics_for_guard[mode_name] = GuardMetricsLive(
            wr=m.wr,
            ev_total=m.ev_total,
            ev20=m.ev20,
            consistency=m.consistency,
            n_core=m.n_core,
            trades_since_negative_start=m.trades_since_negative_start,
        )

    # Load previous state (for diff printing)
    prev_state_json = {}
    try:
        p = Path(guard_state_path)
        if p.exists():
            with open(p, "r", encoding="utf-8") as f:
                prev_state_json = json.load(f)
    except Exception:
        prev_state_json = {}

    new_state = update_guard_state_from_metrics(metrics_for_guard)

    print(f"Guard state updated at {guard_state_path}")
    print("mode_name | prev_state -> new_state | reason")
    for mode_name, entry in new_state.items():
        prev = prev_state_json.get(mode_name, {})
        prev_state = prev.get("current_state", "N/A") if isinstance(prev, dict) else "N/A"
        print(
            f"{mode_name} | {prev_state} -> {entry.current_state} | {entry.reason or ''}"
        )


if __name__ == "__main__":
    main()

