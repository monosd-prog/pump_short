from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Dict

import pandas as pd

from analytics.load import (
    get_datasets_date_range,
    load_events_v2,
    load_outcomes,
)
from scripts.daily_tg_report import (
    _enrich_core_with_events,
    _ensure_stage_column,
    _sort_outcomes_for_series,
)
from analytics.auto_risk_guard_metrics import (
    GuardModeMetrics,
    build_guard_metrics_by_mode,
)


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
        return None, None, date_range

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

