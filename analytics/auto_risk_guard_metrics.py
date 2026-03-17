from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Optional

import pandas as pd

from .short_pump_blocks import filter_active_trades
from .stats import (
    _core_mask,
    _normalize_outcome_raw,
    ev_core_from_tp_sl_pnl,
    rolling_wr_ev_core,
    wr_core_from_tp_sl,
)
from .executive_report import (
    _core_pnl_series,
    _edge_consistency_frac,
    _trades_since_ev_negative,
    _fast0_mode_mask_base_1r,
    _fast0_mode_mask_1p5r,
    _fast0_mode_mask_2r,
)


@dataclass
class GuardModeMetrics:
    """Metrics snapshot for one боевой режим (compatible with pump_short GuardMetrics)."""

    wr: float
    ev_total: float
    ev20: float
    consistency: Optional[float]
    n_core: int
    trades_since_negative_start: int


def _metrics_for_subset(df_core: pd.DataFrame, rolling_n: int) -> GuardModeMetrics:
    """
    Compute WR, EV_total, EV(rolling_n), consistency and trades_since_negative_start
    using the same math as executive_report.
    """
    if df_core is None or df_core.empty:
        return GuardModeMetrics(
            wr=0.0,
            ev_total=0.0,
            ev20=0.0,
            consistency=None,
            n_core=0,
            trades_since_negative_start=0,
        )

    norm = df_core["outcome"].apply(_normalize_outcome_raw)
    tp = int((norm == "TP_hit").sum())
    sl = int((norm == "SL_hit").sum())
    pnl_core = _core_pnl_series(df_core)
    wr_val = wr_core_from_tp_sl(tp, sl) * 100.0
    ev_total = ev_core_from_tp_sl_pnl(tp, sl, pnl_core)
    _, ev20_val, _ = rolling_wr_ev_core(df_core, rolling_n)
    consistency = _edge_consistency_frac(pnl_core, rolling_n)
    trades_neg = _trades_since_ev_negative(pnl_core, rolling_n)

    return GuardModeMetrics(
        wr=wr_val,
        ev_total=ev_total,
        ev20=ev20_val,
        consistency=consistency,
        n_core=len(df_core),
        trades_since_negative_start=trades_neg,
    )


def build_guard_metrics_by_mode(
    df_short_pump_enriched: Optional[pd.DataFrame],
    df_fast0_enriched: Optional[pd.DataFrame],
    *,
    rolling_n: int = 20,
    tg_dist_min: float = 3.5,
) -> Dict[str, GuardModeMetrics]:
    """
    Build GuardModeMetrics for боевые режимы:

    - short_pump_active_1R  (short_pump ACTIVE Base 1R: stage=4, dist>=tg_dist_min)
    - short_pump_mid        (short_pump MID: dist in [3.5,5), cs in [0.4,0.6))
    - short_pump_deep       (short_pump DEEP: stage=3, dist in [7.5,10), cs in [0.4,0.6), liqL30s=0)
    - fast0_base_1R         (dist<=1.5, liq=0)
    - fast0_1p5R            (dist<=1.5, 5k<liq<=25k)
    - fast0_2R              (dist<=1.5, liq>100k)
    - fast0_selective       (fast0 selective: cs in [0.4,0.6), optional vol1m>1e6 when present)

    Uses the same filters and math as executive_report.
    """
    metrics: Dict[str, GuardModeMetrics] = {}

    # SHORT_PUMP ACTIVE Base 1R
    df_sp = df_short_pump_enriched.copy() if df_short_pump_enriched is not None else None
    if df_sp is not None and not df_sp.empty and "stage" in df_sp.columns and "dist_to_peak_pct" in df_sp.columns:
        _, df_active_core, _, _ = filter_active_trades(df_sp, "short_pump", tg_dist_min)
        if df_active_core is not None and not df_active_core.empty:
            metrics["short_pump_active_1R"] = _metrics_for_subset(df_active_core, rolling_n)
        else:
            metrics["short_pump_active_1R"] = _metrics_for_subset(pd.DataFrame(), rolling_n)
    else:
        metrics["short_pump_active_1R"] = _metrics_for_subset(pd.DataFrame(), rolling_n)

    # SHORT_PUMP MID / DEEP (prefer risk_profile label; fallback to feature filters)
    if df_sp is not None and not df_sp.empty:
        rp = df_sp["risk_profile"].astype(str).str.strip().str.lower() if "risk_profile" in df_sp.columns else pd.Series([""] * len(df_sp), index=df_sp.index)
        has_rp = (rp != "") & (rp != "nan")

        # MID
        by_rp_mid = has_rp & (rp == "short_pump_mid")
        if "dist_to_peak_pct" in df_sp.columns and "context_score" in df_sp.columns:
            dist = pd.to_numeric(df_sp["dist_to_peak_pct"], errors="coerce")
            cs = pd.to_numeric(df_sp["context_score"], errors="coerce")
            fb_mid = (~has_rp) & dist.notna() & cs.notna() & (dist >= 3.5) & (dist < 5.0) & (cs >= 0.4) & (cs < 0.6)
        else:
            fb_mid = pd.Series([False] * len(df_sp), index=df_sp.index)
        sub_mid = df_sp[(by_rp_mid | fb_mid) & _core_mask(df_sp["outcome"])].copy() if "outcome" in df_sp.columns else pd.DataFrame()
        metrics["short_pump_mid"] = _metrics_for_subset(sub_mid, rolling_n)

        # DEEP
        by_rp_deep = has_rp & (rp == "short_pump_deep")
        if "stage" in df_sp.columns and "dist_to_peak_pct" in df_sp.columns and "context_score" in df_sp.columns and "liq_long_usd_30s" in df_sp.columns:
            st = pd.to_numeric(df_sp["stage"], errors="coerce")
            dist = pd.to_numeric(df_sp["dist_to_peak_pct"], errors="coerce")
            cs = pd.to_numeric(df_sp["context_score"], errors="coerce")
            liq = pd.to_numeric(df_sp["liq_long_usd_30s"], errors="coerce")
            fb_deep = (~has_rp) & st.notna() & dist.notna() & cs.notna() & liq.notna() & (st == 3) & (dist >= 7.5) & (dist < 10.0) & (cs >= 0.4) & (cs < 0.6) & (liq == 0)
        else:
            fb_deep = pd.Series([False] * len(df_sp), index=df_sp.index)
        sub_deep = df_sp[(by_rp_deep | fb_deep) & _core_mask(df_sp["outcome"])].copy() if "outcome" in df_sp.columns else pd.DataFrame()
        metrics["short_pump_deep"] = _metrics_for_subset(sub_deep, rolling_n)
    else:
        metrics["short_pump_mid"] = _metrics_for_subset(pd.DataFrame(), rolling_n)
        metrics["short_pump_deep"] = _metrics_for_subset(pd.DataFrame(), rolling_n)

    # FAST0 modes (core subset + liq/dist buckets identical to executive_report)
    df_f0 = df_fast0_enriched.copy() if df_fast0_enriched is not None else None
    if df_f0 is not None and not df_f0.empty and "outcome" in df_f0.columns:
        outcome_f0 = df_f0["outcome"].apply(_normalize_outcome_raw)
        df_f0_core = df_f0[outcome_f0.isin(["TP_hit", "SL_hit"])].copy()

        for mode_name, mask_fn in [
            ("fast0_base_1R", _fast0_mode_mask_base_1r),
            ("fast0_1p5R", _fast0_mode_mask_1p5r),
            ("fast0_2R", _fast0_mode_mask_2r),
        ]:
            sub = df_f0_core[mask_fn(df_f0_core)]
            metrics[mode_name] = _metrics_for_subset(sub, rolling_n)

        # FAST0 SELECTIVE (prefer risk_profile label; fallback to context_score window)
        rp = df_f0["risk_profile"].astype(str).str.strip().str.lower() if "risk_profile" in df_f0.columns else pd.Series([""] * len(df_f0), index=df_f0.index)
        has_rp = (rp != "") & (rp != "nan")
        by_rp = has_rp & (rp == "fast0_selective")
        if "context_score" in df_f0.columns:
            cs = pd.to_numeric(df_f0["context_score"], errors="coerce")
            fb = (~has_rp) & cs.notna() & (cs >= 0.4) & (cs < 0.6)
        else:
            fb = pd.Series([False] * len(df_f0), index=df_f0.index)
        sub_sel = df_f0_core[by_rp.loc[df_f0_core.index] | fb.loc[df_f0_core.index]]
        metrics["fast0_selective"] = _metrics_for_subset(sub_sel, rolling_n)
    else:
        # No FAST0 data: still populate keys with empty metrics for safety
        for mode_name in ("fast0_base_1R", "fast0_1p5R", "fast0_2R", "fast0_selective"):
            metrics[mode_name] = _metrics_for_subset(pd.DataFrame(), rolling_n)

    return metrics

