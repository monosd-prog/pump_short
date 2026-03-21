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
    _fast0_mode_mask_selective,
    _short_pump_mode_mask,
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
    df_short_pump_filtered_enriched: Optional[pd.DataFrame],
    df_fast0_enriched: Optional[pd.DataFrame],
    *,
    rolling_n: int = 20,
    tg_dist_min: float = 3.5,
) -> Dict[str, GuardModeMetrics]:
    """
    Build GuardModeMetrics for боевые режимы:

    - short_pump_active_1R  (short_pump ACTIVE Base 1R: stage=4, dist>=tg_dist_min)
    - short_pump_filtered_1R (short_pump_filtered: TP/SL core outcomes)
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

    # SHORT_PUMP FILTERED 1R
    df_spf = df_short_pump_filtered_enriched.copy() if df_short_pump_filtered_enriched is not None else None
    if df_spf is not None and not df_spf.empty and "outcome" in df_spf.columns:
        out_spf = df_spf["outcome"].apply(_normalize_outcome_raw)
        df_spf_core = df_spf[out_spf.isin(["TP_hit", "SL_hit"])].copy()
        metrics["short_pump_filtered_1R"] = _metrics_for_subset(df_spf_core, rolling_n)
    else:
        metrics["short_pump_filtered_1R"] = _metrics_for_subset(pd.DataFrame(), rolling_n)

    # SHORT_PUMP MID / DEEP — reuse masks from executive_report for perfect sync
    if df_sp is not None and not df_sp.empty and "outcome" in df_sp.columns:
        out_sp = df_sp["outcome"].apply(_normalize_outcome_raw)
        df_sp_core = df_sp[out_sp.isin(["TP_hit", "SL_hit"])].copy()

        for mode_name in ("short_pump_mid", "short_pump_deep"):
            sub = df_sp_core[_short_pump_mode_mask(df_sp_core, mode_name=mode_name)]
            metrics[mode_name] = _metrics_for_subset(sub, rolling_n)
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

        # FAST0 SELECTIVE — reuse _fast0_mode_mask_selective from executive_report
        sub_sel = df_f0_core[_fast0_mode_mask_selective(df_f0_core)]
        metrics["fast0_selective"] = _metrics_for_subset(sub_sel, rolling_n)
    else:
        # No FAST0 data: still populate keys with empty metrics for safety
        for mode_name in ("fast0_base_1R", "fast0_1p5R", "fast0_2R", "fast0_selective"):
            metrics[mode_name] = _metrics_for_subset(pd.DataFrame(), rolling_n)

    return metrics

