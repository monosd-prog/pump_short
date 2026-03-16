from __future__ import annotations

import os
from typing import List, Tuple

import pandas as pd

from .stats import _format_filter_line, _maturity_status_unified, _normalize_outcome_raw, _safe_stats_core, wr_core_from_tp_sl, ev_core_from_tp_sl_pnl

# Production liq regime for FAST0 what-if TP (analytics only)
FAST0_LIQ_MIN_USD = float(os.getenv("FAST0_LIQ_MIN_USD", "5000"))
FAST0_LIQ_MAX_USD = float(os.getenv("FAST0_LIQ_MAX_USD", "25000"))

# Hypothetical TP levels to compare (SL unchanged)
FAST0_WHATIF_TP_PCTS = [1.2, 1.5, 1.8, 2.0]
MIN_N_FOR_BEST_TP = 5

# Edge decay: time buckets (sec) and MFE targets (%)
FAST0_EDGE_TIME_BUCKETS = [30, 60, 120, 180]
FAST0_EDGE_MFE_TARGETS = [0.5, 1.2, 1.5, 1.8, 2.0]
MIN_N_EDGE_DECAY = 5


def _regime_stats(df: pd.DataFrame, mask: pd.Series) -> Tuple[int, float, float]:
    """Return (n, wr, ev) for rows under mask. Uses pnl_pct and core stats."""
    n = int(mask.sum())
    if n == 0:
        return (0, 0.0, 0.0)
    pnl = pd.to_numeric(df.loc[mask, "pnl_pct"], errors="coerce").dropna()
    if pnl.empty:
        return (n, 0.0, 0.0)
    wr, _aw, _al, ev = _safe_stats_core(pnl)
    return (n, wr, ev)


def _add_fast0_hypothesis_blocks(
    lines: List[str],
    df_core: pd.DataFrame,
    n_core: int,
    debug: bool,  # kept for signature compatibility
) -> None:
    """Add FAST0 hypothesis blocks: context_score, dist_to_peak_pct, volume_zscore, liq."""
    n_total = len(df_core)

    if "context_score" in df_core.columns:
        ctx = pd.to_numeric(df_core["context_score"], errors="coerce")
        for thresh in [0.25, 0.35, 0.45, 0.55, 0.60, 0.70, 0.75]:
            m = ctx >= thresh
            lines.append(_format_filter_line(df_core, m, n_total, f"FAST0 context_score>={thresh}"))
    else:
        lines.append("FAST0 context_score: недоступно (нет колонки)")

    lines.append("")

    if "dist_to_peak_pct" in df_core.columns:
        dtp = pd.to_numeric(df_core["dist_to_peak_pct"], errors="coerce")
        for thresh in [0.05, 0.10, 0.20]:
            m = dtp >= thresh
            lines.append(_format_filter_line(df_core, m, n_total, f"FAST0 dist_to_peak>={thresh}"))
    else:
        lines.append("FAST0 dist_to_peak_pct: недоступно (нет колонки)")

    lines.append("")

    if "volume_zscore" in df_core.columns:
        vz = pd.to_numeric(df_core["volume_zscore"], errors="coerce")
        for thresh in [0.0, 0.5, 1.0]:
            m = vz >= thresh
            lines.append(_format_filter_line(df_core, m, n_total, f"FAST0 volume_zscore>={thresh}"))
    else:
        lines.append("FAST0 volume_zscore: недоступно (нет колонки)")

    lines.append("")

    liq_col = "liq_long_usd_30s" if "liq_long_usd_30s" in df_core.columns else (
        "liq_long_count_30s" if "liq_long_count_30s" in df_core.columns else None
    )
    if liq_col is None:
        lines.append("FAST0 liquidations hypotheses: недоступно (нет колонки liq_long_usd_30s/liq_long_count_30s)")
        return

    liq = pd.to_numeric(df_core[liq_col], errors="coerce")
    nonzero_mask = (liq.notna()) & (liq > 0)
    zero_mask = ~nonzero_mask
    lines.append(_format_filter_line(df_core, nonzero_mask, n_total, f"FAST0 {liq_col} > 0"))
    lines.append(_format_filter_line(df_core, zero_mask, n_total, f"FAST0 {liq_col} == 0"))

    # USD-only explicit thresholds (skip when using count column)
    if liq_col == "liq_long_usd_30s":
        for thresh in [5000, 25000, 100000]:
            m = (liq.notna()) & (liq >= thresh)
            lines.append(_format_filter_line(df_core, m, n_total, f"FAST0 liq_long_usd_30s >= {thresh}"))
        lines.append("")
        # Explicit range hypotheses (USD)
        range_defs = [
            ("(0,5000]", (liq > 0) & (liq <= 5_000)),
            ("(5000,25000]", (liq > 5_000) & (liq <= 25_000)),
            ("(25000,100000]", (liq > 25_000) & (liq <= 100_000)),
            ("> 100000", liq > 100_000),
        ]
        for label, mask in range_defs:
            lines.append(_format_filter_line(df_core, mask, n_total, f"FAST0 liq_long_usd_30s in {label}"))
        lines.append("")

    nonzero_count = int(nonzero_mask.sum())
    if nonzero_count < 5:
        lines.append(f"FAST0 {liq_col}: недостаточно nonzero для квантилей (n={nonzero_count})")
        return

    nonzero_vals = liq[nonzero_mask].dropna()
    p50 = nonzero_vals.quantile(0.50)
    p75 = nonzero_vals.quantile(0.75)
    p90 = nonzero_vals.quantile(0.90)
    if pd.notna(p50) and (liq >= p50).any():
        m_p50 = nonzero_mask & (liq >= p50)
        lines.append(_format_filter_line(df_core, m_p50, n_total, f"FAST0 {liq_col}>=p50(nonzero)"))
    if pd.notna(p75) and (liq >= p75).any():
        m_p75 = nonzero_mask & (liq >= p75)
        lines.append(_format_filter_line(df_core, m_p75, n_total, f"FAST0 {liq_col}>=p75(nonzero)"))
    if pd.notna(p90) and (liq >= p90).any():
        m_p90 = nonzero_mask & (liq >= p90)
        lines.append(_format_filter_line(df_core, m_p90, n_total, "FAST0 top-spike bucket (>=p90 nonzero)"))

    # Bucketed view: where does FAST0 perform best by liq_long?
    # Use USD buckets when working with liq_long_usd_30s; otherwise adapt for count-based metrics.
    lines.append("")
    if liq_col.endswith("_usd_30s"):
        bucket_defs = [
            ("==0", liq == 0),
            ("(0,5k]", (liq > 0) & (liq <= 5_000)),
            ("(5k,25k]", (liq > 5_000) & (liq <= 25_000)),
            ("(25k,100k]", (liq > 25_000) & (liq <= 100_000)),
            ("(100k,+inf)", liq > 100_000),
        ]
    else:
        # Count-based: treat 0 separately, then small/medium/large spike counts.
        bucket_defs = [
            ("==0", liq == 0),
            ("(0,10]", (liq > 0) & (liq <= 10)),
            ("(10,50]", (liq > 10) & (liq <= 50)),
            ("(50,200]", (liq > 50) & (liq <= 200)),
            ("(200,+inf)", liq > 200),
        ]

    for label, mask in bucket_defs:
        lines.append(_format_filter_line(df_core, mask, n_total, f"FAST0 {liq_col} bucket {label}"))

    # Best EV regime (among buckets, n>=5) and practical candidate (n>=10, EV>0)
    lines.append("")
    regime_stats_list: List[Tuple[str, int, float, float]] = []
    for label, mask in bucket_defs:
        n, wr, ev = _regime_stats(df_core, mask)
        regime_stats_list.append((label, n, wr, ev))
    best = max((c for c in regime_stats_list if c[1] >= 5), key=lambda c: c[3], default=None)
    if best:
        label, n, wr, ev = best
        lines.append(f"FAST0 liq regime best EV: {label} n={n} WR={wr:.1%} EV={ev:+.4f}R")
    else:
        lines.append("FAST0 liq regime best EV: no regime with n>=5")
    practical = max((c for c in regime_stats_list if c[1] >= 10 and c[3] > 0), key=lambda c: c[3], default=None)
    if practical:
        label, n, wr, ev = practical
        lines.append(f"FAST0 practical liq filter candidate: {label} n={n} WR={wr:.1%} EV={ev:+.4f}R")
    else:
        lines.append("FAST0 practical liq filter candidate: not enough stable data")


def _add_fast0_hypothesis_blocks_compact(
    lines: List[str],
    df_core: pd.DataFrame,
    n_total: int,
) -> None:
    """
    Compact FAST0 liq research: only liq==0, (5k,25k] production regime, best EV, practical candidate.
    Used when Telegram length is limited so priority blocks fit without truncation.
    """
    liq_col = "liq_long_usd_30s" if "liq_long_usd_30s" in df_core.columns else (
        "liq_long_count_30s" if "liq_long_count_30s" in df_core.columns else None
    )
    if liq_col is None:
        lines.append("FAST0 liq (compact): нет колонки liq")
        return

    liq = pd.to_numeric(df_core[liq_col], errors="coerce")
    zero_mask = (liq.isna()) | (liq == 0)
    lines.append(_format_filter_line(df_core, zero_mask, n_total, f"FAST0 {liq_col} == 0"))

    if liq_col == "liq_long_usd_30s":
        prod_mask = (liq.notna()) & (liq > FAST0_LIQ_MIN_USD) & (liq <= FAST0_LIQ_MAX_USD)
        lines.append(_format_filter_line(df_core, prod_mask, n_total, "FAST0 liq_long_usd_30s in (5k,25k]"))

    bucket_defs: List[Tuple[str, pd.Series]]
    if liq_col.endswith("_usd_30s"):
        bucket_defs = [
            ("==0", liq == 0),
            ("(0,5k]", (liq > 0) & (liq <= 5_000)),
            ("(5k,25k]", (liq > 5_000) & (liq <= 25_000)),
            ("(25k,100k]", (liq > 25_000) & (liq <= 100_000)),
            ("(100k,+inf)", liq > 100_000),
        ]
    else:
        bucket_defs = [
            ("==0", liq == 0),
            ("(0,10]", (liq > 0) & (liq <= 10)),
            ("(10,50]", (liq > 10) & (liq <= 50)),
            ("(50,200]", (liq > 50) & (liq <= 200)),
            ("(200,+inf)", liq > 200),
        ]

    regime_stats_list: List[Tuple[str, int, float, float]] = []
    for label, mask in bucket_defs:
        n, wr, ev = _regime_stats(df_core, mask)
        regime_stats_list.append((label, n, wr, ev))
    best = max((c for c in regime_stats_list if c[1] >= 5), key=lambda c: c[3], default=None)
    if best:
        label, n, wr, ev = best
        lines.append(f"FAST0 liq regime best EV: {label} n={n} WR={wr:.1%} EV={ev:+.4f}R")
    else:
        lines.append("FAST0 liq regime best EV: no regime with n>=5")
    practical = max((c for c in regime_stats_list if c[1] >= 10 and c[3] > 0), key=lambda c: c[3], default=None)
    if practical:
        label, n, wr, ev = practical
        lines.append(f"FAST0 practical liq filter candidate: {label} n={n} WR={wr:.1%} EV={ev:+.4f}R")
    else:
        lines.append("FAST0 practical liq filter candidate: not enough stable data")


def _add_fast0_whatif_tp_block(
    lines: List[str],
    df_hyp: pd.DataFrame,
) -> None:
    """
    Add FAST0 WHAT-IF TP ANALYSIS block (analytics only).
    Restricts to production liq regime 5000 < liq_long_usd_30s <= 25000.
    For each TP scenario (1.2%, 1.5%, 1.8%, 2.0%) shows n, WR, EV, avg_hold.
    Uses mfe_pct to simulate hypothetical TP hit: hyp_pnl = TP_pct if mfe_pct >= TP_pct else pnl_pct.
    SL unchanged. Summary: best TP (n>=5) or "not enough stable data".
    """
    lines.append("")
    lines.append("FAST0 WHAT-IF TP ANALYSIS")
    lines.append("(liq 5k–25k bucket, core TP/SL only, SL unchanged)")
    lines.append("")

    liq_col = "liq_long_usd_30s" if "liq_long_usd_30s" in df_hyp.columns else None
    if liq_col is None:
        lines.append("not enough stable data (no liq_long_usd_30s)")
        return

    liq = pd.to_numeric(df_hyp[liq_col], errors="coerce")
    liq_mask = (liq.notna()) & (liq > FAST0_LIQ_MIN_USD) & (liq <= FAST0_LIQ_MAX_USD)
    df_liq = df_hyp.loc[liq_mask].copy()
    if df_liq.empty:
        lines.append("not enough stable data (no trades in liq range)")
        return

    outcome_norm = df_liq["outcome"].apply(_normalize_outcome_raw)
    core_mask = outcome_norm.isin(["TP_hit", "SL_hit"])
    df_core = df_liq.loc[core_mask]
    n_core = len(df_core)
    if n_core < 1:
        lines.append("not enough stable data (no core TP/SL in liq range)")
        return

    if "mfe_pct" not in df_core.columns:
        lines.append("not enough stable data (no mfe_pct)")
        return

    mfe = pd.to_numeric(df_core["mfe_pct"], errors="coerce")
    pnl = pd.to_numeric(df_core["pnl_pct"], errors="coerce").fillna(0.0)
    valid = mfe.notna()
    n_valid = int(valid.sum())
    if n_valid < MIN_N_FOR_BEST_TP:
        lines.append("not enough stable data (mfe_pct valid n < 5)")
        return

    mfe_v = mfe.loc[valid]
    pnl_v = pnl.loc[valid]
    hold_avail = "hold_seconds" in df_core.columns
    hold_v = pd.to_numeric(df_core.loc[valid, "hold_seconds"], errors="coerce") if hold_avail else None

    scenario_stats: List[Tuple[float, int, float, float, float]] = []
    for tp_pct in FAST0_WHATIF_TP_PCTS:
        hyp_tp_hit = mfe_v >= tp_pct
        hyp_pnl = pnl_v.where(~hyp_tp_hit, float(tp_pct))
        wr = float(hyp_tp_hit.mean())
        ev = float(hyp_pnl.mean())
        avg_hold = float(hold_v.mean()) if hold_avail and hold_v is not None and hold_v.notna().any() else 0.0
        scenario_stats.append((tp_pct, n_valid, wr, ev, avg_hold))

    for tp_pct, n, wr, ev, avg_hold in scenario_stats:
        if hold_avail and avg_hold > 0:
            lines.append(f"TP={tp_pct}%: n={n} WR={wr:.1%} EV={ev:+.4f}R avg_hold={avg_hold:.0f}s")
        else:
            lines.append(f"TP={tp_pct}%: n={n} WR={wr:.1%} EV={ev:+.4f}R")

    best = max((s for s in scenario_stats if s[1] >= MIN_N_FOR_BEST_TP), key=lambda s: s[3], default=None)
    if best:
        tp_pct, n, wr, ev, _ = best
        lines.append("")
        lines.append(f"FAST0 what-if best TP: {tp_pct}% n={n} WR={wr:.1%} EV={ev:+.4f}R")
    else:
        lines.append("")
        lines.append("FAST0 what-if best TP: not enough stable data")


def _add_fast0_edge_decay_block(
    lines: List[str],
    df_hyp: pd.DataFrame,
) -> None:
    """
    Add FAST0 EDGE DECAY / TP TUNING block (analytics only).
    Production liq regime only: 5000 < liq_long_usd_30s <= 25000.
    Uses CORE only (TP/SL) in bucket — does not mix all and core.
    For each time bucket (30/60/120/180s), % of core trades (hold>=T) that reached each MFE target.
    Plus avg MFE, median MFE, avg hold_seconds and a compact TP hint.
    """
    lines.append("")
    lines.append("FAST0 EDGE DECAY / TP TUNING")
    lines.append("(liq 5k–25k bucket, core TP/SL only)")
    lines.append("")

    liq_col = "liq_long_usd_30s" if "liq_long_usd_30s" in df_hyp.columns else None
    if liq_col is None:
        lines.append("not enough stable data (no liq_long_usd_30s)")
        return

    liq = pd.to_numeric(df_hyp[liq_col], errors="coerce")
    liq_mask = (liq.notna()) & (liq > FAST0_LIQ_MIN_USD) & (liq <= FAST0_LIQ_MAX_USD)
    df_liq = df_hyp.loc[liq_mask].copy()
    if df_liq.empty:
        lines.append("not enough stable data (no trades in liq range)")
        return

    outcome_norm = df_liq["outcome"].apply(_normalize_outcome_raw)
    core_mask = outcome_norm.isin(["TP_hit", "SL_hit"])
    df_liq = df_liq.loc[core_mask].copy()
    if df_liq.empty:
        lines.append("not enough stable data (no core TP/SL in liq range)")
        return

    if "mfe_pct" not in df_liq.columns:
        lines.append("not enough stable data (no mfe_pct)")
        return
    if "hold_seconds" not in df_liq.columns:
        lines.append("not enough stable data (no hold_seconds)")
        return

    mfe = pd.to_numeric(df_liq["mfe_pct"], errors="coerce")
    hold = pd.to_numeric(df_liq["hold_seconds"], errors="coerce")
    valid = mfe.notna() & hold.notna() & (hold > 0)
    n_valid = int(valid.sum())
    if n_valid < MIN_N_EDGE_DECAY:
        lines.append("not enough stable data (mfe+hold valid n < 5)")
        return

    mfe_v = mfe.loc[valid]
    hold_v = hold.loc[valid]

    for T in FAST0_EDGE_TIME_BUCKETS:
        mask_t = hold_v >= T
        n_t = int(mask_t.sum())
        if n_t == 0:
            lines.append(f"FAST0 MFE<={T}s: n=0")
            continue
        mfe_t = mfe_v.loc[mask_t]
        parts = [f"+{t}%={100 * (mfe_t >= t).mean():.0f}%" for t in FAST0_EDGE_MFE_TARGETS]
        lines.append(f"FAST0 MFE<={T}s: n={n_t} " + "  ".join(parts))

    avg_mfe = float(mfe_v.mean())
    med_mfe = float(mfe_v.median())
    avg_hold = float(hold_v.mean())
    lines.append("")
    lines.append(f"avg MFE={avg_mfe:.2f}%  median MFE={med_mfe:.2f}%  avg hold={avg_hold:.0f}s")

    # Compact TP hint from hit-rate decay across buckets
    def _pct_at_time(sec: int) -> dict:
        mask_t = hold_v >= sec
        if mask_t.sum() < 3:
            return {}
        mfe_t = mfe_v.loc[mask_t]
        return {t: (mfe_t >= t).mean() for t in FAST0_EDGE_MFE_TARGETS}

    p30 = _pct_at_time(30)
    p60 = _pct_at_time(60)
    p120 = _pct_at_time(120)
    p180 = _pct_at_time(180)

    hint = "FAST0 TP hint: "
    if not p30 or 1.2 not in p30:
        hint += "not enough stable data"
    else:
        r_12_30 = p30.get(1.2, 0)
        r_12_60 = p60.get(1.2, 0)
        r_15_60 = p60.get(1.5, 0)
        r_15_120 = p120.get(1.5, 0)
        r_18_120 = p120.get(1.8, 0)
        r_18_180 = p180.get(1.8, 0)
        if r_12_30 >= 0.5 and r_12_60 >= 0.4 and (r_15_60 or 0) < 0.35:
            hint += "keep 1.2"
        elif (r_15_60 or 0) >= 0.4 and (r_15_120 or 0) >= 0.35:
            hint += "1.5 looks feasible"
        elif (r_18_180 or 0) >= 0.25 and (r_18_120 or 0) < 0.3:
            hint += "1.8 only for aggressive mode"
        elif r_12_30 >= 0.45:
            hint += "keep 1.2"
        else:
            hint += "keep 1.2"
    lines.append("")
    lines.append(hint)


# dist_to_peak_pct thresholds for FAST0 analysis
FAST0_DIST_THRESHOLDS = [1.0, 1.5, 2.0, 2.5, 3.0, 4.0, 5.0]
# Live autotrading: dist<=1.5, liq in [0, (5k,25k], >100k]
FAST0_OP_DIST_MAX = 1.5
FAST0_OP_LIQ_5K = 5000.0
FAST0_OP_LIQ_25K = 25000.0
FAST0_OP_LIQ_100K = 100000.0


def _add_fast0_dist_to_peak_block(
    lines: List[str],
    df_hyp: pd.DataFrame,
) -> None:
    """
    Add FAST0 DIST TO PEAK ANALYSIS block.
    Uses all FAST0 core trades (TP/SL only), slices by dist_to_peak_pct.
    """
    lines.append("")
    lines.append("FAST0 DIST TO PEAK ANALYSIS")
    lines.append("(core TP/SL)")
    lines.append("")

    if "dist_to_peak_pct" not in df_hyp.columns:
        lines.append("dist_to_peak_pct недоступно (нет колонки)")
        return

    outcome_norm = df_hyp["outcome"].apply(_normalize_outcome_raw)
    core_mask = outcome_norm.isin(["TP_hit", "SL_hit"])
    df_core = df_hyp.loc[core_mask].copy()
    n_all_core = len(df_core)
    if n_all_core == 0:
        lines.append("нет core сделок (TP/SL)")
        return

    dist = pd.to_numeric(df_core["dist_to_peak_pct"], errors="coerce")
    valid_dist = dist.notna()
    n_valid = int(valid_dist.sum())
    if n_valid == 0:
        lines.append("нет сделок с dist_to_peak_pct")
        return

    pnl = pd.to_numeric(df_core["pnl_pct"], errors="coerce").fillna(0.0)

    for thresh in FAST0_DIST_THRESHOLDS:
        mask = valid_dist & (dist <= thresh)
        n = int(mask.sum())
        if n == 0:
            lines.append(f"dist <= {thresh} : n=0 WR=N/A EV=N/A")
            continue
        n, wr, ev = _regime_stats(df_core, mask)
        lines.append(f"dist <= {thresh} : n={n} WR={wr:.1%} EV={ev:+.4f}R")

    # FAST0 OPERATIONAL BLOCKS (live autotrading: dist<=1.5, liq in [0,(5k,25k],>100k])
    liq_col = "liq_long_usd_30s" if "liq_long_usd_30s" in df_hyp.columns else (
        "liq_long_count_30s" if "liq_long_count_30s" in df_hyp.columns else None
    )
    if liq_col and "dist_to_peak_pct" in df_hyp.columns:
        dist_all = pd.to_numeric(df_hyp["dist_to_peak_pct"], errors="coerce")
        liq_all = pd.to_numeric(df_hyp[liq_col], errors="coerce")
        valid_dist_all = dist_all.notna()
        op_dist_all = valid_dist_all & (dist_all <= FAST0_OP_DIST_MAX)
        outcome_all = df_hyp["outcome"].apply(_normalize_outcome_raw)
        core_all = outcome_all.isin(["TP_hit", "SL_hit"])
        n_full_all = int(core_all.sum())
        n_full_core = n_full_all

        df_hyp_core = df_hyp.loc[core_all]

        def _op_block(label: str, mask: pd.Series) -> None:
            m_core = (mask & core_all).loc[core_all]
            n_c = int(m_core.sum())
            n_a = int(mask.sum())
            if n_c == 0:
                lines.append(f"• {label}: n_all={n_a} n_core=0 WR=N/A EV=N/A")
                return
            n_c, wr, ev = _regime_stats(df_hyp_core, m_core)
            share = 100 * n_c / n_full_core if n_full_core else 0.0
            status = "exploratory only" if n_c < 20 else ""
            status_str = f" {status}" if status else ""
            lines.append(f"• {label}: n_all={n_a} n_core={n_c} WR core={wr:.1%} EV core={ev:+.4f}R покрытие={share:.0f}%{status_str}")

        lines.append("")
        lines.append("FAST0 OPERATIONAL (dist<=1.5, live autotrading)")
        op_liq = (liq_all == 0) | ((liq_all > FAST0_OP_LIQ_5K) & (liq_all <= FAST0_OP_LIQ_25K)) | (liq_all > FAST0_OP_LIQ_100K)
        op_universe = op_dist_all & (liq_all.isna() | op_liq)
        _op_block("universe (dist<=1.5, liq in 0|(5k,25k]|>100k)", op_universe)

        lines.append("")
        lines.append("FAST0 risk mode fast0_base_1R (dist<=1.5, liq==0)")
        _op_block("fast0_base_1R", op_dist_all & (liq_all.isna() | (liq_all == 0)))

        lines.append("")
        lines.append("FAST0 risk mode fast0_1p5R (dist<=1.5, 5k<liq<=25k)")
        _op_block("fast0_1p5R", op_dist_all & (liq_all.notna()) & (liq_all > FAST0_OP_LIQ_5K) & (liq_all <= FAST0_OP_LIQ_25K))

        lines.append("")
        lines.append("FAST0 risk mode fast0_2R (dist<=1.5, liq>100k)")
        _op_block("fast0_2R", op_dist_all & (liq_all.notna()) & (liq_all > FAST0_OP_LIQ_100K))

        lines.append("")
        lines.append("FAST0 dist<=2.0 (legacy, reference only)")
        legacy_mask = valid_dist_all & (dist_all <= 2.0)
        m_leg = (legacy_mask & core_all).loc[core_all]
        n_leg = int(m_leg.sum())
        if n_leg == 0:
            lines.append("• n=0 WR=N/A EV=N/A")
        else:
            n_leg, wr, ev = _regime_stats(df_hyp_core, m_leg)
            share = 100 * n_leg / n_all_core if n_all_core else 0.0
            lines.append(f"• n={n_leg} WR core={wr:.1%} EV core={ev:+.4f}R покрытие={share:.0f}%")


def _add_fast0_active_mode_block(
    lines: List[str],
    df_hyp: pd.DataFrame,
    rolling_n: int,
    maturity_threshold: int,
) -> None:
    """
    Add FAST0 candidate bucket block (liq 5k–25k) for short_pump_fast0.
    Shown as candidate, not confirmed active mode.
    n_all = TP/SL/TIMEOUT, n_core = TP/SL only.
    If n_core < 20: exploratory only, no operational verdict.
    """
    lines.append("")
    lines.append("FAST0 candidate bucket (liq 5k–25k)")
    lines.append("")

    liq_col = "liq_long_usd_30s" if "liq_long_usd_30s" in df_hyp.columns else None
    if liq_col is None:
        lines.append("liq_long_usd_30s недоступно — блок пропущен")
        return

    liq = pd.to_numeric(df_hyp[liq_col], errors="coerce")
    active_mask = (liq.notna()) & (liq > FAST0_LIQ_MIN_USD) & (liq <= FAST0_LIQ_MAX_USD)
    df_active = df_hyp.loc[active_mask].copy()
    n_active_all = len(df_active)
    n_full_all = len(df_hyp)

    outcome_norm = df_active["outcome"].apply(_normalize_outcome_raw)
    core_mask_active = outcome_norm.isin(["TP_hit", "SL_hit"])
    df_active_core = df_active.loc[core_mask_active]
    n_active_core = len(df_active_core)

    outcome_full = df_hyp["outcome"].apply(_normalize_outcome_raw)
    core_mask_full = outcome_full.isin(["TP_hit", "SL_hit"])
    n_full_core = int(core_mask_full.sum())

    if n_active_all == 0:
        lines.append("нет сделок в диапазоне liq 5k–25k")
        lines.append("")
        lines.append("FAST0 bucket verdict: not enough stable data")
        return

    pnl = pd.to_numeric(df_active["pnl_pct"], errors="coerce").fillna(0.0)
    equity_all = pnl.cumsum()
    mdd = (equity_all - equity_all.cummax()).min()
    lines.append(f"• Кривая капитала (все): последнее={equity_all.iloc[-1]:.3f}R  MDD={mdd:.3f}R")
    lines.append(f"• Сделки: n_all={n_active_all}  n_core={n_active_core}")

    tp = int((outcome_norm == "TP_hit").sum())
    sl = int((outcome_norm == "SL_hit").sum())
    timeout = int((outcome_norm == "TIMEOUT").sum())
    wr_str = "N/A"
    ev_str = "N/A"
    ev_active = 0.0
    if n_active_core > 0:
        pnl_core = pd.to_numeric(df_active.loc[core_mask_active, "pnl_pct"], errors="coerce").fillna(0.0)
        wr_val = wr_core_from_tp_sl(tp, sl)
        ev_val = ev_core_from_tp_sl_pnl(tp, sl, pnl_core)
        ev_active = float(ev_val)
        wr_str = f"{wr_val:.1%}"
        ev_str = f"{ev_val:+.4f}R"
    lines.append(f"• TP/SL/TIMEOUT: {tp}/{sl}/{timeout}  WR core={wr_str}  EV core={ev_str}")

    maturity_status = _maturity_status_unified(n_active_core)
    lines.append(f"• Зрелость (core): {n_active_core}  статус={maturity_status}")

    if n_active_core == 0:
        lines.append("• Скользящий WR/EV (core): нет core исходов")
    else:
        pnl_core_vals = pnl[core_mask_active].values
        win_core = (pnl_core_vals > 0).astype(float)
        roll_n = min(rolling_n, n_active_core)
        suffix = f" (n<{rolling_n})" if n_active_core < rolling_n else f" (N={rolling_n})"
        wr_last = win_core[-roll_n:].mean() * 100 if roll_n else 0.0
        ev_last = pnl_core_vals[-roll_n:].mean() if roll_n else 0.0
        lines.append(f"• Скользящий WR core{suffix}: {wr_last:.1f}%")
        lines.append(f"• Скользящий EV core{suffix}: {ev_last:+.4f}R")
        lines.append(f"• Скользящее кол-во сделок{suffix}: {roll_n}")

    kept_core = f"{n_active_core}/{n_full_core}" if n_full_core else "0/0"
    kept_all = f"{n_active_all}/{n_full_all}" if n_full_all else "0/0"
    lines.append(f"• Покрытие: core {kept_core}  all {kept_all}")

    wr_full_str = "N/A"
    ev_full_val = 0.0
    if n_full_core > 0:
        tp_full = int((outcome_full == "TP_hit").sum())
        sl_full = int((outcome_full == "SL_hit").sum())
        pnl_full = pd.to_numeric(df_hyp.loc[core_mask_full, "pnl_pct"], errors="coerce").fillna(0.0)
        wr_full = wr_core_from_tp_sl(tp_full, sl_full)
        ev_full_val = float(ev_core_from_tp_sl_pnl(tp_full, sl_full, pnl_full))
        wr_full_str = f"{wr_full:.1%}"
    ev_full_str = f"{ev_full_val:+.4f}R" if n_full_core else "N/A"
    active_wr_ev = f"WR={wr_str} EV={ev_str}" if n_active_core else "WR=N/A EV=N/A"
    lines.append("")
    lines.append(f"FAST0 split: bucket 5k–25k {active_wr_ev}  vs all FAST0 WR={wr_full_str} EV={ev_full_str}")

    if n_active_core < 20:
        verdict = "exploratory only, no operational verdict"
    elif n_active_core < 5:
        verdict = "not enough stable data"
    elif ev_active > 0.02:
        verdict = "positive candidate"
    elif ev_active > 0:
        verdict = "weak candidate"
    else:
        verdict = "weak candidate"
    lines.append("")
    lines.append(f"FAST0 bucket verdict: {verdict}")


