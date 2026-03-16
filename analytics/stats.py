from __future__ import annotations

from typing import Any, List, Tuple

import pandas as pd

# LightGBM readiness thresholds (separate from maturity_threshold=50)
LIGHTGBM_MIN = 150
LIGHTGBM_COMFORT = 200


CORE_OUTCOMES = ("TP_hit", "SL_hit")


def wr_core_from_tp_sl(tp: int, sl: int) -> float:
    """WR core = TP / (TP+SL). Count-based, consistent with displayed TP/SL. Returns 0.0 if tp+sl==0."""
    denom = tp + sl
    return (tp / denom) if denom else 0.0


def _sanity_check_wr(tp: int, sl: int, pnl_series: pd.Series, label: str = "") -> None:
    """If DEBUG=1: warn when WR from TP/SL differs from pnl-based WR by >5%."""
    import os
    import sys
    if os.getenv("DEBUG") != "1" or (tp + sl) == 0:
        return
    wr_tp_sl = wr_core_from_tp_sl(tp, sl)
    pnl = pd.to_numeric(pnl_series, errors="coerce").dropna()
    if pnl.empty:
        return
    wr_pnl = float((pnl > 0).mean())
    if abs(wr_tp_sl - wr_pnl) > 0.05:
        print(
            f"DEBUG WR mismatch {label}: TP/SL implies {wr_tp_sl:.1%}, pnl-based {wr_pnl:.1%}",
            file=sys.stderr,
        )


def _sanity_check_ev_core(tp: int, sl: int, pnl_series: pd.Series, ev: float, label: str = "") -> None:
    """
    If DEBUG=1: verify that supplied EV is consistent with WR/avg_win/avg_loss within 0.01R.
    This is a defensive check against future regressions in EV computation.
    """
    import os
    import sys
    if os.getenv("DEBUG") != "1" or (tp + sl) == 0:
        return
    wr = wr_core_from_tp_sl(tp, sl)
    pnl = pd.to_numeric(pnl_series, errors="coerce").dropna()
    if pnl.empty:
        return
    wins = pnl[pnl > 0]
    losses = pnl[pnl < 0]
    avg_win = wins.mean() if len(wins) > 0 else 0.0
    avg_loss = losses.mean() if len(losses) > 0 else 0.0
    if pd.isna(avg_win):
        avg_win = 0.0
    if pd.isna(avg_loss):
        avg_loss = 0.0
    expected_ev = wr * avg_win + (1.0 - wr) * avg_loss
    if pd.isna(expected_ev):
        expected_ev = 0.0
    if abs(float(ev) - float(expected_ev)) > 0.01:
        print(
            f"DEBUG EV mismatch {label}: ev={float(ev):+.4f}R, expected={float(expected_ev):+.4f}R",
            file=sys.stderr,
        )


def ev_core_from_tp_sl_pnl(tp: int, sl: int, pnl_series: pd.Series) -> float:
    """EV core = WR*avg_win + (1-WR)*avg_loss. Uses wr=TP/(TP+SL) for consistency with displayed counts."""
    wr = wr_core_from_tp_sl(tp, sl)
    pnl = pd.to_numeric(pnl_series, errors="coerce").dropna()
    if pnl.empty:
        return 0.0
    avg_win = pnl[pnl > 0].mean() if (pnl > 0).any() else 0.0
    avg_loss = pnl[pnl < 0].mean() if (pnl < 0).any() else 0.0
    if pd.isna(avg_win):
        avg_win = 0.0
    if pd.isna(avg_loss):
        avg_loss = 0.0
    ev = wr * avg_win + (1.0 - wr) * avg_loss
    ev_out = float(ev) if not pd.isna(ev) else 0.0
    _sanity_check_ev_core(tp, sl, pnl, ev_out)
    return ev_out


def _normalize_outcome_raw(raw: Any) -> str:
    """Normalize raw outcome value to canonical label."""
    if pd.isna(raw):
        return "UNKNOWN"
    outcome_norm = str(raw).upper().strip()
    if outcome_norm == "":
        return "UNKNOWN"
    if outcome_norm in ("TP_HIT", "TP"):
        return "TP_hit"
    if outcome_norm in ("SL_HIT", "SL"):
        return "SL_hit"
    if outcome_norm in ("TIMEOUT", "TIME_OUT"):
        return "TIMEOUT"
    if outcome_norm == "UNKNOWN":
        return "UNKNOWN"
    return "OTHER"


def _core_mask(outcome_series: pd.Series) -> pd.Series:
    """Core trades mask: TP/SL only (TIMEOUT and other outcomes excluded)."""
    norm = outcome_series.apply(_normalize_outcome_raw)
    return norm.isin(CORE_OUTCOMES)


def rolling_wr_ev_core(df: pd.DataFrame, window: int = 20) -> Tuple[float, float, int]:
    """
    Rolling WR/EV on core subset only (TP/SL).
    - Takes last `window` core trades (by existing order in df)
    - WR = TP / (TP+SL) on that subset
    - EV = WR*avg_win + (1-WR)*avg_loss on that subset
    Returns (wr_fraction, ev_R, n_core_used).
    """
    if df is None or df.empty or "outcome" not in df.columns or "pnl_pct" not in df.columns:
        return 0.0, 0.0, 0
    norm = df["outcome"].apply(_normalize_outcome_raw)
    core_mask = norm.isin(CORE_OUTCOMES)
    df_core = df.loc[core_mask].copy()
    n_core = len(df_core)
    if n_core == 0:
        return 0.0, 0.0, 0
    last_core = df_core.tail(window)
    n_used = len(last_core)
    if n_used == 0:
        return 0.0, 0.0, 0
    norm_last = last_core["outcome"].apply(_normalize_outcome_raw)
    tp = int((norm_last == "TP_hit").sum())
    sl = int((norm_last == "SL_hit").sum())
    denom = tp + sl
    if denom == 0:
        return 0.0, 0.0, n_used
    wr = wr_core_from_tp_sl(tp, sl)
    pnl = pd.to_numeric(last_core["pnl_pct"], errors="coerce").dropna()
    if pnl.empty:
        return wr, 0.0, n_used
    wins = pnl[pnl > 0]
    losses = pnl[pnl < 0]
    avg_win = wins.mean() if len(wins) > 0 else 0.0
    avg_loss = losses.mean() if len(losses) > 0 else 0.0
    if pd.isna(avg_win):
        avg_win = 0.0
    if pd.isna(avg_loss):
        avg_loss = 0.0
    ev = wr * avg_win + (1.0 - wr) * avg_loss
    if pd.isna(ev):
        ev = 0.0
    return float(wr), float(ev), int(n_used)


def _safe_stats_core(pnl_series: pd.Series) -> Tuple[float, float, float, float]:
    """
    Compute (winrate, avg_win, avg_loss, ev) for core trades.
    Handles NaNs and empty series; returns zeros when stats are not available.
    """
    pnl = pd.to_numeric(pnl_series, errors="coerce").dropna()
    if pnl.empty:
        return 0.0, 0.0, 0.0, 0.0

    wins = pnl[pnl > 0]
    losses = pnl[pnl < 0]

    wr = (pnl > 0).mean()
    avg_win = wins.mean() if len(wins) > 0 else 0.0
    avg_loss = losses.mean() if len(losses) > 0 else 0.0

    if pd.isna(avg_win):
        avg_win = 0.0
    if pd.isna(avg_loss):
        avg_loss = 0.0

    ev = wr * avg_win + (1.0 - wr) * avg_loss
    if pd.isna(ev):
        ev = 0.0

    return float(wr), float(avg_win), float(avg_loss), float(ev)


def _maturity_status_unified(n_core: int) -> str:
    """
    Unified maturity phrasing:
    - n_core >= 50: statistically meaningful
    - 20..49: early but usable
    - <20: exploratory only
    """
    if n_core >= 50:
        return "🟢 statistically meaningful"
    if n_core >= 20:
        return "🟡 early but usable"
    return "🔴 exploratory only"


def _format_filter_line(
    df: pd.DataFrame,
    mask: pd.Series,
    n_total: int,
    prefix: str,
) -> str:
    """
    Canonical formatted line for hypothesis / filter descriptions.
    Matches semantics of _hypothesis_line in daily_tg_report.
    """
    n = int(mask.sum())
    if n_total == 0:
        share_str = "0.0%"
    else:
        share_str = f"{(100 * n / n_total):.1f}%"

    if n == 0:
        return f"{prefix}: n=0, WR=N/A, EV=N/A, доля={share_str}"

    pnl = pd.to_numeric(df.loc[mask, "pnl_pct"], errors="coerce").dropna().values
    if len(pnl) == 0:
        return f"{prefix}: n={n}, WR=N/A, EV=N/A, доля={share_str}"

    wr, avg_win, avg_loss, ev = _safe_stats_core(pd.Series(pnl))
    return f"{prefix}: n={n}, WR={wr:.1%}, EV={ev:+.4f}R, доля={share_str}"


def lightgbm_readiness_block(n_core: int) -> List[str]:
    """
    Build LIGHTGBM READINESS block for management reports.
    Status: 🔴 Рано (core<150), 🟡 Почти готово (150<=core<200), 🟢 Можно начинать (core>=200).
    """
    short_min = max(0, LIGHTGBM_MIN - n_core)
    short_comfort = max(0, LIGHTGBM_COMFORT - n_core)
    if n_core < LIGHTGBM_MIN:
        status = "🔴 Рано"
    elif n_core < LIGHTGBM_COMFORT:
        status = "🟡 Почти готово"
    else:
        status = "🟢 Можно начинать"
    return [
        "",
        "🤖 LIGHTGBM READYNESS",
        "",
        f"• Core-сделки: {n_core}",
        f"• Минимум для старта ML: {LIGHTGBM_MIN}",
        f"• Комфортно для старта ML: {LIGHTGBM_COMFORT}",
        f"• Не хватает до минимума: {short_min}",
        f"• Не хватает до комфортного уровня: {short_comfort}",
        f"• Статус: {status}",
    ]


