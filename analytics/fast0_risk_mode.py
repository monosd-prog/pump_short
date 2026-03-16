"""
FAST0 risk mode: actual vs inferred for safe analytics extension.

Concepts:
- risk_profile_actual: from outcomes (source of truth, never overwritten)
- risk_profile_inferred: reconstructed from dist/liq when actual is missing

Used for two-layer stats:
- factual: actual only
- extended: actual + inferred (for historical coverage)
"""

from __future__ import annotations

from typing import Any, Dict, Optional, Tuple

import pandas as pd

from .fast0_blocks import FAST0_OP_DIST_MAX, FAST0_OP_LIQ_5K, FAST0_OP_LIQ_25K, FAST0_OP_LIQ_100K

VALID_MODES = ("fast0_base_1R", "fast0_1p5R", "fast0_2R")


def _normalized_to_canonical(norm: str) -> str:
    """Map lowercase normalized value to canonical VALID_MODES, or ''."""
    if not norm:
        return ""
    for m in VALID_MODES:
        if m.lower() == norm:
            return m
    return ""


def _normalize_rp(val: Any) -> str:
    """Normalize risk_profile to lowercase or empty."""
    if pd.isna(val) or val is None:
        return ""
    s = str(val).strip().lower()
    if s in ("", "nan", "none"):
        return ""
    return s


def _infer_single(
    dist: float | None,
    liq: float | None,
) -> str:
    """
    Infer FAST0 risk mode from dist/liq using production rules.
    Returns mode name or empty string if not inferrable.
    """
    if dist is None or dist > FAST0_OP_DIST_MAX:
        return ""
    liq_val = float(liq) if liq is not None and not pd.isna(liq) else None
    if liq_val is None or liq_val == 0:
        return "fast0_base_1R"
    if FAST0_OP_LIQ_5K < liq_val <= FAST0_OP_LIQ_25K:
        return "fast0_1p5R"
    if liq_val > FAST0_OP_LIQ_100K:
        return "fast0_2R"
    return ""


def add_risk_profile_columns(df: pd.DataFrame, inplace: bool = False) -> pd.DataFrame:
    """
    Add risk_profile_actual and risk_profile_inferred to df (copy by default).

    - risk_profile_actual: from existing 'risk_profile' column, normalized
    - risk_profile_inferred: computed from dist/liq only when actual is empty

    Does NOT overwrite source data. Returns a copy unless inplace=True.
    """
    out = df if inplace else df.copy()
    rp_col = "risk_profile" if "risk_profile" in out.columns else None
    rp = out[rp_col].apply(_normalize_rp) if rp_col else pd.Series([""] * len(out), index=out.index)
    out["risk_profile_actual"] = rp.apply(_normalized_to_canonical)

    dist_col = "dist_to_peak_pct" if "dist_to_peak_pct" in out.columns else None
    liq_col = "liq_long_usd_30s" if "liq_long_usd_30s" in out.columns else "liq_long_count_30s"
    if liq_col not in out.columns:
        liq_col = None

    def _infer_row(row: pd.Series) -> str:
        if rp_col and _normalized_to_canonical(_normalize_rp(row.get(rp_col, ""))):
            return ""
        dist_val = None
        if dist_col:
            try:
                v = row.get(dist_col)
                if v is not None and not pd.isna(v):
                    dist_val = float(v)
            except (TypeError, ValueError):
                pass
        liq_val = None
        if liq_col:
            try:
                v = row.get(liq_col)
                if v is not None and not pd.isna(v):
                    liq_val = float(v)
            except (TypeError, ValueError):
                pass
        return _infer_single(dist_val, liq_val)

    inferred = out.apply(_infer_row, axis=1)
    out["risk_profile_inferred"] = inferred
    return out


def get_factual_mode_mask(df: pd.DataFrame, mode: str) -> pd.Series:
    """Mask for rows with actual risk_profile == mode. Only factual data."""
    if mode not in VALID_MODES:
        return pd.Series([False] * len(df), index=df.index)
    col = "risk_profile_actual" if "risk_profile_actual" in df.columns else "risk_profile"
    if col not in df.columns:
        return pd.Series([False] * len(df), index=df.index)
    rp = df[col].apply(_normalize_rp)
    return (rp == mode.lower()) & (rp != "")


def get_extended_mode_mask(df: pd.DataFrame, mode: str) -> pd.Series:
    """Mask for rows: actual == mode OR (actual empty AND inferred == mode)."""
    if mode not in VALID_MODES:
        return pd.Series([False] * len(df), index=df.index)
    work = add_risk_profile_columns(df) if "risk_profile_actual" not in df.columns else df
    actual = work["risk_profile_actual"] == mode
    inferred_used = (work["risk_profile_actual"] == "") & (work["risk_profile_inferred"] == mode)
    return (actual | inferred_used).reindex(df.index).fillna(False)


def fast0_risk_modes_stats(
    df: pd.DataFrame,
    *,
    core_mask: Optional[pd.Series] = None,
) -> Dict[str, Dict[str, Any]]:
    """
    Compute factual and extended stats per FAST0 mode.

    Returns:
        {
            "fast0_base_1R": {"factual_n": int, "extended_n": int, "recovered_n": int},
            ...
        }
    """
    if df is None or df.empty:
        return {m: {"factual_n": 0, "extended_n": 0, "recovered_n": 0} for m in VALID_MODES}
    sub = df[core_mask] if core_mask is not None else df
    if sub.empty:
        return {m: {"factual_n": 0, "extended_n": 0, "recovered_n": 0} for m in VALID_MODES}
    df_work = add_risk_profile_columns(sub.copy())
    out: Dict[str, Dict[str, Any]] = {}
    for mode in VALID_MODES:
        factual = get_factual_mode_mask(df_work, mode).reindex(df_work.index).fillna(False)
        extended = get_extended_mode_mask(df_work, mode)
        recovered = extended & ~factual
        out[mode] = {
            "factual_n": int(factual.sum()),
            "extended_n": int(extended.sum()),
            "recovered_n": int(recovered.sum()),
        }
    return out


def factual_extended_summary(df: pd.DataFrame, core_mask: Optional[pd.Series] = None) -> Tuple[str, Dict[str, Any]]:
    """
    Produce a text summary and raw counts for audit/debug.
    Returns (text, dict) with factual/extended/recovered per mode and total.
    """
    if df is None or df.empty:
        return "No data", {}
    sub = df[core_mask] if core_mask is not None else df
    stats = fast0_risk_modes_stats(sub)
    df_work = add_risk_profile_columns(sub.copy())
    n_actual_any = (df_work["risk_profile_actual"] != "").sum()
    n_inferred_any = ((df_work["risk_profile_actual"] == "") & (df_work["risk_profile_inferred"] != "")).sum()
    n_unrecoverable = ((df_work["risk_profile_actual"] == "") & (df_work["risk_profile_inferred"] == "")).sum()

    lines = [
        "FAST0 risk mode stats (factual vs extended)",
        "",
        f"Total core rows: {len(sub)}",
        f"  with risk_profile_actual: {n_actual_any}",
        f"  recovered by inferred:    {n_inferred_any}",
        f"  unrecoverable:            {n_unrecoverable}",
        "",
        "Per mode:",
    ]
    for mode in VALID_MODES:
        s = stats[mode]
        lines.append(f"  {mode}: factual={s['factual_n']} extended={s['extended_n']} recovered={s['recovered_n']}")
    summary = {
        "total": len(sub),
        "with_actual": int(n_actual_any),
        "recovered_inferred": int(n_inferred_any),
        "unrecoverable": int(n_unrecoverable),
        "per_mode": stats,
    }
    return "\n".join(lines), summary
