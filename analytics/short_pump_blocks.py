from __future__ import annotations

from typing import Tuple

import pandas as pd

from .stats import _normalize_outcome_raw, _core_mask


def filter_active_trades(
    df: pd.DataFrame,
    strategy: str,
    tg_dist_min: float = 3.5,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Split df into ACTIVE (stage4 + dist>=tg) and RAW.
    Returns (df_active, df_active_core, df_raw, df_raw_core).
    core = outcome in TP/SL (TIMEOUT excluded).
    For non-short_pump: df_active=df_raw, df_active_core=df_raw_core.
    """
    df_raw = df
    outcome_norm = df["outcome"].apply(lambda x: _normalize_outcome_raw(x))
    core_mask_raw = _core_mask(df["outcome"])
    df_raw_core = df[core_mask_raw].copy()

    if strategy != "short_pump":
        return df_raw, df_raw_core, df_raw, df_raw_core

    if "stage" not in df.columns or "dist_to_peak_pct" not in df.columns:
        return df_raw, df_raw_core, df_raw, df_raw_core

    stage_num = pd.to_numeric(df["stage"], errors="coerce")
    dtp = pd.to_numeric(df["dist_to_peak_pct"], errors="coerce")
    active_mask = (stage_num == 4) & stage_num.notna() & (dtp >= tg_dist_min)
    df_active = df[active_mask].copy()
    outcome_active = df_active["outcome"].apply(lambda x: _normalize_outcome_raw(x))
    core_mask_active = outcome_active.isin(["TP_hit", "SL_hit"])
    df_active_core = df_active[core_mask_active].copy()

    return df_active, df_active_core, df_raw, df_raw_core


