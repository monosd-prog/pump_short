from __future__ import annotations

from itertools import combinations

import numpy as np
import pandas as pd

from analytics.utils import dprint, DEBUG_ENABLED

# Bucket columns used for rule candidates (gates); only those present in df are used
RULE_BUCKET_COLS = [
    "context_score_bucket",
    "delta_ratio_30s_bucket",
    "dist_to_peak_pct_bucket",
    "funding_rate_bucket",
    "oi_change_fast_bucket",
    "cvd_1m_bucket",
]


def _default_context_bins() -> list[float]:
    return [-np.inf, 0.0, 0.2, 0.4, 0.6, 0.8, 1.0, np.inf]


def _default_delta_bins() -> list[float]:
    return [-np.inf, 0.5, 0.8, 1.0, 1.2, 1.5, 2.0, np.inf]


def _default_liq_bins() -> list[float]:
    return [-np.inf, 1_000, 5_000, 10_000, 25_000, 50_000, 100_000, np.inf]


def _pick_liq_usd(df: pd.DataFrame) -> pd.Series:
    for window in ["30s", "60s", "5m"]:
        short_col = f"liq_short_usd_{window}"
        long_col = f"liq_long_usd_{window}"
        if short_col in df.columns or long_col in df.columns:
            if short_col in df.columns:
                short_vals = pd.to_numeric(df[short_col], errors="coerce")
            else:
                short_vals = pd.Series([np.nan] * len(df), index=df.index)
            if long_col in df.columns:
                long_vals = pd.to_numeric(df[long_col], errors="coerce")
            else:
                long_vals = pd.Series([np.nan] * len(df), index=df.index)
            return short_vals.fillna(0) + long_vals.fillna(0)
    return pd.Series([np.nan] * len(df), index=df.index)


def add_buckets(
    df: pd.DataFrame,
    context_bins: list[float] | None = None,
    delta_bins: list[float] | None = None,
    liq_bins: list[float] | None = None,
) -> pd.DataFrame:
    result = df.copy()

    if context_bins is None:
        context_bins = _default_context_bins()
    if delta_bins is None:
        delta_bins = _default_delta_bins()
    if liq_bins is None:
        liq_bins = _default_liq_bins()

    if "context_score" in result.columns:
        result["context_score_bucket"] = pd.cut(
            pd.to_numeric(result["context_score"], errors="coerce"),
            bins=context_bins,
        )

    if "delta_ratio_30s" in result.columns:
        result["delta_ratio_30s_bucket"] = pd.cut(
            pd.to_numeric(result["delta_ratio_30s"], errors="coerce"),
            bins=delta_bins,
        )

    result["liq_usd"] = _pick_liq_usd(result)
    result["liq_usd_bucket"] = pd.cut(
        pd.to_numeric(result["liq_usd"], errors="coerce"),
        bins=liq_bins,
    )

    return result


def _aggregate_by(df: pd.DataFrame, group_col: str) -> pd.DataFrame:
    if group_col not in df.columns:
        return pd.DataFrame()

    grouped = (
        df.groupby(group_col, dropna=False, observed=True)
        .agg(**{
            "n": ("win", "size"),
            "winrate": ("win", "mean"),
            "ev": ("pnl_pct", "mean"),
            "avg_hold_seconds": ("hold_seconds", "mean"),
        })
        .reset_index()
    )

    return grouped


def aggregate_buckets(df: pd.DataFrame) -> dict[str, pd.DataFrame]:
    df = add_buckets(df)
    return {
        "context_score_bucket": _aggregate_by(df, "context_score_bucket"),
        "delta_ratio_30s_bucket": _aggregate_by(df, "delta_ratio_30s_bucket"),
        "liq_usd_bucket": _aggregate_by(df, "liq_usd_bucket"),
    }


def _aggregate_by_pair(
    df: pd.DataFrame,
    group_col_a: str,
    group_col_b: str,
) -> pd.DataFrame:
    if group_col_a not in df.columns or group_col_b not in df.columns:
        return pd.DataFrame()
    if "win" not in df.columns or "pnl_pct" not in df.columns:
        return pd.DataFrame()
    grouped = (
        df.groupby([group_col_a, group_col_b], dropna=False, observed=True)
        .agg(
            n=("win", "size"),
            winrate=("win", "mean"),
            ev=("pnl_pct", "mean"),
            avg_hold_seconds=("hold_seconds", "mean"),
        )
        .reset_index()
    )
    return grouped


def top_buckets_by_pair(
    df: pd.DataFrame,
    group_col_a: str,
    group_col_b: str,
    top_n: int = 10,
    min_count: int = 5,
    include_nan: bool = False,
) -> pd.DataFrame:
    """Top bucket pairs by EV. If no rows with n>=min_count, retries with min_count=1."""
    agg = _aggregate_by_pair(df, group_col_a, group_col_b)
    if agg.empty:
        return agg
    if not include_nan:
        agg = agg.dropna(subset=[group_col_a, group_col_b])
    filtered = agg[agg["n"] >= min_count]
    if filtered.empty and min_count > 1:
        filtered = agg[agg["n"] >= 1]
    return filtered.sort_values("ev", ascending=False).head(top_n)


def top_buckets(
    df: pd.DataFrame,
    group_col: str,
    top_n: int = 10,
    min_count: int = 1,
    include_nan: bool = False,
) -> pd.DataFrame:
    if group_col not in df.columns:
        dprint(DEBUG_ENABLED, f"[DEBUG_AGG] missing group_col={group_col}")
        return pd.DataFrame()

    dprint(DEBUG_ENABLED, "[DEBUG_AGG] group_col:", group_col)
    dprint(DEBUG_ENABLED, "[DEBUG_AGG] df.columns has group_col:", group_col in df.columns)
    dprint(
        DEBUG_ENABLED,
        "[DEBUG_AGG] group_col dtype:",
        df[group_col].dtype if group_col in df.columns else None,
    )
    if group_col in df.columns:
        dprint(
            DEBUG_ENABLED,
            "[DEBUG_AGG] group_col value_counts:\n",
            df[group_col].value_counts(dropna=False),
        )
        dprint(
            DEBUG_ENABLED,
            "[DEBUG_AGG] group_col unique sample:",
            list(df[group_col].dropna().unique())[:10],
        )

    nonnull_bucket = int(df[group_col].notna().sum())
    if "pnl_pct" in df.columns:
        nonnull_pnl = int(df["pnl_pct"].notna().sum())
        rows_after_dropna = df.dropna(subset=[group_col, "pnl_pct"]).shape[0]
    else:
        nonnull_pnl = 0
        rows_after_dropna = 0
    dprint(
        DEBUG_ENABLED,
        f"[DEBUG_AGG] nonnull_bucket={nonnull_bucket} "
        f"nonnull_pnl={nonnull_pnl} rows_after_dropna={rows_after_dropna}"
    )

    work_cols = [group_col]
    if "pnl_pct" in df.columns:
        work_cols.append("pnl_pct")
    work = df[work_cols].copy()
    dprint(DEBUG_ENABLED, "[DEBUG_AGG] work columns:", work.columns.tolist())
    dprint(
        DEBUG_ENABLED,
        "[DEBUG_AGG] work[group_col] value_counts:\n",
        work[group_col].value_counts(dropna=False),
    )

    if not include_nan:
        work = work.dropna(subset=[group_col])

    agg = _aggregate_by(df.loc[work.index], group_col)
    if agg.empty:
        return agg

    sizes = work.groupby(group_col, dropna=False, observed=True).size().head(10)
    dprint(DEBUG_ENABLED, "[DEBUG_AGG] group sizes head:")
    dprint(DEBUG_ENABLED, sizes)
    dprint(DEBUG_ENABLED, f"[DEBUG_AGG] min_trades={min_count}")

    agg = agg[agg["n"] >= min_count]
    return agg.sort_values("ev", ascending=False).head(top_n)


def _rule_cond_string(candidate: tuple[tuple[str, object], ...]) -> str:
    """Canonical condition string: sorted by column name, col=val & col2=val2."""
    parts = sorted(candidate, key=lambda x: x[0])
    return " & ".join(f"{c}={v}" for c, v in parts)


def rule_candidates(
    df: pd.DataFrame,
    total_trades: int,
    min_support: int,
    top_n: int = 10,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Generate rule candidates (gates) from bucket columns: singles, pairs, triples.
    Returns (top_by_ev, worst_by_ev) with columns cond, n, winrate, ev, share.
    """
    if total_trades <= 0 or "pnl_pct" not in df.columns:
        return pd.DataFrame(), pd.DataFrame()
    bucket_cols = [c for c in RULE_BUCKET_COLS if c in df.columns]
    if not bucket_cols:
        return pd.DataFrame(), pd.DataFrame()

    uniques = {col: df[col].dropna().unique() for col in bucket_cols}
    win_ser = df.get("win")
    if win_ser is not None:
        win_ser = pd.to_numeric(win_ser, errors="coerce")
    pnl_ser = pd.to_numeric(df["pnl_pct"], errors="coerce")

    def eval_candidate(candidate: tuple[tuple[str, object], ...]) -> dict | None:
        mask = pd.Series(True, index=df.index)
        for col, val in candidate:
            mask = mask & (df[col] == val)
        n = int(mask.sum())
        if n < min_support:
            return None
        sub_pnl = pnl_ser.loc[mask]
        ev = sub_pnl.mean() if sub_pnl.notna().any() else np.nan
        if win_ser is not None:
            sub_win = win_ser.loc[mask]
            winrate = sub_win.mean() if sub_win.notna().any() else np.nan
        else:
            winrate = np.nan
        share = n / total_trades if total_trades else 0
        return {
            "cond": _rule_cond_string(candidate),
            "n": n,
            "winrate": winrate,
            "ev": ev,
            "share": share,
        }

    rows: list[dict] = []
    seen: set[str] = set()

    # Singles
    for col in bucket_cols:
        for val in uniques[col]:
            cand = ((col, val),)
            key = _rule_cond_string(cand)
            if key in seen:
                continue
            seen.add(key)
            r = eval_candidate(cand)
            if r is not None:
                rows.append(r)

    # Pairs
    for c1, c2 in combinations(bucket_cols, 2):
        for v1 in uniques[c1]:
            for v2 in uniques[c2]:
                cand = tuple(sorted([(c1, v1), (c2, v2)], key=lambda x: x[0]))
                key = _rule_cond_string(cand)
                if key in seen:
                    continue
                seen.add(key)
                r = eval_candidate(cand)
                if r is not None:
                    rows.append(r)

    # Triples
    for c1, c2, c3 in combinations(bucket_cols, 3):
        for v1 in uniques[c1]:
            for v2 in uniques[c2]:
                for v3 in uniques[c3]:
                    cand = tuple(sorted([(c1, v1), (c2, v2), (c3, v3)], key=lambda x: x[0]))
                    key = _rule_cond_string(cand)
                    if key in seen:
                        continue
                    seen.add(key)
                    r = eval_candidate(cand)
                    if r is not None:
                        rows.append(r)

    if not rows:
        return pd.DataFrame(), pd.DataFrame()
    res = pd.DataFrame(rows)
    res = res.sort_values(
        by=["ev", "n", "winrate"],
        ascending=[False, False, False],
        na_position="last",
    )
    top = res.head(top_n)
    worst = res.tail(top_n).sort_values("ev", ascending=True).reset_index(drop=True)
    return top, worst
