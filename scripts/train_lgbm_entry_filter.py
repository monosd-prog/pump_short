#!/usr/bin/env python3
"""
LightGBM entry filter v1 (offline).

Goal:
  Train a binary classifier to score ENTRY_OK events and help filter bad entries.
  This script DOES NOT change runtime trading logic; it produces a model + artifacts.

Dataset:
  - mode=live
  - strategies: short_pump + short_pump_fast0
  - join: outcomes_v3 (TP_hit/SL_hit) -> trades_v3 -> events_v3
  - target: TP_hit -> 1, SL_hit -> 0
  - TIMEOUT is excluded in v1

Outputs:
  - models/lgbm_entry_filter.txt
  - models/lgbm_entry_filter_meta.json (feature list + median imputation)
  - reports/lgbm_entry_filter_report.txt
"""

from __future__ import annotations

import argparse
import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Sequence, Tuple

import numpy as np
import pandas as pd

import lightgbm as lgb

from analytics.load import load_events_v2, load_outcomes, load_trades_v3
from analytics.factor_report import _join_outcomes_trades_events  # reuse join logic


STRATEGIES_DEFAULT = ("short_pump", "short_pump_fast0")

FEATURES = [
    # core
    "dist_to_peak_pct",
    "context_score",
    "liq_long_usd_30s",
    "liq_short_usd_30s",
    "funding_rate_abs",
    "oi_change_1m_pct",
    "oi_change_5m_pct",
    "delta_ratio_30s",
    "delta_ratio_1m",
    # volume
    "volume_1m",
    "volume_zscore_20",
    "volume_ratio_1m_20",
    "volume_ratio_5m_20",
    # new velocity/shape
    "price_change_30s_pct",
    "price_change_1m_pct",
    "price_change_3m_pct",
    "accel_30s_vs_3m",
    "green_candles_5",
    "max_candle_body_pct_5",
    "wick_body_ratio_last",
    # time-life
    "time_since_peak_sec",
    "time_since_signal_sec",
    "pump_age_sec",
]


@dataclass
class TrainReport:
    n_rows: int
    n_train: int
    n_valid: int
    auc_valid: float
    features: List[str]
    top10: List[Tuple[str, float]]
    score_buckets: Dict[str, Dict[str, Any]]


def _auc_roc(y_true: np.ndarray, y_score: np.ndarray) -> float:
    """
    ROC-AUC without sklearn (rank-based).
    Returns 0.5 for degenerate cases.
    """
    y_true = np.asarray(y_true).astype(int)
    y_score = np.asarray(y_score).astype(float)
    n_pos = int((y_true == 1).sum())
    n_neg = int((y_true == 0).sum())
    if n_pos == 0 or n_neg == 0:
        return 0.5
    # ranks with average ties
    order = np.argsort(y_score)
    ranks = np.empty_like(order, dtype=float)
    ranks[order] = np.arange(1, len(y_score) + 1, dtype=float)
    # tie handling
    uniq, inv, counts = np.unique(y_score, return_inverse=True, return_counts=True)
    if (counts > 1).any():
        for i, c in enumerate(counts):
            if c <= 1:
                continue
            idx = np.where(inv == i)[0]
            ranks[idx] = ranks[idx].mean()
    sum_ranks_pos = float(ranks[y_true == 1].sum())
    auc = (sum_ranks_pos - n_pos * (n_pos + 1) / 2.0) / (n_pos * n_neg)
    return float(auc)


def _impute_median(df: pd.DataFrame, cols: Sequence[str]) -> Tuple[pd.DataFrame, Dict[str, float]]:
    out = df.copy()
    med: Dict[str, float] = {}
    for c in cols:
        s = pd.to_numeric(out.get(c), errors="coerce")
        m = float(s.median()) if s.dropna().any() else 0.0
        med[c] = m
        out[c] = s.fillna(m)
    return out, med


def _time_split(df: pd.DataFrame, ts_col: str, valid_frac: float) -> Tuple[pd.DataFrame, pd.DataFrame]:
    df = df.copy()
    ts = pd.to_datetime(df[ts_col], errors="coerce", utc=True)
    df["_ts"] = ts
    df = df.sort_values("_ts")
    df = df[df["_ts"].notna()].copy()
    n = len(df)
    split = max(1, int(n * (1.0 - valid_frac)))
    return df.iloc[:split].copy(), df.iloc[split:].copy()


def _score_dist(y: np.ndarray, score: np.ndarray) -> Dict[str, Dict[str, Any]]:
    def _wr(mask: np.ndarray) -> float:
        if mask.sum() == 0:
            return 0.0
        return float(y[mask].mean())

    buckets = {
        "gt_0_6": score > 0.6,
        "lt_0_4": score < 0.4,
    }
    out: Dict[str, Dict[str, Any]] = {}
    for name, m in buckets.items():
        out[name] = {
            "n": int(m.sum()),
            "wr": _wr(m),
            "mean_score": float(score[m].mean()) if m.sum() else 0.0,
        }
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--data-dir", required=True, help="datasets root (contains date=YYYYMMDD/...)")
    ap.add_argument("--days", type=int, default=60)
    ap.add_argument("--mode", type=str, default="live")
    ap.add_argument("--strategies", type=str, default=",".join(STRATEGIES_DEFAULT))
    ap.add_argument("--valid-frac", type=float, default=0.25)
    ap.add_argument("--out-model", type=str, default=str(Path("models") / "lgbm_entry_filter.txt"))
    ap.add_argument("--out-meta", type=str, default=str(Path("models") / "lgbm_entry_filter_meta.json"))
    ap.add_argument("--out-report", type=str, default=str(Path("reports") / "lgbm_entry_filter_report.txt"))
    ap.add_argument(
        "--max-nan-frac",
        type=float,
        default=0.7,
        help="Max allowed NaN fraction per row across selected features (MVP: 0.7).",
    )
    args = ap.parse_args()

    base_dir = Path(args.data_dir)
    strategies = [s.strip() for s in args.strategies.split(",") if s.strip()]

    rows_all: List[pd.DataFrame] = []
    for strat in strategies:
        outcomes = load_outcomes(base_dir=base_dir, strategy=strat, mode=args.mode, days=args.days, include_test=False)
        if isinstance(outcomes, tuple):
            outcomes = outcomes[0]
        trades = load_trades_v3(data_dir=base_dir, strategy=strat, mode=args.mode, days=args.days, include_test=False)
        if isinstance(trades, tuple):
            trades = trades[0]
        events = load_events_v2(data_dir=base_dir, strategy=strat, mode=args.mode, days=args.days, include_test=False, raw=True)
        if isinstance(events, tuple):
            events = events[0]
        joined = _join_outcomes_trades_events(outcomes, trades, events)
        joined["strategy"] = strat
        rows_all.append(joined)

    df = pd.concat(rows_all, ignore_index=True) if rows_all else pd.DataFrame()
    if df.empty:
        raise SystemExit("No data loaded.")

    print(f"[ML_TRAIN] rows_joined={int(len(df))}")

    # Filter TP/SL only
    outc = df.get("outcome", pd.Series([], dtype=str)).astype(str).str.strip()
    mask = outc.isin(["TP_hit", "SL_hit"])
    df = df.loc[mask].copy()
    df["target"] = (outc.loc[mask] == "TP_hit").astype(int).values

    rows_tpsl = int(len(df))
    print(f"[ML_TRAIN] rows_tpsl={rows_tpsl} (TP_hit/SL_hit only)")

    # Keep only numeric features
    feat_present = [f for f in FEATURES if f in df.columns]
    print(f"[ML_TRAIN] features_present ({len(feat_present)}): {feat_present}")
    X_raw = df[feat_present].copy()
    X, med = _impute_median(X_raw, feat_present)

    if not feat_present:
        raise SystemExit("No requested features are present in the joined dataset. Aborting.")

    # Drop rows with too many NaN originally (before impute): allow up to max_nan_frac missing
    nan_frac = X_raw.apply(lambda r: pd.to_numeric(r, errors="coerce").isna().mean(), axis=1)
    df = df.loc[nan_frac <= float(args.max_nan_frac)].copy()
    X = X.loc[df.index].copy()
    y = df["target"].astype(int).values

    print(f"[ML_TRAIN] rows_after_nan_filter={int(len(df))} (max-nan-frac={float(args.max_nan_frac)})")
    if int(len(df)) == 0:
        raise SystemExit(
            "Train dataset is empty after NaN filtering. "
            "Try increasing --max-nan-frac (e.g. 0.7) or check feature availability in datasets."
        )

    # Time split (by outcome_time_utc)
    ts_col = "outcome_time_utc" if "outcome_time_utc" in df.columns else ("wall_time_utc" if "wall_time_utc" in df.columns else None)
    if ts_col is None:
        raise SystemExit("No time column for time split (need outcome_time_utc or wall_time_utc).")
    train_df, valid_df = _time_split(df.assign(**{**{c: X[c] for c in X.columns}}), ts_col=ts_col, valid_frac=float(args.valid_frac))

    n_train = int(len(train_df))
    n_valid = int(len(valid_df))
    print(f"[ML_TRAIN] n_train={n_train} n_valid={n_valid} valid-frac={float(args.valid_frac)}")
    if n_train == 0 or n_valid == 0:
        raise SystemExit(
            f"Empty train or valid set after time split. n_train={n_train} n_valid={n_valid}. "
            "Try increasing --days or adjusting --valid-frac."
        )

    X_train = train_df[feat_present].values
    y_train = train_df["target"].astype(int).values
    X_valid = valid_df[feat_present].values
    y_valid = valid_df["target"].astype(int).values

    dtrain = lgb.Dataset(X_train, label=y_train, feature_name=feat_present, free_raw_data=False)
    dvalid = lgb.Dataset(X_valid, label=y_valid, reference=dtrain, feature_name=feat_present, free_raw_data=False)

    params = {
        "objective": "binary",
        "metric": "auc",
        "learning_rate": 0.05,
        "num_leaves": 31,
        "feature_fraction": 0.9,
        "bagging_fraction": 0.9,
        "bagging_freq": 1,
        "min_data_in_leaf": 25,
        "seed": 42,
        "verbose": -1,
    }
    booster = lgb.train(
        params,
        dtrain,
        num_boost_round=400,
        valid_sets=[dvalid],
        valid_names=["valid"],
        callbacks=[lgb.early_stopping(stopping_rounds=50, verbose=False)],
    )

    pred_valid = booster.predict(X_valid)
    auc_valid = _auc_roc(y_valid, pred_valid)

    imp = booster.feature_importance(importance_type="gain")
    pairs = sorted(zip(feat_present, imp), key=lambda x: float(x[1]), reverse=True)
    top10 = [(k, float(v)) for k, v in pairs[:10]]

    report = TrainReport(
        n_rows=int(len(df)),
        n_train=int(len(train_df)),
        n_valid=int(len(valid_df)),
        auc_valid=float(auc_valid),
        features=list(feat_present),
        top10=top10,
        score_buckets=_score_dist(y_valid, np.asarray(pred_valid)),
    )

    out_model = Path(args.out_model)
    out_meta = Path(args.out_meta)
    out_report = Path(args.out_report)
    out_model.parent.mkdir(parents=True, exist_ok=True)
    out_report.parent.mkdir(parents=True, exist_ok=True)

    booster.save_model(str(out_model))
    meta = {
        "version": 1,
        "trained_at_utc": datetime.now(timezone.utc).isoformat(),
        "strategies": strategies,
        "mode": args.mode,
        "days": int(args.days),
        "features": feat_present,
        "impute_median": med,
        "valid_auc": float(auc_valid),
        "n_rows": int(len(df)),
        "n_train": int(len(train_df)),
        "n_valid": int(len(valid_df)),
    }
    out_meta.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")

    lines: List[str] = []
    lines.append("LGBM ENTRY FILTER v1")
    lines.append(f"trained_at_utc={meta['trained_at_utc']}")
    lines.append(f"strategies={','.join(strategies)} mode={args.mode} days={args.days}")
    lines.append(f"rows={report.n_rows} train={report.n_train} valid={report.n_valid}")
    lines.append(f"AUC_valid={report.auc_valid:.4f}")
    lines.append("")
    lines.append("Top-10 feature importance (gain):")
    for k, v in report.top10:
        lines.append(f"  - {k}: {v:.4g}")
    lines.append("")
    lines.append("Score buckets (valid):")
    for name, st in report.score_buckets.items():
        lines.append(f"  - {name}: n={st['n']} wr={st['wr']:.3f} mean_score={st['mean_score']:.3f}")
    out_report.write_text("\n".join(lines) + "\n", encoding="utf-8")

    print(f"OK: model={out_model} meta={out_meta} report={out_report}")


if __name__ == "__main__":
    main()

