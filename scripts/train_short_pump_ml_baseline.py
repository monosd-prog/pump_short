#!/usr/bin/env python3
"""
Baseline ML pipeline for SHORT_PUMP (analysis-only, offline).

SAFE FEATURES ONLY (pre-entry, no target leakage):
  - context_score
  - cvd_delta_ratio_1m
  - cvd_delta_ratio_30s
  - dist_to_peak_pct
  - funding_rate_abs
  - liq_long_usd_30s
  - liq_short_usd_30s
  - oi_change_fast_pct
  - stage

FORBIDDEN / LEAKAGE FEATURES (MUST NOT BE USED):
  - outcome / outcome_label / outcome_norm
  - pnl_pct / pnl_r / pnl_usd
  - mae_pct / mfe_pct / mae_r / mfe_r
  - exit_price / tp_price / sl_price
  - hold_seconds
  - any post-entry or close-derived columns

Targets:
  TP_hit   -> 1
  SL_hit   -> 0
  TIMEOUT  -> 0

This script:
  - joins short_pump live outcomes_v3 with trades_v3 and events_v3
  - builds a clean core dataset
  - trains LogisticRegression and RandomForestClassifier baselines (if sklearn available)
  - evaluates metrics and simple EV uplift in top-k% buckets by model score
  - writes a human-readable report and test predictions CSV to reports/

RUNTIME / LIVE / PAPER LOGIC IS NOT MODIFIED.
"""

from __future__ import annotations

import argparse
import os
from dataclasses import dataclass
from pathlib import Path
from typing import List, Tuple

import numpy as np
import pandas as pd

try:
  # sklearn is optional; script degrades gracefully if missing.
  from sklearn.linear_model import LogisticRegression
  from sklearn.ensemble import RandomForestClassifier
  from sklearn.metrics import (
      roc_auc_score,
      average_precision_score,
      accuracy_score,
      precision_score,
      recall_score,
      f1_score,
      confusion_matrix,
  )
  SKLEARN_AVAILABLE = True
except Exception:
  SKLEARN_AVAILABLE = False


STRATEGY = "short_pump"
MODE = "live"
CORE_OUTCOMES = ("TP_hit", "SL_hit", "TIMEOUT")

SAFE_FEATURES = [
    "context_score",
    "cvd_delta_ratio_1m",
    "cvd_delta_ratio_30s",
    "dist_to_peak_pct",
    "funding_rate_abs",
    "liq_long_usd_30s",
    "liq_short_usd_30s",
    "oi_change_fast_pct",
    "stage",
]


@dataclass
class DatasetInfo:
  total_core: int
  matched_full: int
  used_features: List[str]
  dropped_features: List[str]


def _find_dataset_root(data_dir: Path) -> Path:
  """Resolve datasets root (data_dir or data_dir/datasets)."""
  data_dir = data_dir.resolve()
  if any((data_dir / d).is_dir() for d in ["date=20200101"]):
    return data_dir
  sub = data_dir / "datasets"
  if sub.is_dir():
    return sub
  return data_dir


def _collect_paths(root: Path) -> Tuple[List[Path], List[Path], List[Path]]:
  events_paths: List[Path] = []
  trades_paths: List[Path] = []
  outcomes_paths: List[Path] = []
  for date_dir in sorted(root.glob("date=*")):
    if not date_dir.is_dir():
      continue
    base = date_dir / f"strategy={STRATEGY}" / f"mode={MODE}"
    e = base / "events_v3.csv"
    t = base / "trades_v3.csv"
    o = base / "outcomes_v3.csv"
    if e.is_file():
      events_paths.append(e)
    if t.is_file():
      trades_paths.append(t)
    if o.is_file():
      outcomes_paths.append(o)
  return events_paths, trades_paths, outcomes_paths


def _read_concat(paths: List[Path]) -> pd.DataFrame:
  if not paths:
    return pd.DataFrame()
  frames = []
  for p in paths:
    try:
      df = pd.read_csv(p)
      df["__source_file"] = str(p)
      frames.append(df)
    except Exception:
      continue
  if not frames:
    return pd.DataFrame()
  return pd.concat(frames, ignore_index=True)


def _normalize_outcome(o: pd.Series) -> pd.Series:
  return o.astype(str).str.strip()


def _build_core_dataset(
  events: pd.DataFrame, trades: pd.DataFrame, outcomes: pd.DataFrame
) -> Tuple[pd.DataFrame, DatasetInfo]:
  # Filter core outcomes.
  if outcomes.empty:
    core = pd.DataFrame()
  else:
    out_norm = _normalize_outcome(outcomes.get("outcome", pd.Series([])))
    core_mask = out_norm.isin(CORE_OUTCOMES)
    core = outcomes.loc[core_mask].copy()
    core["outcome_norm"] = out_norm[core_mask]

  total_core = len(core)

  if total_core == 0:
    return pd.DataFrame(), DatasetInfo(total_core=0, matched_full=0, used_features=[], dropped_features=[])

  # Join with trades on trade_id/symbol/strategy when possible.
  joined = core
  if not trades.empty and "trade_id" in core.columns and "trade_id" in trades.columns:
    joined = joined.merge(
      trades,
      how="left",
      on=["trade_id", "symbol", "strategy"],
      suffixes=("", "_trade"),
    )

  # Join with events on event_id/symbol/strategy when possible.
  if not events.empty and "event_id" in joined.columns and "event_id" in events.columns:
    joined = joined.merge(
      events,
      how="left",
      on=["event_id", "symbol", "strategy"],
      suffixes=("", "_event"),
    )

  # Consider full join when both entry_time_utc and time_utc are available.
  full_mask = pd.Series(True, index=joined.index)
  if "entry_time_utc" in joined.columns:
    full_mask &= joined["entry_time_utc"].notna()
  if "time_utc" in joined.columns:
    full_mask &= joined["time_utc"].notna()
  df = joined.loc[full_mask].copy()

  matched_full = len(df)

  # Target mapping: TP_hit->1, SL_hit/TIMEOUT->0
  target = df["outcome_norm"].map({"TP_hit": 1, "SL_hit": 0, "TIMEOUT": 0})
  df["target"] = target
  df = df[target.notna()].copy()

  # Prepare feature matrix: use only available SAFE_FEATURES.
  available = [f for f in SAFE_FEATURES if f in df.columns]
  dropped = [f for f in SAFE_FEATURES if f not in df.columns]

  X = df[available].copy()
  # Ensure numeric types and median imputation.
  for col in available:
    X[col] = pd.to_numeric(X[col], errors="coerce")
    median = float(X[col].median()) if not X[col].dropna().empty else 0.0
    X[col] = X[col].fillna(median)
  df_features = X
  df["target"] = df["target"].astype(int)

  info = DatasetInfo(
    total_core=total_core,
    matched_full=matched_full,
    used_features=available,
    dropped_features=dropped,
  )
  # Attach features back for convenience.
  for c in df_features.columns:
    df[c] = df_features[c]
  return df, info


def _time_split(df: pd.DataFrame, train_frac: float = 0.8) -> Tuple[pd.DataFrame, pd.DataFrame]:
  if df.empty:
    return df, df
  if "outcome_time_utc" not in df.columns:
    return df, pd.DataFrame()
  ts = pd.to_datetime(df["outcome_time_utc"], errors="coerce", utc=True)
  df = df.copy()
  df["_ts"] = ts
  df = df.sort_values("_ts")
  n = len(df)
  if n < 10:
    return df, pd.DataFrame()
  split_idx = int(n * train_frac)
  train = df.iloc[:split_idx].copy()
  test = df.iloc[split_idx:].copy()
  return train, test


def _compute_bucket_metrics(y_true: np.ndarray, y_score: np.ndarray, buckets: List[float]) -> List[str]:
  lines: List[str] = []
  n = len(y_true)
  order = np.argsort(-y_score)  # descending
  for frac in buckets:
    k = max(1, int(n * frac))
    idx = order[:k]
    y_top = y_true[idx]
    cov = k / n if n else 0.0
    winrate = y_top.mean() if len(y_top) > 0 else 0.0
    # EV proxy: TP=+1.2R, SL/TIMEOUT=0?1? Here TIMEOUT treated as 0R, SL as -1R.
    # target already 1 for TP, 0 for SL/TIMEOUT; we need original labels.
    # For simplicity assume:
    # - y_true==1 -> +1.2R
    # - y_true==0 -> -1.0R * p_sl, with TIMEOUT approximated as 0R (so use separate series if needed).
    # Here we approximate EV as 1.2*P(TP) - 1.0*(1-P(TP)).
    ev = 1.2 * winrate - 1.0 * (1.0 - winrate)
    lines.append(f"top-{int(frac*100)}%: coverage={cov:.2f} winrate={winrate:.3f} EV_proxy={ev:.3f}")
  return lines


def _compute_ev_proxy(y: np.ndarray) -> float:
  """
  EV proxy: TP=+1.2R, SL=-1.0R, TIMEOUT=0R. Here we only know target (1 for TP, 0 for SL/TIMEOUT),
  so we approximate EV as 1.2*P(TP) - 1.0*(1-P(TP)) with TIMEOUT blended into negatives.
  """
  if len(y) == 0:
    return 0.0
  p_tp = float(y.mean())
  return 1.2 * p_tp - 1.0 * (1.0 - p_tp)


def main() -> None:
  ap = argparse.ArgumentParser(description="Train baseline ML models for SHORT_PUMP (analysis-only)")
  ap.add_argument(
    "--data-dir",
    default="/root/pump_short/datasets",
    help="Datasets root (default: /root/pump_short/datasets)",
  )
  args = ap.parse_args()

  root = _find_dataset_root(Path(args.data_dir))
  print(f"DATASETS_ROOT: {root}")

  events_paths, trades_paths, outcomes_paths = _collect_paths(root)
  print(f"FILES: events={len(events_paths)} trades={len(trades_paths)} outcomes={len(outcomes_paths)}")
  if not outcomes_paths:
    print("No outcomes_v3.csv found for short_pump/mode=live. Nothing to train.")
    return

  events = _read_concat(events_paths)
  trades = _read_concat(trades_paths)
  outcomes = _read_concat(outcomes_paths)

  df_core, info = _build_core_dataset(events, trades, outcomes)
  print("\n--- DATASET SUMMARY ---")
  print(f"total_core_outcomes: {info.total_core}")
  print(f"matched_full (outcome+trade+event+target): {info.matched_full}")
  print(f"used_features: {info.used_features}")
  print(f"dropped_missing_features: {info.dropped_features}")

  if df_core.empty or info.matched_full == 0 or not info.used_features:
    print("Insufficient data after join/feature selection. Abort training.")
    return

  # Train/test time split.
  train, test = _time_split(df_core)
  if test.empty or train.empty:
    print("Not enough samples for time-based train/test split. Abort training.")
    return

  y_train = train["target"].to_numpy()
  y_test = test["target"].to_numpy()
  X_train = train[info.used_features].to_numpy()
  X_test = test[info.used_features].to_numpy()

  print("\n--- CLASS BALANCE ---")
  print(f"train: n={len(y_train)} pos={int(y_train.sum())} neg={len(y_train)-int(y_train.sum())} pos_frac={y_train.mean():.3f}")
  print(f"test:  n={len(y_test)} pos={int(y_test.sum())} neg={len(y_test)-int(y_test.sum())} pos_frac={y_test.mean():.3f}")

  reports_dir = Path("reports")
  reports_dir.mkdir(parents=True, exist_ok=True)
  report_lines: List[str] = []

  if not SKLEARN_AVAILABLE:
    msg = "sklearn is not available in this environment; skipping model training."
    print(msg)
    report_lines.append(msg)
    (reports_dir / "short_pump_ml_baseline_report.txt").write_text("\n".join(report_lines), encoding="utf-8")
    return

  # Baseline 1: Logistic Regression.
  print("\n--- MODEL 1: LogisticRegression ---")
  logreg = LogisticRegression(max_iter=200, n_jobs=None)
  logreg.fit(X_train, y_train)
  scores_lr = logreg.predict_proba(X_test)[:, 1]
  preds_lr = (scores_lr >= 0.5).astype(int)

  roc_lr = roc_auc_score(y_test, scores_lr)
  pr_lr = average_precision_score(y_test, scores_lr)
  acc_lr = accuracy_score(y_test, preds_lr)
  prec_lr = precision_score(y_test, preds_lr, zero_division=0)
  rec_lr = recall_score(y_test, preds_lr, zero_division=0)
  f1_lr = f1_score(y_test, preds_lr, zero_division=0)
  cm_lr = confusion_matrix(y_test, preds_lr)

  print(f"ROC AUC: {roc_lr:.3f}")
  print(f"PR  AUC: {pr_lr:.3f}")
  print(f"accuracy: {acc_lr:.3f} precision: {prec_lr:.3f} recall: {rec_lr:.3f} f1: {f1_lr:.3f}")
  print(f"confusion_matrix:\n{cm_lr}")

  print("\nWINRATE / EV PROXY BUCKETS (LogReg):")
  bucket_lines_lr = _compute_bucket_metrics(y_test, scores_lr, [0.3, 0.2, 0.1])
  for line in bucket_lines_lr:
    print("  " + line)
  ev_raw_lr = _compute_ev_proxy(y_test)
  print(f"EV_proxy raw (test, LogReg target approximation): {ev_raw_lr:.3f}")

  # Coefficients.
  coef = logreg.coef_[0]
  coef_pairs = sorted(zip(info.used_features, coef), key=lambda x: abs(x[1]), reverse=True)
  print("\nLogReg coefficients (sorted by |coef|):")
  for name, val in coef_pairs:
    print(f"  {name}: {val:+.4f}")

  # Baseline 2: RandomForest.
  print("\n--- MODEL 2: RandomForestClassifier ---")
  rf = RandomForestClassifier(
    n_estimators=200,
    random_state=42,
    n_jobs=-1,
    max_depth=None,
  )
  rf.fit(X_train, y_train)
  scores_rf = rf.predict_proba(X_test)[:, 1]
  preds_rf = (scores_rf >= 0.5).astype(int)

  roc_rf = roc_auc_score(y_test, scores_rf)
  pr_rf = average_precision_score(y_test, scores_rf)
  acc_rf = accuracy_score(y_test, preds_rf)
  prec_rf = precision_score(y_test, preds_rf, zero_division=0)
  rec_rf = recall_score(y_test, preds_rf, zero_division=0)
  f1_rf = f1_score(y_test, preds_rf, zero_division=0)
  cm_rf = confusion_matrix(y_test, preds_rf)

  print(f"ROC AUC: {roc_rf:.3f}")
  print(f"PR  AUC: {pr_rf:.3f}")
  print(f"accuracy: {acc_rf:.3f} precision: {prec_rf:.3f} recall: {rec_rf:.3f} f1: {f1_rf:.3f}")
  print(f"confusion_matrix:\n{cm_rf}")

  print("\nWINRATE / EV PROXY BUCKETS (RF):")
  bucket_lines_rf = _compute_bucket_metrics(y_test, scores_rf, [0.3, 0.2, 0.1])
  for line in bucket_lines_rf:
    print("  " + line)
  ev_raw_rf = _compute_ev_proxy(y_test)
  print(f"EV_proxy raw (test, RF target approximation): {ev_raw_rf:.3f}")

  # Feature importance.
  importances = rf.feature_importances_
  imp_pairs = sorted(zip(info.used_features, importances), key=lambda x: x[1], reverse=True)
  print("\nRandomForest feature importances:")
  for name, val in imp_pairs:
    print(f"  {name}: {val:.4f}")

  # Build report text.
  report_lines.extend([
    f"DATASETS_ROOT: {root}",
    f"FILES: events={len(events_paths)} trades={len(trades_paths)} outcomes={len(outcomes_paths)}",
    "",
    "--- DATASET SUMMARY ---",
    f"total_core_outcomes: {info.total_core}",
    f"matched_full: {info.matched_full}",
    f"used_features: {info.used_features}",
    f"dropped_missing_features: {info.dropped_features}",
    "",
    "--- MODEL 1: LogisticRegression ---",
    f"ROC AUC: {roc_lr:.3f}",
    f"PR  AUC: {pr_lr:.3f}",
    f"accuracy: {acc_lr:.3f}",
    f"precision: {prec_lr:.3f}",
    f"recall: {rec_lr:.3f}",
    f"f1: {f1_lr:.3f}",
    f"confusion_matrix:\n{cm_lr}",
    "WINRATE / EV PROXY BUCKETS (LogReg):",
  ])
  report_lines.extend(bucket_lines_lr)
  report_lines.append(f"EV_proxy raw (test, LogReg): {ev_raw_lr:.3f}")
  report_lines.append("LogReg coefficients:")
  report_lines.extend([f"  {name}: {val:+.4f}" for name, val in coef_pairs])

  report_lines.extend([
    "",
    "--- MODEL 2: RandomForest ---",
    f"ROC AUC: {roc_rf:.3f}",
    f"PR  AUC: {pr_rf:.3f}",
    f"accuracy: {acc_rf:.3f}",
    f"precision: {prec_rf:.3f}",
    f"recall: {rec_rf:.3f}",
    f"f1: {f1_rf:.3f}",
    f"confusion_matrix:\n{cm_rf}",
    "WINRATE / EV PROXY BUCKETS (RF):",
  ])
  report_lines.extend(bucket_lines_rf)
  report_lines.append(f"EV_proxy raw (test, RF): {ev_raw_rf:.3f}")
  report_lines.append("RandomForest feature importances:")
  report_lines.extend([f"  {name}: {val:.4f}" for name, val in imp_pairs])

  # Simple interpretation / threshold suggestion.
  report_lines.append("")
  report_lines.append("--- INTERPRETATION ---")
  report_lines.append("This is an offline baseline only; no runtime integration is done.")
  report_lines.append(
    "If winrate/EV in top-20% or top-10% buckets is meaningfully higher than raw, "
    "a soft ML gate (e.g., require score above top-30% threshold) may be viable for experimentation."
  )

  # Save report.
  (reports_dir / "short_pump_ml_baseline_report.txt").write_text("\n".join(report_lines), encoding="utf-8")
  print(f"\nReport written to: {reports_dir / 'short_pump_ml_baseline_report.txt'}")

  # Save test predictions CSV.
  out_df = pd.DataFrame({
    "outcome_time_utc": test.get("outcome_time_utc"),
    "run_id": test.get("run_id"),
    "event_id": test.get("event_id"),
    "symbol": test.get("symbol"),
    "outcome": test.get("outcome_norm"),
    "target": y_test,
    "model_score_logreg": scores_lr,
    "model_score_rf": scores_rf,
  })
  # Sort predictions by time for easier analysis.
  if "outcome_time_utc" in out_df.columns:
    out_df["outcome_time_utc"] = pd.to_datetime(out_df["outcome_time_utc"], errors="coerce", utc=True)
    out_df = out_df.sort_values("outcome_time_utc")
  out_path = reports_dir / "short_pump_ml_test_predictions.csv"
  out_df.to_csv(out_path, index=False)
  print(f"Test predictions written to: {out_path}")


if __name__ == "__main__":
  main()

