#!/usr/bin/env python3
"""Parity validation: ShortPumpStrategy vs v1 labeled short_pump_mid trades."""
from __future__ import annotations

import sys
import warnings
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pump_v2.core.market_context import MarketContext
from pump_v2.indicators.context_score_5m import ContextScore5m
from pump_v2.indicators.dbg5_builder import Dbg5Bundle
from pump_v2.strategies.short_pump import ShortPumpStrategy

CLOSES_PATH = ROOT / "datasets" / "trading_closes.csv"
EVENTS_ROOT = ROOT / "datasets"

EVENT_COLS = ["event_id", "stage", "dist_to_peak_pct", "context_score", "symbol"]

# Negatives: must NOT classify as short_pump_mid (None from gate or another profile).
NEG_CASES = [
    {"label": "dist_below_tradeable_gate", "stage": 4, "dist": 2.9, "ctx": 0.50},
    {"label": "dist_above_mid_range_goes_to_active", "stage": 4, "dist": 5.5, "ctx": 0.50},
    {"label": "ctx_below_mid_goes_to_active", "stage": 4, "dist": 4.2, "ctx": 0.35},
    {"label": "ctx_above_mid_goes_to_active", "stage": 4, "dist": 4.2, "ctx": 0.65},
    {"label": "stage_below_tradeable_gate", "stage": 1, "dist": 4.2, "ctx": 0.50},
    {"label": "dist_mid_upper_boundary_goes_to_active", "stage": 4, "dist": 5.0, "ctx": 0.50},
    {"label": "ctx_mid_upper_boundary_goes_to_active", "stage": 4, "dist": 4.2, "ctx": 0.60},
]


def _read_csv_robust(path: Path) -> pd.DataFrame:
    try:
        return pd.read_csv(path, low_memory=False)
    except Exception:
        return pd.read_csv(path, engine="python", on_bad_lines="skip")


def _load_all_events_v3(datasets_root: Path) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for p in sorted(datasets_root.rglob("events_v3.csv")):
        df = _read_csv_robust(p)
        if df.empty or "event_id" not in df.columns:
            continue
        use = [c for c in EVENT_COLS if c in df.columns]
        if "event_id" not in use:
            continue
        sub = df[use].copy()
        sub["event_id"] = sub["event_id"].astype(str)
        frames.append(sub)
    if not frames:
        return pd.DataFrame(columns=EVENT_COLS)
    out = pd.concat(frames, ignore_index=True)
    out = out.drop_duplicates(subset=["event_id"], keep="first")
    return out


def _load_positives() -> pd.DataFrame:
    if not CLOSES_PATH.exists():
        raise FileNotFoundError(f"missing {CLOSES_PATH}")

    mid_closes = pd.read_csv(CLOSES_PATH, low_memory=False)
    mid_closes = mid_closes[mid_closes["risk_profile"].astype(str) == "short_pump_mid"].copy()
    if mid_closes.empty:
        warnings.warn("no short_pump_mid rows in trading_closes.csv")
        return pd.DataFrame(columns=EVENT_COLS)

    events = _load_all_events_v3(EVENTS_ROOT)
    if events.empty:
        warnings.warn("no events_v3 rows loaded")
        return pd.DataFrame(columns=EVENT_COLS)

    mid_closes["event_id"] = mid_closes["event_id"].astype(str)
    merged = mid_closes.merge(events, on="event_id", how="left", suffixes=("_close", ""))

    missing = merged["stage"].isna()
    for _, row in merged.loc[missing].iterrows():
        warnings.warn(f"events_v3 miss event_id={row['event_id']} symbol={row.get('symbol_close', row.get('symbol'))}")

    positives = merged.loc[~missing].copy()
    if "symbol_close" in positives.columns:
        positives["symbol"] = positives["symbol_close"].fillna(positives.get("symbol"))
    elif "symbol" not in positives.columns:
        positives["symbol"] = "UNKNOWN"

    for col in ("stage", "dist_to_peak_pct", "context_score"):
        positives[col] = pd.to_numeric(positives[col], errors="coerce")

    positives = positives.dropna(subset=["stage", "dist_to_peak_pct", "context_score"])
    return positives[["event_id", "symbol", "stage", "dist_to_peak_pct", "context_score"]].reset_index(drop=True)


def _build_ctx(row: pd.Series) -> MarketContext:
    bundle = Dbg5Bundle(
        stage=int(row["stage"]),
        dist_to_peak_pct=float(row["dist_to_peak_pct"]),
        oi_change_5m_pct=0.0,
        oi_divergence_5m=False,
        vol_z=0.0,
        atr_14_5m_pct=0.0,
    )
    ctx_score = ContextScore5m(score=float(row["context_score"]), parts={})
    ctx = MarketContext(
        symbol=str(row["symbol"]),
        ts_utc=datetime.now(timezone.utc),
        dbg5=bundle,
    )
    ctx.candles["5m"] = [{"close": 1.0}]
    ctx.indicators["context_score_5m"] = ctx_score
    return ctx


def _build_ctx_from_neg(case: dict) -> MarketContext:
    row = pd.Series(
        {
            "symbol": "PARITY_NEG",
            "stage": case["stage"],
            "dist_to_peak_pct": case["dist"],
            "context_score": case["ctx"],
        }
    )
    return _build_ctx(row)


def _run_positives(strategy: ShortPumpStrategy, positives: pd.DataFrame) -> list[dict]:
    results: list[dict] = []
    for _, row in positives.iterrows():
        ctx = _build_ctx(row)
        sig = strategy.check_signal(ctx)
        results.append(
            {
                "event_id": row["event_id"],
                "stage": row["stage"],
                "dist": row["dist_to_peak_pct"],
                "ctx_score": row["context_score"],
                "expected": "Signal",
                "got": "Signal" if sig is not None else "None",
                "ok": sig is not None,
            }
        )
    return results


def _run_negatives(strategy: ShortPumpStrategy) -> list[dict]:
    results: list[dict] = []
    for case in NEG_CASES:
        ctx = _build_ctx_from_neg(case)
        sig = strategy.check_signal(ctx)
        risk_profile = sig.metadata.get("risk_profile") if sig is not None else None
        ok = sig is None or risk_profile != "short_pump_mid"
        if sig is None:
            got = "None"
        else:
            got = f"Signal:{risk_profile}"
        results.append(
            {
                "label": case["label"],
                "stage": case["stage"],
                "dist": case["dist"],
                "ctx_score": case["ctx"],
                "expected": "not short_pump_mid",
                "got": got,
                "ok": ok,
                "risk_profile": risk_profile,
            }
        )
    return results


def main() -> int:
    print("ShortPumpStrategy parity — short_pump_mid")
    print(f"closes: {CLOSES_PATH}")
    print(f"events: {EVENTS_ROOT}/**/events_v3.csv\n")

    positives = _load_positives()
    labeled_mid = int((pd.read_csv(CLOSES_PATH, low_memory=False)["risk_profile"].astype(str) == "short_pump_mid").sum())
    print(f"labeled short_pump_mid in trading_closes: {labeled_mid}")
    print(f"positives with events_v3 join: {len(positives)}\n")

    strategy = ShortPumpStrategy(params={}, risk={})
    pos_results = _run_positives(strategy, positives)
    neg_results = _run_negatives(strategy)

    total_pos = len(pos_results)
    passed_pos = sum(r["ok"] for r in pos_results)
    failed_pos = [r for r in pos_results if not r["ok"]]

    print(f"=== POSITIVES: {passed_pos}/{total_pos} ===")
    if failed_pos:
        print("FAILED:")
        for r in failed_pos:
            print(
                f"  {r['event_id']}: stage={r['stage']} dist={r['dist']:.4f} ctx={r['ctx_score']}"
            )

    total_neg = len(neg_results)
    passed_neg = sum(r["ok"] for r in neg_results)
    failed_neg = [r for r in neg_results if not r["ok"]]

    print(f"\n=== NEGATIVES: {passed_neg}/{total_neg} ===")
    if failed_neg:
        print("FAILED:")
        for r in failed_neg:
            print(
                f"  {r['label']}: stage={r['stage']} dist={r['dist']} ctx={r['ctx_score']} "
                f"got={r['got']} expected={r['expected']}"
            )

    if failed_pos or failed_neg:
        print("\n✗ PARITY CHECKS FAILED")
        return 1

    print("\n✓ ALL PARITY CHECKS PASSED")
    return 0


if __name__ == "__main__":
    sys.exit(main())
