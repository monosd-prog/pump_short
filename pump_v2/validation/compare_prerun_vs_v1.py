#!/usr/bin/env python3
"""Compare pump_v2 pre-run signals with v1 ENTRY_OK from events_v3."""
from __future__ import annotations

import argparse
import json
import sys
import warnings
from pathlib import Path
from typing import Any, Optional

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

PRERUN_PATH = ROOT / "datasets" / "prerun_signals_v2.csv"
EVENTS_ROOT = ROOT / "datasets"

V1_EVENT_COLS = [
    "event_id",
    "symbol",
    "strategy",
    "time_utc",
    "wall_time_utc",
    "stage",
    "dist_to_peak_pct",
    "context_score",
    "funding_rate_abs",
    "entry_ok",
    "payload_json",
]

# Same thresholds as pump_v2/strategies/short_pump.py (autonomous; no import)
_TRADEABLE_DIST_MIN = 3.5
_DEEP_DIST_MIN = 7.5
_DEEP_DIST_MAX = 10.0
_DEEP_LIQ_THRESHOLD = 100.0
_ACTIVE_DIST_MIN = 3.5
_FUNDING_BAND_1 = (0.0005, 0.001)
_FUNDING_BAND_2 = (0.005, 0.01)


def _read_csv_robust(path: Path) -> pd.DataFrame:
    try:
        return pd.read_csv(path, low_memory=False)
    except Exception:
        return pd.read_csv(path, engine="python", on_bad_lines="skip")


def _parse_ts(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, utc=True, errors="coerce")


def _entry_ok_mask(df: pd.DataFrame) -> pd.Series:
    if "entry_ok" not in df.columns:
        return pd.Series(False, index=df.index)
    s = df["entry_ok"]
    return s.isin((1, True, "1", "true", "True", "TRUE")) | (s.astype(str).str.upper() == "TRUE")


def _strategy_mask(df: pd.DataFrame) -> pd.Series:
    if "strategy" in df.columns:
        return df["strategy"].astype(str) == "short_pump"
    if "route_strategy" in df.columns:
        return df["route_strategy"].astype(str) == "short_pump"
    warnings.warn("events_v3: no strategy/route_strategy column; skipping strategy filter")
    return pd.Series(True, index=df.index)


def _to_float(val: Any) -> Optional[float]:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def estimate_v1_profile(
    stage: Any,
    dist: Any,
    ctx: Any,
    funding_abs: Any = None,
    liq_long_usd_30s: Any = None,
) -> Optional[str]:
    """Estimate v1 risk_profile from features (events_v3 does not store risk_profile)."""
    stage_i = _to_float(stage)
    dist_f = _to_float(dist)
    ctx_f = _to_float(ctx)
    if stage_i is None or dist_f is None or ctx_f is None:
        return None
    if int(stage_i) != 4 or dist_f < _TRADEABLE_DIST_MIN:
        return None

    fr = _to_float(funding_abs)
    if fr is not None and (
        (_FUNDING_BAND_1[0] <= fr < _FUNDING_BAND_1[1])
        or (_FUNDING_BAND_2[0] <= fr < _FUNDING_BAND_2[1])
    ):
        return "short_pump_funding_1R"

    liq = _to_float(liq_long_usd_30s)
    if (
        dist_f >= _DEEP_DIST_MIN
        and dist_f < _DEEP_DIST_MAX
        and 0.4 <= ctx_f < 0.6
        and (liq is None or liq < _DEEP_LIQ_THRESHOLD)
    ):
        return "short_pump_deep"

    if 3.5 <= dist_f < 5.0 and 0.4 <= ctx_f < 0.6:
        return "short_pump_mid"

    if dist_f >= _ACTIVE_DIST_MIN:
        return "short_pump_active_1R"

    return None


def session_keys_from_df(df: pd.DataFrame) -> set[tuple[str, str]]:
    """Session = (symbol, date) from ts_utc."""
    if df.empty or "ts_utc" not in df.columns or "symbol" not in df.columns:
        return set()
    dates = pd.to_datetime(df["ts_utc"], utc=True).dt.strftime("%Y-%m-%d")
    return set(zip(df["symbol"].astype(str), dates))


def compute_session_overlap(
    prerun: pd.DataFrame,
    v1_events: pd.DataFrame,
) -> tuple[int, int, int]:
    """Return (n_v2_sessions, n_v1_sessions, n_overlap)."""
    v2_sessions = session_keys_from_df(prerun)
    v1_sessions = session_keys_from_df(v1_events)
    overlap = len(v2_sessions & v1_sessions)
    return len(v2_sessions), len(v1_sessions), overlap


def _extract_risk_profile(row: pd.Series) -> Optional[str]:
    if "risk_profile" in row.index:
        val = row.get("risk_profile")
        if pd.notna(val) and str(val).strip():
            return str(val).strip()
    payload = row.get("payload_json")
    if pd.isna(payload) or not str(payload).strip():
        return None
    try:
        data = json.loads(payload) if isinstance(payload, str) else payload
        if isinstance(data, dict):
            rp = data.get("risk_profile") or data.get("profile")
            if rp is not None and str(rp).strip():
                return str(rp).strip()
    except (json.JSONDecodeError, TypeError):
        pass
    return None


def load_prerun(path: Path, days: Optional[int] = None) -> pd.DataFrame:
    if not path.exists() or path.stat().st_size == 0:
        return pd.DataFrame()
    try:
        df = pd.read_csv(path, parse_dates=["ts_utc"])
    except Exception as exc:
        warnings.warn(f"prerun read failed: {exc}")
        return pd.DataFrame()
    if "ts_utc" not in df.columns:
        warnings.warn("prerun_signals_v2.csv: missing ts_utc column")
        return pd.DataFrame()
    df["ts_utc"] = _parse_ts(df["ts_utc"])
    df = df.dropna(subset=["ts_utc"])
    if days is not None and not df.empty:
        cutoff = df["ts_utc"].max() - pd.Timedelta(days=days)
        df = df[df["ts_utc"] >= cutoff].copy()
    return df


def load_v1_entry_ok(datasets_root: Path, days: Optional[int] = None) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for p in sorted(datasets_root.rglob("events_v3.csv")):
        try:
            df = _read_csv_robust(p)
        except Exception as exc:
            warnings.warn(f"skip {p}: {exc}")
            continue
        if df.empty:
            continue
        use = [c for c in V1_EVENT_COLS if c in df.columns]
        if "symbol" not in use:
            continue
        sub = df[use].copy()
        sub = sub[_strategy_mask(sub) & _entry_ok_mask(sub)]
        if sub.empty:
            continue
        if "time_utc" in sub.columns:
            sub["ts_utc"] = _parse_ts(sub["time_utc"])
        elif "wall_time_utc" in sub.columns:
            sub["ts_utc"] = _parse_ts(sub["wall_time_utc"])
        else:
            warnings.warn(f"events_v3 {p}: no time_utc/wall_time_utc")
            continue
        sub = sub.dropna(subset=["ts_utc"])
        sub["risk_profile"] = sub.apply(_extract_risk_profile, axis=1)
        frames.append(sub)

    if not frames:
        return pd.DataFrame(columns=["symbol", "ts_utc"])

    out = pd.concat(frames, ignore_index=True)
    if "event_id" in out.columns:
        out = out.drop_duplicates(subset=["event_id"], keep="first")
    if days is not None and not out.empty:
        cutoff = out["ts_utc"].max() - pd.Timedelta(days=days)
        out = out[out["ts_utc"] >= cutoff].copy()
    return out


def find_match(
    v2_row: pd.Series,
    v1_events: pd.DataFrame,
    window_minutes: int = 5,
) -> Optional[pd.Series]:
    """Return first v1 ENTRY_OK row within ±window_minutes for same symbol, or None."""
    if v1_events.empty or "symbol" not in v1_events.columns or "ts_utc" not in v1_events.columns:
        return None
    sym = v2_row.get("symbol")
    ts = v2_row.get("ts_utc")
    if pd.isna(sym) or pd.isna(ts):
        return None
    window = pd.Timedelta(minutes=window_minutes)
    candidates = v1_events[
        (v1_events["symbol"] == sym)
        & (v1_events["ts_utc"] >= ts - window)
        & (v1_events["ts_utc"] <= ts + window)
    ]
    if candidates.empty:
        return None
    candidates = candidates.sort_values("ts_utc")
    return candidates.iloc[0]


def compare_prerun_to_v1(
    prerun: pd.DataFrame,
    v1_events: pd.DataFrame,
    window_minutes: int = 5,
) -> tuple[list[dict[str, Any]], list[pd.Series], pd.DataFrame]:
    """Match v2 rows to v1; return (matches, v2_unmatched rows, v1_unmatched df)."""
    if prerun.empty:
        return [], [], v1_events.copy() if not v1_events.empty else pd.DataFrame()

    matches: list[dict[str, Any]] = []
    v2_unmatched: list[pd.Series] = []
    matched_v1_idx: set[Any] = set()

    for _, v2_row in prerun.iterrows():
        best = find_match(v2_row, v1_events, window_minutes=window_minutes)
        if best is None:
            v2_unmatched.append(v2_row)
            continue
        matched_v1_idx.add(best.name)
        v1_ts = best["ts_utc"]
        v2_ts = v2_row["ts_utc"]
        delta_min = (v2_ts - v1_ts).total_seconds() / 60.0
        v2_profile = v2_row.get("risk_profile")
        v1_profile = best.get("risk_profile")
        estimated_v1_profile = estimate_v1_profile(
            best.get("stage"),
            best.get("dist_to_peak_pct"),
            best.get("context_score"),
            funding_abs=best.get("funding_rate_abs"),
        )
        matches.append(
            {
                "symbol": v2_row.get("symbol"),
                "v2_ts": v2_ts,
                "v1_ts": v1_ts,
                "delta_minutes": delta_min,
                "v2_profile": v2_profile,
                "v1_profile": v1_profile,
                "estimated_v1_profile": estimated_v1_profile,
                "v2_stage": v2_row.get("stage"),
                "v2_dist": v2_row.get("dist_to_peak_pct"),
                "v2_ctx": v2_row.get("context_score"),
                "v1_stage": best.get("stage"),
                "v1_dist": best.get("dist_to_peak_pct"),
                "v1_ctx": best.get("context_score"),
                "profile_match": (
                    pd.notna(v2_profile)
                    and pd.notna(v1_profile)
                    and str(v2_profile) == str(v1_profile)
                ),
                "profile_match_estimated": (
                    pd.notna(v2_profile)
                    and estimated_v1_profile is not None
                    and str(v2_profile) == str(estimated_v1_profile)
                ),
            }
        )

    if v1_events.empty:
        v1_unmatched = pd.DataFrame()
    else:
        v1_unmatched = v1_events[~v1_events.index.isin(matched_v1_idx)].copy()
    return matches, v2_unmatched, v1_unmatched


def _period_bounds(prerun: pd.DataFrame, v1_events: pd.DataFrame) -> tuple[str, str]:
    ts_list: list[pd.Timestamp] = []
    if not prerun.empty and "ts_utc" in prerun.columns:
        ts_list.extend([prerun["ts_utc"].min(), prerun["ts_utc"].max()])
    if not v1_events.empty and "ts_utc" in v1_events.columns:
        ts_list.extend([v1_events["ts_utc"].min(), v1_events["ts_utc"].max()])
    if not ts_list:
        return "—", "—"
    return str(min(ts_list)), str(max(ts_list))


def print_report(
    prerun: pd.DataFrame,
    v1_events: pd.DataFrame,
    matches: list[dict[str, Any]],
    v2_unmatched: list[pd.Series],
    v1_unmatched: pd.DataFrame,
    window_minutes: int = 5,
) -> None:
    n_v2 = len(prerun)
    n_v1 = len(v1_events)
    n_matched = len(matches)
    n_v2_unmatched = len(v2_unmatched)
    n_v1_unmatched = len(v1_unmatched)

    period_start, period_end = _period_bounds(prerun, v1_events)
    print("=== PRERUN COMPARISON REPORT ===")
    print(f"Period: {period_start} — {period_end}")
    print(f"V2 signals:    {n_v2}")
    print(f"V1 ENTRY_OK:   {n_v1}")
    print()

    print(f"=== MATCHED (v2 ↔ v1 в окне ±{window_minutes}m): {n_matched} ===")
    if n_matched:
        n_profile_match = sum(1 for m in matches if m.get("profile_match"))
        n_profile_est = sum(1 for m in matches if m.get("profile_match_estimated"))
        avg_delta = sum(abs(m["delta_minutes"]) for m in matches) / n_matched
        print(f"Profile match: {n_profile_match}/{n_matched}")
        print(f"Profile match (estimated): {n_profile_est}/{n_matched}")
        print("  (v1 risk_profile не пишется в events_v3; оценка по stage/dist/ctx)")
        print(f"Avg delta: {avg_delta:.1f} min")
    else:
        print("Profile match: 0/0")
        print("Profile match (estimated): 0/0")
        print("  (v1 risk_profile не пишется в events_v3; оценка по stage/dist/ctx)")
        print("Avg delta: 0.0 min")
    print()

    n_v2_sess, n_v1_sess, n_overlap = compute_session_overlap(prerun, v1_events)
    pct_sess = (100.0 * n_overlap / n_v2_sess) if n_v2_sess else 0.0
    print("=== SESSION-LEVEL MATCH (deduplicated) ===")
    print(f"V2 sessions (unique symbol+date): {n_v2_sess}")
    print(f"V1 ENTRY_OK sessions:             {n_v1_sess}")
    print(f"Overlap (same symbol+date):        {n_overlap} ({pct_sess:.0f}%)")
    print()

    print(f"=== V2 UNMATCHED (v2 сигнал без v1 ENTRY_OK): {n_v2_unmatched} ===")
    print("(v2 срабатывает там где v1 не дал ENTRY_OK — расхождение условий)")
    print()

    print(f"=== V1 UNMATCHED (v1 ENTRY_OK без v2 сигнала): {n_v1_unmatched} ===")
    print("(v1 срабатывает там где v2 не срабатывает — пропуски v2)")
    if n_v1_unmatched:
        print("Top V1 unmatched (первые 5):")
        top = v1_unmatched.sort_values("ts_utc").head(5)
        for _, row in top.iterrows():
            print(
                f"  {row.get('symbol')} | {row.get('ts_utc')} | "
                f"stage={row.get('stage')} dist={row.get('dist_to_peak_pct')} "
                f"ctx={row.get('context_score')}"
            )
    print()

    print("=== SUMMARY ===")
    pct_v2 = (100.0 * n_matched / n_v2) if n_v2 else 0.0
    pct_v1 = (100.0 * n_matched / n_v1) if n_v1 else 0.0
    print(f"Match rate v2→v1: {n_matched}/{n_v2} ({pct_v2:.0f}%)")
    print(f"Match rate v1→v2: {n_matched}/{n_v1} ({pct_v1:.0f}%)")


def main() -> int:
    parser = argparse.ArgumentParser(description="Compare v2 prerun signals vs v1 ENTRY_OK")
    parser.add_argument("--days", type=int, default=None, help="Анализировать только последние N дней")
    parser.add_argument("--window", type=int, default=5, help="Окно матчинга в минутах (default: 5)")
    args = parser.parse_args()

    if not PRERUN_PATH.exists() or PRERUN_PATH.stat().st_size == 0:
        print("prerun_signals_v2.csv пуст или не существует. Нечего сравнивать.")
        return 0

    prerun = load_prerun(PRERUN_PATH, days=args.days)
    if prerun.empty:
        print("prerun_signals_v2.csv пуст или не существует. Нечего сравнивать.")
        return 0

    v1_events = load_v1_entry_ok(EVENTS_ROOT, days=args.days)
    matches, v2_unmatched, v1_unmatched = compare_prerun_to_v1(
        prerun, v1_events, window_minutes=args.window
    )
    print_report(
        prerun, v1_events, matches, v2_unmatched, v1_unmatched, window_minutes=args.window
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
