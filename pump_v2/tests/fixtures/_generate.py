#!/usr/bin/env python3
"""
Regenerate golden fixtures under pump_v2/tests/fixtures/symbols/.

Uses v1 Bybit loaders (short_pump.bybit_api) and v1 indicator helpers only.
Does NOT modify v1 modules.

Pitfalls (read before re-running):
- get_recent_trades() returns only the latest ``limit`` trades (no historical range).
  Trades in a fixed past window may be empty or incomplete vs live exchange history.
- get_open_interest() returns the latest ``limit`` OI points (5m), not a time-range API;
  we filter rows to [window_start, window_end] after fetch.
- datasets/liquidations.csv may not cover early windows (collector start time); the
  filtered fixture file can be empty even when the exchange had liquidations.
"""
from __future__ import annotations

import inspect
import json
import sys
import time
from dataclasses import asdict
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Callable, Dict, List, Optional, Tuple

import pandas as pd

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from common import market_features as mf  # noqa: E402
from short_pump import features as spf  # noqa: E402
from short_pump.bybit_api import (  # noqa: E402
    get_funding_rate,
    get_klines_1m_range,
    get_klines_5m_range,
    get_open_interest,
    get_recent_trades,
)
from short_pump.context5m import (  # noqa: E402
    StructureState,
    build_dbg5,
    compute_context_score_5m,
    update_structure,
)

CATEGORY = "linear"
LIQ_CSV = ROOT / "datasets" / "liquidations.csv"
FIX_ROOT = Path(__file__).resolve().parent
SYMS_ROOT = FIX_ROOT / "symbols"
SLEEP_SEC = 0.55


def _sleep() -> None:
    time.sleep(SLEEP_SEC)


def v1_ref(fn: Callable[..., Any]) -> str:
    try:
        src = Path(inspect.getsourcefile(fn) or "").resolve()
        try:
            rel = src.relative_to(ROOT)
        except ValueError:
            rel = src
        line = inspect.getsourcelines(fn)[1]
        return f"{rel.as_posix()}:{fn.__qualname__}:{line}"
    except Exception:
        return f"unknown:{getattr(fn, '__qualname__', str(fn))}:0"


def round6(x: Any) -> Any:
    if x is None:
        return None
    if isinstance(x, bool):
        return x
    if isinstance(x, (int,)):
        return x
    if isinstance(x, float):
        if pd.isna(x):
            return None
        return round(float(x), 6)
    if isinstance(x, dict):
        return {k: round6(v) for k, v in x.items()}
    if isinstance(x, (list, tuple)):
        return [round6(v) for v in x]
    return x


def pack(
    value: Any,
    fn: Callable[..., Any],
    *,
    params: Optional[Dict[str, Any]] = None,
    error: Optional[BaseException] = None,
) -> Dict[str, Any]:
    out: Dict[str, Any] = {
        "v1_ref": v1_ref(fn),
        "params": params or {},
    }
    if error is not None:
        out["value"] = None
        out["raises"] = f"{type(error).__name__}: {error}"
        out["v1_behavior"] = str(error)
        return out
    out["value"] = round6(value)
    return out


def ts_ms_bounds(ws: pd.Timestamp, we: pd.Timestamp) -> Tuple[int, int]:
    wsu = _as_utc(ws)
    weu = _as_utc(we)
    start_ms = int(wsu.timestamp() * 1000)
    end_ms = int(weu.timestamp() * 1000)
    return start_ms, end_ms


def _as_utc(ts: pd.Timestamp) -> pd.Timestamp:
    t = pd.Timestamp(ts)
    if t.tzinfo is None:
        return t.tz_localize("UTC")
    return t.tz_convert("UTC")


def filter_window(df: pd.DataFrame, ws: pd.Timestamp, we: pd.Timestamp, col: str = "ts") -> pd.DataFrame:
    if df is None or df.empty:
        return df
    x = df.copy()
    if col not in x.columns:
        return pd.DataFrame(columns=x.columns)
    t = pd.to_datetime(x[col], utc=True)
    wsu = _as_utc(ws)
    weu = _as_utc(we)
    mask = (t >= wsu) & (t <= weu)
    return x.loc[mask].reset_index(drop=True)


def klines_to_schema(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=["ts_utc", "open", "high", "low", "close", "volume"])
    out = df.copy()
    out["ts_utc"] = pd.to_datetime(out["ts"], utc=True)
    cols = ["ts_utc", "open", "high", "low", "close", "volume"]
    for c in cols:
        if c not in out.columns and c != "ts_utc":
            out[c] = pd.NA
    return out[cols]


def oi_to_schema(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=["ts_utc", "oi"])
    out = df.copy()
    out["ts_utc"] = pd.to_datetime(out["ts"], utc=True)
    out["oi"] = out["openInterest"].astype(float) if "openInterest" in out.columns else out.get("oi")
    return out[["ts_utc", "oi"]]


def trades_to_schema(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=["ts_utc", "side", "qty", "price", "value_usd"])
    out = df.copy()
    out["ts_utc"] = pd.to_datetime(out["ts"], utc=True)
    out["value_usd"] = (out["qty"].astype(float) * out["price"].astype(float)).astype(float)
    return out[["ts_utc", "side", "qty", "price", "value_usd"]]


def oi_for_v1(oi_schema: pd.DataFrame) -> pd.DataFrame:
    if oi_schema is None or oi_schema.empty:
        return pd.DataFrame(columns=["ts", "openInterest"])
    x = oi_schema.copy()
    x["ts"] = pd.to_datetime(x["ts_utc"], utc=True)
    x["openInterest"] = x["oi"].astype(float)
    return x[["ts", "openInterest"]]


def klines_for_v1(k: pd.DataFrame) -> pd.DataFrame:
    if k is None or k.empty:
        return pd.DataFrame(columns=["ts", "open", "high", "low", "close", "volume"])
    x = k.copy()
    x["ts"] = pd.to_datetime(x["ts_utc"], utc=True)
    for c in ("open", "high", "low", "close", "volume"):
        x[c] = x[c].astype(float)
    return x[["ts", "open", "high", "low", "close", "volume"]]


def trades_for_v1(t: pd.DataFrame) -> pd.DataFrame:
    if t is None or t.empty:
        return pd.DataFrame(columns=["ts", "side", "qty", "price"])
    x = t.copy()
    x["ts"] = pd.to_datetime(x["ts_utc"], utc=True)
    return x


def liq_side_bucket(side: str) -> str:
    s = str(side).strip().lower()
    if s == "buy":
        return "short"
    if s == "sell":
        return "long"
    return "unknown"


def liquidation_rollups_from_csv(
    symbol: str, window_end: pd.Timestamp, liq_csv: Path
) -> Dict[str, Any]:
    """Rollups for (window_end-30s, window_end] and (window_end-60s, window_end]."""
    zero = {
        "long_count_30s": 0,
        "long_usd_30s": 0.0,
        "short_count_30s": 0,
        "short_usd_30s": 0.0,
        "long_count_60s": 0,
        "long_usd_60s": 0.0,
        "short_count_60s": 0,
        "short_usd_60s": 0.0,
    }
    if not liq_csv.is_file():
        return zero.copy()
    df = pd.read_csv(liq_csv)
    if df.empty:
        return zero.copy()
    df = df[df["symbol"].astype(str).str.upper() == symbol.upper()].copy()
    df["ts"] = pd.to_datetime(df["ts_utc"], utc=True)
    we = _as_utc(window_end)
    out: Dict[str, float | int] = {}
    for sec, tag in ((30, "30s"), (60, "60s")):
        start = we - pd.Timedelta(seconds=sec)
        w = df[(df["ts"] > start) & (df["ts"] <= we)]
        lc = sc = 0
        lu = su = 0.0
        for _, row in w.iterrows():
            usd = float(row.get("value_usd") or 0.0)
            b = liq_side_bucket(str(row.get("side", "")))
            if b == "long":
                lc += 1
                lu += usd
            elif b == "short":
                sc += 1
                su += usd
        out[f"long_count_{tag}"] = lc
        out[f"long_usd_{tag}"] = lu
        out[f"short_count_{tag}"] = sc
        out[f"short_usd_{tag}"] = su
    return out


def replay_structure(
    cfg: Any, candles_5m_schema: pd.DataFrame
) -> Tuple[StructureState, float]:
    """Replay update_structure bar-by-bar (watcher-like tail-20 peak on prefix)."""
    st = StructureState()
    if candles_5m_schema is None or candles_5m_schema.empty:
        return st, 0.0
    df = klines_for_v1(candles_5m_schema).sort_values("ts").reset_index(drop=True)
    for i in range(len(df)):
        sub = df.iloc[: i + 1]
        peak_price = float(sub["high"].tail(20).max())
        last_price = float(sub.iloc[-1]["close"])
        update_structure(cfg, st, last_price, peak_price)
    last_close = float(df.iloc[-1]["close"])
    win_high = float(df["high"].max())
    dist_win = (win_high - last_close) / win_high * 100.0 if win_high > 0 else 0.0
    return st, dist_win


def safe_call(fn: Callable[..., Any], *args: Any, **kwargs: Any) -> Tuple[Any, Optional[BaseException]]:
    try:
        return fn(*args, **kwargs), None
    except BaseException as e:  # noqa: BLE001 — capture v1 behavior for fixtures
        return None, e


def write_parquet(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        df.to_parquet(path, index=False, engine="pyarrow")
    except Exception:
        df.to_parquet(path, index=False, engine="fastparquet")


def build_fixture(spec: Dict[str, Any]) -> Dict[str, Any]:
    name = spec["name"]
    symbol = spec["symbol"]
    ts_end = pd.Timestamp(spec["ts_snapshot_utc"])
    if ts_end.tzinfo is None:
        ts_end = ts_end.tz_localize("UTC")
    window_minutes = int(spec.get("window_minutes", 60))
    ws = ts_end - pd.Timedelta(minutes=window_minutes)
    we = ts_end
    start_ms, end_ms = ts_ms_bounds(ws, we)

    out_dir = SYMS_ROOT / name
    out_dir.mkdir(parents=True, exist_ok=True)

    print(f"\n=== {name} | {symbol} | {ws.isoformat()} .. {we.isoformat()} ===")

    print("  fetch klines 1m/5m …")
    kl1 = get_klines_1m_range(CATEGORY, symbol, start_ms=start_ms, end_ms=end_ms, limit=1000)
    _sleep()
    kl5 = get_klines_5m_range(CATEGORY, symbol, start_ms=start_ms, end_ms=end_ms, limit=500)
    _sleep()
    kl1w = filter_window(kl1, ws, we)
    kl5w = filter_window(kl5, ws, we)
    kl1s = klines_to_schema(kl1w)
    kl5s = klines_to_schema(kl5w)
    write_parquet(kl1s, out_dir / "klines_1m.parquet")
    write_parquet(kl5s, out_dir / "klines_5m.parquet")

    print("  fetch OI …")
    oi_raw = get_open_interest(CATEGORY, symbol, limit=200)
    _sleep()
    skip_oi = bool(spec.get("omit_oi_parquet"))
    oi_w = filter_window(oi_raw, ws, we)
    oi_fallback = False
    if not skip_oi:
        if oi_w.empty and oi_raw is not None and not oi_raw.empty:
            oi_w = oi_raw.copy()
            oi_fallback = True
        oi_schema = oi_to_schema(oi_w)
    else:
        oi_schema = pd.DataFrame(columns=["ts_utc", "oi"])
    if skip_oi:
        write_parquet(pd.DataFrame(columns=["ts_utc", "oi"]), out_dir / "oi.parquet")
    else:
        write_parquet(oi_schema, out_dir / "oi.parquet")

    print("  fetch trades …")
    tr = get_recent_trades(CATEGORY, symbol, limit=1000)
    _sleep()
    tr_w = filter_window(tr, ws, we)
    tr_s = trades_to_schema(tr_w)
    write_parquet(tr_s, out_dir / "trades.parquet")

    print("  fetch funding …")
    funding = get_funding_rate(CATEGORY, symbol)
    _sleep()
    (out_dir / "funding.json").write_text(json.dumps(funding or {}, indent=2), encoding="utf-8")

    print("  slice liquidations.csv …")
    if LIQ_CSV.is_file():
        liq_all = pd.read_csv(LIQ_CSV)
        liq_all["ts"] = pd.to_datetime(liq_all["ts_utc"], utc=True)
        liq_sym = liq_all[
            (liq_all["symbol"].astype(str).str.upper() == symbol.upper())
            & (liq_all["ts"] >= ws)
            & (liq_all["ts"] <= we)
        ]
        liq_sym.to_csv(out_dir / "liquidations.csv", index=False)
        n_liq = len(liq_sym)
    else:
        pd.DataFrame(columns=["ts_utc", "ts_ms", "symbol", "side", "qty", "price", "value_usd"]).to_csv(
            out_dir / "liquidations.csv", index=False
        )
        n_liq = 0

    meta = {
        "symbol": symbol,
        "ts_snapshot_utc": ts_end.isoformat(),
        "window_start_utc": ws.isoformat(),
        "window_end_utc": we.isoformat(),
        "description": spec.get("description", ""),
        "oi_parquet_note": (
            "open_interest API returns only the latest N 5m points (~16h lookback); "
            "no rows fell inside [window_start_utc, window_end_utc] — stored full latest snapshot for non-null v1 OI metrics."
            if oi_fallback
            else ""
        ),
    }
    meta = {k: v for k, v in meta.items() if v != ""}
    (out_dir / "meta.json").write_text(json.dumps(meta, indent=2), encoding="utf-8")

    # --- v1 computation copies ---
    oi_v1 = oi_for_v1(oi_schema if not skip_oi else pd.DataFrame(columns=["ts_utc", "oi"]))
    kl5_v1 = klines_for_v1(kl5s)
    kl1_v1 = klines_for_v1(kl1s)
    tr_v1 = trades_for_v1(tr_s)

    cfg = SimpleNamespace(
        symbol=symbol,
        run_id=f"golden_{name}",
        drop1_min_pct=3.0,
        bounce1_min_pct=1.0,
        drop2_min_pct=2.0,
        bounce2_min_pct=0.8,
    )
    st, dist_win = replay_structure(cfg, kl5s)

    roll_raw = liquidation_rollups_from_csv(symbol, we, LIQ_CSV)
    roll_fn = liquidation_rollups_from_csv

    dbg5_list = kl5_v1.rename(columns={"ts": "ts"}).to_dict("records")
    # build_dbg5 expects list of dicts with ts-compatible keys
    recs: List[Dict[str, Any]] = []
    for row in dbg5_list:
        r = dict(row)
        r["ts"] = r["ts"]
        recs.append(r)

    oi_block: Optional[Dict[str, Any]] = None
    if oi_v1 is not None and not oi_v1.empty:
        oi_block = {"oi_df": oi_v1}

    dbg5 = build_dbg5(cfg, recs, oi_block, None, st)
    ctx_score, ctx_parts = compute_context_score_5m(dbg5)

    pump_shape = mf.pump_shape_features_5m(kl5_v1, lookback=5)
    fr, fr_ts, fr_abs = mf.normalize_funding(funding)

    cvd_abs_5m, cvd_ratio_5m = mf.cvd_5m(tr_v1, we)

    expected: Dict[str, Any] = {}

    for lb, key in ((1, "oi_change_pct_1m"), (3, "oi_change_pct_3m"), (5, "oi_change_pct_5m")):
        v, err = safe_call(spf.oi_change_pct, oi_v1, lookback_minutes=lb)
        expected[key] = pack(v, spf.oi_change_pct, params={"lookback_minutes": lb}, error=err)

    oi5 = expected["oi_change_pct_5m"]["value"]
    oi5_f = float(oi5) if oi5 is not None else None
    dist_peak_raw = float(dbg5.get("dist_to_peak_pct") or 0.0)
    dist_peak_dbg = round(dist_peak_raw, 6)
    div_v, div_e = safe_call(spf.oi_divergence_5m, oi5_f, dist_peak_raw)
    expected["oi_divergence_5m"] = pack(
        div_v,
        spf.oi_divergence_5m,
        params={"oi_change_5m_pct": oi5_f, "dist_to_peak_pct": dist_peak_dbg},
        error=div_e,
    )

    for sec, key in ((30, "delta_ratio_30s"), (60, "delta_ratio_1m"), (180, "delta_ratio_3m")):
        since = we - pd.Timedelta(seconds=sec)
        v, err = safe_call(spf.delta_ratio, tr_v1, since)
        expected[key] = pack(v, spf.delta_ratio, params={"since_ts": since.isoformat()}, error=err)

    for sec, key in ((30, "cvd_delta_ratio_30s"), (60, "cvd_delta_ratio_1m")):
        since = we - pd.Timedelta(seconds=sec)
        v, err = safe_call(spf.cvd_delta_ratio, tr_v1, since)
        expected[key] = pack(v, spf.cvd_delta_ratio, params={"since_ts": since.isoformat()}, error=err)

    v_atr, err_atr = safe_call(spf.atr_pct, kl5_v1, period=14)
    expected["atr_pct_5m_14"] = pack(v_atr, spf.atr_pct, params={"period": 14}, error=err_atr)

    v_volz, err_volz = safe_call(spf.volume_zscore, kl5_v1, lookback=50)
    expected["volume_zscore"] = pack(v_volz, spf.volume_zscore, params={"lookback": 50}, error=err_volz)

    cvd_note = None
    if cvd_abs_5m is None:
        cvd_note = "cvd_5m returns (None, None) when trades empty or no rows in 5m window"
    expected["cvd_5m"] = {
        "value": round6(cvd_abs_5m),
        "v1_ref": v1_ref(mf.cvd_5m),
        "params": {"now_ts_utc": we.isoformat()},
        **({"v1_behavior": cvd_note} if cvd_note else {}),
    }

    expected["funding_snapshot"] = {
        "value": {"funding_rate": round6(fr), "funding_rate_abs": round6(fr_abs), "ts_utc": fr_ts},
        "v1_ref": v1_ref(mf.normalize_funding),
        "params": {"payload": "see funding.json"},
    }

    expected["pump_shape_5m"] = {
        "value": round6(pump_shape),
        "v1_ref": v1_ref(mf.pump_shape_features_5m),
        "params": {"lookback": 5},
    }

    expected["liquidation_rollups"] = {
        "value": round6(roll_raw),
        "v1_ref": v1_ref(roll_fn),
        "params": {"symbol": symbol, "window_end": we.isoformat(), "liq_csv": str(LIQ_CSV.relative_to(ROOT))},
    }

    st_dump = {k: round6(v) for k, v in asdict(st).items()}
    expected["structure_state"] = {
        "value": st_dump,
        "v1_ref": v1_ref(update_structure),
        "params": {"cfg_defaults": "drop1_min_pct=3 bounce1_min_pct=1 drop2_min_pct=2 bounce2_min_pct=0.8"},
    }
    dbg5_dist = float(dbg5.get("dist_to_peak_pct") or 0.0)
    expected["dist_to_fsm_peak_pct"] = pack(
        dbg5_dist,
        build_dbg5,
        params={"field": "dist_to_peak_pct", "replay": "StructureState via update_structure bar-by-bar"},
    )
    expected["dist_to_window_peak_pct"] = {
        "value": round6(dist_win),
        "v1_ref": "derived:window_high_vs_last_close",
        "params": {
            "formula": "(max(high) in klines_5m fixture - last_close) / max(high) * 100",
            "inputs": "short_pump.bybit_api.get_klines_5m_range + filter_window",
        },
    }

    expected["context_score_5m"] = {
        "value": round6(ctx_score),
        "parts": round6(ctx_parts),
        "v1_ref": v1_ref(compute_context_score_5m),
        "params": {"dbg5_from": v1_ref(build_dbg5)},
    }

    # build_dbg5 / watcher internal helpers (document-only; used for context path)
    expected["_note_context_score_inputs"] = {
        "vol_z_dbg5": round6(dbg5.get("vol_z")),
        "atr_14_5m_pct_dbg5": round6(dbg5.get("atr_14_5m_pct")),
        "oi_change_5m_pct_dbg5": round6(dbg5.get("oi_change_5m_pct")),
        "v1_ref_vol_z": "short_pump/context5m.py:_volume_z (used by build_dbg5, not features.volume_zscore)",
    }

    (out_dir / "expected_indicators.json").write_text(json.dumps(expected, indent=2, default=str), encoding="utf-8")

    summary = {
        "name": name,
        "symbol": symbol,
        "klines_1m": len(kl1s),
        "klines_5m": len(kl5s),
        "oi_rows": 0 if skip_oi else len(oi_schema),
        "trades": len(tr_s),
        "liquidations_in_window": n_liq,
    }
    print(f"  done: {summary}")
    return summary


FIXTURES: List[Dict[str, Any]] = [
    {
        "name": "btc_typical",
        "symbol": "BTCUSDT",
        "ts_snapshot_utc": "2026-05-13T12:00:00+00:00",
        "window_minutes": 60,
        "description": "Typical BTC window; liquidations.csv may be empty if collector started after window_end.",
    },
    {
        "name": "pump_recent",
        "symbol": "DYMUSDT",
        "ts_snapshot_utc": "2026-05-13T00:18:55.398976+00:00",
        "window_minutes": 60,
        "description": "watch_start from datasets events_v3 (pump_pct in payload); 60m window before snapshot.",
    },
    {
        "name": "quiet_alt",
        "symbol": "DOGEUSDT",
        "ts_snapshot_utc": "2026-05-13T12:00:00+00:00",
        "window_minutes": 60,
        "description": "Lower-activity alt vs BTC; same snapshot wall as btc_typical.",
    },
    {
        "name": "no_oi",
        "symbol": "BTCUSDT",
        "ts_snapshot_utc": "2026-05-13T12:00:00+00:00",
        "window_minutes": 60,
        "omit_oi_parquet": True,
        "description": "Synthetic missing OI: oi.parquet empty; klines/trades same fetch as BTC window.",
    },
    {
        "name": "no_liquidations",
        "symbol": "IOTAUSDT",
        "ts_snapshot_utc": "2026-05-14T11:00:00+00:00",
        "window_minutes": 60,
        "description": "Hour window with zero rows for this symbol in datasets/liquidations.csv (verified offline).",
    },
]


def main() -> None:
    SYMS_ROOT.mkdir(parents=True, exist_ok=True)
    rows: List[Dict[str, Any]] = []
    for spec in FIXTURES:
        rows.append(build_fixture(spec))
    print("\n=== SUMMARY ===")
    df = pd.DataFrame(rows)
    print(df.to_string(index=False))
    print("\nWrote fixtures under:", SYMS_ROOT)


if __name__ == "__main__":
    main()
