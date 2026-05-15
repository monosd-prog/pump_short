#!/usr/bin/env python3
"""
Compute v1 expected_indicators.json from live_collected raw parquet/csv.
"""
from __future__ import annotations

import importlib.util
import json
import sys
from dataclasses import asdict
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Dict, List, Optional

import pandas as pd

ROOT = Path(__file__).resolve().parents[4]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from common import market_features as mf  # noqa: E402
from short_pump import features as spf  # noqa: E402
from short_pump.context5m import build_dbg5, compute_context_score_5m, update_structure  # noqa: E402

_gen_path = Path(__file__).resolve().parents[1] / "_generate.py"
_spec = importlib.util.spec_from_file_location("fixture_gen", _gen_path)
assert _spec and _spec.loader
_gen = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_gen)

LIVE_ROOT = Path(__file__).resolve().parent
META_PATH = LIVE_ROOT / "meta.json"


def _read_parquet(path: Path) -> pd.DataFrame:
    if not path.is_file():
        return pd.DataFrame()
    return pd.read_parquet(path)


def _per_symbol_meta(meta: Dict[str, Any], symbol: str, sym_dir: Path) -> None:
    out = {
        "symbol": symbol,
        "ts_snapshot_utc": meta["collection_end_utc"],
        "window_start_utc": meta["collection_start_utc"],
        "window_end_utc": meta["collection_end_utc"],
        "description": "Live WS+REST collection window (see live_collected/meta.json)",
        "source": "live_collected",
    }
    (sym_dir / "meta.json").write_text(json.dumps(out, indent=2), encoding="utf-8")


def _count_non_null(expected: Dict[str, Any]) -> int:
    n = 0
    for ent in expected.values():
        if isinstance(ent, dict) and ent.get("value") is not None:
            n += 1
    return n


def compute_symbol(sym_dir: Path, symbol: str, meta: Dict[str, Any]) -> Dict[str, Any]:
    we = pd.Timestamp(meta["collection_end_utc"])
    if we.tzinfo is None:
        we = we.tz_localize("UTC")

    _per_symbol_meta(meta, symbol, sym_dir)

    kl5s = _read_parquet(sym_dir / "klines_5m.parquet")
    oi_raw = _read_parquet(sym_dir / "oi_history.parquet")
    tr_raw = _read_parquet(sym_dir / "trades.parquet")

    if not tr_raw.empty:
        tr_raw = tr_raw.copy()
        if "ts" not in tr_raw.columns:
            if "ts_ms" in tr_raw.columns:
                tr_raw["ts"] = pd.to_datetime(tr_raw["ts_ms"], unit="ms", utc=True)
            else:
                tr_raw["ts"] = pd.to_datetime(tr_raw["ts_utc"], utc=True)

    oi_schema = pd.DataFrame(columns=["ts_utc", "oi"])
    if not oi_raw.empty:
        oi_schema = oi_raw.copy()
        oi_schema["ts_utc"] = pd.to_datetime(oi_schema["ts_utc"], utc=True)

    tr_s = _gen.trades_to_schema(tr_raw) if not tr_raw.empty else tr_raw
    oi_v1 = _gen.oi_for_v1(oi_schema)
    kl5_v1 = _gen.klines_for_v1(kl5s)
    tr_v1 = _gen.trades_for_v1(tr_s)

    funding: Dict[str, Any] = {}
    fp = sym_dir / "funding.json"
    if fp.is_file():
        funding = json.loads(fp.read_text(encoding="utf-8"))
    rest_funding = funding.get("rest_tickers") or funding.get("ws_ticker_last") or {}

    liq_path = sym_dir / "liquidations.csv"
    n_liq = len(pd.read_csv(liq_path)) if liq_path.is_file() else 0

    cfg = SimpleNamespace(
        symbol=symbol,
        run_id=f"live_{symbol.lower()}",
        drop1_min_pct=3.0,
        bounce1_min_pct=1.0,
        drop2_min_pct=2.0,
        bounce2_min_pct=0.8,
    )
    st, dist_win = _gen.replay_structure(cfg, kl5s)
    roll_raw = _gen.liquidation_rollups_from_csv(symbol, we, liq_path)

    recs = [dict(r) for r in kl5_v1.to_dict("records")]
    oi_block = {"oi_df": oi_v1} if not oi_v1.empty else None
    dbg5 = build_dbg5(cfg, recs, oi_block, None, st)
    ctx_score, ctx_parts = compute_context_score_5m(dbg5)

    pump_shape = mf.pump_shape_features_5m(kl5_v1, lookback=5)
    fr, fr_ts, fr_abs = mf.normalize_funding(rest_funding)
    cvd_abs_5m, cvd_ratio_5m = mf.cvd_5m(tr_v1, we)

    expected: Dict[str, Any] = {}
    for lb, key in ((1, "oi_change_pct_1m"), (3, "oi_change_pct_3m"), (5, "oi_change_pct_5m")):
        v, err = _gen.safe_call(spf.oi_change_pct, oi_v1, lookback_minutes=lb)
        expected[key] = _gen.pack(v, spf.oi_change_pct, params={"lookback_minutes": lb}, error=err)

    oi5 = expected["oi_change_pct_5m"]["value"]
    oi5_f = float(oi5) if oi5 is not None else None
    dist_peak_raw = float(dbg5.get("dist_to_peak_pct") or 0.0)
    div_v, div_e = _gen.safe_call(spf.oi_divergence_5m, oi5_f, dist_peak_raw)
    expected["oi_divergence_5m"] = _gen.pack(
        div_v,
        spf.oi_divergence_5m,
        params={"oi_change_5m_pct": oi5_f, "dist_to_peak_pct": round(dist_peak_raw, 6)},
        error=div_e,
    )

    for sec, key in ((30, "delta_ratio_30s"), (60, "delta_ratio_1m"), (180, "delta_ratio_3m")):
        since = we - pd.Timedelta(seconds=sec)
        v, err = _gen.safe_call(spf.delta_ratio, tr_v1, since)
        expected[key] = _gen.pack(v, spf.delta_ratio, params={"since_ts": since.isoformat()}, error=err)

    for sec, key in ((30, "cvd_delta_ratio_30s"), (60, "cvd_delta_ratio_1m")):
        since = we - pd.Timedelta(seconds=sec)
        v, err = _gen.safe_call(spf.cvd_delta_ratio, tr_v1, since)
        expected[key] = _gen.pack(v, spf.cvd_delta_ratio, params={"since_ts": since.isoformat()}, error=err)

    v_atr, err_atr = _gen.safe_call(spf.atr_pct, kl5_v1, period=14)
    expected["atr_pct_5m_14"] = _gen.pack(v_atr, spf.atr_pct, params={"period": 14}, error=err_atr)

    v_volz, err_volz = _gen.safe_call(spf.volume_zscore, kl5_v1, lookback=50)
    expected["volume_zscore"] = _gen.pack(v_volz, spf.volume_zscore, params={"lookback": 50}, error=err_volz)

    cvd_note = "cvd_5m returns (None, None) when trades empty" if cvd_abs_5m is None else None
    expected["cvd_5m"] = {
        "value": _gen.round6(cvd_abs_5m),
        "v1_ref": _gen.v1_ref(mf.cvd_5m),
        "params": {"now_ts_utc": we.isoformat()},
        **({"v1_behavior": cvd_note} if cvd_note else {}),
    }
    if cvd_ratio_5m is not None:
        expected["cvd_5m"]["cvd_ratio_5m"] = _gen.round6(cvd_ratio_5m)

    expected["funding_snapshot"] = {
        "value": {"funding_rate": _gen.round6(fr), "funding_rate_abs": _gen.round6(fr_abs), "ts_utc": fr_ts},
        "v1_ref": _gen.v1_ref(mf.normalize_funding),
        "params": {"payload": "see funding.json"},
    }
    expected["pump_shape_5m"] = {
        "value": _gen.round6(pump_shape),
        "v1_ref": _gen.v1_ref(mf.pump_shape_features_5m),
        "params": {"lookback": 5},
    }
    expected["liquidation_rollups"] = {
        "value": _gen.round6(roll_raw),
        "v1_ref": _gen.v1_ref(_gen.liquidation_rollups_from_csv),
        "params": {"symbol": symbol, "window_end": we.isoformat()},
    }
    expected["structure_state"] = {
        "value": {k: _gen.round6(v) for k, v in asdict(st).items()},
        "v1_ref": _gen.v1_ref(update_structure),
        "params": {},
    }
    expected["dist_to_fsm_peak_pct"] = _gen.pack(
        float(dbg5.get("dist_to_peak_pct") or 0.0),
        build_dbg5,
        params={"field": "dist_to_peak_pct"},
    )
    expected["dist_to_window_peak_pct"] = {
        "value": _gen.round6(dist_win),
        "v1_ref": "derived:window_high_vs_last_close",
        "params": {},
    }
    expected["context_score_5m"] = {
        "value": _gen.round6(ctx_score),
        "parts": _gen.round6(ctx_parts),
        "v1_ref": _gen.v1_ref(compute_context_score_5m),
        "params": {},
    }

    (sym_dir / "expected_indicators.json").write_text(json.dumps(expected, indent=2, default=str), encoding="utf-8")

    return {
        "symbol": symbol,
        "n_trades": len(tr_s) if tr_s is not None and not tr_s.empty else 0,
        "n_oi_history": len(oi_schema),
        "n_liquidations": n_liq,
        "n_non_null_indicators": _count_non_null(expected),
    }


def main() -> None:
    if not META_PATH.is_file():
        print("Missing", META_PATH)
        sys.exit(1)
    meta = json.loads(META_PATH.read_text(encoding="utf-8"))
    rows = []
    for sym in meta.get("symbols", []):
        sym_dir = LIVE_ROOT / sym.lower()
        if not sym_dir.is_dir():
            print("SKIP", sym_dir)
            continue
        print(f"=== {sym} ===")
        rows.append(compute_symbol(sym_dir, sym.upper(), meta))
    print("\n=== SUMMARY ===")
    print(pd.DataFrame(rows).to_string(index=False))


if __name__ == "__main__":
    main()
