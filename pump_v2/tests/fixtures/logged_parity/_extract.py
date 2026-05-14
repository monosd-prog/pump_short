#!/usr/bin/env python3
"""
Extract stratified logged-parity samples from datasets/**/events_v3.csv.

Entry-row heuristic (see README):
- event_id matches _entry_|entry_fast|entry_confirm, OR
- entry_ok==1 and event_id contains fast0, OR
- skip_reasons contains entry_ok and entry_ok==1 (false_pump entries).

Does not modify v1. Read-only on datasets.
"""
from __future__ import annotations

import csv
import json
import math
import random
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Dict, List, Tuple

import pandas as pd
import numpy as np

ROOT = Path(__file__).resolve().parents[4]
DATASETS = ROOT / "datasets"
OUT_DIR = Path(__file__).resolve().parent
SAMPLES_DIR = OUT_DIR / "samples"
INDEX_CSV = OUT_DIR / "index.csv"

STRATA_QUOTA: Dict[str, int] = {
    "short_pump": 15,
    "short_pump_fast0": 10,
    "short_pump_fast0_filtered": 10,
    "short_pump_wick": 5,
    "short_pump_premium": 5,
    "short_pump_filtered": 3,
    "false_pump": 2,
}

LOGGED_KEYS: List[str] = [
    "oi_change_1m_pct",
    "oi_change_5m_pct",
    "oi_change_fast_pct",
    "delta_ratio_30s",
    "delta_ratio_1m",
    "delta_ratio_3m",
    "cvd_delta_ratio_30s",
    "cvd_delta_ratio_1m",
    "cvd_abs_5m",
    "cvd_ratio_5m",
    "funding_rate",
    "funding_rate_abs",
    "green_candles_5",
    "wick_body_ratio_last",
    "upper_wick_ratio_last",
    "lower_wick_ratio_last",
    "max_candle_body_pct_5",
    "avg_candle_body_pct_5",
    "volume_zscore_20",
    "stage",
    "dist_to_peak_pct",
    "context_score",
    "context_parts",
    "liq_long_count_30s",
    "liq_long_usd_30s",
    "liq_short_count_30s",
    "liq_short_usd_30s",
    "liq_long_count_1m",
    "liq_long_usd_1m",
    "liq_short_count_1m",
    "liq_short_usd_1m",
]

# Keys that may appear both in CSV columns and payload_json (for conflict check)
CONFLICT_CHECK_KEYS = [
    "oi_change_1m_pct",
    "oi_change_5m_pct",
    "oi_change_fast_pct",
    "delta_ratio_30s",
    "delta_ratio_1m",
    "delta_ratio_3m",
    "cvd_delta_ratio_30s",
    "cvd_delta_ratio_1m",
    "cvd_abs_5m",
    "cvd_ratio_5m",
    "funding_rate",
    "funding_rate_abs",
    "green_candles_5",
    "wick_body_ratio_last",
    "upper_wick_ratio_last",
    "lower_wick_ratio_last",
    "max_candle_body_pct_5",
    "avg_candle_body_pct_5",
    "volume_zscore_20",
    "stage",
    "dist_to_peak_pct",
    "context_score",
    "liq_long_count_30s",
    "liq_long_usd_30s",
    "liq_short_count_30s",
    "liq_short_usd_30s",
    "liq_long_count_1m",
    "liq_long_usd_1m",
    "liq_short_count_1m",
    "liq_short_usd_1m",
]


def _norm_scalar(v: Any) -> Any:
    if v is None or (isinstance(v, float) and (math.isnan(v) or math.isinf(v))):
        return None
    if isinstance(v, str) and v.strip() == "":
        return None
    if isinstance(v, (int, float)) and not isinstance(v, bool):
        return float(v)
    if isinstance(v, bool):
        return v
    if isinstance(v, str):
        try:
            return float(v)
        except ValueError:
            return v.strip()
    return v


def _close(a: Any, b: Any, eps: float = 1e-5) -> bool:
    if a is None and b is None:
        return True
    if a is None or b is None:
        return False
    if isinstance(a, (int, float)) and isinstance(b, (int, float)):
        return abs(float(a) - float(b)) <= eps
    return a == b


def _json_default(o: Any) -> Any:
    if isinstance(o, (np.integer, np.int64, np.int32)):
        return int(o)
    if isinstance(o, (np.floating, np.float64, np.float32)):
        x = float(o)
        if math.isnan(x) or math.isinf(x):
            return None
        return x
    if isinstance(o, np.bool_):
        return bool(o)
    raise TypeError(repr(o))


def _round_out(v: Any) -> Any:
    if v is None:
        return None
    if isinstance(v, float):
        if math.isnan(v) or math.isinf(v):
            return None
        return round(v, 6)
    if isinstance(v, dict):
        return {k: _round_out(x) for k, x in v.items()}
    if isinstance(v, list):
        return [_round_out(x) for x in v]
    return v


def _parse_payload(raw: Any) -> Dict[str, Any]:
    if raw is None or (isinstance(raw, float) and pd.isna(raw)):
        return {}
    s = str(raw).strip()
    if not s:
        return {}
    try:
        return json.loads(s)
    except json.JSONDecodeError:
        return {}


def _csv_val(row: pd.Series, key: str) -> Any:
    if key not in row.index:
        return None
    v = row[key]
    if pd.isna(v):
        return None
    return v


def build_logged_indicators(row: pd.Series, payload: Dict[str, Any]) -> Tuple[Dict[str, Any], bool]:
    """Returns logged_indicators dict and has_any_conflict."""
    logged: Dict[str, Any] = {}
    any_conflict = False

    for key in LOGGED_KEYS:
        if key == "context_parts":
            parts = payload.get("context_parts")
            if parts is None or parts == "":
                logged[key] = {"value": None, "source": "payload_json"}
            else:
                logged[key] = {"value": _round_out(parts), "source": "payload_json"}
            continue

        if key == "delta_ratio_3m":
            v = payload.get("delta_ratio_3m")
            src = "payload_json"
            if v is None and key in row.index:
                cv = _norm_scalar(_csv_val(row, key))
                if cv is not None:
                    v = cv
                    src = "csv_column"
            logged[key] = {"value": _round_out(_norm_scalar(v)), "source": src}
            continue

        csv_v = _norm_scalar(_csv_val(row, key))
        pay_v = _norm_scalar(payload.get(key))

        entry: Dict[str, Any]
        if csv_v is not None:
            entry = {"value": _round_out(csv_v), "source": "csv_column"}
            if key in CONFLICT_CHECK_KEYS and pay_v is not None and not _close(csv_v, pay_v):
                entry["payload_json_conflict"] = True
                entry["csv_value"] = _round_out(csv_v)
                entry["payload_value"] = _round_out(pay_v)
                any_conflict = True
        elif pay_v is not None:
            entry = {"value": _round_out(pay_v), "source": "payload_json"}
        else:
            entry = {"value": None, "source": "csv_column"}

        logged[key] = entry

    return logged, any_conflict


def count_non_null(logged: Dict[str, Any]) -> int:
    n = 0
    for k, ent in logged.items():
        if isinstance(ent, dict) and ent.get("value") is not None:
            n += 1
    return n


def main() -> None:
    paths = sorted(DATASETS.rglob("events_v3.csv"))
    if not paths:
        print("No events_v3.csv under", DATASETS)
        sys.exit(1)

    pools: Dict[str, List[Tuple[str, int, pd.Series]]] = defaultdict(list)

    for p in paths:
        rel = str(p.relative_to(ROOT))
        try:
            df = pd.read_csv(p, low_memory=False)
        except (pd.errors.ParserError, ValueError) as e:
            try:
                df = pd.read_csv(p, engine="python", on_bad_lines="skip")
            except TypeError:
                df = pd.read_csv(p, engine="python")
        except Exception as e:
            print("SKIP read fail", rel, e)
            continue
        if df.empty:
            continue
        # 0-based row index in this CSV (first data row = 0), stable across filters
        df = df.reset_index(drop=True)
        df["_source_row_index"] = list(range(len(df)))
        sch = pd.to_numeric(df.get("schema_version"), errors="coerce")
        df = df[sch.fillna(-1).eq(3)]
        if df.empty:
            continue
        df = df[df["mode"].astype(str).isin(["live", "paper"])]
        if df.empty:
            continue
        eid = df["event_id"].astype(str)
        sr = df["skip_reasons"].astype(str)
        id_pat = eid.str.contains(r"_entry_|entry_fast|entry_confirm", case=False, na=False, regex=True)
        fast0_ok = pd.to_numeric(df["entry_ok"], errors="coerce").fillna(0).eq(1) & eid.str.contains(
            "fast0", case=False, na=False
        )
        skip_entry_ok = sr.str.contains("entry_ok", case=False, na=False) & pd.to_numeric(
            df["entry_ok"], errors="coerce"
        ).fillna(0).eq(1)
        df = df[id_pat | fast0_ok | skip_entry_ok]
        if df.empty:
            continue
        df = df[df["stage"].notna() & df["dist_to_peak_pct"].notna()]
        if df.empty:
            continue

        for idx in df.index:
            row = df.loc[idx]
            strat = str(row.get("strategy", "")).strip()
            if strat not in STRATA_QUOTA:
                continue
            row_pos = int(row["_source_row_index"])
            pools[strat].append((rel, row_pos, row))

    rng = random.Random(42)
    picked: List[Tuple[str, int, pd.Series]] = []
    picked_ids: set[Tuple[str, int]] = set()

    for strat, quota in STRATA_QUOTA.items():
        pool = pools.get(strat, [])
        k = min(quota, len(pool))
        if k == 0:
            continue
        chosen = rng.sample(pool, k)
        for item in chosen:
            sid = (item[0], item[1])
            if sid not in picked_ids:
                picked_ids.add(sid)
                picked.append(item)

    fill_order = [
        "short_pump",
        "short_pump_fast0",
        "short_pump_fast0_filtered",
        "short_pump_wick",
        "short_pump_premium",
        "short_pump_filtered",
        "false_pump",
    ]
    if len(picked) < 50:
        remainder: List[Tuple[str, int, pd.Series]] = []
        for strat in fill_order:
            for item in pools.get(strat, []):
                sid = (item[0], item[1])
                if sid not in picked_ids:
                    remainder.append(item)
        rng.shuffle(remainder)
        for item in remainder:
            if len(picked) >= 50:
                break
            sid = (item[0], item[1])
            if sid not in picked_ids:
                picked_ids.add(sid)
                picked.append(item)

    picked = picked[:50]

    # stable order for sample_001..050
    def sort_key(it: Tuple[str, int, pd.Series]) -> Tuple[str, str]:
        r = it[2]
        t = str(r.get("time_utc", ""))
        return (t, it[0])

    picked.sort(key=sort_key)

    SAMPLES_DIR.mkdir(parents=True, exist_ok=True)
    index_rows: List[Dict[str, Any]] = []
    null_counter: Counter[str] = Counter()
    conflict_fields: Counter[str] = Counter()
    fullish = 0  # all core keys non-null

    core_keys = [
        "oi_change_5m_pct",
        "delta_ratio_30s",
        "delta_ratio_1m",
        "cvd_ratio_5m",
        "stage",
        "dist_to_peak_pct",
        "context_score",
    ]

    for i, (rel, row_pos, row) in enumerate(picked, start=1):
        sid = f"sample_{i:03d}"
        payload = _parse_payload(row.get("payload_json"))
        logged, has_conf = build_logged_indicators(row, payload)

        for k, ent in logged.items():
            if isinstance(ent, dict) and ent.get("value") is None:
                null_counter[k] += 1
            if isinstance(ent, dict) and ent.get("payload_json_conflict"):
                conflict_fields[k] += 1

        ok_core = all(
            isinstance(logged.get(k), dict) and logged[k].get("value") is not None for k in core_keys
        )
        if ok_core:
            fullish += 1

        sample = {
            "meta": {
                "sample_id": sid,
                "source_file": rel,
                "source_row_index": row_pos,
                "symbol": str(row.get("symbol", "")),
                "strategy": str(row.get("strategy", "")).strip(),
                "mode": str(row.get("mode", "")).strip(),
                "event_id": str(row.get("event_id", "")),
                "time_utc": str(row.get("time_utc", "")),
            },
            "logged_indicators": logged,
            "raw_inputs_available": False,
            "note": (
                "Logged values from production watcher. For parity tests only — "
                "cannot independently recompute without raw klines/trades/oi."
            ),
        }
        out_path = SAMPLES_DIR / f"{sid}.json"
        out_path.write_text(
            json.dumps(sample, indent=2, ensure_ascii=False, allow_nan=False, default=_json_default),
            encoding="utf-8",
        )

        nn = count_non_null(logged)
        index_rows.append(
            {
                "sample_id": sid,
                "source_file": rel,
                "symbol": sample["meta"]["symbol"],
                "strategy": sample["meta"]["strategy"],
                "mode": sample["meta"]["mode"],
                "event_id": sample["meta"]["event_id"],
                "time_utc": sample["meta"]["time_utc"],
                "n_non_null_indicators": nn,
                "has_payload_json_conflicts": str(has_conf).lower(),
            }
        )

    with INDEX_CSV.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(
            f,
            fieldnames=[
                "sample_id",
                "source_file",
                "symbol",
                "strategy",
                "mode",
                "event_id",
                "time_utc",
                "n_non_null_indicators",
                "has_payload_json_conflicts",
            ],
        )
        w.writeheader()
        for r in index_rows:
            w.writerow(r)

    print("Wrote", len(picked), "samples to", SAMPLES_DIR)
    print("Index:", INDEX_CSV)
    print("--- null frequency (top 10) ---")
    for k, c in null_counter.most_common(10):
        print(k, c)
    print("--- payload_json conflicts by field ---")
    if conflict_fields:
        for k, c in conflict_fields.most_common():
            print(k, c)
    else:
        print("(none)")
    print("--- samples with all core keys non-null ---", fullish, "/", len(picked))
    print("--- strategy counts in index ---")
    sc = Counter(r["strategy"] for r in index_rows)
    for s, c in sorted(sc.items(), key=lambda x: -x[1]):
        print(s, c)


if __name__ == "__main__":
    main()
