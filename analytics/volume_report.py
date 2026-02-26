"""
Read signals_queue.jsonl.processed and report FAST0 volume features (volume_zscore_20, volume_1m, volume_sma_20).
Use for management report: no longer show 'volume_zscore: недоступно' for FAST0; show splits like liq_long_usd_30s.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent


def _read_processed_lines(path: Path | None = None) -> list[dict]:
    p = path or _ROOT / "datasets" / "signals_queue.jsonl.processed"
    if not p.exists():
        return []
    lines = []
    with open(p, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                lines.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return lines


def fast0_volume_splits(records: list[dict] | None = None) -> dict:
    """
    From processed FAST0 signals compute volume_zscore_20 splits (buckets).
    Returns dict with keys like "volume_zscore_20_<2", "volume_zscore_20_2_3", etc. and counts.
    """
    if records is None:
        records = _read_processed_lines()
    fast0 = [r for r in records if r.get("strategy") == "short_pump_fast0"]
    if not fast0:
        return {"total": 0, "with_volume_zscore_20": 0}

    with_val = [r for r in fast0 if r.get("volume_zscore_20") is not None]
    total = len(fast0)
    with_volume = len(with_val)

    buckets = {
        "volume_zscore_20_lt_0": 0,
        "volume_zscore_20_0_1": 0,
        "volume_zscore_20_1_2": 0,
        "volume_zscore_20_ge_2": 0,
    }
    for r in with_val:
        try:
            z = float(r["volume_zscore_20"])
        except (TypeError, ValueError, KeyError):
            continue
        if z < 0:
            buckets["volume_zscore_20_lt_0"] += 1
        elif z < 1:
            buckets["volume_zscore_20_0_1"] += 1
        elif z < 2:
            buckets["volume_zscore_20_1_2"] += 1
        else:
            buckets["volume_zscore_20_ge_2"] += 1

    return {
        "total": total,
        "with_volume_zscore_20": with_volume,
        "volume_1m_present": sum(1 for r in fast0 if r.get("volume_1m") is not None),
        "volume_sma_20_present": sum(1 for r in fast0 if r.get("volume_sma_20") is not None),
        **buckets,
    }


def print_fast0_volume_report(path: Path | None = None) -> None:
    """Print FAST0 volume report (volume_zscore_20 splits). Use from management report."""
    records = _read_processed_lines(path)
    s = fast0_volume_splits(records)
    if s["total"] == 0:
        print("FAST0 volume_zscore_20: no FAST0 signals in processed file")
        return
    print("FAST0 volume features (signals_queue.jsonl.processed)")
    print(f"  total FAST0 signals: {s['total']}")
    print(f"  with volume_zscore_20: {s['with_volume_zscore_20']}")
    print(f"  with volume_1m: {s['volume_1m_present']}")
    print(f"  with volume_sma_20: {s['volume_sma_20_present']}")
    if s["with_volume_zscore_20"]:
        print("  volume_zscore_20 splits:")
        print(f"    < 0:   {s['volume_zscore_20_lt_0']}")
        print(f"    0–1:   {s['volume_zscore_20_0_1']}")
        print(f"    1–2:   {s['volume_zscore_20_1_2']}")
        print(f"    >= 2:  {s['volume_zscore_20_ge_2']}")
    else:
        print("  volume_zscore_20: no data (run FAST0 session to populate)")


if __name__ == "__main__":
    print_fast0_volume_report()
    sys.exit(0)
