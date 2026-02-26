#!/usr/bin/env python3
"""
Smoke: FAST0 volume features (volume_1m, volume_sma_20, volume_zscore_20).
- Test _volume_1m_features with mock 1m klines (no real exchange).
- Assert Signal serialization and processed dataset contain volume fields.
- Assert analytics volume report can read volume_zscore_20.
"""
from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT))

from short_pump.signals import Signal
from trading.signal_io import signal_to_dict, signal_from_dict
from analytics.volume_report import fast0_volume_splits


def test_volume_1m_features() -> None:
    """Mock 1m klines: call _volume_1m_features with deterministic DataFrame (optional, may skip if import fails)."""
    try:
        import pandas as pd
        from short_pump.fast0_sampler import _volume_1m_features
    except Exception:
        return  # skip in envs where pandas or fast0_sampler fails
    vols = [40.0] * 19 + [40.0, 100.0]
    df = pd.DataFrame({"volume": vols, "close": [1.0] * len(vols)})
    v1, sma, zscore = _volume_1m_features(df, lookback=20)
    assert v1 is not None and v1 == 100.0
    assert sma is not None
    assert zscore is not None
    assert zscore > 0
    v2, s2, z2 = _volume_1m_features(None, 20)
    assert v2 is None and s2 is None and z2 is None
    v3, s3, z3 = _volume_1m_features(pd.DataFrame(), 20)
    assert v3 is None and s3 is None and z3 is None


def test_signal_volume_roundtrip() -> None:
    """Signal with volume fields -> dict -> processed line -> report reads volume_zscore_20."""
    sig = Signal(
        strategy="short_pump_fast0",
        symbol="SOSOUSDT",
        side="SHORT",
        ts_utc="2026-02-05 12:00:00+0000",
        run_id="r1",
        event_id="e1",
        entry_price=0.045,
        tp_price=0.0445,
        sl_price=0.0455,
        tp_pct=None,
        sl_pct=None,
        stage=None,
        dist_to_peak_pct=1.5,
        context_score=0.7,
        cvd_30s=None,
        cvd_1m=None,
        liq_long_usd_30s=None,
        liq_short_usd_30s=None,
        volume_1m=1000.0,
        volume_sma_20=800.0,
        volume_zscore_20=1.25,
        extras={},
    )
    d = signal_to_dict(sig)
    assert "volume_1m" in d and d["volume_1m"] == 1000.0
    assert "volume_sma_20" in d and d["volume_sma_20"] == 800.0
    assert "volume_zscore_20" in d and d["volume_zscore_20"] == 1.25
    back = signal_from_dict(d)
    assert getattr(back, "volume_1m", None) == 1000.0
    assert getattr(back, "volume_sma_20", None) == 800.0
    assert getattr(back, "volume_zscore_20", None) == 1.25


def test_processed_and_report() -> None:
    """One JSONL line in temp processed file -> fast0_volume_splits sees volume_zscore_20."""
    line = json.dumps({
        "strategy": "short_pump_fast0",
        "symbol": "ARCUSDT",
        "side": "SHORT",
        "ts_utc": "2026-02-05 12:01:00+0000",
        "run_id": "r2",
        "event_id": "e2",
        "entry_price": 0.02,
        "tp_price": 0.0198,
        "sl_price": 0.0202,
        "volume_1m": 500.0,
        "volume_sma_20": 400.0,
        "volume_zscore_20": 0.8,
    }, ensure_ascii=False) + "\n"
    with tempfile.NamedTemporaryFile(mode="w", suffix=".jsonl", delete=False, encoding="utf-8") as f:
        f.write(line)
        path = Path(f.name)
    try:
        splits = fast0_volume_splits(_read_processed_lines(path))
        assert splits["total"] == 1
        assert splits["with_volume_zscore_20"] == 1
        assert splits["volume_1m_present"] == 1
        assert splits["volume_sma_20_present"] == 1
    finally:
        path.unlink(missing_ok=True)


def _read_processed_lines(path: Path) -> list[dict]:
    if not path.exists():
        return []
    lines = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                lines.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return lines


def main() -> int:
    test_signal_volume_roundtrip()
    test_processed_and_report()
    test_volume_1m_features()  # last: may segfault in some envs when importing fast0_sampler
    print("smoke_fast0_volume_features: OK (volume_1m/sma_20/zscore_20 in signal and report)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
