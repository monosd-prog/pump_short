"""
Historical replay / lab framework.

This package is intentionally isolated from live/paper execution code paths.
It builds canonical event rows using common feature layers and writes datasets into mode=lab.

LAB MODE V1 is candle-only:
- honest: dist_to_peak_pct, volume_* from klines, candle-based outcomes
- not honest without extra sources: delta/cvd/liq/oi/funding/microstructure, true watcher stage/context_score
"""

