"""Adapt pump_v2 Signal → v1-compatible queue dict."""
from __future__ import annotations

import uuid
from typing import Any, Dict

from pump_v2.core.market_context import MarketContext
from pump_v2.core.strategy_base import Signal


def v2_signal_to_queue_dict(
    signal: Signal,
    ctx: MarketContext,
) -> Dict[str, Any]:
    """
    Convert v2 Signal to dict for trading.queue.enqueue_signal_dict().

    Required for runner: strategy, symbol, side, ts_utc, run_id, event_id,
    entry_price, tp_price, sl_price, tp_pct, sl_pct,
    stage, dist_to_peak_pct, context_score.
    """
    meta = signal.metadata
    ts = signal.ts_utc
    if ts.tzinfo is None:
        ts_str = ts.strftime("%Y-%m-%dT%H:%M:%S+00:00")
    else:
        ts_str = ts.strftime("%Y-%m-%dT%H:%M:%S+00:00")

    uid = str(uuid.uuid4())[:8]
    run_id = f"v2_{signal.symbol}_{uid}"
    event_id = f"v2_{ts.strftime('%Y%m%d_%H%M%S')}_{signal.symbol}"

    entry = signal.entry_price
    tp_pct = abs(entry - signal.tp_price) / entry if entry else 0.006
    sl_pct = abs(signal.sl_price - entry) / entry if entry else 0.004

    liq_long = ctx.indicators.get("liq_long_usd_30s")
    liq_short = None
    liq_feats = ctx.indicators.get("liquidation_features") or {}
    if isinstance(liq_feats, dict):
        liq_short = liq_feats.get("liq_short_usd_30s")

    return {
        "strategy": "short_pump",
        "symbol": signal.symbol,
        "side": (signal.side or "short").upper(),
        "ts_utc": ts_str,
        "run_id": run_id,
        "event_id": event_id,
        "source": "pump_v2",
        "entry_price": signal.entry_price,
        "tp_price": signal.tp_price,
        "sl_price": signal.sl_price,
        "tp_pct": round(tp_pct, 6),
        "sl_pct": round(sl_pct, 6),
        "stage": meta.get("stage", 0),
        "dist_to_peak_pct": meta.get("dist_to_peak_pct", 0.0),
        "context_score": meta.get("context_score", 0.0),
        "funding_rate_abs": meta.get("funding_rate_abs"),
        "liq_long_usd_30s": liq_long,
        "liq_short_usd_30s": liq_short,
        "cvd_30s": 0.0,
        "cvd_1m": 0.0,
        "volume_1m": 0.0,
        "volume_sma_20": 0.0,
        "volume_zscore_20": 0.0,
    }
