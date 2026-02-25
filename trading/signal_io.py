"""Serialize/deserialize Signal for queue and runner. Contract: short_pump.signals.Signal."""
from __future__ import annotations

import json
from typing import Any

from short_pump.signals import Signal


def signal_to_dict(signal: Signal) -> dict[str, Any]:
    """Signal -> JSON-serializable dict (primitives only; extras omitted for safety)."""
    return {
        "strategy": signal.strategy,
        "symbol": signal.symbol,
        "side": signal.side or "",
        "ts_utc": signal.ts_utc or "",
        "run_id": signal.run_id or "",
        "event_id": str(signal.event_id) if signal.event_id is not None else "",
        "entry_price": signal.entry_price,
        "tp_price": signal.tp_price,
        "sl_price": signal.sl_price,
        "tp_pct": signal.tp_pct,
        "sl_pct": signal.sl_pct,
        "stage": signal.stage,
        "dist_to_peak_pct": signal.dist_to_peak_pct,
        "context_score": signal.context_score,
        "cvd_30s": signal.cvd_30s,
        "cvd_1m": signal.cvd_1m,
        "liq_long_usd_30s": signal.liq_long_usd_30s,
        "liq_short_usd_30s": signal.liq_short_usd_30s,
    }


def signal_from_dict(data: dict[str, Any]) -> Signal:
    """Dict (from JSON) -> Signal. extras left empty."""
    return Signal(
        strategy=str(data.get("strategy", "")),
        symbol=str(data.get("symbol", "")),
        side=str(data.get("side", "")),
        ts_utc=str(data.get("ts_utc", "")),
        run_id=str(data.get("run_id", "")),
        event_id=data.get("event_id") or None,
        entry_price=float(data["entry_price"]) if data.get("entry_price") is not None else None,
        tp_price=float(data["tp_price"]) if data.get("tp_price") is not None else None,
        sl_price=float(data["sl_price"]) if data.get("sl_price") is not None else None,
        tp_pct=float(data["tp_pct"]) if data.get("tp_pct") is not None else None,
        sl_pct=float(data["sl_pct"]) if data.get("sl_pct") is not None else None,
        stage=int(data["stage"]) if data.get("stage") is not None else None,
        dist_to_peak_pct=float(data["dist_to_peak_pct"]) if data.get("dist_to_peak_pct") is not None else None,
        context_score=float(data["context_score"]) if data.get("context_score") is not None else None,
        cvd_30s=float(data["cvd_30s"]) if data.get("cvd_30s") is not None else None,
        cvd_1m=float(data["cvd_1m"]) if data.get("cvd_1m") is not None else None,
        liq_long_usd_30s=float(data["liq_long_usd_30s"]) if data.get("liq_long_usd_30s") is not None else None,
        liq_short_usd_30s=float(data["liq_short_usd_30s"]) if data.get("liq_short_usd_30s") is not None else None,
        extras={},
    )
