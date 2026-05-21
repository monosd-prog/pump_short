"""Build MarketContext from v1 watcher locals for pre-run."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from pump_v2.core.market_context import MarketContext
from pump_v2.indicators.context_score_5m import ContextScore5m
from pump_v2.indicators.dbg5_builder import Dbg5Bundle


def build_market_context_from_watcher(
    symbol: str,
    candles_5m: List[Dict[str, Any]],
    oi_dict: Optional[Dict[str, Any]],
    funding_rate: float,
    dbg5: Dict[str, Any],
    context_score: float,
    ctx_parts: Dict[str, float],
) -> MarketContext:
    _ = oi_dict
    oi_change = dbg5.get("oi_change_5m_pct")
    oi_change_5m_pct = float(oi_change) if oi_change is not None else None

    bundle = Dbg5Bundle(
        stage=int(dbg5.get("stage", 0)),
        dist_to_peak_pct=float(dbg5.get("dist_to_peak_pct", 0.0)),
        oi_change_5m_pct=oi_change_5m_pct,
        oi_divergence_5m=bool(dbg5.get("oi_divergence_5m", False)),
        vol_z=float(dbg5.get("vol_z", 0.0)),
        atr_14_5m_pct=float(dbg5.get("atr_14_5m_pct", 0.0)),
    )

    ctx = MarketContext(
        symbol=symbol,
        ts_utc=datetime.now(timezone.utc),
        funding=funding_rate,
        dbg5=bundle,
    )
    ctx.candles["5m"] = candles_5m
    ctx.indicators["context_score_5m"] = ContextScore5m(
        score=float(context_score),
        parts=dict(ctx_parts),
    )
    return ctx
