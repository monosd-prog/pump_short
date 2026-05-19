"""Short pump strategy — pilot: short_pump_mid only (Step 3.1)."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional

from pump_v2.core.market_context import MarketContext
from pump_v2.core.strategy_base import Signal, Strategy
from pump_v2.indicators.context_score_5m import ContextScore5m

# v1 defaults (trading/config.py LIVE_* + short_pump/config tp/sl confirm) — not imported
_LIVE_FIXED_NOTIONAL_USD = 10.0
_LIVE_LEVERAGE = 4
_TP_PCT_CONFIRM = 0.006
_SL_PCT_CONFIRM = 0.004
_SHORT_PUMP_MID_RISK_MULT = 0.7
_TRADEABLE_DIST_MIN = 3.5


@dataclass
class ShortPumpProfile:
    """Результат classify_profile — всё что нужно для Signal."""

    name: str
    notional_usd: float
    leverage: int
    tp_pct: float
    sl_pct: float
    risk_mult: float


class ShortPumpStrategy(Strategy):
    name = "short_pump"

    def required_indicators(self) -> list[str]:
        return ["dbg5_builder", "context_score_5m"]

    def check_signal(self, ctx: MarketContext) -> Optional[Signal]:
        dbg5 = ctx.dbg5
        if dbg5 is None:
            return None

        stage = int(dbg5.stage)
        dist_to_peak_pct = float(dbg5.dist_to_peak_pct)

        if stage != 4 or dist_to_peak_pct < _TRADEABLE_DIST_MIN:
            return None

        context_score = self._read_context_score(ctx)
        if context_score is None:
            return None

        profile = self._classify_profile(stage, dist_to_peak_pct, context_score)
        if profile is None:
            return None

        entry_price = self._last_close_5m(ctx)
        if entry_price is None or entry_price <= 0:
            return None

        tp_price = entry_price * (1.0 - profile.tp_pct)
        sl_price = entry_price * (1.0 + profile.sl_pct)

        return Signal(
            strategy=self.name,
            symbol=ctx.symbol,
            side="short",
            entry_price=entry_price,
            sl_price=sl_price,
            tp_price=tp_price,
            notional_usd=profile.notional_usd,
            leverage=profile.leverage,
            ts_utc=ctx.ts_utc,
            metadata={
                "risk_profile": profile.name,
                "stage": stage,
                "dist_to_peak_pct": dist_to_peak_pct,
                "context_score": context_score,
                "risk_mult": profile.risk_mult,
            },
        )

    def _read_context_score(self, ctx: MarketContext) -> Optional[float]:
        raw = ctx.indicators.get("context_score_5m")
        if raw is None:
            return None
        if isinstance(raw, ContextScore5m):
            return float(raw.score)
        if isinstance(raw, dict):
            try:
                return float(raw["score"])
            except (KeyError, TypeError, ValueError):
                return None
        return None

    def _last_close_5m(self, ctx: MarketContext) -> Optional[float]:
        candles = ctx.candles.get("5m")
        if not candles:
            return None
        last: Any = candles[-1]
        if isinstance(last, dict):
            val = last.get("close", last.get("price"))
        else:
            val = getattr(last, "close", None) or getattr(last, "price", None)
        if val is None:
            return None
        try:
            return float(val)
        except (TypeError, ValueError):
            return None

    def _classify_profile(
        self,
        stage: int,
        dist_to_peak_pct: float,
        context_score: float,
    ) -> Optional[ShortPumpProfile]:
        if (
            stage in (3, 4)
            and 3.5 <= dist_to_peak_pct < 5.0
            and 0.4 <= context_score < 0.6
        ):
            return ShortPumpProfile(
                name="short_pump_mid",
                notional_usd=_LIVE_FIXED_NOTIONAL_USD * _SHORT_PUMP_MID_RISK_MULT,
                leverage=_LIVE_LEVERAGE,
                tp_pct=_TP_PCT_CONFIRM,
                sl_pct=_SL_PCT_CONFIRM,
                risk_mult=_SHORT_PUMP_MID_RISK_MULT,
            )

        if (
            7.5 <= dist_to_peak_pct < 10.0
            and 0.4 <= context_score < 0.6
        ):
            raise NotImplementedError("Phase 3.3+")

        return None
