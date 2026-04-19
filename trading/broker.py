"""Execution adapter: paper (simulated fills + gating) vs live (exchange orders)."""
from __future__ import annotations

import logging
import os
import uuid
from abc import ABC, abstractmethod
from typing import Any, Optional, Tuple

logger = logging.getLogger(__name__)

# Defaults for paper simulation (config can override)
DEFAULT_PAPER_FEE_BPS = 6
DEFAULT_PAPER_SLIPPAGE_BPS = 2
DEFAULT_DIST_MIN = 3.5


def _dist_min() -> float:
    try:
        v = os.getenv("SHORT_PUMP_AUTO_DIST_MIN") or os.getenv("TG_ENTRY_DIST_MIN") or os.getenv("TG_DIST_TO_PEAK_MIN", "3.5")
        return float(str(v).replace(",", "."))
    except (TypeError, ValueError):
        return DEFAULT_DIST_MIN


def _fast0_liq_min_usd() -> float:
    try:
        v = os.getenv("FAST0_LIQ_MIN_USD", "5000")
        return float(str(v).replace(",", "."))
    except (TypeError, ValueError):
        return 5000.0


def _fast0_liq_max_usd() -> float:
    try:
        v = os.getenv("FAST0_LIQ_MAX_USD", "25000")
        return float(str(v).replace(",", "."))
    except (TypeError, ValueError):
        return 25000.0


def _fast0_filtered_dist_max() -> float:
    try:
        v = os.getenv("SHORT_PUMP_FAST0_FILTERED_DIST_MAX", "1.0")
        return float(str(v).replace(",", "."))
    except (TypeError, ValueError):
        return 1.0


def _fast0_filtered_enabled() -> bool:
    return (os.getenv("SHORT_PUMP_FAST0_FILTERED_ENABLE", "0") or "").strip().lower() in ("1", "true", "yes", "y", "on")


def allow_entry_short_pump(signal: Any) -> Tuple[bool, str]:
    """
    Variant #3: short_pump — allow only if stage==4 and dist_to_peak_pct >= 3.5.
    Returns (allowed, reason).
    """
    stage = getattr(signal, "stage", None)
    dist = getattr(signal, "dist_to_peak_pct", None)
    if stage is None:
        return False, "missing stage"
    try:
        stage_i = int(stage)
    except (TypeError, ValueError):
        return False, f"invalid stage={stage!r}"
    if stage_i != 4:
        return False, f"stage={stage_i} (require 4)"
    if dist is None:
        return False, "missing dist_to_peak_pct"
    try:
        dist_f = float(dist)
    except (TypeError, ValueError):
        return False, f"invalid dist_to_peak_pct={dist!r}"
    if not (dist_f >= _dist_min()):
        return False, f"dist_to_peak_pct={dist_f:.2f} < {_dist_min()}"
    return True, ""


def allow_entry_false_pump(signal: Any) -> Tuple[bool, str]:
    """
    false_pump — allow if stage==4.
    dist_to_peak_pct не проверяем жёстко т.к. стратегия ищет лже-памп
    У ВЕРШИНЫ (near_top=True), поэтому dist может быть < 3.5%.
    Достаточно stage==4 и dist_to_peak_pct не None.
    """
    stage = getattr(signal, "stage", None)
    if stage is None:
        return False, "missing stage"
    try:
        stage_i = int(stage)
    except (TypeError, ValueError):
        return False, f"invalid stage={stage!r}"
    if stage_i != 4:
        return False, f"stage={stage_i} (require 4)"
    dist = getattr(signal, "dist_to_peak_pct", None)
    if dist is None:
        return False, "missing dist_to_peak_pct"
    return True, "ok"


def allow_entry_short_pump_filtered(signal: Any) -> Tuple[bool, str]:
    """short_pump_filtered — allow only if enabled, stage==4 and dist_to_peak_pct <= threshold."""
    from short_pump.rollout import SHORT_PUMP_FILTERED_DIST_TO_PEAK_MAX, SHORT_PUMP_FILTERED_ENABLE

    if not SHORT_PUMP_FILTERED_ENABLE:
        return False, "SHORT_PUMP_FILTERED_ENABLE=0"
    stage = getattr(signal, "stage", None)
    dist = getattr(signal, "dist_to_peak_pct", None)
    if stage is None:
        return False, "missing stage"
    try:
        stage_i = int(stage)
    except (TypeError, ValueError):
        return False, f"invalid stage={stage!r}"
    if stage_i != 4:
        return False, f"stage={stage_i} (require 4)"
    if dist is None:
        return False, "missing dist_to_peak_pct"
    try:
        dist_f = float(dist)
    except (TypeError, ValueError):
        return False, f"invalid dist_to_peak_pct={dist!r}"
    if not (dist_f <= SHORT_PUMP_FILTERED_DIST_TO_PEAK_MAX):
        return False, f"dist_to_peak_pct={dist_f:.2f} > {SHORT_PUMP_FILTERED_DIST_TO_PEAK_MAX}"
    return True, ""


def allow_entry_short_pump_fast0(signal: Any) -> Tuple[bool, str]:
    """
    short_pump_fast0 — allow only if dist<=1.5 AND liq in [0, (5k,25k], >100k].
    Risk: liq==0->1R, 5k<liq<=25k->1.5R, liq>100k->2R.
    """
    from trading.risk_profile import FAST0_AUTO_ENABLE, get_risk_profile, is_fast0_entry_allowed
    if not FAST0_AUTO_ENABLE:
        return False, "FAST0_AUTO_ENABLE=0"
    liq = getattr(signal, "liq_long_usd_30s", None)
    dist = getattr(signal, "dist_to_peak_pct", None)
    if liq is None:
        return False, "missing liq_long_usd_30s"
    allowed, reason = is_fast0_entry_allowed(liq, dist)
    if not allowed:
        return False, reason or "no matching risk profile"
    profile, risk_mult, _ = get_risk_profile(
        "short_pump_fast0",
        dist_to_peak_pct=dist,
        liq_long_usd_30s=liq,
        event_id=getattr(signal, "event_id", "") or "",
        trade_id=getattr(signal, "trade_id", "") or "",
        symbol=getattr(signal, "symbol", "") or "",
    )
    if not profile or risk_mult <= 0:
        return False, reason or "no matching risk profile"
    return True, ""


def allow_entry_short_pump_fast0_filtered(signal: Any) -> Tuple[bool, str]:
    """short_pump_fast0_filtered — allow only if enabled, stage/liq match fast0 and dist <= tighter filter."""
    from trading.risk_profile import FAST0_AUTO_ENABLE, get_risk_profile, is_fast0_entry_allowed
    if not FAST0_AUTO_ENABLE:
        return False, "FAST0_AUTO_ENABLE=0"
    if not _fast0_filtered_enabled():
        return False, "SHORT_PUMP_FAST0_FILTERED_ENABLE=0"
    liq = getattr(signal, "liq_long_usd_30s", None)
    dist = getattr(signal, "dist_to_peak_pct", None)
    if liq is None:
        return False, "missing liq_long_usd_30s"
    allowed, reason = is_fast0_entry_allowed(liq, dist)
    if not allowed:
        return False, reason
    try:
        dist_f = float(dist) if dist is not None else None
    except (TypeError, ValueError):
        dist_f = None
    if dist_f is None:
        return False, "missing dist_to_peak_pct"
    if dist_f > _fast0_filtered_dist_max():
        return False, f"dist_to_peak_pct={dist_f:.2f} > {_fast0_filtered_dist_max()}"
    profile, risk_mult, _ = get_risk_profile(
        "short_pump_fast0_filtered",
        dist_to_peak_pct=dist,
        liq_long_usd_30s=liq,
        event_id=getattr(signal, "event_id", "") or "",
        trade_id=getattr(signal, "trade_id", "") or "",
        symbol=getattr(signal, "symbol", "") or "",
    )
    if not profile or risk_mult <= 0:
        return False, reason or "no matching risk profile"
    return True, ""


def allow_entry(signal: Any) -> Tuple[bool, str]:
    """Strategy-specific gating. Returns (allowed, reason). Unknown strategy -> reject."""
    strategy = (getattr(signal, "strategy", None) or "").strip()
    if strategy == "short_pump":
        return allow_entry_short_pump(signal)
    if strategy == "false_pump":
        return allow_entry_false_pump(signal)
    if strategy == "short_pump_filtered":
        return allow_entry_short_pump_filtered(signal)
    if strategy == "short_pump_premium":
        return allow_entry_short_pump(signal)
    if strategy == "short_pump_wick":
        return allow_entry_short_pump(signal)
    if strategy == "short_pump_fast0":
        return allow_entry_short_pump_fast0(signal)
    if strategy == "short_pump_fast0_filtered":
        return allow_entry_short_pump_fast0_filtered(signal)
    return False, f"unknown strategy={strategy!r}"


class ExecutionAdapter(ABC):
    """Interface for order execution (paper or live)."""

    @abstractmethod
    def open_position(
        self,
        signal: Any,
        qty_notional_usd: float,
        risk_usd: float,
        leverage: int,
        opened_ts: str,
    ) -> Optional[dict[str, Any]]:
        """
        Open a position from signal. Returns position dict or None (rejected / not filled).
        Position must include: strategy, symbol, side, entry, sl, tp, opened_ts, notional_usd, risk_usd, leverage, status, run_id, event_id.
        """
        pass


class PaperBroker(ExecutionAdapter):
    """
    Paper execution: gating (Variant #3) + simulated fills with fee/slippage.
    Entry fill: mid * (1 ± slippage_bps), direction-aware. TP/SL from signal. TIMEOUT handled by caller at mid ± slippage.
    """

    def __init__(
        self,
        fee_bps: int = DEFAULT_PAPER_FEE_BPS,
        slippage_bps: int = DEFAULT_PAPER_SLIPPAGE_BPS,
        log_rejected: bool = True,
        log_accepted: bool = True,
    ):
        self.fee_bps = fee_bps
        self.slippage_bps = slippage_bps
        self.log_rejected = log_rejected
        self.log_accepted = log_accepted

    def _entry_fill_price(self, mid: float, side: str) -> float:
        """SHORT: worse entry = higher = mid * (1 + slippage). LONG: worse = lower = mid * (1 - slippage)."""
        mult = 1.0 + (self.slippage_bps / 10000.0) if (side or "SHORT").strip().upper() == "SHORT" else 1.0 - (self.slippage_bps / 10000.0)
        return mid * mult

    def open_position(
        self,
        signal: Any,
        qty_notional_usd: float,
        risk_usd: float,
        leverage: int,
        opened_ts: str,
    ) -> Optional[dict[str, Any]]:
        allowed, reason = allow_entry(signal)
        if not allowed:
            if self.log_rejected:
                logger.info(
                    "PAPER_REJECTED | strategy=%s symbol=%s time=%s reason=%s stage=%s dist=%s liq_long_usd_30s=%s",
                    getattr(signal, "strategy", ""),
                    getattr(signal, "symbol", ""),
                    getattr(signal, "ts_utc", ""),
                    reason,
                    getattr(signal, "stage", None),
                    getattr(signal, "dist_to_peak_pct", None),
                    getattr(signal, "liq_long_usd_30s", None),
                )
            return None

        entry_mid = float(signal.entry_price) if signal.entry_price is not None else 0.0
        tp = float(signal.tp_price) if signal.tp_price is not None else 0.0
        sl = float(signal.sl_price) if signal.sl_price is not None else 0.0
        if entry_mid <= 0 or tp <= 0 or sl <= 0:
            if self.log_rejected:
                logger.info("PAPER_REJECTED | strategy=%s symbol=%s reason=missing_or_invalid_entry_tp_sl", getattr(signal, "strategy", ""), getattr(signal, "symbol", ""))
            return None

        side = (getattr(signal, "side", None) or "SHORT").strip().upper()
        entry_fill = self._entry_fill_price(entry_mid, side)
        from trading.state import make_position_id
        strategy = getattr(signal, "strategy", "") or ""
        run_id = getattr(signal, "run_id", "") or ""
        event_id = str(getattr(signal, "event_id", "") or "")
        symbol = getattr(signal, "symbol", "") or ""
        trade_id = make_position_id(strategy, run_id, event_id, symbol)
        position = {
            "strategy": getattr(signal, "strategy", ""),
            "symbol": getattr(signal, "symbol", ""),
            "side": side,
            "entry": entry_fill,
            "sl": sl,
            "tp": tp,
            "opened_ts": opened_ts,
            "notional_usd": qty_notional_usd,
            "risk_usd": risk_usd,
            "leverage": leverage,
            "status": "open",
            "run_id": getattr(signal, "run_id", "") or "",
            "event_id": getattr(signal, "event_id", "") or "",
            "trade_id": trade_id,
            "mode": "live",
        }
        if self.log_accepted:
            logger.info(
                "PAPER_ACCEPTED | trade_id=%s strategy=%s symbol=%s entry_mid=%.4f entry_fill=%.4f tp=%.4f sl=%.4f",
                trade_id, position["strategy"], position["symbol"], entry_mid, entry_fill, tp, sl,
            )
        return position


def get_broker(mode: str, dry_run_live: bool = False) -> ExecutionAdapter:
    """Factory: mode 'live' -> BybitLiveBroker, else PaperBroker with config."""
    if (mode or "").strip().lower() == "live":
        try:
            from trading.bybit_live import BybitLiveBroker
            api_key = (os.getenv("BYBIT_API_KEY") or "").strip()
            api_secret = (os.getenv("BYBIT_API_SECRET") or "").strip()
            if not api_key or not api_secret:
                logger.error("LIVE_BROKER_ENABLED=false | BYBIT_API_KEY or BYBIT_API_SECRET missing")
                raise ValueError("BYBIT_API_KEY and BYBIT_API_SECRET required for live mode")
            return BybitLiveBroker(api_key=api_key, api_secret=api_secret, dry_run=dry_run_live)
        except Exception as e:
            logger.exception("get_broker live failed: %s", e)
            raise
    fee_bps = int(os.getenv("PAPER_FEE_BPS", str(DEFAULT_PAPER_FEE_BPS)))
    slippage_bps = int(os.getenv("PAPER_SLIPPAGE_BPS", str(DEFAULT_PAPER_SLIPPAGE_BPS)))
    return PaperBroker(fee_bps=fee_bps, slippage_bps=slippage_bps)
