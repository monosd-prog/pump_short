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
        v = os.getenv("TG_ENTRY_DIST_MIN") or os.getenv("TG_DIST_TO_PEAK_MIN", "3.5")
        return float(str(v).replace(",", "."))
    except (TypeError, ValueError):
        return DEFAULT_DIST_MIN


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


def allow_entry_short_pump_fast0(signal: Any) -> Tuple[bool, str]:
    """
    Variant #3: short_pump_fast0 — allow only if liq_long_usd_30s > 0.
    Returns (allowed, reason).
    """
    liq = getattr(signal, "liq_long_usd_30s", None)
    if liq is None:
        return False, "missing liq_long_usd_30s"
    try:
        liq_f = float(liq)
    except (TypeError, ValueError):
        return False, f"invalid liq_long_usd_30s={liq!r}"
    if not (liq_f > 0):
        return False, f"liq_long_usd_30s={liq_f} (require > 0)"
    return True, ""


def allow_entry(signal: Any) -> Tuple[bool, str]:
    """Strategy-specific gating. Returns (allowed, reason). Unknown strategy -> reject."""
    strategy = (getattr(signal, "strategy", None) or "").strip()
    if strategy == "short_pump":
        return allow_entry_short_pump(signal)
    if strategy == "short_pump_fast0":
        return allow_entry_short_pump_fast0(signal)
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
        trade_id = str(uuid.uuid4())
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
