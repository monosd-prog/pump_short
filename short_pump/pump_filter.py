# short_pump/pump_filter.py
from typing import Any, Dict, Optional, Tuple

from short_pump.config import Config


def validate_pump_event(
    *,
    cfg: Config,
    symbol: str,
    exchange: Optional[str],
    pump_pct: Optional[float],
    pump_ts: Optional[str],
    extra: Optional[Dict[str, Any]],
) -> Tuple[bool, Optional[Dict[str, Any]]]:
    """
    Returns:
      (ok, ignored_response)
      - if ok == True => event passes filters
      - if ok == False => ignored_response is JSON dict to return from endpoint
    """
    # 1) Exchange filter
    ex = (exchange or "bybit").lower()
    if ex != "bybit":
        return False, {"status": "ignored", "reason": "exchange_not_bybit", "symbol": symbol}

    # 2) Pump pct filter
    if pump_pct is not None:
        try:
            pp = float(pump_pct)
        except Exception:
            pp = None

        if pp is not None and pp < cfg.min_pump_pct:
            return False, {
                "status": "ignored",
                "reason": "pump_pct_too_small",
                "symbol": symbol,
                "pump_pct": pp,
            }

    # 3) Optional require 10m window
    if cfg.require_10m_window:
        exx = extra or {}
        tf = str(exx.get("tf", "")).lower()
        win = exx.get("window_minutes")

        ok_10m = False
        if tf in ("10m", "10min", "10"):
            ok_10m = True
        if isinstance(win, (int, float)) and int(win) == 10:
            ok_10m = True

        if not ok_10m:
            return False, {"status": "ignored", "reason": "not_10m_window", "symbol": symbol}

    return True, None