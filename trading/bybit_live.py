"""Bybit live broker: REST v5 private endpoints for linear USDT perpetuals."""
from __future__ import annotations

import hashlib
import hmac
import json
import logging
import os
import time
import uuid
from typing import Any, Optional

import requests

from trading.instrument import InstrumentLimits, round_qty_down, set_instrument_limits_override

logger = logging.getLogger(__name__)

BYBIT_MAINNET = "https://api.bybit.com"
BYBIT_TESTNET = "https://api-testnet.bybit.com"
CATEGORY = "linear"
RECV_WINDOW = "5000"


def _base_url() -> str:
    testnet = (os.getenv("BYBIT_TESTNET") or "false").strip().lower() in ("1", "true", "yes", "y")
    return BYBIT_TESTNET if testnet else BYBIT_MAINNET


def _sign(api_key: str, api_secret: str, timestamp: str, recv_window: str, payload: str) -> str:
    """HMAC SHA256 sign for Bybit v5. payload = queryString (GET) or jsonBody (POST)."""
    sign_str = f"{timestamp}{api_key}{recv_window}{payload}"
    sig = hmac.new(
        api_secret.encode("utf-8"),
        sign_str.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()
    return sig


def _request(
    method: str,
    path: str,
    api_key: str,
    api_secret: str,
    params: Optional[dict] = None,
    data: Optional[dict] = None,
) -> dict:
    base = _base_url()
    url = base + path
    timestamp = str(int(time.time() * 1000))
    headers = {
        "X-BAPI-API-KEY": api_key,
        "X-BAPI-TIMESTAMP": timestamp,
        "X-BAPI-RECV-WINDOW": RECV_WINDOW,
    }
    if method.upper() == "GET":
        params = params or {}
        qs = "&".join(f"{k}={v}" for k, v in sorted(params.items()))
        payload = qs
        headers["X-BAPI-SIGN"] = _sign(api_key, api_secret, timestamp, RECV_WINDOW, payload)
        r = requests.get(url, params=params, headers=headers, timeout=15)
    else:
        body = json.dumps(data) if data else "{}"
        headers["X-BAPI-SIGN"] = _sign(api_key, api_secret, timestamp, RECV_WINDOW, body)
        headers["Content-Type"] = "application/json"
        r = requests.post(url, data=body, headers=headers, timeout=15)
    r.raise_for_status()
    j = r.json()
    if j.get("retCode") != 0:
        raise RuntimeError(f"Bybit API error retCode={j.get('retCode')} retMsg={j.get('retMsg')}")
    return j


def _norm_symbol(symbol: str) -> str:
    return (symbol or "").strip().upper()


def side_to_position_idx(side: str) -> int:
    """
    Map side to Bybit positionIdx for hedge mode.
    LONG/Buy -> 1, SHORT/Sell -> 2.
    oneway uses 0 (caller handles).
    """
    s = (side or "").strip().upper()
    if s in ("LONG", "BUY"):
        return 1
    if s in ("SHORT", "SELL"):
        return 2
    return 2  # default SHORT for our strategies


class BybitLiveBroker:
    """
    Live execution via Bybit REST v5 (linear USDT perpetuals).
    Requires BYBIT_API_KEY, BYBIT_API_SECRET. BYBIT_TESTNET=true for testnet.
    BYBIT_POSITION_MODE: hedge | oneway | auto (default). Auto detects from position list.
    """

    def __init__(
        self,
        api_key: str,
        api_secret: str,
        dry_run: bool = False,
    ):
        self.api_key = api_key
        self.api_secret = api_secret
        self.dry_run = dry_run
        self._base = _base_url()
        self._position_mode: Optional[str] = None  # "hedge" | "oneway"
        _mode = (os.getenv("BYBIT_POSITION_MODE") or "auto").strip().lower()
        if _mode in ("hedge", "oneway"):
            self._position_mode = _mode
        logger.info(
            "LIVE_BROKER_ENABLED | base=%s testnet=%s dry_run=%s position_mode=%s",
            self._base,
            "testnet" in self._base,
            dry_run,
            self._position_mode or "auto",
        )

    def _ensure_position_mode(self, symbol: str) -> str:
        """Detect hedge vs oneway if auto. Returns 'hedge' or 'oneway'."""
        if self._position_mode:
            return self._position_mode
        try:
            params = {"category": CATEGORY, "symbol": _norm_symbol(symbol)}
            j = _request("GET", "/v5/position/list", self.api_key, self.api_secret, params=params)
            lst = j.get("result", {}).get("list", []) or []
            for pos in lst:
                idx = pos.get("positionIdx")
                try:
                    idx_i = int(idx) if idx is not None else -1
                except (TypeError, ValueError):
                    idx_i = -1
                if idx_i in (1, 2):
                    self._position_mode = "hedge"
                    logger.info("LIVE_BROKER_ENABLED | position_mode=hedge (auto-detected)")
                    return "hedge"
            if lst:
                self._position_mode = "oneway"
                logger.info("LIVE_BROKER_ENABLED | position_mode=oneway (auto-detected)")
                return "oneway"
            self._position_mode = "hedge"
            logger.info("LIVE_BROKER_ENABLED | position_mode=hedge (auto, no positions)")
            return "hedge"
        except Exception as e:
            logger.warning("LIVE_BROKER | position_mode auto-detect failed: %s, assuming hedge", e)
            self._position_mode = "hedge"
            return "hedge"

    def _get_position_idx(self, symbol: str, side: str) -> int:
        """Return positionIdx for orders: 0=oneway, 1=LONG, 2=SHORT."""
        mode = self._ensure_position_mode(symbol)
        if mode == "oneway":
            return 0
        return side_to_position_idx(side)

    def get_instrument(self, symbol: str) -> dict:
        """Fetch instrument info. Returns lotSizeFilter, priceFilter."""
        params = {"category": CATEGORY, "symbol": _norm_symbol(symbol)}
        j = _request("GET", "/v5/market/instruments-info", self.api_key, self.api_secret, params=params)
        lst = j.get("result", {}).get("list", [])
        if not lst:
            raise ValueError(f"Instrument not found: {symbol}")
        return lst[0]

    def get_instrument_limits(self, symbol: str) -> dict:
        """Return minQty, minNotional, lotStep, tickSize, qtyPrecision."""
        info = self.get_instrument(symbol)
        lot = info.get("lotSizeFilter", {}) or {}
        price = info.get("priceFilter", {}) or {}
        min_notional = float(lot.get("minNotionalValue") or 5)
        min_qty = float(lot.get("minOrderQty") or 0.001)
        qty_step = float(lot.get("qtyStep") or 0.001)
        tick_size = float(price.get("tickSize") or 0.01)
        qty_precision = max(0, len(str(qty_step).rstrip("0").split(".")[-1]))
        return {
            "min_qty": min_qty,
            "min_notional_usd": min_notional,
            "lot_step": qty_step,
            "tick_size": tick_size,
            "qty_precision": qty_precision,
        }

    def get_balance(self) -> float:
        """Wallet total equity (USD) for UNIFIED account."""
        params = {"accountType": "UNIFIED"}
        j = _request("GET", "/v5/account/wallet-balance", self.api_key, self.api_secret, params=params)
        lst = j.get("result", {}).get("list", [])
        if not lst:
            return 0.0
        eq = lst[0].get("totalEquity") or "0"
        equity = float(eq)
        logger.info("LIVE_BALANCE | totalEquity=%.2f", equity)
        return equity

    def set_leverage(self, symbol: str, leverage: int) -> None:
        """Set leverage for symbol. In hedge mode, sets both buy/sell to same value (idempotent)."""
        if self.dry_run:
            logger.info("LIVE_DRY_RUN | set_leverage symbol=%s leverage=%s (skipped)", symbol, leverage)
            return
        data = {
            "category": CATEGORY,
            "symbol": _norm_symbol(symbol),
            "buyLeverage": str(leverage),
            "sellLeverage": str(leverage),
        }
        _request("POST", "/v5/position/set-leverage", self.api_key, self.api_secret, data=data)

    def get_open_position(self, symbol: str) -> Optional[dict]:
        """Get open position for symbol. Returns {size, side, avgPrice} or None."""
        params = {"category": CATEGORY, "symbol": _norm_symbol(symbol)}
        j = _request("GET", "/v5/position/list", self.api_key, self.api_secret, params=params)
        lst = j.get("result", {}).get("list", []) or []
        for pos in lst:
            sz = float(pos.get("size") or 0)
            if sz > 0:
                logger.info(
                    "LIVE_POSITION | symbol=%s side=%s size=%s avgPrice=%s",
                    symbol, pos.get("side"), sz, pos.get("avgPrice"),
                )
                return {
                    "size": sz,
                    "side": pos.get("side", ""),
                    "avgPrice": float(pos.get("avgPrice") or 0),
                }
        return None

    def place_market_order(
        self,
        symbol: str,
        side: str,
        qty: float,
        reduce_only: bool = False,
        order_link_id: Optional[str] = None,
        position_idx: Optional[int] = None,
    ) -> Optional[dict]:
        """Place market order. positionIdx: 0=oneway, 1=LONG, 2=SHORT. Auto-computed if None."""
        if self.dry_run:
            logger.info(
                "LIVE_DRY_RUN | place_market_order symbol=%s side=%s qty=%s (skipped)",
                symbol, side, qty,
            )
            return {"orderId": "dry-run", "orderLinkId": order_link_id or "dry-run"}
        side_up = (side or "").strip().capitalize()
        if side_up.upper() == "SHORT":
            side_up = "Sell"
        elif side_up.upper() == "LONG":
            side_up = "Buy"
        idx = position_idx if position_idx is not None else self._get_position_idx(symbol, side)
        data = {
            "category": CATEGORY,
            "symbol": _norm_symbol(symbol),
            "side": side_up,
            "orderType": "Market",
            "qty": str(qty),
            "reduceOnly": reduce_only,
            "positionIdx": idx,
        }
        if order_link_id:
            data["orderLinkId"] = order_link_id
        try:
            j = _request("POST", "/v5/order/create", self.api_key, self.api_secret, data=data)
            res = j.get("result", {})
            logger.info(
                "LIVE_ORDER_PLACE | symbol=%s side=%s qty=%s orderId=%s positionIdx=%s",
                symbol, side_up, qty, res.get("orderId"), idx,
            )
            return res
        except Exception as e:
            logger.warning(
                "LIVE_ORDER_REJECT | symbol=%s side=%s qty=%s positionIdx=%s reason=%s",
                symbol, side, qty, idx, e,
            )
            raise

    def place_limit_order(
        self,
        symbol: str,
        side: str,
        qty: float,
        price: float,
        reduce_only: bool = False,
        post_only: bool = False,
        order_link_id: Optional[str] = None,
        position_idx: Optional[int] = None,
    ) -> Optional[dict]:
        """Place limit order. positionIdx: 0=oneway, 1=LONG, 2=SHORT. Auto-computed if None."""
        if self.dry_run:
            logger.info(
                "LIVE_DRY_RUN | place_limit_order symbol=%s side=%s qty=%s price=%s (skipped)",
                symbol, side, qty, price,
            )
            return {"orderId": "dry-run", "orderLinkId": order_link_id or "dry-run"}
        side_up = (side or "").strip().capitalize()
        if side_up.upper() == "SHORT":
            side_up = "Sell"
        elif side_up.upper() == "LONG":
            side_up = "Buy"
        idx = position_idx if position_idx is not None else self._get_position_idx(symbol, side)
        data = {
            "category": CATEGORY,
            "symbol": _norm_symbol(symbol),
            "side": side_up,
            "orderType": "Limit",
            "qty": str(qty),
            "price": str(price),
            "timeInForce": "PostOnly" if post_only else "GTC",
            "reduceOnly": reduce_only,
            "positionIdx": idx,
        }
        if order_link_id:
            data["orderLinkId"] = order_link_id
        try:
            j = _request("POST", "/v5/order/create", self.api_key, self.api_secret, data=data)
            res = j.get("result", {})
            logger.info(
                "LIVE_ORDER_PLACE | symbol=%s side=%s qty=%s price=%s orderId=%s positionIdx=%s",
                symbol, side_up, qty, price, res.get("orderId"), idx,
            )
            return res
        except Exception as e:
            logger.warning(
                "LIVE_ORDER_REJECT | symbol=%s side=%s qty=%s price=%s positionIdx=%s reason=%s",
                symbol, side, qty, price, idx, e,
            )
            raise

    def cancel_order(self, symbol: str, order_id: str) -> None:
        """Cancel order by orderId."""
        if self.dry_run:
            logger.info("LIVE_DRY_RUN | cancel_order symbol=%s orderId=%s (skipped)", symbol, order_id)
            return
        data = {"category": CATEGORY, "symbol": _norm_symbol(symbol), "orderId": order_id}
        _request("POST", "/v5/order/cancel", self.api_key, self.api_secret, data=data)

    def open_position(
        self,
        signal: Any,
        qty_notional_usd: float,
        risk_usd: float,
        leverage: int,
        opened_ts: str,
    ) -> Optional[dict]:
        """
        Open position via market order. Implements ExecutionAdapter.
        Returns position dict or None (rejected).
        """
        from trading.broker import allow_entry

        allowed, reason = allow_entry(signal)
        if not allowed:
            logger.info(
                "LIVE_ORDER_REJECT | strategy=%s symbol=%s reason=%s",
                getattr(signal, "strategy", ""),
                getattr(signal, "symbol", ""),
                reason,
            )
            return None

        symbol = _norm_symbol(getattr(signal, "symbol", "") or "")
        side = (getattr(signal, "side", None) or "SHORT").strip().upper()
        position_idx = self._get_position_idx(symbol, side)
        entry_price = float(signal.entry_price or 0)
        tp = float(signal.tp_price or 0)
        sl = float(signal.sl_price or 0)
        if entry_price <= 0 or tp <= 0 or sl <= 0:
            logger.info("LIVE_ORDER_REJECT | symbol=%s reason=missing_entry_tp_sl", symbol)
            return None

        limits = self.get_instrument_limits(symbol)
        ilim = InstrumentLimits(
            min_qty=limits["min_qty"],
            min_notional_usd=limits["min_notional_usd"],
            lot_step=limits["lot_step"],
            qty_precision=limits["qty_precision"],
        )
        set_instrument_limits_override(symbol, ilim)

        raw_qty = qty_notional_usd / entry_price if entry_price > 0 else 0
        qty = round_qty_down(raw_qty, limits["lot_step"], limits["qty_precision"])
        if qty < limits["min_qty"]:
            logger.info(
                "LIVE_ORDER_REJECT | symbol=%s positionIdx=%s reason=qty_below_min qty=%.6f minQty=%.6f",
                symbol, position_idx, qty, limits["min_qty"],
            )
            return None
        notional = qty * entry_price
        if notional < limits["min_notional_usd"]:
            logger.info(
                "LIVE_ORDER_REJECT | symbol=%s positionIdx=%s reason=notional_below_min notional=%.2f minNotional=%.2f",
                symbol, position_idx, notional, limits["min_notional_usd"],
            )
            return None

        self.set_leverage(symbol, leverage)
        if self.dry_run:
            logger.info("LIVE_DRY_RUN | place_market_order skipped | symbol=%s side=%s qty=%s", symbol, side, qty)
            return None
        try:
            self.place_market_order(symbol, side, qty, reduce_only=False, position_idx=position_idx)
        except Exception as e:
            logger.warning("LIVE_ORDER_REJECT | symbol=%s positionIdx=%s reason=%s", symbol, position_idx, e)
            return None

        trade_id = str(uuid.uuid4())
        position = {
            "strategy": getattr(signal, "strategy", ""),
            "symbol": symbol,
            "side": side,
            "entry": entry_price,
            "sl": sl,
            "tp": tp,
            "opened_ts": opened_ts,
            "notional_usd": notional,
            "risk_usd": risk_usd,
            "leverage": leverage,
            "status": "open",
            "run_id": getattr(signal, "run_id", "") or "",
            "event_id": getattr(signal, "event_id", "") or "",
            "trade_id": trade_id,
            "mode": "live",
        }
        return position
