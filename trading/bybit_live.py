"""Bybit live broker: REST v5 private endpoints for linear USDT perpetuals."""
from __future__ import annotations

import hashlib
import hmac
import json
import logging
import os
import time
import uuid
from typing import Any, Optional, Set

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
    allow_ret_codes: Optional[Set[int]] = None,
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
    ret_code = j.get("retCode")
    if ret_code != 0:
        if allow_ret_codes and ret_code in allow_ret_codes:
            return j
        raise RuntimeError(f"Bybit API error retCode={ret_code} retMsg={j.get('retMsg')}")
    return j


def _norm_symbol(symbol: str) -> str:
    return (symbol or "").strip().upper()


def _round_price_to_tick(price: float, tick_size: float) -> float:
    """Round price to valid tick step. tick_size fallback 0.0001."""
    ts = tick_size if tick_size and tick_size > 0 else 0.0001
    return round(price / ts) * ts


def _tpsl_buffer(price: float, tick_size: float, min_pct: float = 0.0005) -> float:
    """Safety buffer: max(3 ticks, price * min_pct)."""
    ts = tick_size if tick_size and tick_size > 0 else 0.0001
    buf_ticks = ts * 3
    buf_pct = price * min_pct
    return max(buf_ticks, buf_pct)


def _recompute_tpsl_for_price_move(
    side: str,
    entry: float,
    tp: float,
    sl: float,
    current_price: float,
    tick_size: float,
) -> tuple[float, float]:
    """
    Recompute TP/SL valid for current price. For SHORT: TP < current - buf, SL > current + buf.
    For LONG: TP > current + buf, SL < current - buf. Preserves risk distance where possible.
    """
    buf = _tpsl_buffer(current_price, tick_size)
    stop_dist = abs(sl - entry)
    tp_dist = abs(tp - entry)
    side_upper = (side or "").strip().upper()
    if side_upper in ("SHORT", "SELL"):
        sl_cand = entry + stop_dist
        tp_cand = entry - tp_dist
        if sl_cand <= current_price + buf:
            sl_cand = current_price + buf
        if tp_cand >= current_price - buf:
            tp_cand = current_price - buf
        if tp_cand >= sl_cand or tp_cand <= 0:
            tp_cand = current_price - buf
    else:
        sl_cand = entry - stop_dist
        tp_cand = entry + tp_dist
        if sl_cand >= current_price - buf:
            sl_cand = current_price - buf
        if tp_cand <= current_price + buf:
            tp_cand = current_price + buf
        if tp_cand <= sl_cand or sl_cand <= 0:
            sl_cand = current_price - buf
    return (
        _round_price_to_tick(tp_cand, tick_size),
        _round_price_to_tick(sl_cand, tick_size),
    )


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
        j = _request(
            "POST", "/v5/position/set-leverage",
            self.api_key, self.api_secret, data=data,
            allow_ret_codes={110043},
        )
        if j.get("retCode") == 110043:
            logger.info(
                "LIVE_LEVERAGE_ALREADY_SET | symbol=%s leverage=%s retCode=110043",
                symbol, leverage,
            )

    def get_ticker_price(self, symbol: str) -> Optional[dict]:
        """Fetch last/mark price from public tickers. Returns {lastPrice, markPrice} or None."""
        if self.dry_run:
            return None
        try:
            base = _base_url()
            params = {"category": CATEGORY, "symbol": _norm_symbol(symbol)}
            r = requests.get(f"{base}/v5/market/tickers", params=params, timeout=10)
            r.raise_for_status()
            j = r.json()
            if j.get("retCode") != 0:
                return None
            lst = j.get("result", {}).get("list", []) or []
            if not lst:
                return None
            item = lst[0]
            last = float(item.get("lastPrice") or 0)
            mark = float(item.get("markPrice") or last)
            return {"lastPrice": last, "markPrice": mark}
        except Exception as e:
            logger.warning("get_ticker_price failed symbol=%s: %s", symbol, e)
            return None

    def get_open_position(self, symbol: str, side: Optional[str] = None) -> Optional[dict]:
        """Get open position for symbol. side: filter by 'Sell'/'Buy' (hedge). Returns {size, side, avgPrice} or None."""
        params = {"category": CATEGORY, "symbol": _norm_symbol(symbol)}
        j = _request("GET", "/v5/position/list", self.api_key, self.api_secret, params=params)
        lst = j.get("result", {}).get("list", []) or []
        want_side = (side or "").strip().capitalize()
        if want_side.upper() == "SHORT":
            want_side = "Sell"
        elif want_side.upper() == "LONG":
            want_side = "Buy"
        for pos in lst:
            sz = float(pos.get("size") or 0)
            if sz > 0:
                pos_side = pos.get("side", "")
                if want_side and pos_side != want_side:
                    continue
                logger.info(
                    "LIVE_POSITION | symbol=%s side=%s size=%s avgPrice=%s",
                    symbol, pos_side, sz, pos.get("avgPrice"),
                )
                return {
                    "size": sz,
                    "side": pos_side,
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

    def set_trading_stop(
        self,
        symbol: str,
        position_idx: int,
        take_profit: float | None,
        stop_loss: float | None,
    ) -> dict:
        """
        Set TP/SL on position. POST /v5/position/trading-stop.
        take_profit/stop_loss: prices; None or 0 = cancel that side.
        Returns result dict. Logs LIVE_TPSL_SET on success, LIVE_ORDER_REJECT on error.
        """
        if self.dry_run:
            logger.info(
                "LIVE_DRY_RUN | set_trading_stop symbol=%s positionIdx=%s tp=%s sl=%s (skipped)",
                symbol, position_idx, take_profit, stop_loss,
            )
            return {}
        data = {
            "category": CATEGORY,
            "symbol": _norm_symbol(symbol),
            "positionIdx": position_idx,
            "tpslMode": "Full",
            "tpTriggerBy": "LastPrice",
            "slTriggerBy": "LastPrice",
        }
        if take_profit is not None and take_profit > 0:
            data["takeProfit"] = str(take_profit)
        else:
            data["takeProfit"] = "0"
        if stop_loss is not None and stop_loss > 0:
            data["stopLoss"] = str(stop_loss)
        else:
            data["stopLoss"] = "0"
        try:
            _request("POST", "/v5/position/trading-stop", self.api_key, self.api_secret, data=data)
            logger.info(
                "LIVE_TPSL_SET | symbol=%s positionIdx=%s takeProfit=%s stopLoss=%s",
                symbol, position_idx, data["takeProfit"], data["stopLoss"],
            )
            return {"takeProfit": data["takeProfit"], "stopLoss": data["stopLoss"]}
        except RuntimeError as e:
            logger.warning(
                "LIVE_ORDER_REJECT | symbol=%s positionIdx=%s reason=set_trading_stop_failed %s",
                symbol, position_idx, e,
            )
            raise

    def get_closed_pnl(
        self,
        symbol: str,
        start_time_ms: Optional[int] = None,
        end_time_ms: Optional[int] = None,
        limit: int = 50,
        *,
        raise_on_network_error: bool = False,
    ) -> list[dict]:
        """Fetch closed PnL records. Returns list of {symbol, orderId, side, avgEntryPrice, avgExitPrice, closedPnl, updatedTime, execType}.
        If raise_on_network_error=True, re-raises TimeoutError, ConnectionError, requests.RequestException for retry logic."""
        if self.dry_run:
            return []
        params = {"category": CATEGORY, "symbol": _norm_symbol(symbol), "limit": str(min(limit, 100))}
        if start_time_ms is not None:
            params["startTime"] = str(start_time_ms)
        if end_time_ms is not None:
            params["endTime"] = str(end_time_ms)
        try:
            j = _request("GET", "/v5/position/closed-pnl", self.api_key, self.api_secret, params=params)
        except Exception as e:
            if raise_on_network_error:
                exc_types = (TimeoutError, ConnectionError, OSError)
                try:
                    import requests as _req
                    exc_types = exc_types + (_req.exceptions.Timeout, _req.exceptions.ConnectionError, _req.exceptions.HTTPError, _req.exceptions.RequestException)
                except ImportError:
                    pass
                if isinstance(e, exc_types):
                    raise
            logger.warning("get_closed_pnl failed symbol=%s: %s", symbol, e)
            return []
        lst = j.get("result", {}).get("list", []) or []
        return lst

    def get_executions(self, symbol: str, order_id: Optional[str] = None, start_time_ms: Optional[int] = None, end_time_ms: Optional[int] = None, limit: int = 50) -> list[dict]:
        """Fetch execution records. orderId has highest priority if provided. Returns list of {execPrice, execQty, execTime, closedSize, ...}."""
        if self.dry_run:
            return []
        params = {"category": CATEGORY, "symbol": _norm_symbol(symbol), "limit": str(min(limit, 100))}
        if order_id:
            params["orderId"] = order_id
        if start_time_ms is not None:
            params["startTime"] = str(start_time_ms)
        if end_time_ms is not None:
            params["endTime"] = str(end_time_ms)
        try:
            j = _request("GET", "/v5/execution/list", self.api_key, self.api_secret, params=params)
        except Exception as e:
            logger.warning("get_executions failed symbol=%s: %s", symbol, e)
            return []
        lst = j.get("result", {}).get("list", []) or []
        return lst

    def cancel_order(self, symbol: str, order_id: str) -> None:
        """Cancel order by orderId."""
        if self.dry_run:
            logger.info("LIVE_DRY_RUN | cancel_order symbol=%s orderId=%s (skipped)", symbol, order_id)
            return
        data = {"category": CATEGORY, "symbol": _norm_symbol(symbol), "orderId": order_id}
        _request("POST", "/v5/order/cancel", self.api_key, self.api_secret, data=data)

    def _set_tpsl_with_retry(
        self,
        symbol: str,
        position_idx: int,
        side: str,
        entry_price: float,
        tp: float,
        sl: float,
        tick_size: float,
        qty: float,
    ) -> bool:
        """
        Set TP/SL. On failure: retry once with recomputed TP/SL from current price.
        If retry fails: close position (reduce-only) and return False.
        Returns True on success.
        """
        try:
            logger.info(
                "LIVE_TPSL_CALC | symbol=%s entry=%.4f tp=%.4f sl=%.4f tick_size=%.6f",
                symbol, entry_price, tp, sl, tick_size,
            )
            self.set_trading_stop(symbol, position_idx, tp, sl)
            return True
        except RuntimeError as e:
            err_str = str(e)
            logger.warning(
                "LIVE_TPSL_RETRY | symbol=%s first_attempt_failed %s",
                symbol, err_str,
            )
            pos = self.get_open_position(symbol, side)
            ticker = self.get_ticker_price(symbol)
            last_price = None
            if ticker:
                last_price = ticker.get("lastPrice") or ticker.get("markPrice")
            if last_price is None or last_price <= 0:
                logger.warning("LIVE_TPSL_RETRY | symbol=%s no_last_price, closing position", symbol)
                self._close_position_immediate(symbol, side, qty, position_idx)
                logger.info(
                    "LIVE_TPSL_FAILED_CLOSED | symbol=%s | side=%s | entry=%.4f | last=N/A | tp=%.4f | sl=%.4f | retCode=N/A retMsg=no_last_price",
                    symbol, side, entry_price, tp, sl,
                )
                return False
            entry_actual = float(pos.get("avgPrice", entry_price)) if pos else entry_price
            tp_retry, sl_retry = _recompute_tpsl_for_price_move(
                side, entry_actual, tp, sl, last_price, tick_size,
            )
            logger.info(
                "LIVE_TPSL_CALC | symbol=%s last_price=%.4f entry_actual=%.4f tp_retry=%.4f sl_retry=%.4f",
                symbol, last_price, entry_actual, tp_retry, sl_retry,
            )
            try:
                self.set_trading_stop(symbol, position_idx, tp_retry, sl_retry)
                return True
            except RuntimeError as e2:
                ret_code = ""
                ret_msg = str(e2)
                if "retCode=" in ret_msg:
                    parts = ret_msg.split("retCode=")
                    if len(parts) > 1:
                        rest = parts[1].split()
                        ret_code = rest[0].rstrip(",") if rest else ""
                logger.info(
                    "LIVE_TPSL_FAILED_CLOSED | symbol=%s | side=%s | entry=%.4f | last=%.4f | tp=%.4f | sl=%.4f | retCode=%s retMsg=%s",
                    symbol, side, entry_actual, last_price, tp_retry, sl_retry, ret_code, ret_msg,
                )
                self._close_position_immediate(symbol, side, qty, position_idx)
                return False

    def _close_position_immediate(
        self, symbol: str, side: str, qty: float, position_idx: int
    ) -> None:
        """Close position with reduce-only market order. SHORT -> Buy to close, LONG -> Sell to close."""
        close_side = "Buy" if (side or "").strip().upper() in ("SHORT", "SELL") else "Sell"
        try:
            self.place_market_order(
                symbol, close_side, qty, reduce_only=True, position_idx=position_idx
            )
            logger.info(
                "LIVE_TPSL_CLOSING | symbol=%s reduce_only side=%s qty=%s",
                symbol, close_side, qty,
            )
        except Exception as e:
            logger.error(
                "LIVE_TPSL_FAILED_CLOSE_ERROR | symbol=%s failed to close position: %s",
                symbol, e,
            )
            raise

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
            logger.info("LIVE_DRY_RUN | set_trading_stop skipped (dry_run)")
            return None
        try:
            order_res = self.place_market_order(symbol, side, qty, reduce_only=False, position_idx=position_idx)
        except Exception as e:
            logger.warning("LIVE_ORDER_REJECT | symbol=%s positionIdx=%s reason=%s", symbol, position_idx, e)
            return None

        order_id = (order_res or {}).get("orderId") or ""

        tick_size = limits.get("tick_size") or 0.0001
        tp_rounded = _round_price_to_tick(tp, tick_size)
        sl_rounded = _round_price_to_tick(sl, tick_size)
        if tp_rounded <= 0 or sl_rounded <= 0:
            logger.warning(
                "LIVE_ORDER_REJECT | symbol=%s positionIdx=%s reason=invalid_tpsl tp=%.6f sl=%.6f",
                symbol, position_idx, tp_rounded, sl_rounded,
            )
        elif abs(tp_rounded - sl_rounded) < 1e-12:
            logger.warning(
                "LIVE_ORDER_REJECT | symbol=%s positionIdx=%s reason=invalid_tpsl tp_eq_sl",
                symbol, position_idx,
            )
        else:
            tpsl_ok = self._set_tpsl_with_retry(
                symbol=symbol,
                position_idx=position_idx,
                side=side,
                entry_price=entry_price,
                tp=tp_rounded,
                sl=sl_rounded,
                tick_size=tick_size,
                qty=qty,
            )
            if not tpsl_ok:
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
            "order_id": order_id,
            "position_idx": position_idx,
        }
        return position
