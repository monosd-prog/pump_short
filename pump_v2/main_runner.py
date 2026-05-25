"""pump_v2 standalone daemon — Phase 7."""
from __future__ import annotations

import json
import logging
import os
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any, Dict, Optional, Tuple

from common.market_features import liquidation_features
from pump_v2.execution.enqueuer import maybe_enqueue_v2_signal
from pump_v2.prerun.context_builder import build_market_context_from_watcher
from pump_v2.strategies.short_pump import ShortPumpStrategy
from short_pump.bybit_api import (
    get_funding_rate,
    get_klines_5m,
    get_open_interest,
    get_recent_trades,
)
from short_pump.context5m import (
    StructureState,
    build_dbg5,
    compute_context_score_5m,
    update_structure,
)
from short_pump.features import normalize_funding
from short_pump.liquidations import (
    get_liq_stats,
    register_symbol,
    start_liquidation_listener,
    unregister_symbol,
)

logger = logging.getLogger("pump_v2.runner")

# ── Config (env with defaults) ─────────────────────────────────────────────
CATEGORY = os.environ.get("CATEGORY", "linear")
WATCH_MINUTES = int(os.environ.get("WATCH_MINUTES", "90"))
POLL_SECONDS = int(os.environ.get("PUMP_V2_POLL_SECONDS", "60"))
MAX_CONCURRENT = int(os.environ.get("MAX_CONCURRENT", "3"))
PORT = int(os.environ.get("PUMP_V2_PORT", "8001"))
PUMP_V2_REPLACE = os.environ.get("PUMP_V2_REPLACE", "0") == "1"

# symbol → expires_at (UTC)
_active: Dict[str, datetime] = {}
_active_lock = threading.Lock()
_states: Dict[str, StructureState] = {}
_states_lock = threading.Lock()


def _env_float(name: str, default: float) -> float:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    try:
        return float(raw.replace(",", "."))
    except ValueError:
        return default


def _normalize_symbol(symbol: str) -> str:
    s = (symbol or "").strip().upper()
    if not s:
        return ""
    if not s.endswith("USDT"):
        s = s + "USDT"
    return s


@dataclass
class V2WatchConfig:
    """Minimal cfg for build_dbg5 / update_structure (v1-compatible fields)."""

    symbol: str
    category: str = "linear"
    run_id: str = ""
    drop1_min_pct: float = 3.0
    bounce1_min_pct: float = 1.0
    drop2_min_pct: float = 2.0
    bounce2_min_pct: float = 0.8

    def __post_init__(self) -> None:
        self.symbol = self.symbol.strip().upper()
        if not self.run_id:
            self.run_id = f"v2_{time.strftime('%Y%m%d_%H%M%S')}_{self.symbol}"

    @classmethod
    def for_symbol(cls, symbol: str) -> V2WatchConfig:
        sym = _normalize_symbol(symbol)
        return cls(
            symbol=sym,
            category=os.environ.get("CATEGORY", "linear"),
            run_id=f"v2_{time.strftime('%Y%m%d_%H%M%S')}_{sym}",
            drop1_min_pct=_env_float("DROP1_MIN_PCT", 3.0),
            bounce1_min_pct=_env_float("BOUNCE1_MIN_PCT", 1.0),
            drop2_min_pct=_env_float("DROP2_MIN_PCT", 2.0),
            bounce2_min_pct=_env_float("BOUNCE2_MIN_PCT", 0.8),
        )


def reset_registry_for_tests() -> None:
    """Clear in-memory registry (tests only)."""
    with _active_lock:
        _active.clear()
    with _states_lock:
        _states.clear()


def accept_pump(symbol: str, exchange: Optional[str] = None) -> Tuple[int, Dict[str, Any]]:
    """
    Register symbol for watching. Returns (http_status, body_dict).
    """
    sym = _normalize_symbol(symbol)
    if not sym:
        return 400, {"status": "error", "reason": "missing_symbol"}

    ex = (exchange or "bybit").strip().lower()
    if ex != "bybit":
        return 400, {"status": "error", "reason": "exchange_not_bybit"}

    expires = datetime.now(timezone.utc) + timedelta(minutes=WATCH_MINUTES)

    with _active_lock:
        if sym in _active:
            _active[sym] = expires
            logger.info("PUMP_REFRESH | symbol=%s expires=%s", sym, expires)
            return 200, {"status": "ok", "symbol": sym, "refreshed": True}

        if len(_active) >= MAX_CONCURRENT:
            logger.info("PUMP_IGNORED | symbol=%s reason=max_concurrent", sym)
            return 429, {"status": "ignored", "reason": "max_concurrent", "symbol": sym}

        _active[sym] = expires

    try:
        register_symbol(sym)
    except Exception as exc:
        logger.warning("register_symbol failed: %s", exc)

    logger.info("PUMP_ACCEPTED | symbol=%s expires=%s", sym, expires)
    return 200, {"status": "ok", "symbol": sym}


def _get_or_create_state(symbol: str) -> StructureState:
    with _states_lock:
        st = _states.get(symbol)
        if st is None:
            st = StructureState()
            _states[symbol] = st
        return st


def _tick(symbol: str, strategy: ShortPumpStrategy, cfg: V2WatchConfig) -> None:
    """One poll tick: fetch → structure → ctx → signal → optional enqueue."""
    candles_5m = get_klines_5m(cfg.category, symbol, limit=250)
    if candles_5m is None or getattr(candles_5m, "empty", True):
        return
    if len(candles_5m) < 15:
        return

    peak_price = float(candles_5m["high"].tail(20).max())
    last_price = float(candles_5m.iloc[-1]["close"])

    st = _get_or_create_state(symbol)
    update_structure(cfg, st, last_price, peak_price)

    oi = get_open_interest(cfg.category, symbol, limit=80)
    trades = get_recent_trades(cfg.category, symbol, limit=1000)
    funding_payload = get_funding_rate(cfg.category, symbol)
    funding_rate, _funding_ts = normalize_funding(funding_payload)

    candles_5m_list = candles_5m.to_dict("records")
    trades_list = (
        trades.to_dict("records")
        if trades is not None and not getattr(trades, "empty", True)
        else []
    )
    oi_dict: Optional[Dict[str, Any]] = None
    if oi is not None and not getattr(oi, "empty", True):
        oi_dict = {"oi_df": oi}

    dbg5 = build_dbg5(cfg, candles_5m_list, oi_dict, trades_list, st)
    if not dbg5:
        return

    context_score, ctx_parts = compute_context_score_5m(dbg5)

    liq: Optional[Dict[str, Any]] = None
    try:
        liq = liquidation_features(
            symbol=symbol,
            now_ts=time.time(),
            get_liq_stats=get_liq_stats,
        )
    except Exception:
        liq = None

    ctx = build_market_context_from_watcher(
        symbol=symbol,
        candles_5m=candles_5m_list,
        oi_dict=oi_dict,
        funding_rate=float(funding_rate) if funding_rate is not None else 0.0,
        dbg5=dbg5,
        context_score=context_score,
        ctx_parts=ctx_parts,
        liq_features=liq,
        trades_list=trades_list,
    )

    sig = strategy.check_signal(ctx)

    try:
        from pump_v2.core.funnel import log_funnel_event

        log_funnel_event(
            strategy="short_pump",
            symbol=symbol,
            stage="signal_generated" if sig else "candidate_scanned",
            blocked_reason="",
        )
    except Exception:
        pass

    if sig is not None and PUMP_V2_REPLACE:
        maybe_enqueue_v2_signal(sig, ctx)


def watch_symbol(symbol: str) -> None:
    """Poll loop for one symbol until watch window expires."""
    sym = _normalize_symbol(symbol)
    cfg = V2WatchConfig.for_symbol(sym)
    strategy = ShortPumpStrategy(params={}, risk={})

    logger.info("WATCH_START | symbol=%s run_id=%s", sym, cfg.run_id)

    try:
        while True:
            now = datetime.now(timezone.utc)
            with _active_lock:
                expires = _active.get(sym)

            if expires is None or now > expires:
                with _active_lock:
                    _active.pop(sym, None)
                with _states_lock:
                    _states.pop(sym, None)
                try:
                    unregister_symbol(sym)
                except Exception:
                    pass
                logger.info("WATCH_DONE | symbol=%s", sym)
                return

            try:
                _tick(sym, strategy, cfg)
            except Exception as exc:
                logger.warning("TICK_ERROR | symbol=%s error=%s", sym, exc)

            time.sleep(POLL_SECONDS)
    except Exception as exc:
        logger.exception("WATCH_CRASH | symbol=%s error=%s", sym, exc)
        with _active_lock:
            _active.pop(sym, None)


class PumpWebhookHandler(BaseHTTPRequestHandler):
    """POST /pump — same contract as v1 short_pump server."""

    def do_POST(self) -> None:
        if self.path != "/pump":
            self.send_response(404)
            self.end_headers()
            return

        try:
            length = int(self.headers.get("Content-Length", 0))
            raw = self.rfile.read(length) if length > 0 else b"{}"
            body = json.loads(raw.decode("utf-8") or "{}")
        except (json.JSONDecodeError, ValueError):
            self.send_response(400)
            self.end_headers()
            return

        status, resp = accept_pump(
            symbol=str(body.get("symbol", "")),
            exchange=body.get("exchange"),
        )
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps(resp).encode("utf-8"))

    def log_message(self, format: str, *args: Any) -> None:
        return


def _supervisor_loop(_watching: Dict[str, threading.Thread]) -> None:
    """Start watch_symbol threads for newly active symbols."""
    while True:
        with _active_lock:
            symbols = list(_active.keys())

        for sym in symbols:
            t = _watching.get(sym)
            if t is None or not t.is_alive():
                t = threading.Thread(
                    target=watch_symbol,
                    args=(sym,),
                    name=f"v2-watch-{sym}",
                    daemon=True,
                )
                t.start()
                _watching[sym] = t

        dead = [s for s, t in _watching.items() if not t.is_alive()]
        for s in dead:
            _watching.pop(s, None)

        time.sleep(5)


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    try:
        start_liquidation_listener(CATEGORY)
    except Exception as exc:
        logger.warning("liquidation listener start failed: %s", exc)

    server = HTTPServer(("0.0.0.0", PORT), PumpWebhookHandler)
    threading.Thread(
        target=server.serve_forever,
        name="pump-v2-webhook",
        daemon=True,
    ).start()

    logger.info(
        "pump_v2 runner started | port=%d poll=%ds watch_min=%d max_concurrent=%d replace=%s",
        PORT,
        POLL_SECONDS,
        WATCH_MINUTES,
        MAX_CONCURRENT,
        PUMP_V2_REPLACE,
    )

    watching: Dict[str, threading.Thread] = {}
    try:
        _supervisor_loop(watching)
    except KeyboardInterrupt:
        logger.info("pump_v2 runner shutdown")
        server.shutdown()


if __name__ == "__main__":
    main()
