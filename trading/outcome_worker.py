"""Outcome Worker: resolve LIVE trade outcomes from Bybit, close positions via close_from_live_outcome."""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger(__name__)

_LIVE_OUTCOME_TG_SUFFIX = ":live"


def _deliver_live_outcome_tg(
    *,
    strategy: str,
    position: dict[str, Any],
    symbol: str,
    run_id: str,
    event_id: str,
    res: str,
    exit_price: float,
    pnl_pct: float,
    exit_ts: str,
) -> None:
    """Send dedicated TG for live Bybit-confirmed outcome. Uses delivery_key_suffix=':live' to avoid
    collision with the paper candle fallback key."""
    try:
        from notifications.tg_format import format_fast0_outcome_message
        from trading.outcome_delivery import deliver_outcome_tg

        entry_price = float(position.get("entry", 0) or 0)
        tp_price = float(position.get("tp", 0) or 0)
        sl_price = float(position.get("sl", 0) or 0)
        risk_profile = (position.get("risk_profile") or "").strip() or None
        notional_usd_raw = position.get("notional_usd")
        leverage_raw = position.get("leverage")
        margin_mode = (position.get("margin_mode") or "").strip() or None
        notional_usd = float(notional_usd_raw) if notional_usd_raw is not None else None
        leverage = int(leverage_raw) if leverage_raw is not None else None

        hold_seconds = 0.0
        try:
            opened_dt = _parse_opened_ts(position.get("opened_ts", ""))
            exit_dt_str = str(exit_ts).strip().replace("Z", "+00:00").replace("+0000", "+00:00")
            exit_dt = datetime.fromisoformat(exit_dt_str)
            if exit_dt.tzinfo is None:
                exit_dt = exit_dt.replace(tzinfo=timezone.utc)
            if opened_dt:
                hold_seconds = max(0.0, (exit_dt - opened_dt).total_seconds())
        except Exception:
            pass

        msg = format_fast0_outcome_message(
            symbol=symbol,
            run_id=run_id,
            event_id=event_id,
            strategy=strategy,
            res=res,
            entry_price=entry_price,
            tp_price=tp_price,
            sl_price=sl_price,
            exit_price=exit_price,
            pnl_pct=pnl_pct,
            hold_seconds=hold_seconds,
            risk_profile=risk_profile,
            notional_usd=notional_usd,
            leverage=leverage,
            margin_mode=margin_mode,
            live_confirmed=True,
        )

        deliver_outcome_tg(
            logger=logger,
            delivery_strategy=strategy,
            run_id=run_id,
            event_id=event_id,
            symbol=symbol,
            res=res,
            send_text=msg,
            tg_send_enabled=True,
            delivery_reason="live_bybit_confirmed",
            delivery_mode="live_bybit_confirmed",
            stage=None,
            step="LIVE_OUTCOME_TG",
            delivery_key_suffix=_LIVE_OUTCOME_TG_SUFFIX,
            send_telegram_kwargs={
                "strategy": strategy,
                "side": "SHORT",
                "mode": "LIVE",
                "event_id": str(event_id),
            },
        )
    except Exception:
        logger.exception(
            "LIVE_OUTCOME_TG_FAILED | symbol=%s run_id=%s event_id=%s step=LIVE_OUTCOME_TG",
            symbol, run_id, event_id,
        )

LIVE_OUTCOME_POLL_INTERVAL_SEC = int(
    (__import__("os").getenv("LIVE_OUTCOME_POLL_INTERVAL_SEC") or "5").replace(",", ".")
)
# Timeout for resolve_live_outcome per position: allow multiple API calls (Bybit closed-pnl can lag)
LIVE_OUTCOME_RESOLVE_TIMEOUT_SEC = int(
    (__import__("os").getenv("LIVE_OUTCOME_RESOLVE_TIMEOUT_SEC") or "30").replace(",", ".")
)


def _parse_opened_ts(opened_ts: str) -> datetime | None:
    """Parse opened_ts to UTC-aware datetime. Return None on failure."""
    if not opened_ts or not str(opened_ts).strip():
        return None
    s = str(opened_ts).strip().replace("Z", "+00:00").replace("+0000", "+00:00")
    for fmt in (
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%S.%f%z",
        "%Y-%m-%d %H:%M:%S%z",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%dT%H:%M:%S",
    ):
        try:
            dt = datetime.strptime(s, fmt)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except ValueError:
            continue
    try:
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except ValueError:
        return None


def run_outcome_worker(state: dict[str, Any], broker: Any) -> None:
    """
    Iterate over live open positions, resolve outcomes via Bybit, close on TP/SL.
    Modifies state in place.
    Network/timeout must NOT close position; log OUTCOME_PENDING and keep open.
    """
    from trading.bybit_live_outcome import resolve_live_outcome
    from trading.paper_outcome import close_from_live_outcome
    from trading.state import save_state

    open_positions = state.get("open_positions") or {}
    live_positions: list[tuple[str, str, dict[str, Any]]] = []

    for strategy, strat_pos in open_positions.items():
        if not isinstance(strat_pos, dict):
            continue
        for position_id, position in strat_pos.items():
            if not isinstance(position, dict):
                continue
            mode = (position.get("mode") or "").strip().lower()
            if mode != "live":
                continue
            order_id = position.get("order_id")
            position_idx = position.get("position_idx")
            if not order_id or position_idx is None:
                continue
            live_positions.append((strategy, position_id, position))

    n_live = len(live_positions)
    print(f"OUTCOME_WORKER_TICK | n_live_open={n_live}", flush=True)

    for strategy, position_id, position in live_positions:
        symbol = position.get("symbol", "")
        run_id = position.get("run_id", "")
        event_id = (position.get("event_id") or "") or ""
        order_id = str(position.get("order_id", ""))
        position_idx = int(position.get("position_idx", 0))
        opened_ts = position.get("opened_ts", "")
        entry_price = float(position.get("entry", 0) or 0)
        tp_price = float(position.get("tp", 0) or 0)
        sl_price = float(position.get("sl", 0) or 0)
        side = (position.get("side") or "SHORT").strip()

        if not symbol or entry_price <= 0 or tp_price <= 0 or sl_price <= 0:
            logger.warning("OUTCOME_WORKER_SKIP | position_id=%s symbol=%s missing_fields", position_id, symbol)
            continue

        try:
            outcome = resolve_live_outcome(
                symbol=symbol,
                order_id=order_id,
                position_idx=position_idx,
                opened_ts=opened_ts,
                entry_price=entry_price,
                tp_price=tp_price,
                sl_price=sl_price,
                side=side,
                broker=broker,
                timeout_sec=max(10, LIVE_OUTCOME_RESOLVE_TIMEOUT_SEC),
                raise_on_network_error=False,
            )
        except Exception as e:
            age_sec = 0
            opened_dt = _parse_opened_ts(opened_ts)
            if opened_dt:
                age_sec = (datetime.now(timezone.utc) - opened_dt).total_seconds()
            logger.info(
                "OUTCOME_PENDING | position_id=%s symbol=%s run_id=%s age_sec=%.0f err=%s",
                position_id, symbol, run_id, age_sec, type(e).__name__,
            )
            logger.info(
                "OUTCOME_DEBUG | strategy=%s symbol=%s position_id=%s found_on_exchange=False pnl_source=timeout reason=%s",
                strategy,
                symbol,
                position_id,
                type(e).__name__,
            )
            continue

        if outcome:
            res = outcome.get("status", "")
            pnl_source = str(outcome.get("pnl_source") or "exchange")
            found_on_exchange = bool(outcome.get("found_on_exchange", pnl_source != "timeout"))
            reason = str(outcome.get("resolution_path") or outcome.get("reason") or res or "resolved")
            logger.info(
                "OUTCOME_DEBUG | strategy=%s symbol=%s position_id=%s found_on_exchange=%s pnl_source=%s reason=%s",
                strategy,
                symbol,
                position_id,
                found_on_exchange,
                pnl_source,
                reason,
            )
            if res in ("TP_hit", "SL_hit", "EARLY_EXIT"):
                exit_price = float(outcome.get("exit_price", 0) or 0)
                pnl_pct = float(outcome.get("pnl_pct", 0) or 0)
                exit_ts = outcome.get("exit_ts", datetime.now(timezone.utc).isoformat())
                ok = close_from_live_outcome(
                    strategy=strategy,
                    symbol=symbol,
                    run_id=run_id,
                    event_id=event_id,
                    res=res,
                    exit_price=exit_price,
                    pnl_pct=pnl_pct,
                    ts_utc=exit_ts,
                    state=state,
                )
                if ok:
                    print(
                        f"OUTCOME_RESOLVED | position_id={position_id} symbol={symbol} run_id={run_id} res={res}",
                        flush=True,
                    )
                    save_state(state)
                    _deliver_live_outcome_tg(
                        strategy=strategy,
                        position=position,
                        symbol=symbol,
                        run_id=run_id,
                        event_id=event_id,
                        res=res,
                        exit_price=exit_price,
                        pnl_pct=pnl_pct,
                        exit_ts=exit_ts,
                    )
            else:
                logger.debug("OUTCOME_WORKER_SKIP | position_id=%s res=%s (not TP/SL)", position_id, res)
        else:
            age_sec = 0
            opened_dt = _parse_opened_ts(opened_ts)
            if opened_dt:
                age_sec = (datetime.now(timezone.utc) - opened_dt).total_seconds()
            logger.info(
                "OUTCOME_PENDING | position_id=%s symbol=%s run_id=%s age_sec=%.0f",
                position_id, symbol, run_id, age_sec,
            )
            logger.info(
                "OUTCOME_DEBUG | strategy=%s symbol=%s position_id=%s found_on_exchange=False pnl_source=timeout reason=no_match",
                strategy,
                symbol,
                position_id,
            )
