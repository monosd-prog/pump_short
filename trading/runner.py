"""Trading runner: run_once consumes signals from queue, PAPER open, CSV log. CLI: python3 -m trading.runner --once."""
from __future__ import annotations

import csv
import logging
import sys
from pathlib import Path
from typing import List

# Ensure project root on path
_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from short_pump.signals import Signal

from trading.config import (
    LEVERAGE,
    LOG_PATH,
    MAX_OPEN_PER_STRATEGY,
    MAX_TOTAL_RISK_PCT,
    MODE,
    PAPER_EQUITY_USD,
    RISK_PCT,
    SIGNALS_QUEUE_PATH,
    STATE_PATH,
)
from trading.paper import simulate_open
from trading.risk import (
    calc_notional_usd,
    calc_risk_usd,
    calc_stop_distance_pct,
    validate_stop_distance,
    can_open,
)
from trading.signal_io import signal_from_dict
from trading.state import has_open_position, load_state, record_open, save_state

logger = logging.getLogger(__name__)


def _ensure_dir(path: str) -> None:
    p = Path(path)
    if p.suffix:
        p = p.parent
    p.mkdir(parents=True, exist_ok=True)


def get_latest_signals() -> List[Signal]:
    """
    MVP: read signals from JSONL queue, return list and clear file (processed).
    TODO: optional persistence of processed ids instead of truncate; live adapter feed.
    """
    import json

    signals: List[Signal] = []
    try:
        with open(SIGNALS_QUEUE_PATH, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    d = json.loads(line)
                    signals.append(signal_from_dict(d))
                except Exception as e:
                    logger.warning("get_latest_signals: skip invalid line: %s", e)
    except FileNotFoundError:
        return signals

    # Truncate after read (processed)
    if signals:
        try:
            with open(SIGNALS_QUEUE_PATH, "w", encoding="utf-8") as f:
                pass
        except Exception as e:
            logger.warning("get_latest_signals: truncate failed: %s", e)
    return signals


def _dedupe_key(signal: Signal) -> str:
    return f"{signal.strategy}:{signal.run_id}:{getattr(signal, 'event_id', '') or ''}:{signal.symbol}"


def _append_trade_row(
    ts: str,
    strategy: str,
    symbol: str,
    side: str,
    entry: float,
    tp: float,
    sl: float,
    notional_usd: float,
    risk_usd: float,
    stop_pct: float,
    run_id: str,
    event_id: str,
) -> None:
    _ensure_dir(LOG_PATH)
    file_exists = Path(LOG_PATH).exists()
    with open(LOG_PATH, "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        if not file_exists:
            w.writerow([
                "ts", "strategy", "symbol", "side", "entry", "tp", "sl",
                "notional_usd", "risk_usd", "stop_pct", "run_id", "event_id",
            ])
        w.writerow([
            ts, strategy, symbol, side, f"{entry:.6f}", f"{tp:.6f}", f"{sl:.6f}",
            f"{notional_usd:.2f}", f"{risk_usd:.2f}", f"{stop_pct:.6f}", run_id, event_id or "",
        ])


def run_once() -> None:
    """One iteration: fetch signals, dedupe, validate, risk check, PAPER open, log CSV."""
    state = load_state()
    equity = PAPER_EQUITY_USD
    last_signal_ids = state.setdefault("last_signal_ids", {})

    signals = get_latest_signals()
    if not signals:
        logger.debug("run_once: no signals")
        return

    for signal in signals:
        dedupe_key = _dedupe_key(signal)
        if last_signal_ids.get(signal.strategy) == dedupe_key:
            logger.debug("run_once: dedupe skip strategy=%s key=%s", signal.strategy, dedupe_key)
            continue

        entry = signal.entry_price
        tp = signal.tp_price
        sl = signal.sl_price
        if entry is None or tp is None or sl is None:
            logger.warning(
                "run_once: skip missing entry/tp/sl strategy=%s symbol=%s entry=%s tp=%s sl=%s",
                signal.strategy, signal.symbol, entry, tp, sl,
            )
            continue

        entry_f = float(entry)
        sl_f = float(sl)
        risk_usd = calc_risk_usd(equity, RISK_PCT)
        stop_distance_pct = calc_stop_distance_pct(entry_f, sl_f)
        ok, reason = validate_stop_distance(stop_distance_pct)
        if not ok:
            logger.warning("run_once: reject stop_distance strategy=%s %s", signal.strategy, reason)
            continue

        if not can_open(signal.strategy, state, equity, RISK_PCT, MAX_TOTAL_RISK_PCT):
            logger.info("run_once: can_open=false strategy=%s", signal.strategy)
            continue

        if has_open_position(state, signal.strategy):
            logger.info("run_once: already open strategy=%s", signal.strategy)
            continue

        notional_usd = calc_notional_usd(risk_usd, stop_distance_pct)

        if MODE == "paper":
            position = simulate_open(
                signal,
                qty_notional_usd=notional_usd,
                risk_usd=risk_usd,
                leverage=LEVERAGE,
                opened_ts=signal.ts_utc or "",
            )
            record_open(state, position)
            last_signal_ids[signal.strategy] = dedupe_key
            _append_trade_row(
                ts=signal.ts_utc or "",
                strategy=signal.strategy,
                symbol=signal.symbol,
                side=signal.side or "SHORT",
                entry=entry_f,
                tp=float(tp),
                sl=sl_f,
                notional_usd=notional_usd,
                risk_usd=risk_usd,
                stop_pct=stop_distance_pct,
                run_id=signal.run_id or "",
                event_id=str(signal.event_id or ""),
            )
            logger.info(
                "run_once: PAPER open strategy=%s symbol=%s entry=%.4f notional=%.2f risk_usd=%.2f",
                signal.strategy, signal.symbol, entry_f, notional_usd, risk_usd,
            )
        else:
            # TODO: live adapter place order, then record_open when fill
            logger.warning("run_once: MODE=%s not implemented, skip", MODE)

    save_state(state)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")
    if "--once" in sys.argv:
        run_once()
    else:
        print("Usage: python3 -m trading.runner --once")
        sys.exit(1)


if __name__ == "__main__":
    main()
