"""Trading runner: run_once consumes signals from queue, PAPER open, CSV log. CLI: python3 -m trading.runner --once."""
from __future__ import annotations

import csv
import fcntl
import logging
import os
import sys
from datetime import datetime, timezone
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
    PROCESSED_PATH,
    PROCESSING_PATH,
    RISK_PCT,
    RUNNER_LOCK_PATH,
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
from trading.paper_outcome import close_on_timeout
from trading.signal_io import signal_from_dict
from trading.state import load_state, make_position_id, record_open, save_state

logger = logging.getLogger(__name__)


def _ensure_dir(path: str) -> None:
    p = Path(path)
    if p.suffix:
        p = p.parent
    p.mkdir(parents=True, exist_ok=True)


def _acquire_lock() -> tuple[bool, int | None]:
    """Acquire file lock. Return (True, fd) if acquired, (False, None) if already locked. Caller must release."""
    _ensure_dir(RUNNER_LOCK_PATH)
    try:
        fd = os.open(RUNNER_LOCK_PATH, os.O_CREAT | os.O_RDWR, 0o644)
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        return True, fd
    except (BlockingIOError, OSError) as e:
        if getattr(e, "errno", None) == 11 or "Resource temporarily unavailable" in str(e):
            logger.info("run_once: lock held, exit cleanly")
            return False, None
        raise


def get_latest_signals() -> tuple[List[Signal], List[str]]:
    """
    Crash-safe: atomic rename QUEUE_PATH -> PROCESSING_PATH, read, return (signals, raw_lines).
    At start: if PROCESSING_PATH exists (recovery from crash), process it first.
    Dedupe unchanged. Caller must _finish_queue_processing(raw_lines) after successful run.
    """
    import json

    QUEUE_PATH = SIGNALS_QUEUE_PATH
    path_to_read: str | None = None
    raw_lines: List[str] = []

    # Recovery: if PROCESSING_PATH exists and non-empty, process it first
    if Path(PROCESSING_PATH).exists():
        content = Path(PROCESSING_PATH).read_text(encoding="utf-8").strip()
        if content:
            path_to_read = PROCESSING_PATH
            raw_lines = [ln for ln in content.splitlines() if ln.strip()]

    # Normal: if no processing file, try to claim QUEUE_PATH
    if path_to_read is None:
        if not Path(QUEUE_PATH).exists():
            return [], []
        st = Path(QUEUE_PATH).stat()
        if st.st_size == 0:
            try:
                os.remove(QUEUE_PATH)
            except OSError:
                pass
            return [], []
        try:
            os.replace(QUEUE_PATH, PROCESSING_PATH)
        except FileNotFoundError:
            return [], []
        path_to_read = PROCESSING_PATH
        raw_lines = [ln for ln in Path(PROCESSING_PATH).read_text(encoding="utf-8").splitlines() if ln.strip()]

    signals: List[Signal] = []
    for line in raw_lines:
        line = line.strip()
        if not line:
            continue
        try:
            d = json.loads(line)
            signals.append(signal_from_dict(d))
        except Exception as e:
            logger.warning("get_latest_signals: skip invalid line: %s", e)
    return signals, raw_lines


def _finish_queue_processing(raw_lines: List[str]) -> None:
    """After successful processing: append to processed file (audit), remove processing file."""
    _ensure_dir(PROCESSED_PATH)
    if raw_lines:
        try:
            with open(PROCESSED_PATH, "a", encoding="utf-8") as f:
                for ln in raw_lines:
                    if ln.strip():
                        f.write(ln if ln.endswith("\n") else ln + "\n")
        except Exception as e:
            logger.warning("_finish_queue_processing: append to processed failed: %s", e)
    try:
        if Path(PROCESSING_PATH).exists():
            os.remove(PROCESSING_PATH)
    except OSError as e:
        logger.warning("_finish_queue_processing: remove processing failed: %s", e)


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
    volume_1m: float | None = None,
    volume_sma_20: float | None = None,
    volume_zscore_20: float | None = None,
) -> None:
    _ensure_dir(LOG_PATH)
    file_exists = Path(LOG_PATH).exists()
    v1 = f"{volume_1m:.2f}" if volume_1m is not None else ""
    v20 = f"{volume_sma_20:.2f}" if volume_sma_20 is not None else ""
    vz = f"{volume_zscore_20:.2f}" if volume_zscore_20 is not None else ""
    with open(LOG_PATH, "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        if not file_exists:
            w.writerow([
                "ts", "strategy", "symbol", "side", "entry", "tp", "sl",
                "notional_usd", "risk_usd", "stop_pct", "run_id", "event_id",
                "volume_1m", "volume_sma_20", "volume_zscore_20",
            ])
        w.writerow([
            ts, strategy, symbol, side, f"{entry:.6f}", f"{tp:.6f}", f"{sl:.6f}",
            f"{notional_usd:.2f}", f"{risk_usd:.2f}", f"{stop_pct:.6f}", run_id, event_id or "",
            v1, v20, vz,
        ])


def run_once() -> None:
    """One iteration: fetch signals, dedupe, validate, risk check, PAPER open, log CSV."""
    ok, lock_fd = _acquire_lock()
    if not ok:
        return
    try:
        _run_once_body()
    finally:
        if lock_fd is not None:
            try:
                fcntl.flock(lock_fd, fcntl.LOCK_UN)
                os.close(lock_fd)
            except Exception:
                pass


def _run_once_body() -> None:
    state = load_state()
    equity = PAPER_EQUITY_USD
    last_signal_ids = state.setdefault("last_signal_ids", {})

    now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S+00:00")
    if close_on_timeout(state, now_utc):
        save_state(state)

    signals, raw_lines = get_latest_signals()
    if not signals:
        logger.debug("run_once: no signals")
        _finish_queue_processing(raw_lines)
        return

    try:
        for signal in signals:
            position_id = _dedupe_key(signal)
            processed = last_signal_ids.get(signal.strategy) or []
            if not isinstance(processed, list):
                processed = [processed] if processed else []
            if position_id in processed:
                logger.debug("run_once: dedupe skip strategy=%s position_id=%s", signal.strategy, position_id)
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

            notional_usd = calc_notional_usd(risk_usd, stop_distance_pct)

            if MODE == "paper":
                position = simulate_open(
                    signal,
                    qty_notional_usd=notional_usd,
                    risk_usd=risk_usd,
                    leverage=LEVERAGE,
                    opened_ts=signal.ts_utc or "",
                )
                pid = record_open(state, position)
                if pid:
                    lst = last_signal_ids.setdefault(signal.strategy, [])
                    if not isinstance(lst, list):
                        lst = [lst] if lst else []
                    if pid not in lst:
                        lst.append(pid)
                    last_signal_ids[signal.strategy] = lst
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
                    volume_1m=getattr(signal, "volume_1m", None),
                    volume_sma_20=getattr(signal, "volume_sma_20", None),
                    volume_zscore_20=getattr(signal, "volume_zscore_20", None),
                )
                logger.info(
                    "run_once: PAPER open strategy=%s symbol=%s position_id=%s entry=%.4f notional=%.2f risk_usd=%.2f",
                    signal.strategy, signal.symbol, pid or position_id, entry_f, notional_usd, risk_usd,
                )
            else:
                # TODO: live adapter place order, then record_open when fill
                logger.warning("run_once: MODE=%s not implemented, skip", MODE)

        save_state(state)
    finally:
        _finish_queue_processing(raw_lines)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")
    if "--once" in sys.argv:
        run_once()
    else:
        print("Usage: python3 -m trading.runner --once")
        sys.exit(1)


if __name__ == "__main__":
    main()
