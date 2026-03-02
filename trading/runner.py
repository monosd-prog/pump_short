"""Trading runner: run_once consumes signals from queue, PAPER open, CSV log. CLI: python3 -m trading.runner --once."""
from __future__ import annotations

import argparse
import csv
import fcntl
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, List, Optional

# Ensure project root on path
_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from short_pump.signals import Signal

from common.io_dataset import get_dataset_dir
from trading.config import (
    AUTO_TRADING_ENABLE,
    DATASET_BASE_DIR,
    DISABLE_MAX_CONCURRENT_TRADES,
    EXECUTION_MODE,
    FIXED_POSITION_USD,
    LEVERAGE,
    LOG_PATH,
    MAX_CONCURRENT_TRADES,
    MAX_DAILY_LOSS_USD,
    MAX_LEVERAGE,
    MAX_OPEN_PER_STRATEGY,
    MAX_RISK_USD_PER_TRADE,
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
from trading.broker import get_broker
from trading.risk import (
    calc_position_size,
    calc_risk_usd,
    calc_stop_distance_pct,
    risk_usd_for_live,
    validate_notional_leverage,
    validate_stop_distance,
    can_open,
)
from trading.paper_outcome import close_on_timeout
from trading.signal_io import signal_from_dict
from trading.state import (
    count_open_positions,
    get_daily_realized_pnl_usd,
    load_state,
    make_position_id,
    record_open,
    save_state,
)

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


def _get_allowed_strategies() -> List[str]:
    """Runtime list from STRATEGIES env (so CLI --strategies can override)."""
    raw = os.getenv("STRATEGIES", "short_pump").strip()
    out = [s.strip() for s in raw.split(",") if s.strip()]
    return out if out else ["short_pump"]


# Priority for multi-strategy selection: lower index = higher priority
_STRATEGY_PRIORITY: dict[str, int] = {"short_pump_fast0": 0, "short_pump": 1}


def _selection_key(signal: Signal) -> tuple:
    """Sort key: priority (fast0 first), then context_score desc, dist_to_peak_pct desc, symbol asc."""
    prio = _STRATEGY_PRIORITY.get(signal.strategy, 99)
    ctx = getattr(signal, "context_score", None)
    ctx_val = float(ctx) if ctx is not None else -1e9
    dist = getattr(signal, "dist_to_peak_pct", None)
    dist_val = float(dist) if dist is not None else -1e9
    return (prio, -ctx_val, -dist_val, (signal.symbol or "").upper())


def _pick_one_candidate(signals: List[Signal], allowed: List[str]) -> tuple[Optional[Signal], int]:
    """Filter by allowed strategies, sort by priority + tie-breaks. Returns (picked signal, n_candidates)."""
    candidates = [s for s in signals if (s.strategy or "").strip() in allowed]
    if not candidates:
        return None, 0
    candidates.sort(key=_selection_key)
    return candidates[0], len(candidates)


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


def run_once(*, dry_run_live: bool = False) -> None:
    """One iteration: fetch signals, dedupe, validate, risk check, open position, log CSV."""
    ok, lock_fd = _acquire_lock()
    if not ok:
        return
    try:
        _run_once_body(dry_run_live=dry_run_live)
    finally:
        if lock_fd is not None:
            try:
                fcntl.flock(lock_fd, fcntl.LOCK_UN)
                os.close(lock_fd)
            except Exception:
                pass


def _normalize_position_for_record(
    position: dict,
    *,
    signal: Any,
    notional_usd: float,
    risk_usd: float,
    entry_f: float,
    tp: float,
    sl_f: float,
    leverage_use: int,
) -> dict:
    """
    Normalize position dict for record_open. Merge broker result with signal data
    so LIVE_ACCEPTED always writes a complete position (strategy, symbol, side, mode,
    opened_ts, run_id, event_id, status, entry, tp, sl, notional_usd, risk_usd,
    order_id, position_idx).
    """
    strategy = position.get("strategy") or getattr(signal, "strategy", "")
    symbol = position.get("symbol") or getattr(signal, "symbol", "")
    side = position.get("side") or getattr(signal, "side", "SHORT")
    opened_ts = position.get("opened_ts") or (getattr(signal, "ts_utc", "") or "")
    run_id = position.get("run_id") if position.get("run_id") is not None else (getattr(signal, "run_id", "") or "")
    event_id = position.get("event_id") if position.get("event_id") is not None else (str(getattr(signal, "event_id", "") or ""))

    out = dict(position)
    out["strategy"] = strategy
    out["symbol"] = symbol
    out["side"] = side
    out["mode"] = position.get("mode") or ("live" if EXECUTION_MODE == "live" else "paper")
    out["opened_ts"] = opened_ts
    out["run_id"] = run_id
    out["event_id"] = event_id
    out["status"] = position.get("status", "open")
    out["entry"] = float(position.get("entry", entry_f) or entry_f)
    out["tp"] = float(position.get("tp", tp) or tp)
    out["sl"] = float(position.get("sl", sl_f) or sl_f)
    out["notional_usd"] = float(position.get("notional_usd", notional_usd) or notional_usd)
    out["risk_usd"] = float(position.get("risk_usd", risk_usd) or risk_usd)
    out["leverage"] = int(position.get("leverage", leverage_use) or leverage_use)
    # Live: ensure order_id/position_idx present (for outcome worker)
    if EXECUTION_MODE == "live":
        out.setdefault("order_id", position.get("order_id", ""))
        out.setdefault("position_idx", position.get("position_idx"))
    return out


def _fmt_opt(val: Optional[float | int], fmt: str = "%s") -> str:
    """Format optional value for journald log; use N/A if None."""
    if val is None:
        return "N/A"
    try:
        return fmt % (val,) if "%" in fmt else str(val)
    except (TypeError, ValueError):
        return str(val) if val is not None else "N/A"


def _run_once_body(*, dry_run_live: bool = False) -> None:
    ts_iso = datetime.now(timezone.utc).isoformat()
    allowed = _get_allowed_strategies()
    strategies_str = ",".join(allowed)
    print(f"RUNNER_TICK | mode={EXECUTION_MODE} strategies={strategies_str} ts={ts_iso}", flush=True)
    if FIXED_POSITION_USD > 0:
        logger.info("FIXED_POSITION_USD=%.2f (fixed sizing enabled)", FIXED_POSITION_USD)
    base_abs = os.path.abspath(DATASET_BASE_DIR)
    example_dir = get_dataset_dir("short_pump", ts_iso, base_dir=DATASET_BASE_DIR)
    print(
        f"DATASET_DIR | exec_mode={EXECUTION_MODE} base_dir={base_abs} "
        f"example={example_dir}",
        flush=True,
    )

    state = load_state()
    equity = PAPER_EQUITY_USD
    broker = None
    if EXECUTION_MODE == "live":
        try:
            broker = get_broker(EXECUTION_MODE, dry_run_live=dry_run_live)
            if hasattr(broker, "get_balance"):
                equity = float(broker.get_balance())
        except ValueError as e:
            if "BYBIT_API" in str(e):
                logger.error("LIVE_BROKER_ENABLED=false | %s", e)
                return
            raise
        except Exception as e:
            logger.error("LIVE_BALANCE | failed to fetch equity: %s", e)
            return
    last_signal_ids = state.setdefault("last_signal_ids", {})

    now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S+00:00")
    if close_on_timeout(state, now_utc):
        save_state(state)

    if EXECUTION_MODE == "live" and broker is not None:
        from trading.outcome_worker import run_outcome_worker
        run_outcome_worker(state, broker)
        save_state(state)

    signals, raw_lines = get_latest_signals()
    n_signals = len(signals)
    if signals:
        signal, n_candidates = _pick_one_candidate(signals, allowed)
    else:
        signal, n_candidates = None, 0

    print(f"RUNNER_QUEUE | n_signals={n_signals} n_candidates={n_candidates}", flush=True)

    if signal is None:
        if n_candidates == 0:
            print(f"RUNNER_NO_CANDIDATES | n_signals={n_signals} allowed={allowed}", flush=True)
        logger.debug("run_once: no candidates in allowed strategies %s", allowed)
        _finish_queue_processing(raw_lines)
        return

    print(
        "RUNNER_PICKED | strategy=%s symbol=%s context_score=%s dist_to_peak_pct=%s stage=%s liq_long_usd_30s=%s"
        % (
            signal.strategy or "N/A",
            signal.symbol or "N/A",
            _fmt_opt(getattr(signal, "context_score", None), "%.4f"),
            _fmt_opt(getattr(signal, "dist_to_peak_pct", None), "%.2f"),
            _fmt_opt(getattr(signal, "stage", None)),
            _fmt_opt(getattr(signal, "liq_long_usd_30s", None), "%.0f"),
        ),
        flush=True,
    )

    if n_candidates > 1:
        logger.info(
            "SELECTED_SIGNAL | picked_strategy=%s picked_symbol=%s n_candidates=%d",
            signal.strategy, signal.symbol, n_candidates,
        )

    try:
        position_id = _dedupe_key(signal)
        processed = last_signal_ids.get(signal.strategy) or []
        if not isinstance(processed, list):
            processed = [processed] if processed else []
        if position_id in processed:
            logger.debug("run_once: dedupe skip strategy=%s position_id=%s", signal.strategy, position_id)
            _finish_queue_processing(raw_lines)
            save_state(state)
            return

        entry = signal.entry_price
        tp = signal.tp_price
        sl = signal.sl_price
        if entry is None or tp is None or sl is None:
            logger.warning(
                "run_once: skip missing entry/tp/sl strategy=%s symbol=%s entry=%s tp=%s sl=%s",
                signal.strategy, signal.symbol, entry, tp, sl,
            )
            _finish_queue_processing(raw_lines)
            save_state(state)
            return

        entry_f = float(entry)
        sl_f = float(sl)
        stop_distance_pct = calc_stop_distance_pct(entry_f, sl_f)
        ok, reason = validate_stop_distance(stop_distance_pct)
        if not ok:
            logger.warning("run_once: reject stop_distance strategy=%s %s", signal.strategy, reason)
            _finish_queue_processing(raw_lines)
            save_state(state)
            return

        risk_usd_override = risk_usd_for_live(equity) if EXECUTION_MODE == "live" else None
        notional_usd, risk_usd, reject_reason = calc_position_size(
            entry_f,
            sl_f,
            equity,
            signal.symbol or "",
            RISK_PCT,
            stop_distance_pct,
            risk_usd_override=risk_usd_override,
        )
        if reject_reason:
            logger.info(
                "run_once: reject sizing reason=%s strategy=%s symbol=%s",
                reject_reason, signal.strategy, signal.symbol,
            )
            _finish_queue_processing(raw_lines)
            save_state(state)
            return

        if EXECUTION_MODE == "live":
            ok_lev, reason_lev = validate_notional_leverage(notional_usd, equity, MAX_LEVERAGE)
            if not ok_lev:
                logger.info(
                    "LIVE_REJECTED | reason=%s strategy=%s symbol=%s",
                    reason_lev, signal.strategy, signal.symbol,
                )
                _finish_queue_processing(raw_lines)
                save_state(state)
                return
            leverage_use = min(LEVERAGE, MAX_LEVERAGE)
            today_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            daily_pnl = get_daily_realized_pnl_usd(state, today_utc)
            if daily_pnl <= -MAX_DAILY_LOSS_USD:
                logger.info(
                    "LIVE_BLOCKED_DAILY_LOSS | daily_pnl=%.2f limit=%.2f",
                    daily_pnl, -MAX_DAILY_LOSS_USD,
                )
                _finish_queue_processing(raw_lines)
                save_state(state)
                return
            if not DISABLE_MAX_CONCURRENT_TRADES and count_open_positions(state, None) >= MAX_CONCURRENT_TRADES:
                logger.info(
                    "LIVE_REJECTED | reason=max_concurrent_trades strategy=%s symbol=%s",
                    signal.strategy, signal.symbol,
                )
                _finish_queue_processing(raw_lines)
                save_state(state)
                return
        else:
            risk_pct_for_can_open = (risk_usd / equity) if equity > 0 else RISK_PCT
            if not can_open(signal.strategy, state, equity, risk_pct_for_can_open, MAX_TOTAL_RISK_PCT):
                logger.info("run_once: can_open=false strategy=%s", signal.strategy)
                _finish_queue_processing(raw_lines)
                save_state(state)
                return
            leverage_use = LEVERAGE

        if EXECUTION_MODE == "live" and not dry_run_live and not AUTO_TRADING_ENABLE:
            logger.info(
                "LIVE_ORDER_REJECT | reason=AUTO_TRADING_ENABLE not set (require 1 to place orders) strategy=%s symbol=%s",
                signal.strategy, signal.symbol,
            )
            _finish_queue_processing(raw_lines)
            save_state(state)
            return

        broker = get_broker(EXECUTION_MODE, dry_run_live=dry_run_live)
        position = broker.open_position(
            signal,
            qty_notional_usd=notional_usd,
            risk_usd=risk_usd,
            leverage=leverage_use,
            opened_ts=signal.ts_utc or "",
        )
        if position is None:
            if EXECUTION_MODE == "live":
                logger.info(
                    "LIVE_REJECTED | reason=broker strategy=%s symbol=%s",
                    signal.strategy, signal.symbol,
                )
            _finish_queue_processing(raw_lines)
            save_state(state)
            return

        position = _normalize_position_for_record(
            position,
            signal=signal,
            notional_usd=notional_usd,
            risk_usd=risk_usd,
            entry_f=entry_f,
            tp=tp,
            sl_f=sl_f,
            leverage_use=leverage_use,
        )
        if EXECUTION_MODE == "live":
            logger.info(
                "LIVE_ACCEPTED | strategy=%s symbol=%s position_id=%s entry=%.4f notional=%.2f risk_usd=%.2f",
                signal.strategy, signal.symbol,
                make_position_id(signal.strategy, signal.run_id or "", str(signal.event_id or ""), signal.symbol),
                float(position.get("entry", entry_f)), notional_usd, risk_usd,
            )
        pid = record_open(state, position)
        if pid:
            lst = last_signal_ids.setdefault(signal.strategy, [])
            if not isinstance(lst, list):
                lst = [lst] if lst else []
            if pid not in lst:
                lst.append(pid)
            last_signal_ids[signal.strategy] = lst
        entry_log = float(position.get("entry", entry_f))
        _append_trade_row(
            ts=signal.ts_utc or "",
            strategy=signal.strategy,
            symbol=signal.symbol,
            side=signal.side or "SHORT",
            entry=entry_log,
            tp=float(position.get("tp", tp)),
            sl=float(position.get("sl", sl_f)),
            notional_usd=notional_usd,
            risk_usd=risk_usd,
            stop_pct=stop_distance_pct,
            run_id=position.get("run_id", signal.run_id or ""),
            event_id=str(position.get("event_id", signal.event_id or "")),
            volume_1m=getattr(signal, "volume_1m", None),
            volume_sma_20=getattr(signal, "volume_sma_20", None),
            volume_zscore_20=getattr(signal, "volume_zscore_20", None),
        )
        logger.info(
            "run_once: open strategy=%s symbol=%s position_id=%s entry=%.4f notional=%.2f risk_usd=%.2f",
            signal.strategy, signal.symbol, pid or position_id, entry_log, notional_usd, risk_usd,
        )

        save_state(state)
    finally:
        _finish_queue_processing(raw_lines)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")
    parser = argparse.ArgumentParser(description="Trading runner: consume signals, open paper/live positions")
    parser.add_argument("--once", action="store_true", help="Run one iteration and exit")
    parser.add_argument(
        "--dry-run-live",
        action="store_true",
        help="Live mode: only get_balance + instrument + set_leverage, no orders",
    )
    parser.add_argument(
        "--strategies",
        type=str,
        default=None,
        help='Comma-separated strategies (e.g. "short_pump,short_pump_fast0"). Overrides env STRATEGIES.',
    )
    args = parser.parse_args()
    if args.strategies is not None:
        os.environ["STRATEGIES"] = args.strategies
    if args.once:
        run_once(dry_run_live=args.dry_run_live)
    else:
        print("Usage: python3 -m trading.runner --once [--strategies short_pump,short_pump_fast0] [--dry-run-live]")
        sys.exit(1)


if __name__ == "__main__":
    main()
