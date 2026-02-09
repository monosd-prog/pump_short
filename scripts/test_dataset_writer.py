from __future__ import annotations

import json
import os
from datetime import datetime, timedelta, timezone

from common.io_dataset import write_event_row, write_outcome_row, write_trade_row
from common.outcome_tracker import build_outcome_row
from common.runtime import wall_time_utc


def _utc_ts() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")


def _path_1m(n: int, base_price: float) -> list[dict[str, float | str]]:
    now = datetime.now(timezone.utc)
    rows = []
    for i in range(n):
        ts = now + timedelta(minutes=i + 1)
        rows.append({"t": ts.isoformat(), "h": base_price + 1.0, "l": base_price - 1.0})
    return rows


def main() -> None:
    strategy = os.getenv("STRATEGY", "short_pump").strip()
    case = os.getenv("CASE", "PATH_ONLY").strip().upper()
    symbol = os.getenv("SYMBOL", "TESTUSDT").strip().upper()
    mode = os.getenv("MODE", "live").strip()
    n_path = int(os.getenv("N_PATH", "5"))
    conflict_policy = os.getenv("CONFLICT_POLICY")
    if case == "CONFLICT" and not conflict_policy:
        conflict_policy = "NEUTRAL"
    conflict_policy = (conflict_policy or "SL_FIRST").strip().upper()

    run_id = f"test_{_utc_ts()}"
    event_id = f"{run_id}_entry_fast"
    trade_id = f"{event_id}_trade"
    now_utc = wall_time_utc()
    entry_time_utc = now_utc
    entry_price = 100.0
    tp_price = 110.0
    sl_price = 95.0
    tp_pct = ((tp_price - entry_price) / entry_price) * 100.0
    sl_pct = ((entry_price - sl_price) / entry_price) * 100.0

    event_row = {
        "run_id": run_id,
        "event_id": event_id,
        "symbol": symbol,
        "strategy": strategy,
        "mode": mode,
        "side": "SHORT" if strategy == "short_pump" else "LONG",
        "wall_time_utc": now_utc,
        "time_utc": entry_time_utc,
        "stage": 4,
        "entry_ok": 1,
        "skip_reasons": "",
        "context_score": 0.5,
        "price": entry_price,
        "payload_json": json.dumps({"case": case}, ensure_ascii=False),
    }
    write_event_row(event_row, strategy=strategy, mode=mode, wall_time_utc=now_utc, schema_version=2)

    trade_row = {
        "trade_id": trade_id,
        "event_id": event_id,
        "run_id": run_id,
        "symbol": symbol,
        "strategy": strategy,
        "mode": mode,
        "side": "SHORT" if strategy == "short_pump" else "LONG",
        "entry_time_utc": entry_time_utc,
        "entry_price": entry_price,
        "tp_price": tp_price,
        "sl_price": sl_price,
        "trade_type": "PAPER",
    }
    write_trade_row(trade_row, strategy=strategy, mode=mode, wall_time_utc=now_utc, schema_version=2)

    path_1m = _path_1m(n_path, entry_price) if case == "PATH_ONLY" else []
    tp_sl_same_candle = 1 if case == "CONFLICT" else 0
    end_reason = "TIMEOUT"
    pnl_pct = 0.0
    if case == "TP":
        end_reason = "TP_hit"
        pnl_pct = tp_pct
    elif case == "SL":
        end_reason = "SL_hit"
        pnl_pct = -sl_pct
    elif case == "CONFLICT":
        if conflict_policy == "TP_FIRST":
            end_reason = "TP_hit"
            pnl_pct = tp_pct
        elif conflict_policy == "SL_FIRST":
            end_reason = "SL_hit"
            pnl_pct = -sl_pct
        else:
            end_reason = "CONFLICT"
            pnl_pct = 0.0

    details_payload = {
        "tp_hit": end_reason == "TP_hit",
        "sl_hit": end_reason == "SL_hit",
        "tp_sl_same_candle": tp_sl_same_candle,
        "conflict_policy": conflict_policy,
        "use_candle_hilo": True,
    }
    if case == "CONFLICT":
        details_payload.update(
            {
                "candle_high": max(tp_price, sl_price) + 1.0,
                "candle_low": min(tp_price, sl_price) - 1.0,
                "alt_outcome_tp_first": "TP_hit",
                "alt_pnl_tp_first": tp_pct,
                "alt_outcome_sl_first": "SL_hit",
                "alt_pnl_sl_first": -sl_pct,
            }
        )
    if case == "PATH_ONLY":
        details_payload["path_1m"] = path_1m

    summary = {
        "run_id": run_id,
        "symbol": symbol,
        "entry_time_utc": entry_time_utc,
        "entry_price": entry_price,
        "entry_source": "test",
        "entry_type": "TEST",
        "tp_price": tp_price,
        "sl_price": sl_price,
        "tp_pct": tp_pct,
        "sl_pct": sl_pct,
        "end_reason": end_reason,
        "outcome": end_reason,
        "hit_time_utc": entry_time_utc,
        "minutes_to_hit": 0.0,
        "mfe_pct": 0.0,
        "mae_pct": 0.0,
        "timeout_exit_price": entry_price,
        "timeout_pnl_pct": 0.0,
        "trade_type": "PAPER",
        "exit_time_utc": entry_time_utc,
        "exit_price": entry_price if end_reason in ("TIMEOUT", "CONFLICT") else (tp_price if end_reason == "TP_hit" else sl_price),
        "pnl_pct": pnl_pct,
        "r_multiple": 0.0,
        "details_payload": json.dumps(details_payload, ensure_ascii=False),
        "path_1m": path_1m,
    }

    outcome_row = build_outcome_row(
        summary,
        trade_id=trade_id,
        event_id=event_id,
        run_id=run_id,
        symbol=symbol,
        strategy=strategy,
        mode=mode,
        side="SHORT" if strategy == "short_pump" else "LONG",
        outcome_time_utc=now_utc,
    )
    if outcome_row is None:
        raise RuntimeError("Outcome row was deduped unexpectedly")
    write_outcome_row(outcome_row, strategy=strategy, mode=mode, wall_time_utc=now_utc, schema_version=2)

    print(f"OK test_dataset_writer | strategy={strategy} | case={case} | symbol={symbol} | run_id={run_id}")


if __name__ == "__main__":
    main()
