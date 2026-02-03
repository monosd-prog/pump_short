from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

_repo_root = Path(__file__).resolve().parents[1]
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

try:
    from common.io_dataset import write_event_row, write_outcome_row, write_trade_row
except ModuleNotFoundError as e:
    print("Failed to import project modules. Try:")
    print(f"  PYTHONPATH={_repo_root} python {Path(__file__).name}")
    raise


def main() -> None:
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    # short_pump sample
    event_short = {
        "run_id": "smoke_run_short",
        "event_id": "evt_short_1",
        "symbol": "BTCUSDT",
        "strategy": "short_pump",
        "mode": "live",
        "side": "SHORT",
        "wall_time_utc": ts,
        "time_utc": ts,
        "stage": 3,
        "entry_ok": False,
        "skip_reasons": "stage_lt_2",
        "context_score": 0.5,
        "price": 65000,
        "dist_to_peak_pct": 1.2,
        "payload_json": json.dumps({"sample": True}),
    }
    write_event_row(event_short, strategy="short_pump", mode="live", wall_time_utc=ts, schema_version=2)

    trade_short = {
        "trade_id": "trade_short_1",
        "event_id": "evt_short_1",
        "run_id": "smoke_run_short",
        "symbol": "BTCUSDT",
        "strategy": "short_pump",
        "mode": "live",
        "side": "SHORT",
        "entry_time_utc": ts,
        "entry_price": 65000,
        "tp_price": 64000,
        "sl_price": 65500,
        "trade_type": "PAPER",
    }
    write_trade_row(trade_short, strategy="short_pump", mode="live", wall_time_utc=ts, schema_version=2)

    outcome_short = {
        "trade_id": "trade_short_1",
        "event_id": "evt_short_1",
        "run_id": "smoke_run_short",
        "symbol": "BTCUSDT",
        "strategy": "short_pump",
        "mode": "live",
        "side": "SHORT",
        "exit_time_utc": ts,
        "end_reason": "TP_hit",
        "pnl_pct": 1.5,
        "details_payload": json.dumps({"tp_hit": True}),
    }
    write_outcome_row(outcome_short, strategy="short_pump", mode="live", wall_time_utc=ts, schema_version=2)

    # long_pullback sample
    event_long = {
        "run_id": "smoke_run_long",
        "event_id": "evt_long_1",
        "symbol": "BTCUSDT",
        "strategy": "long_pullback",
        "mode": "live",
        "side": "LONG",
        "wall_time_utc": ts,
        "time_utc": ts,
        "stage": 1,
        "entry_ok": False,
        "skip_reasons": "stage_lt_2",
        "context_score": 0.3,
        "price": 65000,
        "payload_json": json.dumps({"sample": True}),
    }
    write_event_row(event_long, strategy="long_pullback", mode="live", wall_time_utc=ts, schema_version=2)

    trade_long = {
        "trade_id": "trade_long_1",
        "event_id": "evt_long_1",
        "run_id": "smoke_run_long",
        "symbol": "BTCUSDT",
        "strategy": "long_pullback",
        "mode": "live",
        "side": "LONG",
        "entry_time_utc": ts,
        "entry_price": 65000,
        "tp_price": 66000,
        "sl_price": 64500,
        "trade_type": "PAPER",
    }
    write_trade_row(trade_long, strategy="long_pullback", mode="live", wall_time_utc=ts, schema_version=2)

    outcome_long = {
        "trade_id": "trade_long_1",
        "event_id": "evt_long_1",
        "run_id": "smoke_run_long",
        "symbol": "BTCUSDT",
        "strategy": "long_pullback",
        "mode": "live",
        "side": "LONG",
        "exit_time_utc": ts,
        "end_reason": "TIMEOUT",
        "pnl_pct": -0.2,
        "details_payload": json.dumps({"timeout": True}),
    }
    write_outcome_row(outcome_long, strategy="long_pullback", mode="live", wall_time_utc=ts, schema_version=2)

    print("smoke_dataset_v2 done")


if __name__ == "__main__":
    main()
