from __future__ import annotations

import csv
import os
import uuid
from datetime import datetime, timezone

import pandas as pd

from common.io_dataset import write_outcome_row
from common.outcome_tracker import build_outcome_row, track_outcome
from common.runtime import wall_time_utc
from long_pullback.config import Config


def _dataset_dir(strategy: str, mode: str, wall_time: str) -> str:
    try:
        dt = datetime.fromisoformat(wall_time.replace("Z", "+00:00"))
        day = dt.strftime("%Y%m%d")
    except Exception:
        day = "unknown_date"
    return os.path.join("datasets", f"date={day}", f"strategy={strategy}", f"mode={mode}")


def _fake_klines_1m(*_args, **_kwargs) -> pd.DataFrame:
    now = pd.Timestamp.now(tz="UTC")
    rows = []
    for i in range(5):
        ts = now - pd.Timedelta(minutes=5 - i)
        rows.append(
            {
                "ts": ts,
                "open": 100.0,
                "high": 101.0,
                "low": 99.0,
                "close": 100.0,
                "volume": 0.0,
            }
        )
    return pd.DataFrame(rows)


def main() -> None:
    cfg = Config()
    cfg.outcome_watch_minutes = 0
    cfg.outcome_poll_seconds = 1

    entry_ts = pd.Timestamp.now(tz="UTC")
    entry_price = 100.0
    tp_price = 110.0
    sl_price = 95.0

    trade_id = f"{uuid.uuid4()}_trade"
    event_id = str(uuid.uuid4())
    run_id = "smoke_long_outcome"
    symbol = "TESTUSDT"
    strategy = "long_pullback"
    mode = "live"

    summary = track_outcome(
        cfg,
        side="long",
        entry_ts_utc=entry_ts,
        entry_price=entry_price,
        tp_price=tp_price,
        sl_price=sl_price,
        entry_source="1m",
        entry_type="PULLBACK",
        conflict_policy=os.getenv("CONFLICT_POLICY"),
        run_id=run_id,
        symbol=symbol,
        category=cfg.category,
        fetch_klines_1m=_fake_klines_1m,
        strategy_name=strategy,
    )
    summary["hold_seconds"] = 0.0

    outcome_time_utc = summary.get("exit_time_utc") or summary.get("hit_time_utc") or wall_time_utc()
    outcome_row = build_outcome_row(
        summary,
        trade_id=trade_id,
        event_id=event_id,
        run_id=run_id,
        symbol=symbol,
        strategy=strategy,
        mode=mode,
        side="LONG",
        outcome_time_utc=outcome_time_utc,
    )
    if outcome_row is None:
        raise RuntimeError("Outcome row was deduped unexpectedly")
    write_outcome_row(outcome_row, strategy=strategy, mode=mode, wall_time_utc=outcome_time_utc, schema_version=2)

    base_dir = _dataset_dir(strategy, mode, outcome_time_utc)
    path = os.path.join(base_dir, "outcomes_v2.csv")
    with open(path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = [row for row in reader if row.get("trade_id") == trade_id]
        if len(rows) != 1:
            raise RuntimeError(f"Expected 1 row for trade_id, got {len(rows)}")
        header = reader.fieldnames or []
        for col in ("hold_seconds", "mae_pct", "mfe_pct"):
            if col not in header:
                raise RuntimeError(f"Missing column in outcomes_v2.csv: {col}")
    print("OK smoke_outcome_long")


if __name__ == "__main__":
    main()
