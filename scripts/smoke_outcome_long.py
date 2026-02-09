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


def _fake_klines_1m(case: str, entry_ts: pd.Timestamp, entry_price: float, tp_price: float, sl_price: float) -> pd.DataFrame:
    rows = []
    if case == "CONFLICT":
        ts = entry_ts + pd.Timedelta(minutes=1)
        rows.append(
            {
                "ts": ts,
                "open": entry_price,
                "high": max(tp_price, sl_price) + 1.0,
                "low": min(tp_price, sl_price) - 1.0,
                "close": entry_price,
                "volume": 0.0,
            }
        )
    else:
        for i in range(5):
            ts = entry_ts + pd.Timedelta(minutes=i + 1)
            rows.append(
                {
                    "ts": ts,
                    "open": entry_price,
                    "high": entry_price + 1.0,
                    "low": entry_price - 1.0,
                    "close": entry_price,
                    "volume": 0.0,
                }
            )
    return pd.DataFrame(rows)


def main() -> None:
    cfg = Config()
    case = os.getenv("SMOKE_CASE", "PATH_ONLY").strip().upper()
    cfg.outcome_watch_minutes = 2
    cfg.outcome_poll_seconds = 1

    entry_ts = pd.Timestamp.now(tz="UTC")
    entry_price = 100.0
    tp_price = 110.0
    sl_price = 95.0
    conflict_policy = os.getenv("CONFLICT_POLICY") or ("NEUTRAL" if case == "CONFLICT" else None)

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
        conflict_policy=conflict_policy,
        run_id=run_id,
        symbol=symbol,
        category=cfg.category,
        fetch_klines_1m=lambda *_args, **_kwargs: _fake_klines_1m(case, entry_ts, entry_price, tp_price, sl_price),
        strategy_name=strategy,
    )
    if case == "PATH_ONLY":
        path_1m = []
        for i in range(5):
            ts = entry_ts + pd.Timedelta(minutes=i + 1)
            path_1m.append({"t": ts.isoformat(), "h": entry_price + 1.0, "l": entry_price - 1.0})
        summary["path_1m"] = path_1m
        try:
            payload = json.loads(summary.get("details_payload") or "{}")
        except Exception:
            payload = {}
        payload["path_1m"] = path_1m
        summary["details_payload"] = json.dumps(payload, ensure_ascii=False)
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
        details_json = rows[0].get("details_json", "")
    if "path_1m" not in details_json:
        raise RuntimeError("Expected path_1m in details_json")
    if case == "CONFLICT" and '"tp_sl_same_candle": 1' not in details_json:
        raise RuntimeError("Expected tp_sl_same_candle=1 in details_json for CONFLICT case")
    print(f"OK smoke_outcome_long case={case}")


if __name__ == "__main__":
    main()
