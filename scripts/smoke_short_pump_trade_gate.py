"""Smoke: short_pump trade row write gated by is_tradeable_short_pump (stage=4, dist>=3.5)."""
from __future__ import annotations

import csv
import os
import sys
import tempfile
from pathlib import Path

_repo_root = Path(__file__).resolve().parents[1]
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

os.environ["TG_ENTRY_DIST_MIN"] = "3.5"
os.environ["EXECUTION_MODE"] = "paper"

from common.io_dataset import get_dataset_dir, write_trade_row
from short_pump.telegram import is_tradeable_short_pump


def main() -> None:
    strategy = "short_pump"
    wall_time_utc = "2026-02-26T12:00:00Z"

    with tempfile.TemporaryDirectory(prefix="smoke_trade_gate_") as base:
        path = get_dataset_dir(strategy, wall_time_utc, base_dir=base)
        trades_file = Path(path) / "trades_v3.csv"

        # stage=3 or dist<3.5 -> not tradeable -> must NOT write trade (watcher gate)
        for stage, dist in [(3, 5.0), (4, 3.4), (4, None)]:
            tradeable = is_tradeable_short_pump(stage, dist)
            assert tradeable is False, f"stage={stage} dist={dist} must not be tradeable"
            # Simulate watcher: only write when tradeable
            if tradeable:
                write_trade_row(
                    {
                        "trade_id": "would_be_wrong",
                        "event_id": "evt",
                        "run_id": "run",
                        "symbol": "BTCUSDT",
                        "strategy": strategy,
                        "side": "SHORT",
                        "entry_time_utc": wall_time_utc,
                        "entry_price": 1000.0,
                        "tp_price": 994.0,
                        "sl_price": 1006.0,
                        "trade_type": "PAPER",
                    },
                    strategy=strategy,
                    mode="live",
                    wall_time_utc=wall_time_utc,
                    base_dir=base,
                    schema_version=3,
                )
        # No trade row should exist (we never wrote when not tradeable)
        rows = []
        if trades_file.exists():
            with open(trades_file, encoding="utf-8") as f:
                rows = list(csv.DictReader(f))
        assert len(rows) == 0, (
            f"stage=3 or dist<3.5 must not produce short_pump trade row; found {len(rows)}"
        )
        print("OK: no trade row when stage!=4 or dist<3.5")

        # stage=4, dist=4 -> tradeable -> write trade
        tradeable = is_tradeable_short_pump(4, 4.0)
        assert tradeable is True
        write_trade_row(
            {
                "trade_id": "smoke_trade_ok",
                "event_id": "evt_ok",
                "run_id": "run_ok",
                "symbol": "BTCUSDT",
                "strategy": strategy,
                "side": "SHORT",
                "entry_time_utc": wall_time_utc,
                "entry_price": 1000.0,
                "tp_price": 994.0,
                "sl_price": 1006.0,
                "trade_type": "PAPER",
            },
            strategy=strategy,
            mode="live",
            wall_time_utc=wall_time_utc,
            base_dir=base,
            schema_version=3,
        )
        with open(trades_file, encoding="utf-8") as f:
            rows = list(csv.DictReader(f))
        assert len(rows) >= 1, "stage=4 dist=4 must produce trade row"
        assert rows[-1].get("mode") == "paper"
        print("OK: trade row written when stage=4 and dist>=3.5")

    print("smoke_short_pump_trade_gate: short_pump trade gating OK")


if __name__ == "__main__":
    main()
