from typing import Any, Dict

import pandas as pd

from common.outcome_tracker import track_outcome
from short_pump.bybit_api import get_klines_1m
from short_pump.config import Config


def track_outcome_short(
    cfg: Config,
    entry_ts_utc: pd.Timestamp,
    entry_price: float,
    tp_price: float,
    sl_price: float,
    entry_source: str = "unknown",
    entry_type: str = "unknown",
    run_id: str = "",
    symbol: str = "",
) -> Dict[str, Any]:
    return track_outcome(
        cfg,
        side="short",
        entry_ts_utc=entry_ts_utc,
        entry_price=entry_price,
        tp_price=tp_price,
        sl_price=sl_price,
        entry_source=entry_source,
        entry_type=entry_type,
        run_id=run_id,
        symbol=symbol,
        category=cfg.category,
        fetch_klines_1m=get_klines_1m,
        strategy_name="short_pump",
    )