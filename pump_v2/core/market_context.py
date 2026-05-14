from dataclasses import dataclass, field
from datetime import datetime
from typing import Any


@dataclass
class MarketContext:
    symbol: str
    ts_utc: datetime
    candles: dict = field(default_factory=dict)  # {'1m': df, '5m': df, ...}
    oi: float = 0.0
    oi_history: Any = None  # pd.Series
    funding: float = 0.0
    cvd: float = 0.0
    liquidations: list = field(default_factory=list)
    indicators: dict = field(default_factory=dict)  # extensible
