from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Optional


@dataclass
class Signal:
    strategy: str
    symbol: str
    side: str  # "short" | "long"
    entry_price: float
    sl_price: float
    tp_price: float
    notional_usd: float
    leverage: int
    ts_utc: datetime
    metadata: dict = field(default_factory=dict)


class Strategy(ABC):
    name: str = ""  # переопределяется в подклассах, snake_case

    def __init__(self, params: dict, risk: dict):
        self.params = params
        self.risk = risk
        if not self.name:
            raise ValueError(f"{self.__class__.__name__}.name must be set")

    @abstractmethod
    def check_signal(self, ctx: "MarketContext") -> Optional[Signal]:
        ...

    @abstractmethod
    def required_indicators(self) -> list[str]:
        ...
