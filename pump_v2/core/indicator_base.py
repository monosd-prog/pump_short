from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any


class Indicator(ABC):
    name: str = ""

    @abstractmethod
    def compute(self, symbol: str, ts: datetime, history: Any) -> Any:
        ...
