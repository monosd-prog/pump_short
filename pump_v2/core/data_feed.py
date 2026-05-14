"""Abstract data feed; Live / backtest implementations in later phases."""

from abc import ABC, abstractmethod


class DataFeed(ABC):
    """Abstract market data feed."""

    @abstractmethod
    def connect(self) -> None:
        ...

    @abstractmethod
    def close(self) -> None:
        ...


class LiveDataFeed(DataFeed):
    def connect(self) -> None:
        raise NotImplementedError

    def close(self) -> None:
        raise NotImplementedError


class BacktestDataFeed(DataFeed):
    def connect(self) -> None:
        raise NotImplementedError

    def close(self) -> None:
        raise NotImplementedError
