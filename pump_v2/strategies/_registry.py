"""Auto-discovery всех Strategy подклассов.
Реализация в Phase 3 когда появятся первые стратегии."""

from pump_v2.core.strategy_base import Strategy


def discover_strategies() -> dict[str, type[Strategy]]:
    """Сканирует pump_v2/strategies/*.py, импортирует,
    собирает все классы наследующие Strategy.
    Возвращает {strategy.name: StrategyClass}"""
    return {}  # TODO Phase 3
