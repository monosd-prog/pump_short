# pump_v2 — Architecture

Версия документа: 1.0 — 2026-05-14

## 1. Цели рефакторинга

**Что не работало в v1:**
- Три источника правды конфигурации (`live_config.json` + `.env` + `STRATEGIES=` в unit) → конфликты, забытые флаги
- Несовпадение имён (`false_pump_1R` vs `false_pump_live_1R` vs пустой profile) → false_pump молча не торговал
- Режимы `paper`, `accumulating`, `shadow` и две очереди сигналов → конфликт runner-ов
- Стратегия и risk-профиль слиты → нельзя одну стратегию запустить с разным sizing
- Нет signal funnel → диагностика "почему не торгует" вручную через grep
- Нет auto-disable → отрицательный EV месяцами в live (пример: short_pump EV −0.74 на N=23, fast0_selective деградировал с +0.05 до −0.12 на N=129)
- Нет общего интерфейса Strategy → backtest требует отдельной реализации, расхождения с live

**Целевые свойства v2:**
- Один источник правды: `config/strategies.yaml`
- Бинарный режим стратегии (`enabled: true/false`), без paper-режимов
- Strategy + RiskManager разделены
- Один класс Strategy работает в live и backtest идентично
- Signal funnel на каждом этапе
- Auto-disable правило закодировано, не в голове
- Plug-and-play стратегии: один файл = одна стратегия
- Self-monitoring OI (отказ от внешнего webhook)

## 2. Структура папок
/root/pump_short/pump_v2/
├── ARCHITECTURE.md
├── README.md
├── main_runner.py
│
├── core/
│   ├── strategy_base.py     # ABC Strategy, dataclass Signal
│   ├── indicator_base.py    # ABC Indicator
│   ├── market_context.py    # dataclass MarketContext
│   ├── data_feed.py         # ABC DataFeed, LiveDataFeed, BacktestDataFeed
│   ├── risk_manager.py      # (Phase 5)
│   ├── trade_executor.py    # (Phase 5)
│   └── funnel.py            # signal funnel logging
│
├── strategies/
│   ├── short_pump_mid.py
│   ├── short_pump_funding.py
│   ├── liquidation_short.py
│   ├── false_pump.py
│   └── _registry.py
│
├── indicators/
│   ├── oi_delta.py          # self-monitoring OI (Bybit WS/REST)
│   ├── cvd.py
│   ├── funding.py
│   ├── liquidations.py      # читает collector CSV
│   ├── wick.py
│   └── volume_profile.py
│
├── backtest/
│   ├── engine.py
│   ├── metrics.py
│   ├── data_loader.py
│   ├── promotion.py
│   └── reports/
│
├── config/
│   └── strategies.yaml
│
├── monitoring/
│   ├── dashboard.py
│   └── funnel_view.py
│
├── jobs/
│   ├── auto_disable.py
│   └── ev_monitor.py
│
├── deploy/systemd/
│   ├── pump-v2-runner.service
│   └── pump-v2-auto-disable.timer
│
├── tests/
└── logs/                     # gitignored, runtime artefacts

`datasets/` — общая с v1, доступ через пути из YAML (не symlink).
`collectors/` — в корне репо, независимый модуль.

## 3. Интерфейсы

### 3.1 Strategy

```python
@dataclass
class Signal:
    strategy: str          # = ключ в YAML
    symbol: str
    side: str              # "short" | "long"
    entry_price: float
    sl_price: float
    tp_price: float
    notional_usd: float
    leverage: int
    ts_utc: datetime
    metadata: dict

class Strategy(ABC):
    name: str              # snake_case, уникальное
    
    def __init__(self, params: dict, risk: dict): ...
    
    @abstractmethod
    def check_signal(self, ctx: MarketContext) -> Optional[Signal]: ...
    
    @abstractmethod
    def required_indicators(self) -> list[str]: ...
```

### 3.2 MarketContext

```python
@dataclass
class MarketContext:
    symbol: str
    ts_utc: datetime
    candles: dict          # {'1m': df, '5m': df, ...}
    oi: float
    oi_history: pd.Series
    funding: float
    cvd: float
    liquidations: list     # последние N ликвидаций по символу
    indicators: dict       # extensible
```

### 3.3 DataFeed

Абстракция, делающая Strategy идентичной в live и backtest.

```python
class DataFeed(ABC):
    @abstractmethod
    def get_context(self, symbol: str, ts: datetime) -> MarketContext: ...
    
    @abstractmethod
    def get_universe(self, ts: datetime) -> list[str]: ...

class LiveDataFeed(DataFeed):
    """Real-time источники:
    - REST Bybit для klines (с кэшем)
    - WS Bybit для OI / funding (incremental)
    - liquidations.csv tail для ликвидаций
    - Internal computation для CVD"""

class BacktestDataFeed(DataFeed):
    """Из исторических файлов:
    - datasets/klines/{symbol}.parquet
    - datasets/oi_history/{symbol}.parquet
    - datasets/liquidations.csv (от collector'а)
    - datasets/funding/{symbol}.parquet"""
```

**Контракт:** Стратегия не знает live это или backtest. Один и тот же `check_signal()` через два feed'а → одинаковые сигналы (валидация в Phase 6).

### 3.4 Indicator

```python
class Indicator(ABC):
    name: str
    
    @abstractmethod
    def compute(self, symbol: str, ts: datetime, history) -> Any: ...
```

### 3.5 RiskManager (Phase 5)

```python
class RiskManager:
    def evaluate(self, signal: Signal) -> tuple[bool, str]:
        """Проверки: max open positions, max daily loss, max DD,
        cooldown after loss, duplicate position on symbol.
        Возвращает: (allowed, reason_if_blocked)"""
    
    def size(self, signal: Signal) -> Signal:
        """Применить sizing из YAML, вернуть обогащённый Signal."""
```

### 3.6 TradeExecutor (Phase 5)

```python
class TradeExecutor:
    def execute(self, signal: Signal) -> ExecutionResult: ...
    def manage_open_positions(self) -> None: ...
```

## 4. Signal funnel

Каждая стратегия проходит фиксированные стадии. Каждый переход — запись в `pump_v2/logs/funnel_log.csv`:

| stage | meaning |
|---|---|
| `candidate_scanned` | Символ прошёл pre-screen |
| `signal_generated` | `check_signal()` вернул не-None |
| `risk_passed` | `RiskManager.evaluate()` разрешил |
| `position_filter_passed` | Нет уже открытой позиции на этом символе |
| `sent_to_exchange` | Ордер отправлен |
| `order_executed` | Ордер filled |
| `closed` | Позиция закрыта (TP/SL/timeout) |

Колонки: `ts_utc, strategy, symbol, stage, blocked_reason`

Дашборд читает этот лог → воронка по каждой стратегии за период. Решает "false_pump молча не торгует" — сразу видно стадию обрыва.

## 5. Конфигурация

`config/strategies.yaml` — единственный источник.

```yaml
runner:
  tick_interval_sec: 5
  symbols_universe: "bybit_usdt_perp_top200"

data_paths:
  base: "/root/pump_short/datasets"
  liquidations: "/root/pump_short/datasets/liquidations.csv"
  events_v3: "/root/pump_short/datasets/events_v3.csv"
  trades_v3: "/root/pump_short/datasets/trades_v3.csv"
  trading_closes: "/root/pump_short/datasets/trading_closes.csv"

global_risk:
  max_open_positions: 5
  max_daily_loss_r: -3.0
  max_drawdown_r: -8.0
  cooldown_after_loss_min: 0

auto_disable:
  enabled: true
  min_trades: 50
  ev_r_floor: -0.05
  lookback_days: 30
  check_time_utc: "00:00"

telegram:
  alerts_enabled: true
  daily_report_time_utc: "10:00"

strategies:
  short_pump_mid:
    enabled: false  # пока не реализована в v2
    risk:
      r_multiplier: 1.0
      notional_usd: 100
      leverage: 10
      sl_pct: 5.0
      tp_pct: 5.0
    params:
      min_pump_pct: 8.0
      stage: 4
      max_dist_to_peak_pct: 3.5
  
  # ... (другие стратегии аналогично)
```

**Запрещено в v2:**
- `.env`-флаги управления стратегиями (`*_ENABLE`)
- `STRATEGIES=` в unit-файле
- Множественные risk-профили в коде на одну стратегию
- Любые "режимы" кроме бинарного `enabled`

## 6. Backtest engine

```python
def run_backtest(
    strategy: Strategy,
    data_from: datetime,
    data_to: datetime,
    risk_manager: RiskManager,
) -> BacktestResult: ...

@dataclass
class BacktestResult:
    n_trades: int
    ev_r: float
    win_rate: float
    max_drawdown_r: float
    sharpe: float
    equity_curve: pd.Series
    trades: list
    funnel_summary: dict
```

### Promotion criteria

```python
PROMOTION_CRITERIA = {
    "min_trades": 50,
    "min_ev_r": 0.05,
    "max_drawdown_r": -8.0,
}
```

Стратегия должна пройти `is_promotable()` на backtest перед `enabled: true`.

## 7. Auto-disable

Daily systemd timer (00:00 UTC):

```python
def daily_check():
    for strategy_name in get_enabled_strategies():
        n, ev_r = compute_live_metrics(strategy_name, lookback_days=30)
        if n >= 50 and ev_r < -0.05:
            set_strategy_enabled(strategy_name, False)
            send_telegram_alert(f"AUTO_DISABLE: {strategy_name} N={n} EV_R={ev_r:.4f}")
            git_commit(f"auto-disable {strategy_name}")
```

## 8. Migration plan (вариант A — параллельный запуск)

### Phase 1 — Skeleton ✅
- pump_v2/ структура
- Базовые классы (Strategy, Indicator, MarketContext, DataFeed, funnel)
- config/strategies.yaml stub
- main_runner.py scaffold

### Phase 2 — Indicators
- Перенести индикаторы из v1 как изолированные модули
- Каждый — отдельный файл, без зависимостей от runner v1
- Self-monitoring OI через Bybit WS/REST (отказ от внешнего /pump webhook)
- Golden-data тесты

### Phase 3 — Strategies
- Перенести `short_pump_mid` и `short_pump_funding` в новые модули
- Через `check_signal(ctx) -> Signal | None`
- Тесты: те же входы → те же сигналы что в v1

### Phase 4 — Backtest engine
- Engine + metrics + data_loader + LiveDataFeed/BacktestDataFeed
- Прогон 2 перенесённых стратегий на 30-дневной истории
- **Validation gate:** результат бэктеста совпадает с реальными сделками v1 в пределах ±20% EV_R и ±15% N. Иначе — bug.

### Phase 5 — Trade executor + Risk manager
- Bybit execution wrapper
- Risk checks
- Funnel logging интегрировано

### Phase 6 — Parallel run
- v1: 2 стратегии × `notional_usd × 0.5`
- v2: те же 2 стратегии × `notional_usd × 0.5`
- 48-72 часа параллельно
- **Validation gate:** v2 генерирует те же сигналы что v1 в real-time (±шум по таймингу)

### Phase 7 — Switch
- `systemctl stop pump-short-live-auto`
- `systemctl start pump-v2-runner`
- v1 disabled, остаётся 30 дней как backup

### Phase 8 — New strategies
- `liquidation_short`: данные от collector'а → backtest → промоушн → live
- `false_pump`: пересмотр логики → backtest → промоушн → live

## 9. Naming convention
strategy_name:   snake_case, в YAML, коде, логах, CSV
✓ short_pump_mid, liquidation_short
✗ ShortPumpMid, short-pump-mid
class:           PascalCase + "Strategy"
✓ class ShortPumpMidStrategy(Strategy)
file:            strategies/{strategy_name}.py
yaml key:        strategies.{strategy_name}
csv column:      strategy = "{strategy_name}"
log prefix:      [{strategy_name}]

**Invariant:** `Strategy.name == YAML key == filename == CSV value`. Startup CI: `assert all_match()`.

## 10. Что НЕ переносим из v1

- ❌ paper / accumulating / shadow режимы
- ❌ `*_ENABLE` env-флаги на стратегии
- ❌ `STRATEGIES=...` в unit-файле
- ❌ guard state (заменён бинарным `enabled`)
- ❌ Отдельные risk-профили в коде (`*_1R`, `*_2R`, и т.д.) — всё в YAML
- ❌ `controlled_test` / `controlled_test_hit` — заменено backtest engine
- ❌ Две очереди сигналов (paper + live)
- ❌ Несколько runner-юнитов — один `pump-v2-runner.service`
- ❌ Внешний OI webhook на `/pump` — заменён self-monitoring через Bybit API

## 11. Resolved decisions

- **MarketContext data feed:** один MarketContext per (symbol, tick). Создаётся свежим каждые `tick_interval_sec` секунд для каждого символа universe. Immutable за время одного tick. DataFeed абстракция делает live/backtest идентичными.
- **OI source:** self-monitoring через Bybit REST `/v5/market/open-interest` + WS `tickers.{symbol}`. Никаких внешних webhook. `/pump` endpoint в v1 остаётся работать до Phase 7, после — отключаем.
- **`datasets/` access:** прямые пути в `config/strategies.yaml` под ключом `data_paths`. Не symlink — конфиг явный, легко переопределяется для тестов.

---
ВЕРСИОНИРОВАНИЕ ДОКУМЕНТА:
v1.0 — 2026-05-14 — initial finalized version (resolves open questions from skeleton)
