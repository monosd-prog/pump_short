# pump_v2/indicators — contracts (Phase 2 step 1)

Документ фиксирует **одну каноническую реализацию** на индикатор/метрику для `pump_v2/indicators/`.  
Цель: убрать дубли v1 (`short_pump/features.py` vs `common/market_features.py`, разные семантики `context_score`, два пути ликвидаций и т.д.).

**Версия:** draft — 2026-05-14 (обновлено решениями open questions)  
**Статус:** контракт для ревью; `.py` модули индикаторов этим коммитом **не** создаются.

**Схема parquet / CSV путей:** [`../datasets/SCHEMA.md`](../datasets/SCHEMA.md)  
Корень данных: **`/root/pump_short/datasets/`** (общий с v1).

---

## Resolved decisions (2026-05-14)

Краткое резюме закрытых вопросов (детали — в разделах ниже):

1. **`oi_monitor` vs `oi_change_pct`** — см. [NEW IN V2](#new-in-v2) и [Data source vs Indicator](#data-source-vs-indicator-oi_monitor-and-oi_change_pct); `oi_monitor` — не `Indicator`, `oi_change_pct` читает `MarketContext.oi_history`.
2. **Backtest paths** — единая схема путей; каждый индикатор ниже ссылается на [`SCHEMA.md`](../datasets/SCHEMA.md) и полный путь.
3. **`dist_to_peak`** — два индикатора: [FSM: `dist_to_fsm_peak_pct`](#fsm-dist_to_fsm_peak_pct) и [Window: `dist_to_window_peak_pct`](#window-dist_to_window_peak_pct); generic `peak_mode` **не** вводим ([DEPRECATED](#deprecated)).
4. **`CVD5m`** — параметры в конструкторе, defaults из v1 (`bar_size_sec=60`, `window_bars=5`); см. [CVD](#cvd-cvd_delta_ratio-and-cvd5m).
5. **`liquidation_rollups`** — фиксированный dict ключей, только **`liquidations.csv`**; см. [Liquidation rollups](#liquidation-rollups).
6. **`compute_cvd_part`** — не существует в v2; логика в **`context_score_5m`** composite; см. [Composite context_score_5m](#composite-context_score_5m) и [DEPRECATED](#deprecated).
7. **LS / orderbook / footprint / volume_profile** — вне Phase 2; см. [OUT OF SCOPE (Phase 8+)](#out-of-scope-phase-8).

---

## Data source vs Indicator: oi_monitor and oi_change_pct

- **`OiMonitor`** (файл в Phase 2 step 4: `pump_v2/data_sources/oi_monitor.py`) — **не** наследует `Indicator`. Это **live data source**: подписка на Bybit WS/REST, обновляет **`MarketContext.oi`** (текущее значение) и **`MarketContext.oi_history`** (ряд для lookback). Нет `compute()` в смысле индикатора — есть tick/update в цикле раннера или async worker.
- **`oi_change_pct`** — обычный **`Indicator`**: чистая функция от **уже заполненного** `oi_history` (live из контекста после `OiMonitor`, backtest из parquet). Так разделяются **сбор данных** и **расчёт производной**.

---

## Сводная таблица

| ID | Каноническое имя v2 | Канон v1 (файл) | Live источник | Backtest источник |
|----|---------------------|-----------------|---------------|-------------------|
| а | `oi_change_pct` | `short_pump/features.py` | `MarketContext.oi_history` (наполняет **`OiMonitor`**) | `/root/pump_short/datasets/oi_history/{symbol}.parquet` |
| б | `oi_divergence_5m` | `short_pump/features.py` | производное (OI% + **`dist_to_fsm_peak_pct`**) | как live |
| в | `delta_ratio` | `short_pump/features.py` | REST `/v5/market/recent-trade` | `/root/pump_short/datasets/trades/{symbol}/{date}.parquet` |
| г | `cvd_delta_ratio` + `CVD5m` | `short_pump/features.py` + `common/market_features.cvd_5m` | recent-trade → DF в контексте | trades parquet |
| д | `atr_pct_5m_14` | `short_pump/features.py` (atr/atr_pct на DF) | REST kline `5` | `/root/pump_short/datasets/klines/{symbol}/5m.parquet` |
| е | `volume_zscore` | `short_pump/features.py` | kline 5m | `/root/pump_short/datasets/klines/{symbol}/5m.parquet` |
| ж | `funding_snapshot` | `common/market_features.normalize_funding` (3-tuple) | REST `/v5/market/tickers` | `/root/pump_short/datasets/funding/{symbol}.parquet` |
| з | `pump_shape_5m` | `common/market_features.pump_shape_features_5m` | REST kline 5m | `/root/pump_short/datasets/klines/{symbol}/5m.parquet` |
| и | `liquidation_rollups` | контракт полей + CSV | **`/root/pump_short/datasets/liquidations.csv`** | тот же путь |
| к | `structure_state` | `short_pump/context5m.py` | 5m цены в контекст | `/root/pump_short/datasets/klines/{symbol}/5m.parquet` |
| л1 | `dist_to_fsm_peak_pct` | `context5m` + entry | FSM `peak_price` + close | klines 5m + FSM replay |
| л2 | `dist_to_window_peak_pct` | `false_pump/detector` peak логика | max(high) за окно **N** минут (из YAML стратегии) | `/root/pump_short/datasets/klines/{symbol}/1m.parquet` (или 5m по выбору стратегии) |
| м | `context_score_5m` | `short_pump/context5m.compute_context_score_5m` | composite (внутри: stage, near_top, oi, vol, atr, **вклад CVD из `CVD5m`**) | те же входы с backtest feed |

---

## а) `oi_change_pct`

| Поле | Значение |
|------|----------|
| **Каноническое имя v2** | `oi_change_pct` |
| **Реализации v1 (все)** | `short_pump/features.py` **73–114**; `common/market_features.py` **308–330** |
| **Канон для v2** | **`short_pump/features.py`** — совместим с `context5m.build_dbg5` (**217–218**). |
| **Устаревает** | Реализация в **`common/market_features.py`**; alias **oi_delta** не вводим. |
| **Сигнатура v2** | `compute(symbol: str, ts: datetime, history: MarketContext \| OiHistoryFrame) -> float \| None` — чтение **`oi_history`** с колонками согласно [`SCHEMA.md`](../datasets/SCHEMA.md) (`ts_utc`, `oi` / `openInterest` после нормализации в feed). |
| **Live** | Ряд OI из **`MarketContext.oi_history`**, наполняемого **`pump_v2/data_sources/oi_monitor.py`** (WS tickers + REST open-interest); не ходит в сеть внутри `compute`. |
| **Backtest** | **`/root/pump_short/datasets/oi_history/{symbol}.parquet`** |

---

## б) `oi_divergence_5m`

| Поле | Значение |
|------|----------|
| **Каноническое имя v2** | `oi_divergence_5m` |
| **Реализации v1** | `short_pump/features.py` **117–138**; используется в `context5m.build_dbg5` **218–219** |
| **Канон для v2** | **`short_pump/features.py`**. |
| **Устаревает** | Дубли логики вне функции. |
| **Сигнатура v2** | `compute(symbol: str, ts: datetime, history: OiDiv5mInputs) -> bool` — входы: `oi_change_5m_pct`, **`dist_to_fsm_peak_pct`** (для short_pump канона вместо смешанного «dist_to_peak»). |
| **Live** | Производное от OI + FSM distance. |
| **Backtest** | `oi_history` + klines 5m + FSM state. |

---

## в) `delta_ratio`

| Поле | Значение |
|------|----------|
| **Каноническое имя v2** | `delta_ratio` |
| **Реализации v1** | `short_pump/features.py` **15–26**; `common/market_features.py` **233–246** |
| **Канон для v2** | **`short_pump/features.py`** — используется `entry.py`, `false_pump/detector`. |
| **Устаревает** | **`common/market_features.delta_ratio`**. |
| **Сигнатура v2** | `compute(symbol: str, ts: datetime, history: TradesFrame) -> float` — окно задаётся параметрами индикатора. |
| **Live** | REST **`/v5/market/recent-trade`**. |
| **Backtest** | **`/root/pump_short/datasets/trades/{symbol}/{date}.parquet`** (агрегация дат по `ts` вокруг `ts`). |

---

## CVD: `cvd_delta_ratio` and `CVD5m`

| Поле | Значение |
|------|----------|
| **Канонические имена v2** | `cvd_delta_ratio`; класс **`CVD5m`** (`Indicator`) |
| **Реализации v1** | `cvd_delta_ratio`: `short_pump/features.py` **141–167**; дубль `common/market_features.py` **249–262**. **`cvd_5m`**: `common/market_features.py` **265–305**. **`compute_cvd_part`**: `short_pump/features.py` **170–197** — **не переносится** ([Composite context_score_5m](#composite-context_score_5m), [DEPRECATED](#deprecated)). |
| **Канон для v2** | **`cvd_delta_ratio`** — `short_pump/features.py`. **`CVD5m`** — один модуль с алгоритмом **`common/market_features.cvd_5m`**, возвращает **`float | None`**: канонически **нормализованное отношение `cvd_ratio`** (второй элемент tuple в v1), которое потребляет **`context_score_5m`**. |
| **Устаревает** | Дубль `cvd_delta_ratio` в common; **`compute_cvd_part`** как отдельная функция. |
| **Параметры конструктора `CVD5m` (канон = v1)** | `bar_size_sec: int = 60` (агрегация сделок в 1m бары), `window_bars: int = 5` (5×1m = 5m окно, как в `cvd_5m`). Стратегия может переопределить через свой YAML. |
| **Пример** | `class CVD5m(Indicator):`<br>`    def __init__(self, bar_size_sec: int = 60, window_bars: int = 5) -> None: ...` |
| **Сигнатура v2** | `cvd_delta_ratio.compute(symbol, ts, history: TradesFrame, *, window) -> float \| None`; `CVD5m.compute(symbol, ts, history: TradesFrame) -> float \| None` |
| **Live** | recent-trade. |
| **Backtest** | **`/root/pump_short/datasets/trades/{symbol}/{date}.parquet`** |

---

## д) `atr_pct_5m_14`

| Поле | Значение |
|------|----------|
| **Каноническое имя v2** | `atr_pct_5m_14` |
| **Реализации v1** | `short_pump/features.py` **30–70**; `context5m._atr_pct_14` **137–153** (list dict — заменить адаптером → DF) |
| **Канон для v2** | **`short_pump/features.atr` + `atr_pct`** на **DataFrame**. |
| **Устаревает** | Дублирующая математика `_atr_pct_14`. |
| **Сигнатура v2** | `compute(symbol: str, ts: datetime, history: CandlesFrame5m) -> float \| None` |
| **Live** | REST **`/v5/market/kline`** `interval=5`. |
| **Backtest** | **`/root/pump_short/datasets/klines/{symbol}/5m.parquet`** |

---

## е) `volume_zscore`

| Поле | Значение |
|------|----------|
| **Каноническое имя v2** | `volume_zscore` |
| **Реализации v1** | `short_pump/features.py` **8–12**; `context5m._volume_z` **156–167** |
| **Канон для v2** | **`short_pump/features.volume_zscore`**. |
| **Устаревает** | `_volume_z` как отдельная формула. |
| **Сигнатура v2** | `compute(symbol: str, ts: datetime, history: CandlesFrame5m, *, lookback: int = 48) -> float` |
| **Live** | kline 5m. |
| **Backtest** | **`/root/pump_short/datasets/klines/{symbol}/5m.parquet`** |

---

## ж) `funding_snapshot`

| Поле | Значение |
|------|----------|
| **Каноническое имя v2** | `funding_snapshot` |
| **Реализации v1** | `short_pump/features.normalize_funding` **200–234** (2-tuple); `common/market_features.normalize_funding` **351–381** (3-tuple) |
| **Канон для v2** | **`common/market_features.normalize_funding`** (rate, ts_utc, **`funding_rate_abs`**). |
| **Устаревает** | 2-tuple из `short_pump/features.py`. |
| **Сигнатура v2** | `compute(symbol: str, ts: datetime, history: FundingPayload) -> FundingSnapshot` |
| **Live** | REST **`/v5/market/tickers`**. |
| **Backtest** | **`/root/pump_short/datasets/funding/{symbol}.parquet`** |

---

## з) `pump_shape_5m`

| Поле | Значение |
|------|----------|
| **Каноническое имя v2** | `pump_shape_5m` |
| **Реализации v1** | `common/market_features.pump_shape_features_5m` **98–160** |
| **Канон для v2** | **`common/market_features`**. |
| **Сигнатура v2** | `compute(symbol: str, ts: datetime, history: CandlesFrame5m, *, lookback: int = 5) -> PumpShape5m` |
| **Live** | kline 5m. |
| **Backtest** | **`/root/pump_short/datasets/klines/{symbol}/5m.parquet`** |

---

## Liquidation rollups

| Поле | Значение |
|------|----------|
| **Каноническое имя v2** | `liquidation_rollups` |
| **Реализации v1** | `common/market_features.liquidation_features` **512–547** (форма результата); источник v1 WS — **не** канон для v2. |
| **Канон для v2** | Агрегация **только** из **`/root/pump_short/datasets/liquidations.csv`** (collector). Счётчики и USD по сторонам и окнам 30s/60s. |
| **Возвращаемый dict (канон ключей)** | `{ "long_count_30s", "long_usd_30s", "short_count_30s", "short_usd_30s", "long_count_60s", "long_usd_60s", "short_count_60s", "short_usd_60s" }` — значения `int` для count, `float` для usd. |
| **Устаревает** | In-memory WS **`short_pump/liquidations.py`** как источник для v2. |
| **Сигнатура v2** | `compute(symbol: str, ts: datetime, history: LiquidationsCsvReader) -> dict[str, int \| float]` |
| **Live** | **`/root/pump_short/datasets/liquidations.csv`** |
| **Backtest** | **`/root/pump_short/datasets/liquidations.csv`** (или фиксированный снимок того же формата для reproducibility) |

---

## к) `structure_state` (FSM)

| Поле | Значение |
|------|----------|
| **Каноническое имя v2** | `structure_state` / `StructureFsm.step` |
| **Реализации v1** | `short_pump/context5m.py` **20–40**, **49–130** |
| **Канон для v2** | **`context5m`**. |
| **Сигнатура v2** | `step(symbol: str, ts: datetime, history: StructureStepInput) -> StructureState` |
| **Live** | 5m close из контекста / REST kline. |
| **Backtest** | **`/root/pump_short/datasets/klines/{symbol}/5m.parquet`** |

---

## FSM: `dist_to_fsm_peak_pct`

| Поле | Значение |
|------|----------|
| **Каноническое имя v2** | `dist_to_fsm_peak_pct` |
| **Реализации v1** | `context5m.build_dbg5` **204**; `update_structure` **79–80**; `entry.py` относительно `peak_price_5m` синхронизирован с FSM |
| **Канон для v2** | Расстояние от текущей цены до **`StructureState.peak_price`** (FSM), формула \((peak - price) / peak \times 100\). **short_pump** стратегии. |
| **Сигнатура v2** | `compute(symbol: str, ts: datetime, history: MarketContext, peak_value_or_window: float) -> float` — **`peak_value_or_window`** = текущий FSM peak (float), передаётся явно из состояния FSM после `step`. |
| **Live** | FSM + last price. |
| **Backtest** | klines 5m + FSM replay. |

---

## Window: `dist_to_window_peak_pct`

| Поле | Значение |
|------|----------|
| **Каноническое имя v2** | `dist_to_window_peak_pct` |
| **Реализации v1** | `false_pump/detector.py` **119–121**; `false_pump/watcher._peak_price_5m` **172–182** (источник peak для окна) |
| **Канон для v2** | Расстояние до **`max(high)`** за скользящее окно **N минут** (параметр стратегии **false_pump**, YAML). **false_pump** и любые стратегии без FSM-peak. |
| **Сигнатура v2** | `compute(symbol: str, ts: datetime, history: CandlesFrame1m \| CandlesFrame5m, peak_value_or_window: WindowPeakSpec) -> float` — `WindowPeakSpec`: например `{ "minutes": int, "timeframe": "1m" \| "5m" }`. |
| **Live** | klines из feed. |
| **Backtest** | **`/root/pump_short/datasets/klines/{symbol}/1m.parquet`** или **`.../5m.parquet`** согласно `WindowPeakSpec.timeframe`. |

---

## Composite: `context_score_5m`

| Поле | Значение |
|------|----------|
| **Каноническое имя v2** | `context_score_5m` |
| **Реализации v1** | **`short_pump/context5m.compute_context_score_5m` 249–305**; внутри v1 CVD-вклад приходил из **`compute_cvd_part`** в `entry.py` — в v2 эта часть **встроена** в composite: используется выход **`CVD5m`** (и пороги из YAML стратегии), без отдельной глобальной функции `compute_cvd_part`. |
| **Канон для v2** | Один composite **`Indicator`** / сервис с теми же весами stage / near_top / oi / vol / atr, плюс согласованный вклад CVD из **`CVD5m`**. |
| **Устаревает** | **`compute_cvd_part`** как публичный контракт; **`common/context_score`** для pump v2; «context_score» false_pump flags ratio → **`false_pump_flags_score`**. |
| **Сигнатура v2** | `compute(symbol: str, ts: datetime, history: Dbg5Row \| MarketContext) -> tuple[float, dict[str, float]]` |
| **Live** | Поля dbg5 / контекст после дочерних индикаторов. |
| **Backtest** | Идентичный pipeline на parquet. |

---

## DEPRECATED

| Элемент v1 | Решение в v2 |
|------------|----------------|
| **`common/context_score.py`** | Не смешивать с `context_score_5m`; long — отдельный контракт при необходимости. |
| **In-memory WS `short_pump/liquidations.py`** | Источник метрик для стратегий v2 **запрещён**; только **`liquidations.csv`**. |
| **`resolve_short_pump_route`** | Политика стратегий, не `indicators/`. |
| **«context_score» false_pump (`flags_hit/total`)** | Переименовать в **`false_pump_flags_score`**. |
| **Дубли delta/cvd/oi в `common/market_features`** | Один канон из `short_pump/features` + `CVD5m`. |
| **`normalize_funding` 2-tuple** | Только 3-tuple канон. |
| **Дублирование `build_dbg5` pipeline** | Один `Dbg5Builder` / use-case. |
| **`compute_cvd_part`** | Заменено на **`CVD5m`** + внутренняя логика **`context_score_5m`**. |
| **Generic `dist_to_peak_pct` + `peak_mode`** | **Не вводим**; только **`dist_to_fsm_peak_pct`** и **`dist_to_window_peak_pct`**. |

---

## NEW IN V2

| Компонент | Описание |
|-----------|----------|
| **`OiMonitor`** | Файл (Phase 2 step 4): **`pump_v2/data_sources/oi_monitor.py`**. **Не** `Indicator`. Data source: Bybit WS (**`tickers.{symbol}`** и/или совместимый public stream) + REST **`/v5/market/open-interest`** для выравнивания истории. Обновляет **`MarketContext.oi`** и **`MarketContext.oi_history`**. Заменяет внешний OI webhook / разрыв с `/pump` по архитектуре v2. |
| **`oi_change_pct`** | **`Indicator`**, **не** ходит в сеть в `compute`; читает **`MarketContext.oi_history`**, подготовленный **`OiMonitor`** (live) или parquet (backtest). |

---

## OUT OF SCOPE (Phase 8+)

Следующие метрики из v1 `market_features_snapshot` **не входят** в Phase 2 контракт индикаторов:

- **`long_short_ratio`** (account-ratio и аналоги)
- **`orderbook_imbalance`** (стакан)
- **`footprint`** (бинning сделок по цене)
- **`volume_profile`**

**Обоснование:** текущий live и план стратегий v2 (**short_pump_mid**, **short_pump_funding**, **liquidation_short**, **false_pump**) на них не завязаны. Добавим отдельные строки контракта, когда появится стратегия или validation gate, которым это нужно.

---

## Согласование с `Indicator` ABC

Базовый контракт: `pump_v2/core/indicator_base.py` — `compute(self, symbol, ts, history) -> Any`.

Расширения по решениям ревью:

- Для **`dist_to_fsm_peak_pct`** / **`dist_to_window_peak_pct`**: публичный API **`compute(symbol, ts, history, peak_value_or_window) -> float`** где 4-й аргумент — **`float`** (FSM peak) или **`WindowPeakSpec`** (окно), согласованно с подклассами.
- Для **`CVD5m`**: параметры **`bar_size_sec`**, **`window_bars`** в **`__init__`** (defaults **60** / **5** = v1 `cvd_5m`); стратегия создаёт инстанс с overrides из YAML.
- **`history`** может быть `MarketContext` или узкий тип (`TradesFrame`, …) — финальный union/type alias в Phase 2 step 4 при появлении кода.
