# pump_v2/indicators — contracts (Phase 2 step 1)

Документ фиксирует **одну каноническую реализацию** на индикатор/метрику для `pump_v2/indicators/`.  
Цель: убрать дубли v1 (`short_pump/features.py` vs `common/market_features.py`, разные семантики `context_score`, два пути ликвидаций и т.д.).

**Версия:** draft — 2026-05-14  
**Статус:** контракт для ревью; `.py` модули индикаторов этим коммитом **не** создаются.

---

## Сводная таблица

| ID | Каноническое имя v2 | Канон v1 (файл) | Live источник | Backtest источник |
|----|---------------------|-----------------|---------------|-------------------|
| а | `oi_change_pct` | `short_pump/features.py` | REST `/v5/market/open-interest` | `datasets/oi_history/{symbol}.parquet` (или эквивалент) |
| б | `oi_divergence_5m` | `short_pump/features.py` | Производное от OI% + цены | Как live, из истории OI + close |
| в | `delta_ratio` | `short_pump/features.py` | REST `/v5/market/recent-trade` | `datasets/trades` / parquet по сделкам |
| г | `cvd_delta_ratio` + `cvd_5m` | `short_pump/features.py` + `common/market_features.py` `cvd_5m` | recent-trade | trades history |
| д | `atr_pct_5m_14` | Унифицировать: логика как `short_pump/features.atr`/`atr_pct` на **5m OHLCV** | REST `/v5/market/kline` interval `5` | `datasets/klines/{symbol}.parquet` 5m |
| е | `volume_zscore` | `short_pump/features.py` `volume_zscore` | klines volume | klines volume |
| ж | `funding_snapshot` | `common/market_features.normalize_funding` (3-tuple) + поля из tickers | REST `/v5/market/tickers` | `datasets/funding/{symbol}.parquet` |
| з | `pump_shape_5m` | `common/market_features.pump_shape_features_5m` | REST kline 5m | klines 5m |
| и | `liquidation_rollups` | Контракт как **`common/market_features.liquidation_features`**, данные из **CSV** | `datasets/liquidations.csv` (collector) | Тот же CSV / агрегированный parquet |
| к | `structure_state` (FSM) | `short_pump/context5m.py` `update_structure` + `StructureState` | Цены 5m (peak/close) | klines 5m |
| л | `dist_to_peak_pct` | `short_pump/context5m.build_dbg5` (и согласованная формула в entry) | peak с FSM + close | klines 5m + FSM |
| м | `context_score_5m` | `short_pump/context5m.compute_context_score_5m` | dbg5 поля | dbg5 из backtest feed |

Ниже — детализация и разрешение дубликатов.

---

## а) `oi_change_pct` (oi_delta / oi_change_pct)

| Поле | Значение |
|------|----------|
| **Каноническое имя v2** | `oi_change_pct` |
| **Реализации v1 (все)** | `short_pump/features.py` **73–114** (`oi_change_pct`); `common/market_features.py` **308–330** (`oi_change_pct`, колонка `openInterest`/`open_interest`/`oi`) |
| **Канон для v2** | **`short_pump/features.py`** — уже используется из `context5m.build_dbg5` через импорт (**`short_pump/context5m.py` 11, 217–218**); одна колонка **`openInterest`**, контракт совпадает с Bybit OI DF из `bybit_api.get_open_interest`. |
| **Устаревает** | Дублирующая реализация в **`common/market_features.py`** для этой метрики; alias **«oi_delta»** в именах модулей v2 не вводим — только `oi_change_pct`. |
| **Сигнатура v2** | `compute(symbol: str, ts: datetime, history: OiHistoryFrame) -> float \| None` — `OiHistoryFrame` = упорядоченный ряд OI с колонками `ts`, `openInterest`. |
| **Live** | REST **`GET /v5/market/open-interest`** (как `short_pump/bybit_api.py` **152–184**). |
| **Backtest** | Parquet/series под **`datasets/oi_history/{symbol}.parquet`** (или общий layout из `ARCHITECTURE.md`); тот же расчёт, что на live DF. |

---

## б) `oi_divergence_5m`

| Поле | Значение |
|------|----------|
| **Каноническое имя v2** | `oi_divergence_5m` |
| **Реализации v1** | `short_pump/features.py` **117–138** (`oi_divergence_5m`); используется в `short_pump/context5m.build_dbg5` **218–219** |
| **Канон для v2** | **`short_pump/features.py`** — единственное явное определение; зависит от `oi_change_5m_pct` и `dist_to_peak_pct`. |
| **Устаревает** | Любая копия логики «вручную» вне функции при переносе из v1. |
| **Сигнатура v2** | `compute(symbol: str, ts: datetime, history: OiDiv5mInputs) -> bool` где `OiDiv5mInputs` содержит `oi_change_5m_pct: float \| None`, `dist_to_peak_pct: float`. |
| **Live** | Производное: OI REST + цена/peak из FSM. |
| **Backtest** | Те же входы с backtest feed. |

---

## в) `delta_ratio`

| Поле | Значение |
|------|----------|
| **Каноническое имя v2** | `delta_ratio` |
| **Реализации v1** | `short_pump/features.py` **15–26**; `common/market_features.py` **233–246** (строже проверяет колонки) |
| **Канон для v2** | **`short_pump/features.py`** — используется **`short_pump/entry.py`** (**31–33**) и **`false_pump/detector.py`** через `from short_pump.features import delta_ratio` (**8, 127**); это фактический контракт short_pump/false_pump. |
| **Устаревает** | **`common/market_features.delta_ratio`** как отдельная реализация — в v2 вызывается один общий модуль; `market_features_snapshot` при разборе на индикаторы должен ссылаться на канон. |
| **Сигнатура v2** | `compute(symbol: str, ts: datetime, history: TradesFrame) -> float` — `TradesFrame`: `ts`, `side`, `qty`; окно задаётся параметром индикатора (`since` = `ts - window`). |
| **Live** | REST **`/v5/market/recent-trade`**. |
| **Backtest** | Parquet trades за период. |

---

## г) CVD: `cvd_delta_ratio` и `cvd_5m`

| Поле | Значение |
|------|----------|
| **Канонические имена v2** | `cvd_delta_ratio` (окно от `since_ts` до `ts`); `cvd_5m` (барная модель 1m → 5m CVD) |
| **Реализации v1** | `cvd_delta_ratio`: `short_pump/features.py` **141–167**; `common/market_features.py` **249–262**. `cvd_5m`: только **`common/market_features.py` 265–305**. `compute_cvd_part`: **`short_pump/features.py` 170–197** (вес в score — не отдельный «сырой» индикатор, а политика short_pump). |
| **Канон для v2** | **`cvd_delta_ratio`** — **`short_pump/features.py`** (как в entry). **`cvd_5m`** — оставить **алгоритм** из **`common/market_features.cvd_5m`** (в v1 нет дубля в features.py), но **реализовать один раз** в `pump_v2/indicators/cvd.py` рядом с `cvd_delta_ratio`, чтобы не тянуть два модуля. |
| **Устаревает** | **`common/market_features.cvd_delta_ratio`** как дубликат тела. |
| **Сигнатура v2** | `cvd_delta_ratio.compute(..., window)`; `cvd_5m.compute(symbol, ts, history: TradesFrame) -> tuple[float \| None, float \| None]` → `(cvd_abs, cvd_ratio)`. |
| **Live** | recent-trade. |
| **Backtest** | trades parquet. |

---

## д) `atr` / `atr_pct` (5m, period 14)

| Поле | Значение |
|------|----------|
| **Каноническое имя v2** | `atr_pct_5m_14` (и при необходимости сырой `atr_5m_14` как промежуточный) |
| **Реализации v1** | `short_pump/features.py` **30–70** (`atr`, `atr_pct`) на **DataFrame**; `short_pump/context5m.py` **137–153** `_atr_pct_14` на **list[dict]** свечей внутри `build_dbg5` |
| **Канон для v2** | **Формула и период как в `short_pump/features.atr` + `atr_pct`** на **едином** представлении свечей (**DataFrame** с `high, low, close`). Список dict из v1 — только адаптер на границе feed → DF; **не** две разные математики. |
| **Устаревает** | Отдельный расчёт `_atr_pct_14` в v2 не копируем; заменяется вызовом канона на DF, собранном из 5m klines. |
| **Сигнатура v2** | `compute(symbol: str, ts: datetime, history: CandlesFrame5m) -> float \| None` — `CandlesFrame5m`: OHLCV + `ts`. |
| **Live** | REST kline **interval 5**. |
| **Backtest** | `datasets/klines/{symbol}.parquet` (5m). |

---

## е) `volume_zscore`

| Поле | Значение |
|------|----------|
| **Каноническое имя v2** | `volume_zscore` |
| **Реализации v1** | `short_pump/features.py` **8–12**; в `context5m` — **`_volume_z`** **156–167** (эквивалент z на tail окна) |
| **Канон для v2** | **`short_pump/features.volume_zscore`** — явное имя и lookback параметр; `context5m._volume_z` при переносе заменяется **вызовом** канона на DF из тех же свечей. |
| **Устаревает** | Дублирующий `_volume_z` как отдельная формула в v2. |
| **Сигнатура v2** | `compute(symbol: str, ts: datetime, history: CandlesFrame5m, *, lookback: int = 48) -> float` |
| **Live** | kline 5m volume. |
| **Backtest** | klines 5m. |

---

## ж) `funding_snapshot` (normalize + abs)

| Поле | Значение |
|------|----------|
| **Каноническое имя v2** | `funding_snapshot` (включает `funding_rate`, `funding_rate_ts_utc`, `funding_rate_abs`) |
| **Реализации v1** | `short_pump/features.normalize_funding` **200–234** → **2-tuple**; `common/market_features.normalize_funding` **351–381** → **3-tuple** с `funding_rate_abs` |
| **Канон для v2** | **`common/market_features.normalize_funding`** (3-tuple) — один контракт для live/backtest и для полей вроде `openInterestValue` в том же tickers payload (**`market_features_snapshot` 806–812**). |
| **Устаревает** | **2-tuple** из `short_pump/features.py` — не переносим; v1 вызовы постепенно сводим на канон при миграции кода стратегий. |
| **Сигнатура v2** | `compute(symbol: str, ts: datetime, history: FundingPayload) -> FundingSnapshot` — `FundingSnapshot` = dataclass с тремя полями выше; `FundingPayload` = сырой dict от Bybit. |
| **Live** | REST **`/v5/market/tickers`**. |
| **Backtest** | `datasets/funding/{symbol}.parquet` (или колонка в общем дневном store). |

---

## з) `pump_shape_5m` (в т.ч. `wick_body_ratio_last`)

| Поле | Значение |
|------|----------|
| **Каноническое имя v2** | `pump_shape_5m` (поля: `wick_body_ratio_last`, `upper_wick_ratio_last`, …) |
| **Реализации v1** | **`common/market_features.pump_shape_features_5m`** **98–160** |
| **Канон для v2** | **`common/market_features`** — уже входит в `market_features_snapshot` и rollout/wick маршрутизацию. |
| **Устаревает** | Разрозненные «ручные» wick расчёты вне этой функции. |
| **Сигнатура v2** | `compute(symbol: str, ts: datetime, history: CandlesFrame5m, *, lookback: int = 5) -> PumpShape5m` (typed dict или dataclass). |
| **Live** | kline 5m. |
| **Backtest** | klines 5m. |

---

## и) `liquidation_rollups` (liquidation_features)

| Поле | Значение |
|------|----------|
| **Каноническое имя v2** | `liquidation_rollups` (или `liquidation_features` как имя модуля — внутри один публичный API) |
| **Реализации v1** | **`common/market_features.liquidation_features`** **512–547** (чистая функция от callbacks); источники: **`short_pump/liquidations.get_liq_stats` / `get_liq_stats_usd`** (WS in-memory); отдельно **`collectors/liquidations_ws.py`** → **`datasets/liquidations.csv`** |
| **Канон для v2** | **Контракт и имена полей — как `liquidation_features`**, но **источник данных в live и backtest — CSV** (tail/chunk reader с кэшем), согласно `ARCHITECTURE.md` (self-monitoring / единый путь). |
| **Устаревает** | **`short_pump/liquidations.py` in-memory WS** как источник для v2 (оставляется только в v1 до вывода из эксплуатации). |
| **Сигнатура v2** | `compute(symbol: str, ts: datetime, history: LiquidationsReader) -> LiquidationRollupsDict` — reader абстрагирует CSV/parquet; поля совместимы с v1 `liquidation_features` result. |
| **Live** | **`datasets/liquidations.csv`** (обновляется collector). |
| **Backtest** | Тот же файл или снимок parquet для воспроизводимости. |

---

## к) `structure_state` (stage FSM)

| Поле | Значение |
|------|----------|
| **Каноническое имя v2** | `structure_state` + операция `structure_step` (или один класс `StructureFsm`) |
| **Реализации v1** | `short_pump/context5m.py` **`StructureState` 20–40**, **`update_structure` 49–130** |
| **Канон для v2** | **Только `context5m`** — единственное определение стадий 0–4 для short_pump. |
| **Устаревает** | Любые параллельные FSM стадий в других модулях для той же семантики. |
| **Сигнатура v2** | `step(symbol: str, ts: datetime, history: StructureStepInput) -> StructureState` где `StructureStepInput` содержит `cfg` thresholds, `last_price`, `peak_price_5m`. |
| **Live** | Цены из 5m klines / last close. |
| **Backtest** | Те же входы с исторического feed. |

---

## л) `dist_to_peak_pct`

| Поле | Значение |
|------|----------|
| **Каноническое имя v2** | `dist_to_peak_pct` |
| **Реализации v1** | **`short_pump/context5m.build_dbg5` 204** и **79–80** в `update_structure`; **`short_pump/entry.py` 42–43** (от `peak_price_5m`); **`short_pump/watcher.py` `_dist_to_peak_pct` / `_sanitize_dist_to_peak` ~70–88**; **`false_pump/detector.py` 119–121** |
| **Канон для v2** | **Формула как в `context5m` / entry:** \((peak - price) / peak \times 100\) при известном peak; для short_pump pipeline — **согласована с `StructureState.peak_price`**. Для false_pump в v2 — либо тот же peak-источник, либо отдельный параметр стратегии (см. Open questions). |
| **Устаревает** | Разнобой без явного «какой peak»: в v2 один helper из канона. |
| **Сигнатура v2** | `compute(symbol: str, ts: datetime, history: DistToPeakInput) -> float` — вход: `peak_price`, `current_price`. |
| **Live** | peak из FSM или из агрегата 5m (как сейчас в ветках v1). |
| **Backtest** | klines + FSM state replay. |

---

## м) `context_score_5m` (short_pump с весами)

| Поле | Значение |
|------|----------|
| **Каноническое имя v2** | `context_score_5m` |
| **Реализации v1** | **`short_pump/context5m.compute_context_score_5m` 249–305**; **`common/context_score.compute_context_score`** **6–9** (просто сумма частей — **другое назначение**); **`false_pump/detector`** «context_score» = **flags_hit/total** **176** |
| **Канон для v2** | **`short_pump/context5m.compute_context_score_5m`** для стратегий short_pump-класса. |
| **Устаревает** | Использование имени **`context_score`** для **false_pump flags ratio** — в v2 переименовать (см. DEPRECATED). **`common/context_score`** для pump v2 не используем. |
| **Сигнатура v2** | `compute(symbol: str, ts: datetime, history: Dbg5Row) -> tuple[float, dict[str, float]]` — `Dbg5Row` = набор полей как после `build_dbg5`. |
| **Live** | Производное от dbg5. |
| **Backtest** | Идентичный dbg5 с backtest feed. |

---

## DEPRECATED (v1 → не переносим в v2 как есть)

| Элемент v1 | Решение в v2 |
|------------|----------------|
| **`common/context_score.py`** (короткая сумма без доменных весов) | Не индикатор short_pump; для long_pullback-аналога — отдельный **`long_context_score`** или reuse с явным именем; **не смешивать** с `context_score_5m`. |
| **In-memory WS `short_pump/liquidations.py`** как источник метрик для стратегий | Замена на **`datasets/liquidations.csv`** + `liquidation_rollups` (см. и). |
| **`resolve_short_pump_route`** (`short_pump/rollout.py`) | **Не индикатор**; в v2 — политика маршрутизации внутри стратегий **`short_pump_premium` / `short_pump_wick` / filtered** (YAML + код стратегии), не пакет `indicators/`. |
| **«context_score» в false_pump detector** (`flags_hit / total_flags`) | Переименовать в **`false_pump_flags_score`** (или `fp_confluence_score`); отдельный тип в контракте, **не** alias к `context_score_5m`. |
| **Дубли `delta_ratio`, `cvd_delta_ratio`, `oi_change_pct` в `common/market_features`** | Один импорт из **`pump_v2` канона** (происхождение: **`short_pump/features.py`**); `market_features_snapshot`-подобный оркестратор в v2 либо собирается из индикаторов, либо живёт в **`core/`**, не дублируя тела. |
| **`normalize_funding` 2-tuple** (`short_pump/features.py`) | **Не переносим**; канон **3-tuple** (`funding_rate`, `funding_rate_ts_utc`, `funding_rate_abs`). |
| **Дублирование пайплайна `build_dbg5`** в watcher и fast0_sampler | В v2 один **`Dbg5Builder`** / use-case слой; не дублировать цикл. |

---

## NEW IN V2

| Компонент | Описание |
|-----------|----------|
| **`oi_monitor`** | Self-monitoring OI: **Bybit WS** `tickers.{symbol}` (и/или публичный stream) **+** REST **`/v5/market/open-interest`** для истории/верификации; заменяет внешний **«OI webhook»** и развязку с `/pump`. Детали протокола и частота — в Phase 2 реализации; в `indicators/` может быть одним модулем или подмодулем `oi_monitor/` не нарушая «один показатель — одна реализация» для `oi_change_pct` (монитор ≠ тот же расчёт %). |

---

## Open questions for review

1. **Имя и границы `oi_monitor` vs `oi_change_pct`:** оставляем `oi_change_pct` только как чистую функцию по ряду OI, а **`oi_monitor`** — отдельный сервис (watchdog/alerts), не входящий в `Indicator` ABC, или формально наследуем `Indicator` с side-effect?
2. **Backtest пути parquet:** подтвердить финальные пути (`datasets/klines/...`, `oi_history`, `funding`) — сейчас в репо часть может отсутствовать; создаём схему **до** Phase 4 или **по факту** ETL?
3. **`dist_to_peak_pct` для `false_pump`:** в v1 peak из **max(close) 5m или 1m** (`false_pump/watcher._peak_price_5m` **172–182**), для short_pump — из **FSM + dbg5**. В v2 **один** `dist_to_peak_pct` с параметром стратегии `peak_mode: fsm \| high_5m \| high_1m` или **два** именованных показателя?
4. **`cvd_5m` и окна:** оставляем жёстко 5m/1m бары как в v1 или выносим `bar_size` в YAML для backtest-согласованности?
5. **`liquidation_rollups` и USD «real»:** в v1 были `get_liq_stats` vs `get_liq_stats_usd`; CSV collector пишет **value_usd** — подтвердить, что v2 **только USD-поля** из CSV делают канон, а count-only — производные?
6. **`compute_cvd_part`:** оставляем как **часть стратегии** (вес из YAML) или как **индикатор** `cvd_score_contribution` с фиксированной формулой из `short_pump/features.py` **170–197**?
7. **LS ratio / orderbook / footprint / volume_profile:** вне списка а–м, но есть в `market_features_snapshot` — включать ли их как отдельные строки контракта Phase 2 step 2 или отложить до стратегий, которым они нужны?

---

## Согласование с `Indicator` ABC (напоминание)

В v2 базовый контракт из `pump_v2/core/indicator_base.py`: метод `compute(self, symbol, ts, history) -> Any`.  
Индикаторы с несколькими входами (OI + trades + candles) в реальности получают **`history` как составной объект** (например `MarketContext` или `IndicatorInputs`) — уточнить общий тип в Phase 2 step 2 после ответов на вопросы выше.
