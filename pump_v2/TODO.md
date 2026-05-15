# pump_v2 — Known Issues / TODO

## Phase 2 wrap-up tasks

### live_collected fixtures contain duplicate 5m timestamps

**Discovered:** при переносе atr_pct_5m_14 (commit fa2b17c)

**Detail:** В `pump_v2/tests/fixtures/live_collected/btcusdt/klines_5m.parquet` 
обнаружено 35 дубликатов 5m timestamps на ~9 уникальных баров. 
Аналогичная ситуация скорее всего для других символов.

**Root cause (предполагается):** `_live_collect.py` делает REST poll klines 
каждые N минут и не дедуплицирует по ts перед append в parquet.

**Impact:** 
- Минимальный для v1↔v2 parity tests — v1 функции тоже работают на 
  неупорядоченных/дублирующихся данных без сортировки, поэтому v2 
  даёт идентичный результат
- v1 production не страдает от этого (там другой data flow через 
  in-memory state)
- Реальные значения индикаторов в expected_indicators.json могут 
  отличаться от "правильных" на чистых данных

**Resolution plan:**
- Fix `pump_v2/tests/fixtures/_live_collect.py`:
  * При append в parquet — дедуплицировать по (ts, symbol) с keep="last"
  * Сортировать klines_5m по ts перед сохранением
- Регенерировать все live_collected fixtures
- Запустить все тесты pump_v2/tests/ — ожидать что некоторые expected 
  values изменятся, обновить .json
- Сделать это ОДНИМ заходом ПОСЛЕ переноса всех индикаторов в Phase 2

**Priority:** medium — не блокирует Phase 2, но обязательно сделать 
до Phase 4 (backtest engine). Backtest требует чистых данных.

### v1 indicators don't sort candles/trades before computation

**Observation:** При переносе atr_pct_5m_14 Cursor заметил что v1 функции 
не сортируют входные DataFrame по ts. Это работает в production потому 
что watcher v1 получает данные в правильном порядке из REST/WS, но 
любое изменение data flow может тихо сломать индикаторы.

**v2 behavior:** Сохраняем то же поведение (MIRRORS_V1_BEHAVIOR).

**Future improvement (post Phase 7):** 
- Добавить explicit sort + dedup в `_candle_utils.normalize_candles_df` 
  и `_trade_utils.normalize_trades_df`
- Это потенциально изменит numerical results — нужен careful migration

**Priority:** low — не блокирует ничего сейчас. Помнить про это при 
дебаге странных результатов.

### liquidation_rollups: семантика и naming

**Discovered:** при переносе liquidation_rollups (commit 7ec54bf)

**Три зафиксированных нюанса:**

1. **Side mapping (Bybit → long/short):**

   v1 реализация: `short_pump/liquidations.py:567-610`. 
   v2 повторяет 1:1. Это семантически корректно, не баг.

2. **Window boundary расходится между v1 и v2:**
   - v1 (in-memory WS buffer): `[now - N, now]` — обе границы включены
   - v2 (CSV-based):            `(now - N, now]` — левая граница исключена
   
   В практике различие минимальное (миллисекундная точность). 
   Если когда-то понадобится exact parity с v1 production — изменить 
   фильтр в `liquidation_rollups.py:compute` с `>` на `>=`.

3. **Naming gap для Phase 3:**
   - v2 dataclass поля: `*_30s`, `*_60s`
   - v1 events_v3.csv колонки: `liq_*_30s`, `liq_*_1m`
   
   Когда будут переноситься стратегии в Phase 3 — их v1 код читает 
   `liq_long_count_1m`, не `liq_long_count_60s`. 
   
   **Нужен один из двух подходов:**
   - (A) В стратегии при чтении из LiquidationRollups использовать 
     `getattr(rollups, "long_count_60s")` через mapping `"1m" → "60s"`
   - (B) Добавить в LiquidationRollups dataclass alias-поля 
     `long_count_1m = long_count_60s` etc. — для совместимости
   
   Решение принимать в Phase 3 при первой стратегии которая использует 
   liquidation features (вероятно false_pump).

**Priority:** medium — не блокирует Phase 2, но обязательно учесть в Phase 3.

### structure_state: FSM defaults, determinism, sort

**Discovered:** при переносе structure_state (commit edf38b1)

**Три зафиксированных нюанса:**

1. **config.py vs update_structure defaults (единицы измерения):**

   v1 production использует `update_structure` defaults (проценты).
   `config.py` содержит ДОЛИ — видимо legacy или другое назначение.
   v2 fixtures и `StructureStateIndicator` используют canon (3/1/2/0.8).

   **Risk:** при чтении `config.py` кто-то может подумать что `0.03` = canon,
   что сломает FSM (порог в 100 раз меньше).

   **Action:** при переносе стратегий в Phase 3 — параметры FSM должны
   браться из YAML `strategies.yaml`, не из `config.py`. Дефолты в YAML —
   `3.0` / `1.0` / `2.0` / `0.8`.

2. **`armed_since_utc` недетерминированно:**

   v1 `update_structure` при входе в stage 4 ставит `datetime.now(UTC)`.
   Это значит:
   - Replay одной и той же истории даёт РАЗНЫЙ `armed_since_utc`
   - Тесты не могут assert на это поле через `==`
   - Backtest reproducibility страдает

   **Решение в v2:** В будущем (когда стратегии будут использовать
   `armed_since_utc`) — добавить параметр `now_fn: Callable[[], datetime]`
   в `compute()`, чтобы можно было injection времени для детерминизма.

   Сейчас в v2 повторяем v1 поведение (MIRRORS_V1).

   **Priority:** разрешить в Phase 4 (backtest engine) — там
   детерминизм критичен.

3. **`structure_state` требует сортировку history по ts:**

   В отличие от `atr_pct_5m_14` / `volume_zscore` (которые НЕ сортируют),
   `structure_state` ОБЯЗАН сортировать — FSM зависит от порядка.

   v1 production это не проблема (REST/WS даёт данные в порядке).
   v2 replay через DataFeed может прийти неотсортированно — поэтому
   индикатор сам сортирует.

   **Implication:** Если в Phase 4 ETL обеспечит уже отсортированные
   parquet — sort внутри FSM станет избыточным (но всё равно безопасным).

**Priority:** medium для Phase 3, high для Phase 4 (backtest determinism).
