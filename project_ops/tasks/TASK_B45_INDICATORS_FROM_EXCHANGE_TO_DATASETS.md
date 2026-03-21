# Task ID
B45_INDICATORS_FROM_EXCHANGE_TO_DATASETS

## Title
Validate indicator collection (exchange) -> datasets fields

## Epic
Validation / Backtest

## Priority
High

## Status
In Spec

---
## 1. Business / Strategy Context
- **Зачем задача нужна?**
  - Перед включением новых strategy-family (в т.ч. NET) нужно быть уверенным, что базовые индикаторы (CVD/OI/Liquidations/Funding и связанные дельты) корректно собираются с биржи и попадают в `events_v3.csv` (а при необходимости — в `trades_v3.csv/outcomes_v3.csv` через связи).
- **Какую проблему она решает?**
  - Исключает ситуацию, когда правила новой стратегии корректные по ТЗ/видео, но входные индикаторы неверно рассчитаны или не записались в dataset (что дает ложные outcomes/EV/WR и ломает валидацию).
- **На какие метрики влияет?**
  - Data-quality: доля непустых полей метрик в dataset.
  - Factor stability / analytics consistency: корректность `cvd_delta_ratio_*`, `oi_change_*_pct`, `funding_rate(_abs)`, `liq_*_count/_usd_*`.
  - Outcomes / events integrity: отсутствие “дырок” entry -> outcome из-за некорректных фич.

---
## 2. Scope
### In scope
1. Договор индикаторов, которые должны быть в `events_v3.csv` (schema v3)
   - `cvd_delta_ratio_30s`, `cvd_delta_ratio_1m`
   - `oi_change_5m_pct`, `oi_change_1m_pct`, `oi_change_fast_pct`
   - `funding_rate`, `funding_rate_abs`
   - `liq_short_count_30s`, `liq_short_usd_30s`, `liq_long_count_30s`, `liq_long_usd_30s`
   - `liq_short_count_1m`, `liq_short_usd_1m`, `liq_long_count_1m`, `liq_long_usd_1m`
   - (опционально, если NET использует) `delta_ratio_30s`, `delta_ratio_1m`, `volume_1m/5m` и т.п.
2. Проверка pipeline “биржа -> вычисление индикаторов -> запись в dataset”
   - определить “sources of truth” для:
     - факта получения сырых данных с биржи (trades/OI/funding/liquidations)
     - вычисленных индикаторов (как минимум: агрегаты и знаки/диапазоны)
     - записанных полей в dataset.
3. Smoke/diagnostic проверка на ограниченном окне времени
   - один символ (или 2-3), короткое окно (например 30-120 минут).
   - режим: paper-first (без live ордеров).
4. Подготовка evidence артефакта
   - отдельный “Feature/Indicator audit report” (CSV/JSON) с:
     - event_id -> значения в `events_v3.csv`
     - вычисленные значения на основе биржевого среза/логики адаптера
     - расхождения (если есть) с tolerance.

### Out of scope
1. Валидация новых NET формул (это будет позже в `TASK_B44_NET_STRATEGIES`).
2. Изменение схемы `common/dataset_schema.py` без отдельного согласования.
3. Реальное включение новых стратегий в live до отдельного production safety sign-off.

---
## 3. Inputs
- **Кодовые модули (primary для проверки/интеграции диагностик):**
  - `short_pump/watcher.py` (где индикаторы/контекст собираются и формируют entry/payload)
  - `trading/runner.py` (consume signals -> paper/live позиции; запись в datasets)
  - `common/io_dataset.py` (writer: как именно индикаторы/поля попадают в `events_v3.csv`)
  - `common/dataset_schema.py` (контракт колонок и нормализация)
  - модули сбора сырых данных:
    - `liquidation_features` и `get_liq_stats`
    - OI/funding нормализация (как минимум `normalize_funding` + adapter)
    - trades/CVD вычисления (если CVD считается на основе trades)
  - (опционально) `analytics/load.py` / фактор-репорт для доп. sanity (без изменения production)

- **Конфиги / ENV:**
  - `EXECUTION_MODE` / `AUTO_TRADING_MODE` (должно быть paper или lab)
  - параметры тайминга/окна для outcome/entry (только в рамках smoke)
  - API rate-limit env (если проект использует)

- **Датасеты / таблицы:**
  - `datasets/**/mode=paper/**/events_v3.csv` для конкретного окна/символа/strategy.

---
## 4. Spec
1. Определить список индикаторов и “контракт соответствия”
   - какие поля обязательны (must-have) и какие tolerant (best-effort).
   - какие допуски расхождений допустимы (например rounding to N decimals).
2. Реализовать диагностическую проверку (в будущем AGENT stage)
   - либо новый smoke-скрипт в `scripts/` (preferred),
   - либо расширение существующего безопасного smoke-скрипта (например “events v3 no nan”, “fast0 outcome tg format” — если там есть подход к извлечению значений).
3. Верифицировать “биржевую” основу индикаторов
   - для каждого must-have поля описать, какой именно биржевой источник дает данные:
     - trades -> delta/CVD
     - OI adapter -> oi_change_*_pct
     - funding adapter -> funding_rate(_abs)
     - liquidation adapter -> liq_*_count/usd
4. Валидация dataset write
   - проверить, что для каждой созданной/входной event (на выбранном окне) соответствующие поля в `events_v3.csv`:
     - непустые (в must-have перечне)
     - в ожидаемых диапазонах
     - знаки/направления соответствуют расчету.

---
## 5. Acceptance Criteria
- [ ] Smoke/diagnostic проверка проходит без ошибок на выбранном символе/окне (paper-first).
- [ ] Для must-have индикаторов в `events_v3.csv`:
  - доля непустых значений >= заданного порога (фиксируем в Spec, например >= 95%).
  - расхождения рассчитанных vs записанных значений не превышают tolerance.
- [ ] Для выявленных расхождений smoke фиксирует:
  - `event_id`, поле, значения (expected vs actual), причину (если есть).
- [ ] Evidence сохраняется в файлах (CSV/JSON report + summary в stdout).
- [ ] Доказано, что при включении будущих NET-стратегий (в `TASK_B44_NET_STRATEGIES`) нужные базовые индикаторы уже корректно собраны и записываются.

---
## 6. Validation Plan
- **Тесты / smoke / ручные проверки:**
  - Запустить диагностическую проверку на ограниченном окне:
    - 1-3 символа
    - 30-120 минут
    - режим paper-only.
  - Проверить артефакты:
    - audit report (CSV/JSON)
    - summary: counts missing fields, max diff, number of mismatches.
- **Какие артефакты должны остаться после проверки:**
  - `datasets/**/events_v3.csv` копия/ссылка (для выбранного окна)
  - `reports/FEATURE_INDICATOR_AUDIT_<date>_<symbol>.json` (или CSV)
  - stdout summary в консоли + журнал логов smoke-скрипта.

---
## 7. Risks
- **Data risk**
  - Тайм-окна и временная привязка (TZ/clock skew) могут дать систематические расхождения.
- **Operational risk**
  - API rate limits при частых вызовах адаптеров.
- **Trading risk**
  - Низкий: smoke должен работать в paper-first и без live ордеров.
- **Rollback notes**
  - Отключить smoke-скрипт/диагностику, не менять runtime-путь, при необходимости откатить только изменения smoke.

---
## 8. Agent Handoff
### PM Agent
- Подтвердить must-have список полей из `common/dataset_schema.py` (schema v3) и связь с NET-метриками из `TASK_B44`.

### Strategy Developer Agent
- Спроектировать безопасный diagnostic path (smoke) и audit report.
- Указать, как фиксировать “биржевую expected value” для каждого must-have индикатора.

### QA / Validation Agent
- Выполнить smoke в paper-only режиме и вынести verdict PASS/FAIL по tolerance и coverage must-have.

### Production Safety Agent
- Убедиться, что smoke:
  - не создает live ордера
  - не влияет на runtime сервисы
  - укладывается в rate limits.

---
## 9. Execution Log
| Date       | Who (Agent / Person)       | Action / Comment                                   |
|-----------|-----------------------------|----------------------------------------------------|
| YYYY-MM-DD|                             |                                                    |

---
## 10. Final Decision
- **Status**: In Spec
- **Reason**: Prerequisite check before enabling NET (and other) strategies.
- **Links to evidence**:
  - `common/dataset_schema.py` (contract of `events_v3.csv` columns)
  - `TASK_B44_NET_STRATEGIES.md` (target NET-derived metrics)

