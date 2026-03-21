# Task ID
B42_DOLF_STRATEGIES

## Title
Implement DOLF strategies in paper-first order

## Epic
Signal Quality

## Priority
High

## Status
In Spec

---
## 1. Business / Strategy Context
- **Зачем задача нужна?**
  - Добавить в контур входа новые торговые сетапы DOLF, основанные на комбинации четырех рыночных метрик (CVD/OI/Liquidations/Funding), и внедрить их строго по порядку из первичного источника.
- **Какую проблему она решает?**
  - Расширяет покрытие “сигналов” за счет well-defined методики DOLF.
  - Упрощает управляемое внедрение: сначала paper-only и валидация на датасетах, затем (позже) live enable по guardrails.
- **На какие метрики влияет?**
  - Signal coverage (число entry событий, попадающих в paper path).
  - Outcome pipeline полнота (TP/SL/TIMEOUT → outcomes_v3) и доставка TG outcome (как контракт).
  - EV/WR/EV20 и стабильность по подрежимам (по outcome datasets).

---
## 2. Scope
### In scope
1. Источник сетапов:
  - зафиксировать список DOLF сетапов из Teletype и порядок внедрения.
2. Маппинг DOLF метрик → доступные фичи в проекте:
  - CVD → `cvd_30s` / `cvd_1m` (или аналогичные поля в payload),
  - OI → open interest / дельты (stage/dist/context5m или отдельная метрика, как будет согласовано),
  - Liquidations → `liq_long_usd_30s` / `liq_short_usd_30s`,
  - Funding Rate → funding rate, доступный в runtime payload/adapter.
3. Интеграция новых стратегий в существующий pipeline:
  - генерация signals (enqueue) для новых strategy names/submodes,
  - запись событий/сделок/исходов в datasets (events_v3/trades_v3/outcomes_v3),
  - paper-first gating: live enable отключен до прохождения QA.
4. Валидация:
  - smoke/proof на фиксированном окне для каждого сетапа (минимум события entry → outcome row),
  - backtest/replay с подтверждением core-исходов (TP_hit/SL_hit/TIMEOUT) в outcomes_v3.

### Out of scope
- Запуск/включение live-торговли DOLF до явного Final Decision и Production Safety.
- Переписывание outcome tracking/контрактов TG outcome (инварианты из B41 не трогаем).
- ML training в рамках первого внедрения (только dataset/output для дальнейших экспериментов).

---
## 3. Inputs
- **Ссылка на источник DOLF сетапов (единственный источник списка):**
  - https://teletype.in/@cryptogramotnost/DOLF_setup
- **Извлеченный порядок сетапов (зафиксировать в реализации как маппинг под submodes):**
  - Long (6):  
    - 1) “Начало растущего тренда”  
    - 2) “Золотой сетап с фандингом”  
    - 3) “Снижение Открытого интереса и флэт цены”  
    - 4) “Продолжение тренда после коррекции”  
    - 5) “Шортодон: лонг с локального дна”  
    - 6) “Рост цены после лонговых ликвидаций”
  - Short (5):
    - 1) “Снижение Открытого интереса после пампа”  
    - 2) “Расхождение Цены и Открытого интереса”  
    - 3) “Недогора, затухающий Открытый интерес”  
    - 4) “FOMO свечи и иллюзия бесконечного роста”  
    - 5) “Снижение цены после ликвидации шортов”
- **Кодовые модули (primary) для интеграции signals/фичей:**
  - `short_pump/watcher.py` (entry decision / signal enqueue path для новых strategy names)
  - `trading/runner.py` (consume signals → open paper/live positions; paper-first)
  - `trading/state.py` (delivery dedup key уже унифицирован — B41 invariants)
  - `common/io_dataset.py` (write events/trades/outcomes rows)
  - `trading/risk_profile.py` (risk profiles для DOLF подрежимов)
  - (опционально) `notifications/tg_format.py` / формат entry сигналов (если entry TG должен быть отдельным для DOLF)

---
## 4. Spec
1. Создать canonical mapping DOLF сетап → подрежим (submode) и strategy name:
   - предложить схему именования (например `dolf_long_setup1 ... setup6` и `dolf_short_setup1 ... setup5`), либо семейство `dolf_long`/`dolf_short` с submode-параметром.
2. Для каждого сетапа определить **entry contract**:
   - какие четыре метрики используются,
   - какие направления/сравнения (например “OI растет”, “funding красный”, “liq_long > 0” и т.п.),
   - какие пороги/окна берутся (например “на интервале N свечей/минут” — согласовать с существующими базовыми окнами проекта).
3. Paper-first rollout:
   - обеспечить, что DOLF signals приводят к paper paths (без live orders) до отдельного Final Decision.
4. Outcomes:
   - exit делаем через текущий TP/SL/TIMEOUT механизм проекта (без внедрения новых exit-алгоритмов, если это не требуется контрактом из DOLF).
5. Instrumentation/Debug:
   - добавлять `skip_reason` на уровне “почему entry не прошел” для подрежимов DOLF.
6. Внедрение по очереди:
   - порядок внедрения такой же, как в Teletype:
     - Long setup1 → … → setup6
     - затем Short setup1 → … → setup5
   - для каждого шага: smoke сначала, затем dataset evidence, затем backtest.

---
## 5. Acceptance Criteria
1. Для каждого DOLF submode в paper-first режиме:
   - при воспроизводимом входном сценарии entry conditions приводят к появлению entry signal в pipeline,
   - в datasets появляется минимум одна строка core outcome в `outcomes_v3.csv` (TP_hit/SL_hit/TIMEOUT) с корректными связками strategy/mode/submode.
2. Контракт outcome→TG не ломается:
   - никаких регрессий по `outcome_tg_sent` delivery-key (B41 invariants).
3. Локальные smoke проверки подтверждают:
   - `events_v3.csv` и `trades_v3.csv` заполняются согласованно с `outcomes_v3.csv` (нет “дырок” для entry → outcome).
4. Наблюдаемые метрики по outcomes подрежимов:
   - EV/WR считаются консистентно (используя существующие analytics/report code),
   - результаты сохраняются как evidence артефакты для QA.

---
## 6. Validation Plan
- **Тесты / smoke / ручные проверки:**
  - Runtime paper capture (основной способ для DOLF):
    - включить DOLF strategies в paper-only режиме,
    - выбрать минимальный набор символов (например 3-5) и короткое окно (например 1-6 часов),
    - убедиться, что для каждого submode entry conditions реально приводят к сигналу/entry и далее к появлению core outcomes в `outcomes_v3.csv`.
  - Dataset checks:
    - отсутствие критичных NaN/дубликатов,
    - core outcomes не “пропадают” (entry → outcomes сохраняется без дыр),
    - консистентность связок `event_id ↔ trade_id ↔ outcome`.
  - Anti-regression (контракт outcome→TG не ломается):
    - для TG-части использовать уже существующие контрактные проверки (B41), но в рамках этой задачи основная проверка — что delivery-key не приводит к пропускам.
- **Replay limitations (важно):**
  - `replay/` lab mode v1 в текущем проекте candle-only и **не реконструирует** CVD/OI/Liquidations/Funding, поэтому “честный” replay DOLF entry rules на этом этапе не является обязательным критерием приемки.
- **(Опционально) EV/WR анализ:**
  - после runtime-сбора datasets прогнать стандартные analytics/report pipeline и зафиксировать baseline EV/WR по submodes.
- **Какие артефакты должны остаться после проверки**
  - stdout/stderr summary прогона,
  - `datasets/**/events_v3.csv`, `trades_v3.csv`, `outcomes_v3.csv` (или ссылки на созданные копии),
  - CSV/JSON summary counts по submodes: entry_count / core_outcomes_count.

---
## 7. Risks
- Risk: неполное/некорректное маппирование DOLF метрик на текущие фичи проекта (нужно уточнение порогов и источника funding/OI).
- Risk: порядок внедрения может требовать дополнительных подстройек окон (N свечей/минут) и повлечь low coverage на ранних submodes.
- Risk: рост runtime-объема из-за усложнения entry decision (нужны предохранители и ограничение на обработку).
- Rollback notes: отключить DOLF strategies по feature flag / ENV, вернуться к прежним стратегическим names.

---
## 8. Agent Handoff
### PM Agent
- Определить: порядок внедрения шагами (setup1..setup6, затем short setup1..setup5) и контрольные окна дат.
### Strategy Developer Agent
- Реализовать: canonical mapping setup → strategy/submode, entry contract, интеграцию в signal enqueue.
### QA / Validation Agent
- Подтвердить: entry → core outcomes consistency, отсутствие regression по outcome→TG контракту и delivery-key логике.
### Production Safety Agent
- Убедиться: live enable не включается до go/no-go, restart scope и env-флаги корректны.

---
## 9. Execution Log
| Date       | Who (Agent / Person)       | Action / Comment                                   |
|-----------|-----------------------------|----------------------------------------------------|
| 2026-03-18| PM Agent                    | Added DOLF strategy rollout task (PLAN stage).    |

---
## 10. Final Decision
- **Status**: In Spec
- **Reason**: Ready for agent implementation after QA/PS review.
- **Links to evidence**:
  - Teletype DOLF setup source (URL above).

