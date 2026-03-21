# Task ID
B2

## Title
Добавить smoke-тесты для валидных/невалидных примеров signal контрактов

## Epic
Signal Quality

## Priority
High

## Status
Done

---
## 1. Business / Strategy Context
- **Зачем задача нужна?**
  - Упростить и ускорить проверку, что входящие сигналы `/pump` (и их последующая упаковка в `Signal`) соответствуют контракту и не ломают gate/проверки.
- **Какую проблему она решает?**
  - На практике “тихий” дрейф контрактов (типов полей, границ stage/dist/liq, missing/NaN значений) приводит к тому, что часть сигналов либо неправильно проходит в трейдинг/защиту, либо неправильно отклоняется. Это ухудшает data-quality и делает статистику/ML менее воспроизводимой.
- **На какие метрики влияет?**
  - acceptance/rejection rate сигналов, корректность попадания событий в `events_v3/trades_v3/outcomes_v3`, снижение “дыр” в датасете и рост доверия к compact-отчетам.

---
## 2. Scope
### In scope
- Добавить набор smoke-тестов, который для типовых примеров сигналов проверяет:
  - gate логики `trading.broker.allow_entry()` для `short_pump` и `short_pump_fast0` (правильные accept/reject решения и ожидаемые причины отказа).
  - поведение при невалидных/пропущенных значениях key-полей для gate (например: `stage`, `dist_to_peak_pct`, `liq_long_usd_30s`, а также корректная обработка `None`/NaN/нечисловых типов).
  - (дополнительно, если подтверждается текущей архитектурой) корректность базовой сериализации/десериализации `Signal` через `trading.signal_io.signal_to_dict()` / `trading.signal_io.signal_from_dict()` на валидных/невалидных примерах.
- Smoke должен покрывать крайние случаи:
  - `short_pump`: stage должен быть ровно `4`, `dist_to_peak_pct` должен быть `>=` порога.
  - `short_pump_fast0`: `FAST0_AUTO_ENABLE` должен соблюдаться, dist ограничен `FAST0_DIST_MAX`, liq должен попадать в разрешенные buckets.

### Out of scope
- Не внедрять/изменять бизнес-логику трейдинга, риск-гард или Telegram-правила.
- Не менять существующие контракты/форматы сигналов (эта задача только проверяет их соблюдение).
- Не проводить архитектурную переработку ML/dataset пайплайнов.

---
## 3. Inputs
- **Кодовые модули**
  - `trading/broker.py` (функции `allow_entry()`, `allow_entry_short_pump()`, `allow_entry_short_pump_fast0()`)
  - `trading/risk_profile.py` (пороги/разрешенные buckets для `fast0`)
  - `trading/signal_io.py` (если используется для проверки сериализации контракта)
  - `short_pump/signals.py` (структура `Signal`)
- **Конфиги / ENV / .service**
  - переменные, влияющие на пороги и поведение gate:
    - `TG_DIST_TO_PEAK_MIN` / `SHORT_PUMP_AUTO_DIST_MIN` (для short_pump dist min)
    - `FAST0_AUTO_ENABLE`, `FAST0_DIST_MAX`
    - `FAST0_LIQ_MIN_USD`, `FAST0_LIQ_MAX_USD` (и связанные env для bucket boundaries, если они используются по умолчанию)
- **Логи / отчеты**
  - smoke output (stdout) и (при наличии) логированные reasons rejection из gate (для воспроизводимости).

---
## 4. Spec
 - **Что именно нужно изменить?**
   - **A. Test placement (выбран один вариант):**
     - реализовать smoke **как новый файл** в `scripts/`:
       - `scripts/smoke_signal_contract_gate.py`
     - краткое обоснование: в проекте smoke-скрипты уже живут в `scripts/` и запускаются локально одной командой без зависимости от тестового раннера, при этом не затрагивают торговые очереди/датасеты.
   - **B. Execution contract:**
     - Smoke должен быть полностью локальным: не запускать `trading.runner`, не обращаться к очередям, не писать в `datasets/`.
     - Точный запуск:
       - `PYTHONPATH=. TG_DIST_TO_PEAK_MIN=3.5 FAST0_AUTO_ENABLE=1 FAST0_DIST_MAX=1.5 FAST0_LIQ_5K_25K_MIN_USD=5000 FAST0_LIQ_5K_25K_MAX_USD=25000 FAST0_LIQ_100K_MIN_USD=100000 python3 scripts/smoke_signal_contract_gate.py`
     - Минимальный обязательный набор ENV:
       - `TG_DIST_TO_PEAK_MIN=3.5` (порог для `short_pump`)
       - `FAST0_AUTO_ENABLE`, `FAST0_DIST_MAX`, `FAST0_LIQ_5K_25K_MIN_USD`, `FAST0_LIQ_5K_25K_MAX_USD`, `FAST0_LIQ_100K_MIN_USD` (порог/бакеты для `fast0`)
     - Кейc `FAST0_AUTO_ENABLE=0` (reject) должен быть проверен в рамках одного запуска:
       - так как `trading/risk_profile.py` читает env на этапе импорта, реализация smoke должна использовать либо `importlib.reload()` (после установки env), либо временное присваивание `trading.risk_profile.FAST0_AUTO_ENABLE` внутри smoke.
   - **C. Assertion strategy (exact vs tolerant match):**
     - Exact reason match — только для стабильных и контрактных reason’ов:
       - `short_pump`:
         - `missing stage`
         - `stage=<N> (require 4)` (фиксированное `N`)
         - `missing dist_to_peak_pct`
         - `invalid stage=...` и `invalid dist_to_peak_pct=...` (если входные значения фиксированы в тесте и reason детерминирован по `repr`)
       - `short_pump_fast0`:
         - `FAST0_AUTO_ENABLE=0`
         - `missing liq_long_usd_30s`
         - `fast0_dist_gt_1_5`
         - `fast0_liq_not_in_allowed_bucket`
         - `liq_missing` (когда `liq_long_usd_30s` не конвертируется в float)
       - `allow_entry` (unknown strategy):
         - `unknown strategy='<...>'` (фиксированная стратегия)
     - Family/substring match — для причин, где reason включает сравнение порога с числом:
       - `short_pump`: rejection вида `dist_to_peak_pct=... < ...`
         - проверять, что `reason` содержит подстроки `dist_to_peak_pct=` и ` < `
         - exact совпадение чисел не требуется (чтобы не было ложных падений при незначительных форматированиях/источнике порога).
   - **D. Minimal fixture set (минимальный набор кейсов):**
     - short_pump:
       - SP-ACCEPT-1: `stage=4`, `dist_to_peak_pct=4.0` -> `allowed=True`
       - SP-REJECT-1: `stage=3`, `dist_to_peak_pct=4.0` -> `allowed=False`, reason exact `stage=3 (require 4)`
       - SP-REJECT-2: `stage=4`, `dist_to_peak_pct=3.4` -> `allowed=False`, reason tolerant (содержит `dist_to_peak_pct=` и ` < `)
       - SP-REJECT-3: `stage=None`, `dist_to_peak_pct=4.0` -> `allowed=False`, reason exact `missing stage`
       - SP-REJECT-4: `stage=4`, `dist_to_peak_pct=None` -> `allowed=False`, reason exact `missing dist_to_peak_pct`
     - short_pump_fast0:
       - F0-REJECT-1: `FAST0_AUTO_ENABLE=0` -> `allowed=False`, reason exact `FAST0_AUTO_ENABLE=0`
       - F0-ACCEPT-1: `dist_to_peak_pct=1.0`, `liq_long_usd_30s=20000` -> `allowed=True`
       - F0-REJECT-2: `dist_to_peak_pct=1.6`, `liq_long_usd_30s=20000` -> `allowed=False`, reason exact `fast0_dist_gt_1_5`
       - F0-REJECT-3: `dist_to_peak_pct=1.0`, `liq_long_usd_30s=2000` -> `allowed=False`, reason exact `fast0_liq_not_in_allowed_bucket`
       - F0-MALFORM-1: `dist_to_peak_pct=1.0`, `liq_long_usd_30s="bad"` -> `allowed=False`, reason exact `liq_missing`
     - malformed input (общие):
       - MAL-REJECT-1: `short_pump`, `stage=4`, `dist_to_peak_pct="bad"` -> `allowed=False`, reason exact `invalid dist_to_peak_pct='bad'`
       - MAL-REJECT-2: `unknown strategy="nope"` -> `allowed=False`, reason exact `unknown strategy='nope'`
 - **Какие ограничения есть?**
   - Не менять код трейдинга/гардов: только добавить smoke-скрипт.
   - Smoke должен быть детерминированным: фиксировать ENV и не зависеть от состояния очередей/датасетов.
 - **Что нельзя ломать?**
   - Не ломать существующие smoke-проверки и не изменять их семантику.

---
## 5. Acceptance Criteria
- [ ] Smoke размещен ровно в одном новом файле: `scripts/smoke_signal_contract_gate.py` (каталог `scripts/`).
- [ ] Запуск smoke по команде из Spec завершился с exit code `0` и прошел все проверки (т.е. ни один assertion не упал).
- [ ] Smoke не имеет side effects:
  - нет обращения к очередям,
  - нет записи в `datasets/`,
  - нет запуска `trading.runner`.
- [ ] `short_pump` выполняет минимальный fixture set из Spec (SP-ACCEPT-1..SP-REJECT-4) с нужной стратегией exact/tolerant match.
- [ ] `short_pump_fast0` выполняет минимальный fixture set из Spec (F0-REJECT-1..F0-MALFORM-1) с нужной стратегией exact/tolerant match.
- [ ] `malformed input` выполняет минимальный fixture set из Spec (MAL-REJECT-1..MAL-REJECT-2) с нужной стратегией exact match.
- [ ] В stdout smoke выводит краткую сводку: что проверено (SP/F0/MAL) и что было итогом (PASS/FAIL).

---
## 6. Validation Plan
- **Тесты / smoke / replay / ручные проверки:**
  - Запустить локально точной командой из Spec.
  - Проверить, что:
    - ни один кейс не пропущен (все SP/F0/MAL кейсы отмечены в stdout),
    - не создаются/не меняются файлы в `datasets/` (если smoke использует temp dirs — только в temp).
- **Какие артефакты должны остаться после проверки**
  - stdout/log результатов smoke:
    - PASS/FAIL
    - список групп (SP / F0 / MAL)
    - для fail: кейс ID (например `SP-REJECT-2`) и ожидаемый/фактический reason match (exact vs tolerant).

### Evidence (local execution)
- Команда:
  - `PYTHONPATH=. TG_DIST_TO_PEAK_MIN=3.5 FAST0_AUTO_ENABLE=1 FAST0_DIST_MAX=1.5 FAST0_LIQ_5K_25K_MIN_USD=5000 FAST0_LIQ_5K_25K_MAX_USD=25000 FAST0_LIQ_100K_MIN_USD=100000 python3 scripts/smoke_signal_contract_gate.py`
- Exit code: `0`
- Stdout:
  - `PASS: smoke_signal_contract_gate`
  - `Checked groups: short_pump (SP), short_pump_fast0 (F0), malformed input (MAL)`
- Side effects: smoke не вызывает `trading.runner`, не использует очереди и не пишет в `datasets/` (в коде нет операций записи/IO в `datasets/`).

---
## 7. Risks
- **Data risk**
  - Минимальный риск: задача не пишет в данные, только проверяет gate/контракт.
- **Trading risk**
  - Низкий: задача не меняет трейдинг-логику, только тесты.
- **Operational risk**
  - Средний риск, если тесты зависят от ENV и не фиксируют значения в начале. Для снижения: в smoke явно установить необходимые env.
- **In-memory toggle risk**
  - smoke временно меняет `trading.risk_profile.FAST0_AUTO_ENABLE` в памяти и восстанавливает исходное значение через `finally`; нет persistence/записи на диск.
- **Rollback notes**
  - Откат на уровне “удалить/выключить smoke-скрипт”: производство не затрагивается.

---
## 8. Agent Handoff
### PM Agent
- **Ответственность на этой задаче:**
  - Уточнить, какие именно “примеры сигналов” считаются базовыми (минимальный набор кейсов для contract smoke) и утвердить список acceptance criteria.
- **Сделано / нужно сделать PM:**
  - [ ] Подтвердить соответствие кейсов текущим порогам (`TG_DIST_TO_PEAK_MIN`, `FAST0_DIST_MAX`, bucket boundaries)
  - [ ] Назначить Owner Agent на реализацию smoke

### Strategy Developer Agent
- **Ответственность на этой задаче:**
  - Подготовить реализацию smoke-тестов без изменения бизнес-кода.
- **Сделано / нужно сделать Strategy Dev:**
  - [ ] Реализовать smoke так, чтобы он падал при несовпадении reason/accept.

### QA / Validation Agent
- **Ответственность на этой задаче:**
  - Запустить smoke и подтвердить, что он детерминированный и покрывает edge-cases.
- **Сделано / нужно сделать QA**:
  - [ ] Выполнить smoke на чистом окружении и при подстановке ENV значений

### Production Safety Agent
- **Ответственность на этой задаче:**
  - Обычно не требуется, так как тесты не затрагивают live-поведение.
- **Сделано / нужно сделать Production Safety**:
  - [x] Подтвердить, что smoke не имеет side effects на live
  - [x] Подтверждено: нет IO в `datasets/`, нет `trading.runner`, нет очередей

---
## 9. Execution Log
| Date       | Who (Agent / Person)       | Action / Comment                                   |
|-----------|-----------------------------|----------------------------------------------------|
| 2026-03-18| QA / Validation Agent       | Executed `scripts/smoke_signal_contract_gate.py` with Spec command; smoke PASSED (exit code 0). |
| 2026-03-18| Production Safety Agent     | Production Safety review: no IO, no runner, no queues, no datasets, in-memory toggle restored. |

---
## 10. Final Decision

- Status: Approved with notes
- Reason:
  - smoke проверяет только contract gate (allow_entry)
  - не использует trading.runner, очереди, datasets
  - не влияет на production execution path
  - rollback тривиален (удаление smoke)
  - QA findings non-blocking (касаются устойчивости тестов, не прод-рисков)

- Links to evidence:
  - команда запуска smoke
  - факт exit code = 0
  - summary stdout (SP/F0/MAL PASS)

