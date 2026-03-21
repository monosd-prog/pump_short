# Task ID
B42_OUTCOME_TG_TIMEOUT_TTL

## Title
Deliver Telegram OUTCOME for paper TIMEOUT (TTL close path) under Delivery Contract

## Epic
Outcome Tracking

## Priority
Critical

## Status
In Validation

---
## 1. Business / Strategy Context
- **Зачем задача нужна?**
  - В paper-пути TIMEOUT (TTL failsafe) сейчас выполняется `close`/`record_close`, но TG outcome delivery не гарантируется.
- **Какую проблему она решает?**
  - Контракт доставки outcome в Telegram (см. `TASK_B41_OUTCOME` / Outcome Delivery Contract) нарушается для TIMEOUT, закрываемых через `trading/paper_outcome.close_on_timeout`.
  - Для этих кейсов не появляются `OUTCOME_TG_*` маркеры и `outcome_tg_sent` не обновляется (так как mark делается только после успешной TG отправки).
- **На какие метрики влияет?**
  - Coverage outcomes → TG для TIMEOUT кейсов (paper).
  - Достоверность state `outcome_tg_sent` ↔ фактическая TG-доставка.
  - Надежность дедупликации (отсутствие дублей TG при повторных TIMEOUT close).

---
## 2. Scope
### In scope
1. Paper TIMEOUT TTL путь:
   - `trading/runner.py` → `trading/paper_outcome.close_on_timeout(...)` → `close_from_outcome(...)` → `record_close(...)`
2. Telegram outcome delivery в точке TIMEOUT TTL close:
   - обеспечить, что для каждого delivery-key гарантированно выполняется попытка TG send и при успехе ставится `outcome_tg_sent`.
3. Соблюдение Outcome Delivery Contract (B41):
   - единый delivery-key = `position_id` (через `make_position_id(strategy, run_id, event_id, symbol)`)
   - `outcome_tg_sent` записывается только после успешной TG отправки
   - bounded retry при ошибках TG send
   - не более одного delivered TG на delivery-key
4. Логи/диагностика:
   - добавить явные `OUTCOME_TG_*` маркеры для попытки send/успеха/skip_reason/retry_reason, чтобы их можно было использовать в reconciliation.
5. Избежание размазывания логики:
   - исключить дублирующую TG-send логику между watcher/fast0 и TTL close path путем переиспользования единого helper/retry+mark контракта.

### Out of scope
1. Live path / outcome_worker Bybit close логика (TP/SL/TIMEOUT из Bybit resolver).
2. Расширение exit-алгоритмов (TP/SL/TIMEOUT классификация остается текущей).
3. Изменение схемы dataset writer для outcomes_v3.

---
## 3. Inputs
- **Кодовые модули (primary)**
  - `trading/runner.py`
    - точка вызова `close_on_timeout` для paper (TTL failsafe)
  - `trading/paper_outcome.py`
    - `close_on_timeout` (TIMEOUT TTL) и `close_from_outcome` / `record_close`
  - `trading/state.py`
    - `make_position_id`
    - `outcome_tg_sent` / `add_outcome_tg_sent`
  - `notifications/tg_format.py`
    - `format_outcome(...)` (формирование текста TG OUTCOME)
  - `short_pump/telegram.py`
    - `send_telegram(...)` (низкоуровневая отправка Telegram)
  - `short_pump/watcher.py` и `short_pump/fast0_sampler.py`
    - текущая (после B41) реализация контракта для TG outcome (откуда нужно переиспользовать retry/mark+logging контракт)

- **ENV / конфиги**
  - `TG_SEND_OUTCOME` (и/или используемые TG outcome enable флаги для paper delivery)
  - `POSITION_TTL_SECONDS`, `TIMEOUT_EXIT_MODE`
  - параметры retry, которые будут использованы/объединены в helper (см. bounded retry в B41)

- **Датасеты / таблицы**
  - `datasets/trading_state.json` (поле `outcome_tg_sent`)
  - `datasets/date=*/strategy=short_pump|short_pump_fast0/mode=paper/outcomes_v3.csv` (core outcome rows для TIMEOUT TTL)

---
## 4. Spec
1. **Где должна жить TG send для TTL TIMEOUT (paper)**
   - TG outcome delivery должен быть инициирован **в точке `trading/paper_outcome.close_on_timeout`**.
   - Причина: именно `close_on_timeout` генерирует TIMEOUT, который не проходит через watcher/fast0 outcome TG send ветку (классическая “TTL failsafe”, а не candle outcome).
   - Реализация должна быть выполнена **только для paper**:
     - внутри цикла `close_on_timeout` условие `mode != "live"` уже присутствует; TG send должен быть привязан к этой же логике.

2. **Единый delivery-key для TIMEOUT TTL**
   - Для каждого закрываемого paper-позиционирования вычислить:
     - `delivery_key = make_position_id(strategy, run_id, event_id, symbol)`
   - Использовать `run_id`, `event_id`, `symbol` из `pos` (state open_positions):
     - `pos.get("run_id")`
     - `pos.get("event_id")`
     - `pos.get("symbol")`
   - Важно: **trade_id не использовать** как delivery-key.

3. **Соблюдение Outcome Delivery Contract (B41 invariants)**
   - **Single source of truth**
     - `outcome_tg_sent` помечается **ТОЛЬКО** после успешного TG send.
   - **Запрет**
     - запрещено записывать `outcome_tg_sent` до успешного TG send.
   - **Retry policy (bounded)**
     - если TG send неуспешен (exception / HTTP неуспех), outcome не помечается и допускается повторная попытка в последующих циклах.
     - retry bounded по delivery-key, чтобы не было flood.
   - **Idempotency**
     - если `outcome_tg_sent(state, delivery_key) == True`:
       - TG send пропускается (log `OUTCOME_TG_SEND_SKIP_ALREADY_FINALIZED`).
       - никакой повторной delivered mark не делается.
     - для одного delivery-key допускается максимум 1 успешный delivery.

4. **Переиспользование existing helper/retry path и избегание размазывания**
   - На уровне архитектуры требуется **один единый helper** для “deliver outcome TG under contract”, который:
     - принимает `delivery_key`, `strategy`, `run_id`, `event_id`, `symbol`, `res="TIMEOUT"`, `meta` для formatting,
     - делает skip по `outcome_tg_sent`,
     - отправляет TG с bounded retry (используя уже согласованную retry-логику/константы из B41),
     - при success вызывает `add_outcome_tg_sent`.
   - Этот helper должен переиспользоваться:
     - watcher/fast0 outcome TG send путями (чтобы не дублировать логику),
     - и новым TTL TIMEOUT close path в `close_on_timeout`.
   - Формирование TG текста использовать через existing `notifications.tg_format.format_outcome(...)`.
   - Низкоуровневую отправку делать через existing `short_pump.telegram.send_telegram(...)`.

5. **Retry и logging contract для TTL TIMEOUT**
   - Для TTL TIMEOUT paper send обязательно должны появляться:
     - `OUTCOME_TG_SEND_START` (с `delivery_key` и `res=TIMEOUT`)
     - `OUTCOME_TG_SEND_ATTEMPT_FAILED` (если были retry попытки)
     - `OUTCOME_TG_DELIVERED_AND_MARKED` (строго после успеха)
     - `OUTCOME_TG_SEND_SKIP_ALREADY_FINALIZED` (если outcome_tg_sent уже есть)
   - Логи должны содержать:
     - `delivery_key`
     - `skip_reason` (например `already_finalized` / `tg_disabled` / `tg_no_token` если применимо)
     - `retry_count` / `attempt` при необходимости

6. **Повторный TIMEOUT close (anti-duplicate)**
   - Даже если `record_close ... reason=TIMEOUT` повторяется (из-за повторного TTL evaluation или задержек state persistence):
     - TG send должен быть защищен дедупом по `delivery_key` через `outcome_tg_sent`.
     - Никаких duplicate TG сообщений не должно происходить.

## Delivery Helper Consolidation

1. **TG outcome delivery logic не копируется**
   - Логика доставки Telegram OUTCOME **не должна** копироваться отдельной реализацией в `close_on_timeout` (TTL TIMEOUT path).
   - Дублирование delivery policy в watcher/fast0/close_on_timeout запрещено.

2. **Один общий helper/contract path для всех трех delivery источников**
   - Должен существовать один общий helper path для:
     - watcher outcome delivery,
     - fast0 outcome delivery,
     - paper TIMEOUT TTL delivery.

3. **Контракт helper (обязательный)**
   - Helper должен инкапсулировать полностью:
     - build `delivery_key` через `make_position_id(strategy, run_id, event_id, symbol)`,
     - check already finalized через `outcome_tg_sent`,
     - bounded retry,
     - `send_telegram(...)`,
     - `add_outcome_tg_sent(...)` только после success,
     - OUTCOME_TG_* logging (включая delivery_key и skip_reason/retry_count).

4. **Delegation from close_on_timeout**
   - `trading/paper_outcome.close_on_timeout` должен только вызывать helper с параметрами (`res="TIMEOUT"` + данные для `format_outcome`), но не реализовывать delivery policy локально.

5. **Reuse / extraction без изменения B41 контракта**
   - Если после B41 есть частично подходящие helper’ы/функции в watcher/fast0, то:
     - либо переиспользовать их напрямую,
     - либо вынести в общий модуль **с сохранением контракта** (B41 invariants).
   - Нельзя оставлять несколько несогласованных реализаций delivery policy.

6. **Preferred helper placement**
   - Предпочтительное место общего helper:
     - `trading/outcome_delivery.py` (или ближайший совместимый runtime модуль внутри `trading/`).
   - Почему так:
     - TTL close path находится в `trading/paper_outcome.py`,
     - watcher/fast0 outcome delivery также тесно связаны с `trading.state` и `outcome_tg_sent`,
     - общий helper в `trading/` логически является единой точкой контракта.

---
## 5. Acceptance Criteria
1. **TG delivery attempt for paper TIMEOUT TTL**
   - Для каждого `delivery_key`, который закрывается через `trading/paper_outcome.close_on_timeout` с `res="TIMEOUT"`, система инициирует попытку TG OUTCOME send (лог `OUTCOME_TG_SEND_START`).
2. **Success → delivered marker**
   - При успешной TG отправке:
     - появляется `OUTCOME_TG_DELIVERED_AND_MARKED` (с тем же `delivery_key`)
     - `outcome_tg_sent` обновляется **только после** success.
3. **Mark after success (strict)**
   - Запись `outcome_tg_sent` отсутствует при ошибках TG send:
     - если TG send упал, `outcome_tg_sent` для ключа не должен появляться/не должен расширяться на этот delivery-key.
4. **No duplicate TG for same delivery-key**
   - Повторный TTL TIMEOUT для того же `delivery_key` не создает duplicate TG:
     - либо пропуск из-за `outcome_tg_sent`
     - либо один delivered event максимум.
5. **No regressions for live path (B41 contract)**
   - Live outcome пути не должны поменять contract semantics:
     - `outcome_tg_sent` по live должно продолжать отражать delivery success согласно B41.
6. **No regressions for B41**
   - watcher/fast0 outcome TG delivery:
     - сохраняют контракт (mark only after success, unified delivery-key = position_id, bounded retry, no duplicates).
7. **Single helper policy**
   - TG delivery policy существует в одном общем helper path и не дублируется вручную:
     - не копируется отдельно в watcher,
     - не копируется отдельно в fast0,
     - не копируется локально в `close_on_timeout`.

---
## 6. Validation Plan
- **Тесты / smoke / ручные проверки:**
  - Подготовить состояние paper-pозиции, которая гарантированно попадет в `close_on_timeout`:
    - выставить `opened_ts` достаточно старым относительно `POSITION_TTL_SECONDS` (в рамках тестового paper состояния).
  - Прогнать `trading.runner --once` в paper mode и убедиться, что:
    - `record_close ... reason=TIMEOUT` выполняется,
    - затем появляется `OUTCOME_TG_SEND_START`,
    - при наличии доступных TG env и рабочем TG — появляется `OUTCOME_TG_DELIVERED_AND_MARKED`,
    - `datasets/trading_state.json` обновляет `outcome_tg_sent` по `delivery_key`.
  - Повторить `runner --once` второй раз на том же state/позиции и подтвердить отсутствие duplicate TG delivery.
- **Какие артефакты должны остаться после проверки**
  - stdout/journal summary по `OUTCOME_TG_*`
  - snapshots `datasets/trading_state.json` до/после
  - подтверждение отсутствия duplicate delivered маркеров для одного `delivery_key`

---
## 6.1 Validation Evidence
- `python3 -m py_compile trading/outcome_delivery.py trading/paper_outcome.py short_pump/watcher.py short_pump/fast0_sampler.py` ✅
- `python3 scripts/smoke_fast0_outcome_tg_format.py` ✅

---
## 7. Risks
- **Risk: TG duplication**
  - Если mark ставится до успеха или дедуп key не совпадает, повторный TTL close создаст duplicate TG.
- **Risk: helper extracted incorrectly -> regression in B41 paths**
  - Если helper extracted/интегрирован неверно, возможны регрессии:
    - delivery-key может отличаться от B41,
    - `outcome_tg_sent` может быть помечен до success,
    - bounded retry/logging может исчезнуть или сломаться,
    - в watcher/fast0 появятся duplicate TG или ложные skip.
- **Risk: retry without bounds**
  - При некорректной retry-политике можно получить flood Telegram при частом TTL evaluation.
- **Risk: repeated `record_close`**
  - Повторный TTL close может многократно вызывать close path; TG должен оставаться идемпотентным по `delivery_key`.
- **Risk: размазывание логики TG send по путям**
  - Если TG отправка будет реализована “в двух разных местах” без общего helper/контракта — высок риск несогласованного поведения и расхождения ключей.
- **Risk: несоответствие env/logging path**
  - TG env (token/chat_id) и логовый path могут различаться между сервисами; reconciliation может не увидеть evidence.

---
## 8. Agent Handoff
### Strategy Developer Agent
- Определить и внедрить:
  - точку TG send в `close_on_timeout`,
  - единый delivery-key `position_id`,
  - bounded retry и mark-only-after-success,
  - единую переиспользуемую helper-логику между watcher/fast0 и TTL path.
- Подготовить диагностические логи `OUTCOME_TG_*` строго по контракту.

### QA / Validation Agent
- Подтвердить acceptance criteria:
  - для paper TTL TIMEOUT появляются `OUTCOME_TG_*`,
  - `outcome_tg_sent` обновляется только после success,
  - повторный TTL не дает duplicate TG,
  - live path не регрессирует.

### Production Safety Agent
- Проверить:
  - что изменения затронут только paper TIMEOUT TTL delivery,
  - что retry bounded и не создает TG flood,
  - что restart scope понятен.

---
## 9. Execution Log
| Date       | Who (Agent / Person)       | Action / Comment                                   |
|-----------|-----------------------------|----------------------------------------------------|
| 2026-03-18| PM Agent                    | Created PLAN spec for paper TIMEOUT TTL TG delivery fix. |
| 2026-03-18| Agent                       | Implemented `trading/outcome_delivery.py`, refactored watcher/fast0 to use it, and wired paper TTL TIMEOUT TG delivery via helper; ran `py_compile` + `smoke_fast0_outcome_tg_format.py`. |

---
## 10. Final Decision
- **Status**: In Spec
- **Reason**: Pending QA/PS review and next agent implementation.

