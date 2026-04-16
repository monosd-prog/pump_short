# Task Brief — TASK_B54_AUTO_DEBUG_PIPELINE

> Формат: `.continue/task_template.md`
> Agent MUST NOT proceed to IMPLEMENT without completed sections 1–7.

---

## 1. Title

**Task ID**: `TASK_B54_AUTO_DEBUG_PIPELINE`
**Title**: Автоматический анализ логов trading-бота на VPS — диагностика проблем без изменения кода

---

## 2. Goal

- **Problem**: При сбоях на VPS (нет сигналов, зависшие позиции, ERROR/EXCEPTION в логах) нет инструмента, который автоматически читает все источники логов, классифицирует проблему и выдаёт конкретный root cause и план действий.
- **Expected outcome**: Агент запускает pipeline анализа, читает `logs/*` и `journalctl pump-short.service`, обнаруживает паттерны сбоев и выдаёт структурированный отчёт: `diagnosis`, `root_cause`, `issues[]`, `suggested_fixes[]`, `check_commands[]`.
- **Affected metrics**: _(latency обнаружения сбоя / signal frequency / data quality)_

---

## 3. Context

- **Strategy**: `short_pump` / `short_pump_fast0`
- **Execution mode**: `live`
- **Current behavior**: Диагностика проводится вручную: инженер смотрит `journalctl`, листает `logs/*.csv`, проверяет `datasets/trading_state.json` — медленно, ошибочно, требует знания контекста.
- **Desired behavior**: Один запуск агента анализирует все источники, классифицирует сбой по типу (нет сигналов / нет outcome / ERROR / зависшая позиция), объясняет root cause и предлагает конкретные shell-команды для подтверждения и исправления.
- **Related docs/decisions**:
  - `docs/ARCHITECTURE_BRIEF.md` — полный pipeline, systemd, файлы состояния
  - `docs/DIAG_CYBERUSDT_20260314_LIVE_OPEN_NO_POSITION.md` — пример зависшей позиции
  - `docs/DIAG_DUPLICATE_LIVE_OUTCOME_DATASET.md` — пример дублированного outcome
  - `docs/DIAG_FAST0_LIVE_DOODUSDT_20260313.md` — пример live outcome fallback
  - `docs/DIAG_EARLY_EXIT_AND_CONFLICTS.md` — конфликты entry/exit

---

## 4. Inputs

**Files to read (анализируемые источники)**:
```
- logs/*.csv                              # торговые события: events, trades, outcomes по символам
- datasets/trading_state.json             # открытые позиции, last_signal_ids
- datasets/trading_trades.csv             # лог открытий runner-а
- datasets/trading_closes.csv             # лог закрытий (paper/live)
- datasets/signals_queue.jsonl            # текущая очередь сигналов
- datasets/signals_queue.processing       # сигнал в обработке (если завис)
- datasets/trading_runner.lock            # блокировка runner (если не снята — runner завис)
```

**Системные источники (shell)**:
```bash
journalctl -u pump-short.service -n 500 --no-pager   # последние 500 строк systemd-юнита
journalctl -u pump-short.service --since "1 hour ago" --no-pager
systemctl status pump-short.service                   # статус юнита
ls -lth logs/ | head -30                              # последние изменённые логи
```

**Reference files** (read-only, только для понимания контекста):
```
- docs/ARCHITECTURE_BRIEF.md              # pipeline, компоненты, контракты
- short_pump/runtime.py                   # Runtime, apply_filters, start_watch
- short_pump/watcher.py                   # run_watch_for_symbol, entry, outcome
- short_pump/fast0_sampler.py             # fast0 pipeline, enqueue, outcome
- trading/runner.py                       # runner: чтение queue, risk, open_position
- trading/state.py                        # open_positions, record_open/close
- trading/paper_outcome.py                # close_from_outcome, close_from_live_outcome
```

**Config / ENV**:
```
- .env (VPS)                              # EXECUTION_MODE, AUTO_TRADING_ENABLE, STRATEGIES
- deploy/systemd/pump-short-live.env.example  # структура переменных окружения
```

---

## 5. Constraints

- [ ] **НЕ изменять код стратегии** — ни `short_pump/`, ни `trading/`, ни `common/`
- [ ] **Только анализ и чтение** — никаких write-операций в `datasets/`, `logs/`, `.env`
- [ ] **Не перезапускать сервисы** — не выполнять `systemctl restart`, `systemctl stop`
- [ ] **Не изменять `trading_state.json`** — только читать; любое исправление state — только через явную инструкцию пользователю
- [ ] **Не удалять очередь / .lock файлы** — предлагать команды, но не выполнять
- [ ] Выводить только факты из логов + обоснованные гипотезы с указанием источника
- [ ] Каждый `suggested_fix` сопровождать shell-командой для верификации
- [ ] Если root cause неоднозначен — перечислить все версии с вероятностью / признаком

---

## 6. Expected Output

### Структура отчёта (diagnosis report):

```
DIAGNOSIS REPORT — TASK_B54_AUTO_DEBUG_PIPELINE
================================================

## Summary
- Период анализа: <from> — <to>
- Источники: journalctl, logs/, trading_state.json, signals_queue
- Обнаружено проблем: N

## Issues Found

### ISSUE-1: <короткое название>
- Тип: ERROR | NO_SIGNAL | NO_OUTCOME | STUCK_POSITION | RUNNER_HANG | OTHER
- Severity: CRITICAL | HIGH | MEDIUM | LOW
- Источник: <файл/строка/timestamp>
- Описание: <что произошло>

### ISSUE-2: ...

## Root Cause Analysis

### Root Cause #1 (для ISSUE-1):
- Гипотеза: <объяснение>
- Признак в логах: <цитата из лога / timestamp>
- Связанный компонент: <runtime | watcher | fast0 | runner | outcome | liquidations>

## Suggested Fixes

### Fix #1 (для ISSUE-1):
- Действие: <что сделать>
- Команда проверки:
  ```bash
  # команда
  ```
- Команда исправления (если применимо):
  ```bash
  # команда (только если безопасно)
  ```
- Риск: <что может пойти не так>

## Check Commands (полный набор)

```bash
# 1. Статус сервиса
systemctl status pump-short.service

# 2. Последние ошибки в journalctl
journalctl -u pump-short.service -n 200 --no-pager | grep -E "ERROR|EXCEPTION|Traceback"

# 3. Открытые позиции
cat datasets/trading_state.json | python3 -m json.tool | grep -A5 "open_positions"

# 4. Проверка зависшего lock
ls -lh datasets/trading_runner.lock

# 5. Очередь сигналов
cat datasets/signals_queue.jsonl | tail -10

# 6. Последние логи символов
ls -lth logs/ | head -20

# 7. Последние исходы (outcomes)
ls -lth datasets/ | grep outcomes | head -10
```
```

### Минимальные обязательные поля вывода:
- `diagnosis` — краткое резюме (1–3 предложения) состояния системы
- `root_cause` — основная причина сбоя (или "no issues found")
- `issues[]` — список найденных проблем с типом и severity
- `suggested_fixes[]` — конкретные действия (без изменений кода)
- `check_commands[]` — верифицированные shell-команды для проверки на VPS

---

## 7. Validation

**Smoke / manual check**:
```bash
# Проверить, что агент корректно читает journalctl (без ошибок парсинга)
journalctl -u pump-short.service -n 100 --no-pager | grep -c "ERROR"

# Проверить наличие анализируемых файлов
ls -lh logs/*.csv | wc -l
ls -lh datasets/trading_state.json

# Проверить что .lock не завис (runner не работает > 60 сек)
find datasets/ -name "*.lock" -mmin +2
```

**Проверка качества отчёта**:
- [ ] Все найденные `ERROR` / `EXCEPTION` из journalctl присутствуют в `issues[]`
- [ ] Каждый `issue` имеет `root_cause` (пусть даже "требует уточнения")
- [ ] Каждый `suggested_fix` содержит shell-команду
- [ ] Нет записей о "изменении кода" или "правке конфига" без явного предупреждения
- [ ] Отчёт содержит timestamp последнего успешного сигнала (из `logs/` или `trading_trades.csv`)
- [ ] Отчёт содержит статус `pump-short.service` (active / failed / inactive)

**Edge cases для проверки агента**:
- [ ] `journalctl` пуст (сервис не запускался) → отчёт: "сервис не запускался, проверьте `systemctl status`"
- [ ] `trading_state.json` содержит позиции старше 24 часов → ISSUE: STUCK_POSITION
- [ ] `signals_queue.processing` существует и старше 5 минут → ISSUE: RUNNER_HANG
- [ ] `logs/` содержит файлы без `outcome` строк → ISSUE: NO_OUTCOME
- [ ] Нет ни одного лога за последние 2 часа при работающем сервисе → ISSUE: NO_SIGNAL

**Definition of done**:
- [ ] Отчёт выведен в консоль / файл в структурированном формате
- [ ] Все `issues` классифицированы по типу и severity
- [ ] Нет изменений в коде, конфигах, датасетах
- [ ] `check_commands` корректны и безопасны для выполнения на VPS
- [ ] `suggested_fixes` не содержат деструктивных операций без предупреждения

---

## 8. Risks

| Risk | Severity | Mitigation |
|------|----------|------------|
| Ложноположительный root cause | Medium | Цитировать источник в логах для каждой гипотезы |
| Пропущенная проблема (false negative) | High | Читать все источники: journalctl + logs/ + state.json + queue |
| Агент предлагает изменить код стратегии | High | Явный constraint: только анализ, не трогать стратегию |
| Агент выполняет `systemctl restart` | Critical | Constraint: команды только в `check_commands`, не выполнять автоматически |
| Чтение устаревших логов (не тот VPS path) | Medium | Проверять реальные пути через `ls -lth logs/` перед анализом |
| trading_state.json повреждён / невалидный JSON | Medium | Обработать JSON parse error, сообщить об этом как об ISSUE |
| journalctl недоступен (права) | Low | Сообщить о недоступности, продолжить анализ logs/ |

**Rollback plan**:
- Нет изменений кода — rollback не требуется.
- Если пользователь выполнил `suggested_fix` и возникла проблема: откат через `git stash` / восстановление `trading_state.json` из backup.

---

## 9. Out of Scope

- Не изменять логику стратегии (`short_pump/`, `trading/`, `common/`)
- Не изменять ML-модели или веса
- Не создавать новые файлы в `datasets/`
- Не добавлять новые колонки в `events_v3.csv` / `outcomes_v3.csv`
- Не настраивать systemd-юниты
- Не изменять `.env` / конфиги
- Не выполнять shell-команды автоматически — только предлагать
- Не анализировать стратегию `long_pullback` (не входит в scope)
- Нет UI / Telegram-отчётности (только консольный вывод)

---

## 10. Model Recommendation

| Stage | Recommended model | Reason |
|-------|------------------|--------|
| PLAN | `high` | Анализ архитектуры, определение источников логов |
| IMPLEMENT | `high` | Сложная логика парсинга, классификация сбоев, неоднозначный контекст |
| VALIDATE | `high` | Проверка полноты диагностики, безопасность команд |
| REVIEW | `high` | Риск ложного root cause, влияние на live торговлю |

**Overall task risk**: `medium`
- Код не меняется, но неправильный `suggested_fix` может привести к некорректным действиям на live VPS.

---

## 11. Mode Recommendation

| Stage | Mode |
|-------|------|
| PLAN | `ask` |
| IMPLEMENT | `ask` |
| VALIDATE | `ask` |
| REVIEW | `ask` |

> **Обоснование `ask` для всех стадий**: задача — анализ и диагностика, не кодирование.
> Агент должен задавать уточняющие вопросы, предлагать гипотезы и ждать подтверждения
> перед выдачей финального отчёта. `agent`-режим не нужен — нет автоматических изменений.

---

## Appendix — Паттерны сбоев и сигнатуры

| Тип проблемы | Источник | Признак |
|---|---|---|
| `ERROR` / `EXCEPTION` | journalctl | `ERROR`, `EXCEPTION`, `Traceback`, `raise` |
| `NO_SIGNAL` | logs/, journalctl | нет новых `logs/*.csv` за > 2 часа при активном сервисе |
| `NO_OUTCOME` | logs/ по символу | файл без строк outcome / пустой outcomes_v3 |
| `STUCK_POSITION` | trading_state.json | `open_positions` с `ts` старше 24ч (для live) |
| `RUNNER_HANG` | .lock / .processing | `trading_runner.lock` или `.processing` старше 5 мин |
| `QUEUE_OVERFLOW` | signals_queue.jsonl | > 10 сигналов в очереди (runner не успевает) |
| `BYBIT_API_ERROR` | journalctl | `bybit`, `10001`, `403`, `timeout`, `ConnectionError` |
| `LIQUIDATION_WS_DOWN` | journalctl | `liquidations`, `WebSocket`, `disconnect`, `reconnect` |
| `FAST0_OUTCOME_UNKNOWN` | logs/ | `UNKNOWN_LIVE` в outcome колонке |
| `DUPLICATE_OUTCOME` | outcomes_v3.csv | одинаковый `event_id` встречается > 1 раза |
