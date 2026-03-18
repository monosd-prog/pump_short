# Task ID
B41_OUTCOME

## Title
Outcome tracking: ensure each eligible signal produces outcome + Telegram outcome (LIVE and PAPER)

## Epic
Outcome Tracking

## Priority
Critical

## Status
In Validation

---
## 1. Business / Strategy Context
- **Зачем задача нужна?**
  - Telegram outcome сообщения — главный пользовательский сигнал и также ключевой источник подтверждения, что outcome tracking и labeling пайплайн работает.
- **Какую проблему она решает?**
  - “Outcome не приходит в Telegram”: часть кейсов, которые доходят до ENTRY_OK/TRADEABLE/GUARD_BLOCKED_PAPER в TG, не получают outcome сообщения (при этом по ТЗ должны приходить и для LIVE, и для PAPER).
- **На какие метрики влияет?**
  - coverage outcomes→TG, доля “потерянных” сделок, корректность dataset labeling (outcomes_v3/close events) и trust в compact report / analytics.

---
## 2. Scope
### In scope
1. Энд-ту-энд анализ пути “signal -> outcome -> TG” для стратегий:
  - `short_pump` (live outcomes via watcher + paper outcomes),
  - `short_pump_fast0` (live outcomes via fast0_sampler/outcome_worker; paper outcomes via close paths).
2. Специфицировать, где событие может теряться:
  - outcome_row не создается в `outcomes_v3`,
  - outcome_row создается, но TG-уведомление не отправляется,
  - TG отправляется, но теряется из-за skip/dedup/rate-limit/ошибок formatting/HTTP.
3. Добавить reconciliation/диагностику и (если нужно) исправить:
  - корректность skip-логики (watcher + runner/outcome-worker),
  - dedup/idempotency ключей (`outcome_tg_sent` state vs watcher keys),
  - соответствие env-флагов (`TG_SEND_OUTCOME`) по процессам (watcher/runner/outcome-worker).
4. Проверить, что после исправления:
  - нет “потерянных” outcome сообщений для обоих режимов (LIVE и PAPER),
  - нет критичных дублей (или дубли контролируются теми же правилами).

### Out of scope
- Не менять правила trade execution (open/close на бирже) кроме диагностических/вспомогательных переключателей.
- Не делать ML/датасет переразметку без подтверждения completeness outcome→TG.

---
## 3. Inputs
- **Кодовые модули (primary)**
  - `short_pump/watcher.py`
    - ENTRY_OK/TG сигнал,
    - outcome tracking для `short_pump`,
    - TG send logic для outcomes: `TG_SEND_OUTCOME`, `_watcher_should_skip_outcome_live`, `send_telegram(format_outcome(...))`.
  - `trading/paper_outcome.py`
    - `close_from_outcome`, `close_from_live_outcome`,
    - где добавляется `outcome_tg_sent` (`add_outcome_tg_sent`).
  - `trading/outcome_worker.py`
    - какие пути отправки OUTCOME TG в live path,
    - skip logic по `outcome_tg_sent`.
  - `trading/state.py`
    - `make_position_id`, `outcome_tg_sent`, `add_outcome_tg_sent` (критично для dedup).
  - `short_pump/fast0_sampler.py`
    - fast0 entry->enqueue->outcome watcher,
    - TG entry logic.
  - `notifications/*` (formatters для TG outcome message)
    - точная формат-компонента, которая может падать/не попадать при определенных полях.
  - `common/io_dataset.py`
    - `write_outcome_row` / duplicate-skipping in datasets (если outcome row может быть пропущен).
  - `analytics/load.py`
    - loader/normalizer, чтобы сравнивать outcomes_v3 raw vs отчет/labels.
- **ENV / конфиги**
  - `TG_SEND_OUTCOME` (должен быть 1 в тех процессах, которые обязаны отправлять outcome TG).
  - `EXECUTION_MODE`, `AUTO_TRADING_ENABLE` (влияют на skip outcome TG в watcher).
  - fast0 outcome enable/limits (если применимо).
- **Датасеты / таблицы**
  - `datasets/**/outcomes_v3.csv`
  - `datasets/**/trades_v3.csv`
  - `datasets/**/events_v3.csv` (если outcomes связываются с events)
  - `datasets/trading_state.json` (ключи `outcome_tg_sent` и open_positions)
- **Логи / артефакты**
  - TG logs: ENTRY_OK / TRADEABLE / GUARD_BLOCKED_PAPER и ожидаемые outcome messages.
  - Логи runtime/watcher/runner/outcome-worker (по run_id/eid/symbol).
  - Compact report до/после (как косвенное подтверждение completeness).

---
## 4. Spec
- **Что именно нужно изменить?**
1. Сформировать “контрольную выборку” кейсов:
  - входные сигналы из TG (run_id, event_id/eid, symbol, стратегия, режим: paper/live),
  - для этих кейсов определить expected outcomes (TP_hit/SL_hit/TIMEOUT) и где они должны появиться.
2. Реализовать reconciliation pipeline (без вмешательства в trading execution):
  - Вход: список TG-сигналов.
  - Шаг A: проверить, что outcome запись существует в datasets:
    - для каждого run_id / trade_id / event_id найти строку outcome в `outcomes_v3.csv`.
  - Шаг B: проверить, что TG-уведомление отправлено:
    - либо по факту TG logs (если доступен),
    - либо по state key `outcome_tg_sent` (где это уместно).
  - Шаг C: классифицировать loss type:
    - (1) outcome row отсутствует,
    - (2) outcome row есть, но TG outcome нет,
    - (3) TG outcome есть, но datasets outcome отсутствует,
    - (4) дубли/идемпотентность.
3. Проверить и формализовать skip/dedup контур:
  - `short_pump/watcher.py`:
    - `_watcher_should_skip_outcome_live` (skip_reason: `live_mode`, `auto_trading_outcome_via_runner`, `already_finalized`),
    - влияние `TG_SEND_OUTCOME`.
  - `trading/state.py`:
    - соответствие ключа `make_position_id(...)` тому, который используется в `add_outcome_tg_sent` в `paper_outcome.py`.
  - `trading/outcome_worker.py`:
    - skip по `outcome_tg_sent` и то, какие исходы проходят.
4. Точечные fix направления (в рамках следующего AGENT stage):
  - Если root cause в env-рассогласовании: унифицировать env-fлаг или обеспечить одинаковые значения `TG_SEND_OUTCOME` в watcher/runner/outcome-worker для обязательного режима.
  - Если root cause в dedup key mismatch: привести к единой схеме ключей (position_id vs trade_id) и убедиться, что watcher либо:
    - вызывает `add_outcome_tg_sent` после успешной отправки outcome TG, либо
    - корректно пропускает send в том же случае, где добавляет ключ.
  - Если root cause в skip_reason: уточнить условия skip для paper/live, чтобы watcher не блокировал send “раньше” чем outcome-worker.
  - Если root cause в exceptions/formatting: добавить fallback полей для format_outcome и жестко перехватывать ошибки формирования message.
5. После исправления добавить регрессионный smoke/диагностику (в QA):
  - на фиксированном window/seed кейсах outcomes_v3 и TG outcome должны совпасть по полноте.

## Outcome Delivery Contract
Определи единые архитектурные правила доставки outcome. Эти правила являются обязательными инвариантами для всей системы (watcher / worker / paper close paths).

1. **Single source of truth**
   - outcome считается доставленным **ТОЛЬКО** если:
     - TG send успешен (отсутствует исключение/HTTP success),
     - и **только после этого** записывается `outcome_tg_sent`.
2. **Запрет**
   - запрещено записывать `outcome_tg_sent` **до** фактической успешной отправки в Telegram.
3. **Retry policy**
   - если TG send неуспешен:
     - outcome **не** помечается как отправленный,
     - допускается повторная попытка в последующих циклах.
4. **Idempotency (единый ключ)**
   - определить единый ключ дедупликации:
     - **`position_id`** как `make_position_id(strategy, run_id, event_id, symbol)`.
   - все компоненты (watcher, outcome_worker, paper close paths) должны использовать **один и тот же ключ-тождество**:
     - `outcome_tg_sent` всегда заполняется именно этим `position_id`,
     - `trade_id` не должен подменять delivery-key (или trade_id гарантированно эквивалентен position_id и никогда не смешивается).
5. **Watcher fallback**
   - watcher должен иметь право отправить outcome, если:
     - outcome существует (есть outcome row в `outcomes_v3.csv` / close path завершился),
     - но TG сообщение фактически не было доставлено (и/или соответствующий ключ не присутствует в `outcome_tg_sent`).
   - watcher соблюдает contract: `outcome_tg_sent` записывается **только после** успешного TG send.

- **Какие ограничения есть?**
  - Не менять бизнес-логику исполнения сделок (open/close).
  - Допускается изменение логики отправки TG и state/dedup только в рамках “обязательности outcome TG” и с регрессионными проверками отсутствия дублей.

- **Что нельзя ломать?**
  - Контракты datasets (`outcomes_v3.csv`, ключи для dedupe/duplicate skipping).
  - Idempotency: должно остаться гарантированное отсутствие критичных дублей.

---
## 5. Acceptance Criteria
- [ ] **0 missing outcomes**: для контрольного окна каждый ожидаемый кейс (TG ENTRY_OK/TRADEABLE/GUARD_BLOCKED_PAPER) имеет:
  - outcome row в `outcomes_v3.csv`,
  - и соответствующий TG outcome message (LIVE и PAPER отдельно),
  - суммарно отсутствующие кейсы = 0 (missing outcome rows = 0 и missing TG outcomes = 0).
- [ ] **State ↔ TG consistency**: множество ключей в `state.outcome_tg_sent` соответствует фактически доставленным Telegram outcome:
  - каждый ключ из `outcome_tg_sent` имеет соответствующий успешный TG delivery в окне,
  - каждый успешный TG delivery приводит к записи ключа в `outcome_tg_sent` (после success).
- [ ] **Единый dedup key**: во всей системе используется один delivery-key:
  - reconciliation подтверждает отсутствие “смешения” `trade_id`/`position_id`,
  - для каждого loss/duplicate кейса явно указан key-type и ключ-тождество.
- [ ] No critical duplicates:
  - для каждого delivery-key допускается максимум 1 успешный TG outcome delivery.
- [ ] Результаты reconciliation оформлены и логируются:
  - таблица loss types,
  - указание skip_reason/dedup key для случаев loss.

---
## 6. Validation Plan
- **Тесты / smoke / replay / ручные проверки:**
  - Прогнать локальный/стейдж reconciliation на одном окне, где известны “потерянные” кейсы:
    - сравнить TG logs entry set vs outcomes_v3 rows vs outcome_tg_sent state.
  - Подтвердить для both modes:
    - mode=live OUTCOME,
    - mode=paper OUTCOME.
- **Какие артефакты должны остаться после проверки**
  - reconciliation report (CSV/JSON + stdout summary):
    - counts: expected entries, found outcomes rows, TG outcomes sent count.
    - breakdown by loss type and skip_reason.

## 6.1 Validation Evidence
- `python3 -m py_compile trading/paper_outcome.py trading/outcome_worker.py short_pump/watcher.py short_pump/fast0_sampler.py` ✅
- `python3 scripts/smoke_fast0_outcome_tg_format.py` ✅
- `python3 scripts/smoke_tg_tradeable_gate.py` ❌ (AssertionError: `is_fast0_tg_entry_allowed({"liq_long_usd_30s": 0})` возвращает `True`; причина несоответствия ожиданий теста текущей логике `trading/risk_profile.is_fast0_entry_allowed` — не связано с Outcome Delivery Contract)

## Release Evidence
- Commit: TBD (after creating `git commit` with message containing `TASK_B41_OUTCOME`)
- Push: TBD (after `git push`)
- Restart required: yes
- Restart scope:
  - `pump-short.service` (API process hosting `short_pump/watcher.py` + `short_pump/fast0_sampler.py` watcher/threads)
  - `pump-short-live-auto.service` (auto-trading API process; same watcher/fast0 entrypoints but with live/autotrading enabled)
  - `pump-trading-runner.timer` (timer that starts `pump-trading-runner.service` → `trading.runner --once` → `trading/outcome_worker.py` + `trading/paper_outcome.py`)
  - NOT required: `pump-short-report-bot.service` / any report-only timers (B41 changes affect outcome->TG delivery, not report generation)
- Commands executed:
  - (After commit/push; not executed now)
    - `git status`
    - `git add <files changed for TASK_B41_OUTCOME>` (evidence update in `project_ops/tasks/TASK_B41_OUTCOME.md` included)
    - `git commit -m "fix(TASK_B41_OUTCOME): outcome_tg_sent only after TG success + unified position_id delivery key"`
    - `git push`
  - Restart / verification (not executed now; run only if units exist/enabled):
    - `systemctl list-units --type=service --all | rg -n "pump-short\\.(service|live-auto\\.service)|pump-trading-runner\\.service" || true`
    - `systemctl list-timers --all | rg -n "pump-trading-runner\\.timer" || true`
    - `systemctl is-enabled pump-short.service`
    - `systemctl is-enabled pump-short-live-auto.service`
    - `systemctl is-enabled pump-trading-runner.timer`
    - `sudo systemctl restart pump-short.service`
    - `sudo systemctl restart pump-short-live-auto.service`
    - `sudo systemctl restart pump-trading-runner.timer`
    - `systemctl status --no-pager -l pump-short.service`
    - `systemctl status --no-pager -l pump-short-live-auto.service`
    - `systemctl status --no-pager -l pump-trading-runner.timer`
- Post-release check:
  - Verify service reload and no runtime exceptions around TG/outcome delivery:
    - `journalctl -u pump-short.service -b --no-pager -n 300 | tail -n 100`
    - `journalctl -u pump-short-live-auto.service -b --no-pager -n 300 | tail -n 100`
  - Verify outcome->TG delivery markers in file logs after restart (watch for next outcomes):
    - `OUT_DIR=/root/pump_short/logs_short/$(date -u +%F); ls -la "$OUT_DIR" | head`
    - `for f in "$OUT_DIR"/*.log; do rg -n "OUTCOME_TG_SEND_START|OUTCOME_TG_DELIVERED_AND_MARKED|OUTCOME_TG_SEND_ATTEMPT_FAILED|OUTCOME_TG_SEND_SKIP_ALREADY_FINALIZED" "$f" | head -n 5; done`
  - Verify state transition evidence: `datasets/trading_state.json` update reflects successful deliveries:
    - `python3 - <<'PY'\nimport json\np='datasets/trading_state.json'\nwith open(p,'r',encoding='utf-8') as f: d=json.load(f)\nsent=d.get('outcome_tg_sent') or []\nprint('outcome_tg_sent_len=', len(sent))\nprint('outcome_tg_sent_tail=', sent[-5:] if isinstance(sent,list) and sent else [])\nPY`
- Notes:
  - Restart нужен, потому что B41 меняет runtime-path для доставки outcome в TG:
    - `short_pump/watcher.py`: watcher теперь делает TG send и **только после успеха** маркирует `state.outcome_tg_sent` по `delivery_key = position_id`.
    - `short_pump/fast0_sampler.py`: FAST0 outcome TG delivery теперь также следует contract (retry + mark only after success).
    - `trading/outcome_worker.py` + `trading/paper_outcome.py`: убран риск неправильных skip/ранней маркировки и приведен delivery-key подход.
  - Не нужно рестартовать “все подряд”: затронуты только процессы, которые могут отправлять/маркировать TG outcome:
    - API (watcher + fast0 threads) и runner/timer (в котором живет outcome_worker close path).
    - report-only сервисы не трогаются, так как контракт доставки outcome не влияет напрямую на генерацию compact reports.
  - После рестарта проверяем outcome->TG delivery именно через:
    - появление `OUTCOME_TG_*` лог-маркеров,
    - отсутствие delivery-key без `OUTCOME_TG_DELIVERED_AND_MARKED`,
    - рост/обновление `outcome_tg_sent` после факта успешной TG отправки (а не до).

---
## 7. Risks
- **Trading risk**
  - Низкий при условии: не трогаем open/close logic.
- **Data risk**
  - Средний: outcome dedupe/duplicate skipping в dataset writer может скрыть часть строк и “ломать” reconciliation, если проверка построена только на файлах.
- **Operational risk**
  - Medium: разные процессы (watcher/runner/outcome-worker/fast0 sampler) могут иметь разные ENV, что ломает TG completeness.
- **Risk: TG duplication**
  - Retry/ordering “отметили sent раньше успешной отправки” либо неправильная дедупликация могут привести к повторной отправке одного и того же outcome в TG.
- **Risk: TG flood**
  - Неправильная retry-политика без строгого delivery-key может создать повторные попытки отправки в высокой частоте и перегрузить TG/канал.
- **Rollback notes**
  - Откат возможен через отклонение изменений TG send policy/skip policy или временное выключение новых диагностик.

---
## 8. Agent Handoff
### PM Agent
- **Ответственность на этой задаче:**
  - Определить контрольный window (date range) и список `run_id/symbol/eid` кейсов, которые считаются обязательными.
- **Сделано / нужно сделать PM:**
  - [ ] Утвердить ключи сопоставления (run_id vs trade_id vs event_id) для reconciliation.
  - [ ] Подтвердить ожидаемые outcome типы (TP_hit/SL_hit/TIMEOUT) для выбранного окна.

### Strategy Developer Agent
- **Ответственность на этой задаче:**
  - Реализовать изменения в TG send/dedup/skip/diagnostics так, чтобы outcome приходил в TG без дублей.
- **Сделано / нужно сделать Strategy Dev:**
  - [ ] Привести к единой схеме ключей state outcome_tg_sent (trade_id vs position_id).
  - [ ] Обеспечить корректные skip_reason при auto_trading и EXECUTION_MODE.

### QA / Validation Agent
- **Ответственность на этой задаче:**
  - Проверить completeness outcome->TG и отсутствие “потерянных” сделок на контрольной выборке.
- **Сделано / нужно сделать QA:**
  - [ ] Запустить reconciliation на window и собрать evidence.

### Production Safety Agent
- **Ответственность на этой задаче:**
  - Убедиться, что изменения касаются только TG/outcome dedup policy и не ломают прод процессы.
- **Сделано / нужно сделать Production Safety**:
  - [ ] Подтвердить, что исправления не создают runaway TG spam (rate-limits/side effects).

---
## 9. Execution Log
| Date       | Who (Agent / Person)       | Action / Comment                                   |
|-----------|-----------------------------|----------------------------------------------------|
| 2026-03-18| PM Agent                    | Created full Spec for B41_OUTCOME (PLAN stage). |
| 2026-03-18| AGENT (Implementation)      | Outcome Delivery Contract: mark `outcome_tg_sent` only after successful TG send (watcher/fast0_sampler), unified delivery key = `position_id` via `make_position_id`, removed premature marking from `trading/paper_outcome.py`, removed `outcome_tg_sent`-based skipping from `trading/outcome_worker.py`, added bounded TG send retry + `delivery_key` diagnostics. Local checks: `py_compile` ✅, `smoke_fast0_outcome_tg_format` ✅, `smoke_tg_tradeable_gate` ❌ (test expectation vs current risk_profile). |

---
## 10. Final Decision
- **Status**: Approved
- **Reason**: Draft Spec готов для запуска workflow в AGENT stage при прохождении QA/PS review.
- **Links to evidence**:
  - TG входные сообщения (ENTRY_OK/TRADEABLE/GUARD_BLOCKED_PAPER) за окно `eid=20260318`.
  - наблюдение: outcome отсутствует в TG при наличии “expected outcomes” по ТЗ.

