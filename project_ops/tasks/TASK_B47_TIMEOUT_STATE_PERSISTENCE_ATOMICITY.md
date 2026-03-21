# Task ID
B47_TIMEOUT_STATE_PERSISTENCE_ATOMICITY

## Title
Prevent stale state overwrite after timeout close and guarantee atomic persistence for outcome marks

## Epic
Outcome Tracking

## Priority
Critical

## Status
Done

---

## 1. Business / Strategy Context
- **Зачем задача нужна?**
  - Controlled validation по B42 подтвердила, что helper delivery path и TG send работают, а `outcome_tg_sent` корректно ставится в helper после delivery.
  - Несмотря на это, финальное состояние может потерять mark из-за последующего сохранения устаревшего snapshot в caller-flow.
- **Какую проблему она решает?**
  - Устраняет race/atomicity проблему persistence в timeout close path: после `close_on_timeout(...)` caller может сохранить stale `state` и перетереть свежие изменения (`outcome_tg_sent`, `open_positions`), уже сделанные внутри helper/close path.
  - Восстанавливает системный инвариант: successful delivery mark и результат timeout-close не должны исчезать после следующего `save_state(...)`.
- **На какие метрики влияет?**
  - Надежность dedup outcome delivery (исключение повторных TG для одного `delivery_key`).
  - Data consistency `trading_state.json` (`outcome_tg_sent`, `open_positions`).
  - Стабильность paper timeout flow без ложных reopen/duplicate событий.
  - Снижение повторных controlled-fail в validation цикле.

---

## 2. Scope
### In scope
1. Инвентаризация всех call-site, где вызывается `close_on_timeout(...)` и затем выполняется persistence состояния.
2. Анализ caller-flow после `close_on_timeout(...)` на предмет stale snapshot save.
3. Спецификация и внедрение минимального safe fix на уровне caller-flow:
   - запрет `save_state(state)` старым объектом после timeout close,
   - либо обязательный `load_state()` перед любым следующим persistence.
4. Фиксация единого persistence contract для timeout close path (post-close consistency).
5. Проверка отсутствия регрессии в runner paper path (включая B41/B42 outcome delivery contracts).

### Out of scope
1. Изменение бизнес-логики outcome formatting/sending в Telegram.
2. Изменение delivery-key semantics (`position_id`) и существующего B41 delivery contract.
3. Рефакторинг не относящихся к timeout close частей state/persistence слоя.
4. Изменения live execution path, не требуемые для устранения stale overwrite в текущем paper timeout flow.

---

## 3. Inputs
- **Кодовые модули:**
  - `trading/runner.py` (caller-flow timeout close и сохранение state)
  - `trading/paper_outcome.py` (`close_on_timeout(...)`, close path side-effects)
  - `trading/outcome_delivery.py` (marking `outcome_tg_sent` внутри helper path)
  - `trading/state.py` (`load_state`, `save_state`, `outcome_tg_sent` helpers)
  - `short_pump/watcher.py`, `short_pump/fast0_sampler.py` (контекст B41/B42 contract integration)
- **Конфиги / ENV / runtime параметры:**
  - `POSITION_TTL_SECONDS`, `TIMEOUT_EXIT_MODE`
  - `TG_SEND_OUTCOME` и связанные TG env (для controlled evidence на dedup)
- **Датасеты / state:**
  - `datasets/trading_state.json` (поля `open_positions`, `outcome_tg_sent`)
  - paper outcomes artifacts из controlled timeout runs
- **Логи / отчеты:**
  - RCA заметки после controlled B42 validation
  - runtime логи с `OUTCOME_TG_DELIVERED_AND_MARKED`, `OUTCOME_TG_SEND_SKIP_ALREADY_FINALIZED`
  - следы save/load state до и после timeout close

---

## 4. Spec
1. **Root cause фиксация (обязательная)**
   - Зафиксировать как baseline RCA:
     - helper delivery path работает корректно;
     - TG реально отправляется;
     - `outcome_tg_sent` mark записывается внутри helper;
     - mark теряется позже из-за stale state overwrite в caller-flow.

2. **Жесткое правило post-timeout-close persistence**
   - После `close_on_timeout(...)` **запрещено** сохранять stale state snapshot.
   - Любой caller, продолжающий работу после `close_on_timeout(...)`, обязан:
     - либо не делать `save_state(state)` старым объектом;
     - либо обязательно выполнить `state = load_state()` перед дальнейшим persistence.

3. **Инвентаризация call-sites**
   - Все места вызова `close_on_timeout(...)` должны быть инвентаризированы и классифицированы:
     - где есть последующий `save_state`,
     - где возможен stale snapshot,
     - где fix обязателен в первую очередь.
   - Для каждого call-site должен быть явно зафиксирован safe post-close behavior.

4. **Минимальный safe fix (implementation strategy)**
   - Принцип: **caller-flow fix first**.
   - Исправление должно быть минимально-инвазивным и локальным к persistence sequencing, без расширения функционального scope.
   - Обязательно не ломать B41/B42 контракты:
     - mark-only-after-success сохраняется;
     - dedup через `outcome_tg_sent` сохраняется;
     - existing delivery helper behavior не деградирует.

5. **Единый persistence contract для timeout close path**
   - Зафиксировать единый контракт:
     - изменения, выполненные внутри timeout close/helper path, считаются authoritative и не должны перетираться старым snapshot;
     - любые последующие state writes обязаны опираться на актуальное state (reload/merge-safe approach).
   - Контракт должен быть применим ко всем timeout close caller-flow, чтобы исключить расхождение поведения между runner/watcher/другими entry points.

6. **Guardrails и инварианты**
   - Нельзя допустить ошибочное восстановление позиции в `open_positions` после timeout close.
   - Нельзя допустить потерю `outcome_tg_sent` после успешной TG доставки.
   - Нельзя допустить регрессию runner paper path по текущему lifecycle закрытия позиций.

---

## 5. Acceptance Criteria
- [x] Выполнен и задокументирован полный inventory всех call-site `close_on_timeout(...)` с указанием post-close persistence поведения.
- [x] После `close_on_timeout(...)` отсутствует `save_state` stale snapshot без reload/merge-safe механизма.
- [x] Первый controlled run по timeout close дает:
  - `OUTCOME_TG_DELIVERED_AND_MARKED`
  - соответствующий `pid` появляется в `outcome_tg_sent`.
- [x] Второй controlled run для того же `delivery_key` дает:
  - `OUTCOME_TG_SEND_SKIP_ALREADY_FINALIZED`
  - duplicate TG не отправляется.
- [x] Stale state больше не перетирает `outcome_tg_sent` после успешного delivery.
- [x] `open_positions` не восстанавливается ошибочно после timeout close.
- [x] Нет регрессии в runner paper path (close lifecycle и persistence остаются корректными).

---

## 6. Validation Plan
- **Notes:**
  - Дрифт в `scripts/smoke_multi_positions.py` (использование `ttl_seconds` в `close_on_timeout` которого нет в `trading/paper_outcome.py`) зафиксирован как `Minor` finding в QA, будет вынесен в отдельный housekeeping backlog item. Не блокирует B47.

- **Тесты / smoke / ручные проверки:**
  - Подготовить controlled paper timeout сценарий (repeatable `delivery_key`) и выполнить два последовательных запуска runner.
  - Зафиксировать state snapshots (`before first run`, `after first run`, `after second run`) с фокусом на `open_positions` и `outcome_tg_sent`.
  - Проверить логи по маркерам `OUTCOME_TG_DELIVERED_AND_MARKED` и `OUTCOME_TG_SEND_SKIP_ALREADY_FINALIZED`.
  - Проверить, что после timeout close последующие persistence шаги не пишут stale snapshot.
- **Какие артефакты должны остаться после проверки:**
  - markdown-таблица inventory call-site `close_on_timeout(...)`;
  - лог-выдержки controlled run #1 и #2;
  - snapshots/диффы `datasets/trading_state.json`;
  - краткий validation summary с verdict по каждому acceptance criterion.

---

## 6.1 Validation Evidence
- **Call-site inventory (`close_on_timeout(...)`):**
  - `trading/runner.py` — primary runtime call-site; после timeout close есть дальнейшие `save_state(...)` в caller-flow, поэтому это primary stale-overwrite fix point.
  - `scripts/smoke_multi_positions.py` — test/smoke call-site; inspected but not changed в рамках B47 runtime fix scope.
- **Controlled double-run (isolated local env/state):**
  - Run #1: `OUTCOME_TG_DELIVERED_AND_MARKED` присутствует для `delivery_key=short_pump:r1:e1:BTCUSDT`.
  - После Run #1: `outcome_tg_sent` содержит `short_pump:r1:e1:BTCUSDT`.
  - Run #2 (same delivery_key): `OUTCOME_TG_SEND_SKIP_ALREADY_FINALIZED` присутствует.
  - После Run #2: duplicate отсутствует (`count(delivery_key)=1` в `outcome_tg_sent`).
- **Safety checks:**
  - Controlled validation запускался в изолированных `TRADING_STATE_PATH`/queue/log paths (без прод-операций).
  - `open_positions` после timeout close повторно не восстанавливалась в state snapshots.
- **Build sanity:**
  - `python3 -m py_compile trading/runner.py` ✅

---

## 7. Risks
- **Data risk**
  - Частичный fix только в одном caller может оставить другие call-site уязвимыми к stale overwrite.
- **Trading risk**
  - Ошибка в sequencing save/load может привести к неконсистентному timeout-close lifecycle в paper path.
- **Operational risk**
  - Недостаточная наблюдаемость post-close persistence шагов усложнит верификацию atomicity.
- **Rollback notes**
  - Rollback выполняется откатом caller-flow persistence changes до предыдущего stable поведения и повторной controlled validation B42/B47 сценария.

---

## 8. Agent Handoff
### PM Agent
- Подтвердить, что B47 идет как отдельная задача reliability/atomicity поверх B42 delivery.
- Утвердить minimal safe fix boundary (caller-flow first, без расширения scope в новые фичи).

### Strategy Developer Agent
- Выполнить call-site inventory `close_on_timeout(...)` и внедрить минимальный safe fix в caller-flow persistence sequencing.
- Зафиксировать единый timeout persistence contract в коде/документации без нарушения B41/B42.

### QA / Validation Agent
- Провести controlled double-run validation одного `delivery_key` и подтвердить отсутствие stale overwrite/duplicate TG.
- Подтвердить отсутствие regressions в runner paper path.

### Production Safety Agent
- Проверить, что fix не влияет на live/risk-policy и ограничен paper timeout persistence semantics.
- Подтвердить rollback-подход и достаточность observability для post-fix контроля.

---

## 9. Execution Log
| Date       | Who (Agent / Person)       | Action / Comment                                   |
|-----------|-----------------------------|----------------------------------------------------|
| 2026-03-19| PM Agent                    | Created B47 spec to address stale timeout state overwrite after B42 controlled validation RCA. |
| 2026-03-19| Strategy Developer Agent    | Completed call-site inventory, confirmed primary fix point in runner caller-flow, executed controlled isolated double-run validation (DELIVERED_AND_MARKED -> SKIP_ALREADY_FINALIZED, no duplicate). |
| 2026-03-19| QA / Validation Agent       | Performed QA review. Verdict: PASS WITH COMMENTS (non-blocking process/doc hygiene issues). |

---

## 10. Final Decision
- **Status**: Approved with notes
- **Reason**:
  - Primary stale overwrite point в `trading/runner.py` устранен (используется `state = load_state()` после `close_on_timeout(...)`).
  - Controlled double-run validation подтвердил:
    - First run -> `OUTCOME_TG_DELIVERED_AND_MARKED`.
    - PID сохраняется в `outcome_tg_sent`.
    - Second run -> `OUTCOME_TG_SEND_SKIP_ALREADY_FINALIZED`.
    - Duplicate TG не отправляется.
  - Регрессии B41/B42 не обнаружены.
  - Remaining notes относятся к `process traceability` и `task doc hygiene`, не блокируют закрытие B47.
- **Links to evidence**:
  - RCA notes from controlled B42 validation (helper marks present before caller overwrite).
  - Controlled validation run evidence (см. `6.1 Validation Evidence`).
  - AGENT stage verification (см. `project_ops/AGENT_VERIFICATION.md` или transcripts).
  
