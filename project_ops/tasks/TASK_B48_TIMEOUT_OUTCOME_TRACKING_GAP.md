# Task ID
B48_TIMEOUT_OUTCOME_TRACKING_GAP

## Title
Find and restore outcome tracking coverage for timeout-dominant positions

## Epic
Outcome Tracking

## Priority
Critical

## Status
In Spec

---

## 1. Business / Strategy Context
- **Зачем задача нужна?**
  - RCA по TIMEOUT-анomalies показал, что paper/live outcomes часто приходят как late timeout close с `exit=entry`, `mae=0`, `mfe=0`, даже когда по рынку было движение.
  - Это мешает корректно оценивать качество стратегии и скрывает реальные TP/SL outcomes.
- **Какую проблему она решает?**
  - Помогает найти lifecycle gap: позиция открылась, но активный outcome tracker не стартовал, стартовал слишком поздно или не увидел позицию.
  - Отделяет настоящий timeout-close behavior от формата сообщения/фоллбэка, чтобы не лечить симптом вместо причины.
- **На какие метрики влияет?**
  - Coverage по TP/SL outcomes.
  - Доля timeout-only rows vs real tracked outcomes.
  - Достоверность `mae/mfe` и `hold_seconds`.
  - Качество diagnostics для paper/live parity.
  - Снижение ложных TIMEOUT и поздних outcome сообщений.

---

## 2. Scope
### In scope
1. Инвентаризация всех entry paths для:
   - `short_pump`
   - `short_pump_fast0`
   - paper
   - live
2. Определение, какой path после открытия позиции:
   - подключает позицию к активному outcome tracker,
   - где именно начинается мониторинг,
   - где считаются TP/SL,
   - где считаются MFE/MAE,
   - где срабатывает timeout fallback.
3. Поиск lifecycle gap:
   - position opened,
   - но tracker/monitoring не стартует, стартует слишком поздно или не видит позицию.
4. Разделение двух причин:
   - timeout fallback formatting artifact,
   - реальное отсутствие TP/SL monitoring.
5. Подготовка minimal safe fix plan:
   - сначала восстановить tracking coverage,
   - timeout formatting не менять до подтверждения gap,
   - не ломать live path.

### Out of scope
1. Изменение финального timeout formatting в Telegram/CSV до завершения RCA.
2. Переписывание outcome label semantics.
3. Массовый рефакторинг pipeline без точного подтверждения gap.
4. Изменение risk-policy или entry logic.

---

## 3. Inputs
- **Кодовые модули:**
  - `short_pump/watcher.py`
  - `short_pump/fast0_sampler.py`
  - `short_pump/outcome.py`
  - `common/outcome_tracker.py`
  - `trading/runner.py`
  - `trading/paper_outcome.py`
  - `trading/outcome_worker.py`
  - `trading/bybit_live_outcome.py`
  - `trading/state.py`
  - `trading/broker.py`
  - `trading/risk_profile.py`
- **Конфиги / ENV / runtime параметры:**
  - `OUTCOME_WATCH_MINUTES`, `OUTCOME_POLL_SECONDS`
  - `FAST0_OUTCOME_WATCH_SEC`, `FAST0_OUTCOME_POLL_SEC`
  - `POSITION_TTL_SECONDS`, `TIMEOUT_EXIT_MODE`
  - `EXECUTION_MODE`, `AUTO_TRADING_ENABLE`, `PAPER_MODE`
- **Датасеты / state:**
  - `datasets/trading_state.json`
  - `datasets/*/outcomes_v3.csv`
  - `datasets/*/trades.csv`
  - `datasets/*/events_v3.csv`
- **Логи / отчеты:**
  - примеры `LYNUSDT`, `SIRENUSDT`, `ENJUSDT`
  - `docs/DIAG_EARLY_EXIT_AND_CONFLICTS.md`
  - outcome logs с `TIMEOUT`, `OUTCOME_DONE`, `OUTCOME_FINAL`, `OUTCOME_PENDING`

---

## 4. Spec
1. **Инвентаризация entry paths**
   - Нужно перечислить все entry paths для:
     - `short_pump` paper
     - `short_pump` live
     - `short_pump_fast0` paper
     - `short_pump_fast0` live
   - Для каждого path зафиксировать:
     - кто создает позицию,
     - кто стартует tracking,
     - кто является source of truth для outcome.

2. **Проверка outcome tracker coverage**
   - Нужно определить для каждого entry path:
     - подключается ли позиция к активному outcome tracker,
     - есть ли polling/monitoring loop,
     - видит ли tracker open position после ENTRY.
   - Отдельно зафиксировать, где считаются:
     - TP/SL hits,
     - MFE/MAE,
     - hold_seconds,
     - timeout fallback outcome.

3. **Lifecycle gap analysis**
   - Нужно найти точную точку разрыва:
     - position opened,
     - но tracker/monitoring не стартует,
     - или tracker стартует, но не получает/не видит позицию,
     - или tracker работает, но слишком поздно начинает видеть candles/position.
   - Для каждого gap описать, это:
     - missing registration,
     - delayed start,
     - wrong mode gate,
     - state visibility issue,
     - or recovery/fallback artifact.

4. **Artifact vs real monitoring failure**
   - Нужно явно разделить:
     - timeout fallback formatting artifact,
     - реальное отсутствие TP/SL tracking.
   - Запрещено делать вывод “это просто формат сообщения” без доказательства, что monitoring действительно работал.

5. **Minimal safe fix plan**
   - Сначала восстановить tracking coverage для тех entry paths, где позиция не попадает в активный tracker.
   - До завершения RCA не менять timeout formatting, `exit=entry` rule и `mae/mfe=0` fallback semantics.
   - Если выяснится, что проблема только в formatting, отдельно описать это как non-blocking cleanup, но не как primary fix.

6. **Paper/live parity**
   - Для live path нужно отдельно определить, является ли timeout-only behavior:
     - нормальным следствием live-resolver design,
     - или следствием того же missing tracking coverage.
   - Если live и paper расходятся, зафиксировать, где именно расхождение допустимо, а где это bug.

---

## 5. Acceptance Criteria
- [ ] Для каждой entry path известно:
  - кто трекает позицию после открытия,
  - где считаются TP/SL,
  - где считаются MFE/MAE,
  - где срабатывает timeout fallback.
- [ ] Найден точный gap для путей, которые уходят в timeout-only behavior.
- [ ] Явно различены:
  - timeout fallback formatting artifact,
  - real missing TP/SL monitoring.
- [ ] Подготовлен fix plan без регрессии для live.
- [ ] После controlled case с рыночным движением и рабочим tracker path outcome больше не выглядит как `timeout with zero mae/mfe`, если tracking действительно должен был работать.

---

## 6. Validation Plan
- **Тесты / smoke / replay / ручные проверки:**
  - Снять mapping по entry paths и tracking start points для `short_pump` и `short_pump_fast0`.
  - Прогнать controlled paper case, где цена реально движется до TP/SL, и проверить:
    - старт tracker,
    - наличие MFE/MAE,
    - отсутствие false timeout-only fallback.
  - Отдельно проверить live path на том же delivery-key / position-id, если доступен безопасный replay или staging.
- **Какие артефакты должны остаться после проверки:**
  - таблица entry path -> tracker owner -> TP/SL source -> MFE/MAE source -> timeout source,
  - runtime log excerpts,
  - one-page RCA summary с выводом по gap vs artifact,
  - список exact call-sites, где tracking coverage отсутствует.

---

## 7. Risks
- **Data risk**
  - Можно перепутать formatting artifact с реальным gap и сделать неправильный вывод о причине TIMEOUT.
- **Trading risk**
  - Неправильное восстановление tracker coverage может вызвать дубль outcomes или конфликт с уже существующим timeout close path.
- **Operational risk**
  - Tracking может зависеть от timing/state visibility; без аккуратного анализа можно получить ложные negatives.
- **Rollback notes**
  - Если подъем tracking coverage изменит поведение, rollback должен вернуть только добавленный monitoring hook, не трогая timeout formatting и entry logic.

---

## 8. Agent Handoff
### PM Agent
- Подтвердить, что задача является RCA/coverage-gap задачей, а не formatting-only cleanup.
- Утвердить приоритет восстановления tracking coverage над изменением timeout formatting.

### Strategy Developer Agent
- Провести inventory entry paths и определить точку, где должен стартовать outcome tracker.
- Подготовить minimal safe fix plan без изменения timeout formatting до подтверждения gap.

### QA / Validation Agent
- Подтвердить, что controlled case с движением цены дает non-zero tracking evidence, если tracker path корректен.
- Зафиксировать, где именно current flow теряет tracking coverage.

### Production Safety Agent
- Проверить, что любые изменения будут ограничены coverage/tracking hooks и не затронут live risk policy.
- Согласовать rollback для monitoring-only изменений.

---

## 9. Execution Log
| Date       | Who (Agent / Person)       | Action / Comment                                   |
|-----------|-----------------------------|----------------------------------------------------|
| 2026-03-19| PM Agent                    | Created B48 spec for outcome tracking coverage gap investigation after timeout-dominant RCA. |

---

## 10. Final Decision
- **Status**: In Spec
- **Reason**:
  - RCA suggests a real lifecycle/tracking coverage gap is more likely than a pure timeout formatting artifact, but this must be proven path-by-path before changing behavior.
- **Links to evidence**:
  - Symptom examples: `LYNUSDT`, `SIRENUSDT`, `ENJUSDT`.
  - Previous RCA notes on timeout fallback behavior and zero MAE/MFE outcomes.

