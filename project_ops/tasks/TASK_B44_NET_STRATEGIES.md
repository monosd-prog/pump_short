# Task ID
B44_NET_STRATEGIES

## Title
Add NET-metric trading strategies (paper-first)

## Epic
Signal Quality

## Priority
High

## Status
In Spec

---

## 1. Business / Strategy Context
- **Зачем задача нужна?**
  - Расширить набор “сигналов” за счет стратегий, основанных на NET-метриках (по материалам PRO-сессий), чтобы улучшить coverage и качество entry/outcome подрежимов.
- **Какую проблему она решает?**
  - Позволяет вводить новые хорошо-описанные сетапы (NET-логика) вместо ad-hoc порогов.
  - Дает управляемый rollout: сначала paper-only (без live ордеров) и затем (позже) live по guardrails.
- **На какие метрики влияет?**
  - Entry coverage по новым NET подрежимам.
  - Coverage outcomes в `outcomes_v3.csv` (TP_hit/SL_hit/TIMEOUT).
  - Стабильность delivery contract `outcome_tg_sent` (B41 invariants) на новых стратегиях.
  - Стабильность dataset связок `event_id ↔ trade_id ↔ outcome`.

---

## 2. Scope
### In scope
1. Источник правил стратегий (единственный источник)
  - Использовать записи встреч PRO (см. `Inputs`) как canonical описание NET-логики.
2. Сбор и фиксация NET-логики
  - После того как пользователь даст текстовую расшифровку роликов, извлечь:
    - определения NET-метрик (какие именно “NET”: net-liq, net-cvd, net-oi и т.п.)
    - окна/таймфреймы (30s/1m/…)
    - направления (long vs short), знаки и сравнения
    - пороги и условия “entry/guard”
3. Mapping NET-метрик → доступные фичи проекта
  - Определить, как получить NET-метрики из уже доступных runtime фич (например, из long/short компонент liquidation, CVD, OI и funding).
  - Если какие-то компоненты отсутствуют, описать минимальный pipeline-дизайн для их получения (но без реализации на этом этапе).
4. Добавление новых strategy/submode имен
  - Зафиксировать каноническое именование (пример: `short_pump_net_setup1...N` либо `short_pump_net` + submode).
  - Обеспечить, что новые strategy names проходят через:
    - enqueue entry сигналов
    - запись `events_v3.csv` / `trades_v3.csv` / `outcomes_v3.csv`
    - paper-first gating (live enable отключен до go/no-go).
5. Интеграция в существующий entry decision path
  - Добавить NET-условия как отдельные ветки guard/entry contract (не ломая текущие entry контракты).
6. Guardrails и отсутствие регрессии outcome→TG contract
  - Гарантировать, что новые стратегии используют существующую outcome→TG delivery архитектуру (B41/B42 контракты, delivery-key = `position_id`).

### Out of scope
1. Изменение outcome tracking/Telegram outcome delivery контракта (B41 invariants) — только использовать уже существующий контракт.
2. Реализация live enable до отдельного Final Decision по production безопасности.
3. ML training/обучение моделей в рамках первого внедрения (только dataset evidence для последующих экспериментов).

---

## 3. Inputs
- **PRO записи (canonical источник правил NET-метрик):**
  - Запись встреч PRO №109 (00:19:27 - 01:57:50)
  - Запись PRO №110 (00:03:12 - 01:23:17)
  - Запись PRO №114 (00:37:33 - 02:09:23)
  - Запись PRO №115 (00:37:16 - 00:48:50)
  - Запись PRO №121 (01:23:55 - 01:36:59)
  - Запись PRO №127 (полностью)
- **Текстовая расшифровка (будет предоставлена пользователем позже)**
  - После получения расшифровок: использовать их как источник порогов/условий/порядка правил.
  - Формат: “понятные буллеты/секреты порогов/формулы”, если это удобно пользователю.
- **Кодовые модули (primary для интеграции, без изменения outcome delivery контракта):**
  - `short_pump/watcher.py` (entry decision / сигнализация / guard-blocked paths)
  - `trading/runner.py` (consume signals → paper/live positions; paper-first gating)
  - `trading/state.py` (delivery-key = position_id для B41/B42)
  - `trading/risk_profile.py` (risk profile / sizing и (возможно) submode маппинг)
  - `common/io_dataset.py` (запись `events_v3/trades_v3/outcomes_v3`)
  - `notifications/tg_format.py` (формат TG сообщений — для проверки consistency, без изменения логики delivery)

---

## 4. Spec
1. **Извлечение NET-логики из расшифровок**
  - Сформировать документ “NET strategy definitions v1” с:
    - списком NET-метрик
    - формулами/как считается NET (если это разница long/short — явно указать)
    - окнами и порогами
    - условиями entry и guard (что разрешает, что запрещает)
2. **Проверка доступности компонентов NET в runtime payload**
  - Сверить, какие базовые фичи уже есть в текущем pipeline:
    - liquidation long/short статистики (`liq_long_usd_30s` и т.п.)
    - CVD (`cvd_30s`, `cvd_1m`)
    - OI (в текущем коде уже используется как часть контекста)
  - Зафиксировать, какие NET-метрики можно вычислить напрямую, а какие потребуют добавления базовой фичи (на уровне Spec — без кода).
3. **Дизайн интеграции entry gate**
  - Выбрать, где именно применять NET-условия:
    - ветка decision на существующих уровнях (например 5m/1m/fast)
    - либо как отдельный этап внутри `decide_entry_1m` (если потребуется)
  - Добавить NET как “family” стратегий, где подрежим определяется параметрами NET-условий.
4. **Naming и mapping в datasets**
  - Зафиксировать правила:
    - `strategy` и `submode` (или эквивалент) как будут отражены в `events_v3/trades_v3/outcomes_v3`.
5. **Paper-first rollout**
  - Обязательная: новые NET стратегии должны по умолчанию идти в paper path.
  - Live enable — только после отдельного production safety sign-off.
6. **No regression**
  - Для новых стратегий не менять контракт B41 по TG outcome delivery:
    - `delivery-key = position_id` (единый ключ)
    - mark после success и bounded retry (как уже реализовано в B41/B42)

---

## 5. Acceptance Criteria
- [ ] После внедрения (в будущем AGENT stage) для каждой NET стратегии/submode:
  - entry conditions порождают соответствующие сигналы (минимум один воспроизводимый сценарий на контрольном окне).
- [ ] Для каждого воспроизводимого сценария появляется минимум одна core outcome строка в `outcomes_v3.csv` (TP_hit/SL_hit/TIMEOUT) с корректными связками `strategy/mode/submode`.
- [ ] Контракт delivery outcome→TG не регрессирует:
  - для новых стратегий не возникает “mixing” ключей (delivery-key должен быть `position_id`).
- [ ] Dataset consistency:
  - отсутствуют критичные NaN/дубликаты, и нет “дырок” entry → outcome.
- [ ] Paper-first:
  - новые NET стратегии не создают live ордера до Final Decision и production safety go/no-go.

---

## 6. Validation Plan
- **Тесты / smoke / ручные проверки:**
  - Runtime paper capture для NET стратегий:
    - включить только NET стратегии (или ограничить символы/окна)
    - подтвердить: entry → outcome row в `outcomes_v3.csv`
  - Контракт outcome→TG (B41 invariants):
    - проверить по логам наличие `OUTCOME_TG_SEND_START` и отсутствие duplicate delivered маркеров по одному delivery-key.
  - Dataset checks:
    - сверка `events_v3.csv`, `trades_v3.csv`, `outcomes_v3.csv` по ключам (event_id/trade_id/outcome_id при наличии схемы).
- **Какие артефакты должны остаться после проверки:**
  - journal/stdout summary с OUTCOME_TG_* маркерами,
  - snapshots `datasets/trading_state.json`,
  - CSV summary counts по NET strategy/submode: `entry_count`, `core_outcomes_count`, `TIMEOUT_count`.

---

## 7. Risks
- **Data risk**
  - NET-метрики могут быть вычислены неверно из long/short компонентов (например sign/scale/window mismatch) → низкое покрытие или неверные outcomes.
- **Trading risk**
  - Неправильный mapping NET → пороги в entry gate может резко увеличить частоту сделок.
  - Risk profile может быть выбран некорректно → неправильный sizing (EV/WR и MDD деградируют).
- **Operational risk**
  - Увеличение вычислений/частоты evaluation (если NET требует новых фич) → рост latency.
- **Rollback notes**
  - Отключить NET family strategies через feature flag/env (на этапе AGENT внедрения предусмотреть переключатель).

---

## 8. Agent Handoff
### PM Agent
- Утвердить список NET-стратегий/подрежимов после расшифровки PRO-встреч.
- Назначить окно проверки (символы/время) для paper-first capture.

### Strategy Developer Agent
- Подготовить дизайн mapping NET-метрик → доступные фичи и определить integration points в entry decision.
- Спроектировать новые `strategy`/submode и их mapping в datasets и risk_profile.

### QA / Validation Agent
- Проверить entry → outcome наличие и отсутствие dataset “дыр”.
- Проверить отсутствие регрессии B41 outcome→TG delivery contract для NET стратегий.

### Production Safety Agent
- Проверить, что live enable для NET стратегий не включен до approval.
- Определить rollback для production.

---

## 9. Execution Log
| Date       | Who (Agent / Person)       | Action / Comment                                   |
|-----------|-----------------------------|----------------------------------------------------|
| YYYY-MM-DD|                             |                                                    |

---

## 10. Final Decision
- **Status**: In Spec
- **Reason**:
  - Pending user-provided textual transcript of PRO videos (NET metric definitions and exact rules).
- **Links to evidence**:
  - PRO recordings list (в `Inputs`)

