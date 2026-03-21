# Task ID
B46_TG_ENV_PARITY_FOR_B42_TIMEOUT_TTL

## Title
Establish Telegram env parity for B42 paper TIMEOUT validation

## Epic
Outcome Tracking

## Priority
Critical

## Status
In Spec

---

## 1. Business / Strategy Context
- **Зачем задача нужна?**
  - Валидация `TASK_B42_OUTCOME_TG_TIMEOUT_TTL` показала `OUTCOME_TG_SEND_START`, но далее `OUTCOME_TG_SEND_NOOP_TG_DISABLED_NO_MARK`, что блокирует получение полного runtime evidence по контракту доставки outcome.
- **Какую проблему она решает?**
  - Устраняет несоответствие окружения validation-процесса и runtime/service окружения (env parity), из-за которого `tg_send_enabled=False` в B42 path, несмотря на то, что TG-отчеты в системе приходят.
  - Дает воспроизводимый и безопасный способ запустить controlled B42 validation в том же env-chain, что и сервисы.
- **На какие метрики влияет?**
  - Полнота validation evidence по B42 (наличие `OUTCOME_TG_DELIVERED_AND_MARKED`, обновление `outcome_tg_sent`, подтверждение anti-duplicate).
  - Снижение ложных REWORK по задачам outcome delivery из-за “не того окружения”.
  - Надежность release decision по runtime задачам outcome tracking.

---

## 2. Scope
### In scope
1. Зафиксировать точный env-chain и source of truth для env переменных Telegram по релевантным процессам:
   - `pump-trading-runner.service`
   - `pump-short.service`
   - `pump-short-live-auto.service`
2. Подтвердить, какой именно EnvironmentFile/Environment используется каждым из сервисов и чем отличаются цепочки env для:
   - outcome delivery path (B42: `paper_outcome -> outcome_delivery`)
   - report sending path (`scripts/daily_tg_report.py`, `telegram/report_bot.py`)
3. Провести сравнение флагов/переменных:
   - `TG_SEND_OUTCOME`
   - `TELEGRAM_BOT_TOKEN` / `TELEGRAM_CHAT_ID`
   - `TG_BOT_TOKEN` / `TG_CHAT_ID`
   - report-related TG env (`TG_REPORT_DATA_ROOT`, `FACTOR_REPORT_DATA_ROOT` и связанные)
4. Подготовить один рекомендуемый safe launch method для B42 validation, который:
   - гарантированно подхватывает те же env, что runtime service
   - выполняет controlled TIMEOUT validation (paper)
   - перед стартом логирует effective env summary:
     - `TG_SEND_OUTCOME` present/value
     - `TELEGRAM_BOT_TOKEN` present yes/no
     - `TELEGRAM_CHAT_ID` present yes/no
   - не требует изменения runtime-кода

### Out of scope
1. Изменение логики `trading/outcome_delivery.py`, `trading/paper_outcome.py`, `short_pump/telegram.py`.
2. Изменение production systemd unit-файлов в рамках этой задачи (только диагностика и спецификация безопасного метода запуска).
3. Изменение report-логики или переименование env-переменных в коде.

---

## 3. Inputs
- **Кодовые модули:**
  - `trading/paper_outcome.py` (TTL path и прокидывание `tg_send_enabled`)
  - `trading/outcome_delivery.py` (ветка `tg_disabled`, `token_missing`, mark-after-success)
  - `short_pump/telegram.py` (import-time инициализация `TG_SEND_OUTCOME`, `TG_BOT_TOKEN/CHAT_ID`)
  - `scripts/daily_tg_report.py` (цепочка creds для report sending)
  - `telegram/report_bot.py` (бот-цепочка report TG)
- **Конфиги / ENV / .service:**
  - `deploy/systemd/pump-trading-runner.service` (известно `EnvironmentFile=/root/pump_short/.env`)
  - Runtime unit references:
    - `pump-short.service`
    - `pump-short-live-auto.service`
  - `.env` (runtime env source; фактическое содержимое в прод-среде)
  - возможные override-файлы systemd (`systemctl cat <unit>` в validation)
- **Логи / отчеты:**
  - Controlled validation stdout с маркерами `OUTCOME_TG_*`
  - journal/service logs по runner/api сервисам
  - факт успешной отправки report TG сообщений

---

## 4. Spec
1. **Определить env parity baseline**
   - Для каждого целевого unit зафиксировать:
     - откуда читается env (`EnvironmentFile`, `Environment`, drop-ins)
     - какая итоговая effective value для ключевых TG env.
2. **Сделать env matrix сравнения**
   - Подготовить таблицу “service/process -> env value” для:
     - `TG_SEND_OUTCOME`
     - `TELEGRAM_BOT_TOKEN` (present yes/no)
     - `TELEGRAM_CHAT_ID` (present yes/no)
     - `TG_BOT_TOKEN` / `TG_CHAT_ID` (present yes/no)
   - Отдельно добавить столбец “используется в report path” vs “используется в outcome path”.
3. **Зафиксировать отличие report chain vs outcome chain**
   - Report path может отправлять без `TG_SEND_OUTCOME`.
   - B42 outcome path в `paper_outcome -> outcome_delivery` зависит от `tg_send_enabled=bool(TG_SEND_OUTCOME)`.
   - Обязательный вывод: почему “reports приходят”, но controlled B42 validation видел `tg_disabled`.
4. **Рекомендуемый safe launch method (без изменения runtime code)**
   - Validation запуск должен идти в env parity с service env (тот же EnvironmentFile/override chain).
   - Метод запуска обязан логировать effective env summary до выполнения controlled TIMEOUT сценария.
   - Запуск должен быть paper-only и без опасных прод-операций (без live ордеров).
5. **Артефакты**
   - Сохранить:
     - env matrix (markdown/csv)
     - stdout controlled validation c `OUTCOME_TG_*`
     - факт/снимок `outcome_tg_sent` по delivery key
     - краткий RCA summary “env parity mismatch confirmed/denied”.

---

## 5. Acceptance Criteria
- [ ] Точно определено, из какого `EnvironmentFile`/override chain читают env:
  - `pump-trading-runner.service`
  - `pump-short.service`
  - `pump-short-live-auto.service`
- [ ] Подготовлена и заполнена env matrix по ключам:
  - `TG_SEND_OUTCOME`
  - `TELEGRAM_BOT_TOKEN` / `TELEGRAM_CHAT_ID`
  - `TG_BOT_TOKEN` / `TG_CHAT_ID`
  - report-related TG env
- [ ] Явно объяснено, почему report path работает, а B42 validation получил `tg_disabled`.
- [ ] Зафиксирован один рекомендуемый safe launch method для B42 validation с env parity.
- [ ] Launch method позволяет получить full B42 evidence (START -> DELIVERED_AND_MARKED -> outcome_tg_sent update -> second run SKIP_ALREADY_FINALIZED) без изменения runtime кода.

---

## 6. Validation Plan
- **Тесты / smoke / ручные проверки:**
  - Снять effective env по целевым unit (через безопасные read-only systemd/env команды).
  - Выполнить controlled B42 validation в env parity режиме.
  - Проверить наличие:
    - `OUTCOME_TG_SEND_START`
    - `OUTCOME_TG_DELIVERED_AND_MARKED`
    - `outcome_tg_sent` update
    - второй прогон: `OUTCOME_TG_SEND_SKIP_ALREADY_FINALIZED`
- **Какие артефакты должны остаться после проверки:**
  - env matrix (md/csv)
  - stdout/journal excerpts с `OUTCOME_TG_*`
  - state snapshots/grep по `outcome_tg_sent`
  - краткое заключение parity pass/fail.

---

## 7. Risks
- **Operational risk**
  - Ошибка интерпретации effective env при наличии systemd drop-ins/override.
- **Validation risk**
  - Запуск в shell без env parity снова даст ложный `tg_disabled` и повторный REWORK.
- **Security risk**
  - Риск утечки чувствительных значений env в артефактах (нужно логировать только present yes/no, без секретов).
- **Rollback notes**
  - Задача process-only; rollback = отмена validation-runbook изменений/документации.

---

## 8. Agent Handoff
### PM Agent
- Утвердить parity-first подход как обязательный prerequisite перед закрытием B42.
- Зафиксировать expected evidence checklist в задаче B42.

### Strategy Developer Agent
- Подготовить безопасный runbook запуска controlled B42 validation в service-parity env.
- Описать env summary format (без раскрытия секретов).

### QA / Validation Agent
- Выполнить parity validation и собрать env matrix + runtime evidence.
- Дать финальный verdict по B42 evidence completeness.

### Production Safety Agent
- Подтвердить, что launch method не затрагивает live execution path и не требует runtime code changes.
- Подтвердить корректность и безопасность способа чтения effective env.

---

## 9. Execution Log
| Date       | Who (Agent / Person)       | Action / Comment                                   |
|-----------|-----------------------------|----------------------------------------------------|
| YYYY-MM-DD|                             |                                                    |

---

## 10. Final Decision
- **Status**: In Spec
- **Reason**:
  - B42 REWORK обусловлен отсутствием env parity в validation, а не доказанной ошибкой в delivery-логике.
- **Links to evidence**:
  - B42 controlled validation output (`tg_disabled`)
  - RCA summary по env-chain расхождению report vs outcome paths

