## Workflow: From Idea to Stable Production

> **Базовое правило:** любая новая задача по торговой системе оформляется через отдельный файл на основе `project_ops/TASK_TEMPLATE.md`.  
> Без заполненного (хотя бы в минимальном объеме) шаблона задача **не может** переходить к реализации.

### 1. Idea

- Инициатор (часто Strategy Developer Agent или Project Manager Agent) формулирует идею:
  - новая гипотеза по фильтру входа,
  - изменение risk guard,
  - улучшение outcome tracking / dataset / мониторинга,
  - ML-эксперимент.
- Идея фиксируется кратко (1–3 абзаца) и привязывается к существующему или новому Epic в `BACKLOG.md`.
- Для идей, которые попадают в работу, **PM создает отдельную карточку задачи на основе `TASK_TEMPLATE.md`** (отдельный файл или раздел), заполняя минимум:
  - Task ID / Title / Epic / Priority / Status,
  - раздел 1 (Business / Strategy Context),
  - раздел 2 (Scope),
  - черновик разделов 3–5 (Inputs, Spec, Acceptance Criteria).
- Project Manager Agent оценивает приоритет и решает, попадает ли идея в текущий план (см. `ROADMAP.md`).

**Передача:**  
Инициатор → Project Manager Agent (описание идеи и мотивации).

---

### 2. Spec

- Project Manager Agent совместно со Strategy Developer Agent подготавливают спецификацию **внутри соответствующего `TASK_TEMPLATE`**:
  - цель изменения и его scope,
  - конкретные изменения в логике/данных/конфиге,
  - **acceptance criteria** (обязательный элемент),
  - риски и предположения.
- Если изменение затрагивает риск-политику, Production Safety Agent обязан участвовать в обсуждении.
- Для существенных архитектурных или риск-решений создается запись в `DECISIONS.md` (ADR-lite).

- **Обязательное правило:**  
  - Задача не может перейти в статус "In Implementation", пока:
    - в `TASK_TEMPLATE` не заполнены разделы 1–5 (Context, Scope, Inputs, Spec, Acceptance Criteria),
    - не определены базовые элементы раздела 6 (Validation Plan) и раздела 7 (Risks).

**Передача:**  
Project Manager Agent → Strategy Developer Agent и QA / Validation Agent (утвержденный Spec + acceptance criteria).

---

### 3. Implementation

- Strategy Developer Agent реализует изменения в коде:
  - торговая логика (`short_pump/`, `long_pullback/`, `trading/`),
  - аналитика и отчеты (`analytics/`),
  - dataset / feature engineering,
  - конфиги (при согласовании с Production Safety Agent).
- Параллельно QA / Validation Agent:
  - подготавливает/обновляет тесты и validation-сценарии,
  - согласует с разработчиком, какие сценарии должны быть пройдены для данной задачи.
  - уточняет и дополняет раздел 6 `Validation Plan` в `TASK_TEMPLATE`.

- Все действия и ключевые шаги фиксируются в разделе 9 `Execution Log` соответствующего `TASK_TEMPLATE`.

**Правила передачи:**
- Strategy Developer Agent обязан:
  - явно указать, какие acceptance criteria реализованы и как они проверяются,
  - передать QA / Validation Agent список команд/скриптов для запуска валидации.

---

### 4. Validation

- QA / Validation Agent запускает и/или настраивает:
  - unit-тесты и интеграционные проверки,
  - smoke на key-path (dataset, risk guard, outcome tracking),
  - реплей или выборочную проверку на исторических данных (если применимо).
- **Validation evidence** (логи прогонов, отчеты, графики, метрики) собирается и сохраняется в понятном месте (например, отдельная директория с отчетами, ссылки в задаче).
  - Ссылки на артефакты и краткое summary заносятся в разделы 6 и 9 `TASK_TEMPLATE`.

**Жесткое правило:**  
**Никакие изменения не считаются завершенными без явных acceptance criteria и зафиксированного validation evidence.**

**Передача:**  
QA / Validation Agent → Strategy Developer Agent (обратная связь по найденным проблемам)  
QA / Validation Agent → Project Manager Agent (отчет о пройденной валидации или блокирующих дефектах).

---

### 5. Review

- Проходит минимум один технический код-ревью:
  - соответствие архитектурным принципам (`ARCHITECTURE_BRIEF.md`, `PROJECT_STRUCTURE.md`),
  - отсутствие очевидных рисков и регрессий,
  - читаемость и простота дальнейшего сопровождения.
- Если изменение затрагивает риск-политику или ML в проде:
  - Production Safety Agent проверяет, что ограничения и режимы соблюдены,
  - при необходимости уточняется запись в `DECISIONS.md`.

**Передача:**  
Strategy Developer Agent → Reviewer (может быть другой Strategy Developer / Production Safety для risk-related частей).  
После успешного ревью: Reviewer → Project Manager Agent с рекомендацией по релизу.

---

### 6. Release decision

- Project Manager Agent собирает входы:
  - статус реализации и тестов от Strategy Developer Agent и QA / Validation Agent,
  - оценку рисков и готовность прод-среды от Production Safety Agent.
- Принимается одно из решений:
  - **Release** — релиз одобрен в текущем виде,
  - **Release with guardrails** — релиз с дополнительными ограничениями (ограниченный объем, только paper, частичный rollout),
  - **Delay** — требуется доработка/дополнительная валидация.
- Решение и ключевые аргументы фиксируются (кратко) в разделе 10 `Final Decision` `TASK_TEMPLATE`, при необходимости — в `DECISIONS.md`.

**Передача:**  
Project Manager Agent → Production Safety Agent (go/no-go сигнал с уровнем guardrails).

---

### 7. Post-release monitoring

- Production Safety Agent следит за:
  - runtime-метриками (PnL, MDD, частота сделок, распределение PnL),
  - логами и алертами (исключения, падения сервисов, аномалии в outcome tracking),
  - консистентностью datasets (отсутствие дыр и дубликатов).
- QA / Validation Agent может запускать пост-релизные проверки (например, периодические smoke на свежем датасете).
- Значимые находки (регрессии, edge-cases, новые риски) превращаются в:
  - новые задачи в `BACKLOG.md`,
  - обновления в `DECISIONS.md` и/или архитектурных доках,
  - корректировки метрик и порогов в `DEFINITION_OF_DONE.md`.

**Передача:**  
Production Safety Agent и QA / Validation Agent → Project Manager Agent и Strategy Developer Agent (обратная связь и новые гипотезы).

---

## Rules for Task Handoffs Between Agents

- **От Project Manager Agent к Strategy Developer Agent**
  - Задача должна содержать: цель, epic, scope, черновой список acceptance criteria **внутри соответствующего `TASK_TEMPLATE`**.
  - Без первичного определения, зачем изменение нужно, реализация не стартует.

- **От Strategy Developer Agent к QA / Validation Agent**
  - Обязан предоставить:
    - что конкретно изменено (раздел 4 `Spec`),
    - какие сценарии считаются критичными (раздел 6 `Validation Plan`),
    - как проверить выполнение acceptance criteria (команды/скрипты, какие логи/отчеты смотреть, ссылки на артефакты в разделах 6 и 9).

- **От QA / Validation Agent к Project Manager Agent**
  - Передается четкий статус: **pass / fail / needs more data**.
  - В случае fail перечисляются найденные дефекты с приоритетами.
  - Статус и выводы отражаются в разделах 6, 9 и 10 `TASK_TEMPLATE`.

- **От Project Manager Agent к Production Safety Agent**
  - Перед релизом передается:
    - краткое резюме изменений,
    - риски и предположения,
    - какие guardrails предлагаются (ограничения на режим, объем, активацию ML и т.п.).
  - Production Safety использует соответствующий `TASK_TEMPLATE` как основной источник правды по задаче.

- **От Production Safety Agent к остальным**
  - В случае обнаружения инцидентов или повышенного риска:
    - обязан уведомить всех агентов и инициировать разбор.
    - при необходимости — инициировать rollback или включение дополнительных ограничений.
  - Результаты инцидента и принятые решения могут быть отражены как в `TASK_TEMPLATE`, так и в `DECISIONS.md`.

---

## Global Rule: Acceptance & Validation

- Любая задача считается **не завершенной**, если:
  - нет явно сформулированных и задокументированных **acceptance criteria**,  
  - нет **validation evidence** (логи прогонов, отчеты, графики, выдержки из smoke/реплея), подтверждающих выполнение критериев.
- Project Manager Agent не может перевести задачу в состояние "Done" без подтверждения от QA / Validation Agent и, при необходимости, Production Safety Agent.
- Любые спорные случаи должны быть зафиксированы как отдельное решение в `DECISIONS.md` (с описанием trade-off).

**Дополнительные правила:**

- **Обязательное правило: TASK_TEMPLATE**  
  - Ни одна задача не идет в реализацию без созданного и заполненного `TASK_TEMPLATE` (минимум разделы 1–5 и черновой validation plan в разделе 6).
  - Любой hotfix/urgent change также требует хотя бы сокращенной версии `TASK_TEMPLATE` (краткий контекст, scope, риск, план отката и фактические действия в Execution Log). Обход процесса запрещен.
- **Последовательность агентов:**  
  - Для любой задачи полный цикл должен проходить в порядке:  
    **PM Spec → Developer Implementation → QA Validation → Production Safety Review → Final Decision.**
  - Пропуск любого шага допускается только при явном документированном решении в `DECISIONS.md` с указанием причин и временных ограничений.
- **Validation evidence как условие завершения:**  
  - Если в разделе 6 `Validation Plan` и разделе 9 `Execution Log` нет ссылок/описаний фактических validation evidence, задача автоматически считается незавершенной вне зависимости от состояния кода.

---

## Minimal lifecycle of one task

1. **Idea & Backlog** — инициатор формулирует идею, PM добавляет запись в `BACKLOG.md` и создает заготовку `TASK_TEMPLATE` (заполняет контекст, scope, epic, приоритет).  
2. **PM Spec** — PM вместе с Strategy Dev уточняют Spec и acceptance criteria в `TASK_TEMPLATE`, задача получает статус "In Spec".  
3. **Developer Implementation** — Strategy Dev реализует изменения, обновляет разделы Spec, Inputs, Execution Log; задача переходит в "In Implementation".  
4. **QA Validation** — QA готовит и выполняет Validation Plan, сохраняет evidence и обновляет разделы 6 и 9; статус "In Validation".  
5. **Production Safety Review** — Production Safety оценивает риски, guardrails, конфиги и rollback, обновляет разделы 7, 8 и 9; статус "In Review / Ready for Release".  
6. **Final Decision** — PM (при участии всех агентов) выставляет итоговый статус в разделе 10 (`Approved / Rework / Blocked`), при необходимости создает запись в `DECISIONS.md`.  
7. **Release & Monitoring** — изменения выкатываются согласно решению, Post-release monitoring отслеживает эффекты; при инцидентах задача может быть возвращена в состояние Rework.  


