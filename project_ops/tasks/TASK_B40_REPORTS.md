# Task ID
B40_REPORTS

## Title
Reconcile: compact/tg reports vs raw outcomes (EV/WR/N/TP/SL/TIMEOUT)

## Epic
Monitoring / Alerts

## Priority
Critical

## Status
In Spec

---
## 1. Business / Strategy Context
- **Зачем задача нужна?**
  - Компактные отчеты, которые уходят в Telegram, используются как оперативная “истина” для оценки edge, guard-state и готовности режимов. Любой рассинхрон между отчетом и raw outcomes приводит к ошибочным управленческим решениям и неверной разметке data-quality.
- **Какую проблему она решает?**
  - “Некорректный пересчет отчетов”: метрики (TP/SL/TIMEOUT, N_core, WR, EV, EV20, маппинг активных/disabled подрежимов) в compact-отчете могут не совпадать с тем, что вычисляется напрямую из raw `outcomes_v3` и соответствующих `events_v2`/`trades_v3`.
- **На какие метрики влияет?**
  - WR/EV/EV20, sample sizes (N_core), качество фильтров (ACTIVE stage/dist; FAST0 operational buckets), стабильность прогноза readiness/guard-state.

---
## 2. Scope
### In scope
1. Определить и формализовать вычислительный контур compact-отчета:
  - где в коде формируются blocks (SYSTEM STATE, Strategy blocks, LIVE/PAPER blocks),
  - какие датасеты/колонки используются,
  - какие фильтры/маски применяются.
2. Реализовать reconciliation/diagnostics, который сравнивает:
  - raw outcomes_v3 (и их нормализацию outcome label),
  - enriched core (outcomes с stage/dist/liq после join с events),
  - финальные выборки в compact-отчете (ACTIVE stage4+dist, FAST0 submodes).
3. Выявить и устранить источник рассинхрона (join key, enrich coverage, filter thresholds, сортировка для EV20/rolling, normalization mismatch).
4. Добавить устойчивые проверки, чтобы в будущем рассинхрон детектировался до отправки TG.

### Out of scope
- Не менять trading execution и outcome tracking.
- Не делать ML-инференс/обучение в рамках этой задачи.
- Не “улучшать” форматы отчетов без проверки метрик на совпадение с raw outcomes.

---
## 3. Inputs
- **Кодовые модули (primary)**
  - `scripts/daily_tg_report.py`
    - загрузка outcomes/events,
    - подготовка stage/dist enrichment,
    - сбор compact executive report (через `build_executive_compact_report`).
  - `analytics/executive_report.py`
    - `build_executive_compact_report()` (логика EV/WR/MDD/EV20/подрежимы).
    - `_dataset_quality_metrics()`, `_core_pnl_series()`, rolling metrics.
  - `scripts/daily_tg_report.py`
    - `_enrich_core_with_events()` (join key selection + fallback по row-count).
    - `_sort_outcomes_for_series()` (порядок для EV20/rolling при отсутствии outcome_time_utc).
  - `analytics/stats.py`
    - `wr_core_from_tp_sl`, `ev_core_from_tp_sl_pnl`, `_core_mask`.
  - `analytics/short_pump_blocks.py`
    - `filter_active_trades()` (stage==4 + dist_to_peak_pct >= tg_dist_min).
  - `analytics/fast0_blocks.py` / fast0 mask functions (operational buckets и submodes).
  - `analytics/load.py`
    - `load_outcomes()`, `load_events_v2()` (schema/version-aware загрузка, dedupe).
2. **Конфиги / ENV**
  - `TG_DIST_TO_PEAK_MIN` (порог ACTIVE для short_pump в отчете).
  - параметры guard_state (`auto_risk_guard_state.json`, и логика обновления через `update_auto_risk_guard.py`).
  - опции daily report генерации (`--days`, `--compact`, `--strategy`, `--rolling`).
3. **Датасеты / таблицы**
  - `datasets/**/outcomes_v3.csv` (LIVE block и DISABLED submodes из mode=paper):
    - `strategy=short_pump`, `strategy=short_pump_fast0`,
    - `mode=live` (для LIVE),
    - `mode=paper` (для PAPER подрежимов).
  - `datasets/**/events_v2.csv` (для stage/dist/entry enrichment под ACTIVE и FAST0 analysis):
    - грузится через `load_events_v2(... raw=True ...)`.
  - (опционально) `trades_v3.csv` если reconciliation будет использовать join на trade_id.
4. **Логи / отчеты**
  - Telegram compact report текст до/после (пример: “АВТОТОРГОВЛЯ — КОМПАКТ ОТЧЕТ”).
  - TG entry signals (источник тикета рассинхрона): run_id/eid/symbol из входящих сообщений.

---
## 4. Spec
- **Что именно нужно изменить?**
1. Формализовать “эталонные” расчеты для reconciliation:
  - нормализация outcome label (TP_hit/SL_hit/TIMEOUT/OTHER),
  - core-подмножество (TP_hit/SL_hit) и правила N_core,
  - EV = WR*avg_win + (1-WR)*avg_loss на core subset,
  - EV20/rolling EV на core subset по тому же порядку (как в `_sort_outcomes_for_series()` и rolling-логике).
2. Проверить стадию enrich-цепочки:
  - сколько core rows получили `stage`/`dist_to_peak_pct`/`liq_long_usd_30s` после `_enrich_core_with_events`,
  - какой join key фактически использовался (event_id vs trade_id vs (run_id,symbol)),
  - не происходит ли падение в fallback “rows_after != rows_before” и что это означает для точности фильтров.
3. Провести reconciliation по точкам расхождения:
  - raw outcomes -> enriched core -> ACTIVE stage/dist filter -> финальные блоки compact-отчета.
  - FAST0: core outcomes -> operational/selective masks -> финальные submodes.
4. Исправить (минимально необходимым способом) источник рассинхрона, если он выявлен:
  - корректный выбор join key,
  - устранение NaN/0 заполнений stage/dist/liq при наличии данных,
  - единообразная сортировка для EV20/rolling,
  - согласование normalization между raw и отчетом.
5. Добавить предотвращающие проверки:
  - если enrich coverage ниже заданного порога, отчет должен либо:
    - использовать safe fallback (вместо “тихих” NaN-провалов),
    - либо явно подсветить в отчете “incomplete enrichment” и/или блокировать публикацию.

- **Какие ограничения есть?**
  - Не изменять торговую логику и outcome tracking.
  - Изменения допускаются только в аналитических/отчетных модулях и диагнастике.

- **Что нельзя ломать?**
  - Структуру dataset loaders (`load_outcomes`, `load_events_v2`) без миграционного плана.
  - Согласованность schema outcomes_v3 и event/trade join ключей.

---
## 5. Acceptance Criteria
- [ ] Для фиксированного окна (например `--days`=2 для date_range из datasets) reconciliation показывает **точное совпадение** TP/SL/TIMEOUT counts по нормализованному outcome label для:
  - Strategy block `SHORT_PUMP` (core TP_hit/SL_hit и TIMEOUT по определению отчета),
  - Strategy block `FAST0` (включая fast0 buckets).
- [ ] Для каждого отображаемого submode в compact executive report `N_core`, `WR`, `EV`, `EV20` совпадают с эталонными вычислениями:
  - EV match: допускается погрешность не более `1e-6` в float или равенство после округления к формату отчета.
  - EV20 match: допускается погрешность не более `1e-6` (и/или равенство округленного значения).
- [ ] “Нет расхождения TP/SL/OTHER”:
  - доля outcome OTHER в исходных data определяется явно и не влияет на core WR/EV;
  - если отчет считает OTHER/нестандартные исходы отдельно (dataset quality), то reconciliation показывает ту же классификацию.
- [ ] Расхождение (если есть) локализуется reconciliation-логами до конкретного шага:
  - join/enrich coverage,
  - mask/filter thresholds (stage/dist/liq),
  - сортировка для rolling/EV20,
  - normalization.
- [ ] Добавлена guard-check логика/диагностика:
  - при падении enrich coverage ниже порога отчет либо использует fallback, либо явно помечает incomplete data (без тихого занижения N/EV).

---
## 6. Validation Plan
- **Тесты / smoke / replay / ручные проверки:**
  - Запустить генерацию compact executive report:
    - `python3 scripts/daily_tg_report.py --data-dir /root/pump_short/datasets --days <DAYS> --strategy short_pump --compact --rolling <ROLLING> --debug`
  - Запустить reconciliation validation (новый QA-script/CLI в рамках задачи, либо встраивание в существующий report runner):
    - сравнить вычисления “эталон” vs “как в отчете” по тем же маскам и core subset.
  - Проверить 1-2 временных окна: “до/после” периода, где замечен рассинхрон.
- **Какие артефакты должны остаться после проверки**
  - stdout/log reconciliation: сводка по расхождениям и шагу локализации,
  - CSV/JSON (если создается) с intermediate counts: raw_core, enriched_core, active_core, fast0 operational/selective counts.

---
## 7. Risks
- **Data risk**
  - Низкий/средний: изменение аналитических join/filter может поменять то, что считается “active”/“operational” в отчете.
  - Требуется строгая верификация формул EV/WR/EV20.
- **Operational risk**
  - Medium: если report-скрипт добавит сложные проверки, могут быть увеличения времени генерации. Нужны пороговые дебаг-режимы.
- **Rollback notes**
  - Вернуться к прежней logic сборки отчета (переключатель/feature flag) и откатить только report/analytics изменения.

---
## 8. Agent Handoff
### PM Agent
- **Ответственность на этой задаче:**
  - Назначить точное окно дат (date_range) и стратегии, где воспроизводится mismatch.
- **Сделано / нужно сделать PM:**
  - [ ] Выбрать один репликационный кейс (window + список run_id/symbol из TG если доступно)
  - [ ] Определить format tolerances (округление EV/EV20 как в отчете).

### Strategy Developer Agent
- **Ответственность на этой задаче:**
  - Реализовать reconciliation/diagnostics и устранить причину расхождений (только в analytics/report code).
- **Сделано / нужно сделать Strategy Dev:**
  - [ ] Обеспечить совпадение формул и mask-логики между эталоном и отчетом.
  - [ ] Добавить защиту от low enrich coverage.

### QA / Validation Agent
- **Ответственность на этой задаче:**
  - Прогнать reconciliation на выбранных окнах и подтвердить acceptance criteria.
- **Сделано / нужно сделать QA:**
  - [ ] Подготовить evidence сравнения (counts/EV/EV20/WR).
  - [ ] В случае расхождений — локализовать шаг (join/enrich/mask/sort/normalize).

### Production Safety Agent
- **Ответственность на этой задаче:**
  - Убедиться, что изменения коснутся только report/analytics и не влияют на trading execution.
- **Сделано / нужно сделать Production Safety**:
  - [ ] Подтвердить отсутствие влияния на runtime trading/queues/datasets write-path.

---
## 9. Execution Log
| Date       | Who (Agent / Person)       | Action / Comment                                   |
|-----------|-----------------------------|----------------------------------------------------|
| 2026-03-18| PM Agent                    | Created full Spec for B40_REPORTS (PLAN stage).  |

---
## 10. Final Decision
- **Status**: Approved
- **Reason**: Draft Spec for reconciliation and reporting integrity.
- **Links to evidence**:
  - TG compact report text (observed diff) and example TG entry signals (run_id/symbol).

