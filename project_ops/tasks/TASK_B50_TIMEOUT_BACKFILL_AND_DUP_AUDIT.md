## Task ID
B50_TIMEOUT_BACKFILL_AND_DUP_AUDIT

## Title
Audit: TIMEOUT backfill feasibility and duplicate records in datasets

## Epic
Outcome Tracking / Data Quality

## Priority
Critical

## Status
In Spec

---

## 1. Business / Strategy Context
- **Зачем задача нужна?**
  - После стабилизации handoff (B49) мы собираем исторические outcomes. Многие rows имеют outcome=TIMEOUT; нужно понять, можно ли ретроспективно восстановить истинный TP/SL (backfill) и одновременно провести аудит дублей в датасетах.
- **Какую проблему она решает?**
  - Определяет границы достоверного backfill'а и выявляет источники дубликатов, чтобы не испортить ML dataset и аналитические выводы.
- **На какие метрики влияет?**
  - Data coverage (share of backfillable TIMEOUTs), label quality for ML, duplicate rate, analytics trust, downstream ML performance.

---

## 2. Scope
### In scope
1. Полный аудит всех rows с outcome == TIMEOUT в datasets (all date/strategy/mode scope as configured).
2. Классификация TIMEOUT rows на:
   - backfillable_from_path (details_payload.path_1m available)
   - backfillable_from_klines (no path_1m but historical klines available from datasets or exchange)
   - unknown_no_path (no path_1m and no klines available)
3. Duplicate audit across datasets:
   - duplicate outcomes rows (same trade_id/event_id)
   - duplicate trades rows
   - duplicate events rows
   - multiple final outcomes for same trade_id (TP/SL/TIMEOUT)
4. Produce audit artifacts (counts, sample lists for manual review, per-date summaries).
5. Produce safe recommendation & plan for a follow-up TASK_B51 (backfill), including confidence tiers.

### Out of scope
1. Do not perform any backfill or data mutation in this task.
2. Do not change production writers/persistors.
3. Do not implement automated reconciliation fixes — only audit + classification + recommendations.

---

## 3. Inputs
- **Кодовые модули:**
  - `common/outcome_tracker.py` (build_outcome_row)
  - `common/io_dataset.py` (write_outcome_row, dataset dir layout)
  - `analytics/load.py` (helpers to read outcomes/trades/events)
  - `scripts/*` (existing smoke / audit helpers)
- **Конфиги / ENV:**
  - DATASET_BASE_DIR, EXECUTION_MODE, OUTCOME_WATCH_MINUTES (for time windows)
- **Датасеты / таблицы:**
  - datasets/date=*/strategy=*/mode=*/outcomes_v3.csv (primary)
  - datasets/.../trades_v3.csv, events_v3.csv
  - existing artifacts produced by B49 validation: /tmp/b49_semi, /tmp/b49_integ (optional)
- **Логи / отчеты:**
  - tracker logs (short_pump.*, common.outcome_tracker)
  - runner logs (trading/runner)

---

## 4. Spec
- **Что именно нужно сделать?**
  1. Enumerate all outcome rows where outcome == "TIMEOUT" across datasets (configurable date window).
  2. For each TIMEOUT row classify into:
     - backfillable_from_path: details_payload contains path_1m with candles covering entry→outcome window.
     - backfillable_from_klines: no path_1m, but historical klines exist in datasets or can be fetched (exchange) for the window.
     - unknown_no_path: no path_1m and no accessible klines.
  3. Compute statistics: absolute counts and percentages per category, per-date and per-strategy.
  4. For backfillable candidates (path or klines) produce a sample subset (e.g., 500 rows) and run deterministic backfill logic offline to classify whether TP/SL would have been hit before timeout and compute inferred MFE/MAE. Record confidence tag.
  5. Duplicate audit:
     - Identify duplicates by primary key trade_id; fallback event_id; composite fallback (run_id,symbol,outcome_time_utc,outcome,side).
     - Produce lists of duplicate groups, classify as: retry/update (different schema_version or later outcome_time), true duplicate (same schema_version and same data repeated), conflicting finals (two final outcomes for same trade_id).
     - For conflicting finals produce a triage list (trade_id, list of rows, source_file, schema_version, outcome_time_utc).
  6. Produce an audit report summarizing counts, top offending dates/files, sample rows for manual review, and a recommended safe plan for TASK_B51 backfill (including confidence tiers, required klines sources, and estimated processing cost).
- **Ограничения**
  - No writes to outcomes_v3.csv or any dataset during this task.
  - All backfill tests run only on copied data or in-memory processing.
- **Что нельзя ломать**
  - Do not change dataset CSV headers or production write paths.
  - Do not remove or alter existing outcomes; audit only.

---

## 5. Acceptance Criteria
- [ ] Inventory: total number of TIMEOUT rows across selected date window (and per-strategy breakdown).
- [ ] Classification counts: numbers for backfillable_from_path, backfillable_from_klines, unknown_no_path (absolute + pct).
- [ ] Backfill trial: sample backfill executed on selected subset with recorded inferred outcomes and confidence tags.
- [ ] Duplicate inventory: counts and sample lists for duplicate outcomes, trades, events, and multiple final outcomes per trade.
- [ ] Conflict list: exportable list of trade_id/event_id cases requiring manual review (CSV).
- [ ] Recommendation: safe plan for TASK_B51 (sources, estimated effort, ordering) attached to report.

---

## 6. Validation Plan
- **Smoke / scripts**:
  - Reuse `analytics/load.py` to load outcomes/trades/events for date window.
  - Implement `scripts/audit_timeout_backfill.py` (temporary) that performs classification and sample backfill (read-only).
  - Implement `scripts/audit_duplicates.py` to detect duplicates and produce triage CSVs.
- **Artifacts to produce**:
  - `reports/B50_summary_{date_range}.md` — executive summary with counts and charts.
  - `reports/B50_timeout_samples.csv` — sample TIMEOUT rows with classification and backfill inference.
  - `reports/B50_duplicates.csv` — list of duplicate groups for manual triage.
  - `reports/B50_recommendation.md` — proposed TASK_B51 plan.
  - All artifacts stored under project_ops/reports/B50/.

---

## 7. Risks
- **Data risk**
  - Large-scale historical klines fetch may be rate-limited; fetching from exchange must be gated and audited.
- **Operational risk**
  - Audit scripts may scan large datasets — use date windowing and sampling to limit runtime.
- **False confidence**
  - Backfill inference has limited confidence when using coarse/partial candles; classification must include confidence flags and not overwrite original labels.
- **Rollback notes**
  - No changes to datasets will be made in this task; rollback not applicable. Any backfill later must be reversible with provenance.

---

## 8. Agent Handoff
### PM Agent
- Approve date window and sample sizes (e.g., last 180 days, sample=500).
- Approve access to exchange klines if required for backfill_from_klines classification.

### Strategy Developer Agent
- Implement read-only audit scripts and produce artifacts per Validation Plan.
- Ensure scripts are deterministic and produce reproducible CSVs.

### QA / Validation Agent
- Run audits, verify counts and sample backfill results, attach evidence files under project_ops/reports/B50/.

### Production Safety Agent
- Approve any access to exchange historical data and rate limits; confirm no production writes.

---

## 9. Execution Log
| Date       | Who (Agent / Person)       | Action / Comment                                   |
|-----------|-----------------------------|----------------------------------------------------|
| 2026-03-19| PM Agent                    | Created TASK_B50 to audit TIMEOUT backfill feasibility and duplicates. |

---

## 10. Final Decision
- **Status**: In Spec
- **Reason**:
  - Preliminary analysis indicates partial feasibility for backfill and the need to quantify duplicates before any data mutation.
  - This task establishes the audit baseline and safe plan for a follow-up backfill task.
- **Links to evidence**:
  - B49 validation artifacts (`/tmp/b49_semi`, `/tmp/b49_integ`) and analytics scripts.

