## Task ID
B52_ENTRIES_V2_RULE_FILTERS

## Title
Evaluate simple rule filters on entries_v2 edge buckets

## Epic
Dataset / ML Readiness

## Priority
High

## Status
In Spec

---
## 1. Business / Strategy Context
- **Зачем задача нужна?**
  - Нужно понять, дают ли найденные edge buckets простые rule-based фильтры устойчивое улучшение на `entries_v2.csv`.
- **Какую проблему она решает?**
  - Показывает, можно ли уже тестировать 1-2 простых фильтра в shadow mode без ML.
- **На какие метрики влияет?**
  - Core winrate, expectancy, keep rate, time-robustness on early/late samples.

---
## 2. Scope
### In scope
1. Offline evaluation of simple rules on `datasets/ml/entries_v2.csv`.
2. Baseline vs filtered comparison.
3. Chronological split into early and late samples for robustness check.
4. Report artifacts in `reports/`.

### Out of scope
1. No changes to production runtime or filters.
2. No retraining or ML pipeline changes.
3. No dataset mutation.

---
## 3. Inputs
- `datasets/ml/entries_v2.csv`
- `reports/entries_v2_edge_buckets.csv`
- `reports/entries_v2_edge_buckets_summary.md`

---
## 4. Spec
- Implement a standalone script that evaluates:
  - baseline
  - rules A-G from the user request
- For each rule report:
  - rows kept, keep rate, TP/SL/TIMEOUT, core winrate, expectancy_pnl_pct, delta vs baseline
- Add early/late chronological split based on `opened_ts` and compute robustness on both halves.

---
## 5. Acceptance Criteria
- [ ] Script exists and runs offline.
- [ ] Summary markdown and CSV are written.
- [ ] Baseline and early/late split are included.
- [ ] Final stdout lists top rules by expectancy and robustness.

---
## 6. Validation Plan
- Run the script on `entries_v2.csv`.
- Verify rule metrics and split metrics in reports.
- Inspect top rules for shadow-mode candidacy.

---
## 7. Risks
- **Data risk**
  - Buckets may be sparse or non-stationary across time.
- **Operational risk**
  - Rules based on a single bucket may overfit the historical sample.
- **Rollback notes**
  - No rollback needed; this is analysis-only.

---
## 8. Agent Handoff
### Strategy Developer Agent
- Implement the offline rule-filter analysis script.

### QA / Validation Agent
- Verify metrics, split behavior, and report artifacts.

### Production Safety Agent
- Confirm analysis is read-only.

---
## 9. Execution Log
| Date       | Who (Agent / Person)       | Action / Comment |
|-----------|-----------------------------|------------------|
| 2026-03-19| PM Agent                    | Created B52 task for rule-filter analysis on entries_v2. |

---
## 10. Final Decision
- **Status**: In Spec
- **Reason**: Pending offline analysis.
- **Links to evidence**:
  - `entries_v2` edge bucket report.
