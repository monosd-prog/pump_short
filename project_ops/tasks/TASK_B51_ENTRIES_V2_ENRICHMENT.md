## Task ID
B51_ENTRIES_V2_ENRICHMENT

## Title
Build entries_v2 enriched with events/trades features for edge analysis

## Epic
Dataset / ML Readiness

## Priority
High

## Status
In Spec

---
## 1. Business / Strategy Context
- **Зачем задача нужна?**
  - `entries_v1` содержит outcome-level сведения, но для нормального edge-analysis не хватает части pre-entry/event features. Нужно собрать enriched dataset с фичами из `events_v3.csv` и `trades_v3.csv`.
- **Какую проблему она решает?**
  - Позволяет делать buckets / segment analysis по контексту сигнала и по фактически записанным event/trade фичам без изменения production runtime.
- **На какие метрики влияет?**
  - Data-quality, coverage joined features, usefulness of edge-analysis, downstream ML-ready segmentation.

---
## 2. Scope
### In scope
1. Offline enrichment `datasets/ml/entries_v1.csv` -> `datasets/ml/entries_v2.csv`.
2. Join with `events_v3.csv` and `trades_v3.csv` using safe key priority:
   - `trade_id` / `position_id` where available
   - `event_id`
   - fallback only if needed: `run_id + symbol + strategy`
3. Add stable feature columns when present:
   - `context_score`, `liq_long_usd_30s`, `liq_short_usd_30s`
   - `oi_change_1m_pct`, `oi_change_5m_pct`
   - `funding_rate_abs`, `risk_profile`, `dist_to_peak_pct`
   - additional stable event-level features from source rows
4. Derived fields:
   - `dist_to_tp_pct`
   - `dist_to_sl_pct`
   - `hour_of_day` from `opened_ts`
5. QC report and summary artifacts.

### Out of scope
1. No changes to runtime trading code.
2. No mutation of production datasets other than writing the new enriched dataset.
3. No automatic backfill/relabel of outcomes.

---
## 3. Inputs
- **Кодовые модули**:
  - `common/dataset_schema.py`
  - `scripts/analyze_entries_v1_edge_buckets.py` (for downstream analysis compatibility)
  - `scripts/*` dataset/report helpers
- **Датасеты**:
  - `datasets/ml/entries_v1.csv`
  - `datasets/**/events_v3.csv`
  - `datasets/**/trades_v3.csv`
- **Reports**:
  - `reports/entries_v1_edge_buckets_summary.md`
  - `reports/entries_v1_edge_buckets.csv`

---
## 4. Spec
- Build a standalone enrichment script that reads `entries_v1.csv`, collects candidate `events_v3.csv` and `trades_v3.csv`, and joins them with a priority order:
  - trade keys first
  - event keys second
  - fallback composite key only when needed
- Keep the join conservative:
  - if a fallback match is ambiguous, do not force it
  - track matched rows by key type in QC
- Write the enriched output to `datasets/ml/entries_v2.csv`.
- Produce `reports/entries_v2_enrichment_qc.md` with counts, matched keys, and missing-feature stats.

---
## 5. Acceptance Criteria
- [ ] `scripts/build_entries_v2_enriched.py` exists and runs offline.
- [ ] `datasets/ml/entries_v2.csv` is produced from `entries_v1.csv` plus joined features.
- [ ] `reports/entries_v2_enrichment_qc.md` includes total rows, primary/fallback match counts, and missing feature counts per column.
- [ ] No runtime / production trading code is changed.

---
## 6. Validation Plan
- Run the enrichment script locally.
- Verify output file existence and row counts.
- Check QC report for matched-by-key breakdown and missing-feature coverage.
- Use `entries_v2.csv` as the input for follow-up edge-analysis.

---
## 7. Risks
- **Data risk**
  - Incorrect fallback joins may create false feature attribution.
- **Operational risk**
  - Multiple source files may contain overlapping rows; merge logic must avoid row explosion.
- **Rollback notes**
  - Remove `entries_v2.csv` and rerun the script if needed; no production rollback is required.

---
## 8. Agent Handoff
### Strategy Developer Agent
- Implement the offline enrichment script and QC report.

### QA / Validation Agent
- Verify output row count, feature coverage, and join-key statistics.

### Production Safety Agent
- Confirm this is read-only against production datasets and does not alter runtime paths.

---
## 9. Execution Log
| Date       | Who (Agent / Person)       | Action / Comment |
|-----------|-----------------------------|------------------|
| 2026-03-19| PM Agent                    | Created B51 task for offline entries_v2 enrichment. |

---
## 10. Final Decision
- **Status**: In Spec
- **Reason**: Pending offline implementation and QC evidence.
- **Links to evidence**:
  - `entries_v1_edge_buckets` reports for downstream analysis context.
