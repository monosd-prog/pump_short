# Task Template — pump_short

> Copy this template for every new task. Fill all sections before implementation starts.
> Agent MUST NOT proceed to IMPLEMENT without completed sections 1–7.

---

## 1. Title
<!-- One-line description of the task -->
**Task ID**: `TASK_XXX`
**Title**: _Short descriptive title_

---

## 2. Goal
<!-- What problem does this solve? What metric improves? -->
- **Problem**:
- **Expected outcome**:
- **Affected metrics**: _(PnL / WR / MDD / signal frequency / data quality / latency)_

---

## 3. Context
<!-- Relevant background. What strategy/module is involved? What is the current behavior? -->
- **Strategy**: _(short_pump / short_pump_fast0 / long_pullback / all)_
- **Execution mode**: _(paper / live / both)_
- **Current behavior**:
- **Desired behavior**:
- **Related docs/decisions**: _(link to DECISIONS.md, DIAG_*.md, etc.)_

---

## 4. Inputs
<!-- Exact files and data sources in scope -->

**Files to change**:
```
- path/to/file1.py   # reason
- path/to/file2.py   # reason
```

**Reference files** (read-only):
```
- path/to/ref.py     # for context only
```

**Data / datasets**:
```
- datasets/date=YYYYMMDD/strategy=.../events_v3.csv
```

**Config / ENV**:
```
- ENV variable: KEY=value
```

---

## 5. Constraints
<!-- Hard rules the agent must follow -->
- [ ] Change ONLY files listed in section 4
- [ ] Do NOT modify trading logic outside scope
- [ ] Do NOT change dataset schema without migration plan
- [ ] Do NOT touch live config without Production Safety review
- [ ] Add logging for every new code path
- [ ] Preserve existing function signatures / interfaces
- [ ] _Additional constraint specific to this task_

---

## 6. Expected Output
<!-- What exactly should be produced -->
- **Code changes**: _(describe what changes, not how)_
- **New files**: _(if any)_
- **Logging**: _(what should appear in logs)_
- **Dataset effect**: _(new columns / no change / schema update)_

---

## 7. Validation
<!-- How to verify the result is correct -->

**Smoke / unit test**:
```bash
# command to run
python scripts/smoke_xxx.py
```

**Manual check**:
- [ ] Check log output: _(what to look for)_
- [ ] Check dataset: _(which file, which columns)_
- [ ] Check Telegram notification: _(if applicable)_

**Edge cases to verify**:
- [ ] Empty input / no signal
- [ ] Duplicate signal
- [ ] Timeout scenario
- [ ] Live vs paper mode difference

**Definition of done**:
- [ ] Smoke passes without errors
- [ ] No regression in existing smoke tests
- [ ] Log output matches expected
- [ ] Dataset schema unchanged (or migration confirmed)

---

## 8. Risks
<!-- Be explicit. No risk = task is not properly analyzed -->

| Risk | Severity | Mitigation |
|------|----------|------------|
| Trading logic regression | High | Run full smoke suite before deploy |
| Dataset schema break | High | Check all consumers of affected CSV |
| PnL impact (signal filter change) | Medium | Compare outcome stats before/after |
| Live position affected | High | Test in paper mode first |
| _Specific risk_ | _Level_ | _Plan_ |

**Rollback plan**:
- _(git revert commit X / restart service Y / restore file Z)_

---

## 9. Out of Scope
<!-- Explicitly list what is NOT done in this task -->
- No changes to unrelated strategies
- No changes to ML model weights
- No UI / reporting changes unless specified
- _Task-specific exclusions_

---

## 10. Model Recommendation
<!-- Select based on task risk and stage -->

| Stage | Recommended model | Reason |
|-------|------------------|--------|
| PLAN | `high` | Architecture analysis |
| IMPLEMENT | `medium` | Focused code change |
| VALIDATE | `high` | Risk assessment |
| REVIEW | `high` / `ultra` | Trading safety |

**Overall task risk**: _(low / medium / high)_
- `low` → log parsing, doc edits, report scripts
- `medium` → new feature, script, backfill, analytics
- `high` → trading logic, ML pipeline, live config, risk guard

---

## 11. Mode Recommendation
<!-- Continue / Cursor mode to use -->

| Stage | Mode |
|-------|------|
| PLAN | `ask` |
| IMPLEMENT | `agent` |
| VALIDATE | `ask` |
| REVIEW | `ask` |

---

---

## Example — Filled Template

**Task ID**: `TASK_B54`
**Title**: Add volume_zscore feature to fast0 sampler

### 2. Goal
- **Problem**: fast0 entries missing volume context, leading to low-quality signals
- **Expected outcome**: volume_zscore column added to events_v3.csv for fast0
- **Affected metrics**: signal quality / dataset coverage

### 3. Context
- **Strategy**: short_pump_fast0
- **Execution mode**: both
- **Current behavior**: fast0_sampler.py does not compute volume Z-score
- **Desired behavior**: volume_zscore computed per event and logged to events_v3
- **Related docs**: docs/ARCHITECTURE_BRIEF.md

### 4. Inputs
```
Files to change:
- short_pump/fast0_sampler.py   # add volume_zscore calculation
- common/dataset_schema.py      # add new column definition

Reference files:
- short_pump/features.py        # existing zscore logic to reuse
```

### 5. Constraints
- [x] Change ONLY fast0_sampler.py and dataset_schema.py
- [x] Do NOT modify outcome tracking or trading logic
- [x] Add log line: `logger.info("volume_zscore=%.4f", zscore)`

### 6. Expected Output
- **Code changes**: volume_zscore computed and passed to write_event_row()
- **Dataset effect**: new column `volume_zscore` in events_v3.csv (float, nullable)

### 7. Validation
```bash
python scripts/smoke_fast0_volume_features.py
```
- [ ] Column `volume_zscore` present in output CSV
- [ ] No NaN for normal market conditions
- [ ] NaN allowed for first window (insufficient history)

### 8. Risks
| Risk | Severity | Mitigation |
|------|----------|------------|
| Schema break for downstream analytics | Medium | Check analytics/load.py handles new column |
| Division by zero in zscore | Low | Add guard: if std == 0 → return None |

**Rollback**: revert fast0_sampler.py and dataset_schema.py, no service restart needed

### 9. Out of Scope
- No changes to short_pump (non-fast0) sampler
- No ML retraining

### 10. Model Recommendation
| Stage | Model |
|-------|-------|
| PLAN | `high` |
| IMPLEMENT | `medium` |
| VALIDATE | `high` |

### 11. Mode Recommendation
| Stage | Mode |
|-------|------|
| PLAN | `ask` |
| IMPLEMENT | `agent` |
| VALIDATE | `ask` |
