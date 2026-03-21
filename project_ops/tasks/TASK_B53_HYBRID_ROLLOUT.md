## Task ID
B53_HYBRID_ROLLOUT

## Title
Hybrid rollout for dist_to_peak_pct filter with PAPER-first route

## Epic
Signal Quality / Strategy Rollout

## Priority
Critical

## Status
In Spec

---
## 1. Business / Strategy Context
- **Зачем задача нужна?**
  - Нужно превратить найденный edge bucket `dist_to_peak_pct < 1` в отдельный routed strategy path и проверить его в PAPER-first rollout.
- **Какую проблему она решает?**
  - Позволяет тестировать новый filtered контур без смешивания статистики с baseline и без глобального рефакторинга submode.
- **На какие метрики влияет?**
  - WR, EV/expectancy, coverage, robustness, outcome completeness, report integrity.

---
## 2. Scope
### In scope
1. Add `short_pump_filtered` as a separate strategy identity.
2. Route signals to `short_pump_filtered` only when:
   - `SHORT_PUMP_FILTERED_ENABLE=1`
   - `dist_to_peak_pct <= SHORT_PUMP_FILTERED_DIST_TO_PEAK_MAX`
3. Keep `short_pump` baseline unchanged for compatibility and analytics.
4. PAPER-first rollout, then LIVE enable only after validation passes.
5. Update logging, dataset identity, and reports so the strategies remain separate.

### Out of scope
1. No global refactor of all submodes.
2. No baseline execution logic changes.
3. No position sizing changes.
4. No other strategy families touched.

---
## 3. Inputs
- **Кодовые модули**:
  - `short_pump/watcher.py`
  - `trading/runner.py`
  - `trading/risk_profile.py`
  - `short_pump/attach.py`
  - `trading/outcome_worker.py`
  - `trading/paper_outcome.py`
  - `trading/outcome_delivery.py`
  - `analytics/load.py`
  - `analytics/executive_report.py`
  - `scripts/daily_tg_report.py`
  - `common/io_dataset.py`
- **Датасеты / отчеты**:
  - `datasets/date=*/strategy=short_pump/**`
  - `datasets/date=*/strategy=short_pump_filtered/**`
  - compact / executive reports
- **Policy / task docs**:
  - `project_ops/MODEL_USAGE_POLICY.md`
  - `project_ops/WORKFLOW.md`
  - `project_ops/AGENT_PROMPT_RULES.md`

---
## 4. Spec
- Add a mutually exclusive route decision:
  - if filter enabled and `dist_to_peak_pct <= SHORT_PUMP_FILTERED_DIST_TO_PEAK_MAX`, route to `short_pump_filtered`
  - else route to `short_pump`
- Keep baseline execution path intact.
- Ensure the same event lineage cannot produce both strategy paths for the same signal.
- Add shadow/debug fields:
  - `would_pass_dist_filter`
  - `dist_to_peak_pct`
  - `filter_reason`
  - `strategy_candidate`
- Make report/loaders explicitly display `short_pump` and `short_pump_filtered` separately.

---
## 5. Acceptance Criteria
- [ ] `SHORT_PUMP_FILTERED_ENABLE` and `SHORT_PUMP_FILTERED_DIST_TO_PEAK_MAX` exist and control routing.
- [ ] Routing is mutually exclusive: one `event_id` / signal lineage cannot produce both `short_pump` and `short_pump_filtered` trade/outcome paths.
- [ ] `short_pump_filtered` is PAPER-first and then LIVE only after paper validation passes.
- [ ] Baseline `short_pump` behavior remains unchanged.
- [ ] No parallel double-open occurs for the same signal lineage.
- [ ] Datasets and reports keep the two strategies separate.
- [ ] Compact report shows separate `short_pump` and `short_pump_filtered` blocks.

---
## 6. Validation Plan
- Verify routing on a controlled PAPER window.
- Confirm no duplicate trade/outcome creation for the same `event_id`.
- Check dataset folders and loader outputs for both strategies.
- Check compact/executive report display and grouping.
- Only after paper validation passes, enable LIVE for `short_pump_filtered`.

---
## 7. Risks
- **Data risk**
  - Incorrect routing could create duplicate lineage or split stats.
- **Operational risk**
  - A whitelist or report mapping could hide the new strategy if not updated.
- **Trading risk**
  - LIVE enable before PAPER validation could put untested edge into execution.
- **Rollback notes**
  - Flip `SHORT_PUMP_FILTERED_ENABLE=0` to disable the new route without touching baseline.

---
## 8. Agent Handoff
### PM Agent
- Approve PAPER-first rollout and LIVE gate after validation.

### Strategy Developer Agent
- Implement routing, logging, and report separation.

### QA / Validation Agent
- Verify mutual exclusivity, paper outcomes, and report splits.

### Production Safety Agent
- Confirm no baseline regression and safe LIVE enable criteria.

---
## 9. Execution Log
| Date       | Who (Agent / Person)       | Action / Comment |
|-----------|-----------------------------|------------------|
| 2026-03-19| PM Agent                    | Created B53 for hybrid rollout of dist_to_peak_pct filter with feature flags. |

---
## 10. Final Decision
- **Status**: In Spec
- **Reason**: Pending implementation and PAPER validation.
- **Links to evidence**:
  - `entries_v2` edge bucket and rule filter reports
  - rollout policy docs in `project_ops`

---
## 11. Rollout Checklist

### Before PAPER enable
- [ ] `SHORT_PUMP_FILTERED_ENABLE=1` is set only in the paper-safe environment.
- [ ] `SHORT_PUMP_FILTERED_DIST_TO_PEAK_MAX=1.0` is confirmed in config.
- [ ] Routing is mutually exclusive for the same `event_id` / signal lineage.
- [ ] `short_pump_filtered` is allowed in loader/report whitelist and display mapping.
- [ ] `short_pump_filtered` is visible as a separate strategy in compact/executive reports.
- [ ] Shadow/debug fields are present in logs and datasets.
- [ ] Baseline `short_pump` execution remains unchanged.

### Before LIVE enable
- [ ] PAPER results for `short_pump_filtered` are available over the agreed observation window.
- [ ] No duplicate trade/outcome paths were observed for any signal lineage.
- [ ] Compact/executive reports show `short_pump` and `short_pump_filtered` separately.
- [ ] Outcome tracking and delivery path work for `short_pump_filtered`.
- [ ] Dataset loading / analytics loaders preserve the new strategy identity.
- [ ] Production Safety review is complete.
- [ ] `SHORT_PUMP_FILTERED_ENABLE` is enabled only after paper validation passes.

### Blocking metrics
- [ ] Any duplicate trade or outcome path for the same `event_id` or signal lineage.
- [ ] Any mixed statistics between `short_pump` and `short_pump_filtered`.
- [ ] Outcome coverage failures for the filtered strategy.
- [ ] Report or loader whitelists that hide `short_pump_filtered`.
- [ ] Any baseline regression in `short_pump` execution path.

### Non-blocking metrics
- [ ] Small sample size during the first PAPER window.
- [ ] Early expectancy volatility before enough samples accumulate.
- [ ] Keep rate lower than baseline if the filtered path is intentionally selective.
- [ ] Missing shadow/debug fields in historical rows only, if current live path is correct.
