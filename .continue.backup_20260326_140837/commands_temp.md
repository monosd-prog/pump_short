# Continue Commands — pump_short

## Project Context Quick Reference
- **Strategies**: short_pump, short_pump_fast0, long_pullback
- **Core**: trading/ (runner, broker, outcome_worker), short_pump/ (watcher, signals, fast0)
- **ML**: ml/entry_filter.py (LGBM model)
- **Data**: datasets/date=YYYYMMDD/strategy=.../mode=.../events_v3.csv, trades_v3.csv, outcomes_v3.csv
- **Deploy**: VPS with systemd services, paper vs live modes, smoke scripts

## 1. /task — Convert & Plan

**Purpose**: Accept a free-form task description or link to task file, transform into `.continue/task_template.md` format, execute PLAN stage only — no code changes.

**Workflow**:
1. Read task description or linked file
2. Extract: goal, context, affected strategy (short_pump/fast0/long_pullback)
3. Identify files to change (explicit list)
4. Assess risk (low/medium/high)
5. Produce task brief in template format

**Required sections in output**:
- Diagnosis: current situation
- Root Cause: underlying issue (if applicable)
- Plan: step-by-step approach
- Files: explicit list with reasoning
- Risks: trading/PnL/data/operational
- Validation: commands, manual checks, edge cases

**Model**: `high` for analysis and risk assessment
**Mode**: `ask` — no agent mode until plan approved

**Example**:
```
/task Add dist_to_peak_pct filter routing to short_pump_filtered path
```

---

## 2. /bug — Debug & Root Cause Analysis

**Purpose**: Analyze bug from logs, code, or description using lineage → breakpoint → root cause → minimal fix approach. Do NOT implement fix without explicit permission.

**Workflow**:
1. Lineage: trace data flow (signal → event → trade → outcome → dataset/report)
2. Breakpoint: identify where expected vs actual behavior diverges
3. Root Cause: find the minimal code/logic issue
4. Minimal Fix: propose smallest possible change
5. Validation: commands to verify fix

**Required sections in output**:
- Diagnosis: bug description, symptoms
- Root Cause: exact code/configuration issue
- Breakpoint: where in lineage the divergence occurs
- Minimal Fix: specific code change proposed
- Validation: smoke/unit tests to verify fix

**Model Selection**:
- Simple bug (config, logging, report): `medium`
- Complex bug (trading logic, outcome tracking, data corruption): `high`
**Mode**: `ask` — analysis only, no implementation

**Example**:
```
/bug Paper outcomes for short_pump_fast0 not appearing in dataset after timeout
```

---

## 3. /agent — Implementation Only

**Purpose**: Execute IMPLEMENT stage for an already-approved task brief. Change ONLY files from scope, make minimal changes, add logging. At end, remind about deploy/commit/push steps.

**Prerequisites**:
- Task brief exists and is approved (usually from `/task` output)
- Scope files list explicitly defined
- Constraints understood

**Workflow**:
1. Read task brief (must contain files list, constraints, expected output)
2. For each file: implement minimal change
3. Add logging: `logger.info(...)` for key decisions/actions
4. Preserve existing interfaces, follow existing patterns
5. Output summary with validation checklist

**Required sections in output**:
- Diagnosis: brief summary of what's being implemented
- Plan: confirmation of files to change
- Action: description of changes made (not code diffs)
- Changed Files: list with brief description per file
- Validation: checklist of what to verify before moving to `/validate`

**Model**: `medium` for focused coding implementation
**Mode**: `agent` — allowed to modify code

**Important reminders to include**:
```
⚠️ Before deploy:
- [ ] Run smoke tests: python scripts/smoke_*.py
- [ ] Check no regressions in existing smoke
- [ ] Validate paper mode first
- [ ] Commit: git add ... && git commit -m "TASK_XXX: description"
- [ ] Push: git push origin main
- [ ] Deploy: bash scripts/deploy_all.sh [--smoke]
- [ ] Monitor logs after restart
```

**Example**:
```
/agent Implement TASK_B53 routing logic per brief in .continue/task_brief_B53.md
```

---

## 4. /validate — Verification & Safety Check

**Purpose**: Validate completed changes. Run verification commands, check edge cases, give verdict. No code modifications.

**Workflow**:
1. Read task brief and implementation summary
2. Run validation commands (smoke, unit tests)
3. Check edge cases identified in plan
4. Verify no regression in unrelated areas
5. Assess risk for production deployment
6. Give verdict with reasoning

**Required sections in output**:
- Diagnosis: what was implemented, what needs verification
- Checks: command outputs, pass/fail status
- Edge Cases: verification of each edge case
- Verdict: `PASS` (ready for deploy), `FAIL` (blocking issues), `NEEDS_REVIEW` (non-blocking but needs human review)
- Rollback Notes: if verdict is FAIL, how to revert safely

**Model**: `high` for thorough safety review
**Mode**: `ask` — verification only, no changes

**Verdict definitions**:
- **PASS**: All validation passes, edge cases handled, no regression detected, safe for deploy
- **FAIL**: Blocking issues found (trading logic break, data corruption, safety violation)
- **NEEDS_REVIEW**: Non-blocking issues (logging gaps, minor edge cases, cosmetic problems) that need human decision before deploy

**Example**:
```
/validate TASK_B53 implementation of short_pump_filtered routing
```

---

## 5. /next — Advance Pipeline Stage

**Purpose**: Move current task to the next stage in the pipeline (PLAN → IMPLEMENT → VALIDATE → REVIEW). Determine current stage from dialog context, task brief, and last response. Never auto-advance to IMPLEMENT without explicit confirmation.

**Workflow**:
1. Analyze context: last messages, any task brief, implementation status
2. Identify current stage using stage markers:
   - **PLAN**: Analysis, task brief creation, no code changes yet
   - **IMPLEMENT**: Code changes in progress or just completed
   - **VALIDATE**: Verification commands being run, edge case checks
   - **REVIEW**: Safety assessment, PnL impact analysis, deployment readiness
3. If stage unclear → explicitly state need for clarification
4. Provide structured transition plan
5. Never auto-advance to IMPLEMENT → always confirm scope first

**Stage Markers**:
- **PLAN** keywords: "analysis", "task brief", "plan", "scope", "files list", "risk assessment"
- **IMPLEMENT** keywords: "implement", "code change", "modify", "add logging", "changed files"
- **VALIDATE** keywords: "validate", "verify", "smoke test", "edge case", "check", "test"
- **REVIEW** keywords: "review", "safety", "risk", "deploy", "production", "live enable"

**Transition Rules**:
- PLAN → IMPLEMENT: Must confirm files scope and have approved brief
- IMPLEMENT → VALIDATE: Must have changed files summary and logging added
- VALIDATE → REVIEW: Must have validation results (pass/fail/needs-review)
- REVIEW → COMPLETE: Must have safety approval and deploy plan

**Required sections in output**:
- Current Stage: determined from context
- Next Stage: logical next step in pipeline
- Why: reasoning for transition
- Action: what to do in next stage
- Recommended Model: based on stage and risk
- Recommended Mode: ask vs agent
- Handoff: if needed, specify role transfer (planner → implementer → validator → reviewer)

**Model Selection by Stage**:
- PLAN → `high` (analysis)
- IMPLEMENT → `medium` (coding)
- VALIDATE → `high` (verification)
- REVIEW → `high`/`ultra` (safety)

**Mode Selection**:
- PLAN → `ask` (planning only)
- IMPLEMENT → `agent` (code changes allowed)
- VALIDATE → `ask` (verification only)
- REVIEW → `ask` (assessment only)

**Example**:
```
/next We have task brief for TASK_B53. Ready to implement?
```
**Output would show**:
```
Current Stage: PLAN
Next Stage: IMPLEMENT
Why: Task brief approved with explicit files list and constraints
Action: Implement routing logic in short_pump/watcher.py and trading/runner.py
Recommended Model: medium (focused coding)
Recommended Mode: agent
Handoff: planner → implementer (you can now use /agent)
```

**Safety Notes**:
- If unclear about current stage → explicitly ask: "Please clarify current stage: Are we in PLAN, IMPLEMENT, VALIDATE, or REVIEW?"
- Never auto-transition to IMPLEMENT if trading logic or risk guard might be affected
- Always confirm scope files before IMPLEMENT transition
- For REVIEW → COMPLETE transitions, require explicit production safety approval

---

## 6. /release — Production Release Preparation

**Purpose**: Final stage after VALIDATE/REVIEW — prepare safe release for pump_short project. No code changes. Create release checklist, rollback plan, and give final verdict.

**Workflow**:
1. Verify pipeline completion: must have evidence of PLAN, IMPLEMENT, VALIDATE stages
2. If VALIDATE not passed → block release and explain why
3. Assess risk level → if high, require explicit REVIEW sign-off
4. Generate release checklist specific to task scope
5. Generate rollback plan based on changes
6. Identify special flags if task affects:
   - Trading logic (position opening/closing, risk guard)
   - Outcome pipeline (TP/SL/TIMEOUT resolution, dataset writing)
   - Paper/live routing (execution mode switching)
   - Analytics/reports (stats, TG reports, dashboard)
7. Give final verdict with reasoning

**Required sections in output**:
- Diagnosis: what task was completed, what changes were made
- Release Readiness: evaluation of readiness for production
- Release Checklist: step-by-step actions to deploy safely
- Rollback Plan: explicit steps to revert if issues arise
- Verdict: `READY_FOR_RELEASE`, `NEEDS_REVIEW`, `BLOCKED`

**Verdict Definitions**:
- **READY_FOR_RELEASE**: All stages complete, validation passed, risks mitigated, safe for production
- **NEEDS_REVIEW**: Non-critical issues or missing validation evidence needs human review
- **BLOCKED**: Critical issues found, validation failed, high-risk changes not approved

**Release Checklist Template**:
```
Before commit:
- [ ] Smoke tests pass: python scripts/smoke_*.py
- [ ] Paper mode verified in dry-run
- [ ] No regression in baseline strategy performance
- [ ] Log output matches expected patterns

Git operations:
- [ ] git add [changed files]
- [ ] git commit -m "TASK_XXX: descriptive message"
- [ ] git push origin main

Deploy operations:
- [ ] bash scripts/deploy_all.sh [--smoke]
- [ ] Verify service restart: systemctl status pump-short.service
- [ ] Monitor logs for first 10 minutes after restart
- [ ] Check Telegram reports for any anomalies

Post-release:
- [ ] Run post-release smoke: python scripts/smoke_*.py
- [ ] Verify outcome delivery for any new trades
- [ ] Monitor PnL/MDD metrics for 24h
```

**Rollback Plan Template**:
```
If issue detected:
1. Immediate rollback trigger: [describe trigger condition]
2. Rollback action: git revert [commit_hash] OR set ENV flags to disable
3. Service restart: systemctl restart pump-short.service
4. Data cleanup: [if dataset affected]
5. Verification: smoke tests after rollback
```

**Special Flags Detection**:
- **Trading logic affected** → require extended monitoring period (48h)
- **Outcome pipeline changed** → verify TP/SL/TIMEOUT resolution on paper first
- **Paper/live routing altered** → test BOTH modes before enabling live
- **Analytics/reports modified** → verify TG report formatting and stats accuracy

**Model**: `high` for production safety assessment
**Mode**: `ask` — no code changes, assessment only

**Example**:
```
/release TASK_B53 short_pump_filtered routing implementation
```

**Safety Rules**:
- No `/release` without completed `/validate` with PASS verdict
- If task affects live trading → require explicit Production Safety Agent approval
- Always include rollback plan — no exceptions
- For high-risk tasks, recommend phased rollout (paper → limited live → full live)

---

## 7. /deploy — VPS Deployment & Service Management

**Purpose**: Deploy changes to VPS, restart services, verify system health. No code modifications — only system commands.

**Context**:
- **VPS**: Ubuntu 20.04+
- **Project path**: `/root/pump_short`
- **Main service**: `pump-short.service` (paper execution)
- **Live service**: `pump-short-live-auto.service` (live trading) — **CAUTION**
- **Deploy script**: `scripts/deploy_all.sh [--smoke]`

**Workflow**:
1. **Safety check**: Confirm deploy is authorized (especially for live trading)
2. **Service selection**: Determine which service to restart based on task scope
3. **Deploy execution**: Run commands in sequence
4. **Health verification**: Check service status, logs, and basic functionality
5. **Diagnostic check**: Verify signals are flowing and outcomes are being tracked

**Required sections in output**:
- Diagnosis: what needs to be deployed, service affected
- Deploy Steps: step-by-step deployment plan
- Commands: ready-to-copy shell commands
- Checks: what to verify after deploy
- Status: OK / WARNING / ERROR with details

**Service Selection Rules**:
- **Paper-only changes** → restart `pump-short.service`
- **Live trading changes** → restart `pump-short-live-auto.service` **ONLY with explicit approval**
- **Configuration/analytics changes** → may require restart of report services: `pump-short-daily-report.timer`, `pump-short-hourly-report-fast0.timer`
- **When in doubt**: restart only `pump-short.service` first

**Deploy Command Template**:
```bash
# 1. Pull latest changes (if applicable)
cd /root/pump_short
git pull origin main

# 2. Run smoke tests (optional but recommended)
bash scripts/deploy_all.sh --smoke

# 3. Restart appropriate service
systemctl restart pump-short.service

# 4. Check service status
systemctl status pump-short.service

# 5. Check logs for errors
journalctl -u pump-short.service -n 100 --no-pager | grep -E "(ERROR|CRITICAL|WARNING|Exception|Traceback)"

# 6. Check basic functionality
# Monitor for incoming signals
tail -f /root/pump_short/logs/logs_short/$(date +%Y-%m-%d)/*.log | grep -E "(WATCH START|ARMED|ENTRY_OK|OUTCOME)" --color=auto
```

**Post-Deploy Verification**:
- **Service status**: `systemctl status <service>` should show `active (running)`
- **No critical errors**: Journal logs should not show `ERROR`/`CRITICAL`/`Traceback`
- **Signal flow**: Within 5-10 minutes, should see new log entries for signal processing
- **Outcome tracking**: Verify new outcomes appear in dataset (`datasets/date=$(date +%Y%m%d)/.../outcomes_v3.csv`)

**Diagnostic Commands**:
```bash
# Check service is running
systemctl is-active pump-short.service

# Check recent errors
journalctl -u pump-short.service --since "5 minutes ago" --no-pager | tail -20

# Check if signals are being processed
ls -la /root/pump_short/datasets/signals_queue.jsonl

# Check latest outcomes
tail -5 /root/pump_short/datasets/date=$(date +%Y%m%d)/strategy=short_pump/mode=paper/outcomes_v3.csv

# Check Telegram bot (if configured)
grep -r "Telegram.*sent" /root/pump_short/logs/logs_short/$(date +%Y-%m-%d)/ 2>/dev/null | tail -3
```

**Safety Rules**:
- **NEVER restart live trading service without explicit approval**
- **ALWAYS check smoke tests before production deploy**
- **Verify paper mode first** → restart paper service, wait for outcomes, then consider live
- **Rollback ready**: Have rollback commands prepared before starting deploy
- **Monitoring required**: Monitor logs for first 15 minutes after restart

**Example**:
```
/deploy TASK_B53 changes to pump-short.service (paper only)
```

**Output would include**:
- **Diagnosis**: Deploying routing logic changes from TASK_B53 to paper service
- **Deploy Steps**: 1) Pull changes, 2) Smoke test, 3) Restart service, 4) Verify
- **Commands**: Ready-to-run commands with exact paths
- **Checks**: Service status, error logs, signal flow, outcome tracking
- **Status**: Expected outcome and what to monitor

**Common Issues & Resolution**:
- **Service fails to start**: Check Python dependencies, config files
- **No signals appearing**: Check webhook endpoint, signal queue
- **Outcomes not recorded**: Check outcome worker logs, dataset permissions
- **High CPU/Memory**: Check for infinite loops in watcher threads

---

## Quick Reference Table

| Command | Purpose | Model | Mode | Outputs |
|---------|---------|-------|------|---------|
| `/task` | Plan & template | high | ask | Diagnosis, Root Cause, Plan, Files, Risks, Validation |
| `/bug` | Debug & root cause | medium/high | ask | Diagnosis, Root Cause, Breakpoint, Minimal Fix, Validation |
| `/agent` | Implement only | medium | agent | Diagnosis, Plan, Action, Changed Files, Validation |
| `/validate` | Verify & safety | high | ask | Diagnosis, Checks, Edge Cases, Verdict, Rollback Notes |
| `/next` | Advance pipeline stage | context-based | ask/agent | Current Stage, Next Stage, Why, Action, Model, Mode, Handoff |
| `/release` | Production release prep | high | ask | Diagnosis, Release Readiness, Release Checklist, Rollback Plan, Verdict |
| `/deploy` | VPS deployment | medium | ask | Diagnosis, Deploy Steps, Commands, Checks, Status |

## Safety Rules
1. **No `/agent` without approved brief** — always run `/task` first
2. **No `/validate` before `/agent`** — implementation must be complete
3. **No `/release` before `/validate` PASS** — validation must succeed
4. **No `/deploy` to live without explicit approval** — always paper-first
5. **No LIVE changes without Production Safety review** — always paper-first
6. **Always verify with existing smoke scripts** — never skip validation
7. **No `/next` to IMPLEMENT without explicit scope confirmation** — trading logic safety first
8. **Always include rollback plan in `/release`** — no exceptions
9. **Always monitor logs after `/deploy`** — first 15 minutes critical

## Common Validation Commands
```bash
# Basic smoke tests
python scripts/smoke_fast0.py
python scripts/smoke_trading_paper.py
python scripts/smoke_dataset_exec_mode.py

# Paper dry-run (gating only)
python scripts/run_paper_dry_run.py --minutes 60

# Dataset integrity
python scripts/smoke_events_v2_no_nan.py
python scripts/smoke_mfe_mae_columns.py

# Report generation
python scripts/daily_tg_report.py

# Deploy with optional smoke
bash scripts/deploy_all.sh --smoke
```