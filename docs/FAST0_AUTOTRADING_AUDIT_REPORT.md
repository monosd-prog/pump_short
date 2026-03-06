# FAST0 + Autotrading — Architecture & Data Flow Audit Report

**MODE:** READ-ONLY (no code changes)  
**GOAL:** Explain architecture + data flow for FAST0 + autotrading to fix "outcomes exist but events missing" without breaking live autotrading.

---

## 1. Entry Points & Runtime

### How the system starts in live mode

| Component | Entry Point | How Started |
|-----------|-------------|-------------|
| **Runner** | `trading.runner` (`python -m trading.runner --once`) | systemd timer (`pump-trading-runner.service` + `.timer`), every 10 sec |
| **API server** | `short_pump.__main__` → `uvicorn.run("short_pump.server:app")` | Manual/separate process: `python -m short_pump api` |
| **Watcher (short_pump)** | `run_watch_for_symbol` | Started by API on POST `/pump` via `Runtime.start_watch` (asyncio thread) |
| **FAST0** | `run_fast0_for_symbol` | Started by API on POST `/pump` if `ENABLE_FAST_FROM_PUMP=1` (daemon thread) |

### systemd service

```text
# deploy/systemd/pump-trading-runner.service
Type=oneshot
WorkingDirectory=/root/pump_short
EnvironmentFile=/root/pump_short/.env
ExecStart=/root/pump_short/venv/bin/python -m trading.runner --once
Restart=on-failure
```

### Key modules

| Module | Role |
|--------|------|
| `short_pump/server.py` | FastAPI app: `/pump`, `/status`; starts watcher + FAST0 |
| `short_pump/runtime.py` | `Runtime.start_watch`, filters, asyncio-to-thread bridge |
| `trading/runner.py` | Consumes signal queue, risk checks, `broker.open_position`, `record_open` |
| `short_pump/fast0_sampler.py` | FAST0 tick loop, ENTRY_OK, events/trades/outcomes, outcome watcher |
| `short_pump/watcher.py` | short_pump watcher loop, decide_entry, track_outcome_short |
| `trading/outcome_worker.py` | Resolves live outcomes from Bybit, `close_from_live_outcome` |
| `trading/bybit_live.py` | BybitLiveBroker, `open_position`, `get_closed_pnl` |
| `common/io_dataset.py` | `write_event_row`, `write_trade_row`, `write_outcome_row`, `_dataset_dir` |
| `trading/paper_outcome.py` | `close_from_live_outcome`, `_write_live_outcome_to_datasets` |

---

## 2. FAST0 Pipeline (End-to-End)

### Call graph (FAST0)

```
POST /pump (server.py:145)
  → Runtime.start_watch (runtime.py:123)
  → run_watch_for_symbol (short_pump) — asyncio thread
  → ENABLE_FAST_FROM_PUMP and _can_start_fast0(symbol):
       run_fast0_for_symbol(symbol, run_id, pump_ts, mode="live", base_dir=DATASETS_ROOT)
       — daemon thread (server.py:191–203)
```

### FAST0 tick loop

| Step | Location | Description |
|------|----------|-------------|
| **Trigger** | `short_pump/server.py:184–203` | POST `/pump` + `ENABLE_FAST_FROM_PUMP` + `_can_start_fast0(symbol)` |
| **Tick loop** | `fast0_sampler.py:545` | `while (time.time() - start_ts) < FAST0_WINDOW_SEC` |
| **Poll interval** | `fast0_sampler.py:547` | `time.sleep(FAST0_POLL_SECONDS)` (default 10) |
| **Window** | `fast0_sampler.py:41` | `FAST0_WINDOW_SEC` (default 180) |

### ENTRY_OK decision

- **Function:** `should_fast0_entry_ok(payload, tick)` — `fast0_sampler.py:119`
- **Conditions:** liq_long_usd_30s > 0, tick ≥ FAST0_ENTRY_MIN_TICK, context_score ≥ FAST0_ENTRY_CONTEXT_MIN, dist_to_peak_pct ≥ FAST0_ENTRY_DIST_MIN, cvd ratios within limits
- **One-shot:** `entry_ok_fired` ensures only one ENTRY_OK per run (`fast0_sampler.py:574–577`)

### trade_id / event_id generation

| Field | Location | Format |
|-------|----------|--------|
| **run_id** | `runtime.py:148` | `time.strftime("%Y%m%d_%H%M%S")` (from /pump) |
| **event_id** | `fast0_sampler.py:634` | `{run_id}_fast0_{tick}_{uuid8}` |
| **trade_id** | `fast0_sampler.py:725` | `{event_id}_trade` |

### TG signal gate

- **Entry TG:** `FAST0_TG_ENTRY_ENABLE` (default 0) + `liq_tg > 0` — `fast0_sampler.py:766–797`
- **Outcome TG:** `FAST0_TG_OUTCOME_ENABLE` + `dist_val >= FAST0_TG_OUTCOME_MIN_DIST` + `liq_val > 0` — `fast0_sampler.py:433–471`

### Outcome watcher

- **Start:** `fast0_sampler.py:704–817` — daemon thread when ENTRY_OK
- **Paper:** `track_outcome()` (candles) → TP/SL/TIMEOUT → `write_outcome_row`
- **Live:** `resolve_live_outcome()` (Bybit); no candle fallback; outcome worker in runner resolves

---

## 3. Dataset Writing (Critical)

### Path logic (all writers)

```python
# common/io_dataset.py:48–62
def _dataset_dir(strategy, wall_time_utc, base_dir=None):
    dt = datetime.fromisoformat(wall_time_utc.replace("Z", "+00:00").replace("+0000", "+00:00"))
    day = dt.strftime("%Y%m%d")
    exec_mode = _get_exec_mode()  # EXECUTION_MODE / AUTO_TRADING_MODE
    rel_parts = (f"date={day}", f"strategy={strategy}", f"mode={exec_mode}")
    return os.path.join(base_dir or "datasets", *rel_parts)
```

**Resolved path:** `{base_dir}/date=YYYYMMDD/strategy={strategy}/mode={paper|live}/`

### events_v3.csv

| When | Location | wall_time_utc source |
|------|----------|----------------------|
| **Create/header** | `ensure_dataset_files` at FAST0 start | `now_utc_start = wall_time_utc()` |
| **Rows** | `write_event_row` at every tick | `now_utc = wall_time_utc()` |
| **base_dir** | `fast0_sampler.py:715–717` | `base_dir_str` (DATASETS_ROOT from server) |

```python
# fast0_sampler.py:712–717
write_event_row(row, strategy=STRATEGY, mode=mode, wall_time_utc=now_utc,
                schema_version=3, base_dir=base_dir_str)
```

### trades_v3.csv

| When | Location | wall_time_utc source |
|------|----------|----------------------|
| **Rows** | `write_trade_row` on ENTRY_OK | `now_utc` |
| **base_dir** | `fast0_sampler.py:741–746` | `base_dir_str` |

### outcomes_v3.csv (FAST0 path)

| When | Location | wall_time_utc source |
|------|----------|----------------------|
| **Paper (track_outcome)** | `fast0_sampler.py:406–412` | `outcome_time_utc` (TP/SL: `hit_time_utc`; TIMEOUT: `datetime.now(timezone.utc).isoformat()`) |
| **base_dir** | `fast0_sampler.py:411` | `base_dir` (from outcome watcher kwargs = `base_dir_str`) |

```python
# fast0_sampler.py:386–389 (TIMEOUT path)
outcome_ts = datetime.now(timezone.utc)
outcome_time_utc = outcome_ts.isoformat()
# ...
write_outcome_row(..., wall_time_utc=outcome_time_utc, base_dir=base_dir)
```

### outcomes_v3.csv (LIVE path — runner outcome worker)

| When | Location | wall_time_utc source |
|------|----------|----------------------|
| **Live close** | `paper_outcome.py:309–316` via `_write_live_outcome_to_datasets` | `outcome_ts_utc` (Bybit exit ts) |
| **base_dir** | `paper_outcome.py:315` | `base_dir or DATASET_BASE_DIR` |

---

## 4. Config/Env Affecting Paths or Behavior

| Env | Default | Effect |
|-----|---------|--------|
| `DATASETS_ROOT` | `/root/pump_short/datasets` | FAST0 base_dir (server) |
| `DATASET_BASE_DIR` | `{project_root}/datasets` | Runner, paper_outcome |
| `EXECUTION_MODE` / `AUTO_TRADING_MODE` | `paper` | `_get_exec_mode()` → `mode=` in path |
| `ENABLE_FAST_FROM_PUMP` | `0` | Enables FAST0 on /pump |
| `FAST0_WINDOW_SEC` | `180` | Tick loop duration |
| `FAST0_POLL_SECONDS` | `10` | Tick interval |
| `FAST0_OUTCOME_WATCH_SEC` | `1800` | Outcome watcher timeout |
| `AUTO_TRADING_ENABLE` | `0` | Enables enqueue_signal → runner |
| `DATASET_V1` | `1` | Also writes legacy events.csv, trades.csv, outcomes.csv |

---

## 5. Autotrading Safety Surface

### Must NOT be changed (live autotrading)

| Area | Modules | Rationale |
|------|---------|-----------|
| Execution / broker | `trading/bybit_live.py`, `trading/broker.py` | Order placement, TPSL, get_closed_pnl |
| Risk sizing | `trading/risk.py`, `trading/runner.py` (calc_position_size, validate_*) | Live reject logic |
| Position / state | `trading/state.py`, `trading/outcome_worker.py` | record_open/record_close, live outcome sync |
| Runner loop | `trading/runner.py` | Queue consumption, broker.open_position, outcome_worker |
| Signal queue | `trading/queue.py` | enqueue_signal contract |

### Safe to change (analytics-only)

| Area | Modules |
|------|---------|
| Analysis load | `pump_short_analysis/analysis/load.py` |
| Report scripts | `scripts/daily_tg_report.py`, `scripts/sanity_check.py` |
| Dataset schema parsing | Analysis-side schema handling |

---

## 6. Hypotheses: outcomes exist for date=20260302 but events missing

### H1: Different base_dir (DATASETS_ROOT vs DATASET_BASE_DIR)

- **FAST0 events/trades:** `base_dir=DATASETS_ROOT` (server: `/root/pump_short/datasets`)
- **Live outcomes:** `base_dir=DATASET_BASE_DIR` (trading.config)
- **Code:** `short_pump/server.py:198` vs `trading/paper_outcome.py:315`
- If `DATASET_BASE_DIR` ≠ `DATASETS_ROOT`, events go to one root and outcomes to another.

### H2: Cross-day date partition

- **Events:** `wall_time_utc=now_utc` at tick (e.g. 23:58 on 20260301)
- **Outcomes (TIMEOUT):** `wall_time_utc=outcome_time_utc` ~30 min later (00:28 on 20260302)
- **Code:** `_dataset_dir` uses `day = dt.strftime("%Y%m%d")` from `wall_time_utc`
- Events → `date=20260301`, outcomes → `date=20260302`.

### H3: FAST0 process crash before event write

- Events written inside tick loop (`fast0_sampler.py:712`).
- Outcome watcher runs in daemon thread; if main process dies after ENTRY_OK but before next `write_event_row`, events for later ticks are lost.
- Outcomes can still be written by runner’s outcome worker (live) or by outcome watcher if it runs in a different process (unlikely for FAST0).

### H4: Different strategy or mode path

- Analysis filters by `strategy=short_pump_fast0` and `mode=live`.
- If events were written with wrong strategy/mode (e.g. typo or env), they would land under a different path.
- **Code:** `_get_exec_mode()` uses `EXECUTION_MODE`; if server and analysis use different env, mode can differ.

### H5: ensure_dataset_files vs actual writers use different roots

- `ensure_dataset_files` creates empty files with headers.
- FAST0 calls it with `base_dir=DATASETS_ROOT` at start (`fast0_sampler.py:528`).
- If `DATASETS_ROOT` is unset or wrong at runtime, `base_dir_str` could be `None` → `io_dataset` uses `os.path.join("datasets", ...)` (CWD-relative).
- Runner/outcome worker uses `DATASET_BASE_DIR`. CWD and project root can differ.

---

## 7. Verification Commands (no edits)

```bash
# 1. Compare DATASETS_ROOT vs DATASET_BASE_DIR at runtime
grep -E "DATASETS_ROOT|DATASET_BASE_DIR" /root/pump_short/.env 2>/dev/null || true

# 2. Resolved dataset path from logs (FAST0)
journalctl -u pump-short-api -n 500 --no-pager 2>/dev/null | grep -E "FAST0_DATASET_ROOT|DATASET_DIR|date="

# 3. Runner dataset path
journalctl -u pump-trading-runner -n 200 --no-pager 2>/dev/null | grep DATASET_DIR

# 4. Search run_id in events (all date dirs)
ROOT=/root/pump_short/datasets
grep -r "20260302_191710" "$ROOT"/date=*/strategy=short_pump_fast0/mode=live/events_v3.csv 2>/dev/null | head -5

# 5. Search run_id in outcomes
grep -r "20260302_191710" "$ROOT"/date=*/strategy=short_pump_fast0/mode=live/outcomes_v3.csv 2>/dev/null | head -5

# 6. List date dirs with outcomes but no events
for d in "$ROOT"/date=*/strategy=short_pump_fast0/mode=live; do
  [ -f "$d/outcomes_v3.csv" ] && [ ! -s "$d/events_v3.csv" ] && echo "OUTCOMES_ONLY: $d"
done

# 7. Python: load_outcomes and inspect max_date / row
PYTHONPATH=/opt/pump_short_analysis python3 -c "
from analysis.load import load_outcomes
df, n = load_outcomes(base_dir='/root/pump_short/datasets', strategy='short_pump_fast0', mode='live', days=30, verbose=True, return_file_count=True)
print('rows', len(df), 'file_count', n)
if len(df) > 0:
    print('max_date', df['date'].max())
    r = df[df['run_id'] == '20260302_191710'].iloc[0] if '20260302_191710' in df['run_id'].values else None
    if r is not None:
        print('row', r.to_dict())
"
```

---

*Report generated from code reading. No patches applied.*
