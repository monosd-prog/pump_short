# Phase 2 Summary — Indicators (closed 2026-05-15)

Phase 2 goal: port v1 market/context indicators into `pump_v2/indicators/` with parity tests against three fixture suites (`live_collected`, `symbols`, `logged_parity`). v1 production code was not modified as part of this phase.

Closure commits: `f971495` (last indicator), `e72b772` (TODO + Phase 3 notes).

---

## Completed

### Indicator ports (14 modules, 13 commits)

| Indicator | Commit | v1 source / notes |
|-----------|--------|-------------------|
| `oi_change_pct` | `f8f08d4` | `short_pump/features.py` — lookbacks 1 / 3 / 5m |
| `delta_ratio` | `7d500b6` | windows 30s / 60s / 180s |
| `cvd_5m` | `cc9030d` | `common/market_features.cvd_5m` |
| `cvd_delta_ratio` | `e754f69` | 30s / 60s |
| `atr_pct_5m_14` | `fa2b17c` | `features.atr_pct` (fraction); **no sort** on candles (duplicate 5m ts in fixtures) |
| `volume_zscore` | `4c863a6` | default lookback **50**; `nan` when history too short |
| `funding_snapshot` | `c209876` | `normalize_funding` → dataclass (3 fields) |
| `pump_shape_5m` | `1eef444` | `pump_shape_features_5m` |
| `liquidation_rollups` | `7ec54bf` | CSV rollups; Buy→short, Sell→long; window `(end-N, end]` |
| `structure_state` | `edf38b1` | FSM replay; sort + tail(20) peak; cfg **3/1/2/0.8** (not `config.py` fractions) |
| `dist_to_fsm_peak_pct` | `d8227b3` | FSM peak vs last close (`build_dbg5` canon) |
| `dist_to_window_peak_pct` | `d8227b3` | fixture canon: max(high) all bars; optional window |
| `oi_divergence_5m` | `c2f3244` | composite: `dist≤3.5` & `oi_change<0`; `oi None` → False |
| `context_score_5m` | `f971495` | `compute_context_score_5m`; input = **dbg5-shaped** dict |

### Shared utilities

| File | Role |
|------|------|
| `pump_v2/indicators/_candle_utils.py` | `normalize_candles_df` — no sort (MIRRORS_V1_BEHAVIOR) |
| `pump_v2/indicators/_trade_utils.py` | trade normalization for CVD / delta paths |

### Fixture & test harness

- `pump_v2/tests/fixtures/live_collected/` — 5 symbols, WS+REST window, `expected_indicators.json` per symbol
- `pump_v2/tests/fixtures/symbols/` — 5 edge cases (`btc_typical`, `quiet_alt`, `pump_recent`, `no_liquidations`, `no_oi`)
- `pump_v2/tests/fixtures/logged_parity/` — 50 samples from `events_v3.csv`
- `pump_v2/tests/fixtures/_generate.py`, `_compute_expected.py` — v1 golden expected values
- `pump_v2/TODO.md` — discovered quirks and Phase 3+ action items

### Tests count

| Milestone | Passed |
|-----------|--------|
| Phase 2 start (baseline) | 338 |
| After `context_score_5m` (closure) | **886** |

Run: `python3 -m pytest pump_v2/tests/ -q`

---

## Carried over to Phase 3+

Summarized from `pump_v2/TODO.md` (full detail there).

| Topic | Priority | Phase |
|-------|----------|-------|
| **context_score_5m dbg5 input** — not v2 `volume_zscore` / `atr_pct_5m_14`; need `MarketContext` bundle or helpers (`_volume_z`, `_atr_pct_14`) | **high** | 3 |
| **liquidation_rollups naming** — v2 `*_60s` vs v1 CSV `liq_*_1m`; side mapping & window boundary documented | medium | 3 |
| **dist_to_window_peak_pct** — 3 peak semantics (watcher close 60m vs fixture max(high) vs v2 param) | medium | 3 / false_pump |
| **structure_state** — FSM defaults from YAML not `config.py`; `armed_since_utc` nondeterministic | medium | 3; determinism **high** | 4 |
| **live_collected duplicate 5m timestamps** — fix `_live_collect.py` dedup/sort, regen fixtures | medium | before Phase 4 backtest |
| **v1 no-sort on candles/trades** — keep MIRRORS_V1; optional sort/dedup post Phase 7 | low | 7+ |

---

## Validation status

### Overall

- **886 tests passed** at Phase 2 close (`pytest pump_v2/tests/ -q`)
- Pattern per indicator: **GROUP A** (fixtures) + **GROUP B** (logged) + **GROUP C/D** (edge cases)
- Rule: do not change `expected_indicators.json` on failure without explicit approval

### GROUP A — `live_collected` + `symbols`

- **Exact parity** with `expected_indicators.json` (values generated via v1 + `_generate.py` / `_compute_expected.py`)
- `context_score_5m`: end-to-end via `build_dbg5` + `oi_history.parquet` (live) / `oi.parquet` (symbols)

### GROUP B — `logged_parity` (50 samples)

| Mode | Indicators |
|------|------------|
| **Exact / formula on logged CSV fields** | `oi_divergence_5m` (recompute from `oi_change_5m_pct` + `dist_to_peak_pct`) |
| **Smoke** (range, keys, non-crash) | all others — missing raw candles/trades/OI in samples; `context_score` vs `context_parts` may include legacy **`cvd`** key not in canonical `compute_context_score_5m` |

### GROUP C — edge cases

- Empty / short history, clamp bounds, FSM stages, liquidation empty CSV, `no_oi` fixture, OI snapshot timing, etc.
- Covered in per-indicator `test_*.py` files

---

## What awaits Phase 3

### Blocker: dbg5 / context bundle

`ContextScore5mIndicator` expects a **dbg5-shaped** dict (`vol_z`, `atr_14_5m_pct` in percent scale, etc.), not direct outputs of `VolumeZScore` / `ATRPct5m14`.

**Recommended (TODO):** option **(C)** — `MarketContext` / `context_bundle` composite filled each tick like v1 `build_dbg5`, keeping `ContextScore5mIndicator` a pure scorer.

May require porting `context5m._volume_z` (lookback ~48) and `context5m._atr_pct_14` (percent) as helpers or small indicators.

### Strategy migration (pilot order)

1. **short_pump_mid** — first consumer of context score / structure / OI stack
2. **short_pump_funding** — funding + shape features
3. Resolve liquidation `1m` vs `60s` naming and dist-to-peak semantics when wiring false_pump-related paths

### Pre-run validation

Before any v2 strategy goes live:

- [ ] `MarketContext` supplies dbg5-compatible fields where strategies read `context_score_5m`
- [ ] Parity spot-check: one live symbol window vs v1 watcher output
- [ ] `pytest pump_v2/tests/ -q` green
- [ ] Document strategy-specific peak/OI/liq field mappings in strategy YAML or adapter layer

---

## Out of scope (Phase 2)

- v1 production changes (`short_pump/*`, `trading/runner.py`) — left uncommitted; separate false_pump / OI-screener WIP on disk
- Strategies, execution, datasets ETL, backtest engine (Phases 3–4+)
