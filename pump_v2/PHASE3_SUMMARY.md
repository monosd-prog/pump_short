# Phase 3 Summary — Strategies (closed 2026-05-21)

## Completed

| Шаг | Задача | Commit | Тесты |
|-----|--------|--------|-------|
| 3.0 | Dbg5Builder indicator | 96de6ba | 4 |
| 3.1 | ShortPumpStrategy (short_pump_mid pilot) | 6e4bf2e | 8 |
| 3.2 | Parity validation: 56/56 positives, 7/7 negatives | 9fd0aeb | — |
| 3.3 | short_pump_funding_1R (F-fix: stage∈{3,4}) | f4d923b | 14 |
| 3.4 | Pre-run hook в watcher + signal logger | f26e712 | 909 total |

## Design decisions

- A: ShortPumpStrategy — один класс, classify_profile() внутри
- Y: short_pump_mid stage-fix: classify проверяет stage∈{3,4}; tradeable gate остаётся stage==4
- F: short_pump_funding_1R stage-fix: было dead code (v1 stage==3 никогда не доходил до gate); v2 classify stage∈{3,4}
- Pre-run hook: PUMP_V2_PRERUN_ENABLE=1 в /root/pump_short/.env; лог → datasets/prerun_signals_v2.csv

## What's NOT done (intentionally)

- ❌ short_pump_deep, short_pump_active_1R — NotImplementedError, Phase 4+
- ❌ Trade execution — Phase 5
- ❌ Queue integration — Phase 5
- ❌ Outcome tracking для pre-run сигналов — Phase 4

## Artifacts

- pump_v2/indicators/dbg5_builder.py
- pump_v2/strategies/short_pump.py
- pump_v2/prerun/context_builder.py
- pump_v2/prerun/signal_logger.py
- pump_v2/validation/validate_short_pump_mid.py
- pump_v2/validation/validate_short_pump_funding.py
- datasets/prerun_signals_v2.csv (runtime, не в git)

## Test counts

Phase 2 baseline: 886 → Phase 3 итог: 909 (+23)

## What's next — Phase 4

- Outcome tracking для pre-run (сравнение v2 сигналов с v1 ENTRY_OK по времени/символу)
- short_pump_deep, short_pump_active_1R профили
- MarketContext наполнение liquidations + trades для полного entry gate
