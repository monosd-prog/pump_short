# Migration map: pump_short_analysis → pump_short

## Целевая структура pump_short

```
pump_short/
  trading/
  short_pump/
  analytics/       # + load, report_blocks (ex shared_analytics), executive_report
  telegram/        # NEW: bot, send_helpers
  scripts/         # + daily_tg_report, send_hourly_report, update_auto_risk_guard, ...
  common/
  docs/
  deploy/
  datasets/
  reports/
  artifacts/
  tests/
```

## Файловая карта миграции

| pump_short_analysis | → pump_short |
|--------------------|--------------|
| analysis/utils.py | analytics/utils.py |
| analysis/load.py | analytics/load.py |
| analysis/joins.py | analytics/joins.py |
| analysis/aggregates.py | analytics/aggregates.py |
| analysis/features.py | analytics/features.py |
| analysis/plots.py | analytics/plots.py |
| shared_analytics/stats.py | analytics/stats.py |
| shared_analytics/fast0_blocks.py | analytics/fast0_blocks.py |
| shared_analytics/short_pump_blocks.py | analytics/short_pump_blocks.py |
| shared_analytics/joins.py | (merged into analytics/joins.py — same API) |
| shared_analytics/executive_report.py | analytics/executive_report.py |
| shared_analytics/auto_risk_guard_metrics.py | analytics/auto_risk_guard_metrics.py |
| shared_analytics/fast0_risk_mode.py | analytics/fast0_risk_mode.py |
| scripts/tg_report_bot.py | telegram/report_bot.py + telegram/send_helpers.py |
| scripts/daily_tg_report.py | scripts/daily_tg_report.py (imports → analytics.*, telegram.*) |
| scripts/send_hourly_report.py | scripts/send_hourly_report.py |
| scripts/update_auto_risk_guard.py | scripts/update_auto_risk_guard.py |
| scripts/audit_*.py, smoke_*, etc. | scripts/ (selected; no duplicates) |

## Импорты после миграции

- `from analysis.X` → `from analytics.X`
- `from shared_analytics.X` → `from analytics.X`
- Bot: `from scripts.daily_tg_report import generate_compact_autotrading_report` → `from scripts.daily_tg_report` (unchanged path) or `from analytics.daily_report import generate_compact_autotrading_report` if we extract.

## Что остаётся в pump_short_analysis (legacy)

- Точная копия для архива: `archive/pump_short_analysis_legacy` или список файлов для архивирования.
- Не переносим: run_analysis.py, debug_smoke.py (можно вызывать из pump_short после миграции), tools/liq_probe.py (по необходимости), временные/артефакты.
