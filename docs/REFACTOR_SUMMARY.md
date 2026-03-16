# Консолидация: pump_short_analysis → pump_short

## 1. Краткий diff summary

- **Добавлено в pump_short:** пакет `analytics/` (load, joins, stats, fast0_blocks, short_pump_blocks, executive_report, auto_risk_guard_metrics, fast0_risk_mode, aggregates, features, plots, utils), пакет `telegram/` (send_helpers, report_bot), скрипты `scripts/daily_tg_report.py`, `scripts/send_hourly_report.py`, `scripts/update_auto_risk_guard.py`, `scripts/report_factors.py` (stub).
- **Импорты:** везде заменены `analysis.*` → `analytics.*`, `shared_analytics.*` → `analytics.*`. Точки входа вызывают код внутри pump_short.
- **Runtime (trading, broker, guard, execution):** не менялся.
- **pump_short_analysis:** не удалялся; оставлен как legacy (см. раздел 4).

---

## 2. Списки файлов

### Перенесены (содержимое скопировано и импорты обновлены)

| Из pump_short_analysis | В pump_short |
|------------------------|--------------|
| analysis/utils.py | analytics/utils.py |
| analysis/load.py | analytics/load.py |
| analysis/joins.py | analytics/joins.py |
| analysis/aggregates.py | analytics/aggregates.py |
| analysis/features.py | analytics/features.py |
| analysis/plots.py | analytics/plots.py |
| shared_analytics/stats.py | analytics/stats.py |
| shared_analytics/fast0_blocks.py | analytics/fast0_blocks.py |
| shared_analytics/short_pump_blocks.py | analytics/short_pump_blocks.py |
| shared_analytics/executive_report.py | analytics/executive_report.py |
| shared_analytics/auto_risk_guard_metrics.py | analytics/auto_risk_guard_metrics.py |
| shared_analytics/fast0_risk_mode.py | analytics/fast0_risk_mode.py |
| scripts/daily_tg_report.py | scripts/daily_tg_report.py |
| scripts/send_hourly_report.py | scripts/send_hourly_report.py |
| scripts/update_auto_risk_guard.py | scripts/update_auto_risk_guard.py |
| scripts/tg_report_bot.py (логика) | telegram/report_bot.py |

### Созданы

| Файл | Назначение |
|------|------------|
| telegram/__init__.py | Пакет telegram |
| telegram/send_helpers.py | send_message, send_photo для TG API |
| telegram/report_bot.py | Бот /report, /help (long-polling) |
| scripts/report_factors.py | Заглушка CLI для factor report |
| docs/MIGRATION_MAP.md | Карта миграции |
| docs/REFACTOR_SUMMARY.md | Этот отчёт |

### Изменены (только импорты)

| Файл | Изменения |
|------|-----------|
| analytics/joins.py | `analysis.utils` → `analytics.utils` |
| analytics/load.py | `analysis.utils` → `analytics.utils` |
| analytics/aggregates.py | `analysis.utils` → `analytics.utils` |
| analytics/features.py | `analysis.utils` → `analytics.utils` |
| scripts/daily_tg_report.py | `analysis.*` → `analytics.*`, `shared_analytics.*` → `analytics.*` |
| scripts/update_auto_risk_guard.py | `analysis.*` → `analytics.*`, `shared_analytics.*` → `analytics.*` |
| analytics/__init__.py | Комментарий о составе пакета |

---

## 3. Итоговая структура pump_short

```
pump_short/
  trading/
  short_pump/
  long_pullback/
  analytics/           # + load, stats, fast0_blocks, short_pump_blocks, executive_report,
                       #   auto_risk_guard_metrics, fast0_risk_mode, joins, aggregates, features, plots, utils
  telegram/            # NEW: send_helpers, report_bot
  scripts/             # + daily_tg_report, send_hourly_report, update_auto_risk_guard, report_factors (stub)
  common/
  notifications/
  docs/
  deploy/
  datasets/
  reports/
  artifacts/
  tests/
```

---

## 4. Что осталось в pump_short_analysis и почему

- **analysis/, shared_analytics/, scripts/** — перенесены в pump_short; в legacy остаются как архив/копия.
- **run_analysis.py, debug_smoke.py** — точки входа анализа; после миграции их можно вызывать из pump_short (обновив импорты на `analytics.*`) или оставить в legacy и вызывать из pump_short через subprocess с нужным PYTHONPATH.
- **tools/liq_probe.py** — утилита; при необходимости перенести в pump_short/scripts/ или tools/.
- **deploy/systemd/** — сервисы отчётов; в рамках рефакторинга не менялись; позже можно переключить на один deploy из pump_short.
- Рекомендация: не удалять pump_short_analysis сразу; либо скопировать в `archive/pump_short_analysis_legacy`, либо держать список файлов для архива и удалять после проверки работы отчётов и бота из pump_short.

---

## 5. Как запускать

Из **корня pump_short** (`cursor_short_pump/pump_short` или `/root/pump_short` на VPS):

```bash
# Factor report CLI (stub)
PYTHONPATH=. python3 scripts/report_factors.py [--data-dir PATH] [--days N]

# ML audit CLI
PYTHONPATH=. python3 scripts/audit_short_pump_ml_readiness.py
PYTHONPATH=. python3 scripts/audit_short_pump_ml_readiness.py --data-dir /root/pump_short/datasets

# Baseline train CLI
PYTHONPATH=. python3 scripts/train_short_pump_ml_baseline.py
PYTHONPATH=. python3 scripts/train_short_pump_ml_baseline.py --data-dir /root/pump_short/datasets

# Telegram report bot (/report, /help)
PYTHONPATH=. python3 -m telegram.report_bot
# или
PYTHONPATH=. python3 telegram/report_bot.py

# Ежедневный TG-отчёт (compact)
PYTHONPATH=. python3 scripts/daily_tg_report.py --root /root/pump_short/datasets --strategy short_pump --days 7 --compact

# Почасовой отчёт (subprocess к daily_tg_report)
PYTHONPATH=. python3 scripts/send_hourly_report.py [--strategy short_pump_fast0] [--days 0]
```

Переменные окружения (как и раньше): `TELEGRAM_BOT_TOKEN` / `TG_BOT_TOKEN`, `TELEGRAM_CHAT_ID` / `TG_CHAT_ID`, при необходимости `TG_REPORT_DATA_ROOT`, `PYTHONPATH=.` при запуске из корня pump_short.

---

## 6. Исправленные импорты

- В **analytics/** (load, joins, aggregates, features): `from analysis.utils` → `from analytics.utils`.
- В **scripts/daily_tg_report.py**: `from analysis.load` → `from analytics.load`; `from shared_analytics.stats` → `from analytics.stats`; то же для fast0_blocks, short_pump_blocks, executive_report.
- В **scripts/update_auto_risk_guard.py**: `from analysis.load` → `from analytics.load`; `from shared_analytics.auto_risk_guard_metrics` → `from analytics.auto_risk_guard_metrics`.
- **telegram/report_bot.py**: использует `from telegram.send_helpers import send_message` и `from scripts.daily_tg_report import generate_compact_autotrading_report` (без импортов из pump_short_analysis).

Старых импортов на pump_short_analysis в перенесённом коде не осталось.

---

## 7. Дубликаты и мёртвые модули

- **shared_analytics/joins.py** и **analysis/joins.py** имели одинаковый API (`join_outcomes_with_events`, `join_entries_outcomes`); в pump_short оставлен один модуль **analytics/joins.py** (из analysis/joins.py).
- Мёртвые модули в pump_short не вводились; старые скрипты в pump_short_analysis (audit_early_exit_conflicts, audit_fast0_risk_modes, debug_fast0_liq_bucket, migrate_fast0_paper_to_live, smoke_*, sanity_check, quick_report_outcomes_v2 и т.д.) можно по необходимости переносить выборочно или оставить в legacy.

---

## 8. Рекомендации по следующим шагам

- **Legacy:** сделать архив `archive/pump_short_analysis_legacy` или бэкап, затем после 1–2 недель стабильной работы отчётов и бота из pump_short можно удалить или отключить старый репозиторий.
- **report_factors:** реализовать в `analytics/factor_report.py` и в `scripts/report_factors.py` (TXT + JSON, summary + sendDocument в TG по желанию).
- **Единый deploy:** перевести systemd-сервисы отчётов (daily-report, hourly-report-fast0, smoke) на запуск из pump_short (WorkingDirectory=/root/pump_short, PYTHONPATH=.).
- **Notion / AI agents:** подключать к одному проекту pump_short (analytics + telegram уже в одном месте).
