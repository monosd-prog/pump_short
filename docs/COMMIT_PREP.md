# Подготовка к git push перед деплоем

## 1. Состояние git

- **Ветка:** main, up to date с origin/main
- **Конфликтов по trading/paper_outcome.py нет:** файл в репозитории не отслеживается, в рабочей директории отсутствует (есть trading/paper.py)
- **Staged:** ничего
- **Modified:** analytics/__init__.py
- **Untracked:** только релевантные файлы рефакторинга и factor report (список ниже)

## 2. Файлы, которые ИДУТ в коммит

Только эти пути (рефакторинг analytics/telegram, factor report, /report_factors, send_document, docs):

```
analytics/__init__.py
analytics/aggregates.py
analytics/auto_risk_guard_metrics.py
analytics/executive_report.py
analytics/factor_report.py
analytics/fast0_blocks.py
analytics/fast0_risk_mode.py
analytics/features.py
analytics/joins.py
analytics/load.py
analytics/plots.py
analytics/short_pump_blocks.py
analytics/stats.py
analytics/utils.py
docs/COMMIT_PREP.md
docs/MIGRATION_MAP.md
docs/REFACTOR_SUMMARY.md
scripts/daily_tg_report.py
scripts/report_factors.py
scripts/send_hourly_report.py
scripts/update_auto_risk_guard.py
telegram/__init__.py
telegram/report_bot.py
telegram/send_helpers.py
```

## 3. Файлы, которые НЕ идут в коммит

- reports/* (если появятся — не добавлять)
- datasets/* (уже в .gitignore)
- artifacts/* (не добавлять)
- logs/* (в .gitignore)
- .env, *.env, .env.bak* (в .gitignore / не добавлять)
- Временные csv/txt/json вне перечня выше
- Случайные локальные/мусорные файлы
- trading/*, short_pump/* (runtime) — не менялись в этом коммите

## 4. Команды

```bash
cd /Users/sergey/Desktop/Проекты/До\ движения/Algo_traiding/cursor_short_pump/pump_short

# Добавить только выбранные файлы
git add \
  analytics/__init__.py \
  analytics/aggregates.py \
  analytics/auto_risk_guard_metrics.py \
  analytics/executive_report.py \
  analytics/factor_report.py \
  analytics/fast0_blocks.py \
  analytics/fast0_risk_mode.py \
  analytics/features.py \
  analytics/joins.py \
  analytics/load.py \
  analytics/plots.py \
  analytics/short_pump_blocks.py \
  analytics/stats.py \
  analytics/utils.py \
  docs/COMMIT_PREP.md \
  docs/MIGRATION_MAP.md \
  docs/REFACTOR_SUMMARY.md \
  scripts/daily_tg_report.py \
  scripts/report_factors.py \
  scripts/send_hourly_report.py \
  scripts/update_auto_risk_guard.py \
  telegram/__init__.py \
  telegram/report_bot.py \
  telegram/send_helpers.py

git status
# Проверить: только перечисленные файлы в "Changes to be committed"

git commit -m "Analytics + Telegram refactor, factor report, /report_factors

- Consolidate analytics: load, stats, report blocks, executive_report, joins, aggregates, features, plots, utils; load_trades_v3
- Add factor_report.py: outcomes→trades→events join, EV proxy, single/delta/symbol/regime/combos/candidates, MIN_N_COMBO/MIN_N_CANDIDATE
- Add telegram/: send_helpers (send_message, send_photo, send_document), report_bot (/report, /report_factors, /help)
- Scripts: daily_tg_report, send_hourly_report, update_auto_risk_guard, report_factors CLI
- Docs: MIGRATION_MAP.md, REFACTOR_SUMMARY.md
- No changes to trading/broker/guard runtime"

git push
```

## 5. Краткий diff summary

- **Новые:** analytics (aggregates, auto_risk_guard_metrics, executive_report, factor_report, fast0_blocks, fast0_risk_mode, features, joins, load, plots, short_pump_blocks, stats, utils), telegram (__init__, report_bot, send_helpers), scripts (daily_tg_report, report_factors, send_hourly_report, update_auto_risk_guard), docs (MIGRATION_MAP, REFACTOR_SUMMARY)
- **Изменён:** analytics/__init__.py (комментарий о составе пакета)
- **Без изменений:** trading/*, short_pump/*, common/*, deploy/*, notifications/*
