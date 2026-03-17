# Структура проекта Algo_traiding

После рефакторинга консолидации:

1. **cursor_short_pump/pump_short** — единый основной проект: торговля, аналитика, Telegram-бот отчётов, скрипты (report, audit, train), ML/dataset audit.
2. **pump_short_analysis** — legacy/архив; аналитика и Telegram перенесены в pump_short (см. pump_short/docs/REFACTOR_SUMMARY.md).

---

## 1. cursor_short_pump/pump_short (торговый репозиторий)

**Путь на VPS:** `/root/pump_short`

### Структура каталогов

```
pump_short/
├── common/                    # Общие модули
│   ├── io_dataset.py          # Запись events/trades/outcomes в dataset (get_dataset_dir, write_event_row и т.д.)
│   ├── dataset_schema.py      # Схема dataset v2/v3
│   └── outcome_tracker.py     # Трекинг исходов позиций
│
├── short_pump/                # Стратегия Short Pump
│   ├── server.py              # FastAPI: /pump, /status — приём webhook, запуск watcher
│   ├── watcher.py             # run_watch_for_symbol — основной цикл (klines, OI, entry)
│   ├── fast0_sampler.py       # FAST0: быстрый сэмплинг после pump (short_pump_fast0)
│   ├── signals.py             # Формирование сигналов, enqueue_signal
│   ├── outcome.py             # track_outcome_short (TP/SL по свечам)
│   ├── telegram.py            # Уведомления в Telegram
│   ├── liquidations.py        # WebSocket ликвидаций Bybit
│   ├── bybit_api.py           # API Bybit
│   ├── config.py              # Конфиг short_pump
│   └── runtime.py             # Runtime, apply_filters, start_watch
│
├── long_pullback/             # Стратегия Long Pullback (отдельная)
│   ├── watcher.py
│   ├── entry.py
│   └── config.py
│
├── trading/                   # Ядро торговли
│   ├── runner.py              # --once: читает очередь, открывает позиции
│   ├── broker.py              # Абстрактный broker, BybitLiveBroker
│   ├── bybit_live.py          # BybitLiveBroker: leverage, market order, trading stop
│   ├── bybit_live_outcome.py  # Live outcome resolver (closed pnl Bybit)
│   ├── paper.py               # Paper-режим
│   ├── paper_outcome.py       # close_from_outcome, close_from_live_outcome
│   ├── outcome_worker.py      # Обработка исходов
│   ├── config.py
│   ├── state.py               # trading_state.json, record_open/record_close
│   ├── signal_io.py           # Очередь сигналов (JSONL)
│   ├── queue.py               # Очередь
│   ├── risk.py                # Риск-контроль
│   └── instrument.py          # Инструменты
│
├── analytics/                 # Аналитика: load, stats, report blocks, executive_report, volume_report, joins, aggregates, features, plots
│   ├── volume_report.py
│   ├── load.py
│   ├── stats.py
│   ├── executive_report.py
│   └── ...
│
├── telegram/                  # Telegram: бот /report, /help; send_helpers (sendMessage, sendPhoto)
│   ├── send_helpers.py
│   └── report_bot.py
│
├── notifications/             # Уведомления
│   └── tg_format.py           # Форматирование для Telegram
│
├── scripts/                   # Скрипты и smoke-тесты
│   ├── daily_tg_report.py     # Ежедневный/compact отчёт в TG
│   ├── send_hourly_report.py  # Почасовой отчёт (subprocess daily_tg_report)
│   ├── update_auto_risk_guard.py
│   ├── report_factors.py      # Factor report CLI (stub)
│   ├── deploy_all.sh          # Деплой на VPS
│   ├── run_local.sh           # Локальный запуск
│   ├── run_paper_dry_run.py
│   ├── run_watch.py / run_watch_long.py
│   ├── migrate_trading_closes_header.py
│   ├── backfill_timeout_mfe_mae.py
│   ├── smoke_*.py             # Smoke-тесты (dataset, exec_mode, fast0, liq, и др.)
│   └── test_*.py              # Unit-тесты
│
├── deploy/systemd/            # Systemd
│   ├── pump-trading-runner.service / .timer
│   ├── pump-short-live.env.example
│   └── README.md
│
├── docs/                      # Документация
│   ├── ARCHITECTURE_BRIEF.md
│   ├── FAST0_AUTOTRADING_AUDIT_REPORT.md
│   ├── FAST0_LIQ_FILTER_DEPLOY.md
│   ├── UNIFIED_LIVE_OUTCOME_PLAN.md
│   └── README.md
│
├── datasets/                  # Данные (на VPS)
│   ├── date=YYYYMMDD/strategy=short_pump|short_pump_fast0|long_pullback/mode=paper|live/
│   │   ├── events_v3.csv, trades_v3.csv, outcomes_v3.csv
│   ├── signals_queue.jsonl
│   ├── trading_state.json
│   ├── trading_closes.csv
│   └── trading_trades.csv
│
└── logs/                      # Логи
```

### Поток данных (кратко)

1. Webhook POST `/pump` → server → Runtime → watcher / fast0
2. Watcher/fast0: entry → event → signal → enqueue_signal → queue
3. Runner (timer): queue → broker.open_position → state, trading_trades
4. Outcome: TP/SL (paper: candle, live: Bybit) → trading_closes, outcomes_v3

---

## 2. pump_short_analysis (аналитический репозиторий)

**Путь на VPS:** `/opt/pump_short_analysis`

### Структура каталогов

```
pump_short_analysis/
├── analysis/                  # Загрузка и анализ данных
│   ├── load.py                # load_outcomes, load_events_v2
│   ├── aggregates.py          # Агрегация метрик
│   ├── features.py            # Фичи, enrich
│   ├── joins.py               # Джойны events ↔ outcomes
│   ├── plots.py               # Графики
│   └── utils.py
│
├── shared_analytics/          # Общие блоки отчётов
│   ├── stats.py               # _format_filter_line, _safe_stats_core
│   ├── fast0_blocks.py        # FAST0: liq, what-if TP, edge decay, ACTIVE MODE
│   ├── short_pump_blocks.py   # filter_active_trades (stage4 + dist>=3.5)
│   └── joins.py
│
├── scripts/                   # Скрипты
│   ├── daily_tg_report.py     # Ежедневный отчёт в Telegram (short_pump, short_pump_fast0)
│   ├── send_hourly_report.py  # Почасовой отчёт FAST0
│   ├── migrate_fast0_paper_to_live.py  # Миграция paper→live (short_pump, short_pump_fast0)
│   ├── smoke_fast0_report.py  # Smoke: отчёт FAST0
│   ├── smoke_events_no_nan.py # Smoke: проверка events CSV
│   ├── clean_fast0_outcomes.py
│   ├── sanity_check.py
│   └── quick_report_outcomes_v2.py
│
├── tools/
│   └── liq_probe.py
│
├── deploy/systemd/            # Systemd отчётов и smoke
│   ├── pump-short-daily-report.service / .timer
│   ├── pump-short-hourly-report-fast0.service / .timer
│   ├── pump-short-smoke.service / .timer
│   ├── pump-short-smoke-fast0.service / .timer
│   └── README.md
│
├── docs/
│   └── SHORT_PUMP_REPORT_AUDIT.md
│
├── run_analysis.py            # Основной скрипт анализа
├── debug_smoke.py
└── README.md
```

### Отчёты

- **daily_tg_report.py** — Daily/инвесторский отчёт: MDD, WR, EV, ACTIVE MODE (stage4+dist), RAW, гипотезы
- **send_hourly_report.py** — Почасовой FAST0-отчёт
- **run_analysis.py** — Анализ outcomes, агрегаты по бакетам

---

## 3. Деплой

**Скрипт:** `pump_short/scripts/deploy_all.sh`

```bash
cd /root/pump_short
bash scripts/deploy_all.sh [--smoke]
```

Обновляет оба репозитория, опционально запускает smoke, перезапускает сервисы.

**Сервисы:**

| Сервис | Репозиторий | Назначение |
|--------|-------------|------------|
| pump-short.service | pump_short | Торговый runner (или основной процесс) |
| pump-short-live-auto.service | pump_short | Авто-торговля |
| pump-trading-runner.service | pump_short | Runner --once (по таймеру) |
| pump-short-daily-report | pump_short_analysis | Ежедневный TG-отчёт |
| pump-short-hourly-report-fast0 | pump_short_analysis | Почасовой FAST0 TG |
| pump-short-smoke / pump-short-smoke-fast0 | pump_short_analysis | Smoke events |

---

## 4. Данные (datasets)

Общая схема:

```
datasets/
  date=YYYYMMDD/
    strategy=short_pump|short_pump_fast0|long_pullback/
      mode=paper|live/
        events_v3.csv
        trades_v3.csv
        outcomes_v3.csv
```

- **events_v3** — события (stage, dist_to_peak_pct, context_score, liq_long_usd_30s)
- **trades_v3** — сделки (entry, TP, SL, mfe_pct, mae_pct)
- **outcomes_v3** — исходы (TP_hit, SL_hit, TIMEOUT, pnl_pct, hold_seconds)

---

## 5. Стратегии

| Стратегия | Описание |
|-----------|----------|
| short_pump | Short pump: stage 3/4, dist_to_peak, TG filter (stage4 + dist>=3.5) |
| short_pump_fast0 | Быстрое окно после pump, liq 5k–25k, TP/SL |
| long_pullback | Long pullback (отдельная логика) |

---

*Документ сформирован автоматически. Актуальность: 2026-02.*
