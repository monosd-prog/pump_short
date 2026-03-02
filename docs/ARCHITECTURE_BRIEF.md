# Pump Short — Architectural Brief

## 1. Systemd Units and Timers

**pump-trading-runner.service** — oneshot service, запускает runner. ExecStart: `/root/pump_short/venv/bin/python -m trading.runner --once`. EnvironmentFile: `/root/pump_short/.env`. WorkingDirectory: `/root/pump_short`. Restart: on-failure.

**pump-trading-runner.timer** — таймер для периодического запуска runner. OnBootSec: 10, OnUnitActiveSec: 10 (каждые 10 секунд после предыдущего запуска). Persistent: true (пропущенные запуски догоняются после перезагрузки).

Других systemd-юнитов в репозитории нет. FastAPI-сервер (short_pump api) и watchers обычно запускаются вне systemd (uvicorn, скрипты run_watch).

**Env/EnvironmentFile:** основной конфиг — `.env` в корне проекта. Пример live-конфига: `deploy/systemd/pump-short-live.env.example` (BYBIT_*, EXECUTION_MODE, AUTO_TRADING_ENABLE, STRATEGIES, risk-параметры). Пути данных: DATASET_BASE_DIR, TRADING_STATE_PATH, SIGNALS_QUEUE_PATH, TRADING_CLOSES_PATH (по умолчанию в `datasets/`).

---

## 2. Data Pipeline

**Webhook / pump event → dataset:**

1. **Триггер.** Webhook POST /pump (`short_pump.server`) с `{symbol, exchange, pump_pct, pump_ts, extra}`. Runtime.apply_filters проверяет exchange, pump_pct, window. Runtime.start_watch создаёт run_id (YYYYMMDD_HHMMSS), запускает `run_watch_for_symbol` в asyncio-потоке.

2. **Watcher (short_pump).** `run_watch_for_symbol` циклично опрашивает Bybit (klines 5m/1m, OI, trades, funding). Структурный контекст (context5m) обновляет stage. Entry решает `decide_entry_fast` / `decide_entry_1m` (stage=4, dist_to_peak_pct и др.). При ENTRY_OK:
   - формируется event_id (например `{run_id}_outcome`);
   - пишется event в dataset;
   - строится Signal, вызывается enqueue_signal;
   - отправляется TG (если включено);
   - запускается track_outcome_short (candle-логика TP/SL).

3. **Fast0 (опционально).** Если ENABLE_FAST_FROM_PUMP=1, server параллельно запускает `run_fast0_for_symbol` в потоке. Fast0 — короткое окно после pump, пишет events/trades/outcomes в short_pump_fast0. event_id = `{run_id}_fast0_{tick}_{uuid8}`. При ENTRY_OK тоже вызывает enqueue_signal и поднимает outcome watcher.

4. **Queue.** `enqueue_signal` пишет JSONL в `datasets/signals_queue.jsonl`. Строка = один Signal (strategy, symbol, side, run_id, event_id, entry_price, tp_price, sl_price, ts_utc и др.).

5. **Runner.** Timer каждые 10 сек запускает `trading.runner --once`. Runner атомарно переименовывает queue в .processing, читает сигналы, выбирает один (по приоритету стратегий), проверяет risk, вызывает `broker.open_position`. Для live — BybitLiveBroker: set_leverage, place_market_order, set_trading_stop. При успехе: record_open в trading_state.json, _append_trade_row в trading_trades.csv.

6. **Outcome.** Paper: track_outcome (candles) в watcher/fast0 → close_from_outcome → record_close, _append_close_row, write_outcome_row. Live: outcome watcher в fast0 пытается resolve_live_outcome (Bybit closed-pnl); при успехе — close_from_live_outcome; при timeout — retry, затем UNKNOWN_LIVE без candle fallback.

**event_id / run_id:**
- run_id создаётся в Runtime.start_watch при /pump: `time.strftime("%Y%m%d_%H%M%S")`.
- event_id в short_pump watcher: `{run_id}_outcome` для итогового outcome; в fast0 — `{run_id}_fast0_{tick}_{uuid8}` на каждый tick.
- position_id = `{strategy}:{run_id}:{event_id}:{symbol}` (trading.state.make_position_id).

**Файлы и директории:**
- `datasets/date=YYYYMMDD/strategy={strategy}/mode={paper|live}/` — events_v3.csv, trades_v3.csv, outcomes_v3.csv.
- `datasets/trading_state.json` — open_positions[strategy][position_id], last_signal_ids.
- `datasets/trading_closes.csv` — закрытые бумажные/живые позиции.
- `datasets/signals_queue.jsonl` — очередь сигналов (и .processing, .processed).
- `datasets/trading_trades.csv` — лог открытий (runner).
- `datasets/trading_runner.lock` — блокировка runner.

---

## 3. Компоненты и ответственность

**short_pump.server** — FastAPI: /pump, /status. Принимает pump-события, фильтрует, запускает watcher и при необходимости fast0. Не решает entry и не ставит ордера.

**short_pump.watcher (run_watch_for_symbol)** — основной цикл short_pump: опрос данных, обновление структуры, решение entry (decide_entry_fast/decide_entry_1m). При ENTRY_OK пишет event/trade в dataset, enqueue_signal, track_outcome_short. Закрывает paper-позиции через close_from_outcome. Регистрирует символ в liquidations WS.

**short_pump.fast0_sampler** — быстрый сэмплинг после pump: короткое окно (FAST0_WINDOW_SEC), пишет events/trades. Entry для fast0 — should_fast0_entry_ok (liq_long_usd_30s, context_score и др.). При ENTRY_OK — enqueue_signal, daemon-поток _run_fast0_outcome_watcher. Для live — Bybit outcome resolver с retry; для paper — track_outcome + close_from_outcome.

**short_pump.liquidations** — WebSocket Bybit allLiquidation. start_liquidation_listener в Runtime. register_symbol/unregister_symbol управляют подпиской. get_liq_stats даёт агрегаты за окно (count, usd). Используется watcher и fast0 для gating (liq_long_usd_30s и др.).

**trading.runner** — читает очередь, выбирает один сигнал, risk-проверки, вызывает broker.open_position. Записывает open в state, пишет в trading_trades. Вызывает close_on_timeout для TTL.

**trading.bybit_live** — BybitLiveBroker: set_leverage, place_market_order, set_trading_stop, get_closed_pnl, get_open_position. open_position возвращает position dict с order_id, position_idx. TPSL retry при 10001, close при повторном fail.

**trading.state** — load_state/save_state, record_open/record_close, make_position_id. Единое хранилище open_positions и last_signal_ids.

**trading.paper_outcome** — close_from_outcome (paper: TP/SL от candle), close_from_live_outcome (live: Bybit exit_price/pnl), close_on_timeout, _write_live_outcome_to_datasets. Работает с state и trading_closes.csv.

**Кто решает entry:** watcher (decide_entry_fast/decide_entry_1m для short_pump), fast0 (should_fast0_entry_ok). Кто ставит ордер: только runner через broker.open_position. Кто фиксирует outcome: watcher/fast0 через track_outcome (candle) или resolve_live_outcome (Bybit); close_from_outcome/close_from_live_outcome обновляют state и dataset.

---

## 4. Точки интеграции и shared contracts

**Обязательные поля:**
- Signal: strategy, symbol, side, run_id, event_id, entry_price, tp_price, sl_price, ts_utc. Для live также нужны order_id, position_idx в position после open.
- Position: strategy, symbol, side, entry, tp, sl, run_id, event_id, mode, order_id, position_idx (для live outcome resolver).
- position_id = `{strategy}:{run_id}:{event_id}:{symbol}`.

**Возможные рассинхроны:**
1. **Live outcome vs paper outcome.** Candle-логика в track_outcome может выдать TP/SL, не совпадающий с реальным Bybit close. Outcome watcher в live сначала пытается Bybit; при timeout раньше делал candle fallback и закрывал paper — это исправлено: retry, затем UNKNOWN_LIVE без candle fallback.
2. **Runner vs outcome watcher.** Runner пишет position в state после open. Outcome watcher ждёт position с order_id до 60 сек. Если runner не успел записать (частота timer, lock), outcome watcher уйдёт в fallback no_position.
3. **Разные event_id.** short_pump watcher использует `{run_id}_outcome`, fast0 — `{run_id}_fast0_{tick}_{uuid}`. position_id и поиск позиции зависят от согласованности run_id/event_id между enqueue и record_open.

---

## 5. Рекомендации по унификации

**Single source of truth для сделки:** Runner и outcome должен опираться на один источник — exchange (Bybit) для live. Position в state — кэш с order_id/position_idx; outcome — только из Bybit closed-pnl/executions. Candle-логика — только для paper и как fallback при отсутствии order_id.

**Кому принадлежит outcome в live:** Outcome в live должен определяться исключительно Bybit (closed-pnl, executions). Детекторы (watcher, fast0) не должны писать live outcome по свечам. Имеет смысл выделить отдельный outcome worker, который по state (open_positions с order_id) опрашивает Bybit и при закрытии вызывает close_from_live_outcome.

**Что оставить в детекторах, что перенести:** В детекторах оставить: формирование Signal, enqueue, запись events/trades в dataset, TG. Entry decision остаётся в детекторах. Перенести в runner/outcome worker: (1) ожидание и повторные попытки resolve по Bybit для live; (2) единый цикл "проверить открытые live-позиции, запросить closed-pnl, обновить outcome" — можно в runner или отдельном timer-based сервисе.
