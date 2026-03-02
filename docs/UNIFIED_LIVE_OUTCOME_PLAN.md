# Unified Live Outcome — план и список правок

## 1. State открытых позиций

**Хранение:** `trading.config.STATE_PATH` → `datasets/trading_state.json` (env: `TRADING_STATE_PATH`).

**Формат:** `open_positions[strategy][position_id] = position`.

**position_id:** `{strategy}:{run_id}:{event_id}:{symbol}` (trading.state.make_position_id, строка 17–18).

**Запись позиции (ключи):** strategy, symbol, side, entry, sl, tp, opened_ts, notional_usd, risk_usd, leverage, status, run_id, event_id, trade_id, mode, order_id, position_idx.

**Где пишется при LIVE_ACCEPTED:** `trading.runner._run_once_body` после `broker.open_position()` (строка ~448): `pid = record_open(state, position)` в trading.state.record_open (строка 127–143). Позиция формируется в `trading.bybit_live.BybitLiveBroker.open_position` (строки ~670–693).

**Явного поля status/ts_open нет.** Используются: opened_ts, status="open". Для pending outcome можно добавить `last_outcome_check_ts`, `outcome_retry_count`.

---

## 2. Outcome Worker (minimal design)

**Идея:** на каждом тике runner (или отдельный timer) обходить open_positions с mode=live и order_id/position_idx, вызывать resolve_live_outcome; при успехе — close_from_live_outcome; при сетевой ошибке/timeout — не закрывать.

**Критерий "position not found on exchange":** `broker.get_open_position(symbol, side)` возвращает None (нет позиции с size>0), при этом closed-pnl за окно после opened_ts уже содержит запись с тем же symbol/side и avgEntryPrice ~ entry. То есть позиция закрыта, но resolver её нашел. Если closed-pnl пуст и get_open_position пуст — это либо ещё открыта (тогда resolver вернёт None), либо закрыта не нашим TP/SL (ручное закрытие). Критерий NOT_FOUND: get_open_position(symbol, side)==None **и** resolve_live_outcome вернул None после timeout, **и** opened_ts старше N минут (например 60) — тогда считаем "позиция закрыта вне нашей логики, надо синхронизировать state". Альтернатива: NOT_FOUND = closed-pnl пуст + get_open_position пуст + age > N min. Без closed-pnl — считаем pending, не трогаем.

---

## 3. Точные места изменений

**Добавить проход по open positions (Outcome Worker):**

- `trading.runner._run_once_body` — в начале (после load_state, до get_latest_signals) или в конце, после close_on_timeout: вызвать новую функцию `run_outcome_worker(state, broker)` при EXECUTION_MODE==live.
- Либо новый модуль `trading.outcome_worker` с функцией `run_outcome_worker(state, broker) -> None`, импортируемой в runner.

**Новые функции:**

- `trading.outcome_worker.run_outcome_worker(state, broker)` — итерация по open_positions, фильтр mode==live и наличие order_id/position_idx; для каждой позиции вызов resolve_live_outcome (timeout_sec=LIVE_OUTCOME_RETRY_INTERVAL_SEC, raise_on_network_error=False чтобы не падать по сети); при result — close_from_live_outcome; логи OUTCOME_WORKER_TICK, OUTCOME_RESOLVED, OUTCOME_PENDING, OUTCOME_NOT_FOUND.

**Расширение position (опционально):**

- `trading.state` — при record_open ничего не менять. При необходимости хранить last_outcome_check_ts / outcome_retry_count — добавить в position dict при первой проверке в outcome_worker и сохранять через save_state. Альтернатива: держать в отдельном ключе state, например `pending_outcomes: {position_id: {last_check_ts, retry_count}}`.

**Логи:**

- OUTCOME_WORKER_TICK — в run_outcome_worker при старте прохода (n_live_open=…).
- OUTCOME_RESOLVED — при успешном close_from_live_outcome (уже есть LIVE_OUTCOME_RESOLVED в fast0; в worker можно дублировать или использовать OUTCOME_WORKER_RESOLVED).
- OUTCOME_PENDING — позиция есть, resolve вернул None, сеть/timeout; оставляем open.
- OUTCOME_NOT_FOUND — позиция закрыта на бирже (get_open_position пуст), closed-pnl пуст или не удалось определить — после age > N min помечаем/закрываем как "unknown" и делаем record_close с reason=outcome_not_found.

**Файлы:**

- Новый: `trading/outcome_worker.py` — run_outcome_worker, maybe resolve_and_close_live_position(position, broker) -> resolved|pending|not_found.
- Изменить: `trading.runner` — импорт run_outcome_worker, вызов в _run_once_body при EXECUTION_MODE==live (после close_on_timeout, до или после основной логики сигналов).

---

## 4. Убрать/ограничить в watcher и fast0

**short_pump.fast0_sampler._run_fast0_outcome_watcher (строки 206–364):**

- В live при no_position / no_order_id (строки 355–361): сейчас идёт fallback на track_outcome (candle). Рекомендация: не вызывать track_outcome для live вообще. Вместо fallback логировать UNKNOWN_LIVE_NOT_RESOLVED и return (pending). Outcome worker потом подхватит, если позиция появится в state с order_id.
- Если позиция так и не появится (runner не записал) — это не live-сделка, candle fallback не нужен. Безопаснее: UNKNOWN_LIVE_NOT_RESOLVED, без записи outcome в dataset.

**short_pump.watcher (run_watch_for_symbol):**

- Сейчас track_outcome_short вызывается для всех режимов; close_from_outcome — только при MODE==paper (строки 1591–1612). Для live: candle outcome пишется в dataset, но state не закрывается — рассинхрон. Рекомендация (опционально в шаге 2): для live не писать candle outcome в dataset; outcome только через worker. Файл: short_pump/watcher.py, блок ~1383–1612 (track_outcome_short + close_from_outcome).

**Итого для fast0 в live:**

- Полностью убрать вызов track_outcome при mode==live.
- При no_position/no_order_id — не fallback, а UNKNOWN_LIVE_NOT_RESOLVED + return.
- Вся логика live outcome — только через Bybit (resolve_live_outcome) и outcome worker.

---

## 5. Migration plan (3 шага)

**Шаг 1 — Outcome Worker и закрытие live по бирже**

- Создать `trading/outcome_worker.py` с run_outcome_worker.
- В `trading.runner._run_once_body` при EXECUTION_MODE==live вызывать run_outcome_worker после close_on_timeout.
- Worker: пройти open_positions (mode=live, order_id, position_idx), resolve_live_outcome, при успехе — close_from_live_outcome.
- Логи: OUTCOME_WORKER_TICK, OUTCOME_RESOLVED, OUTCOME_PENDING.

**Шаг 2 — Запрет candle fallback в live**

- В `short_pump.fast0_sampler._run_fast0_outcome_watcher`: при mode==live не вызывать track_outcome ни при каких условиях.
- При no_position/no_order_id — логировать UNKNOWN_LIVE_NOT_RESOLVED и return.
- Блок с track_outcome (строка 365) выполнять только если mode != live (paper).

**Шаг 3 — Алерт/метрика по pending**

- В run_outcome_worker при OUTCOME_PENDING: считать age = now - opened_ts.
- Если age > N минут (env OUTCOME_PENDING_ALERT_MINUTES, default 60): логировать OUTCOME_PENDING_OLD | position_id=... age_min=... symbol=... run_id=....
- Опционально: писать в state pending_outcomes[position_id] = {last_check_ts, age_min} для метрик/дашборда.
- При NOT_FOUND (get_open_position пуст, closed-pnl пуст, age > N): record_close с reason=outcome_not_found_sync, чтобы не плодить вечные pending.
