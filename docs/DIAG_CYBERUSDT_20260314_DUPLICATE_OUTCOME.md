# Расследование: дубль OUTCOME для live short_pump (CYBERUSDT 20260314_030255)

**Кейс:** symbol=CYBERUSDT, run_id=20260314_030255, strategy=short_pump, risk_profile=short_pump_active_1R

**Факт:** 2 OUTCOME сообщения:
1. OUTCOME TIMEOUT #FAST — 04:18:25 (от watcher)
2. OUTCOME TIMEOUT #LIVE — 04:33:36 (от close_on_timeout)

---

## 1. Root cause

**Watcher** (API process) и **Runner** (trading process) — разные процессы с разным env:

| Процесс | EXECUTION_MODE | AUTO_TRADING_ENABLE |
|---------|----------------|---------------------|
| API (watcher) | paper (или не задан) | 1 |
| Runner | live | 1 |

`_watcher_should_skip_outcome_live` проверяет только `EXECUTION_MODE == "live"`. В API это False → skip не срабатывает → watcher запускает `track_outcome_short` (candle) и отправляет OUTCOME #FAST.

Watcher при отправке outcome **не вызывает** `add_outcome_tg_sent`. Только `_send_live_outcome_telegram` в paper_outcome добавляет ключ. Поэтому `close_on_timeout` в runner не видит outcome_tg_sent и отправляет второй OUTCOME #LIVE.

---

## 2. Цепочка

```
1. Webhook /pump → API → run_watch_for_symbol (thread)
2. Watcher: ENTRY_OK → TRADEABLE → enqueue_signal
3. Runner: dequeue → open_position (LIVE) → record_open
4. Watcher: track_outcome_short → TIMEOUT
5. Watcher: _watcher_should_skip_outcome_live → False (EXECUTION_MODE!=live в API)
6. Watcher: send_telegram(format_outcome) → OUTCOME #FAST 04:18:25
7. Watcher: НЕ вызывает add_outcome_tg_sent
8. Runner (04:33:36): close_on_timeout → position превысил TTL
9. close_on_timeout: outcome_tg_sent(key) → False
10. close_on_timeout: _send_live_outcome_telegram(res="TIMEOUT") → OUTCOME #LIVE 04:33:36
```

---

## 3. Отличие от предыдущего фикса (TP/SL)

Предыдущий фикс блокировал дубли для **outcome_worker** (TP_hit, SL_hit, EARLY_EXIT) через `outcome_tg_sent`. Но **TIMEOUT** для live приходит не от outcome_worker, а от **close_on_timeout**. close_on_timeout проверяет outcome_tg_sent, но watcher при отправке #FAST не добавляет ключ → проверка не срабатывает.

---

## 4. Safe fix

Расширить `_watcher_should_skip_outcome_live`: если `AUTO_TRADING_ENABLE == True`, watcher должен пропускать outcome. При авто-торговле исход приходит от outcome_worker (TP/SL/EARLY_EXIT) или close_on_timeout (TIMEOUT).

```python
if AUTO_TRADING_ENABLE:
    return True, "auto_trading_outcome_via_runner"
```

---

## 5. Фикс (внесён)

**Файл:** `short_pump/watcher.py`

**Изменение:** В `_watcher_should_skip_outcome_live` добавлена проверка `AUTO_TRADING_ENABLE`. Если включена авто-торговля, watcher пропускает отправку outcome (исход будет от runner: outcome_worker или close_on_timeout).

```diff
+        if AUTO_TRADING_ENABLE:
+            return True, "auto_trading_outcome_via_runner"
```

---

## 6. Команды проверки на VPS

```bash
# State и outcome_tg_sent
python3 -c "
import json
from pathlib import Path
from trading.state import make_position_id
p = Path('datasets/trading_state.json')
if p.exists():
    d = json.load(p.open())
    key = make_position_id('short_pump', '20260314_030255', '20260314_030255_entry_fast', 'CYBERUSDT')
    sent = d.get('outcome_tg_sent') or []
    print('position_id key:', key[:80] + '...')
    print('in outcome_tg_sent:', any(key in s or 'CYBERUSDT' in s and '20260314_030255' in s for s in sent))
"

# Outcomes в datasets
grep -r "20260314_030255" datasets/date=*/strategy=short_pump/mode=live/outcomes_v3.csv 2>/dev/null
grep -r "CYBERUSDT" datasets/date=20260314/strategy=short_pump/mode=live/outcomes_v3.csv 2>/dev/null

# Closes
grep -E "20260314_030255|CYBERUSDT" datasets/trading_closes*.csv 2>/dev/null

# Логи watcher
grep -E "20260314_030255|CYBERUSDT|WATCHER_OUTCOME|skip" logs/logs_short/2026-03-14/CYBERUSDT.log 2>/dev/null | head -50

# Логи runner / close_on_timeout
journalctl -u pump-trading-runner --since "2026-03-14 04:00" --until "2026-03-14 05:00" 2>/dev/null | grep -E "CYBERUSDT|20260314_030255|close_on_timeout|OUTCOME"
```
