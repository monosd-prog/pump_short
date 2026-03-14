# Расследование: LIVE_OPEN пришёл, но позиции на бирже нет (CYBERUSDT 20260314_030255)

**Кейс:** symbol=CYBERUSDT, run_id=20260314_030255, strategy=short_pump, risk_profile=short_pump_active_1R

**Факт:** TG: TRADEABLE #FAST → LIVE_OPEN #LIVE → 2× OUTCOME TIMEOUT (04:18 #FAST, 04:33 #LIVE). Пользователь не видит позицию на бирже.

---

## 1. Цепочка open → LIVE_OPEN

```
runner.run_once
  → broker.open_position(signal)
    → place_market_order (Bybit POST /v5/order/create)
    → _set_tpsl_with_retry (set_trading_stop)
    → get_open_position (для actual_entry, НЕ для проверки)
    → return position dict
  → record_open(state, position)
  → send_telegram(format_live_open_message)  ← LIVE_OPEN
  → save_state
```

---

## 2. Критическая точка: нет верификации позиции

`BybitLiveBroker.open_position` после `place_market_order`:
- вызывает `get_open_position` только для **actual_entry** (avgPrice)
- если `get_open_position` возвращает `None`, использует `signal.entry_price` и **всё равно возвращает position** с status="open"
- **не проверяет**, что позиция реально существует на бирже перед return

То есть LIVE_OPEN может уйти при:
1. `place_market_order` вернул orderId (ордер принят, но не обязательно исполнен)
2. `_set_tpsl_with_retry` успешен
3. `get_open_position` мог вернуть `None` — мы не падаем и не отменяем

---

## 3. Возможные сценарии

| Сценарий | Описание |
|----------|----------|
| **A. Позиция была, потом закрылась** | Открылась, TP/SL или ликвидация, пользователь смотрел после закрытия |
| **B. Ложный LIVE_OPEN** | Ордер принят, но не исполнен / отменён; верификации нет → LIVE_OPEN ушёл |
| **C. Быстрое закрытие** | Открылась, почти сразу закрылась (до проверки пользователем) |
| **D. close_on_timeout без close на бирже** | Для live TIMEOUT мы **не ставим** close-ордер на бирже — только снимаем из state и шлём TG. Позиция может остаться открытой на Bybit |

**Важно по D:** при live TIMEOUT `close_on_timeout` только `record_close`, пишет в datasets и шлёт OUTCOME TG. **На биржу close-ордер не отправляется.** Позиция может продолжать висеть на бирже.

---

## 4. Что дают 2 TIMEOUT

1. **04:18 #FAST** — watcher (candle-based), `track_outcome_short` → TIMEOUT
2. **04:33 #LIVE** — `close_on_timeout` (TTL), `_send_live_outcome_telegram(res="TIMEOUT")`

Если #LIVE пришёл от `close_on_timeout`, значит позиция была в `open_positions` до 04:33. То есть `record_open` сработал, позиция попала в state. Это не объясняет «позиции нет» — если только пользователь смотрел **до** 04:33 или закрытие на бирже произошло без нашего участия (TP/SL, ликвидация).

---

## 5. Команды проверки на VPS

```bash
cd /root/pump_short

# 1. State: была ли позиция, order_id, position_idx
python3 -c "
import json
from pathlib import Path
p = Path('datasets/trading_state.json')
if p.exists():
    d = json.load(p.open())
    op = d.get('open_positions', {}).get('short_pump', {})
    for pid, pos in op.items():
        if 'CYBERUSDT' in str(pos.get('symbol','')) or '20260314_030255' in str(pos.get('run_id','')):
            print('OPEN:', pid, pos)
    # Проверяем outcome_tg_sent
    sent = d.get('outcome_tg_sent') or []
    for s in sent:
        if 'CYBERUSDT' in s or '20260314_030255' in s:
            print('SENT:', s[:100])
"

# 2. Closes: close_reason, exit_price
grep -E "CYBERUSDT|20260314_030255" datasets/trading_closes*.csv 2>/dev/null

# 3. Outcomes
grep -E "CYBERUSDT|20260314_030255" datasets/date=20260314/strategy=short_pump/mode=live/outcomes_v3.csv 2>/dev/null

# 4. Trading trades (entry)
grep -E "CYBERUSDT|20260314_030255" datasets/date=20260314/strategy=short_pump/mode=live/trades_v3.csv 2>/dev/null

# 5. Логи runner: LIVE_ORDER_PLACE, LIVE_ACCEPTED, LIVE_ENTRY_ACTUAL, LIVE_POSITION
journalctl -u pump-trading-runner --since "2026-03-14 03:00" --until "2026-03-14 05:00" --no-pager 2>/dev/null | grep -E "CYBERUSDT|20260314_030255|LIVE_ORDER_PLACE|LIVE_ACCEPTED|LIVE_ENTRY_ACTUAL|LIVE_POSITION|LIVE_TPSL"

# 6. Bybit order/position (если доступны логи API)
journalctl -u pump-trading-runner --since "2026-03-14 03:00" --until "2026-03-14 05:00" --no-pager 2>/dev/null | grep -E "orderId|positionIdx|LIVE_ORDER"
```

---

## 6. Потенциальная проблема UX/логики

**LIVE_OPEN отправляется без проверки наличия позиции на бирже.**

Текущий поток:
- `place_market_order` возвращает orderId (ордер принят)
- `_set_tpsl_with_retry` вызывается
- `get_open_position` используется только для avgPrice, не для проверки существования
- При `get_open_position is None` мы всё равно считаем open успешным и шлём LIVE_OPEN

**Рекомендация:** перед отправкой LIVE_OPEN проверять `get_open_position(symbol, side)` и считать open успешным только если позиция есть. При `None` — не писать в state и не слать LIVE_OPEN (или логировать предупреждение и не слать TG).

---

## 7. Файлы для проверки

| Файл | Роль |
|------|------|
| `trading/bybit_live.py` | `open_position`, `place_market_order`, `get_open_position` |
| `trading/runner.py` | `record_open`, LIVE_OPEN TG |
| `trading/paper_outcome.py` | `close_on_timeout`, `_send_live_outcome_telegram` |

---

## 8. Итог расследования

1. **Позиция реально была:** close_on_timeout сработал в 04:33 — позиция была в state. Скорее всего, она открывалась.
2. **Позиция не открылась:** маловероятно при наличии LIVE_ACCEPTED и двух OUTCOME — state фиксировал позицию.
3. **Пользователь смотрел после 04:33:** close_on_timeout не закрывает позицию на бирже при TIMEOUT — только снимает из state. Если позиция к тому моменту уже закрылась (TP/SL/ликвидация), на бирже её не будет.
4. **Ранний LIVE_OPEN:** подтверждение позиции через `get_open_position` не требуется — возможен ложный LIVE_OPEN при асинхронном исполнении/отмене ордера.

После выполнения команд на VPS можно уточнить, был ли `LIVE_ENTRY_ACTUAL` в логах (значит `get_open_position` нашёл позицию) или только `LIVE_ORDER_PLACE`/`LIVE_ACCEPTED`.

---

## 9. Fix: подтверждение позиции перед LIVE_OPEN

### Root cause

LIVE_OPEN отправлялся сразу после возврата `broker.open_position()`, без проверки, что позиция реально появилась в списке позиций Bybit. `get_open_position` вызывался только для avgPrice; при `None` код всё равно возвращал position с status="open" и runner слал LIVE_OPEN.

### Source of truth

**Критерий подтверждения:** `get_open_position(symbol, side)` возвращает dict с **size > 0** (позиция в списке позиций Bybit). Надёжные признаки в логах: `LIVE_POSITION` (логируется при нахождении позиции), `LIVE_ENTRY_ACTUAL` (avgPrice взят с биржи). Если в логах нет LIVE_ENTRY_ACTUAL / LIVE_POSITION для кейса — позиция не была подтверждена; при старом коде LIVE_OPEN мог уйти без подтверждения.

### Изменённые файлы

| Файл | Изменение |
|------|-----------|
| `trading/bybit_live.py` | В `open_position`: после `get_open_position` (с одной повторной попыткой через 1 с при `pos is None`) выставляется `confirmed_on_exchange = (pos is not None and float(pos.get("size") or 0) > 0)`, флаг добавляется в возвращаемый position dict. |
| `trading/runner.py` | Перед отправкой LIVE_OPEN: если `not position.get("confirmed_on_exchange", True)` — не слать TG, логировать `LIVE_OPEN_SKIPPED_UNCONFIRMED` (strategy, symbol, position_id). |

### Поведение после fix

| Аспект | До | После |
|--------|-----|--------|
| **TG** | LIVE_OPEN всегда при успешном open_position. | LIVE_OPEN только при `confirmed_on_exchange=True`. При неподтверждённой позиции — в логах `LIVE_OPEN_SKIPPED_UNCONFIRMED`, в TG ничего. |
| **State** | `record_open(state, position)` как раньше. | Без изменений: позиция по-прежнему пишется в state (outcome_worker и close_on_timeout видят её). |
| **Datasets** | Запись открытия/закрытия как раньше. | Без изменений. |
| **TIMEOUT path** | close_on_timeout срабатывает по state. | Без изменений: неподтверждённая позиция всё равно в state → таймаут по TTL возможен; мы только не слали ложный LIVE_OPEN. |

Орфанных позиций на бирже без state не создаём: ордер уже отправлен; при неподтверждении мы лишь не шлём LIVE_OPEN в TG.

### Команды проверки на VPS после деплоя

```bash
cd /root/pump_short  # или актуальный путь проекта

# 1. Убедиться, что код с флагом confirmed_on_exchange задеплоен
grep -n "confirmed_on_exchange" trading/bybit_live.py trading/runner.py

# 2. При следующем открытии: в логах либо LIVE_OPEN (есть LIVE_POSITION/LIVE_ENTRY_ACTUAL), либо LIVE_OPEN_SKIPPED_UNCONFIRMED
journalctl -u pump-trading-runner -f --no-pager 2>/dev/null | grep -E "LIVE_OPEN_SKIPPED_UNCONFIRMED|LIVE_ENTRY_ACTUAL|LIVE_POSITION|LIVE_OPEN_TG"

# 3. Для кейса CYBERUSDT (если логи ещё доступны): наличие/отсутствие LIVE_ENTRY_ACTUAL и LIVE_POSITION
journalctl -u pump-trading-runner --since "2026-03-14 03:00" --until "2026-03-14 05:00" --no-pager 2>/dev/null | grep -E "CYBERUSDT|20260314_030255|LIVE_ENTRY_ACTUAL|LIVE_POSITION"
```

### Краткий diff

- **bybit_live.py**  
  - Сохраняем результат `get_open_position` в `pos`; при `pos is None` — `time.sleep(1)` и один повторный вызов `get_open_position`.  
  - `confirmed_on_exchange = (pos is not None and float(pos.get("size") or 0) > 0)`.  
  - В `position` добавлено поле `"confirmed_on_exchange": confirmed_on_exchange`.

- **runner.py**  
  - В блоке LIVE_OPEN: если `not position.get("confirmed_on_exchange", True)` — логируем `LIVE_OPEN_SKIPPED_UNCONFIRMED` и не вызываем `send_telegram`; иначе — прежняя отправка LIVE_OPEN.
