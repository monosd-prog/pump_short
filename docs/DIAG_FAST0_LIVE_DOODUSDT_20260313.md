# Диагностика: FAST0 live DOODUSDT run_id=20260313_151135

**Кейс:** TRADEABLE пришёл в TG, позиция на бирже открылась и закрылась в плюс, но LIVE_OPEN и OUTCOME не пришли.

## Цепочка данных (что проверять по порядку)

### 1. TRADEABLE → Enqueue → Runner

```
FAST0 ENTRY_OK (fast0_sampler)
  → format_tg(sig) = "⚡ FAST0 TRADEABLE | DOODUSDT | dist=0.27% | cs=0.60 | liqL30s=5880 ..."
  → send_telegram (если FAST0_TG_ENTRY_ENABLE)
  → enqueue_signal(sig) (если AUTO_TRADING_ENABLE)
  → _run_fast0_outcome_watcher (daemon thread)
```

**Файлы:** `datasets/signals_queue.jsonl` — должна быть строка с run_id=20260313_151135.

### 2. Runner → LIVE_ACCEPTED → record_open → LIVE_OPEN

```
run_once (runner)
  → broker.open_position  (Bybit)
  → LIVE_ACCEPTED (log)
  → record_open(state, position)  → open_positions[strategy][position_id]
  → format_live_open_message + send_telegram  → LIVE_OPEN в TG
  → save_state(state)
```

**Файлы:** `datasets/trading_state.json` — `open_positions.short_pump_fast0[position_id]`.

**Важно:** LIVE_OPEN не использует tg_entry_filter — шлётся безусловно после успешного open. Исключение → `LIVE_OPEN_TG_FAILED`.

### 3. Outcome resolution (2 пути)

#### A) fast0_sampler (_run_fast0_outcome_watcher)

- Ждёт до 60 сек, пока в state появится позиция с `order_id` и `position_idx`.
- Вызывает `resolve_live_outcome` (Bybit closed-pnl).
- При TP/SL → `close_from_live_outcome` → `_send_live_outcome_telegram`.

**Возможные логи:**
- `UNKNOWN_LIVE_NOT_RESOLVED | reason=no_position|no_order_id` — позиции нет в state или нет order_id.
- `LIVE_OUTCOME_RETRY_SCHEDULED` — таймаут/сеть.
- `LIVE_OUTCOME_UNKNOWN_LIVE` — retry исчерпан.
- `FAST0_LIVE_CLOSE_FROM_OUTCOME failed` — ошибка при close.

#### B) outcome_worker (отдельный процесс)

- Периодически итерирует `open_positions`, для live вызывает `resolve_live_outcome`, затем `close_from_live_outcome`.

**Возможные логи:**
- `OUTCOME_WORKER_SKIP_FINALIZED` — уже в outcome_tg_sent.
- `OUTCOME_PENDING` — resolve не нашёл closed-pnl.
- `OUTCOME_RESOLVED` — успех.

### 4. close_from_live_outcome → OUTCOME TG

```
close_from_live_outcome
  → _find_position_for_outcome(run_id, event_id, symbol)
  → если not found: LIVE_OUTCOME_ALREADY_FINALIZED (position уже удалена или не было)
  → record_close, save_state
  → _append_close_row (trading_closes)
  → _write_live_outcome_to_datasets (outcomes_v3.csv)
  → _send_live_outcome_telegram
```

**`_send_live_outcome_telegram`:** если `TG_SEND_OUTCOME != "1"` — **выйдет без отправки**.

---

## Команды проверки на VPS

Выполнять из `/root/pump_short` (или каталога pump_short).

### 1. State и позиции

```bash
# Есть ли позиция/close для DOODUSDT 20260313_151135 в state
python3 -c "
import json
from pathlib import Path
p = Path('datasets/trading_state.json')
if not p.exists():
    print('trading_state.json NOT FOUND')
else:
    d = json.load(p.open())
    op = d.get('open_positions') or {}
    sp = op.get('short_pump_fast0') or {}
    for pid, pos in list(sp.items()):
        if pos.get('symbol') == 'DOODUSDT' or '20260313_151135' in str(pos.get('run_id','')):
            print('OPEN:', pid, pos)
    sent = d.get('outcome_tg_sent') or []
    for k in sent:
        if 'DOODUSDT' in k or '20260313_151135' in k:
            print('OUTCOME_TG_SENT:', k)
    print('total open short_pump_fast0:', len(sp))
    print('total outcome_tg_sent:', len(sent))
"
```

### 2. Closes (trading_closes)

```bash
grep -E "DOODUSDT|20260313_151135" datasets/trading_closes*.csv 2>/dev/null || echo "no matches in closes"
```

### 3. Outcomes в datasets

```bash
grep -r "20260313_151135" datasets/date=*/strategy=short_pump_fast0/ 2>/dev/null | head -20
```

### 4. Signals queue (был ли enqueue)

```bash
grep "20260313_151135" datasets/signals_queue.jsonl 2>/dev/null || echo "no signal in queue"
```

### 5. Логи (LIVE_ACCEPTED, LIVE_OPEN, OUTCOME, FAST0)

```bash
# Логи runner / outcome_worker / fast0
grep -E "20260313_151135|DOODUSDT" /var/log/pump_short/*.log 2>/dev/null | grep -E "LIVE_ACCEPTED|LIVE_OPEN|OUTCOME|FAST0|UNKNOWN_LIVE|LIVE_OUTCOME" | tail -50
# или где у вас лежат логи
journalctl -u pump_short --since "2026-03-13 15:00" --until "2026-03-13 16:00" 2>/dev/null | grep -E "20260313_151135|DOODUSDT"
```

### 6. TG_SEND_OUTCOME

```bash
echo "TG_SEND_OUTCOME=${TG_SEND_OUTCOME:-not set}"
# Должно быть TG_SEND_OUTCOME=1 для отправки OUTCOME в TG
```

---

## Типичные точки разрыва

| Симптом | Возможная причина |
|--------|-------------------|
| LIVE_OPEN не пришёл | `send_telegram` exception → LIVE_OPEN_TG_FAILED; или runner не дошёл до этого места |
| OUTCOME не пришёл | TG_SEND_OUTCOME≠1; или `close_from_live_outcome` не вызван; или position not found |
| position not found | Позиция не попала в state; или event_id/run_id не совпадают при поиске |
| UNKNOWN_LIVE_NOT_RESOLVED | Позиция не появилась в state за 60s; или нет order_id/position_idx |
| Запись в outcomes_v3 есть, OUTCOME в TG нет | TG_SEND_OUTCOME=0 или дубликат заблокирован (outcome_tg_sent) |

---

## Что нужно проверить в первую очередь

1. **Есть ли запись в `outcomes_v3.csv`** для run_id=20260313_151135, symbol=DOODUSDT.
2. **Есть ли запись в `trading_closes`**.
3. **Есть ли ключ в `outcome_tg_sent`** (значит TG пытались отправить или уже отправили).
4. **Значение TG_SEND_OUTCOME** в окружении runner/outcome_worker.
5. **Логи** с LIVE_ACCEPTED, LIVE_OPEN_TG_FAILED, LIVE_OUTCOME_ALREADY_FINALIZED, UNKNOWN_LIVE_NOT_RESOLVED.

После выполнения команд — приложить вывод для уточнения root cause.
