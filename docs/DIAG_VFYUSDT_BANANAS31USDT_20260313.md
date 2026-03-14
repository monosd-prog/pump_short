# Расследование: VFYUSDT и BANANAS31USDT — внешние сигналы без ENTRY_OK / TRADEABLE

**Кейсы:**
1. **VFYUSDT** — 13.03.2026 17:27:50, Bybit 10m, +17.99%
2. **BANANAS31USDT** — 13.03.2026 17:52:00, Bybit 10m, +11.86%

Внешние сигналы были, но ENTRY_OK / TRADEABLE / live-входа не было. Задача: найти root cause.

---

## 1. Цепочка: откуда монета попадает в систему

```
Внешний pump-detector (10m Bybit)
  → POST /pump {symbol, pump_pct, pump_ts, exchange="bybit", extra={tf:"10m", window_minutes:10}}
  → apply_filters (exchange, min_pump_pct, require_10m_window)
  → start_watch (cooldown, already_running)
  → run_watch_for_symbol (short_pump) + run_fast0_for_symbol (если ENABLE_FAST_FROM_PUMP)
```

**Возможные точки отказа:**
- Webhook не отправлен / не дошёл до нашего сервера
- `apply_filters`: exchange≠bybit, pump_pct < min_pump_pct, not_10m_window
- `start_watch`: already_running, cooldown
- Watcher: stage не достиг 3/4, context_score, dist, delta, CVD, OI, funding, liq veto
- TRADEABLE gate: stage!=4 или dist < TG_ENTRY_DIST_MIN

---

## 2. Команды проверки на VPS

Выполнять из `/root/pump_short` (или каталога pump_short). Заменить `YYYYMMDD` на `20260313`.

### 2.1 Был ли webhook и какой результат

```bash
# Логи API (pump-short-api или основной сервис)
journalctl -u pump-short-api --since "2026-03-13 17:20" --until "2026-03-13 18:00" --no-pager 2>/dev/null | grep -E "VFYUSDT|BANANAS31USDT|PUMP_ACCEPTED|PUMP_IGNORED|apply_filters|pump_pct_too_small|not_10m_window|already_running|cooldown"

# Альтернатива: поиск по сервису pump_short (если один процесс)
journalctl -u pump-short --since "2026-03-13 17:20" --until "2026-03-13 18:00" --no-pager 2>/dev/null | grep -E "VFYUSDT|BANANAS31USDT|PUMP|pump"
```

### 2.2 Watcher: run_id и этапы (short_pump)

Логи watcher идут в `{LOG_ROOT}/logs_short/{YYYY-MM-DD}/{SYMBOL}.log`. По умолчанию `LOG_ROOT=logs`.

```bash
# VFYUSDT
grep -E "WATCH_START|Stage transition|ARMED|ENTRY_CANDIDATE_SET|ENTRY_OK|ENTRY_OK_CONFIRMED|TIMEOUT|PAPER_REJECTED" logs/logs_short/2026-03-13/VFYUSDT.log 2>/dev/null || echo "No VFYUSDT watcher log"

# BANANAS31USDT
grep -E "WATCH_START|Stage transition|ARMED|ENTRY_CANDIDATE_SET|ENTRY_OK|ENTRY_OK_CONFIRMED|TIMEOUT|PAPER_REJECTED" logs/logs_short/2026-03-13/BANANAS31USDT.log 2>/dev/null || echo "No BANANAS31USDT watcher log"
```

### 2.3 Events CSV (stage, entry_ok, skip_reasons, context_score, dist_to_peak_pct)

```bash
# short_pump events
for sym in VFYUSDT BANANAS31USDT; do
  echo "=== $sym ==="
  grep "$sym" datasets/date=20260313/strategy=short_pump/mode=live/events_v3.csv 2>/dev/null || echo "No events"
done

# short_pump_fast0 events (если ENABLE_FAST_FROM_PUMP)
for sym in VFYUSDT BANANAS31USDT; do
  echo "=== FAST0 $sym ==="
  grep "$sym" datasets/date=20260313/strategy=short_pump_fast0/mode=live/events_v3.csv 2>/dev/null || echo "No fast0 events"
done
```

### 2.4 Поиск run_id по символу и времени

```bash
# run_id формат YYYYMMDD_HHMMSS. Около 17:27 → 20260313_1727xx, 17:52 → 20260313_1752xx
grep -r "VFYUSDT" datasets/date=20260313/ 2>/dev/null | head -20
grep -r "BANANAS31USDT" datasets/date=20260313/ 2>/dev/null | head -20
```

### 2.5 Журнал (journalctl) — все упоминания символов

```bash
journalctl --since "2026-03-13 17:20" --until "2026-03-13 18:10" --no-pager 2>/dev/null | grep -E "VFYUSDT|BANANAS31USDT" | head -100
```

### 2.6 Логи CSV watcher (5m, 1m, fast, summary)

```bash
# Если есть логи вида logs/20260313_HHMMSS_VFYUSDT_*.csv
ls -la logs/20260313_*VFYUSDT* 2>/dev/null
ls -la logs/20260313_*BANANAS31* 2>/dev/null
```

### 2.7 Торговые outcomes и trades

```bash
grep -E "VFYUSDT|BANANAS31USDT" datasets/date=20260313/strategy=short_pump/mode=live/outcomes_v3.csv 2>/dev/null
grep -E "VFYUSDT|BANANAS31USDT" datasets/date=20260313/strategy=short_pump/mode=live/trades_v3.csv 2>/dev/null
grep -E "VFYUSDT|BANANAS31USDT" datasets/date=20260313/strategy=short_pump_fast0/mode=live/outcomes_v3.csv 2>/dev/null
```

### 2.8 Сигналы в очереди (если был ENTRY_OK / TRADEABLE)

```bash
grep -E "VFYUSDT|BANANAS31USDT" datasets/signals_queue.jsonl 2>/dev/null
```

---

## 3. Интерпретация: на каком этапе остановилось

| Симптом | Интерпретация |
|--------|----------------|
| Нет записей в events_v3 | Watcher не запускался или символ не в pipeline (webhook не пришёл / rejected) |
| Только watch_start, skip_reasons=watch_start | Запуск был, дальше timeout или ошибка |
| stage=0, 1, 2 | Структура не дошла до ARMED (stage 3/4). Причины: цена не дала pullback→bounce→pullback→bounce |
| stage=3/4, нет ARMED | Странно; ARMED пишется при первом stage>=3 |
| ARMED есть, нет ENTRY_CANDIDATE_SET | context_filter не прошёл: near_top, oi_ok, funding_ok, context_score < entry_context_min_score |
| ENTRY_CANDIDATE_SET есть, нет ENTRY_OK_CONFIRMED | trigger не прошёл: delta_ok, break_low (для 1m) или delta_ok (для fast) |
| ENTRY_OK есть, нет TRADEABLE | PAPER_REJECTED_SHORT_PUMP_GATE: stage!=4 или dist_to_peak_pct < TG_ENTRY_DIST_MIN |
| TRADEABLE есть, нет live-входа | Runner не обработал / rejected / лимиты / auto-trading off |

---

## 4. Что смотреть в events_v3 (колонки)

- `run_id`, `event_id`, `symbol`, `stage`, `entry_ok`, `skip_reasons`
- `context_score`, `dist_to_peak_pct`
- `liq_long_usd_30s`, `liq_short_usd_30s`
- `oi_change_fast_pct`, `cvd_delta_ratio_30s`, `funding_rate_abs`
- `payload_json` — полный payload события

---

## 5. Причины отсутствия ENTRY_OK (short_pump watcher)

**decide_entry_fast:**
- `context_score_with_cvd < 0.65` → no entry
- `near_top = False` (dist_to_peak > dist_to_peak_max_pct, обычно 12%)
- `delta_ok = False` (delta_ratio_30s вне коридора, или late_mode и не strong_delta)

**decide_entry_1m:**
- `entry_ok = delta_ok and (hit >= need)`; need зависит от context_score
- `break_low` и `delta_ok` оба нужны для 1m
- `no_new_high`, `near_top`, `delta_ok` — flags для hit

**context_filter (_context_filter_pass):**
- stage < 3
- near_top = False
- oi_change > entry_context_oi_change_max_pct
- funding_abs > entry_context_funding_abs_max
- context_score < entry_context_min_score (0.45 default)

**liq veto:**
- liq_short_usd_30s >= entry_liq_short_veto_usd_30s

---

## 6. FAST0 (short_pump_fast0)

Если `ENABLE_FAST_FROM_PUMP=1`, для каждого pump запускается `run_fast0_for_symbol` параллельно с short_pump watcher. FAST0:
- Работает в узком окне после pump (FAST0_WINDOW_SEC, default 180)
- Требует: context_score, dist, CVD, liq в диапазоне 5k–25k
- Логи: тот же `logs/logs_short/{date}/{SYMBOL}.log` (один логгер) — ищи `FAST0`, `run_fast0`

```bash
grep -E "FAST0|run_fast0|VFYUSDT|BANANAS31USDT" logs/logs_short/2026-03-13/*.log 2>/dev/null | head -50
```

---

## 7. Checklist для каждого символа

- [ ] Был ли webhook (PUMP_ACCEPTED / ignored в логах)?
- [ ] Какой run_id?
- [ ] Max stage в events?
- [ ] ARMED, ENTRY_CANDIDATE_SET, ENTRY_OK — что есть?
- [ ] context_score, dist_to_peak_pct, liq — какие значения?
- [ ] skip_reasons в events?
- [ ] ENTRY_TRIGGER_VETOED? (liq_short_veto)
- [ ] PAPER_REJECTED_SHORT_PUMP_GATE? (stage!=4 или dist)

---

## 8. Шаблон отчёта по символу

```
## VFYUSDT
- Root cause: ...
- Timeline: WATCH_START → stage X → [ARMED] → [ENTRY_CANDIDATE] → [ENTRY_OK] → [TRADEABLE]
- Конкретная причина отсутствия ENTRY_OK: ...
- Нужно ли править логику: да/нет. Если нет: "Система сознательно отфильтровала по X"

## BANANAS31USDT
- ... (аналогично)
```

---

## 9. Сервисы (имена для journalctl)

Проверить актуальные имена:

```bash
systemctl list-units --type=service | grep -i pump
# Типичные: pump-short, pump-short-api, pump-trading-runner, pump-short-live-auto
```
