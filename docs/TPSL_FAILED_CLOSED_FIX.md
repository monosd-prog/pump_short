# Fix: TPSL_SETUP_FAILED_CLOSED — сохранение историчности

## Root cause

При сбое установки TP/SL после успешного исполнения entry-ордера:

1. `bybit_live.open_position` вызывал `_close_position_immediate` и возвращал `None`
2. Runner получал `position is None` → считал сделку полностью отклонённой
3. Ничего не сохранялось: нет `record_open`, нет `LIVE_OPEN`, нет записей в state/datasets/closes
4. Позиция фактически была открыта и закрыта, но в системах бота следов не было

## Решение

Вместо `None` возвращается dict со статусом `TPSL_SETUP_FAILED_CLOSED`, который runner обрабатывает отдельно: пишет в trading_closes, outcomes_v3 и отправляет TG-алерт.

## Новый статус

**`TPSL_SETUP_FAILED_CLOSED`** — вход исполнен, установка TP/SL не удалась, позиция закрыта.

## Изменённые файлы

| Файл | Изменения |
|------|-----------|
| `trading/bybit_live.py` | `_set_tpsl_with_retry` возвращает `(bool, close_info)`; при ошибке `open_position` возвращает dict со статусом вместо `None` |
| `trading/paper_outcome.py` | `record_tpsl_failed_closed()`, доработка `_write_live_outcome_to_datasets` под `close_reason=tpsl_setup_failed` |
| `trading/runner.py` | Обработка `position.status == "TPSL_SETUP_FAILED_CLOSED"` → вызов `record_tpsl_failed_closed` |
| `notifications/tg_format.py` | `format_tpsl_failed_closed_message()` для TG-алерта |
| `scripts/backfill_tpsl_failed_closed.py` | Скрипт backfill для старых кейсов |

## Пример TG-сообщения

```
🟥 SHORT | short_pump_fast0 | TPSL_SETUP_FAILED_CLOSED | sym=DOODUSDT
⚠️ Entry filled, TP/SL setup failed → position force-closed
run_id=20260313_151135 eid=20260313
entry=0.1234 tp=0.1219 sl=0.1247
exit≈0.1250
pnl≈1.30%
risk_profile=fast0_1p5R | notional=100 USD | lev=x4
```

## Backfill для DOODUSDT 20260313_151135

Нужны `entry` (avgPrice) и `exit` (цена закрытия) из логов или Bybit closed-pnl.

```bash
cd /root/pump_short
DATASET_BASE_DIR=/root/pump_short/datasets PYTHONPATH=. python3 scripts/backfill_tpsl_failed_closed.py \
  --run-id 20260313_151135 \
  --symbol DOODUSDT \
  --strategy short_pump_fast0 \
  --entry 0.XXXX \
  --exit 0.YYYY \
  --risk-profile fast0_1p5R \
  --data-dir /root/pump_short/datasets
```

Без `--exit` берётся `entry` (pnl≈0). Сначала проверить в dry-run:

```bash
PYTHONPATH=. python3 scripts/backfill_tpsl_failed_closed.py \
  --run-id 20260313_151135 --symbol DOODUSDT --strategy short_pump_fast0 \
  --entry 0.XXXX --exit 0.YYYY --risk-profile fast0_1p5R --dry-run
```

## Поиск похожих кейсов в логах

```bash
grep -E "LIVE_TPSL_FAILED_CLOSED|LIVE_REJECTED.*reason=broker" /var/log/pump_short/*.log
# или
journalctl -u pump_short | grep -E "LIVE_TPSL_FAILED_CLOSED|LIVE_REJECTED.*reason=broker"
```

Проверить наличие успешного entry перед reject:

```bash
grep -B5 "LIVE_TPSL_FAILED_CLOSED" /var/log/pump_short/*.log
grep -B5 "LIVE_REJECTED.*reason=broker" /var/log/pump_short/*.log
```

## Команды проверки на VPS

```bash
# 1. После деплоя — проверка, что кейсы сохраняются
# При следующем TPSL_FAILED_CLOSED должны появиться:
# - запись в trading_closes.csv
# - запись в datasets/date=YYYYMMDD/strategy=short_pump_fast0/mode=live/outcomes_v3.csv
# - TG-сообщение TPSL_SETUP_FAILED_CLOSED

# 2. Backfill DOODUSDT (подставить entry/exit из Bybit)
cd /root/pump_short
PYTHONPATH=. python3 scripts/backfill_tpsl_failed_closed.py \
  --run-id 20260313_151135 --symbol DOODUSDT --strategy short_pump_fast0 \
  --entry <avgPrice> --exit <closePrice> --risk-profile fast0_1p5R \
  --data-dir /root/pump_short/datasets
```
