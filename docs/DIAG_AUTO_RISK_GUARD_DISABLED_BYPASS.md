# Расследование: DISABLED режим guard не блокировал live-входы

**Проблема:** AUTO_RISK_GUARD показывал fast0_base_1R = DISABLED, но live-входы по этому режиму прошли (MBOXUSDT, PLAYSOUTUSDT).

## 1. Факты

- До сделок guard/report показывали: fast0_base_1R => DISABLED
- После: RUNNER_PICKED, RISK_PROFILE selected_profile=fast0_base_1R, LIVE_ACCEPTED, реальное открытие
- В логах нет AUTO_RISK_GUARD_BLOCKED

## 2. Root cause

**Причина:** `AUTO_RISK_GUARD_ENABLE` и/или `AUTO_RISK_GUARD_ENFORCE` были **false** в окружении runner'а.

Логика до фикса:
- `is_entry_allowed_for_signal` при `AUTO_RISK_GUARD_ENABLE=false` сразу возвращал `True` (allow) — guard не проверялся
- При `AUTO_RISK_GUARD_ENFORCE=false` — dry-run, логировал, но всё равно возвращал `True`
- Guard state файл (`auto_risk_guard_state.json`) содержал fast0_base_1R=DISABLED, но runner его не применял для блокировки из-за отключённых флагов

`pump-trading-runner.service` использует `EnvironmentFile=/root/pump_short/.env`. В `.env` не были заданы `AUTO_RISK_GUARD_ENABLE=1` и `AUTO_RISK_GUARD_ENFORCE=1`, по умолчанию они `false` в `config.py`.

## 3. Enforce path (полная цепочка)

1. **runner.py** (около строки 432): `allowed_guard, guard_reason = is_entry_allowed_for_signal(signal, risk_profile_name)`
2. **risk_profile_name** берётся из `get_risk_profile()` (стр. 417–429) — для MBOXUSDT/PLAYSOUTUSDT возвращал `fast0_base_1R`
3. **auto_risk_guard.is_entry_allowed_for_signal**:
   - `_mode_name_for_signal_and_profile(signal, "fast0_base_1R")` → `"fast0_base_1R"`
   - **Старая логика:** при `AUTO_RISK_GUARD_ENABLE=false` → `return True` **до** проверки state

## 4. Fix (применён)

В `trading/auto_risk_guard.py`:

- **LIVE mode:** если `mode_name` в guard set и `state in {DISABLED, RECOVERY}` → **всегда блокировать**, независимо от `AUTO_RISK_GUARD_ENABLE` и `AUTO_RISK_GUARD_ENFORCE`.
- DISABLED/RECOVERY больше не зависят от env — блокировка обязательна в live.

Проверка state выполняется **до** проверки `AUTO_RISK_GUARD_ENABLE`.

## 5. Режимы и консистентность

| Режим               | Guard key        | Блокировка DISABLED в live |
|---------------------|------------------|----------------------------|
| short_pump_active_1R| short_pump_active_1R | ✅ Всегда                 |
| fast0_base_1R       | fast0_base_1R    | ✅ Всегда                   |
| fast0_1p5R          | fast0_1p5R       | ✅ Всегда                   |
| fast0_2R            | fast0_2R         | ✅ Всегда                   |

## 6. Изменённые файлы и diff

| Файл | Изменение |
|------|-----------|
| `trading/auto_risk_guard.py` | Импорт `EXECUTION_MODE`; в `is_entry_allowed_for_signal` — при live и DISABLED/RECOVERY всегда block до проверки ENABLE/ENFORCE |
| `trading/runner.py` | Лог: `risk_profile` → `profile` в AUTO_RISK_GUARD_BLOCKED |

## 7. Почему MBOXUSDT и PLAYSOUTUSDT не блокировались

У runner'а были `AUTO_RISK_GUARD_ENABLE=false` и `AUTO_RISK_GUARD_ENFORCE=false`. Guard state не проверялся для блокировки — функция сразу возвращала allow.

После фикса: при `EXECUTION_MODE=live` проверка guard state и блокировка DISABLED выполняются **всегда**, независимо от env.

## 8. Команды проверки на VPS

```bash
# 1. Проверить guard state
cat /root/pump_short/datasets/auto_risk_guard_state.json | python3 -m json.tool

# 2. Проверить env runner'а (значения в .env)
grep -E "AUTO_RISK_GUARD|EXECUTION_MODE" /root/pump_short/.env 2>/dev/null || echo "not found in .env"

# 3. Логи — искать блокировки
journalctl -u pump-trading-runner --since "2026-03-14 17:00" --until "2026-03-14 19:00" --no-pager 2>/dev/null | grep -E "AUTO_RISK_GUARD|MBOXUSDT|PLAYSOUTUSDT|LIVE_ACCEPTED|RUNNER_PICKED|RISK_PROFILE"

# 4. После деплоя — проверить, что при DISABLED есть AUTO_RISK_GUARD_BLOCKED
journalctl -u pump-trading-runner -f --no-pager | grep -E "AUTO_RISK_GUARD|RUNNER_PICKED|LIVE_ACCEPTED"
```

## 9. Deploy

Добавить в `.env` или systemd unit (рекомендуется, но не обязательно для блокировки DISABLED в live после фикса):

```
AUTO_RISK_GUARD_ENABLE=1
AUTO_RISK_GUARD_ENFORCE=1
```

После фикса блокировка DISABLED в live работает и без этих переменных.
