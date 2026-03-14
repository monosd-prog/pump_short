# Расследование: аномальный TP_hit с отрицательным pnl (NFPUSDT 20260314)

**Кейс:** run_id=20260314_210836, event_id=20260314_210836_fast0_13_fcd09539, symbol=NFPUSDT, strategy=short_pump_fast0, risk_profile=fast0_base_1R

## 1. Факты из outcomes_v3

| Поле | Значение |
|------|----------|
| outcome | TP_hit |
| pnl_pct | -0.268726 |
| entry | 0.019492 |
| exit_price | 0.01954438 |
| tp | 0.019499168 |
| sl | 0.01993336 |

Для SHORT:
- exit_price > entry → убыток (цена выросла)
- tp > entry → для short TP должен быть ниже entry (профит при падении цены)
- outcome=TP_hit при pnl<0 → неконсистентно

## 2. Цепочка формирования

```
1. Signal: fast0_sampler ENTRY_OK
   - entry_price = last_price (tick)
   - tp = entry * (1 - FAST0_TP_PCT) = entry * 0.988
   - sl = entry * (1 + FAST0_SL_PCT) = entry * 1.01

2. Runner: open_position
   - signal.entry_price, signal.tp_price, signal.sl_price
   - bybit_live: _round_price_to_tick(tp, tick_size), _round_price_to_tick(sl, tick_size)
   - _set_tpsl_with_retry: при ошибке — _recompute_tpsl_for_price_move (retry)

3. Live outcome: resolve_live_outcome
   - get_closed_pnl → _match_closed_record → _classify_exit_price(avg_exit, entry, tp, sl, side)

4. close_from_live_outcome → _write_live_outcome_to_datasets
   - outcome, exit_price, pnl_pct из результата resolve_live_outcome
```

## 3. Root cause

### 3.1 Классификатор _classify_exit_price

Для SHORT:
- TP_hit: `exit_price <= tp_tol` (tp_tol = tp * 1.005)
- Классификатор **не проверяет**, что tp < entry.

В кейсе:
- tp = 0.019499168 > entry = 0.019492 (tp в зоне убытка!)
- tp_tol = 0.019499168 * 1.005 = 0.01959666
- exit = 0.01954438 <= tp_tol → классификатор вернул TP_hit
- Но exit > entry → реальный убыток. TP_hit семантически = профит.

### 3.2 Откуда tp > entry

Возможные причины:
1. **Slippage:** paper entry (last_price) ≠ actual fill (avgEntryPrice). Signal: entry=0.01948, tp=0.01923. Fill: 0.019492. Position хранит entry=0.019492, tp мог прийти из retry с другой логикой.
2. **_recompute_tpsl_for_price_move:** при TPSL retry, если цена ушла вверх, tp_cand = current_price - buf может оказаться > entry при сильном движении.
3. **Tick rounding:** на микропрайсах (tick 0.00001) округление редко даёт tp > entry при корректной формуле.

Наиболее вероятно: **комбинация slippage + retry** или **неверный tp в position** (записан не из signal, а из другого источника).

### 3.3 Класс бага

- **Не единичный:** любой short, у которого tp окажется >= entry (из-за retry/slippage/округления), даст ложный TP_hit при exit в зоне убытка.
- **Classifier bug:** _classify_exit_price должен учитывать направление PnL. Для short: если exit > entry (убыток), не может быть TP_hit.

## 4. Fix (применён)

В `trading/bybit_live_outcome.py` в `_classify_exit_price` добавлен guard по направлению PnL:

- **SHORT:** если exit_price > entry (убыток) → не возвращать TP_hit. Если exit >= sl_tol → SL_hit, иначе EARLY_EXIT.
- **LONG:** если exit_price < entry (убыток) → не возвращать TP_hit. Если exit <= sl_tol → SL_hit, иначе EARLY_EXIT.

Для NFPUSDT кейса: exit=0.019544, entry=0.019492, sl_tol≈0.019834 → exit < sl_tol → **EARLY_EXIT** (корректно: закрытие с убытком между entry и sl).

## 5. Historical repair

Для NFPUSDT 20260314_210836: outcome должен быть **EARLY_EXIT** (exit между entry и sl, с убытком; TP зона невалидна из-за tp>entry).

Способ: ручная правка `outcomes_v3.csv` или скрипт:

```python
# Найти строку event_id=20260314_210836_fast0_13_fcd09539
# Заменить outcome: TP_hit → EARLY_EXIT
```

Файл: `datasets/date=20260314/strategy=short_pump_fast0/mode=live/outcomes_v3.csv`

## 6. Изменённые файлы и diff

| Файл | Изменение |
|------|-----------|
| `trading/bybit_live_outcome.py` | Добавлен guard: для SHORT (exit>entry) и LONG (exit<entry) не возвращать TP_hit; при loss zone — SL_hit или EARLY_EXIT |
| `docs/DIAG_NFPUSDT_20260314_TP_HIT_ANOMALY.md` | Диагностика и отчёт |

Diff (`bybit_live_outcome.py`): добавлены проверки `if exit_price > entry` (Sell) и `if exit_price < entry` (Buy) перед ветками TP_hit; при срабатывании — SL_hit или EARLY_EXIT.

## 7. Scope

- **Класс:** любой short/long, где exit в зоне убытка, но level-based логика из-за невалидных tp/sl (tp>entry для short, tp<entry для long) ошибочно классифицировал как TP_hit.
- **Затронуты:** fast0 и другие стратегии, использующие `resolve_live_outcome` / `_classify_exit_price`.
- **fast0_base_1R:** не оптимизирован; фикс только классификатора.

## 8. Команды проверки на VPS

```bash
# Найти строку
grep "20260314_210836\|NFPUSDT" datasets/date=20260314/strategy=short_pump_fast0/mode=live/outcomes_v3.csv

# Логи runner / outcome
journalctl -u pump-trading-runner --since "2026-03-14 21:00" --until "2026-03-14 22:00" --no-pager 2>/dev/null | grep -E "NFPUSDT|20260314_210836|LIVE_TPSL|LIVE_OUTCOME|LIVE_ORDER"

# Логи fast0
grep -E "NFPUSDT|20260314_210836" logs/logs_short/2026-03-14/NFPUSDT.log 2>/dev/null | head -80
```
