# Диагностика: EARLY_EXIT и conflicts (tp_sl_same_candle)

Метрики блока «🧱 МОНИТОРИНГ ДАТАСЕТОВ»: EARLY_EXIT, Конфликты связок. Root cause, цепочка кода, классификация.

---

## 1. EARLY_EXIT

### 1.1 Кто пишет EARLY_EXIT

| Файл | Функция | Условие |
|------|---------|---------|
| `trading/bybit_live_outcome.py` | `_classify_exit_price` | exit_price между TP и SL (tp < exit < sl для short) |
| `trading/paper_outcome.py` | `close_from_live_outcome` → `_write_live_outcome_to_datasets` | close_reason="early_exit" при res=="EARLY_EXIT" |

### 1.2 Полная цепочка

```
Bybit closed-pnl / order history / executions
  → resolve_live_outcome (bybit_live_outcome.py)
  → _match_closed_record → _classify_exit_price(avg_exit, entry, tp, sl, side)
  
_classify_exit_price:
  - TP_hit: exit_price <= tp + tol (short)
  - SL_hit: exit_price >= sl - tol (short)
  - EARLY_EXIT: tp < exit_price < sl (закрытие между уровнями)
  
  → outcome_worker или fast0_sampler / paper_outcome.close_from_live_outcome
  → close_from_live_outcome(res="EARLY_EXIT", ...)
  → _write_live_outcome_to_datasets(close_reason="early_exit")
  → build_outcome_row → write_outcome_row → outcomes_v3.csv
  → outcome = "EARLY_EXIT"
```

### 1.3 Режимы

- **LIVE только**: EARLY_EXIT возникает только при live-торговле (Bybit exit). Paper/candle resolution его не генерирует.
- **Стратегии**: short_pump, short_pump_fast0, long_pullback (если есть live).

### 1.4 Штатный или аварийный

**Штатный**. Означает: позиция закрыта на бирже по цене между TP и SL (ручное закрытие, частичное закрытие, ликвидация и т.п.). Это не ошибка, а нормальный исход live-сделки.

### 1.5 outcomes_v3

- Да, EARLY_EXIT попадает в outcomes_v3.csv.
- `outcome` = "EARLY_EXIT", `trade_type` = "LIVE".
- details_json: `{"source":"bybit","tp_hit":false,"sl_hit":false}` (из _write_live_outcome_to_datasets для early_exit).

### 1.6 «Нестандартный исход»?

В `_dataset_quality_metrics`:
- `valid = norm.isin(["TP_hit", "SL_hit", "TIMEOUT"])` — EARLY_EXIT не в valid.
- `n_other = (~valid).sum()` — EARLY_EXIT считается «нестандартным».
- `n_early = (raw_out == "EARLY_EXIT").sum()` — отдельный счётчик.
- `quality_ok = (n_dup==0 and n_conf==0 and n_other==0)` — EARLY_EXIT ухудшает quality_ok через n_other.

**Вывод**: EARLY_EXIT логично вынести из «нестандартных» — это допустимый live-исход. Либо показывать отдельно и не учитывать в quality_ok.

### 1.7 Root cause

EARLY_EXIT — ожидаемый результат для live: закрытие между TP и SL. Не баг, а осознанное решение классификации.

---

## 2. Conflicts (tp_sl_same_candle)

### 2.1 Где формируется tp_sl_same_candle

| Файл | Функция | Условие |
|------|---------|---------|
| `common/outcome_tracker.py` | `track_outcome` | tp_hit и sl_hit в одной и той же свече (high/low оба пересекают уровни) |

### 2.2 Где задаётся conflict_policy

| Файл | Переменная |
|------|------------|
| `short_pump/config.py` | `conflict_policy` (default "SL_FIRST"), env CONFLICT_POLICY |
| `long_pullback/config.py` | `conflict_policy` (default "SL_FIRST") |
| `common/outcome_tracker.py` | OUTCOME_TP_SL_CONFLICT (env), `conflict_policy` в вызове track_outcome |

### 2.3 Сценарии conflict

- **TP и SL в одной свече**: low ≤ tp_price И high ≥ sl_price (short). Порядок касания неизвестен.
- **Используется OUTCOME_USE_CANDLE_HILO** (high/low). Если 0 — только close, конфликт маловероятен.
- **Политика**: SL_FIRST → outcome="SL_hit"; TP_FIRST → outcome="TP_hit"; NEUTRAL → outcome="CONFLICT".

### 2.4 Влияние на outcome / pnl / отчёт

- outcome: TP_hit, SL_hit или CONFLICT (при NEUTRAL).
- pnl: считается по выбранному outcome.
- details_json: `tp_sl_same_candle=1`, `conflict_policy`, `alt_outcome_tp_first`, `alt_outcome_sl_first`, `alt_pnl_tp_first`, `alt_pnl_sl_first`.
- В отчёте: core = TP/SL; CONFLICT не в core. Конфликты считаются в n_conf для «МОНИТОРИНГ ДАТАСЕТОВ».

### 2.5 Реальная проблема или артефакт

**Артефакт свечного бэктеста**. При 1m-свечах нельзя однозначно определить порядок касания TP и SL — это ограничение модели. Политика даёт детерминированный выбор. Данные корректны.

### 2.6 Root cause

tp_sl_same_candle — ожидаемая ситуация при candle-based resolution. Документирована, разрешается conflict_policy. Ошибкой не является.

---

## 3. Файлы для проверки

| Файл | Роль |
|------|------|
| `pump_short/trading/bybit_live_outcome.py` | EARLY_EXIT при классификации exit_price |
| `pump_short/trading/paper_outcome.py` | close_from_live_outcome, _write_live_outcome_to_datasets |
| `pump_short/common/outcome_tracker.py` | track_outcome, tp_sl_same_candle, conflict_policy |
| `pump_short/short_pump/outcome.py` | track_outcome_short → track_outcome |
| `pump_short/short_pump/config.py` | conflict_policy |
| `pump_short_analysis/shared_analytics/executive_report.py` | _dataset_quality_metrics, блок МОНИТОРИНГ ДАТАСЕТОВ |
| `pump_short_analysis/shared_analytics/stats.py` | _normalize_outcome_raw (EARLY_EXIT → OTHER) |

---

## 4. Классификация

| Группа | Оценка | Рекомендация |
|--------|--------|--------------|
| EARLY_EXIT | Допустимо | Выделить в отдельную метрику; не считать «нестандартным» в quality_ok. |
| Conflicts (tp_sl_same_candle) | Допустимо | Информационная метрика; не считать quality issue. |

---

## 5. Что менять (если нужно)

### 5.1 Отчётность (executive_report.py)

Вариант: разделять «нестандартные» и EARLY_EXIT:

```python
# n_other = outcomes not in (TP_hit, SL_hit, TIMEOUT)
# n_early = subset of n_other
# n_other_excl_early = n_other - n_early  # «реально нестандартные»
# quality_ok: например (n_dup==0 and n_other_excl_early==0) — т.е. EARLY_EXIT не портит quality
```

Или оставить как есть, но добавить комментарий, что EARLY_EXIT — штатный live-исход.

### 5.2 Conflicts

Текущая логика адекватна. Можно оставить n_conf как информативную метрику, но не включать в quality_ok, если хотите считать конфликты «нормальными».

---

## 6. Audit script

```bash
cd pump_short_analysis
PYTHONPATH=. python scripts/audit_early_exit_conflicts.py --base-dir /path/to/datasets --days 90
```

Локально (datasets рядом с pump_short_analysis):
```bash
PYTHONPATH=. python scripts/audit_early_exit_conflicts.py --days 90
```

На VPS (datasets в pump_short):
```bash
cd /opt/pump_short_analysis
PYTHONPATH=. python scripts/audit_early_exit_conflicts.py --base-dir /root/pump_short --days 90
```

---

## 7. Команды проверки на VPS

```bash
# Количество EARLY_EXIT
grep -r "EARLY_EXIT" /root/pump_short/datasets/date=*/strategy=*/mode=*/outcomes_v3.csv 2>/dev/null | wc -l

# Конфликты (tp_sl_same_candle в details_json)
grep -l "tp_sl_same_candle" /root/pump_short/datasets/date=*/strategy=*/mode=*/outcomes_v3.csv 2>/dev/null

# Распределение outcome
awk -F',' '{print $10}' /root/pump_short/datasets/date=*/strategy=*/mode=*/outcomes_v3.csv 2>/dev/null | sort | uniq -c
```
