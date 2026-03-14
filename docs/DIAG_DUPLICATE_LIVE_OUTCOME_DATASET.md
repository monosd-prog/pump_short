# Расследование: duplicate live outcome rows в outcomes_v3.csv

**Кейс:** symbol=CYBERUSDT, run_id=20260314_030255, trade_id=short_pump:20260314_030255:20260314_030255_entry_fast:CYBERUSDT

**Факт:** 2 строки TIMEOUT в outcomes_v3.csv:
1. pnl_pct=-0.035014
2. pnl_pct=0.000000

---

## 1. Ветки записи (write paths)

| # | Путь | Файл | Условие |
|---|------|------|---------|
| 1 | **Watcher** track_outcome_short | short_pump/watcher.py | TIMEOUT от candle-логики → write_outcome_row |
| 2 | **close_on_timeout** (runner) | trading/paper_outcome.py | TTL превышен → _write_paper_outcome_to_datasets → write_outcome_row |

Оба вызывают `write_outcome_row` напрямую. Ни watcher, ни _write_paper_outcome_to_datasets не проверяют наличие trade_id перед записью.

**Примечание:** `_write_live_outcome_to_datasets` (close_from_live_outcome, TP/SL/EARLY_EXIT) имеет проверку по event_id, но **close_on_timeout использует _write_paper_outcome_to_datasets**, где такой проверки нет.

**fast0_sampler:** для mode=live возвращает до track_outcome/write_outcome_row — не пишет live outcomes.

---

## 2. Какая строка правильная для CYBERUSDT

| Строка | pnl_pct | Источник | Описание |
|--------|---------|----------|----------|
| 1 | -0.035014 | Watcher (04:18) | Candle-close на момент timeout, mark-to-market |
| 2 | 0.000000 | close_on_timeout (04:33) | exit_mode=entry → exit_price=entry → pnl=0 |

**Рекомендация:** для TIMEOUT правильнее считать **-0.035014**. Это приближённая оценка по цене закрытия свечи на момент timeout; 0.0 — консервативная конвенция (как будто выход по entry). При дедупликации оставляем первую строку (keep first).

---

## 3. Root cause

Две независимые ветки пишут outcome для одного и того же live trade:
1. Watcher (API): EXECUTION_MODE≠live → не срабатывает _watcher_should_skip_outcome_live → track_outcome_short → write_outcome_row.
2. Runner: close_on_timeout → _write_paper_outcome_to_datasets → write_outcome_row.

В `write_outcome_row` (common/io_dataset.py) не было проверки дубликатов по trade_id для mode=live.

---

## 4. Safe fix (dataset idempotency)

**Файл:** `common/io_dataset.py`

**Изменение:** Перед записью в `outcomes_v3.csv` при mode=live вызывается `_live_outcome_duplicate(path, row_v3)`. Если trade_id уже есть в файле — запись пропускается, логируется `OUTCOME_DUPLICATE_SKIPPED`.

- Один общий механизм для всех callers (watcher, close_on_timeout, close_from_live_outcome).
- Безопасно для live: первая запись проходит, повторные — skip.
- Paper mode не затронут.

---

## 5. Diff

```diff
--- a/common/io_dataset.py
+++ b/common/io_dataset.py
+def _live_outcome_duplicate(path: str, row: Dict[str, Any]) -> bool:
+    """Return True if trade_id already exists in outcomes_v3.csv (idempotency for live)."""
+    tid = (row.get("trade_id") or row.get("tradeId") or "").strip()
+    ...
+
 def write_outcome_row(...):
     ...
     if schema_version == 3:
         path = os.path.join(dir_path, "outcomes_v3.csv")
         row_v3 = normalize_outcome_v3(row)
+        if mode_for_path == "live" and _live_outcome_duplicate(path, row_v3):
+            logging.getLogger(__name__).info(
+                "OUTCOME_DUPLICATE_SKIPPED | strategy=%s trade_id=%s event_id=%s (already in outcomes_v3)",
+                ...
+            )
+            return
         _write_row(path, row_v3, OUTCOME_FIELDS_V3)
```

---

## 6. Safe repair для исторических дублей

Использовать существующий скрипт:

```bash
cd /root/pump_short

# Dry-run
python3 scripts/dedup_live_outcomes_v3.py --root . 

# Применить (создаст backup outcomes_v3.csv.bak_YYYYMMDD_HHMMSS)
python3 scripts/dedup_live_outcomes_v3.py --root . --apply
```

Скрипт оставляет первую строку по trade_id (для CYBERUSDT — с pnl_pct=-0.035014).

---

## 7. Проверка других live duplicate rows

```bash
cd /root/pump_short

# short_pump + fast0
for f in datasets/date=*/strategy=short_pump/mode=live/outcomes_v3.csv \
         datasets/date=*/strategy=short_pump_fast0/mode=live/outcomes_v3.csv; do
  [ -f "$f" ] || continue
  n=$(awk -F',' 'NR>1 {print $2}' "$f" | sort | uniq -d | wc -l)
  [ "$n" -gt 0 ] && echo "DUPLICATES: $f" && awk -F',' 'NR>1 {print $2}' "$f" | sort | uniq -d
done

# Или через dedup dry-run
python3 scripts/dedup_live_outcomes_v3.py --root .
```

---

## 8. Команды проверки на VPS после деплоя

```bash
cd /root/pump_short

# 1. Код с idempotency
grep -n "_live_outcome_duplicate\|OUTCOME_DUPLICATE_SKIPPED" common/io_dataset.py

# 2. При повторной попытке записи того же trade_id — в логах
# OUTCOME_DUPLICATE_SKIPPED
journalctl -u pump-trading-runner -f --no-pager 2>/dev/null | grep OUTCOME_DUPLICATE_SKIPPED

# 3. Дедупликация исторических данных
python3 scripts/dedup_live_outcomes_v3.py --root . --apply
```
