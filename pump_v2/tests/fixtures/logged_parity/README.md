# Logged parity fixtures

Цель: **regression tests против production watcher v1** — каждый `sample_*.json` — снимок того, что v1 записал в `events_v3.csv` в конкретной строке.

Используется для проверки: **v2 на тех же входных условиях (когда они восстановимы из контекста) должен дать то же значение**, либо для документирования расхождений.

## Ограничения

- **Нет сырых входов** (klines / trades tape / OI history). Только скаляры из CSV и вложенный `payload_json`.
- Для индикаторов с **совпадающим именем и семантикой** — строгий parity.
- Где в CSV другое окно или имя (`volume_zscore_20` vs `volume_zscore` из `features.py`) — нужен явный «мост» в тестах или отдельный контракт.

## Отбор строк (`_extract.py`)

- Файлы: `datasets/**/events_v3.csv`
- `schema_version == 3`, `mode in (live, paper)`
- Событие считается **entry-кандидатом**, если выполняется одно из:
  - `event_id` содержит `_entry_`, `entry_fast` или `entry_confirm` (как в `short_pump`);
  - или `entry_ok == 1` и в `event_id` есть `fast0` (ветки `short_pump_fast0` / `short_pump_fast0_filtered`, где нет подстроки `_entry_` в id);
  - или в `skip_reasons` есть `entry_ok` **и** `entry_ok == 1` (например `entry_ok_false_pump` у `false_pump`).
- Обязательны непустые **`stage`** и **`dist_to_peak_pct`**.
- Стратифицированная выборка **50** строк, `random.seed(42)`, квоты по колонке `strategy` см. `STRATA_QUOTA` в `_extract.py`.

## Пересоздание

Из корня репозитория:

```bash
python3 pump_v2/tests/fixtures/logged_parity/_extract.py
```

## Использование в тестах

```python
import json
from pathlib import Path

p = Path("pump_v2/tests/fixtures/logged_parity/samples/sample_001.json")
sample = json.loads(p.read_text(encoding="utf-8"))
logged = sample["logged_indicators"]
# assert v2(...) == logged["context_score"]["value"]
```

**Важно:** без сырых рядов большинство индикаторов v2 нельзя пересчитать только из sample; типичные применения — сравнение с **другим слоем** эталона (например parquet golden), проверка цепочек (`oi_divergence` из уже залогированных полей), регрессия сериализации.

## Файлы

- `samples/sample_NNN.json` — один снапшот на файл.
- `index.csv` — сводка: `sample_id`, путь к исходному CSV, символ, стратегия, `event_id`, число ненулевых индикаторов, флаг конфликтов CSV vs `payload_json`.
