# Golden fixtures (pump_v2)

Снимки реальных рыночных данных и **эталонные значения индикаторов**, посчитанные **текущим v1 кодом** (`short_pump`, `common.market_features`, `short_pump.context5m`).  
Используются в Phase 2+ для проверки, что перенесённые индикаторы v2 дают **те же числа** на тех же входах.

## Структура

```text
pump_v2/tests/fixtures/
├── README.md
├── _generate.py              # пересоздание всех фикстур (Bybit + CSV)
└── symbols/
    ├── btc_typical/
    ├── pump_recent/
    ├── quiet_alt/
    ├── no_oi/
    └── no_liquidations/
```

В каждой подпапке:

- `klines_1m.parquet`, `klines_5m.parquet` — окно `[window_start_utc, window_end_utc]` (колонки см. `pump_v2/datasets/SCHEMA.md`)
- `oi.parquet` — OI 5m ряд из Bybit (колонки `ts_utc`, `oi` = openInterest)
- `trades.parquet` — recent trades, отфильтрованные по окну
- `funding.json` — сырой payload `get_funding_rate` на момент генерации
- `liquidations.csv` — подмножество глобального `datasets/liquidations.csv` по символу и окну
- `meta.json` — символ, границы окна, описание
- `expected_indicators.json` — значения + `v1_ref` + `params` (округление до 6 знаков)

## Использование в тестах (пример)

```python
import json
from pathlib import Path
import pandas as pd

FIX = Path("pump_v2/tests/fixtures/symbols/btc_typical")
meta = json.loads((FIX / "meta.json").read_text())
kl5 = pd.read_parquet(FIX / "klines_5m.parquet")
exp = json.loads((FIX / "expected_indicators.json").read_text())
# v2: result = MyIndicator(...).compute(...)
# assert abs(result - exp["volume_zscore"]["value"]) < 1e-5
```

## Пересоздание

Из корня репозитория `/root/pump_short`:

```bash
python3 pump_v2/tests/fixtures/_generate.py
```

Требуется сеть (Bybit REST). Между запросами — небольшая задержка против rate limit.

Зависимость для `parquet`: `pyarrow` (см. `requirements.txt`).

### OI и trades (важно)

- **Open interest** Bybit REST отдаёт только последние N пятиминуток (~200 точек ≈ сутки). Если в интервале `[window_start, window_end]` нет ни одной точки, генератор сохраняет **полный последний снимок** и пишет пояснение в `meta.json` (`oi_parquet_note`). Иначе эталонные `oi_change_pct_*` были бы всегда `null` на «исторических» окнах.
- **Recent trades** — только последние `limit` сделок «сейчас», без диапазона по времени. На прошлом окне фильтр часто даёт **0 строк** — эталонные `delta_ratio_*` / `cvd_*` отражают это поведение v1 на пустых trades.
- **Funding `ts_utc`**: в ответе Bybit `nextFundingTime` иногда строка; `common.market_features.normalize_funding` может вернуть её как строку цифр (мс) без ISO — в `expected_indicators.json` зафиксировано **ровно то, что вернул v1**.

## Edge cases

| Папка | Смысл |
|-------|--------|
| `no_oi` | `oi.parquet` пустой — имитация отсутствия OI; в `expected_indicators.json` для OI-зависимых полей зафиксировано поведение v1 (`null` / `raises`). |
| `no_liquidations` | Символ и окно без строк в `liquidations.csv` — все rollup count/usd = 0. |
