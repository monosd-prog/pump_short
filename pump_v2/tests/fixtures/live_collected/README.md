# Live-collected raw fixtures (pump_v2)

Реальные рыночные потоки, собранные **live** через Bybit WebSocket + REST за фиксированное окно (~45 минут). Используются для regression-тестов индикаторов v2 против **v1 эталона** (`expected_indicators.json`).

## Символы

`BTCUSDT`, `ETHUSDT`, `SOLUSDT`, `DOGEUSDT`, `LINKUSDT`

## Структура

```text
live_collected/
├── README.md
├── meta.json                 # период сбора, stats по символам
├── _compute_expected.py      # v1 expected из parquet/csv
├── btcusdt/
│   ├── trades.parquet        # WS publicTrade (ts_utc, ts_ms, side, price, qty, trade_id)
│   ├── oi_history.parquet    # REST OI poll каждые 30s
│   ├── klines_1m.parquet
│   ├── klines_5m.parquet     # REST kline poll каждые 5m
│   ├── funding.json          # WS ticker + REST tickers snapshot
│   ├── liquidations.csv      # slice из datasets/liquidations.csv за окно сбора
│   ├── meta.json             # window_start/end = collection period
│   └── expected_indicators.json
└── ...
```

## Пересоздание

Требуется сеть. **Не останавливайте** `pump-liquidations-collector` — ликвидации пишутся в `datasets/liquidations.csv`.

```bash
# 1) Проверка liquidations collector
systemctl status pump-liquidations-collector --no-pager | head -5
tail -1 datasets/liquidations.csv

# 2) Сбор (~45 мин, flush trades каждые 60s)
python3 pump_v2/tests/fixtures/_live_collect.py

# Опционально: connectivity check (60s)
python3 pump_v2/tests/fixtures/_live_collect.py --dry-run

# 3) v1 expected
python3 pump_v2/tests/fixtures/live_collected/_compute_expected.py
```

Скрипт сбора: `pump_v2/tests/fixtures/_live_collect.py`  
Константа длительности: `COLLECT_DURATION_MIN = 45`

## Период сбора (зафиксированный)

| Поле | Значение |
|------|----------|
| start | `2026-05-15T07:05:03+00:00` |
| end | `2026-05-15T07:50:54+00:00` |
| duration | 45 min |

Итоговые stats — в `meta.json` → `stats`.

## Ограничения

- Окно **короткое** (~45 мин): мало 5m-свечей для `atr_pct` / `volume_zscore` (lookback 14/50).
- Quiet-альты могут иметь **0 liquidations** за окно — rollup = 0, это валидный кейс.
- OI history — дискретные REST-снапшоты (30s), не непрерывный WS OI tape.
- Эти fixtures **не заменяют** `symbols/*` golden edge-cases (`no_oi`, `no_liquidations` с пустыми trades).

## Использование в тестах

```python
import json
from pathlib import Path
import pandas as pd

base = Path("pump_v2/tests/fixtures/live_collected/btcusdt")
exp = json.loads((base / "expected_indicators.json").read_text())
trades = pd.read_parquet(base / "trades.parquet")
# assert v2_delta_ratio(trades, ...) == exp["delta_ratio_30s"]["value"]
```
