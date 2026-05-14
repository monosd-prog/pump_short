# Datasets Schema (pump_v2)

Все datasets живут в `/root/pump_short/datasets/` (общая папка с v1).  
Эта документация описывает стандартизованную схему для pump_v2 backtest и indicator модулей.

Подробнее о контракте индикаторов: [`../indicators/CONTRACTS.md`](../indicators/CONTRACTS.md).

## Structure

```text
datasets/
├── klines/{symbol}/{timeframe}.parquet
│   timeframe ∈ {1m, 5m, 15m, 1h}
│   columns: ts_utc, open, high, low, close, volume
│
├── oi_history/{symbol}.parquet
│   columns: ts_utc, oi, oi_value_usd
│
├── funding/{symbol}.parquet
│   columns: ts_utc, funding_rate, funding_rate_abs
│
├── trades/{symbol}/{date}.parquet  (date = YYYY-MM-DD)
│   columns: ts_utc, side, qty, price, value_usd
│
└── liquidations.csv  (managed by collectors/liquidations_ws.py)
    columns: ts_utc, ts_ms, symbol, side, qty, price, value_usd
```

## Status

- **liquidations.csv:** ALREADY LIVE (collector работает с 2026-05-13)
- **klines/, oi_history/, funding/, trades/:** TO BE ETL'd in Phase 4

ETL — отдельная задача в Phase 4 (backtest engine).  
До Phase 4 индикаторы тестируются на synthetic / golden fixtures.
