# entries_v1 edge buckets summary

- Total rows analyzed: 1388
- Rows with count threshold >= 20: 19
- Distinct bucket slices: 19

## Missing columns

- risk_profile
- context_score bucket
- dist_to_tp_pct bucket
- dist_to_sl_pct bucket
- oi_change_1m_pct bucket
- oi_change_5m_pct bucket
- funding_rate_abs bucket
- liq_long_usd_30s bucket
- liq_short_usd_30s bucket
- risk_profile + context_score bucket
- context_score + oi_change_1m_pct bucket

## Top profitable buckets

- None

## Top loss-making buckets

| bucket_family | bucket_key | count | tp | sl | winrate | expectancy | avg_pnl_pct |
| --- | --- | --- | --- | --- | --- | --- | --- |
| symbol | PYRUSDT | 22 | 4 | 18 | 0.181818 | -0.636364 | -0.654545 |
| symbol + hour-of-day | PYRUSDT | -1 | 22 | 4 | 18 | 0.181818 | -0.636364 | -0.654545 |
| symbol + hour-of-day | SIRENUSDT | -1 | 313 | 113 | 199 | 0.362179 | -0.275641 | -0.214830 |
| symbol | SIRENUSDT | 314 | 114 | 199 | 0.364217 | -0.271565 | -0.212132 |
| strategy | short_pump | 742 | 305 | 416 | 0.423024 | -0.153953 | -0.062936 |
| hour-of-day | -1 | 1351 | 568 | 761 | 0.427389 | -0.145222 | -0.065700 |
| strategy | short_pump_fast0 | 646 | 271 | 360 | 0.429477 | -0.141046 | -0.068693 |
| symbol | PIPPINUSDT | 48 | 21 | 27 | 0.437500 | -0.125000 | -0.000952 |
| symbol + hour-of-day | PIPPINUSDT | -1 | 48 | 21 | 27 | 0.437500 | -0.125000 | -0.000952 |
| symbol | LYNUSDT | 172 | 74 | 95 | 0.437870 | -0.124260 | -0.034650 |

## Stability note

No positive-expectancy segments met the minimum count threshold.
