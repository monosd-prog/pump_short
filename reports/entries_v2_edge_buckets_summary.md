# entries_v2 edge buckets summary

- Total rows analyzed: 1388
- Rows with count threshold >= 20: 104
- Distinct bucket slices: 74

## Missing columns

- None

## Top profitable buckets

| bucket_family | bucket_key | count | tp | sl | timeout | winrate_core | expectancy_pnl_pct | avg_r_multiple |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| dist_to_peak_pct + oi_change_fast_pct bucket | [0.5, 1) | nan | 155 | 117 | 34 | 0 | 0.774834 | 0.681302 | 0.000000 |
| dist_to_peak_pct bucket | [0.5, 1) | 158 | 118 | 36 | 0 | 0.766234 | 0.663302 | 0.000000 |
| risk_profile + dist_to_peak_pct bucket | nan | [0.5, 1) | 158 | 118 | 36 | 0 | 0.766234 | 0.663302 | 0.000000 |
| dist_to_peak_pct + oi_change_fast_pct bucket | [0, 0.5) | nan | 84 | 60 | 19 | 0 | 0.759494 | 0.653112 | 0.000000 |
| dist_to_peak_pct bucket | [0, 0.5) | 87 | 62 | 20 | 0 | 0.756098 | 0.646683 | 0.000000 |
| risk_profile + dist_to_peak_pct bucket | nan | [0, 0.5) | 87 | 62 | 20 | 0 | 0.756098 | 0.646683 | 0.000000 |
| context_score + funding_rate_abs bucket | (216.2, 323.8] | [5e-05, 0.0001) | 29 | 19 | 9 | 1 | 0.678571 | 0.487164 | 0.000000 |
| oi_change_1m_pct bucket | [1, 2) | 26 | 14 | 8 | 1 | 0.636364 | 0.219447 | 0.000000 |
| oi_change_5m_pct bucket | [1, 2) | 26 | 14 | 8 | 1 | 0.636364 | 0.219447 | 0.000000 |
| context_score + funding_rate_abs bucket | (216.2, 323.8] | [0.002, inf) | 31 | 19 | 11 | 0 | 0.633333 | 0.418701 | 0.000000 |

## Top loss-making buckets

| bucket_family | bucket_key | count | tp | sl | timeout | winrate_core | expectancy_pnl_pct | avg_r_multiple |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| symbol | PYRUSDT | 22 | 4 | 18 | 0 | 0.181818 | -0.654545 | 0.000000 |
| dist_to_peak_pct + oi_change_fast_pct bucket | [5, inf) | nan | 278 | 67 | 211 | 0 | 0.241007 | -0.469784 | 0.000000 |
| dist_to_peak_pct + oi_change_fast_pct bucket | [2, 5) | nan | 292 | 85 | 207 | 0 | 0.291096 | -0.367808 | 0.000000 |
| dist_to_peak_pct bucket | [5, inf) | 449 | 135 | 297 | 6 | 0.312500 | -0.285099 | 0.000000 |
| risk_profile + dist_to_peak_pct bucket | nan | [5, inf) | 449 | 135 | 297 | 6 | 0.312500 | -0.285099 | 0.000000 |
| funding_rate_abs bucket | [1e-05, 5e-05) | 24 | 8 | 16 | 0 | 0.333333 | -0.210544 | 0.000000 |
| context_score + funding_rate_abs bucket | (431.4, 539.0] | [0.002, inf) | 58 | 19 | 37 | 0 | 0.339286 | -0.206132 | 0.000000 |
| dist_to_peak_pct bucket | [2, 5) | 411 | 144 | 263 | 1 | 0.353808 | -0.233411 | 0.000000 |
| risk_profile + dist_to_peak_pct bucket | nan | [2, 5) | 411 | 144 | 263 | 1 | 0.353808 | -0.233411 | 0.000000 |
| context_score + oi_change_1m_pct bucket | (431.4, 539.0] | nan | 104 | 36 | 64 | 0 | 0.360000 | -0.137169 | 0.000000 |

## Stability note

Positive-expectancy segments exist: 23 slices have expectancy > 0 with count >= 20.
