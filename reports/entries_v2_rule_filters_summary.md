# entries_v2 rule filters summary

- Baseline expectancy_pnl_pct: -0.065615
- Baseline rows: 1388
- Early sample rows: 694
- Late sample rows: 694

## Rule Metrics

| rule | rows_kept | keep_rate | tp | sl | timeout | core_winrate | expectancy_pnl_pct | delta_vs_baseline | early_expectancy_pnl_pct | late_expectancy_pnl_pct | robustness_score |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| baseline | 1388 | 1.000000 | 576 | 776 | 7 | 0.426036 | -0.065615 | 0.000000 | -0.053951 | -0.077280 | -0.077280 |
| A_dist_to_peak_lt_1 | 245 | 0.176513 | 180 | 56 | 0 | 0.762712 | 0.657401 | 0.723016 | 0.548387 | 0.724100 | 0.548387 |
| B_dist_to_peak_lt_2 | 518 | 0.373199 | 296 | 210 | 0 | 0.584980 | 0.265439 | 0.331054 | 0.171028 | 0.331899 | 0.171028 |
| C_exclude_dist_to_peak_ge_5 | 939 | 0.676513 | 441 | 479 | 1 | 0.479348 | 0.039335 | 0.104950 | 0.061337 | 0.019767 | 0.019767 |
| D_exclude_symbol_PYRUSDT | 1366 | 0.984150 | 572 | 758 | 7 | 0.430075 | -0.056130 | 0.009485 | -0.043399 | -0.068861 | -0.068861 |
| E_dist_to_peak_lt_1_and_best_oi_bucket | 13 | 0.009366 | 6 | 5 | 0 | 0.545455 | 0.171616 | 0.237231 | 0.833333 | -0.395570 | -0.395570 |
| F_dist_to_peak_lt_1_and_best_funding_bucket | 8 | 0.005764 | 5 | 1 | 0 | 0.833333 | 0.684529 | 0.750144 | 0.650000 | 0.719058 | 0.650000 |
| G_exclude_worst_dist_bucket | 939 | 0.676513 | 441 | 479 | 1 | 0.479348 | 0.039335 | 0.104950 | 0.061337 | 0.019767 | 0.019767 |
