# B51 Backfill Summary

Total TIMEOUT rows processed: 12

Breakdown by inferred label:

- neither_hit: 7 (58.33%)
- candidate_from_klines: 5 (41.67%)

Examples CSV (first 20 rows):

trade_id,event_id,run_id,symbol,strategy,opened_ts,outcome_time_utc,entry,tp,sl,backfill_label,backfill_source,backfill_confidence,reconstructed_mfe_pct,reconstructed_mae_pct,__source_file
20260221_213400_e2e_fast0_trade_e9138cdb,20260221_213400_e2e_fast0_1_d00def08,20260221_213400_e2e,BTCUSDT,short_pump_fast0,,2026-02-21T18:34:04+00:00,,,,candidate_from_klines,klines,medium,,,/root/pump_short/datasets/date=20260221/strategy=short_pump_fast0/mode=live/outcomes_v3.csv
20260221_222023_e2e_fast0_1_22226109_trade,20260221_222023_e2e_fast0_1_22226109,20260221_222023_e2e,BTCUSDT,short_pump_fast0,,2026-02-21T19:20:27.258382+00:00,,,,candidate_from_klines,klines,medium,,,/root/pump_short/datasets/date=20260221/strategy=short_pump_fast0/mode=live/outcomes_v3.csv
20260221_232959_e2e_fast0_1_e6e4ae6c_trade,20260221_232959_e2e_fast0_1_e6e4ae6c,20260221_232959_e2e,BTCUSDT,short_pump_fast0,,2026-02-21T20:30:03.809042+00:00,,,,candidate_from_klines,klines,medium,,,/root/pump_short/datasets/date=20260221/strategy=short_pump_fast0/mode=live/outcomes_v3.csv
20260223_143840_entry_fast_trade,20260223_143840_entry_fast,20260223_143840,IDEXUSDT,short_pump,,2026-02-23T12:59:14.744708+00:00,,,,neither_hit,path_1m,high,,,/root/pump_short/datasets/date=20260223/strategy=short_pump/mode=live/outcomes_v3.csv
9ee6c5b4-d4be-47c5-a0e7-f9fed3f77438,manual_test_1,manual_test_1,POWERUSDT,short_pump_fast0,,2026-02-26 14:17:00+00:00,,,,candidate_from_klines,klines,medium,,,/root/pump_short/datasets/date=20260226/strategy=short_pump_fast0/mode=live/outcomes_v3.csv
20260228_135405_entry_fast_trade,20260228_135405_entry_fast,20260228_135405,SIRENUSDT,short_pump,,2026-02-28T11:24:15.646923+00:00,,,,neither_hit,path_1m,high,,,/root/pump_short/datasets/date=20260228/strategy=short_pump/mode=paper/outcomes_v3.csv
20260303_034725_entry_fast_trade,20260303_034725_entry_fast,20260303_034725,KITEUSDT,short_pump,,2026-03-03T01:47:46.658315+00:00,,,,neither_hit,path_1m,high,,,/root/pump_short/datasets/date=20260303/strategy=short_pump/mode=paper/outcomes_v3.csv
20260306_005550_entry_fast_trade,20260306_005550_entry_fast,20260306_005550,XCNUSDT,short_pump,,2026-03-05T22:41:07.923383+00:00,,,,neither_hit,path_1m,high,,,/root/pump_short/datasets/date=20260305/strategy=short_pump/mode=paper/outcomes_v3.csv
short_pump:20260314_030255:20260314_030255_entry_fast:CYBERUSDT,20260314_030255_entry_fast,20260314_030255,CYBERUSDT,short_pump,,2026-03-14T01:03:20.609224+00:00,0.5712,0.5643456,0.5769120000000001,neither_hit,path_1m,high,0.19257703081232314,0.6127450980392065,/root/pump_short/datasets/date=20260314/strategy=short_pump/mode=live/outcomes_v3.csv
short_pump_fast0:20260316_102525:20260316_102525_fast0_6_29e0a253:ARIAUSDT,20260316_102525_fast0_6_29e0a253,20260316_102525,ARIAUSDT,short_pump_fast0,2026-03-16 07:26:35+0000,2026-03-16 12:51:30+00:00,0.1742048339999999,0.1720799599999999,0.1759117,candidate_from_klines,klines,medium,,,/root/pump_short/datasets/date=20260316/strategy=short_pump_fast0/mode=paper/outcomes_v3.csv
short_pump:20260318_193240:20260318_193240_entry_fast:PTBUSDT,20260318_193240_entry_fast,20260318_193240,PTBUSDT,short_pump,,2026-03-18T17:53:26.263756+00:00,0.0010929,0.0010797851999999,0.001103829,neither_hit,path_1m,high,0.7777472778845315,0.14639948760179697,/root/pump_short/datasets/date=20260318/strategy=short_pump/mode=paper/outcomes_v3.csv
short_pump:20260319_030937:20260319_030937_entry_fast:SNTUSDT,20260319_030937_entry_fast,20260319_030937,SNTUSDT,short_pump,,2026-03-19T01:35:12.722457+00:00,0.01119,0.01105572,0.0113019,neither_hit,path_1m,high,0.5361930294906257,0.45576407506701483,/root/pump_short/datasets/date=20260319/strategy=short_pump/mode=live/outcomes_v3.csv
