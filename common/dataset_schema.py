from __future__ import annotations

from typing import Any, Dict

SCHEMA_VERSION = 3

EVENT_FIELDS_V2 = [
    "schema_version",
    "run_id",
    "event_id",
    "symbol",
    "strategy",
    "mode",
    "source_mode",
    "side",
    "wall_time_utc",
    "time_utc",
    "stage",
    "entry_ok",
    "skip_reasons",
    "context_score",
    "price",
    "dist_to_peak_pct",
    # Pre-entry delta ratios (logged by watcher/entry; required for factor_report)
    "delta_ratio_30s",
    "delta_ratio_1m",
    "cvd_delta_ratio_30s",
    "cvd_delta_ratio_1m",
    "cvd_abs_5m",
    "cvd_ratio_5m",
    "oi_change_5m_pct",
    "oi_change_1m_pct",
    "oi_change_fast_pct",
    "funding_rate",
    "funding_rate_abs",
    "ls_ratio_buy",
    "ls_ratio_sell",
    "oi_abs",
    "oi_abs_usd",
    "liq_short_count_30s",
    "liq_short_usd_30s",
    "liq_long_count_30s",
    "liq_long_usd_30s",
    # 1m liquidation window (present in canonical contract; may be empty for older runs)
    "liq_short_count_1m",
    "liq_short_usd_1m",
    "liq_long_count_1m",
    "liq_long_usd_1m",
    # Real USD liquidation values (qty * price, not just qty)
    "liq_short_usd_30s_real",
    "liq_long_usd_30s_real",
    "liq_short_usd_1m_real",
    "liq_long_usd_1m_real",
    # Market microstructure / volume features (used by FAST0 + factor_report)
    "volume_1m",
    "volume_5m",
    "volume_sma_20",
    "volume_zscore_20",
    # Velocity / acceleration (candle-only)
    "price_change_30s_pct",
    "price_change_1m_pct",
    "price_change_3m_pct",
    "accel_30s_vs_3m",
    # Shape / structure (candle-only)
    "green_candles_5",
    "max_candle_body_pct_5",
    "avg_candle_body_pct_5",
    "upper_wick_ratio_last",
    "lower_wick_ratio_last",
    "wick_body_ratio_last",
    # Relative volume anomaly
    "volume_ratio_1m_20",
    "volume_ratio_5m_20",
    # Time-life (best-effort)
    "time_since_peak_sec",
    "time_since_signal_sec",
    "pump_age_sec",
    "spread_bps",
    "orderbook_imbalance_10",
    # Price structure (RSI + MA/EMA)
    "rsi_14_1m",
    "ma_20_1m",
    "ema_20_1m",
    "dist_to_ma20_pct",
    "dist_to_ema20_pct",
    # Outcome label can be logged on outcome events (canonical contract)
    "outcome_label",
    "payload_json",
]

EVENT_FIELDS_V3 = [
    *EVENT_FIELDS_V2[:-1],
    "payload_json",
]

TRADE_FIELDS_V2 = [
    "schema_version",
    "trade_id",
    "event_id",
    "run_id",
    "symbol",
    "strategy",
    "mode",
    "source_mode",
    "side",
    "entry_time_utc",
    "entry_price",
    "tp_price",
    "sl_price",
    "trade_type",
]

OUTCOME_FIELDS_V2 = [
    "schema_version",
    "trade_id",
    "event_id",
    "run_id",
    "symbol",
    "strategy",
    "mode",
    "source_mode",
    "side",
    "outcome_time_utc",
    "outcome",
    "pnl_pct",
    "hold_seconds",
    "mae_pct",
    "mfe_pct",
    "details_json",
]

TRADE_FIELDS_V3 = TRADE_FIELDS_V2
# Extended for live reconciliation: opened_ts, entry, tp, sl, exit_price, pnl_r, pnl_usd, order_id, etc.
OUTCOME_FIELDS_V3 = OUTCOME_FIELDS_V2 + [
    "opened_ts",
    "entry",
    "tp",
    "sl",
    "exit_price",
    "pnl_r",
    "pnl_usd",
    "risk_profile",
    "order_id",
    "position_idx",
    "notional_usd",
    "leverage",
    "margin_mode",
    "outcome_source",
    "data_available",
]


def _fill(fields: list[str], row: Dict[str, Any]) -> Dict[str, Any]:
    return {k: row.get(k, "") for k in fields}


def normalize_event_v2(row: Dict[str, Any]) -> Dict[str, Any]:
    payload_json = row.get("entry_payload") or row.get("payload_json") or ""
    return _fill(
        EVENT_FIELDS_V2,
        {
            **row,
            "schema_version": 2,
            "entry_ok": int(bool(row.get("entry_ok", False))),
            "payload_json": payload_json,
            "source_mode": row.get("source_mode", ""),
        },
    )


def normalize_event_v3(row: Dict[str, Any]) -> Dict[str, Any]:
    payload_json = row.get("entry_payload") or row.get("payload_json") or ""
    return _fill(
        EVENT_FIELDS_V3,
        {
            **row,
            "schema_version": SCHEMA_VERSION,
            "entry_ok": int(bool(row.get("entry_ok", False))),
            "outcome_label": row.get("outcome_label", ""),
            "payload_json": payload_json,
            "source_mode": row.get("source_mode", ""),
        },
    )


def normalize_trade_v2(row: Dict[str, Any]) -> Dict[str, Any]:
    return _fill(
        TRADE_FIELDS_V2,
        {
            **row,
            "schema_version": 2,
            "entry_time_utc": row.get("entry_time_utc") or row.get("time_utc") or "",
            "entry_price": row.get("entry_price") or row.get("price") or "",
            "source_mode": row.get("source_mode", ""),
        },
    )


def normalize_trade_v3(row: Dict[str, Any]) -> Dict[str, Any]:
    return _fill(
        TRADE_FIELDS_V3,
        {
            **row,
            "schema_version": SCHEMA_VERSION,
            "entry_time_utc": row.get("entry_time_utc") or row.get("time_utc") or "",
            "entry_price": row.get("entry_price") or row.get("price") or "",
            "source_mode": row.get("source_mode", ""),
        },
    )


def normalize_outcome_v2(row: Dict[str, Any]) -> Dict[str, Any]:
    return _fill(
        OUTCOME_FIELDS_V2,
        {
            **row,
            "schema_version": 2,
            "outcome_time_utc": (
                row.get("outcome_time_utc")
                or row.get("exit_time_utc")
                or row.get("hit_time_utc")
                or ""
            ),
            "outcome": row.get("end_reason") or row.get("outcome") or "UNKNOWN",
            "details_json": row.get("details_payload") or row.get("details_json") or "",
            "source_mode": row.get("source_mode", ""),
        },
    )


def normalize_outcome_v3(row: Dict[str, Any]) -> Dict[str, Any]:
    return _fill(
        OUTCOME_FIELDS_V3,
        {
            **row,
            "schema_version": SCHEMA_VERSION,
            "outcome_time_utc": (
                row.get("outcome_time_utc")
                or row.get("exit_time_utc")
                or row.get("hit_time_utc")
                or ""
            ),
            "outcome": row.get("end_reason") or row.get("outcome") or "UNKNOWN",
            "details_json": row.get("details_payload") or row.get("details_json") or "",
            "source_mode": row.get("source_mode", ""),
            "outcome_source": row.get("outcome_source", ""),
            "data_available": row.get("data_available", True),
        },
    )
