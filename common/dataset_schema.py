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
    "side",
    "wall_time_utc",
    "time_utc",
    "stage",
    "entry_ok",
    "skip_reasons",
    "context_score",
    "price",
    "dist_to_peak_pct",
    "cvd_delta_ratio_30s",
    "cvd_delta_ratio_1m",
    "oi_change_5m_pct",
    "oi_change_1m_pct",
    "oi_change_fast_pct",
    "funding_rate",
    "funding_rate_abs",
    "liq_short_count_30s",
    "liq_short_usd_30s",
    "liq_long_count_30s",
    "liq_long_usd_30s",
    "payload_json",
]

EVENT_FIELDS_V3 = [
    *EVENT_FIELDS_V2[:-1],
    "liq_short_count_1m",
    "liq_short_usd_1m",
    "liq_long_count_1m",
    "liq_long_usd_1m",
    "outcome_label",
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
OUTCOME_FIELDS_V3 = OUTCOME_FIELDS_V2


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
        },
    )
