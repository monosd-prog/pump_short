from __future__ import annotations

import json
from typing import Any, Dict, Iterable, Mapping

# Canonical contract for event feature rows across strategies.
# Keep "missing" consistent with existing dataset_schema normalize_* which uses "".
MISSING: str = ""


CANONICAL_EVENT_FIELDS: list[str] = [
    # identity/meta
    "strategy",
    "mode",
    "source_mode",
    "symbol",
    "run_id",
    "event_id",
    "side",
    "time_utc",
    "wall_time_utc",
    # core context
    "stage",
    "entry_ok",
    "skip_reasons",
    "context_score",
    "price",
    "dist_to_peak_pct",
    # delta
    "delta_ratio_30s",
    "delta_ratio_1m",
    "cvd_delta_ratio_30s",
    "cvd_delta_ratio_1m",
    # oi
    "oi_change_fast_pct",
    "oi_change_1m_pct",
    "oi_change_5m_pct",
    # funding
    "funding_rate",
    "funding_rate_abs",
    # liquidations
    "liq_short_count_30s",
    "liq_short_usd_30s",
    "liq_long_count_30s",
    "liq_long_usd_30s",
    "liq_short_count_1m",
    "liq_short_usd_1m",
    "liq_long_count_1m",
    "liq_long_usd_1m",
    # volume
    "volume_1m",
    "volume_5m",
    "volume_sma_20",
    "volume_zscore_20",
    # velocity / acceleration (candle-only)
    "price_change_30s_pct",
    "price_change_1m_pct",
    "price_change_3m_pct",
    "accel_30s_vs_3m",
    # pump shape / structure (candle-only)
    "green_candles_5",
    "max_candle_body_pct_5",
    "avg_candle_body_pct_5",
    "upper_wick_ratio_last",
    "lower_wick_ratio_last",
    "wick_body_ratio_last",
    # relative volume anomaly
    "volume_ratio_1m_20",
    "volume_ratio_5m_20",
    # time-life (best-effort candle-only + optional timestamps)
    "time_since_peak_sec",
    "time_since_signal_sec",
    "pump_age_sec",
    # microstructure
    "spread_bps",
    "orderbook_imbalance_10",
    # misc
    "outcome_label",
    "payload_json",
]


def canonical_event_fields() -> list[str]:
    return list(CANONICAL_EVENT_FIELDS)


def _as_payload_json(payload: Any) -> str:
    if payload is None:
        return MISSING
    if isinstance(payload, str):
        return payload
    if isinstance(payload, Mapping):
        try:
            return json.dumps(dict(payload), ensure_ascii=False)
        except Exception:
            return MISSING
    return MISSING


def normalize_event_feature_row(
    *,
    base: Mapping[str, Any] | None = None,
    payload: Mapping[str, Any] | None = None,
    extra: Mapping[str, Any] | None = None,
) -> Dict[str, Any]:
    """
    Build a canonical event feature row dict with stable keys across strategies.

    - Always returns all CANONICAL_EVENT_FIELDS keys.
    - Does NOT touch trading logic; only normalizes logging payloads.
    - Missing values are filled with "" (project convention for CSV schema normalization).
    """
    row: Dict[str, Any] = {k: MISSING for k in CANONICAL_EVENT_FIELDS}
    if base:
        row.update(dict(base))
    if payload:
        row.update(dict(payload))
        if not row.get("payload_json"):
            row["payload_json"] = _as_payload_json(payload)
    if extra:
        row.update(dict(extra))
    if not row.get("payload_json"):
        row["payload_json"] = _as_payload_json(payload)
    return row


def missing_canonical_fields(columns: Iterable[str]) -> list[str]:
    s = set(columns)
    return [c for c in CANONICAL_EVENT_FIELDS if c not in s]

