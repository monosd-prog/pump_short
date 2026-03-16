from __future__ import annotations

import inspect
import json
import traceback
from typing import Any

import numpy as np
import pandas as pd

from analytics.utils import dprint, DEBUG_ENABLED


DEBUG_FEATURES = True


def _debug_features(message: str) -> None:
    dprint(DEBUG_ENABLED and DEBUG_FEATURES, message)


def _safe_json_load(value: Any) -> dict:
    if not isinstance(value, str) or not value.strip():
        return {}
    try:
        data = json.loads(value)
        return data if isinstance(data, dict) else {}
    except json.JSONDecodeError:
        return {}


def _get_nested(data: dict, keys: list[str]) -> Any:
    current = data
    for key in keys:
        if not isinstance(current, dict) or key not in current:
            return None
        current = current[key]
    return current


def _coalesce(*values: Any) -> Any:
    for value in values:
        if value is not None and not (isinstance(value, float) and np.isnan(value)):
            return value
    return None


def _extract_liq_usd(liq_stats: Any, side: str, window: str) -> float:
    if not isinstance(liq_stats, dict):
        return np.nan

    candidates = [
        (side, window, "usd"),
        (side, f"{window}_usd"),
        (f"{side}_{window}", "usd"),
        (f"{side}_{window}_usd",),
    ]

    for path in candidates:
        value = _get_nested(liq_stats, list(path))
        if value is not None:
            if isinstance(value, dict):
                value = value.get("usd")
            return pd.to_numeric(value, errors="coerce")

    side_dict = liq_stats.get(side)
    if isinstance(side_dict, dict):
        value = side_dict.get(window)
        if isinstance(value, dict):
            value = value.get("usd")
        return pd.to_numeric(value, errors="coerce")

    return np.nan


def _extract_context_score(snapshot: dict) -> float:
    return _coalesce(
        snapshot.get("context_score"),
        _get_nested(snapshot, ["context", "score"]),
        snapshot.get("contextScore"),
    )


def _extract_context_parts(snapshot: dict) -> Any:
    return _coalesce(
        snapshot.get("context_parts"),
        _get_nested(snapshot, ["context", "parts"]),
    )


def _extract_delta_ratio(snapshot: dict) -> float:
    """Extract delta_ratio_30s with fallback to alternative field names."""
    return _coalesce(
        snapshot.get("delta_ratio_30s"),
        snapshot.get("delta_ratio"),
        snapshot.get("deltaRatio30s"),
        snapshot.get("delta_ratio_fast_late_max"),
        snapshot.get("delta_ratio_fast"),
        snapshot.get("cvd_delta_ratio_30s"),
    )


def _fill_from_events(result: pd.DataFrame) -> pd.DataFrame:
    dprint(DEBUG_ENABLED, "[DEBUG_FEATURES] ENTER _fill_from_events (this definition)")
    df = result
    dprint(DEBUG_ENABLED, "[DEBUG_FEATURES] ENTER _fill_from_events rows=", len(df))
    dprint(
        DEBUG_ENABLED,
        "[DEBUG_FEATURES] _fill_from_events columns:",
        [c for c in df.columns if "payload" in c][:20],
    )
    _debug_features(f"[DEBUG_FEATURES] enter _fill_from_events rows={len(df)}")
    payload_cols = sorted([c for c in df.columns if "payload" in c])
    _debug_features(f"[DEBUG_FEATURES] payload cols={payload_cols}")

    # Priority 1: Direct mapping from events DataFrame columns (base column -> _event suffix -> json fallback)
    # Use notna() for "has values"; 0.0 is valid and must not be overwritten by NaN
    direct_cols_mapping = {
        "liq_short_usd_30s": ["liq_short_usd_30s", "liq_short_usd_30s_event"],
        "liq_long_usd_30s": ["liq_long_usd_30s", "liq_long_usd_30s_event"],
        "liq_short_usd_1m": ["liq_short_usd_1m", "liq_short_usd_1m_event"],
        "liq_long_usd_1m": ["liq_long_usd_1m", "liq_long_usd_1m_event"],
        "liq_short_count_30s": ["liq_short_count_30s", "liq_short_count_30s_event"],
        "liq_long_count_30s": ["liq_long_count_30s", "liq_long_count_30s_event"],
        "delta_ratio_30s": ["delta_ratio_30s", "delta_ratio_30s_event"],
        "funding_rate": ["funding_rate", "funding_rate_event"],
        "dist_to_peak_pct": ["dist_to_peak_pct", "dist_to_peak_pct_event"],
        "stage": ["stage", "stage_event"],
        "entry_ok": ["entry_ok", "entry_ok_event", "entryOk", "entryOk_event"],
        "oi_change_fast_pct": ["oi_change_fast_pct", "oi_change_fast_pct_event"],
        "oi_change_1m_pct": ["oi_change_1m_pct", "oi_change_1m_pct_event"],
        "oi_change_5m_pct": ["oi_change_5m_pct", "oi_change_5m_pct_event"],
        "cvd_delta_ratio_30s": ["cvd_delta_ratio_30s", "cvd_delta_ratio_30s_event"],
        "cvd_delta_ratio_1m": ["cvd_delta_ratio_1m", "cvd_delta_ratio_1m_event"],
    }
    
    filled_from_direct = set()
    
    for target_col, source_candidates in direct_cols_mapping.items():
        for source_col in source_candidates:
            if source_col not in df.columns:
                continue
            source_values = pd.to_numeric(df[source_col], errors="coerce")
            if not source_values.notna().any():
                continue
            if target_col in result.columns:
                existing = pd.to_numeric(result[target_col], errors="coerce")
                result[target_col] = existing.combine_first(source_values)
            else:
                result[target_col] = source_values
            filled_from_direct.add(target_col)
            _debug_features(f"[DEBUG_FEATURES] filled {target_col} from {source_col} (nonnull={source_values.notna().sum()})")
            break

    # Pass-through non-numeric event columns (e.g. skip_reasons)
    for target_col, source_candidates in [
        ("skip_reasons", ["skip_reasons", "skip_reasons_event"]),
    ]:
        for source_col in source_candidates:
            if source_col not in df.columns or not df[source_col].notna().any():
                continue
            if target_col in result.columns:
                result[target_col] = result[target_col].combine_first(df[source_col])
            else:
                result[target_col] = df[source_col].copy()
            break

    # Try to get details_json from events first, then from outcomes
    if "details_json_event" in df.columns:
        events_details = df["details_json_event"].apply(_safe_json_load)
    elif "details_json" in df.columns:
        events_details = df["details_json"].apply(_safe_json_load)
    else:
        events_details = pd.Series([{}] * len(df), index=df.index)
    
    payload_candidates = ["payload_json_event", "payload_json", "payload_json_y", "payload_json_x"]
    payload_col = next((c for c in payload_candidates if c in df.columns), None)
    
    # Also check details_json for entry_snapshot or details_payload (for v3)
    details_json_col = None
    for candidate in ["details_json", "details_json_event"]:
        if candidate in df.columns:
            details_json_col = candidate
            break
    dprint(DEBUG_ENABLED, "[DEBUG_FEATURES] payload_col:", payload_col)
    if payload_col is None:
        dprint(DEBUG_ENABLED, "[DEBUG_FEATURES] EXIT _fill_from_events: no payload col")
        dprint(
            DEBUG_ENABLED,
            "[DEBUG_FEATURES] EXIT _fill_from_events delta_ratio_30s nonnull:",
            (df["delta_ratio_30s"].notna().sum() if "delta_ratio_30s" in df.columns else "NO_COL"),
        )
        return df

    dprint(
        DEBUG_ENABLED,
        "[DEBUG_FEATURES] payload nonnull:",
        int(df[payload_col].notna().sum()),
        "/",
        len(df),
    )
    dprint(
        DEBUG_ENABLED,
        "[DEBUG_FEATURES] payload samples:",
        df[payload_col].dropna().astype(str).head(2).str.slice(0, 200).tolist(),
    )

    dprint(DEBUG_ENABLED, "[DEBUG_FEATURES] REACHED PARSE BLOCK")
    def _parse_payload(x: Any) -> dict:
        if x is None or x == "" or (isinstance(x, float) and np.isnan(x)):
            return {}
        if isinstance(x, dict):
            return x
        try:
            return json.loads(x)
        except Exception:
            return {}

    payload_details = df[payload_col].apply(_parse_payload)

    extractors = {
        "delta_ratio_30s": lambda d: _coalesce(
            d.get("delta_ratio_30s"),
            d.get("delta_ratio_fast_late_max"),
            d.get("delta_ratio_fast"),
            d.get("cvd_delta_ratio_30s"),
            _get_nested(d, ["entry_snapshot", "delta_ratio_30s"]),
            _get_nested(d, ["details_payload", "delta_ratio_30s"]),
        ),
        "liq_short_usd_30s": lambda d: _coalesce(
            d.get("liq_short_usd_30s"),
            _get_nested(d, ["entry_snapshot", "liq_stats", "short", "30s", "usd"]),
            _get_nested(d, ["details_payload", "liq_short_usd_30s"]),
        ),
        "liq_long_usd_30s": lambda d: _coalesce(
            d.get("liq_long_usd_30s"),
            _get_nested(d, ["entry_snapshot", "liq_stats", "long", "30s", "usd"]),
            _get_nested(d, ["details_payload", "liq_long_usd_30s"]),
        ),
        "liq_short_count_30s": lambda d: d.get("liq_short_count_30s"),
        "liq_long_count_30s": lambda d: d.get("liq_long_count_30s"),
        "funding_rate": lambda d: _coalesce(
            d.get("funding_rate"),
            _get_nested(d, ["entry_snapshot", "funding_rate"]),
            _get_nested(d, ["details_payload", "funding_rate"]),
        ),
        "oi_change_fast_pct": lambda d: _coalesce(
            d.get("oi_change_fast_pct"),
            d.get("oi_change_1m_pct"),
        ),
        "dist_to_peak_pct": lambda d: _coalesce(
            d.get("dist_to_peak_pct"),
            _get_nested(d, ["entry_snapshot", "dist_to_peak_pct"]),
            _get_nested(d, ["details_payload", "dist_to_peak_pct"]),
        ),
    }

    # Priority 2: Extract from JSON (only fill missing values, don't overwrite direct columns)
    for col, getter in extractors.items():
        # Skip if already filled from direct columns
        if col in filled_from_direct:
            continue
        values = pd.to_numeric(events_details.apply(getter), errors="coerce")
        if col in result.columns:
            result[col] = result[col].combine_first(values)
        else:
            result[col] = values
    
    # Also try extracting from details_json if payload_col is missing or empty
    if details_json_col and (payload_col is None or df[payload_col].isna().all()):
        details_data = df[details_json_col].apply(_safe_json_load)
        # Try entry_snapshot or details_payload
        for source_key in ["entry_snapshot", "details_payload"]:
            snapshots = details_data.apply(lambda d: d.get(source_key) or {})
            if snapshots.apply(lambda s: bool(s)).any():
                for col, getter in extractors.items():
                    # Skip if already filled from direct columns
                    if col in filled_from_direct:
                        continue
                    values = pd.to_numeric(snapshots.apply(getter), errors="coerce")
                    if col in result.columns:
                        result[col] = result[col].combine_first(values)
                    else:
                        result[col] = values
                break

    if "delta_ratio_30s" not in result.columns:
        result["delta_ratio_30s"] = np.nan

    before_nonnull = int(result["delta_ratio_30s"].notna().sum())
    parsed_ok = 0
    extracted = []
    for x in df[payload_col].tolist():
        obj = _parse_payload(x)
        if obj:
            parsed_ok += 1
        extracted.append(obj.get("delta_ratio_30s"))

    extracted_series = np.array(extracted, dtype="float64")
    mask = np.isnan(df["delta_ratio_30s"].to_numpy(dtype="float64"))
    df.loc[mask, "delta_ratio_30s"] = extracted_series[mask]
    after = int(df["delta_ratio_30s"].notna().sum())

    dprint(DEBUG_ENABLED, "[DEBUG_FEATURES] parsed_ok:", parsed_ok, "/", len(df))
    dprint(
        DEBUG_ENABLED,
        "[DEBUG_FEATURES] extracted_nonnull:",
        int(np.sum(~np.isnan(extracted_series))),
        "/",
        len(df),
    )
    dprint(
        DEBUG_ENABLED,
        "[DEBUG_FEATURES] delta_ratio_30s nonnull before/after:",
        before_nonnull,
        "->",
        after,
    )
    dprint(
        DEBUG_ENABLED,
        "[DEBUG_FEATURES] delta_ratio_30s samples:",
        df["delta_ratio_30s"].dropna().head(5).tolist(),
    )

    parsed_rows = int(payload_details.apply(lambda d: bool(d)).sum())
    for col in ["delta_ratio_30s", "delta_ratio_1m", "delta_ratio_3m"]:
        extracted = pd.to_numeric(
            payload_details.apply(lambda d, c=col: d.get(c)), errors="coerce"
        )
        if col in result.columns:
            result[col] = result[col].fillna(extracted)
        else:
            result[col] = extracted

    nonnull_delta = int(result["delta_ratio_30s"].notna().sum())
    _debug_features(
        f"[DEBUG_FEATURES] payload parsed rows={parsed_rows} delta_ratio_30s nonnull={nonnull_delta}"
    )

    dprint(
        DEBUG_ENABLED,
        "[DEBUG_FEATURES] EXIT _fill_from_events final nonnull delta_ratio_30s:",
        (df["delta_ratio_30s"].notna().sum() if "delta_ratio_30s" in df.columns else "NO_COL"),
    )
    return df


def parse_features(df: pd.DataFrame, json_col: str = "details_json") -> pd.DataFrame:
    if df.empty:
        return df.copy()

    result = df.copy()
    _debug_features("[DEBUG_FEATURES] fn=parse_features")
    _debug_features(f"[DEBUG_FEATURES] columns={list(result.columns[:30])}")
    payload_col = None
    for candidate in [
        "payload_json_event",
        "payload_json",
        "payload_json_y",
        "payload_json_x",
    ]:
        if candidate in result.columns:
            payload_col = candidate
            break
    _debug_features(
        f"[DEBUG_FEATURES] chosen_payload_col={payload_col} "
        f"nonnull={int(result[payload_col].notna().sum()) if payload_col else 0}/{len(result)}"
    )

    if json_col in result.columns:
        details = result[json_col].apply(_safe_json_load)
    else:
        details = pd.Series([{}] * len(result), index=result.index)

    snapshots = details.apply(lambda d: d.get("entry_snapshot") or {})

    result["context_score"] = snapshots.apply(_extract_context_score)
    result["context_parts"] = snapshots.apply(_extract_context_parts)
    result["delta_ratio_30s"] = snapshots.apply(_extract_delta_ratio)

    # liq_*: prefer existing columns from join (e.g. events_v3), then fill from snapshots
    # Use combine_first so 0.0 from join is preserved (do not overwrite with NaN from empty snapshot)
    liq_stats = snapshots.apply(lambda d: d.get("liq_stats") or d.get("liqStats") or {})
    for side in ["short", "long"]:
        for window, label in [("30s", "30s"), ("60s", "60s"), ("5m", "5m")]:
            col = f"liq_{side}_usd_{label}"
            from_snapshots = pd.to_numeric(
                liq_stats.apply(
                    lambda stats, s=side, w=window: _extract_liq_usd(stats, s, w)
                ),
                errors="coerce",
            )
            if col in result.columns:
                existing = pd.to_numeric(result[col], errors="coerce")
                result[col] = existing.combine_first(from_snapshots)
            else:
                result[col] = from_snapshots

    result["hold_seconds"] = pd.to_numeric(
        result.get("hold_seconds", details.apply(lambda d: d.get("hold_seconds"))),
        errors="coerce",
    )
    result["mae_pct"] = pd.to_numeric(
        result.get("mae_pct", details.apply(lambda d: d.get("mae_pct"))),
        errors="coerce",
    )
    result["mfe_pct"] = pd.to_numeric(
        result.get("mfe_pct", details.apply(lambda d: d.get("mfe_pct"))),
        errors="coerce",
    )
    result["outcome_time_utc"] = result.get(
        "outcome_time_utc", details.apply(lambda d: d.get("outcome_time_utc"))
    )

    # Support multiple ways to determine win
    if "TP_hit" in result.columns:
        result["win"] = pd.to_numeric(result["TP_hit"], errors="coerce").fillna(0).astype(int)
    elif "tp_hit" in result.columns:
        result["win"] = pd.to_numeric(result["tp_hit"], errors="coerce").fillna(0).astype(int)
    elif "win" in result.columns:
        result["win"] = pd.to_numeric(result["win"], errors="coerce").fillna(0).astype(int)
    elif "outcome" in result.columns:
        outcome = result["outcome"].astype(str).str.lower()
        result["win"] = (
            outcome.str.contains("tp", na=False)
            | outcome.str.contains("tp_hit", na=False)
            | (outcome == "tp")
        ).astype(int)
    else:
        result["win"] = np.nan

    # Support alternative pnl field names
    if "pnl_pct" in result.columns:
        result["pnl_pct"] = pd.to_numeric(result["pnl_pct"], errors="coerce")
    elif "pnlPct" in result.columns:
        result["pnl_pct"] = pd.to_numeric(result["pnlPct"], errors="coerce")
    elif "roi_pct" in result.columns:
        result["pnl_pct"] = pd.to_numeric(result["roi_pct"], errors="coerce")
    else:
        pnl_from_details = details.apply(
            lambda d: _coalesce(
                d.get("pnl_pct"),
                d.get("pnlPct"),
                d.get("roi_pct"),
            )
        )
        result["pnl_pct"] = pd.to_numeric(pnl_from_details, errors="coerce")

    if any(
        col in result.columns
        for col in [
            "payload_json_event",
            "payload_json",
            "payload_json_y",
            "payload_json_x",
        ]
    ):
        dprint(DEBUG_ENABLED, "[DEBUG_FEATURES] _fill_from_events obj:", _fill_from_events)
        try:
            dprint(
                DEBUG_ENABLED,
                "[DEBUG_FEATURES] _fill_from_events file:",
                inspect.getsourcefile(_fill_from_events),
            )
            dprint(
                DEBUG_ENABLED,
                "[DEBUG_FEATURES] _fill_from_events line:",
                inspect.getsourcelines(_fill_from_events)[1],
            )
        except Exception as e:
            dprint(DEBUG_ENABLED, "[DEBUG_FEATURES] inspect failed:", repr(e))
        dprint(DEBUG_ENABLED, "[DEBUG_FEATURES] about to call _fill_from_events")
        try:
            result = _fill_from_events(result)
            dprint(DEBUG_ENABLED, "[DEBUG_FEATURES] returned from _fill_from_events")
        except Exception as e:
            dprint(DEBUG_ENABLED, "[DEBUG_FEATURES] _fill_from_events EXCEPTION:", repr(e))
            dprint(DEBUG_ENABLED, traceback.format_exc())

    return result


def add_buckets(df: pd.DataFrame, strategy: str = "short_pump") -> pd.DataFrame:
    result = df.copy()

    if "context_score" in result.columns:
        context_bins = [0.0, 0.3, 0.5, 0.7, 0.85, 1.01]
        context_labels = ["<=0.3", "0.3-0.5", "0.5-0.7", "0.7-0.85", ">0.85"]
        result["context_score_bucket"] = pd.cut(
            pd.to_numeric(result["context_score"], errors="coerce"),
            bins=context_bins,
            labels=context_labels,
            include_lowest=True,
        )
    else:
        result["context_score_bucket"] = np.nan

    if "delta_ratio_30s" in result.columns:
        if strategy == "long_pullback":
            delta_bins = [-np.inf, 0.0, 0.05, 0.10, 0.20, 0.35, np.inf]
            delta_labels = [
                "<=0.00",
                "0.00-0.05",
                "0.05-0.10",
                "0.10-0.20",
                "0.20-0.35",
                ">0.35",
            ]
        else:
            delta_bins = [-np.inf, -0.40, -0.30, -0.22, -0.18, -0.12, 0]
            delta_labels = [
                "<=-0.40",
                "-0.40--0.30",
                "-0.30--0.22",
                "-0.22--0.18",
                "-0.18--0.12",
                ">-0.12",
            ]
        result["delta_ratio_30s_bucket"] = pd.cut(
            pd.to_numeric(result["delta_ratio_30s"], errors="coerce"),
            bins=delta_bins,
            labels=delta_labels,
            include_lowest=True,
        )
    else:
        result["delta_ratio_30s_bucket"] = np.nan

    if "liq_short_usd_30s" in result.columns or "liq_long_usd_30s" in result.columns:
        short_vals = pd.to_numeric(result.get("liq_short_usd_30s"), errors="coerce")
        long_vals = pd.to_numeric(result.get("liq_long_usd_30s"), errors="coerce")
        result["liq_usd_30s"] = short_vals.fillna(0) + long_vals.fillna(0)
        liq_bins = [0, 1000, 5000, 20000, 100000, np.inf]
        liq_labels = ["0-1k", "1k-5k", "5k-20k", "20k-100k", ">100k"]
        result["liq_usd_bucket"] = pd.cut(
            pd.to_numeric(result["liq_usd_30s"], errors="coerce"),
            bins=liq_bins,
            labels=liq_labels,
            include_lowest=True,
        )
    else:
        result["liq_usd_30s"] = np.nan
        result["liq_usd_bucket"] = np.nan

    # dist_to_peak_pct_bucket: 0-1, 1-2.5, 2.5-5, 5-10, 10+
    if "dist_to_peak_pct" in result.columns:
        bins_dtp = [-np.inf, 1.0, 2.5, 5.0, 10.0, np.inf]
        labels_dtp = ["0-1", "1-2.5", "2.5-5", "5-10", "10+"]
        result["dist_to_peak_pct_bucket"] = pd.cut(
            pd.to_numeric(result["dist_to_peak_pct"], errors="coerce"),
            bins=bins_dtp,
            labels=labels_dtp,
            include_lowest=True,
        )
    else:
        result["dist_to_peak_pct_bucket"] = np.nan

    # funding_rate_bucket: <=-0.002, -0.002..0, 0..0.002, >=0.002
    if "funding_rate" in result.columns:
        bins_fr = [-np.inf, -0.002, 0.0, 0.002, np.inf]
        labels_fr = ["<=-0.002", "-0.002..0", "0..0.002", ">=0.002"]
        result["funding_rate_bucket"] = pd.cut(
            pd.to_numeric(result["funding_rate"], errors="coerce"),
            bins=bins_fr,
            labels=labels_fr,
            include_lowest=True,
        )
    else:
        result["funding_rate_bucket"] = np.nan

    # oi_change_fast_bucket (in %): <=-2, -2..-1, -1..0, 0..1, >=1
    col_oi = "oi_change_fast_pct"
    if col_oi in result.columns:
        bins_oi = [-np.inf, -2.0, -1.0, 0.0, 1.0, np.inf]
        labels_oi = ["<=-2", "-2..-1", "-1..0", "0..1", ">=1"]
        result["oi_change_fast_bucket"] = pd.cut(
            pd.to_numeric(result[col_oi], errors="coerce"),
            bins=bins_oi,
            labels=labels_oi,
            include_lowest=True,
        )
    else:
        result["oi_change_fast_bucket"] = np.nan

    # cvd_1m_bucket: <=-0.30, -0.30..-0.20, -0.20..-0.10, -0.10..0, >0
    col_cvd = "cvd_delta_ratio_1m"
    if col_cvd in result.columns:
        bins_cvd = [-np.inf, -0.30, -0.20, -0.10, 0.0, np.inf]
        labels_cvd = ["<=-0.30", "-0.30..-0.20", "-0.20..-0.10", "-0.10..0", ">0"]
        result["cvd_1m_bucket"] = pd.cut(
            pd.to_numeric(result[col_cvd], errors="coerce"),
            bins=bins_cvd,
            labels=labels_cvd,
            include_lowest=True,
        )
    else:
        result["cvd_1m_bucket"] = np.nan

    return result
