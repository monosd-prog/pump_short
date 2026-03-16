from __future__ import annotations

from typing import Iterable

import pandas as pd

from analytics.utils import dprint, DEBUG_ENABLED


def _dedupe_events(events: pd.DataFrame) -> pd.DataFrame:
    if events.empty:
        return events

    # Try alternative event_id names
    event_id_col = None
    for candidate in ["event_id", "eventId", "event_uuid", "id"]:
        if candidate in events.columns:
            event_id_col = candidate
            break

    if event_id_col is None:
        return events

    time_col = None
    for candidate in ["ts_utc", "time_utc", "timestamp", "created_at"]:
        if candidate in events.columns:
            time_col = candidate
            break

    if time_col:
        times = pd.to_datetime(events[time_col], errors="coerce")
        if times.notna().any():
            sort_df = events.assign(_event_time=times)
            return (
                sort_df.sort_values([event_id_col, "_event_time"], ascending=[True, True])
                .drop_duplicates(subset=[event_id_col], keep="last")
                .drop(columns=["_event_time"])
            )

    sort_df = events.reset_index().rename(columns={"index": "_row_index"})
    return (
        sort_df.sort_values([event_id_col, "_row_index"], ascending=True)
        .drop_duplicates(subset=[event_id_col], keep="last")
        .drop(columns=["_row_index"])
    )


def join_entries_outcomes(
    entries: pd.DataFrame,
    outcomes: pd.DataFrame,
    key_candidates: Iterable[str] | None = None,
) -> pd.DataFrame:
    if entries.empty or outcomes.empty:
        return outcomes.copy()

    if key_candidates is None:
        key_candidates = [
            "entry_id",
            "trade_id",
            "order_id",
            "position_id",
            "id",
        ]

    join_key = next(
        (key for key in key_candidates if key in entries.columns and key in outcomes.columns),
        None,
    )

    if join_key is None:
        return outcomes.copy()

    return outcomes.merge(
        entries,
        on=join_key,
        how="left",
        suffixes=("", "_entry"),
    )


def join_outcomes_with_events(out_df: pd.DataFrame, ev_df: pd.DataFrame) -> pd.DataFrame:
    if out_df.empty or ev_df.empty:
        return out_df.copy()

    ev_df = _dedupe_events(ev_df)

    # Try alternative event_id names first
    event_id_out = None
    event_id_ev = None
    for candidate in ["event_id", "eventId", "event_uuid", "id"]:
        if candidate in out_df.columns and candidate in ev_df.columns:
            event_id_out = candidate
            event_id_ev = candidate
            break

    # Fallback to trade_id if event_id not available
    if event_id_out is None:
        trade_id_out = None
        trade_id_ev = None
        for candidate in ["trade_id", "tradeId", "trade_uuid", "position_id"]:
            if candidate in out_df.columns and candidate in ev_df.columns:
                trade_id_out = candidate
                trade_id_ev = candidate
                break
        
        if trade_id_out is None:
            dprint(DEBUG_ENABLED, "[DEBUG_JOIN] No event_id or trade_id found for join")
            return out_df.copy()
        
        # Join by trade_id with time-based matching for better accuracy
        # If both have timestamps, prefer events closest in time
        time_out = None
        time_ev = None
        for candidate in ["outcome_time_utc", "entry_time_utc", "ts_utc", "time_utc", "timestamp"]:
            if candidate in out_df.columns:
                time_out = candidate
                break
        for candidate in ["ts_utc", "time_utc", "timestamp", "created_at"]:
            if candidate in ev_df.columns:
                time_ev = candidate
                break
        
        if time_out and time_ev:
            # Add time columns for sorting
            out_df = out_df.copy()
            ev_df = ev_df.copy()
            out_df["_join_time"] = pd.to_datetime(out_df[time_out], errors="coerce")
            ev_df["_join_time"] = pd.to_datetime(ev_df[time_ev], errors="coerce")
            
            # Merge and then keep closest match
            joined = out_df.merge(
                ev_df,
                left_on=trade_id_out,
                right_on=trade_id_ev,
                how="left",
                suffixes=("", "_event"),
                indicator="_merge",
            )
            
            # For multiple matches, keep the one closest in time
            if (joined["_merge"] == "both").any():
                joined["_time_diff"] = (joined["_join_time"] - joined["_join_time_event"]).abs()
                joined = joined.sort_values([trade_id_out, "_time_diff"]).drop_duplicates(
                    subset=[trade_id_out], keep="first"
                )
            
            joined = joined.drop(columns=["_join_time", "_join_time_event", "_time_diff"], errors="ignore")
        else:
            # Simple merge by trade_id
            joined = out_df.merge(
                ev_df,
                left_on=trade_id_out,
                right_on=trade_id_ev,
                how="left",
                suffixes=("", "_event"),
                indicator="_merge",
            )
        
        matched = int((joined["_merge"] == "both").sum())
        dprint(
            DEBUG_ENABLED,
            f"[DEBUG_JOIN] outcomes->events (by trade_id) rows={len(joined)} matched={matched}",
        )
        payload_cols = [
            col
            for col in ["payload_json", "payload_json_event", "payload_json_x", "payload_json_y"]
            if col in joined.columns
        ]
        dprint(DEBUG_ENABLED, f"[DEBUG_JOIN] payload columns after merge: {payload_cols}")
        return joined.drop(columns=["_merge"])
    
    # Join by event_id (original logic)
    joined = out_df.merge(
        ev_df,
        left_on=event_id_out,
        right_on=event_id_ev,
        how="left",
        suffixes=("", "_event"),
        indicator="_merge",
    )

    matched = int((joined["_merge"] == "both").sum())
    dprint(
        DEBUG_ENABLED,
        f"[DEBUG_JOIN] outcomes->events (by event_id) rows={len(joined)} matched={matched}",
    )
    payload_cols = [
        col
        for col in ["payload_json", "payload_json_event", "payload_json_x", "payload_json_y"]
        if col in joined.columns
    ]
    dprint(DEBUG_ENABLED, f"[DEBUG_JOIN] payload columns after merge: {payload_cols}")
    return joined.drop(columns=["_merge"])
