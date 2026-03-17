from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, Optional

import pandas as pd

from common.feature_contract import normalize_event_feature_row
from common.io_dataset import ensure_dataset_files, write_event_row, write_outcome_row, write_trade_row
from common.market_features import market_features_snapshot
from common.outcome_tracker import build_outcome_row
from replay.input_loader import ReplaySignal
from replay.outcome import resolve_outcome_from_candles


LAB_MODE_DEFAULT = "lab"

# ---------------------------------------------------------------------------
# LAB MODE V1 CAPABILITIES (candle-only)
# ---------------------------------------------------------------------------
# Supported in lab v1 (honest reconstruction):
# - dist_to_peak_pct (from klines)
# - volume_* (from klines)
# - candle-based outcomes (TP/SL/TIMEOUT) given configured TP/SL rules
#
# Not supported honestly without additional historical sources:
# - delta_*, cvd_* (need historical trades tape)
# - liq_* (need historical liquidation tape)
# - oi_* (need historical OI series)
# - funding_* (need historical funding series)
# - spread_bps, orderbook_imbalance_10 (need historical orderbook snapshots)
# - true watcher stage/context_score (need full state-machine replay + additional inputs)

SUPPORTED_IN_LAB_V1: tuple[str, ...] = (
    "dist_to_peak_pct",
    "volume_1m",
    "volume_5m",
    "volume_sma_20",
    "volume_zscore_20",
    "candle_based_outcomes",
)

UNAVAILABLE_IN_LAB_V1: tuple[str, ...] = (
    "delta_ratio_30s",
    "delta_ratio_1m",
    "cvd_delta_ratio_30s",
    "cvd_delta_ratio_1m",
    "liq_*",
    "oi_change_fast_pct",
    "oi_change_1m_pct",
    "oi_change_5m_pct",
    "funding_rate",
    "funding_rate_abs",
    "spread_bps",
    "orderbook_imbalance_10",
    "context_score (true live)",
    "stage (true watcher)",
)


@dataclass(frozen=True)
class ReplayConfig:
    mode: str = LAB_MODE_DEFAULT
    schema_version: int = 3
    # outcome window in minutes (default fallback if strategy-specific not provided)
    outcome_watch_minutes: int = 30


def _parse_float(x: Any, default: float) -> float:
    try:
        return float(x)
    except Exception:
        return default


def _short_pump_tp_sl(entry_price: float) -> tuple[float, float]:
    # Default consistent with watcher fallback: CONFIRM tp/sl from env via Config, but here we avoid importing Config-heavy logic.
    # Use env override if present, else conservative defaults.
    tp_pct = _parse_float(os.getenv("REPLAY_SHORT_PUMP_TP_PCT", "0.012"), 0.012)
    sl_pct = _parse_float(os.getenv("REPLAY_SHORT_PUMP_SL_PCT", "0.010"), 0.010)
    tp = entry_price * (1.0 - tp_pct)
    sl = entry_price * (1.0 + sl_pct)
    return tp, sl


def _fast0_tp_sl(entry_price: float) -> tuple[float, float]:
    tp_pct = _parse_float(os.getenv("FAST0_TP_PCT", "0.012"), 0.012)
    sl_pct = _parse_float(os.getenv("FAST0_SL_PCT", "0.010"), 0.010)
    tp = entry_price * (1.0 - tp_pct)
    sl = entry_price * (1.0 + sl_pct)
    return tp, sl


def run_replay_for_signals(
    *,
    signals: Iterable[ReplaySignal],
    data_dir: str | Path,
    mode: str = LAB_MODE_DEFAULT,
    dry_run: bool = False,
    category: str = "linear",
    candle_provider_1m: Optional[Any] = None,
    candle_provider_5m: Optional[Any] = None,
) -> dict:
    """
    LAB MODE V1 (candle-only replay):
    - builds canonical event rows at signal_time_utc using historical klines (OHLCV)
    - computes candle-derived features only (dist_to_peak_pct, volume_*)
    - resolves candle-based outcomes (TP/SL/TIMEOUT) without touching live/paper runtime state

    NOTE:
    Delta/CVD/Liq/OI/Funding/Microstructure are unavailable unless historical sources are added.
    stage/context_score are NOT reconstructed from watcher state-machine in v1.
    """
    from short_pump.bybit_api import get_klines_1m_range, get_klines_5m_range

    base_dir = str(data_dir)
    out = {"events": 0, "trades": 0, "outcomes": 0, "errors": 0}

    mode_norm = (mode or LAB_MODE_DEFAULT).strip().lower()
    if mode_norm != "lab":
        # MVP standard: lock to 'lab' to keep separation consistent.
        mode_norm = "lab"

    for sig in signals:
        try:
            ts = sig.signal_time_utc
            # Fetch candles around the signal. For MVP, we only use candles for features + outcome.
            # 5m lookback: 250*5m ~= 20h; 1m lookback: 300m ~= 5h.
            end_ms = int(ts.timestamp() * 1000)
            start_5m_ms = int((ts - pd.Timedelta(hours=24)).timestamp() * 1000)
            start_1m_ms = int((ts - pd.Timedelta(hours=6)).timestamp() * 1000)

            kl5 = (
                candle_provider_5m(category, sig.symbol, start_ms=start_5m_ms, end_ms=end_ms, limit=1000)
                if candle_provider_5m
                else get_klines_5m_range(category, sig.symbol, start_ms=start_5m_ms, end_ms=end_ms, limit=1000)
            )
            kl1 = (
                candle_provider_1m(category, sig.symbol, start_ms=start_1m_ms, end_ms=end_ms, limit=1000)
                if candle_provider_1m
                else get_klines_1m_range(category, sig.symbol, start_ms=start_1m_ms, end_ms=end_ms, limit=1000)
            )

            if kl5 is None or kl5.empty:
                raise RuntimeError("no_5m_klines")
            last_price = float(kl5["close"].iloc[-1])
            peak_price = float(kl5["high"].tail(20).max())
            dist_to_peak = (peak_price - last_price) / peak_price * 100.0 if peak_price > 0 else 0.0

            snap = market_features_snapshot(
                trades=None,
                oi=None,
                candles_1m=kl1,
                candles_5m=kl5,
                funding_payload=None,
                now_ts_utc=ts,
            )

            payload: Dict[str, Any] = {
                "time_utc": ts.isoformat(),
                "price": last_price,
                "dist_to_peak_pct": dist_to_peak,
                # context_score is NOT reconstructed in lab v1
                "context_score": "",
                **{k: v for k, v in snap.items() if v is not None},
                "signal_type": sig.signal_type,
                "signal_source": sig.source,
                "lab_mode_version": "v1_candle_only",
                "lab_supported": list(SUPPORTED_IN_LAB_V1),
                "lab_unavailable": list(UNAVAILABLE_IN_LAB_V1),
                **(sig.meta or {}),
            }

            # stage is NOT reconstructed in lab v1; keep empty to avoid confusion.
            stage_tag = "fast0_default" if sig.strategy == "short_pump_fast0" else "short_pump_default"
            base = {
                "run_id": sig.run_id,
                "event_id": sig.event_id,
                "symbol": sig.symbol,
                "strategy": sig.strategy,
                "mode": mode_norm,
                "source_mode": mode_norm,
                "side": "SHORT",
                "wall_time_utc": ts.isoformat(),
                "time_utc": ts.isoformat(),
                "stage": "",
                "entry_ok": 1,
                "skip_reasons": f"replay_{sig.signal_type}",
                "price": last_price,
                "dist_to_peak_pct": dist_to_peak,
                "outcome_label": "",
            }
            row = normalize_event_feature_row(base=base, payload=payload)
            # Preserve explicit stage tagging in payload_json (not as a misleading stage int column)
            row["payload_json"] = row.get("payload_json") or ""
            row["skip_reasons"] = f"{row.get('skip_reasons')};stage_tag={stage_tag}"

            if not dry_run:
                ensure_dataset_files(sig.strategy, mode_norm, ts.isoformat(), schema_version=3, base_dir=base_dir)
                write_event_row(
                    row,
                    strategy=sig.strategy,
                    mode=mode_norm,
                    wall_time_utc=ts.isoformat(),
                    schema_version=3,
                    base_dir=base_dir,
                    path_mode=mode_norm,
                )
            out["events"] += 1

            # Trade + outcome (MVP treats every signal as entry)
            if sig.strategy == "short_pump_fast0":
                tp_price, sl_price = _fast0_tp_sl(last_price)
                watch_min = int(_parse_float(os.getenv("FAST0_OUTCOME_WATCH_SEC", "1800"), 1800.0) / 60.0)
                entry_source = "fast0"
                entry_type = "FAST0"
                trade_type = "FAST0_LAB"
            else:
                tp_price, sl_price = _short_pump_tp_sl(last_price)
                watch_min = int(_parse_float(os.getenv("REPLAY_SHORT_PUMP_OUTCOME_WATCH_MIN", "30"), 30.0))
                entry_source = "replay"
                entry_type = "REPLAY"
                trade_type = "SHORT_PUMP_LAB"

            trade_id = f"{sig.strategy}:{sig.run_id}:{sig.event_id}:{sig.symbol}"
            trade_row = {
                "trade_id": trade_id,
                "event_id": sig.event_id,
                "run_id": sig.run_id,
                "symbol": sig.symbol,
                "strategy": sig.strategy,
                "mode": mode_norm,
                "source_mode": mode_norm,
                "side": "SHORT",
                "entry_time_utc": ts.isoformat(),
                "entry_price": last_price,
                "tp_price": tp_price,
                "sl_price": sl_price,
                "trade_type": trade_type,
            }
            if not dry_run:
                write_trade_row(
                    trade_row,
                    strategy=sig.strategy,
                    mode=mode_norm,
                    wall_time_utc=ts.isoformat(),
                    schema_version=3,
                    base_dir=base_dir,
                    path_mode=mode_norm,
                )
            out["trades"] += 1

            # Outcome candles window: from entry to end.
            end_outcome_ms = int((ts + pd.Timedelta(minutes=watch_min)).timestamp() * 1000)
            candles_outcome = (
                candle_provider_1m(category, sig.symbol, start_ms=end_ms, end_ms=end_outcome_ms, limit=1000)
                if candle_provider_1m
                else get_klines_1m_range(category, sig.symbol, start_ms=end_ms, end_ms=end_outcome_ms, limit=1000)
            )

            resolved = resolve_outcome_from_candles(
                candles_1m=candles_outcome,
                side="short",
                entry_ts_utc=ts,
                entry_price=last_price,
                tp_price=tp_price,
                sl_price=sl_price,
                watch_minutes=watch_min,
            )

            summary = {
                "run_id": sig.run_id,
                "symbol": sig.symbol,
                "entry_time_utc": ts.isoformat(),
                "entry_price": last_price,
                "entry_source": entry_source,
                "entry_type": entry_type,
                "tp_price": tp_price,
                "sl_price": sl_price,
                "end_reason": resolved.end_reason,
                "outcome": resolved.end_reason,
                "hit_time_utc": resolved.hit_time_utc,
                "exit_time_utc": resolved.exit_time_utc,
                "exit_price": resolved.exit_price,
                "pnl_pct": resolved.pnl_pct,
                "hold_seconds": resolved.hold_seconds,
                "mae_pct": resolved.mae_pct,
                "mfe_pct": resolved.mfe_pct,
                "details_payload": resolved.details_payload_json,
                "trade_type": trade_type,
            }
            orow = build_outcome_row(
                summary,
                trade_id=trade_id,
                event_id=sig.event_id,
                run_id=sig.run_id,
                symbol=sig.symbol,
                strategy=sig.strategy,
                mode=mode_norm,
                side="SHORT",
                outcome_time_utc=resolved.exit_time_utc,
            )
            if orow and not dry_run:
                orow["outcome_source"] = "replay"
                write_outcome_row(
                    orow,
                    strategy=sig.strategy,
                    mode=mode_norm,
                    wall_time_utc=resolved.exit_time_utc,
                    schema_version=3,
                    base_dir=base_dir,
                    path_mode=mode_norm,
                )
            out["outcomes"] += 1
        except Exception:
            out["errors"] += 1

    return out

