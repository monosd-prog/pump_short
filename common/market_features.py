from __future__ import annotations

from typing import Any, Callable, Dict, Mapping, Optional, Tuple

import pandas as pd


def delta_ratio(trades: pd.DataFrame, since_ts: pd.Timestamp) -> float:
    if trades is None or trades.empty:
        return 0.0
    if "ts" not in trades.columns or "side" not in trades.columns or "qty" not in trades.columns:
        return 0.0
    x = trades[trades["ts"] >= since_ts]
    if x.empty:
        return 0.0
    buy = x.loc[x["side"].astype(str).str.lower() == "buy", "qty"].astype(float).sum()
    sell = x.loc[x["side"].astype(str).str.lower() == "sell", "qty"].astype(float).sum()
    total = buy + sell
    if total <= 0:
        return 0.0
    return float((buy - sell) / total)


def cvd_delta_ratio(trades: pd.DataFrame, since_ts: pd.Timestamp) -> Optional[float]:
    if trades is None or trades.empty:
        return None
    if "ts" not in trades.columns or "side" not in trades.columns or "qty" not in trades.columns:
        return None
    x = trades[trades["ts"] >= since_ts]
    if x is None or x.empty:
        return None
    buy = x.loc[x["side"].astype(str).str.lower() == "buy", "qty"].astype(float).sum()
    sell = x.loc[x["side"].astype(str).str.lower() == "sell", "qty"].astype(float).sum()
    total = buy + sell
    if total <= 0:
        return None
    return float((buy - sell) / total)


def oi_change_pct(oi_df: pd.DataFrame, lookback_minutes: int) -> Optional[float]:
    if oi_df is None or oi_df.empty:
        return None
    if "ts" not in oi_df.columns:
        return None
    col = None
    for c in ("openInterest", "open_interest", "oi"):
        if c in oi_df.columns:
            col = c
            break
    if col is None or len(oi_df) < 2:
        return None
    now_ts = oi_df["ts"].iloc[-1]
    lookback_ts = now_ts - pd.Timedelta(minutes=lookback_minutes)
    past_rows = oi_df[oi_df["ts"] <= lookback_ts]
    try:
        past = float(past_rows[col].iloc[-1]) if not past_rows.empty else float(oi_df[col].iloc[0])
        cur = float(oi_df[col].iloc[-1])
    except Exception:
        return None
    if past == 0:
        return None
    return float(((cur - past) / past) * 100.0)


def normalize_funding(payload: Optional[Mapping[str, Any]]) -> tuple[Optional[float], Optional[str], Optional[float]]:
    """
    Normalize funding payload to (funding_rate, funding_rate_ts_utc, funding_rate_abs).
    """
    if not payload:
        return None, None, None
    rate = None
    ts_utc = None
    if "fundingRate" in payload:
        try:
            rate = float(payload.get("fundingRate"))  # type: ignore[arg-type]
        except Exception:
            rate = None
    elif "funding_rate" in payload:
        try:
            rate = float(payload.get("funding_rate"))  # type: ignore[arg-type]
        except Exception:
            rate = None
    ts_val = payload.get("nextFundingTime") or payload.get("fundingRateTimestamp") or payload.get("time")
    if isinstance(ts_val, (int, float)):
        ts_f = float(ts_val)
        if ts_f > 10_000_000_000:
            ts_f = ts_f / 1000.0
        try:
            ts_utc = pd.Timestamp.fromtimestamp(ts_f, tz="UTC").isoformat()
        except Exception:
            ts_utc = None
    elif isinstance(ts_val, str):
        ts_utc = ts_val
    rate_abs = abs(rate) if rate is not None else None
    return rate, ts_utc, rate_abs


def volume_1m_features(
    candles_1m: Optional[pd.DataFrame], lookback: int = 20
) -> tuple[Optional[float], Optional[float], Optional[float]]:
    if candles_1m is None or candles_1m.empty or "volume" not in candles_1m.columns:
        return None, None, None
    try:
        vol = candles_1m["volume"].astype(float)
    except Exception:
        return None, None, None
    n = min(lookback, len(vol))
    if n < 2:
        v1 = float(vol.iloc[-1]) if len(vol) >= 1 else None
        return (v1, float(vol.mean()) if len(vol) >= 1 else None, None)
    window = vol.iloc[-n:]
    v1 = float(vol.iloc[-1])
    sma = float(window.mean())
    std = window.std(ddof=1)
    if std is None or pd.isna(std) or float(std) == 0:
        z = None
    else:
        z = float((v1 - sma) / float(std))
    return v1, sma, z


def volume_5m(candles_5m: Optional[pd.DataFrame]) -> Optional[float]:
    if candles_5m is None or candles_5m.empty or "volume" not in candles_5m.columns:
        return None
    try:
        return float(candles_5m["volume"].tail(1).sum())
    except Exception:
        return None


def orderbook_imbalance_and_spread(
    orderbook: Optional[Mapping[str, Any]], *, levels: int = 10
) -> tuple[Optional[float], Optional[float]]:
    if not orderbook:
        return None, None
    bids = orderbook.get("bids") or []
    asks = orderbook.get("asks") or []
    if not bids or not asks:
        return None, None
    bids = list(bids)[:levels]
    asks = list(asks)[:levels]
    try:
        sum_bid = sum(float(p) * float(s) for p, s in bids)
        sum_ask = sum(float(p) * float(s) for p, s in asks)
    except Exception:
        return None, None
    total = sum_bid + sum_ask
    if total <= 0:
        return None, None
    imbalance = (sum_bid - sum_ask) / total
    try:
        best_bid = float(bids[0][0])
        best_ask = float(asks[0][0])
    except Exception:
        return float(imbalance), None
    mid = (best_bid + best_ask) / 2.0 if (best_bid and best_ask) else 0.0
    spread_bps = (best_ask - best_bid) / mid * 10000.0 if mid else None
    return float(imbalance), (float(spread_bps) if spread_bps is not None else None)


def liquidation_features(
    *,
    symbol: str,
    now_ts: float,
    get_liq_stats: Callable[[str, float, int, str], Tuple[int, float]],
) -> Dict[str, Any]:
    sc30, su30 = get_liq_stats(symbol, now_ts, 30, "short")
    sc60, su60 = get_liq_stats(symbol, now_ts, 60, "short")
    lc30, lu30 = get_liq_stats(symbol, now_ts, 30, "long")
    lc60, lu60 = get_liq_stats(symbol, now_ts, 60, "long")
    return {
        "liq_short_count_30s": sc30,
        "liq_short_usd_30s": su30,
        "liq_long_count_30s": lc30,
        "liq_long_usd_30s": lu30,
        "liq_short_count_1m": sc60,
        "liq_short_usd_1m": su60,
        "liq_long_count_1m": lc60,
        "liq_long_usd_1m": lu60,
    }


def market_features_snapshot(
    *,
    trades: Optional[pd.DataFrame],
    oi: Optional[pd.DataFrame],
    candles_1m: Optional[pd.DataFrame],
    candles_5m: Optional[pd.DataFrame],
    funding_payload: Optional[Mapping[str, Any]],
    now_ts_utc: Optional[pd.Timestamp] = None,
) -> Dict[str, Any]:
    """
    Pure feature computation from already-fetched market data.
    No network calls inside.
    """
    now_ts_utc = now_ts_utc or pd.Timestamp.now(tz="UTC")
    trades_df = trades if trades is not None else pd.DataFrame()

    since_30s = now_ts_utc - pd.Timedelta(seconds=30)
    since_1m = now_ts_utc - pd.Timedelta(seconds=60)

    dr30 = delta_ratio(trades_df, since_30s)
    dr1m = delta_ratio(trades_df, since_1m)
    cvd30 = cvd_delta_ratio(trades_df, since_30s)
    cvd1m = cvd_delta_ratio(trades_df, since_1m)

    oi_fast = oi_change_pct(oi, lookback_minutes=3) if oi is not None else None
    oi_1m = oi_change_pct(oi, lookback_minutes=1) if oi is not None else None
    oi_5m = oi_change_pct(oi, lookback_minutes=5) if oi is not None else None

    fr, _ts, fr_abs = normalize_funding(funding_payload)

    v1, vsma, vz = volume_1m_features(candles_1m, lookback=20)
    v5 = volume_5m(candles_5m)

    return {
        "delta_ratio_30s": dr30,
        "delta_ratio_1m": dr1m,
        "cvd_delta_ratio_30s": cvd30,
        "cvd_delta_ratio_1m": cvd1m,
        "oi_change_fast_pct": oi_fast,
        "oi_change_1m_pct": oi_1m,
        "oi_change_5m_pct": oi_5m,
        "funding_rate": fr,
        "funding_rate_abs": fr_abs,
        "volume_1m": v1,
        "volume_5m": v5,
        "volume_sma_20": vsma,
        "volume_zscore_20": vz,
    }

