from __future__ import annotations

from typing import Any, Callable, Dict, Mapping, Optional, Tuple

import pandas as pd


def _safe_float(val: Any) -> Optional[float]:
    try:
        if val is None:
            return None
        return float(val)
    except (TypeError, ValueError):
        return None


def _pct_change(cur: Optional[float], past: Optional[float]) -> Optional[float]:
    if cur is None or past is None:
        return None
    if past == 0:
        return None
    return float((cur - past) / past * 100.0)


def _close_at_or_before(candles: Optional[pd.DataFrame], ts: pd.Timestamp) -> Optional[float]:
    if candles is None or candles.empty:
        return None
    if "ts" not in candles.columns or "close" not in candles.columns:
        return None
    try:
        x = candles[candles["ts"] <= ts]
        if x is None or x.empty:
            return None
        return _safe_float(x.iloc[-1]["close"])
    except Exception:
        return None


def _peak_ts_utc_from_5m(candles_5m: Optional[pd.DataFrame], lookback: int = 20) -> Optional[pd.Timestamp]:
    """
    Best-effort: infer peak timestamp as the ts of max(high) in last `lookback` 5m candles.
    Uses already fetched 5m klines; no network calls.
    """
    if candles_5m is None or candles_5m.empty:
        return None
    if "ts" not in candles_5m.columns or "high" not in candles_5m.columns:
        return None
    try:
        tail = candles_5m.tail(max(1, int(lookback)))
        if tail.empty:
            return None
        idx = tail["high"].astype(float).idxmax()
        ts_val = candles_5m.loc[idx, "ts"]
        return pd.Timestamp(ts_val) if ts_val is not None else None
    except Exception:
        return None


def price_velocity_features(
    candles_1m: Optional[pd.DataFrame],
    *,
    now_ts_utc: pd.Timestamp,
) -> Dict[str, Optional[float]]:
    """
    Candle-only velocity/acceleration features from 1m klines.
    Returns percent changes and a stable acceleration ratio.
    """
    if candles_1m is None or candles_1m.empty:
        return {
            "price_change_30s_pct": None,
            "price_change_1m_pct": None,
            "price_change_3m_pct": None,
            "accel_30s_vs_3m": None,
        }
    cur = _safe_float(candles_1m.iloc[-1].get("close")) if "close" in candles_1m.columns else None
    past_30s = _close_at_or_before(candles_1m, now_ts_utc - pd.Timedelta(seconds=30))
    past_1m = _close_at_or_before(candles_1m, now_ts_utc - pd.Timedelta(seconds=60))
    past_3m = _close_at_or_before(candles_1m, now_ts_utc - pd.Timedelta(seconds=180))

    ch30 = _pct_change(cur, past_30s)
    ch1m = _pct_change(cur, past_1m)
    ch3m = _pct_change(cur, past_3m)

    accel = None
    if ch30 is not None and ch3m is not None:
        eps = 0.2  # pct points, stabilizes denominator for near-zero 3m moves
        denom = max(abs(float(ch3m)), eps)
        accel = float(ch30) / denom

    return {
        "price_change_30s_pct": ch30,
        "price_change_1m_pct": ch1m,
        "price_change_3m_pct": ch3m,
        "accel_30s_vs_3m": accel,
    }


def pump_shape_features_5m(candles_5m: Optional[pd.DataFrame], *, lookback: int = 5) -> Dict[str, Optional[float]]:
    """
    Candle-only shape/structure features from 5m klines.
    """
    out: Dict[str, Optional[float]] = {
        "green_candles_5": None,
        "max_candle_body_pct_5": None,
        "avg_candle_body_pct_5": None,
        "upper_wick_ratio_last": None,
        "lower_wick_ratio_last": None,
        "wick_body_ratio_last": None,
    }
    if candles_5m is None or candles_5m.empty:
        return out
    required = {"open", "high", "low", "close"}
    if not required.issubset(set(candles_5m.columns)):
        return out
    try:
        tail = candles_5m.tail(max(1, int(lookback))).copy()
        if tail.empty:
            return out
        o = tail["open"].astype(float)
        c = tail["close"].astype(float)
        h = tail["high"].astype(float)
        l = tail["low"].astype(float)

        greens = int((c > o).sum())
        body = (c - o).abs()
        body_pct = (body / o.replace(0, pd.NA)) * 100.0
        try:
            max_body = float(body_pct.max(skipna=True))
            avg_body = float(body_pct.mean(skipna=True))
        except Exception:
            max_body, avg_body = None, None

        # last candle wick ratios
        o_last = float(o.iloc[-1])
        c_last = float(c.iloc[-1])
        h_last = float(h.iloc[-1])
        l_last = float(l.iloc[-1])
        body_last = abs(c_last - o_last)
        range_last = max(h_last - l_last, 0.0)
        upper_wick = max(0.0, h_last - max(o_last, c_last))
        lower_wick = max(0.0, min(o_last, c_last) - l_last)

        eps_body = 1e-9
        wick_body_ratio = float((upper_wick + lower_wick) / max(body_last, eps_body))
        upper_ratio = float(upper_wick / range_last) if range_last > 0 else None
        lower_ratio = float(lower_wick / range_last) if range_last > 0 else None

        out.update(
            {
                "green_candles_5": float(greens),
                "max_candle_body_pct_5": max_body,
                "avg_candle_body_pct_5": avg_body,
                "upper_wick_ratio_last": upper_ratio,
                "lower_wick_ratio_last": lower_ratio,
                "wick_body_ratio_last": wick_body_ratio,
            }
        )
    except Exception:
        return out
    return out


def relative_volume_features(
    *,
    volume_1m: Optional[float],
    volume_5m: Optional[float],
    volume_sma_20_1m: Optional[float],
    candles_5m: Optional[pd.DataFrame],
    lookback_5m: int = 20,
) -> Dict[str, Optional[float]]:
    """
    Relative volume anomalies. Uses existing 1m SMA and computes 5m SMA from 5m candles.
    """
    out: Dict[str, Optional[float]] = {
        "volume_ratio_1m_20": None,
        "volume_ratio_5m_20": None,
    }
    eps = 1e-9
    if volume_1m is not None and volume_sma_20_1m is not None and float(volume_sma_20_1m) > 0:
        out["volume_ratio_1m_20"] = float(volume_1m) / max(float(volume_sma_20_1m), eps)

    if candles_5m is not None and not candles_5m.empty and "volume" in candles_5m.columns:
        try:
            vol5_series = candles_5m["volume"].astype(float)
            n = min(int(lookback_5m), len(vol5_series))
            if n >= 2:
                sma5 = float(vol5_series.tail(n).mean())
            elif n == 1:
                sma5 = float(vol5_series.iloc[-1])
            else:
                sma5 = None
            if volume_5m is not None and sma5 is not None and sma5 > 0:
                out["volume_ratio_5m_20"] = float(volume_5m) / max(float(sma5), eps)
        except Exception:
            pass
    return out


def time_life_features(
    *,
    now_ts_utc: pd.Timestamp,
    candles_5m: Optional[pd.DataFrame],
    signal_ts_utc: Optional[pd.Timestamp] = None,
    pump_ts_utc: Optional[pd.Timestamp] = None,
) -> Dict[str, Optional[float]]:
    """
    Time-life features. Works with what we can infer from already fetched candles.
    """
    peak_ts = _peak_ts_utc_from_5m(candles_5m, lookback=20)
    out: Dict[str, Optional[float]] = {
        "time_since_peak_sec": None,
        "time_since_signal_sec": None,
        "pump_age_sec": None,
    }
    try:
        if peak_ts is not None:
            out["time_since_peak_sec"] = float((now_ts_utc - peak_ts).total_seconds())
    except Exception:
        pass
    try:
        if signal_ts_utc is not None:
            out["time_since_signal_sec"] = float((now_ts_utc - signal_ts_utc).total_seconds())
    except Exception:
        pass
    try:
        if pump_ts_utc is not None:
            out["pump_age_sec"] = float((now_ts_utc - pump_ts_utc).total_seconds())
    except Exception:
        pass
    return out


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


def normalize_ls_ratio(payload: Optional[Mapping[str, Any]]) -> tuple[Optional[float], Optional[float]]:
    """
    Normalize Bybit account-ratio payload to (ls_ratio_buy, ls_ratio_sell).
    Returns (None, None) on missing or invalid input.
    """
    if not payload:
        return None, None
    try:
        buy = float(payload["buyRatio"]) if payload.get("buyRatio") is not None else None
    except (TypeError, ValueError):
        buy = None
    try:
        sell = float(payload["sellRatio"]) if payload.get("sellRatio") is not None else None
    except (TypeError, ValueError):
        sell = None
    return buy, sell


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


def price_structure_features_1m(
    candles_1m: Optional[pd.DataFrame],
    rsi_period: int = 14,
    ma_period: int = 20,
) -> Dict[str, Optional[float]]:
    """
    RSI(14), SMA(20), EMA(20) and distance-to-MA features from 1m candles.
    Pure pandas — no external TA library.  All outputs are None-safe.
    """
    out: Dict[str, Optional[float]] = {
        "rsi_14_1m": None,
        "ma_20_1m": None,
        "ema_20_1m": None,
        "dist_to_ma20_pct": None,
        "dist_to_ema20_pct": None,
    }
    if candles_1m is None or candles_1m.empty or "close" not in candles_1m.columns:
        return out
    try:
        close = candles_1m["close"].astype(float).reset_index(drop=True)
        n = len(close)

        # RSI-14: Wilder smoothing via EWM (alpha = 1/period)
        if n >= rsi_period + 1:
            try:
                delta = close.diff()
                gain = delta.clip(lower=0.0).ewm(alpha=1.0 / rsi_period, adjust=False).mean()
                loss = (-delta.clip(upper=0.0)).ewm(alpha=1.0 / rsi_period, adjust=False).mean()
                last_loss = float(loss.iloc[-1])
                last_gain = float(gain.iloc[-1])
                if last_loss == 0.0:
                    out["rsi_14_1m"] = 100.0 if last_gain > 0 else 50.0
                else:
                    rs = last_gain / last_loss
                    out["rsi_14_1m"] = float(100.0 - 100.0 / (1.0 + rs))
            except Exception:
                pass

        last_close = float(close.iloc[-1])

        # SMA-20
        if n >= ma_period:
            try:
                ma = float(close.tail(ma_period).mean())
                out["ma_20_1m"] = ma
                if ma > 0:
                    out["dist_to_ma20_pct"] = (last_close - ma) / ma * 100.0
            except Exception:
                pass

        # EMA-20 (pandas ewm span=period)
        if n >= ma_period:
            try:
                ema = float(close.ewm(span=ma_period, adjust=False).mean().iloc[-1])
                out["ema_20_1m"] = ema
                if ema > 0:
                    out["dist_to_ema20_pct"] = (last_close - ema) / ema * 100.0
            except Exception:
                pass

    except Exception:
        pass
    return out


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
    pump_ts_utc: Optional[pd.Timestamp] = None,
    ls_ratio_payload: Optional[Mapping[str, Any]] = None,
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

    oi_abs: Optional[float] = None
    if oi is not None and not oi.empty and "openInterest" in oi.columns:
        try:
            oi_abs = float(oi.iloc[-1]["openInterest"])
        except (TypeError, ValueError):
            pass

    oi_abs_usd: Optional[float] = None
    if funding_payload:
        try:
            raw = funding_payload.get("openInterestValue")
            if raw is not None:
                oi_abs_usd = float(raw)
        except (TypeError, ValueError):
            pass

    fr, _ts, fr_abs = normalize_funding(funding_payload)
    ls_buy, ls_sell = normalize_ls_ratio(ls_ratio_payload)

    v1, vsma, vz = volume_1m_features(candles_1m, lookback=20)
    v5 = volume_5m(candles_5m)

    vel = price_velocity_features(candles_1m, now_ts_utc=now_ts_utc)
    shape = pump_shape_features_5m(candles_5m, lookback=5)
    rel_vol = relative_volume_features(
        volume_1m=v1,
        volume_5m=v5,
        volume_sma_20_1m=vsma,
        candles_5m=candles_5m,
        lookback_5m=20,
    )
    tlife = time_life_features(now_ts_utc=now_ts_utc, candles_5m=candles_5m, pump_ts_utc=pump_ts_utc)
    price_struct = price_structure_features_1m(candles_1m)

    return {
        "delta_ratio_30s": dr30,
        "delta_ratio_1m": dr1m,
        "cvd_delta_ratio_30s": cvd30,
        "cvd_delta_ratio_1m": cvd1m,
        "oi_change_fast_pct": oi_fast,
        "oi_change_1m_pct": oi_1m,
        "oi_change_5m_pct": oi_5m,
        "oi_abs": oi_abs,
        "oi_abs_usd": oi_abs_usd,
        "funding_rate": fr,
        "funding_rate_abs": fr_abs,
        "ls_ratio_buy": ls_buy,
        "ls_ratio_sell": ls_sell,
        "volume_1m": v1,
        "volume_5m": v5,
        "volume_sma_20": vsma,
        "volume_zscore_20": vz,
        **vel,
        **shape,
        **rel_vol,
        **tlife,
        **price_struct,
    }

