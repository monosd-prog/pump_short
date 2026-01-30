# short_pump/bybit_api.py
from __future__ import annotations

from typing import Dict, Any
import requests
import pandas as pd
import numpy as np

from short_pump.logging_utils import get_logger, log_exception

logger = get_logger(__name__)

BYBIT_REST = "https://api.bybit.com"


_ALLOWED_CATEGORIES = {"linear", "inverse", "spot", "option"}


def _norm_category(category: str | None) -> str:
    c = (category or "").strip().lower()
    if c not in _ALLOWED_CATEGORIES:
        return "linear"
    return c


def _norm_symbol(symbol: str) -> str:
    return (symbol or "").strip().upper()


def _get_json(path: str, params: Dict[str, Any]) -> Dict[str, Any]:
    try:
        r = requests.get(BYBIT_REST + path, params=params, timeout=15)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        log_exception(logger, f"Bybit API request failed", step="BYBIT_API", extra={"path": path, "params": params})
        raise


def _klines(category: str, symbol: str, interval: str, limit: int) -> pd.DataFrame:
    category = _norm_category(category)
    symbol = _norm_symbol(symbol)

    j = _get_json(
        "/v5/market/kline",
        {
            "category": category,
            "symbol": symbol,
            "interval": str(interval),
            "limit": str(limit),
        },
    )
    if j.get("retCode") != 0:
        error_msg = f"Bybit kline({interval}) error: {j}"
        log_exception(logger, error_msg, step="BYBIT_API", extra={"category": category, "symbol": symbol, "interval": interval, "response": j})
        raise RuntimeError(error_msg)

    lst = j["result"]["list"]
    df = pd.DataFrame(
        lst, columns=["ts", "open", "high", "low", "close", "volume", "turnover"]
    )
    df["ts"] = pd.to_datetime(df["ts"].astype(np.int64), unit="ms", utc=True)
    for c in ["open", "high", "low", "close", "volume", "turnover"]:
        df[c] = df[c].astype(float)
    return df.sort_values("ts").reset_index(drop=True)


def get_klines_5m(category: str, symbol: str, limit: int = 250) -> pd.DataFrame:
    return _klines(category, symbol, "5", limit)


def get_klines_1m(category: str, symbol: str, limit: int = 300) -> pd.DataFrame:
    return _klines(category, symbol, "1", limit)


def get_open_interest(category: str, symbol: str, limit: int = 80) -> pd.DataFrame:
    category = _norm_category(category)
    symbol = _norm_symbol(symbol)

    j = _get_json(
        "/v5/market/open-interest",
        {
            "category": category,
            "symbol": symbol,
            "intervalTime": "5min",
            "limit": str(limit),
        },
    )
    if j.get("retCode") != 0:
        error_msg = f"Bybit OI error: {j}"
        log_exception(logger, error_msg, step="BYBIT_API", extra={"category": category, "symbol": symbol, "response": j})
        raise RuntimeError(error_msg)

    df = pd.DataFrame(j["result"]["list"])
    if df.empty:
        return df

    if "timestamp" in df.columns:
        df["ts"] = pd.to_datetime(df["timestamp"].astype(np.int64), unit="ms", utc=True)
    elif "time" in df.columns:
        df["ts"] = pd.to_datetime(df["time"].astype(np.int64), unit="ms", utc=True)
    else:
        df["ts"] = pd.NaT

    if "openInterest" in df.columns:
        df["openInterest"] = df["openInterest"].astype(float)

    return df.sort_values("ts").reset_index(drop=True)


def get_recent_trades(category: str, symbol: str, limit: int = 1000) -> pd.DataFrame:
    category = _norm_category(category)
    symbol = _norm_symbol(symbol)

    j = _get_json(
        "/v5/market/recent-trade",
        {
            "category": category,
            "symbol": symbol,
            "limit": str(limit),
        },
    )
    if j.get("retCode") != 0:
        error_msg = f"Bybit recent-trade error: {j}"
        log_exception(logger, error_msg, step="BYBIT_API", extra={"category": category, "symbol": symbol, "response": j})
        raise RuntimeError(error_msg)

    df = pd.DataFrame(j["result"]["list"])
    if df.empty:
        return df

    tcol = (
        "time"
        if "time" in df.columns
        else ("timestamp" if "timestamp" in df.columns else None)
    )
    if not tcol:
        error_msg = f"Unexpected trades schema: {df.columns.tolist()}"
        log_exception(logger, error_msg, step="BYBIT_API", extra={"category": category, "symbol": symbol, "columns": df.columns.tolist()})
        raise RuntimeError(error_msg)

    df["ts"] = pd.to_datetime(df[tcol].astype(np.int64), unit="ms", utc=True)

    qcol = "size" if "size" in df.columns else ("qty" if "qty" in df.columns else None)
    if not qcol:
        error_msg = f"Unexpected trades schema (no size/qty): {df.columns.tolist()}"
        log_exception(logger, error_msg, step="BYBIT_API", extra={"category": category, "symbol": symbol, "columns": df.columns.tolist()})
        raise RuntimeError(error_msg)

    df["qty"] = df[qcol].astype(float)

    # side: "Buy"/"Sell"
    if "side" in df.columns:
        df["side"] = df["side"].astype(str)

    # price
    if "price" in df.columns:
        df["price"] = df["price"].astype(float)

    return df.sort_values("ts").reset_index(drop=True)