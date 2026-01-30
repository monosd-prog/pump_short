# bybit_api.py
from __future__ import annotations

from typing import Dict, Any
import requests
import pandas as pd
import numpy as np

BYBIT_REST = "https://api.bybit.com"


def _get_json(path: str, params: Dict[str, Any]) -> Dict[str, Any]:
    r = requests.get(BYBIT_REST + path, params=params, timeout=15)
    r.raise_for_status()
    return r.json()


def _klines(category: str, symbol: str, interval: str, limit: int) -> pd.DataFrame:
    j = _get_json("/v5/market/kline", {
        "category": category,
        "symbol": symbol,
        "interval": interval,
        "limit": str(limit)
    })
    if j.get("retCode") != 0:
        raise RuntimeError(f"Bybit kline({interval}) error: {j}")
    lst = j["result"]["list"]
    df = pd.DataFrame(lst, columns=["ts", "open", "high", "low", "close", "volume", "turnover"])
    df["ts"] = pd.to_datetime(df["ts"].astype(np.int64), unit="ms", utc=True)
    for c in ["open", "high", "low", "close", "volume", "turnover"]:
        df[c] = df[c].astype(float)
    return df.sort_values("ts").reset_index(drop=True)


def get_klines_5m(category: str, symbol: str, limit: int = 250) -> pd.DataFrame:
    return _klines(category, symbol, "5", limit)


def get_klines_1m(category: str, symbol: str, limit: int = 300) -> pd.DataFrame:
    return _klines(category, symbol, "1", limit)


def get_open_interest(category: str, symbol: str, limit: int = 80) -> pd.DataFrame:
    j = _get_json("/v5/market/open-interest", {
        "category": category,
        "symbol": symbol,
        "intervalTime": "5min",
        "limit": str(limit)
    })
    if j.get("retCode") != 0:
        raise RuntimeError(f"Bybit OI error: {j}")
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
    j = _get_json("/v5/market/recent-trade", {
        "category": category,
        "symbol": symbol,
        "limit": str(limit)
    })
    if j.get("retCode") != 0:
        raise RuntimeError(f"Bybit recent-trade error: {j}")
    df = pd.DataFrame(j["result"]["list"])
    if df.empty:
        return df

    tcol = "time" if "time" in df.columns else ("timestamp" if "timestamp" in df.columns else None)
    if not tcol:
        raise RuntimeError(f"Unexpected trades schema: {df.columns.tolist()}")
    df["ts"] = pd.to_datetime(df[tcol].astype(np.int64), unit="ms", utc=True)

    qcol = "size" if "size" in df.columns else ("qty" if "qty" in df.columns else None)
    if not qcol:
        raise RuntimeError(f"Unexpected trades schema (no size/qty): {df.columns.tolist()}")
    df["qty"] = df[qcol].astype(float)

    pcol = "price" if "price" in df.columns else ("p" if "p" in df.columns else None)
    if pcol:
        df["price"] = df[pcol].astype(float)

    df["side"] = df["side"].astype(str)
    return df.sort_values("ts").reset_index(drop=True)