from __future__ import annotations

import json
import time
from typing import Any, Dict, Optional

import pandas as pd

from common.bybit_api import get_klines_1m, get_klines_5m, get_open_interest, get_recent_trades
from common.logging_utils import get_logger
from common.runtime import run_id as gen_run_id
from long_pullback.config import Config
from long_pullback.context5m import LongPullbackState, update_context_5m
from long_pullback.entry import decide_entry_long
from long_pullback.telegram import send_telegram
from short_pump.io_csv import append_csv

logger = get_logger(__name__, strategy="long_pullback")


def run_watch_for_symbol(
    symbol: str,
    run_id: Optional[str] = None,
    mode: str = "live",
    cfg: Optional[Config] = None,
) -> Dict[str, Any]:
    cfg = cfg or Config()
    symbol = str(symbol).strip().upper()
    run_id = run_id or gen_run_id()
    logger = get_logger(__name__, strategy="long_pullback", symbol=symbol)

    log_5m = f"{cfg.logs_root}/{run_id}_{symbol}_5m.csv"
    log_1m = f"{cfg.logs_root}/{run_id}_{symbol}_1m.csv"

    logger.info("LONG_WATCH_START | symbol_repr=%r | symbol=%s", symbol, symbol)

    while True:
        try:
            st = LongPullbackState()
            entry_ok = False
            entry_payload: Dict[str, Any] = {}

            start_ts = pd.Timestamp.now(tz="UTC")
            end_ts = start_ts + pd.Timedelta(minutes=90)

            while pd.Timestamp.now(tz="UTC") < end_ts:
                candles_5m = get_klines_5m("linear", symbol, limit=250)
                logger.info(
                    "LONG_TICK | symbol=%s | step=FETCH_5M | candles_5m_rows=%s",
                    symbol,
                    candles_5m.shape[0] if candles_5m is not None else 0,
                )
                st, ctx_parts = update_context_5m(cfg, st, candles_5m)
                context_score = sum(ctx_parts.values())

                # log 5m
                try:
                    append_csv(
                        log_5m,
                        {
                            "run_id": run_id,
                            "symbol": symbol,
                            "time_utc": str(candles_5m.iloc[-1]["ts"]) if candles_5m is not None and not candles_5m.empty else "",
                            "stage": st.stage,
                            "context_score": context_score,
                            "context_parts": json.dumps(ctx_parts, ensure_ascii=False),
                        },
                    )
                except Exception:
                    pass

                if st.stage >= 2 and not entry_ok:
                    candles_1m = get_klines_1m("linear", symbol, limit=250)
                    trades = get_recent_trades("linear", symbol, limit=1000)
                    oi = get_open_interest("linear", symbol, limit=80)

                    entry_ok, entry_payload = decide_entry_long(
                        cfg,
                        run_id=run_id,
                        symbol=symbol,
                        candles_1m=candles_1m,
                        trades=trades,
                        oi_1m=oi,
                        ctx_parts=ctx_parts,
                        mode=mode,
                    )

                    try:
                        append_csv(
                            log_1m,
                            {
                                "run_id": run_id,
                                "symbol": symbol,
                                "time_utc": str(candles_1m.iloc[-1]["ts"]) if candles_1m is not None and not candles_1m.empty else "",
                                "entry_ok": bool(entry_ok),
                                "entry_payload": json.dumps(entry_payload, ensure_ascii=False),
                            },
                        )
                    except Exception:
                        pass

                    if entry_ok:
                        try:
                            send_telegram(
                                f"ENTRY OK: {symbol}\nrun_id={run_id}\n{json.dumps(entry_payload, ensure_ascii=False)}",
                                strategy="long_pullback",
                                side="LONG",
                                mode="ARMED",
                                event_id=str(entry_payload.get("event_id", run_id)),
                                context_score=float(entry_payload.get("context_score", 0.0)),
                                entry_ok=True,
                                skip_reasons=None,
                            )
                        except Exception:
                            pass
                        entry_ok = False
                        entry_payload = {}

                time.sleep(cfg.poll_seconds_5m)
        except Exception:
            logger.exception("LONG_LOOP_ERROR | symbol=%s", symbol)
        time.sleep(cfg.poll_seconds_5m)
