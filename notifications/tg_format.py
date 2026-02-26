from __future__ import annotations

import json
import os
from typing import Any, Dict, Iterable

from short_pump.signals import Signal, format_tg


def _emoji(side: str) -> str:
    return "🟥" if (side or "").strip().upper() == "SHORT" else "🟩"


def _short_eid(event_id: str) -> str:
    return (event_id or "")[:8] or "--------"


def _fmt_num(val: Any, digits: int = 2, empty: str = "n/a") -> str:
    if val is None or val == "":
        return empty
    try:
        return f"{float(val):.{digits}f}"
    except (TypeError, ValueError):
        return empty


def _fmt_pct(val: Any, digits: int = 2) -> str:
    return f"{_fmt_num(val, digits)}%"


def _compact_ctx(parts: Any) -> str:
    if not isinstance(parts, dict):
        return ""
    items: Iterable[tuple[str, Any]] = parts.items()
    return ",".join(f"{k}={_fmt_num(v, 2)}" for k, v in items)


def _maybe_debug_json(label: str, payload: Any) -> str:
    if os.getenv("TG_DEBUG_JSON") != "1":
        return ""
    try:
        return f"{label}={json.dumps(payload, ensure_ascii=False)}"
    except Exception:
        return f"{label}={payload}"


def format_armed_short(*, symbol: str, run_id: str, event_id: str, time_utc: str, price: Any, dist_to_peak_pct: Any, context_score: Any) -> str:
    header = f"{_emoji('SHORT')} SHORT | short_pump | ARMED | sym={symbol}"
    lines = [
        header,
        f"run_id={run_id} eid={_short_eid(event_id)}",
        f"time={time_utc} price={_fmt_num(price)} dist_to_peak={_fmt_pct(dist_to_peak_pct)}",
        f"context_score={_fmt_num(context_score)}",
    ]
    dbg = _maybe_debug_json("armed", {"time_utc": time_utc, "price": price, "dist_to_peak_pct": dist_to_peak_pct})
    if dbg:
        lines.append(dbg)
    return "\n".join(lines)


def build_short_pump_signal(
    *,
    strategy: str,
    side: str,
    symbol: str,
    run_id: str,
    event_id: str,
    time_utc: str,
    price: Any,
    entry_price: Any,
    tp_price: Any,
    sl_price: Any,
    tp_pct: Any,
    sl_pct: Any,
    entry_type: str | None = None,
    context_score: Any = None,
    ctx_parts: Any = None,
    liq_short_usd_30s: Any = None,
    liq_long_usd_30s: Any = None,
    oi_change_fast_pct: Any = None,
    cvd_delta_ratio_30s: Any = None,
    cvd_delta_ratio_1m: Any = None,
    dist_to_peak_pct: Any = None,
    stage: int | None = None,
    debug_payload: Any = None,
) -> Signal:
    """Build Signal for short_pump ENTRY_OK. Same kwargs as format_entry_ok. Used for TG and enqueue."""
    ctx_line = _compact_ctx(ctx_parts)
    extras: Dict[str, Any] = {}
    if ctx_line:
        extras["ctx_line"] = ctx_line
    if os.getenv("TG_DEBUG_JSON") == "1":
        extras["debug"] = debug_payload
    return Signal(
        strategy=strategy,
        symbol=symbol,
        side=side,
        ts_utc=time_utc,
        run_id=run_id,
        event_id=event_id,
        entry_price=float(entry_price) if entry_price is not None else None,
        tp_price=float(tp_price) if tp_price is not None else None,
        sl_price=float(sl_price) if sl_price is not None else None,
        tp_pct=float(tp_pct) if tp_pct is not None else None,
        sl_pct=float(sl_pct) if sl_pct is not None else None,
        stage=stage,
        dist_to_peak_pct=float(dist_to_peak_pct) if dist_to_peak_pct is not None else None,
        context_score=float(context_score) if context_score is not None else None,
        cvd_30s=float(cvd_delta_ratio_30s) if cvd_delta_ratio_30s is not None else None,
        cvd_1m=float(cvd_delta_ratio_1m) if cvd_delta_ratio_1m is not None else None,
        liq_long_usd_30s=float(liq_long_usd_30s) if liq_long_usd_30s is not None else None,
        liq_short_usd_30s=float(liq_short_usd_30s) if liq_short_usd_30s is not None else None,
        extras=extras,
    )


def build_fast0_signal(
    *,
    symbol: str,
    run_id: str,
    dist_to_peak_pct: Any,
    context_score: Any,
    cvd_30s: Any,
    cvd_1m: Any,
    liq_short_usd_30s: Any = None,
    liq_long_usd_30s: Any = None,
    ts_utc: str | None = None,
    event_id: str | None = None,
    entry_price: Any = None,
    tp_price: Any = None,
    sl_price: Any = None,
    volume_1m: Any = None,
    volume_sma_20: Any = None,
    volume_zscore_20: Any = None,
) -> Signal:
    """Build Signal for fast0 ENTRY_OK. entry/tp/sl optional (runner will skip if missing); still enqueue."""
    return Signal(
        strategy="short_pump_fast0",
        symbol=symbol,
        side="SHORT",
        ts_utc=ts_utc or "",
        run_id=run_id,
        event_id=event_id,
        entry_price=float(entry_price) if entry_price is not None else None,
        tp_price=float(tp_price) if tp_price is not None else None,
        sl_price=float(sl_price) if sl_price is not None else None,
        tp_pct=None,
        sl_pct=None,
        stage=None,
        dist_to_peak_pct=float(dist_to_peak_pct) if dist_to_peak_pct is not None else None,
        context_score=float(context_score) if context_score is not None else None,
        cvd_30s=float(cvd_30s) if cvd_30s is not None else None,
        cvd_1m=float(cvd_1m) if cvd_1m is not None else None,
        liq_long_usd_30s=float(liq_long_usd_30s) if liq_long_usd_30s is not None else None,
        liq_short_usd_30s=float(liq_short_usd_30s) if liq_short_usd_30s is not None else None,
        volume_1m=float(volume_1m) if volume_1m is not None else None,
        volume_sma_20=float(volume_sma_20) if volume_sma_20 is not None else None,
        volume_zscore_20=float(volume_zscore_20) if volume_zscore_20 is not None else None,
        extras={},
    )


def format_entry_ok(
    *,
    strategy: str,
    side: str,
    symbol: str,
    run_id: str,
    event_id: str,
    time_utc: str,
    price: Any,
    entry_price: Any,
    tp_price: Any,
    sl_price: Any,
    tp_pct: Any,
    sl_pct: Any,
    entry_type: str | None = None,
    context_score: Any = None,
    ctx_parts: Any = None,
    liq_short_usd_30s: Any = None,
    liq_long_usd_30s: Any = None,
    oi_change_fast_pct: Any = None,
    cvd_delta_ratio_30s: Any = None,
    cvd_delta_ratio_1m: Any = None,
    dist_to_peak_pct: Any = None,
    stage: int | None = None,
    debug_payload: Any = None,
) -> str:
    sig = build_short_pump_signal(
        strategy=strategy,
        side=side,
        symbol=symbol,
        run_id=run_id,
        event_id=event_id,
        time_utc=time_utc,
        price=price,
        entry_price=entry_price,
        tp_price=tp_price,
        sl_price=sl_price,
        tp_pct=tp_pct,
        sl_pct=sl_pct,
        entry_type=entry_type,
        context_score=context_score,
        ctx_parts=ctx_parts,
        liq_short_usd_30s=liq_short_usd_30s,
        liq_long_usd_30s=liq_long_usd_30s,
        oi_change_fast_pct=oi_change_fast_pct,
        cvd_delta_ratio_30s=cvd_delta_ratio_30s,
        cvd_delta_ratio_1m=cvd_delta_ratio_1m,
        dist_to_peak_pct=dist_to_peak_pct,
        stage=stage,
        debug_payload=debug_payload,
    )
    return format_tg(sig)


def format_fast0_entry_ok(
    *,
    symbol: str,
    run_id: str,
    dist_to_peak_pct: Any,
    context_score: Any,
    cvd_30s: Any,
    cvd_1m: Any,
    liq_short_usd_30s: Any = None,
    liq_long_usd_30s: Any = None,
    ts_utc: str | None = None,
    event_id: str | None = None,
) -> str:
    """Format only; no enqueue. Build signal via build_fast0_signal (no entry/tp/sl here)."""
    sig = build_fast0_signal(
        symbol=symbol,
        run_id=run_id,
        dist_to_peak_pct=dist_to_peak_pct,
        context_score=context_score,
        cvd_30s=cvd_30s,
        cvd_1m=cvd_1m,
        liq_short_usd_30s=liq_short_usd_30s,
        liq_long_usd_30s=liq_long_usd_30s,
        ts_utc=ts_utc,
        event_id=event_id,
    )
    return format_tg(sig)


def format_fast0_outcome_message(
    *,
    symbol: str,
    run_id: str,
    event_id: str,
    res: str,
    entry_price: float,
    tp_price: float,
    sl_price: float,
    exit_price: float,
    pnl_pct: float,
    hold_seconds: float,
    dist_to_peak_pct: float | None = None,
    context_score: float | None = None,
) -> str:
    """Format FAST0 outcome for Telegram. Used when FAST0_TG_OUTCOME_ENABLE=1."""
    header = f"{_emoji('SHORT')} SHORT | short_pump_fast0 | OUTCOME | res={res} | sym={symbol}"
    lines = [
        header,
        f"run_id={run_id} eid={_short_eid(event_id)}",
        f"entry={_fmt_num(entry_price)} tp={_fmt_num(tp_price)} sl={_fmt_num(sl_price)}",
        f"pnl={_fmt_pct(pnl_pct)} | hold={_fmt_num(hold_seconds)}s",
    ]
    return "\n".join(lines)


def format_outcome(
    *,
    strategy: str,
    side: str,
    symbol: str,
    run_id: str,
    event_id: str,
    outcome: str,
    entry_price: Any,
    tp_price: Any,
    sl_price: Any,
    tp_pct: Any,
    sl_pct: Any,
    pnl_pct: Any = None,
    hold_seconds: Any = None,
    mae_pct: Any = None,
    mfe_pct: Any = None,
    debug_payload: Any = None,
) -> str:
    header = f"{_emoji(side)} {side.upper()} | {strategy} | OUTCOME | res={outcome} | sym={symbol}"
    lines = [
        header,
        f"run_id={run_id} eid={_short_eid(event_id)}",
        f"entry={_fmt_num(entry_price)} tp={_fmt_num(tp_price)} ({_fmt_pct(tp_pct)}) sl={_fmt_num(sl_price)} ({_fmt_pct(sl_pct)})",
    ]
    metrics = []
    if pnl_pct is not None:
        metrics.append(f"pnl={_fmt_pct(pnl_pct)}")
    if hold_seconds is not None:
        metrics.append(f"hold={_fmt_num(hold_seconds)}s")
    if mae_pct is not None:
        metrics.append(f"mae={_fmt_pct(mae_pct)}")
    if mfe_pct is not None:
        metrics.append(f"mfe={_fmt_pct(mfe_pct)}")
    if metrics:
        lines.append(" | ".join(metrics))
    dbg = _maybe_debug_json("outcome", debug_payload)
    if dbg:
        lines.append(dbg)
    return "\n".join(lines)
