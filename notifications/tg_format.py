from __future__ import annotations

import json
import os
from typing import Any, Dict, Iterable


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
    debug_payload: Any = None,
) -> str:
    extra = f"type={entry_type} | " if entry_type else ""
    header = f"{_emoji(side)} {side.upper()} | {strategy} | ENTRY_OK | {extra}sym={symbol}"
    lines = [
        header,
        f"run_id={run_id} eid={_short_eid(event_id)}",
        f"time={time_utc} price={_fmt_num(price)}",
        f"entry={_fmt_num(entry_price)} tp={_fmt_num(tp_price)} ({_fmt_pct(tp_pct)}) sl={_fmt_num(sl_price)} ({_fmt_pct(sl_pct)})",
    ]
    ctx_line = _compact_ctx(ctx_parts)
    if ctx_line:
        lines.append(f"ctx={ctx_line}")
    metrics = []
    if liq_short_usd_30s is not None or liq_long_usd_30s is not None:
        metrics.append(
            f"liq_s_30s={_fmt_num(liq_short_usd_30s)} liq_l_30s={_fmt_num(liq_long_usd_30s)}"
        )
    if oi_change_fast_pct is not None:
        metrics.append(f"oi_fast={_fmt_pct(oi_change_fast_pct)}")
    if cvd_delta_ratio_30s is not None:
        metrics.append(f"cvd_30s={_fmt_num(cvd_delta_ratio_30s, 3)}")
    if metrics:
        lines.append(" | ".join(metrics))
    if context_score is not None:
        lines.append(f"context_score={_fmt_num(context_score)}")
    dbg = _maybe_debug_json("entry", debug_payload)
    if dbg:
        lines.append(dbg)
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
