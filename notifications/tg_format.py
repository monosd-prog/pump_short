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


def _fmt_price(val: Any, empty: str = "n/a") -> str:
    """Smart price formatting: uses enough decimal places to show 4 significant digits."""
    if val is None or val == "":
        return empty
    try:
        f = float(val)
        if f == 0:
            return "0"
        if f >= 100:
            decimals = 2
        elif f >= 10:
            decimals = 3
        elif f >= 1:
            decimals = 4
        elif f >= 0.1:
            decimals = 5
        elif f >= 0.01:
            decimals = 6
        else:
            decimals = 8
        return f"{f:.{decimals}f}"
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
    funding_rate_abs: Any = None,
    oi_change_fast_pct: Any = None,
    cvd_delta_ratio_30s: Any = None,
    cvd_delta_ratio_1m: Any = None,
    cvd_ratio_5m: Any = None,
    cvd_momentum: Any = None,
    vp_poc_dist_pct: Any = None,
    fp_imbalance_at_entry: Any = None,
    liq_short_usd_30s_real: Any = None,
    liq_long_usd_30s_real: Any = None,
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
        funding_rate_abs=float(funding_rate_abs) if funding_rate_abs is not None else None,
        oi_change_fast_pct=float(oi_change_fast_pct) if oi_change_fast_pct is not None else None,
        cvd_ratio_5m=float(cvd_ratio_5m) if cvd_ratio_5m is not None else None,
        cvd_momentum=float(cvd_momentum) if cvd_momentum is not None else None,
        vp_poc_dist_pct=float(vp_poc_dist_pct) if vp_poc_dist_pct is not None else None,
        fp_imbalance_at_entry=float(fp_imbalance_at_entry) if fp_imbalance_at_entry is not None else None,
        liq_short_usd_30s_real=float(liq_short_usd_30s_real) if liq_short_usd_30s_real is not None else None,
        liq_long_usd_30s_real=float(liq_long_usd_30s_real) if liq_long_usd_30s_real is not None else None,
        extras=extras,
    )


def build_fast0_signal(
    *,
    symbol: str,
    run_id: str,
    strategy: str = "short_pump_fast0",
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
        strategy=strategy,
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
    funding_rate_abs: Any = None,
    oi_change_fast_pct: Any = None,
    cvd_delta_ratio_30s: Any = None,
    cvd_delta_ratio_1m: Any = None,
    cvd_ratio_5m: Any = None,
    cvd_momentum: Any = None,
    vp_poc_dist_pct: Any = None,
    fp_imbalance_at_entry: Any = None,
    liq_short_usd_30s_real: Any = None,
    liq_long_usd_30s_real: Any = None,
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
        funding_rate_abs=funding_rate_abs,
        oi_change_fast_pct=oi_change_fast_pct,
        cvd_delta_ratio_30s=cvd_delta_ratio_30s,
        cvd_delta_ratio_1m=cvd_delta_ratio_1m,
        cvd_ratio_5m=cvd_ratio_5m,
        cvd_momentum=cvd_momentum,
        vp_poc_dist_pct=vp_poc_dist_pct,
        fp_imbalance_at_entry=fp_imbalance_at_entry,
        liq_short_usd_30s_real=liq_short_usd_30s_real,
        liq_long_usd_30s_real=liq_long_usd_30s_real,
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
    strategy: str = "short_pump_fast0",
    res: str,
    entry_price: float,
    tp_price: float,
    sl_price: float,
    exit_price: float,
    pnl_pct: float,
    hold_seconds: float,
    dist_to_peak_pct: float | None = None,
    context_score: float | None = None,
    risk_profile: str | None = None,
    notional_usd: float | None = None,
    leverage: int | None = None,
    margin_mode: str | None = None,
    live_confirmed: bool = False,
) -> str:
    """Format FAST0 outcome for Telegram. Used when FAST0_TG_OUTCOME_ENABLE=1 or live outcome."""
    strategy_name = (strategy or "short_pump_fast0").strip() or "short_pump_fast0"
    header = f"{_emoji('SHORT')} SHORT | {strategy_name} | OUTCOME | res={res} | sym={symbol}"
    lines = [header]
    if live_confirmed:
        lines.append("🟢 LIVE CONFIRMED | source=bybit")
    pnl_line = (
        f"exit={_fmt_num(exit_price)} | pnl={_fmt_pct(pnl_pct)} | hold={_fmt_num(hold_seconds)}s"
        if live_confirmed
        else f"pnl={_fmt_pct(pnl_pct)} | hold={_fmt_num(hold_seconds)}s"
    )
    lines += [
        f"run_id={run_id} eid={_short_eid(event_id)}",
        f"entry={_fmt_num(entry_price)} tp={_fmt_num(tp_price)} sl={_fmt_num(sl_price)}",
        pnl_line,
    ]
    if risk_profile:
        extra = [f"risk_profile={risk_profile}"]
        if notional_usd is not None and notional_usd > 0:
            extra.append(f"notional={notional_usd:.0f} USD")
        if leverage is not None:
            extra.append(f"lev=x{leverage}")
        if margin_mode:
            extra.append(f"margin={margin_mode}")
        if len(extra) > 1:
            lines.append(" | ".join(extra))
        elif extra:
            lines.append(extra[0])
    return "\n".join(lines)


def format_tpsl_failed_closed_message(
    position: Dict[str, Any],
    *,
    pnl_pct: float | None = None,
    exit_price: float | None = None,
) -> str:
    """
    Format TG alert for: entry filled, TP/SL setup failed, position force-closed.
    """
    strategy = (position.get("strategy") or "").strip()
    symbol = (position.get("symbol") or "").strip()
    side = (position.get("side") or "SHORT").strip().upper()
    side_emoji = _emoji(side)
    run_id = (position.get("run_id") or "").strip()
    event_id = (position.get("event_id") or "").strip()
    entry = position.get("entry")
    tp = position.get("tp")
    sl = position.get("sl")
    risk_profile = (position.get("risk_profile") or "").strip()
    notional = position.get("notional_usd")
    leverage = position.get("leverage")
    exit_px = exit_price if exit_price is not None else position.get("exit_price")
    pnl = pnl_pct if pnl_pct is not None else position.get("pnl_pct")

    header = f"{side_emoji} {side} | {strategy} | TPSL_SETUP_FAILED_CLOSED | sym={symbol}"
    lines = [header]
    lines.append("⚠️ Entry filled, TP/SL setup failed → position force-closed")
    if run_id or event_id:
        lines.append(f"run_id={run_id} eid={_short_eid(event_id)}")
    lines.append(f"entry={_fmt_num(entry)} tp={_fmt_num(tp)} sl={_fmt_num(sl)}")
    if exit_px is not None:
        lines.append(f"exit≈{_fmt_num(exit_px)}")
    if pnl is not None:
        lines.append(f"pnl≈{_fmt_pct(pnl)}")
    if risk_profile or notional or leverage:
        rp = risk_profile or "n/a"
        lines.append(f"risk_profile={rp} | notional={_fmt_num(notional, 0)} USD | lev=x{_fmt_num(leverage, 0)}")
    return "\n".join(lines)


def format_live_open_message(position: Dict[str, Any]) -> str:
    """
    Format LIVE_OPEN notification for a successfully opened live position.
    Includes strategy, symbol, run_id, event_id, risk_profile, notional, leverage, margin, and entry/tp/sl.
    """
    strategy = (position.get("strategy") or "").strip()
    symbol = (position.get("symbol") or "").strip()
    side = (position.get("side") or "SHORT").strip().upper()
    side_emoji = _emoji(side)
    run_id = (position.get("run_id") or "").strip()
    event_id = (position.get("event_id") or "").strip()
    risk_profile = (position.get("risk_profile") or "").strip()
    notional = position.get("notional_usd")
    leverage = position.get("leverage")
    margin_mode = (position.get("margin_mode") or "").strip()
    entry = position.get("entry")
    tp = position.get("tp")
    sl = position.get("sl")

    header = f"{side_emoji} {side} | {strategy} | LIVE_OPEN | sym={symbol}"
    lines = [header]
    if run_id or event_id:
        lines.append(f"run_id={run_id} eid={_short_eid(event_id)}")
    if entry is not None or tp is not None or sl is not None:
        lines.append(
            f"entry={_fmt_num(entry)} tp={_fmt_num(tp)} sl={_fmt_num(sl)}"
        )
    if risk_profile or notional or leverage or margin_mode:
        rp = risk_profile or "n/a"
        lines.append(
            f"risk_profile={rp} | notional={_fmt_num(notional, 0)} USD | lev=x{_fmt_num(leverage, 0)} | margin={margin_mode or 'n/a'}"
        )
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
    cvd_ratio_5m: Any = None,
    vp_poc_dist_pct: Any = None,
    fp_imbalance_at_entry: Any = None,
    funding_rate_abs: Any = None,
    liq_short_usd_30s_real: Any = None,
    liq_long_usd_30s_real: Any = None,
    debug_payload: Any = None,
    risk_profile: str | None = None,
    notional_usd: float | None = None,
    leverage: int | None = None,
    margin_mode: str | None = None,
) -> str:
    # Result header with dedicated outcome emoji.
    outcome_emoji = "✅" if outcome == "TP_hit" else "❌" if outcome == "SL_hit" else "⏱"
    header = f"{outcome_emoji} {side.upper()} | {strategy} | OUTCOME | res={outcome} | sym={symbol}"
    lines = [
        header,
        f"🆔 run_id={run_id} eid={_short_eid(event_id)}",
        f"💰 entry={_fmt_price(entry_price)} tp={_fmt_price(tp_price)} ({_fmt_pct(tp_pct)}) sl={_fmt_price(sl_price)} ({_fmt_pct(sl_pct)})",
    ]

    # PnL + hold + MAE/MFE
    metrics = []
    if pnl_pct is not None:
        pnl_emoji = "📈" if float(pnl_pct) > 0 else "📉"
        metrics.append(f"{pnl_emoji} pnl={_fmt_pct(pnl_pct)}")
    if hold_seconds is not None:
        metrics.append(f"⏱ hold={_fmt_num(hold_seconds)}s")
    if mae_pct is not None:
        metrics.append(f"↘ mae={_fmt_pct(mae_pct)}")
    if mfe_pct is not None:
        metrics.append(f"↗ mfe={_fmt_pct(mfe_pct)}")
    if metrics:
        lines.append(" | ".join(metrics))

    # Risk profile
    if risk_profile:
        extra = [f"🎯 {risk_profile}"]
        if notional_usd is not None and notional_usd > 0:
            extra.append(f"{notional_usd:.0f} USD")
        if leverage is not None:
            extra.append(f"x{leverage}")
        if margin_mode:
            extra.append(margin_mode)
        lines.append(" | ".join(extra))

    # CVD + VP/FP + funding (when available)
    market_parts = []
    if cvd_ratio_5m is not None:
        market_parts.append(f"cvd5m={_fmt_num(cvd_ratio_5m, 3)}")
    if vp_poc_dist_pct is not None:
        market_parts.append(f"poc_dist={_fmt_pct(vp_poc_dist_pct)}")
    if fp_imbalance_at_entry is not None:
        market_parts.append(f"fp_imb={_fmt_num(fp_imbalance_at_entry, 3)}")
    if funding_rate_abs is not None:
        market_parts.append(f"fund={_fmt_num(funding_rate_abs, 4)}")
    if market_parts:
        lines.append("📊 " + " | ".join(market_parts))

    # Liquidations real USD
    liq_long = liq_long_usd_30s_real
    liq_short = liq_short_usd_30s_real
    if liq_long is not None or liq_short is not None:
        lines.append(f"💧 liqL={_fmt_num(liq_long, 0)}$ | liqS={_fmt_num(liq_short, 0)}$")

    dbg = _maybe_debug_json("outcome", debug_payload)
    if dbg:
        lines.append(dbg)
    return "\n".join(lines)
