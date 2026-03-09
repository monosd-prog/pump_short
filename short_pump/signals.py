from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Optional


@dataclass
class Signal:
    strategy: str
    symbol: str
    side: str
    ts_utc: str
    run_id: str
    event_id: Optional[str]
    entry_price: Optional[float]
    tp_price: Optional[float]
    sl_price: Optional[float]
    tp_pct: Optional[float]
    sl_pct: Optional[float]
    stage: Optional[int]
    dist_to_peak_pct: Optional[float]
    context_score: Optional[float]
    cvd_30s: Optional[float]
    cvd_1m: Optional[float]
    liq_long_usd_30s: Optional[float]
    liq_short_usd_30s: Optional[float]
    volume_1m: Optional[float] = None
    volume_sma_20: Optional[float] = None
    volume_zscore_20: Optional[float] = None
    extras: Dict[str, Any] = field(default_factory=dict)


def _fmt_num(val: Any, digits: int = 2, empty: str = "n/a") -> str:
    if val is None or val == "":
        return empty
    try:
        return f"{float(val):.{digits}f}"
    except (TypeError, ValueError):
        return empty


def _fmt_pct(val: Any, digits: int = 2) -> str:
    return f"{_fmt_num(val, digits)}%"


def _short_eid(eid: Optional[str]) -> str:
    return (eid or "")[:8] or "--------"


def _get_risk_profile_line(signal: Signal) -> str | None:
    """Compute risk_profile and exec params for TG. Returns line or None."""
    try:
        from trading.risk_profile import get_risk_profile, get_notional_and_leverage
        profile, risk_mult, _ = get_risk_profile(
            (signal.strategy or "").strip(),
            stage=getattr(signal, "stage", None),
            dist_to_peak_pct=getattr(signal, "dist_to_peak_pct", None),
            liq_long_usd_30s=getattr(signal, "liq_long_usd_30s", None),
            event_id=str(getattr(signal, "event_id", "") or ""),
            trade_id="",
            symbol=signal.symbol or "",
        )
        if not profile:
            return None
        notional, leverage, margin_mode = get_notional_and_leverage(risk_mult)
        return f"risk_profile={profile} | liqL30s={_fmt_num(signal.liq_long_usd_30s, 0)} dist={_fmt_pct(signal.dist_to_peak_pct)} cs={_fmt_num(signal.context_score)} | notional={notional:.0f} USD lev=x{leverage} margin={margin_mode}"
    except Exception:
        return None


def format_tg(signal: Signal) -> str:
    side_up = (signal.side or "").upper()
    emoji = "🟥" if side_up == "SHORT" else "🟩"
    lines: list[str] = []

    if signal.strategy == "short_pump_fast0":
        header = (
            f"⚡ FAST0 ENTRY_OK | {signal.symbol} | "
            f"dist={_fmt_pct(signal.dist_to_peak_pct)} | "
            f"cs={_fmt_num(signal.context_score)} | "
            f"liqL30s={_fmt_num(signal.liq_long_usd_30s, 0)} "
            f"liqS30s={_fmt_num(signal.liq_short_usd_30s, 0)} | "
            f"cvd30s={_fmt_num(signal.cvd_30s, 3)} | "
            f"cvd1m={_fmt_num(signal.cvd_1m, 3)} | "
            f"run_id={signal.run_id}"
        )
        lines.append(header)
        rp_line = _get_risk_profile_line(signal)
        if rp_line:
            lines.append(rp_line)

        if signal.entry_price is not None:
            lines.append(
                f"entry={_fmt_num(signal.entry_price)} "
                f"tp={_fmt_num(signal.tp_price)} ({_fmt_pct(signal.tp_pct)}) "
                f"sl={_fmt_num(signal.sl_price)} ({_fmt_pct(signal.sl_pct)})"
            )
    else:
        stage_str = str(signal.stage) if signal.stage is not None else "n/a"
        header = (
            f"{emoji} {side_up} | {signal.strategy} | ENTRY_OK | "
            f"stage={stage_str} | dist={_fmt_pct(signal.dist_to_peak_pct)} | "
            f"sym={signal.symbol}"
        )
        lines.append(header)
        rp_line = _get_risk_profile_line(signal)
        if rp_line:
            lines.append(rp_line)
        lines.append(
            f"run_id={signal.run_id} eid={_short_eid(signal.event_id)} ts={signal.ts_utc}"
        )
        if signal.entry_price is not None:
            lines.append(
                f"entry={_fmt_num(signal.entry_price)} "
                f"tp={_fmt_num(signal.tp_price)} ({_fmt_pct(signal.tp_pct)}) "
                f"sl={_fmt_num(signal.sl_price)} ({_fmt_pct(signal.sl_pct)})"
            )
        metrics: list[str] = []
        if signal.liq_short_usd_30s is not None or signal.liq_long_usd_30s is not None:
            metrics.append(
                f"liqS30s={_fmt_num(signal.liq_short_usd_30s, 0)} "
                f"liqL30s={_fmt_num(signal.liq_long_usd_30s, 0)}"
            )
        if signal.cvd_30s is not None:
            metrics.append(f"cvd30s={_fmt_num(signal.cvd_30s, 3)}")
        if signal.cvd_1m is not None:
            metrics.append(f"cvd1m={_fmt_num(signal.cvd_1m, 3)}")
        if signal.context_score is not None:
            metrics.append(f"cs={_fmt_num(signal.context_score)}")
        if metrics:
            lines.append(" | ".join(metrics))

    ctx_line = signal.extras.get("ctx_line")
    if ctx_line:
        lines.append(f"ctx={ctx_line}")

    return "\n".join(lines)

