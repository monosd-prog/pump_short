# telegram.py
from __future__ import annotations

import os
import requests

from short_pump.logging_utils import get_logger, log_exception

logger = get_logger(__name__)

TG_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TG_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
TG_SEND_OUTCOME = os.getenv("TG_SEND_OUTCOME", "0") == "1"
TG_ARMED_ENABLE = os.getenv("TG_ARMED_ENABLE", "0").strip().lower() in ("1", "true", "yes")
TG_ENTRY_STAGE = (os.getenv("TG_ENTRY_STAGE", "4") or "4").strip()
try:
    TG_DIST_TO_PEAK_MIN = float((os.getenv("TG_DIST_TO_PEAK_MIN", "3.5") or "3.5").replace(",", "."))
except (TypeError, ValueError):
    TG_DIST_TO_PEAK_MIN = 3.5
try:
    _d = os.getenv("TG_ENTRY_DIST_MIN")
    TG_ENTRY_DIST_MIN = float(_d.replace(",", ".")) if _d and _d.strip() else TG_DIST_TO_PEAK_MIN
except (TypeError, ValueError):
    TG_ENTRY_DIST_MIN = TG_DIST_TO_PEAK_MIN

FAST0_TG_OUTCOME_ENABLE = os.getenv("FAST0_TG_OUTCOME_ENABLE", "0").strip().lower() in ("1", "true", "yes", "y", "on")
try:
    _fd = os.getenv("FAST0_TG_OUTCOME_MIN_DIST", "0.0")
    FAST0_TG_OUTCOME_MIN_DIST = float(_fd.replace(",", ".")) if _fd and str(_fd).strip() else 0.0
except (TypeError, ValueError):
    FAST0_TG_OUTCOME_MIN_DIST = 0.0


def is_tradeable_short_pump(stage: int, dist_to_peak_pct: float | None = None) -> bool:
    """True if entry is tradeable: stage==4 and dist_to_peak_pct >= TG_ENTRY_DIST_MIN. Gates TG, autotrading, outcome."""
    if stage != 4:
        return False
    if dist_to_peak_pct is None:
        return False
    try:
        return float(dist_to_peak_pct) >= TG_ENTRY_DIST_MIN
    except (TypeError, ValueError):
        return False


def is_fast0_tg_entry_allowed(payload: dict) -> bool:
    """True if FAST0 TG ENTRY_OK/OUTCOME may be sent: liq_long_usd_30s > 0. Gates TG for short_pump_fast0."""
    try:
        liq = payload.get("liq_long_usd_30s")
        v = float(liq) if liq is not None else 0.0
        return v > 0
    except (TypeError, ValueError):
        return False


def tg_entry_filter(stage, dist_to_peak_pct) -> bool:
    """Return True if ENTRY_OK should send Telegram (stage and dist satisfy thresholds)."""
    try:
        stage_i = int(stage)
    except (TypeError, ValueError):
        return False
    try:
        dist = float(dist_to_peak_pct)
    except (TypeError, ValueError):
        return False
    stage_req = int(TG_ENTRY_STAGE) if TG_ENTRY_STAGE else 4
    if stage_i != stage_req:
        return False
    if not (dist >= TG_DIST_TO_PEAK_MIN):
        return False
    return True


def send_telegram(
    text: str,
    *,
    strategy: str,
    side: str,
    mode: str,
    event_id: str,
    context_score: float | None,
    entry_ok: bool,
    skip_reasons: str | None = None,
    formatted: bool = False,
) -> None:
    if not TG_BOT_TOKEN or not TG_CHAT_ID:
        return
    try:
        side_up = (side or "").strip().upper()
        mode_up = (mode or "").strip().upper()
        strategy_name = (strategy or "").strip()
        emoji = "🟥" if side_up == "SHORT" else "🟩"
        score_val = float(context_score) if context_score is not None else 0.0
        eid = (event_id or "")[:8] or "--------"

        if formatted:
            lines = [text] if text else []
        else:
            header = f"{emoji} {side_up} | {strategy_name} | {mode_up} | score={score_val:.2f} | eid={eid}"
            lines = [header]
            if entry_ok is False:
                lines.append("ENTRY_OK ❌")
                if skip_reasons:
                    lines.append(f"skip: {skip_reasons}")
            if text:
                lines.append(text)
        lines.append(f"#{strategy_name} #{mode_up}")
        final_text = "\n".join(lines)

        url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage"
        payload = {"chat_id": TG_CHAT_ID, "text": final_text, "disable_web_page_preview": True}
        r = requests.post(url, json=payload, timeout=15)
        r.raise_for_status()
    except Exception as e:
        log_exception(logger, "Telegram send failed", step="TELEGRAM_SEND", extra={"text_preview": text[:100] if text else None})
        raise