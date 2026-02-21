# telegram.py
import os
import requests

from short_pump.logging_utils import get_logger, log_exception

logger = get_logger(__name__)

TG_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TG_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
TG_SEND_OUTCOME = os.getenv("TG_SEND_OUTCOME", "0") == "1"
TG_ENTRY_STAGE = (os.getenv("TG_ENTRY_STAGE", "4") or "4").strip()
try:
    TG_DIST_TO_PEAK_MIN = float((os.getenv("TG_DIST_TO_PEAK_MIN", "3.5") or "3.5").replace(",", "."))
except (TypeError, ValueError):
    TG_DIST_TO_PEAK_MIN = 3.5


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