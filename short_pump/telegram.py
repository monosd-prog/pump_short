# telegram.py
import os
import requests

from short_pump.logging_utils import get_logger, log_exception

logger = get_logger(__name__)

TG_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TG_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
TG_SEND_OUTCOME = os.getenv("TG_SEND_OUTCOME", "0") == "1"


def send_telegram(text: str) -> None:
    if not TG_BOT_TOKEN or not TG_CHAT_ID:
        return
    try:
        url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage"
        payload = {"chat_id": TG_CHAT_ID, "text": text, "disable_web_page_preview": True}
        r = requests.post(url, json=payload, timeout=15)
        r.raise_for_status()
    except Exception as e:
        log_exception(logger, "Telegram send failed", step="TELEGRAM_SEND", extra={"text_preview": text[:100] if text else None})
        raise