#!/usr/bin/env python3
"""
Telegram bot for on-demand compact autotrading report.
Commands: /report [days], /help.
Run from pump_short root: PYTHONPATH=. python3 -m telegram.report_bot
"""
from __future__ import annotations

import os
import sys
import time
import traceback
from pathlib import Path
from typing import Optional, Tuple

try:
    import requests
except ImportError:
    requests = None  # type: ignore

# Ensure pump_short root on path when run as script
_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from telegram.send_helpers import send_message as _send_message, send_document as _send_document
from scripts.daily_tg_report import generate_compact_autotrading_report  # type: ignore
from analytics.factor_report import save_factor_report_files


def _get_bot_token() -> str:
    return (os.getenv("TELEGRAM_BOT_TOKEN") or os.getenv("TG_BOT_TOKEN") or "").strip()


def _parse_report_args(text: str) -> Tuple[Optional[int], Optional[str]]:
    """Parse /report [days]. Returns (days, error_msg)."""
    parts = text.strip().split(maxsplit=1)
    if len(parts) == 1:
        return 30, None
    arg = parts[1].strip()
    try:
        days = int(arg)
    except ValueError:
        return None, f"Некорректный параметр дней: '{arg}'. Используйте целое число, например: /report 7"
    if days <= 0:
        return None, "Число дней должно быть > 0."
    if days > 365:
        return 365, "Максимальное окно — 365 дней. Использую 365."
    return days, None


HELP_TEXT = (
    "Команды:\n"
    "- /report [days] — сформировать компактный отчёт по автоторговле (short_pump + FAST0) за N дней\n"
    "  примеры: /report 7, /report 30\n"
    "- /report_factors [days] — факторный отчёт (single factors, combos, candidates) за N дней (default 14)\n"
    "- /help — показать это сообщение\n"
)


def _handle_report(token: str, chat_id: int, msg_id: int, text: str) -> None:
    days, err = _parse_report_args(text)
    if err:
        _send_message(token, chat_id, err, reply_to=msg_id)
        if days is None:
            return
    days_use = days if days is not None else 30
    if err:
        _send_message(token, chat_id, err, reply_to=msg_id)
    _send_message(token, chat_id, f"Генерирую отчёт за {days_use} дней...", reply_to=msg_id)
    try:
        data_root = os.getenv("TG_REPORT_DATA_ROOT") or "/root/pump_short/datasets"
        report = generate_compact_autotrading_report(days_use, data_dir=data_root)
        if len(report) > 4096:
            report = report[:4093] + "..."
        _send_message(token, chat_id, report)
    except Exception as e:
        tb = traceback.format_exc(limit=3)
        _send_message(
            token,
            chat_id,
            f"Ошибка при генерации отчёта: {type(e).__name__}: {e}\n\n{tb}",
        )


def _handle_report_factors(token: str, chat_id: int, msg_id: int, text: str) -> None:
    """
    /report_factors [days]
    - default days = 14
    - generate factor report via analytics.factor_report
    - send short summary as message
    - send full TXT as document
    """
    parts = text.strip().split(maxsplit=1)
    days = 14
    if len(parts) == 2:
        arg = parts[1].strip()
        try:
            val = int(arg)
            if 1 <= val <= 365:
                days = val
            elif val > 365:
                days = 365
                _send_message(token, chat_id, "Максимальное окно для factor report — 365 дней. Использую 365.", reply_to=msg_id)
        except ValueError:
            _send_message(
                token,
                chat_id,
                f"Некорректный параметр дней для /report_factors: '{arg}'. Использую 14.",
                reply_to=msg_id,
            )
    _send_message(token, chat_id, f"Генерирую факторный отчёт за {days} дней...", reply_to=msg_id)
    try:
        data_root = os.getenv("FACTOR_REPORT_DATA_ROOT") or os.getenv("TG_REPORT_DATA_ROOT") or "/root/pump_short/datasets"
        txt_path, json_path, summary = save_factor_report_files(
            base_dir=data_root,
            days=days,
            strategies=None,
        )
        # Short summary as text
        _send_message(token, chat_id, summary)
        # Full TXT as document
        _send_document(token, chat_id, txt_path, caption=f"Factor report {days}d")
    except Exception as e:
        tb = traceback.format_exc(limit=3)
        _send_message(
            token,
            chat_id,
            f"Ошибка при генерации factor report: {type(e).__name__}: {e}\n\n{tb}",
        )


def _handle_help(token: str, chat_id: int, msg_id: int) -> None:
    _send_message(token, chat_id, HELP_TEXT, reply_to=msg_id)


def main() -> None:
    if requests is None:
        print("pip install requests required for report_bot", file=sys.stderr)
        sys.exit(1)
    token = _get_bot_token()
    if not token:
        print("TELEGRAM_BOT_TOKEN/TG_BOT_TOKEN не задан — бот не может стартовать.", file=sys.stderr)
        sys.exit(1)

    base_url = f"https://api.telegram.org/bot{token}"
    offset: Optional[int] = None
    print("telegram.report_bot: started long-polling for /report and /help", file=sys.stderr)

    while True:
        try:
            params = {"timeout": 60}
            if offset is not None:
                params["offset"] = offset
            resp = requests.get(f"{base_url}/getUpdates", params=params, timeout=70)
            resp.raise_for_status()
            data = resp.json()
            for update in data.get("result", []):
                offset = update["update_id"] + 1
                msg = update.get("message") or update.get("edited_message")
                if not msg:
                    continue
                chat = msg.get("chat") or {}
                chat_id = chat.get("id")
                if chat_id is None:
                    continue
                msg_id = msg.get("message_id")
                text = (msg.get("text") or "").strip()
                if not text.startswith("/"):
                    continue
                cmd_part = text.split(maxsplit=1)[0]
                cmd = cmd_part.split("@", 1)[0]
                if cmd == "/report":
                    _handle_report(token, int(chat_id), int(msg_id), text)
                elif cmd == "/report_factors":
                    _handle_report_factors(token, int(chat_id), int(msg_id), text)
                elif cmd == "/help":
                    _handle_help(token, int(chat_id), int(msg_id))
        except KeyboardInterrupt:
            print("telegram.report_bot: interrupted, exiting", file=sys.stderr)
            break
        except Exception as e:
            print(f"telegram.report_bot: error: {type(e).__name__}: {e}", file=sys.stderr)
            time.sleep(5)


if __name__ == "__main__":
    main()
