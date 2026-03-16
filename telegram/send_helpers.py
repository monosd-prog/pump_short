"""
Telegram send helpers: sendMessage, sendPhoto.
Used by report_bot and daily_tg_report.
"""
from __future__ import annotations

import mimetypes
import sys
import uuid
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Optional, Union

try:
    import requests
    HAS_REQUESTS = True
except ImportError:
    HAS_REQUESTS = False


def send_message(
    token: str,
    chat_id: Union[str, int],
    text: str,
    reply_to: Optional[int] = None,
    timeout: int = 20,
) -> None:
    """Send text message via Telegram sendMessage API."""
    chat_id = int(chat_id) if isinstance(chat_id, str) and chat_id.isdigit() else chat_id
    if HAS_REQUESTS:
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        payload = {"chat_id": chat_id, "text": text}
        if reply_to is not None:
            payload["reply_to_message_id"] = reply_to
        resp = requests.post(url, json=payload, timeout=timeout)
        resp.raise_for_status()
        return
    # Fallback: urllib
    base_url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {"chat_id": str(chat_id), "text": text}
    if reply_to is not None:
        payload["reply_to_message_id"] = str(reply_to)
    data = urllib.parse.urlencode(payload).encode("utf-8")
    req = urllib.request.Request(
        base_url,
        data=data,
        method="POST",
        headers={"Content-Type": "application/x-www-form-urlencoded; charset=utf-8"},
    )
    with urllib.request.urlopen(req, timeout=timeout) as response:
        response.read()


def send_photo(
    token: str,
    chat_id: Union[str, int],
    photo_path: Union[str, Path],
    caption: Optional[str] = None,
    timeout: int = 30,
) -> None:
    """Send photo via Telegram sendPhoto (multipart/form-data)."""
    photo_path = Path(photo_path)
    if not photo_path.exists():
        raise FileNotFoundError(f"Photo file not found: {photo_path}")
    chat_id = int(chat_id) if isinstance(chat_id, str) and chat_id.isdigit() else chat_id
    base_url = f"https://api.telegram.org/bot{token}/sendPhoto"
    photo_bytes = photo_path.read_bytes()
    filename = photo_path.name
    mime_type, _ = mimetypes.guess_type(filename)
    mime_type = mime_type or "image/png"
    boundary = "----pump_short_boundary_" + uuid.uuid4().hex
    CRLF = b"\r\n"
    parts = []

    def _mp_add_text(name: str, value: str) -> None:
        parts.append(f"--{boundary}".encode("utf-8"))
        parts.append(CRLF)
        parts.append(f'Content-Disposition: form-data; name="{name}"'.encode("utf-8"))
        parts.append(CRLF)
        parts.append(CRLF)
        parts.append(value.encode("utf-8"))
        parts.append(CRLF)

    def _mp_add_file(name: str, fname: str, content_type: str, data: bytes) -> None:
        parts.append(f"--{boundary}".encode("utf-8"))
        parts.append(CRLF)
        parts.append(f'Content-Disposition: form-data; name="{name}"; filename="{fname}"'.encode("utf-8"))
        parts.append(CRLF)
        parts.append(f"Content-Type: {content_type}".encode("utf-8"))
        parts.append(CRLF)
        parts.append(CRLF)
        parts.append(data)
        parts.append(CRLF)

    _mp_add_text("chat_id", str(chat_id))
    _mp_add_file("photo", filename, mime_type, photo_bytes)
    if caption:
        _mp_add_text("caption", caption)
    parts.append(f"--{boundary}--".encode("utf-8"))
    parts.append(CRLF)
    body_bytes = b"".join(parts)
    req = urllib.request.Request(
        base_url,
        data=body_bytes,
        method="POST",
        headers={"Content-Type": f"multipart/form-data; boundary={boundary}"},
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as response:
            response.read()
    except urllib.error.HTTPError as e:
        error_body = ""
        try:
            error_body = e.read().decode("utf-8", errors="replace")
        except Exception:
            pass
        print(f"TG sendPhoto failed: {e.code} {e.reason} body={error_body}", file=sys.stderr)
        raise


def send_document(
    token: str,
    chat_id: Union[str, int],
    file_path: Union[str, Path],
    caption: Optional[str] = None,
    timeout: int = 30,
) -> None:
    """Send document (e.g. TXT factor report) via Telegram sendDocument."""
    file_path = Path(file_path)
    if not file_path.exists():
        raise FileNotFoundError(f"Document file not found: {file_path}")
    chat_id = int(chat_id) if isinstance(chat_id, str) and chat_id.isdigit() else chat_id
    base_url = f"https://api.telegram.org/bot{token}/sendDocument"
    file_bytes = file_path.read_bytes()
    filename = file_path.name
    mime_type, _ = mimetypes.guess_type(filename)
    mime_type = mime_type or "text/plain"
    boundary = "----pump_short_boundary_" + uuid.uuid4().hex
    CRLF = b"\r\n"
    parts = []

    def _mp_add_text(name: str, value: str) -> None:
        parts.append(f"--{boundary}".encode("utf-8"))
        parts.append(CRLF)
        parts.append(f'Content-Disposition: form-data; name="{name}"'.encode("utf-8"))
        parts.append(CRLF)
        parts.append(CRLF)
        parts.append(value.encode("utf-8"))
        parts.append(CRLF)

    def _mp_add_file(name: str, fname: str, content_type: str, data: bytes) -> None:
        parts.append(f"--{boundary}".encode("utf-8"))
        parts.append(CRLF)
        parts.append(f'Content-Disposition: form-data; name="{name}"; filename="{fname}"'.encode("utf-8"))
        parts.append(CRLF)
        parts.append(f"Content-Type: {content_type}".encode("utf-8"))
        parts.append(CRLF)
        parts.append(CRLF)
        parts.append(data)
        parts.append(CRLF)

    _mp_add_text("chat_id", str(chat_id))
    _mp_add_file("document", filename, mime_type, file_bytes)
    if caption:
        _mp_add_text("caption", caption)
    parts.append(f"--{boundary}--".encode("utf-8"))
    parts.append(CRLF)
    body_bytes = b"".join(parts)
    req = urllib.request.Request(
        base_url,
        data=body_bytes,
        method="POST",
        headers={"Content-Type": f"multipart/form-data; boundary={boundary}"},
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as response:
            response.read()
    except urllib.error.HTTPError as e:
        error_body = ""
        try:
            error_body = e.read().decode("utf-8", errors="replace")
        except Exception:
            pass
        print(f"TG sendDocument failed: {e.code} {e.reason} body={error_body}", file=sys.stderr)
        raise
