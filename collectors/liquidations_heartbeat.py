#!/usr/bin/env python3
"""
Daily liquidations CSV summary → Telegram (cron / systemd timer).

Reads datasets/liquidations.csv, aggregates last 24h (UTC), sends one message.
Exit 0.
"""
from __future__ import annotations

import csv
import logging
import os
import sys
import urllib.parse
import urllib.request
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path

CSV_PATH = Path(__file__).resolve().parent.parent / "datasets" / "liquidations.csv"

log = logging.getLogger("liquidations_heartbeat")


def _parse_ts_utc(s: str) -> datetime | None:
    s = s.strip()
    if not s:
        return None
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except ValueError:
        return None


def _send_telegram(text: str) -> None:
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")
    if not token or not chat_id:
        log.warning("Telegram skipped: missing TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID")
        return
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    data = urllib.parse.urlencode({"chat_id": chat_id, "text": text}).encode()
    req = urllib.request.Request(url, data=data)
    with urllib.request.urlopen(req, timeout=30) as resp:
        resp.read()
    log.info("Telegram message sent OK")


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(hours=24)

    if not CSV_PATH.exists():
        log.warning("CSV missing: %s", CSV_PATH)
        _send_telegram(
            "📊 Liquidations Collector — Daily Heartbeat\n\n"
            f"CSV not found: {CSV_PATH}\n"
            "(no data to summarize)"
        )
        return

    rows_in_window: list[dict[str, str]] = []
    with CSV_PATH.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        if reader.fieldnames is None:
            log.warning("Empty or invalid CSV header")
            _send_telegram(
                "📊 Liquidations Collector — Daily Heartbeat\n\n"
                "Invalid CSV (no header row)."
            )
            return
        for row in reader:
            ts_s = row.get("ts_utc") or ""
            dt = _parse_ts_utc(ts_s)
            if dt is None or dt < cutoff:
                continue
            rows_in_window.append(row)

    total_count = len(rows_in_window)
    sym_counter: Counter[str] = Counter()
    total_value = 0.0
    first_ts: datetime | None = None
    last_ts: datetime | None = None

    for row in rows_in_window:
        sym = (row.get("symbol") or "").strip() or "?"
        sym_counter[sym] += 1
        try:
            total_value += float(row.get("value_usd") or 0.0)
        except ValueError:
            pass
        ts_s = row.get("ts_utc") or ""
        dt = _parse_ts_utc(ts_s)
        if dt is None:
            continue
        if first_ts is None or dt < first_ts:
            first_ts = dt
        if last_ts is None or dt > last_ts:
            last_ts = dt

    unique_symbols = len(sym_counter)
    top5 = sym_counter.most_common(5)

    period_lo = first_ts.isoformat(timespec="seconds") if first_ts else "(n/a)"
    period_hi = last_ts.isoformat(timespec="seconds") if last_ts else "(n/a)"

    lines = [
        "📊 Liquidations Collector — Daily Heartbeat",
        "",
        f"Period: {period_lo} → {period_hi}",
        f"Total liquidations: {total_count}",
        f"Unique symbols: {unique_symbols}",
        f"Total value: ${total_value:,.0f}",
        "",
        "Top 5 by count:",
    ]
    if top5:
        for sym, cnt in top5:
            lines.append(f"{sym}: {cnt}")
    else:
        lines.append("(none)")

    if total_count == 0:
        lines.extend(
            [
                "",
                "🚨 ALERT: 0 liquidations in last 24h — collector likely broken",
            ]
        )

    msg = "\n".join(lines)
    log.info(
        "Summary 24h: count=%d unique=%d value=%.2f",
        total_count,
        unique_symbols,
        total_value,
    )
    _send_telegram(msg)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log.info("shutdown")
    sys.exit(0)
