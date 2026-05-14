import csv
from datetime import datetime, timezone
from pathlib import Path
from threading import Lock

FUNNEL_LOG_PATH = Path("/root/pump_short/pump_v2/logs/funnel_log.csv")
FUNNEL_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)

_STAGES = (
    "candidate_scanned",
    "signal_generated",
    "risk_passed",
    "position_filter_passed",
    "sent_to_exchange",
    "order_executed",
    "closed",
)

_lock = Lock()
_header_written = FUNNEL_LOG_PATH.exists()


def log_funnel_event(
    strategy: str, symbol: str, stage: str, blocked_reason: str = ""
) -> None:
    global _header_written
    if stage not in _STAGES:
        raise ValueError(f"Unknown stage: {stage}")
    with _lock:
        is_new = not FUNNEL_LOG_PATH.exists()
        with FUNNEL_LOG_PATH.open("a", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            if is_new or not _header_written:
                w.writerow(["ts_utc", "strategy", "symbol", "stage", "blocked_reason"])
                _header_written = True
            w.writerow(
                [
                    datetime.now(timezone.utc).isoformat(),
                    strategy,
                    symbol,
                    stage,
                    blocked_reason,
                ]
            )
