from __future__ import annotations

import subprocess
import time
from datetime import datetime, timezone
from typing import Optional


def run_id() -> str:
    return time.strftime("%Y%m%d_%H%M%S")


def wall_time_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S%z")


def code_version() -> str:
    try:
        out = subprocess.check_output(["git", "rev-parse", "--short", "HEAD"], stderr=subprocess.DEVNULL)
        return out.decode().strip()
    except Exception:
        return "unknown"
