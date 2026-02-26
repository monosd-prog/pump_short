#!/usr/bin/env python3
"""Ensure trading_closes.csv has full header (mfe_pct, mae_pct, mfe_r, mae_r). Migrate if needed."""
from __future__ import annotations

import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT))

from trading.paper_outcome import _ensure_closes_header


def main() -> int:
    path = str(_ROOT / "datasets" / "trading_closes.csv")
    result = _ensure_closes_header(path)
    if result == "migrated":
        print("migrated (header extended, backup created)")
    elif result == "ok":
        print("already ok")
    elif result == "created":
        print("created (new file with full header)")
    else:
        print(result)
    return 0


if __name__ == "__main__":
    sys.exit(main())
