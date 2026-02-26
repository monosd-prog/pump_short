#!/usr/bin/env python3
"""Smoke: verify is_tradeable_short_pump gating logic. stage=4 + dist>=3.5 -> tradeable."""
from __future__ import annotations

import os
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT))

# Override TG_ENTRY_DIST_MIN for test
os.environ["TG_ENTRY_DIST_MIN"] = "3.5"

from short_pump.telegram import is_tradeable_short_pump


def main() -> int:
    # stage=4, dist>=3.5 -> tradeable
    assert is_tradeable_short_pump(4, 4.0) is True
    assert is_tradeable_short_pump(4, 3.5) is True
    # stage!=4 or dist<3.5 -> not tradeable
    assert is_tradeable_short_pump(4, 3.4) is False
    assert is_tradeable_short_pump(4, None) is False
    assert is_tradeable_short_pump(3, 5.0) is False
    assert is_tradeable_short_pump(5, 5.0) is False
    print("is_tradeable_short_pump: stage=4+dist>=3.5 -> True, else False. OK.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
