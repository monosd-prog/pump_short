#!/usr/bin/env python3
"""Smoke test for tg_entry_filter: stage 4 + dist_to_peak_pct >= 3.5 only."""

import os

# Set defaults before import (tg reads env at import time)
os.environ.setdefault("TG_ENTRY_STAGE", "4")
os.environ.setdefault("TG_DIST_TO_PEAK_MIN", "3.5")

from short_pump.telegram import tg_entry_filter


def test_tg_entry_filter():
    """Stage 3: never send. Stage 4 + dist<3.5: never send. Stage 4 + dist>=3.5: send."""
    # Stage 3 -> never send
    assert tg_entry_filter(3, 5.0) is False, "stage=3 must not send"
    assert tg_entry_filter(3, 3.5) is False, "stage=3 must not send"
    assert tg_entry_filter(3, 10.0) is False, "stage=3 must not send"

    # Stage 4 + dist < 3.5 -> never send
    assert tg_entry_filter(4, 0.0) is False, "stage=4 dist=0 must not send"
    assert tg_entry_filter(4, 3.0) is False, "stage=4 dist=3 must not send"
    assert tg_entry_filter(4, 3.49) is False, "stage=4 dist=3.49 must not send"

    # Stage 4 + dist >= 3.5 -> send
    assert tg_entry_filter(4, 3.5) is True, "stage=4 dist=3.5 must send"
    assert tg_entry_filter(4, 4.0) is True, "stage=4 dist=4 must send"
    assert tg_entry_filter(4, 10.0) is True, "stage=4 dist=10 must send"

    # Missing/NaN -> never send
    assert tg_entry_filter(None, 5.0) is False, "stage=None must not send"
    assert tg_entry_filter(4, None) is False, "dist=None must not send"
    assert tg_entry_filter("", 5.0) is False, "stage='' must not send"
    assert tg_entry_filter(4, float("nan")) is False, "dist=NaN must not send"
    assert tg_entry_filter(4, "") is False, "dist='' must not send"

    print("✓ All tg_entry_filter smoke tests passed!")


if __name__ == "__main__":
    test_tg_entry_filter()
