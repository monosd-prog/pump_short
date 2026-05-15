"""pytest path setup for pump_v2 tests."""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

FIXTURES = ROOT / "pump_v2" / "tests" / "fixtures"
LIVE_COLLECTED = FIXTURES / "live_collected"
LOGGED_PARITY_SAMPLES = FIXTURES / "logged_parity" / "samples"
SYMBOLS_FIXTURES = FIXTURES / "symbols"
