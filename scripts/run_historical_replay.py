#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from pathlib import Path

# Ensure pump_short root on path
_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from replay.input_loader import load_signals
from replay.runner import SUPPORTED_IN_LAB_V1, UNAVAILABLE_IN_LAB_V1, run_replay_for_signals


def main() -> None:
    p = argparse.ArgumentParser(description="Historical replay / lab runner (datasets writer)")
    p.add_argument("--signals-file", required=True, help="CSV or JSONL with replay signals")
    p.add_argument("--data-dir", required=True, help="Datasets root (contains date=* folders or its parent)")
    p.add_argument("--mode", default="lab", help="Datasets mode (default: lab; MVP locks to lab)")
    p.add_argument("--strategies", nargs="*", default=None, help="Filter strategies (default: all in signals)")
    p.add_argument("--dry-run", action="store_true", help="Do not write datasets, just validate and count")
    args = p.parse_args()

    print("[LAB MODE V1] candle-only replay.")
    print(f"[LAB MODE V1] supported={list(SUPPORTED_IN_LAB_V1)}")
    print(f"[LAB MODE V1] unavailable={list(UNAVAILABLE_IN_LAB_V1)}")
    print(
        "[LAB MODE V1] WARNING: Delta/CVD/Liq/OI/Funding/Microstructure are unavailable unless historical sources are added."
    )
    print("[LAB MODE V1] WARNING: stage/context_score are NOT reconstructed from watcher runtime in v1.")
    print("")

    signals = load_signals(args.signals_file)
    if args.strategies:
        allowed = set(args.strategies)
        signals = [s for s in signals if s.strategy in allowed]

    res = run_replay_for_signals(
        signals=signals,
        data_dir=args.data_dir,
        mode=args.mode,
        dry_run=bool(args.dry_run),
    )
    print(f"[REPLAY] signals={len(signals)} events={res['events']} trades={res['trades']} outcomes={res['outcomes']} errors={res['errors']}")


if __name__ == "__main__":
    main()

