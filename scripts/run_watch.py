#!/usr/bin/env python3
import argparse
import time
import json

from short_pump.watcher import run_watch_for_symbol


def main() -> None:
    p = argparse.ArgumentParser(description="Run single watcher by symbol (CLI)")
    p.add_argument("--symbol", required=True, help="e.g. NOMUSDT")
    p.add_argument("--run-id", default=None, help="optional run_id")
    p.add_argument("--meta", default=None, help="optional JSON meta (string)")
    args = p.parse_args()

    symbol = args.symbol.strip().upper()
    run_id = args.run_id or time.strftime("%Y%m%d_%H%M%S")

    meta = {"source": "cli"}
    if args.meta:
        meta.update(json.loads(args.meta))

    result = run_watch_for_symbol(symbol=symbol, run_id=run_id, meta=meta)
    print(result)


if __name__ == "__main__":
    main()