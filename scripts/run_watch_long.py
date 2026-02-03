from __future__ import annotations

import argparse

from long_pullback.watcher import run_watch_for_symbol


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--symbol", required=False, dest="symbol_opt")
    parser.add_argument("symbol", nargs="?", default=None)
    args = parser.parse_args()
    symbol = args.symbol_opt or args.symbol
    if not symbol:
        print("Usage: python scripts/run_watch_long.py --symbol SYMBOL")
        return
    print(run_watch_for_symbol(symbol))


if __name__ == "__main__":
    main()
