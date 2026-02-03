from __future__ import annotations

import sys

from long_pullback.watcher import run_watch_for_symbol


def main() -> None:
    if len(sys.argv) < 2:
        print("Usage: python scripts/run_watch_long.py SYMBOL")
        return
    symbol = sys.argv[1]
    print(run_watch_for_symbol(symbol))


if __name__ == "__main__":
    main()
