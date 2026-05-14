"""pump_v2 main runner. Phase 1 scaffold — does nothing yet."""

import logging
import sys

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("pump_v2")


def main() -> None:
    log.info("pump_v2 runner — Phase 1 scaffold, not executing yet")
    log.info("See pump_v2/ARCHITECTURE.md for full plan")
    sys.exit(0)


if __name__ == "__main__":
    main()
