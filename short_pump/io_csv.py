# io_csv.py
import os
import csv
from typing import Dict, Any

from short_pump.logging_utils import get_logger, log_exception

logger = get_logger(__name__)


def append_csv(path: str, row: Dict[str, Any]) -> None:
    try:
        # Ensure parent dir exists (e.g. data/, runs/, logs/)
        parent = os.path.dirname(path)
        if parent:
            os.makedirs(parent, exist_ok=True)

        file_exists = os.path.isfile(path)
        with open(path, "a", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=list(row.keys()))
            if not file_exists:
                w.writeheader()
            w.writerow(row)
    except Exception as e:
        log_exception(logger, "CSV write failed", step="CSV_WRITE", extra={"path": path, "row_keys": list(row.keys()) if row else None})
        raise