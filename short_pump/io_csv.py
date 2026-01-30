# io_csv.py
import os
import csv
from typing import Dict, Any


def append_csv(path: str, row: Dict[str, Any]) -> None:
    file_exists = os.path.isfile(path)
    with open(path, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(row.keys()))
        if not file_exists:
            w.writeheader()
        w.writerow(row)