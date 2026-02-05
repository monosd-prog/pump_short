from __future__ import annotations

import csv
import sys
from datetime import datetime, timezone
from pathlib import Path
from statistics import median


def _parse_float(val: str | None) -> float | None:
    if val is None:
        return None
    s = str(val).strip()
    if s == "" or s.lower() in ("nan", "none", "null"):
        return None
    try:
        return float(s)
    except ValueError:
        return None


def main() -> None:
    if len(sys.argv) > 1:
        date_str = sys.argv[1]
    else:
        date_str = datetime.now(timezone.utc).strftime("%Y%m%d")

    rel_path = Path("datasets") / f"date={date_str}" / "strategy=short_pump" / "mode=live" / "events_v2.csv"
    path = Path(__file__).resolve().parents[1] / rel_path
    if not path.exists():
        print(f"events_v2.csv not found: {path}")
        return

    total_rows = 0
    values: list[float] = []
    with path.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            total_rows += 1
            v = _parse_float(row.get("dist_to_peak_pct"))
            if v is not None:
                values.append(v)

    if values:
        min_v = min(values)
        med_v = median(values)
        max_v = max(values)
        print(f"rows={total_rows} non_null={len(values)} min={min_v:.4f} median={med_v:.4f} max={max_v:.4f}")
    else:
        print(f"rows={total_rows} non_null=0 min=n/a median=n/a max=n/a")


if __name__ == "__main__":
    main()
