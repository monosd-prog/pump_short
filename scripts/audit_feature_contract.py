from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from analytics.load import load_events_v2
from common.feature_contract import canonical_event_fields


def _pct(x: float) -> str:
    return f"{x * 100:.0f}%"


def audit_strategy(*, base_dir: Path, strategy: str, mode: str, days: int | None) -> dict:
    df = load_events_v2(data_dir=base_dir, strategy=strategy, mode=mode, days=days, raw=True)
    if df is None or df.empty:
        return {"strategy": strategy, "rows": 0, "missing": canonical_event_fields(), "present": [], "coverage": 0.0}

    cols = list(df.columns)
    canon = canonical_event_fields()
    present = [c for c in canon if c in cols]
    missing = [c for c in canon if c not in cols]

    non_empty = {}
    for c in present:
        s = df[c]
        if pd.api.types.is_numeric_dtype(s):
            non_empty[c] = float(s.notna().mean())
        else:
            non_empty[c] = float(s.astype(str).replace({"": None, "None": None, "nan": None, "NaN": None}).notna().mean())

    avg_cov = float(sum(non_empty.values()) / len(non_empty)) if non_empty else 0.0
    return {
        "strategy": strategy,
        "rows": int(len(df)),
        "present": present,
        "missing": missing,
        "avg_non_empty_coverage": avg_cov,
        "per_field_non_empty_coverage": non_empty,
    }


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--base-dir", required=True, help="datasets root (directory containing date=... folders or its parent)")
    p.add_argument("--days", type=int, default=30)
    p.add_argument("--mode", default="live")
    p.add_argument("--strategies", default="short_pump,short_pump_fast0")
    args = p.parse_args()

    base_dir = Path(args.base_dir)
    days = int(args.days) if args.days and args.days > 0 else None
    mode = str(args.mode)
    strategies = [s.strip() for s in str(args.strategies).split(",") if s.strip()]

    print(f"[AUDIT] base_dir={base_dir} mode={mode} days={days} strategies={strategies}")
    canon = canonical_event_fields()
    print(f"[AUDIT] canonical_fields={len(canon)}")

    for strat in strategies:
        res = audit_strategy(base_dir=base_dir, strategy=strat, mode=mode, days=days)
        print("")
        print(f"[AUDIT] strategy={res['strategy']} rows={res['rows']}")
        print(f"[AUDIT] present={len(res.get('present', []))} missing={len(res.get('missing', []))}")
        if res.get("missing"):
            print(f"[AUDIT] missing_fields={res['missing']}")
        if res.get("rows", 0) > 0:
            avg = res.get("avg_non_empty_coverage", 0.0)
            print(f"[AUDIT] avg_non_empty_coverage={_pct(avg)}")
            cov = res.get("per_field_non_empty_coverage", {})
            worst = sorted(cov.items(), key=lambda kv: kv[1])[:10]
            print("[AUDIT] worst_10_non_empty_coverage:")
            for k, v in worst:
                print(f"  - {k}: {_pct(v)}")


if __name__ == "__main__":
    main()

