"""Smoke: dataset path contains mode=<exec_mode> (paper/live) from EXECUTION_MODE; source_mode=exec_mode in CSV."""
from __future__ import annotations

import csv
import json
import os
import sys
import tempfile
from pathlib import Path

_repo_root = Path(__file__).resolve().parents[1]
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

from common.io_dataset import get_dataset_dir, write_event_row


def main() -> None:
    strategy = "short_pump"
    wall_time_utc = "2026-02-26T12:00:00+00:00"

    with tempfile.TemporaryDirectory(prefix="smoke_exec_mode_") as base:
        # --- 1) Strict path assert: path must contain /mode=EXECUTION_MODE/ (or as part)
        os.environ["EXECUTION_MODE"] = "paper"
        path_paper = get_dataset_dir(strategy, wall_time_utc, base_dir=base)
        assert f"/mode={os.environ['EXECUTION_MODE']}/" in path_paper or path_paper.rstrip("/").endswith(
            f"mode={os.environ['EXECUTION_MODE']}"
        ), f"paper → path must contain mode=paper, got: {path_paper}"
        assert "mode=paper" in Path(path_paper).parts, f"Path.parts must contain mode=paper: {Path(path_paper).parts}"
        print(f"OK paper path: {path_paper}")

        # --- 2) When EXECUTION_MODE=paper, no file must appear under .../mode=live/...
        write_event_row(
            {
                "run_id": "smoke_exec_mode",
                "event_id": "evt_smoke_1",
                "symbol": "BTCUSDT",
                "strategy": strategy,
                "side": "SHORT",
                "wall_time_utc": wall_time_utc,
                "time_utc": wall_time_utc,
                "stage": 2,
                "entry_ok": False,
                "skip_reasons": "smoke",
                "context_score": 0.0,
                "price": 1000.0,
                "dist_to_peak_pct": 0.0,
                "payload_json": "{}",
            },
            strategy=strategy,
            mode="live",  # source_mode; path must still be mode=paper
            wall_time_utc=wall_time_utc,
            base_dir=base,
        )
        live_files = list(Path(base).rglob("*"))
        live_under_mode_live = [p for p in live_files if "mode=live" in p.parts]
        assert not live_under_mode_live, (
            f"EXECUTION_MODE=paper must not write under mode=live; found: {live_under_mode_live}"
        )
        print("OK: no files under .../mode=live/ when exec_mode=paper")

        # --- 2b) In mode=paper dir, CSV source_mode must be paper (no source_mode=live)
        for ev_file in Path(path_paper).glob("events*.csv"):
            with open(ev_file, encoding="utf-8") as f:
                r = csv.DictReader(f)
                for row in r:
                    sm = row.get("source_mode", "")
                    assert sm == "paper", (
                        f"EXECUTION_MODE=paper must not have source_mode=live in CSV; "
                        f"found source_mode={sm!r} in {ev_file}"
                    )
                    # signal_source_mode (original) preserved in payload_json
                    pj = row.get("payload_json", "{}")
                    try:
                        p = json.loads(pj)
                        assert p.get("signal_source_mode") == "live", f"expected signal_source_mode=live in {pj}"
                    except json.JSONDecodeError:
                        pass
        print("OK: no source_mode=live in mode=paper CSV when exec_mode=paper; signal_source_mode in payload")

        # Live path assert
        os.environ["EXECUTION_MODE"] = "live"
        path_live = get_dataset_dir(strategy, wall_time_utc, base_dir=base)
        assert f"/mode={os.environ['EXECUTION_MODE']}/" in path_live or path_live.rstrip("/").endswith(
            f"mode={os.environ['EXECUTION_MODE']}"
        ), f"live → path must contain mode=live, got: {path_live}"
        assert "mode=live" in Path(path_live).parts, f"Path.parts must contain mode=live: {Path(path_live).parts}"
        print(f"OK live path: {path_live}")

        # --- 3) When EXECUTION_MODE=live, no new file must appear under .../mode=paper/... (before/after)
        before = set(str(p) for p in Path(base).rglob("*"))
        os.environ["EXECUTION_MODE"] = "live"
        write_event_row(
            {
                "run_id": "smoke_exec_mode_live",
                "event_id": "evt_smoke_live_1",
                "symbol": "BTCUSDT",
                "strategy": strategy,
                "side": "SHORT",
                "wall_time_utc": wall_time_utc,
                "time_utc": wall_time_utc,
                "stage": 2,
                "entry_ok": False,
                "skip_reasons": "smoke",
                "context_score": 0.0,
                "price": 1000.0,
                "dist_to_peak_pct": 0.0,
                "payload_json": "{}",
            },
            strategy=strategy,
            mode="paper",  # source_mode; path must still be mode=live
            wall_time_utc=wall_time_utc,
            base_dir=base,
        )
        after = set(str(p) for p in Path(base).rglob("*"))
        new_paths = after - before
        new_mode_paper = [p for p in new_paths if "mode=paper" in Path(p).parts]
        assert new_mode_paper == [], (
            f"EXECUTION_MODE=live must not create new files under mode=paper; created: {new_mode_paper}"
        )
        print("OK: no new files under .../mode=paper/ when exec_mode=live")

    print("smoke_dataset_exec_mode: path reflects exec_mode; paper/live do not write under wrong mode")


if __name__ == "__main__":
    main()
