#!/usr/bin/env python3
"""
Smoke: FAST0_AUTO_ENABLE env parsing (_bool_env default handling).
- unset -> default True
- explicit 0 -> False
- explicit 1 -> True
"""
from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[1]


def _run_check(env_extra: dict, expected: bool, label: str) -> None:
    env = os.environ.copy()
    env.pop("FAST0_AUTO_ENABLE", None)
    env.update(env_extra)
    cmd = [
        sys.executable,
        "-c",
        "from trading.risk_profile import FAST0_AUTO_ENABLE; "
        f"assert FAST0_AUTO_ENABLE is {expected}, f'expected {expected}, got {{FAST0_AUTO_ENABLE}}'; "
        f"print('OK: {label}')",
    ]
    r = subprocess.run(cmd, cwd=str(_REPO_ROOT), env=env)
    if r.returncode != 0:
        sys.exit(1)


def main() -> None:
    # unset -> True (default)
    _run_check({}, True, "unset -> default True")
    # explicit 0 -> False
    _run_check({"FAST0_AUTO_ENABLE": "0"}, False, "FAST0_AUTO_ENABLE=0 -> False")
    # explicit 1 -> True
    _run_check({"FAST0_AUTO_ENABLE": "1"}, True, "FAST0_AUTO_ENABLE=1 -> True")
    # explicit true -> True
    _run_check({"FAST0_AUTO_ENABLE": "true"}, True, "FAST0_AUTO_ENABLE=true -> True")
    # explicit false -> False
    _run_check({"FAST0_AUTO_ENABLE": "false"}, False, "FAST0_AUTO_ENABLE=false -> False")
    print("smoke_fast0_auto_enable: all checks passed")


if __name__ == "__main__":
    main()
