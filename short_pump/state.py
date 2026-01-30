# short_pump/state.py
from dataclasses import dataclass
from typing import Any, Dict
import time

from short_pump.config import Config


@dataclass
class Job:
    symbol: str
    run_id: str
    started_at_utc: str
    meta: Dict[str, Any]


@dataclass
class AppState:
    cfg: Config
    active: Dict[str, Job]
    last_started: Dict[str, float]
    done_recent: Dict[str, Dict[str, Any]]

    def cooldown_ok(self, symbol: str) -> bool:
        ts = self.last_started.get(symbol)
        if ts is None:
            return True
        return (time.time() - ts) >= self.cfg.cooldown_minutes * 60


def make_state(cfg: Config) -> AppState:
    return AppState(
        cfg=cfg,
        active={},
        last_started={},
        done_recent={},
    )