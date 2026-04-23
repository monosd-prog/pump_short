from pathlib import Path
import json, threading

_CONFIG_PATH = Path(__file__).parent.parent / "live_config.json"
_lock = threading.Lock()


def load_live_config() -> dict:
    with _lock:
        return json.loads(_CONFIG_PATH.read_text())


def get_live_profiles() -> set:
    return set(load_live_config().get("live_profiles", []))


def get_live_strategies() -> list:
    return load_live_config().get("live_strategies", [])


def get_paper_strategies() -> list:
    return load_live_config().get("paper_strategies", [])


def get_profile_meta() -> dict:
    return load_live_config().get("profile_meta", {})


def set_live_profiles(profiles: list) -> None:
    with _lock:
        cfg = json.loads(_CONFIG_PATH.read_text())
        cfg["live_profiles"] = profiles
        _CONFIG_PATH.write_text(json.dumps(cfg, indent=2, ensure_ascii=False))
