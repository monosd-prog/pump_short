from pathlib import Path
import json, threading

_CONFIG_PATH = Path(__file__).parent.parent / "live_config.json"
_lock = threading.Lock()


def load_live_config() -> dict:
    with _lock:
        return json.loads(_CONFIG_PATH.read_text())


def get_live_profiles() -> set:
    """Возвращает set профилей со статусом 'live'."""
    cfg = load_live_config()
    profiles = cfg.get("profiles", {})
    return {name for name, status in profiles.items() if status == "live"}


def get_all_profiles() -> dict:
    """Возвращает dict {profile_name: status} для всех профилей."""
    return load_live_config().get("profiles", {})


def get_profile_meta() -> dict:
    return load_live_config().get("profile_meta", {})


def set_profile_status(profile: str, status: str) -> None:
    """Устанавливает статус профиля: 'live' или 'paper'."""
    assert status in ("live", "paper")
    with _lock:
        cfg = json.loads(_CONFIG_PATH.read_text())
        if "profiles" not in cfg:
            cfg["profiles"] = {}
        cfg["profiles"][profile] = status
        _CONFIG_PATH.write_text(json.dumps(cfg, indent=2, ensure_ascii=False))
