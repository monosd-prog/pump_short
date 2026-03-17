from __future__ import annotations

import json
import logging
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple

from trading.config import (
    AUTO_RISK_GUARD_ENABLE,
    AUTO_RISK_GUARD_ENFORCE,
    AUTO_RISK_GUARD_STATE_PATH,
    EXECUTION_MODE,
)

logger = logging.getLogger(__name__)


# --- Public constants ---

STATE_ACTIVE = "ACTIVE"
STATE_WATCH = "WATCH"
STATE_DISABLED = "DISABLED"
STATE_RECOVERY = "RECOVERY"


@dataclass
class GuardMetrics:
    """Snapshot of risk metrics for a single mode."""

    wr: float
    ev_total: float
    ev20: float
    consistency: Optional[float]
    n_core: int
    trades_since_negative_start: int = 0


@dataclass
class GuardEntry:
    """Persisted guard state for one mode."""

    mode_name: str
    current_state: str = STATE_ACTIVE
    updated_at: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat(timespec="seconds")
    )
    reason: str = ""
    last_metrics: Dict[str, Any] = field(default_factory=dict)
    trades_since_negative_start: int = 0
    disabled_at: Optional[str] = None
    recovery_started_at: Optional[str] = None
    recovery_confirmations: int = 0
    observations_since_disabled: int = 0


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _load_state_raw() -> Dict[str, Any]:
    """Low-level JSON loader. Returns dict[mode_name, payload]."""
    path = AUTO_RISK_GUARD_STATE_PATH
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, dict):
            return {}
        return data
    except FileNotFoundError:
        return {}
    except Exception:
        logger.exception("AUTO_RISK_GUARD_STATE_LOAD_FAILED | path=%s", path)
        return {}


def _dump_state_raw(state: Dict[str, GuardEntry]) -> None:
    """Write state to JSON. Safe for external callers (e.g. analytics)."""
    path = AUTO_RISK_GUARD_STATE_PATH
    payload: Dict[str, Any] = {mode: asdict(entry) for mode, entry in state.items()}
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2, sort_keys=True)
        logger.info("AUTO_RISK_GUARD_STATE_SAVED | path=%s modes=%d", path, len(payload))
    except Exception:
        logger.exception("AUTO_RISK_GUARD_STATE_SAVE_FAILED | path=%s", path)


def load_guard_state() -> Dict[str, GuardEntry]:
    """Public: load persisted guard state. Missing/invalid -> ACTIVE by default."""
    raw = _load_state_raw()
    state: Dict[str, GuardEntry] = {}
    for mode_name, payload in raw.items():
        if not isinstance(payload, dict):
            continue
        current_state = str(payload.get("current_state") or STATE_ACTIVE)
        if current_state not in {
            STATE_ACTIVE,
            STATE_WATCH,
            STATE_DISABLED,
            STATE_RECOVERY,
        }:
            current_state = STATE_ACTIVE
        entry = GuardEntry(
            mode_name=mode_name,
            current_state=current_state,
            updated_at=str(
                payload.get("updated_at")
                or datetime.now(timezone.utc).isoformat(timespec="seconds")
            ),
            reason=str(payload.get("reason") or ""),
            last_metrics=dict(payload.get("last_metrics") or {}),
            trades_since_negative_start=int(
                payload.get("trades_since_negative_start") or 0
            ),
            disabled_at=payload.get("disabled_at"),
            recovery_started_at=payload.get("recovery_started_at"),
            recovery_confirmations=int(
                payload.get("recovery_confirmations") or 0
            ),
            observations_since_disabled=int(
                payload.get("observations_since_disabled") or 0
            ),
        )
        state[mode_name] = entry
    return state


def get_mode_state(mode_name: str) -> GuardEntry:
    """
    Return GuardEntry for mode_name.
    If no persisted entry yet, return ACTIVE-by-default synthetic entry.
    """
    state = load_guard_state()
    entry = state.get(mode_name)
    if entry is None:
        entry = GuardEntry(mode_name=mode_name, current_state=STATE_ACTIVE)
    return entry


def _mode_name_for_signal_and_profile(signal: Any, risk_profile_name: str) -> Optional[str]:
    """
    Map (strategy, risk_profile_name) to guard mode_name.

    We intentionally reuse existing risk profile identifiers as mode keys:
    - short_pump_active_1R
    - fast0_base_1R
    - fast0_1p5R
    - fast0_2R
    """
    s = (getattr(signal, "strategy", None) or "").strip()
    if not risk_profile_name:
        return None
    if s == "short_pump" and risk_profile_name in {
        "short_pump_active_1R",
        "short_pump_mid",
        "short_pump_deep",
    }:
        return risk_profile_name
    if s == "short_pump_fast0" and risk_profile_name in {
        "fast0_base_1R",
        "fast0_1p5R",
        "fast0_2R",
        "fast0_selective",
    }:
        return risk_profile_name
    return None


def is_entry_allowed_for_signal(signal: Any, risk_profile_name: str) -> Tuple[bool, str]:
    """
    Enforcement hook for runner/broker.

    - When risk_profile is not in guard set (mode_name is None) -> allow.
    - LIVE mode: DISABLED and RECOVERY ALWAYS block, regardless of AUTO_RISK_GUARD_ENABLE/ENFORCE.
      This is critical: DISABLED = no live orders, period.
    - Paper mode: respect AUTO_RISK_GUARD_ENABLE and ENFORCE (dry-run vs block).
    """
    mode_name = _mode_name_for_signal_and_profile(signal, risk_profile_name)
    if mode_name is None:
        # Guard is only defined for specific боевые режимы; everything else passes through.
        return True, ""

    entry = get_mode_state(mode_name)
    state = entry.current_state

    # LIVE mode: DISABLED/RECOVERY must ALWAYS block — no reliance on env flags.
    if (EXECUTION_MODE or "").strip().lower() == "live" and state in {STATE_DISABLED, STATE_RECOVERY}:
        logger.info(
            "AUTO_RISK_GUARD_BLOCKED | strategy=%s symbol=%s profile=%s state=%s reason=%s",
            getattr(signal, "strategy", ""),
            getattr(signal, "symbol", ""),
            mode_name,
            state,
            entry.reason or "guard_disabled",
        )
        return False, f"guard_state={state}"

    # Paper mode or ACTIVE/WATCH: respect feature flags
    if not AUTO_RISK_GUARD_ENABLE:
        return True, ""

    # Dry-run: log state but don't block.
    if not AUTO_RISK_GUARD_ENFORCE:
        logger.info(
            "AUTO_RISK_GUARD_DRY_RUN | mode=%s state=%s strategy=%s symbol=%s",
            mode_name,
            state,
            getattr(signal, "strategy", ""),
            getattr(signal, "symbol", ""),
        )
        return True, ""

    # Enforcement (paper): DISABLED and RECOVERY block
    if state in {STATE_DISABLED, STATE_RECOVERY}:
        logger.info(
            "AUTO_RISK_GUARD_BLOCKED | strategy=%s symbol=%s profile=%s state=%s reason=%s",
            getattr(signal, "strategy", ""),
            getattr(signal, "symbol", ""),
            mode_name,
            state,
            entry.reason or "",
        )
        return False, f"guard_state={state}"

    # ACTIVE / WATCH
    return True, ""


def next_state(current: GuardEntry, metrics: GuardMetrics) -> GuardEntry:
    """
    Pure state machine: apply transition rules based on latest metrics snapshot.

    This function does NOT touch disk; callers are expected to load/save JSON around it.
    """
    st = current.current_state or STATE_ACTIVE
    m = metrics

    # Helper predicates from spec
    n = m.n_core
    ev = m.ev_total
    ev20 = m.ev20
    cons = m.consistency if m.consistency is not None else None
    trades_neg = m.trades_since_negative_start

    def is_watch_trigger() -> bool:
        return (ev20 < 0) or (cons is not None and cons < 0.7)

    # Bootstrap: do not DISABLE purely on "no/low data".
    # While n_core < BOOTSTRAP_MIN_TRADES, modes are considered in warmup and can be ACTIVE/WATCH,
    # but must not go to DISABLED due to ev/ev20/cons alone.
    BOOTSTRAP_MIN_TRADES = 20

    def is_disable_trigger() -> bool:
        if n < BOOTSTRAP_MIN_TRADES:
            return False
        return (ev <= 0) or (cons is not None and cons < 0.4) or (
            ev20 < -0.10 and trades_neg >= 20
        )

    def is_watch_to_active() -> bool:
        return (ev20 >= 0) and (cons is not None and cons >= 0.7)

    def is_disabled_to_recovery() -> bool:
        return (
            n >= 10
            and ev20 > 0
            and (cons is not None and cons >= 0.6)
        )

    def is_recovery_to_active() -> bool:
        return (
            n >= 20
            and ev20 > 0
            and (cons is not None and cons >= 0.7)
        )

    def is_recovery_to_disabled() -> bool:
        return (ev20 <= 0) or (cons is not None and cons < 0.5)

    updated = GuardEntry(
        mode_name=current.mode_name,
        current_state=st,
        updated_at=_now_iso(),
        reason=current.reason,
        last_metrics={
            "wr": m.wr,
            "ev_total": m.ev_total,
            "ev20": m.ev20,
            "consistency": m.consistency,
            "n_core": m.n_core,
            "trades_since_negative_start": m.trades_since_negative_start,
        },
        trades_since_negative_start=m.trades_since_negative_start,
        disabled_at=current.disabled_at,
        recovery_started_at=current.recovery_started_at,
        recovery_confirmations=current.recovery_confirmations,
        observations_since_disabled=current.observations_since_disabled,
    )

    # ACTIVE -> WATCH / ACTIVE
    if st == STATE_ACTIVE:
        if is_disable_trigger():
            updated.current_state = STATE_DISABLED
            updated.disabled_at = _now_iso()
            updated.reason = "AUTO: edge degradation (ACTIVE->DISABLED)"
            logger.info(
                "AUTO_RISK_GUARD_STATE_CHANGE | mode=%s %s->%s reason=%s",
                current.mode_name,
                st,
                updated.current_state,
                updated.reason,
            )
            return updated
        if is_watch_trigger():
            updated.current_state = STATE_WATCH
            updated.reason = "AUTO: watch edge (ACTIVE->WATCH)"
            logger.info(
                "AUTO_RISK_GUARD_STATE_CHANGE | mode=%s %s->%s reason=%s",
                current.mode_name,
                st,
                updated.current_state,
                updated.reason,
            )
            return updated
        return updated

    # WATCH -> DISABLED / ACTIVE / stay WATCH
    if st == STATE_WATCH:
        if is_disable_trigger():
            updated.current_state = STATE_DISABLED
            updated.disabled_at = _now_iso()
            updated.reason = "AUTO: edge degradation (WATCH->DISABLED)"
            logger.info(
                "AUTO_RISK_GUARD_STATE_CHANGE | mode=%s %s->%s reason=%s",
                current.mode_name,
                st,
                updated.current_state,
                updated.reason,
            )
            return updated
        if is_watch_to_active():
            updated.current_state = STATE_ACTIVE
            updated.reason = "AUTO: edge restored (WATCH->ACTIVE)"
            logger.info(
                "AUTO_RISK_GUARD_STATE_CHANGE | mode=%s %s->%s reason=%s",
                current.mode_name,
                st,
                updated.current_state,
                updated.reason,
            )
            return updated
        return updated

    # DISABLED -> RECOVERY / stay DISABLED
    if st == STATE_DISABLED:
        updated.observations_since_disabled = current.observations_since_disabled + 1
        if is_disabled_to_recovery():
            updated.current_state = STATE_RECOVERY
            updated.recovery_started_at = _now_iso()
            updated.recovery_confirmations = 1
            updated.reason = "AUTO: recovery started (DISABLED->RECOVERY)"
            logger.info(
                "AUTO_RISK_GUARD_RECOVERY_START | mode=%s %s->%s reason=%s",
                current.mode_name,
                st,
                updated.current_state,
                updated.reason,
            )
            return updated
        return updated

    # RECOVERY -> ACTIVE / DISABLED / stay RECOVERY
    if st == STATE_RECOVERY:
        if is_recovery_to_disabled():
            updated.current_state = STATE_DISABLED
            updated.disabled_at = _now_iso()
            updated.reason = "AUTO: recovery failed (RECOVERY->DISABLED)"
            updated.recovery_confirmations = 0
            logger.info(
                "AUTO_RISK_GUARD_RECOVERY_FAILED | mode=%s %s->%s reason=%s",
                current.mode_name,
                st,
                updated.current_state,
                updated.reason,
            )
            return updated
        if is_recovery_to_active():
            updated.current_state = STATE_ACTIVE
            updated.reason = "AUTO: recovery confirmed (RECOVERY->ACTIVE)"
            updated.recovery_confirmations = current.recovery_confirmations + 1
            logger.info(
                "AUTO_RISK_GUARD_RECOVERY_CONFIRMED | mode=%s %s->%s reason=%s",
                current.mode_name,
                st,
                updated.current_state,
                updated.reason,
            )
            return updated
        # stay in RECOVERY, increase confirmations if metrics still positive
        if ev20 > 0 and cons is not None and cons >= 0.6:
            updated.recovery_confirmations = current.recovery_confirmations + 1
        return updated

    # Fallback: keep current state
    return updated


def update_guard_state_from_metrics(
    metrics_by_mode: Dict[str, GuardMetrics],
) -> Dict[str, GuardEntry]:
    """
    High-level helper for analytics process:
    - load JSON
    - run next_state() per mode
    - save JSON

    Trading runner only reads the resulting file; it does not call this.
    """
    current_state = load_guard_state()
    new_state: Dict[str, GuardEntry] = {}
    for mode_name, met in metrics_by_mode.items():
        prev = current_state.get(mode_name) or GuardEntry(mode_name=mode_name)
        new_state[mode_name] = next_state(prev, met)
    _dump_state_raw(new_state)
    return new_state

