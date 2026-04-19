"""
Executive compact report for Telegram (/report): lifecycle guard blocks,
aggregates, ML readiness, data health (monospace code block).
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

import os
import pandas as pd

CORE_DECISION_WINDOW = 20  # мин. новых core сделок до решения по режиму при EV(20)<0

from .fast0_blocks import FAST0_OP_DIST_MAX, FAST0_OP_LIQ_5K, FAST0_OP_LIQ_25K, FAST0_OP_LIQ_100K
from .short_pump_blocks import filter_active_trades
from short_pump.rollout import SHORT_PUMP_FILTERED_DIST_TO_PEAK_MAX
from .stats import (
    LIGHTGBM_COMFORT,
    LIGHTGBM_MIN,
    _core_mask,
    _normalize_outcome_raw,
    ev_core_from_tp_sl_pnl,
    rolling_wr_ev_core,
    wr_core_from_tp_sl,
)


def _calendar_days(date_range: Tuple[str, str]) -> int:
    start, end = date_range
    try:
        s = pd.to_datetime(start)
        e = pd.to_datetime(end)
        delta = (e - s).days + 1
        return max(1, int(delta))
    except Exception:
        return 1


def _pnl_series(df: pd.DataFrame) -> pd.Series:
    return pd.to_numeric(df["pnl_pct"], errors="coerce").fillna(0.0)


def _core_pnl_series(df: pd.DataFrame) -> pd.Series:
    mask = _core_mask(df["outcome"])
    return pd.to_numeric(df.loc[mask, "pnl_pct"], errors="coerce").dropna()


def _mdd(pnl: pd.Series) -> float:
    if pnl.empty:
        return 0.0
    equity = pnl.cumsum()
    dd = equity - equity.cummax()
    return float(dd.min()) if dd.notna().any() else 0.0


def _edge_consistency_frac(pnl: pd.Series, rolling_n: int) -> Optional[float]:
    """Доля последних 10 окон EV(rolling_n) > 0. Возвращает 0.0-1.0 или None."""
    if pnl.empty or len(pnl) < rolling_n:
        return None
    roll_ev = pnl.rolling(rolling_n, min_periods=rolling_n).mean()
    last_10 = roll_ev.tail(10).dropna()
    if len(last_10) < 5:
        return None
    return float((last_10 > 0).sum()) / len(last_10)


def _trades_since_ev_negative(pnl: pd.Series, rolling_n: int) -> int:
    """
    Количество core-сделок с момента начала текущей отрицательной фазы EV(rolling_n).
    Идём назад от последней сделки, пока EV(20) <= 0; считаем длину этой серии.
    """
    if pnl.empty or len(pnl) < rolling_n:
        return 0
    roll_ev = pnl.rolling(rolling_n, min_periods=rolling_n).mean()
    last_idx = len(roll_ev) - 1
    if roll_ev.iloc[last_idx] > 0:
        return 0
    start_idx = last_idx
    for i in range(last_idx - 1, -1, -1):
        v = roll_ev.iloc[i]
        if pd.isna(v) or v > 0:
            break
        start_idx = i
    return last_idx - start_idx + 1


EV20_SOFT_DRAWDOWN = -0.10  # EV20 >= -0.10R — мягкая просадка, наблюдаем
EXP_THRESHOLD = 20  # n_core < EXP_THRESHOLD = EXPERIMENT


def _ev20_signal_emoji(ev20: float) -> str:
    """EV20 >= 0.25 🔥 | 0..0.25 🟢 | -0.25..0 ⚠️ | < -0.25 🔴"""
    if ev20 >= 0.25:
        return "🔥"
    if ev20 > 0:
        return "🟢"
    if ev20 >= -0.25:
        return "⚠️"
    return "🔴"


def _health_score(
    ev20: float,
    wr_pct: float,
    mdd_r: float,
    guard_state_str: Optional[str],
    is_experiment: bool,
) -> int:
    """Health 0–100. EV20/WR/MDD/Guard contributions + EXPERIMENT penalty."""
    ev_c = 25 if ev20 >= 0.2 else (15 if ev20 >= 0 else (5 if ev20 >= -0.2 else -10))
    wr_c = 25 if wr_pct >= 55 else (15 if wr_pct >= 50 else (5 if wr_pct >= 45 else -10))
    mdd_c = 20 if mdd_r <= 5 else (10 if mdd_r <= 10 else (0 if mdd_r <= 15 else -10))
    g_c = 20 if guard_state_str == "ACTIVE" else (
        10 if guard_state_str == "WATCH" else (
            5 if guard_state_str == "RECOVERY" else (-20 if guard_state_str == "DISABLED" else 0)
        )
    )
    exp_c = -10 if is_experiment else 0
    return max(0, min(100, 50 + ev_c + wr_c + mdd_c + g_c + exp_c))


def _health_score_submode(
    *,
    ev_total: float,
    ev20: float,
    wr_pct: float,
    n_core: int,
    mdd_r: float,
    guard_state_str: Optional[str],
) -> int:
    """
    Health score for a submode (risk_profile).
    EV20 has highest weight; low sample sizes are penalized and capped.
    """
    if n_core <= 0:
        return 50
    ev20_c = 20 if ev20 >= 0.2 else (12 if ev20 >= 0 else (4 if ev20 >= -0.2 else -12))
    ev_c = 10 if ev_total >= 0.2 else (6 if ev_total >= 0 else (0 if ev_total >= -0.2 else -8))
    wr_c = 20 if wr_pct >= 55 else (12 if wr_pct >= 50 else (4 if wr_pct >= 45 else -10))
    n_c = 10 if n_core >= 50 else (5 if n_core >= 20 else (0 if n_core >= 10 else -10))
    mdd_c = 15 if mdd_r <= 5 else (8 if mdd_r <= 10 else (0 if mdd_r <= 15 else -8))
    g_c = 15 if guard_state_str == "ACTIVE" else (
        8 if guard_state_str == "WATCH" else (
            4 if guard_state_str == "RECOVERY" else (-20 if guard_state_str == "DISABLED" else 0)
        )
    )
    base = max(0, min(100, 50 + ev20_c + ev_c + wr_c + n_c + mdd_c + g_c))
    if n_core < 10:
        base = min(base, 60)
    return base


def _health_emoji(health: int) -> str:
    """health >= 70 🟢 | 40–70 🟡 | < 40 🔴"""
    return "🟢" if health >= 70 else ("🟡" if health >= 40 else "🔴")


# Стандартные исходы: не считаются «нестандартными», не портят quality
_STANDARD_OUTCOMES = frozenset(
    {"TP_hit", "SL_hit", "TIMEOUT", "EARLY_EXIT", "TPSL_SETUP_FAILED_CLOSED"}
)


def _dataset_quality_metrics(df: pd.DataFrame) -> tuple[int, int, int, int, bool]:
    """
    Подсчёт метрик качества датасета из outcomes.
    Возвращает: (n_duplicates, n_outcome_other, n_early_exit, n_conflicts, quality_ok).

    Стандартные исходы (не влияют на quality): TP_hit, SL_hit, TIMEOUT, EARLY_EXIT,
    TPSL_SETUP_FAILED_CLOSED. Конфликты (tp_sl_same_candle) — информационная метрика.
    quality_ok = (дубли == 0 и нестандартные исходы == 0).
    """
    import json
    n_dup, n_other, n_early, n_conf = 0, 0, 0, 0
    if df is None or df.empty:
        return (0, 0, 0, 0, True)
    # Дубли по event_id
    if "event_id" in df.columns:
        n_dup = int(len(df) - df["event_id"].nunique()) if df["event_id"].notna().any() else 0
    # Нестандартные исходы (исключая стандартные); EARLY_EXIT — отдельно, информативно
    if "outcome" in df.columns:
        raw_out = df["outcome"].astype(str).str.upper().str.strip()
        standard_mask = raw_out.isin({s.upper() for s in _STANDARD_OUTCOMES})
        n_other = int((~standard_mask).sum())
        n_early = int((raw_out == "EARLY_EXIT").sum())
    # Конфликты связок (tp_sl_same_candle) — информационная метрика, не влияет на quality
    if "details_json" in df.columns:
        for _, row in df.iterrows():
            val = row.get("details_json")
            if pd.isna(val):
                continue
            try:
                d = json.loads(val) if isinstance(val, str) else (val if isinstance(val, dict) else {})
                v = d.get("tp_sl_same_candle")
                if v is not None and str(v).strip() in ("1", "1.0"):
                    n_conf += 1
                    continue
                pl = d.get("details_payload")
                if isinstance(pl, str) and pl.strip().startswith("{"):
                    pl = json.loads(pl)
                if isinstance(pl, dict):
                    v = pl.get("tp_sl_same_candle")
                    if v is not None and str(v).strip() in ("1", "1.0"):
                        n_conf += 1
            except Exception:
                pass
    quality_ok = (n_dup == 0 and n_other == 0)
    return (n_dup, n_other, n_early, n_conf, quality_ok)


DECISION_WINDOW_VERDICT = CORE_DECISION_WINDOW  # отключение при trades_since_negative_start >= N
RECOVERY_MIN_N = 10  # DISABLED→RECOVERY: мин. сделок с EV20>0, cons≥0.6
RECOVERY_TO_ACTIVE_N = 20  # RECOVERY→ACTIVE: мин. сделок с EV20>0, cons≥0.7


def _guard_state_str(guard_state: Optional[Dict[str, Any]], guard_key: str) -> Optional[str]:
    """Return current_state for guard_key or None."""
    if guard_state is None:
        return None
    entry = guard_state.get(guard_key)
    if not isinstance(entry, dict):
        return None
    return (entry.get("current_state") or "").strip().upper() or None


# Whitelist for Bybit live execution — must match trading/runner.py (allowed_live_profiles).
ALLOWED_LIVE_PROFILES_FOR_REPORT = frozenset(
    {"short_pump_active_1R", "fast0_selective", "short_pump_filtered_1R"}
)

_LIVE_ELIGIBILITY_GUARD_KEYS: tuple[str, ...] = (
    "short_pump_mid",
    "short_pump_deep",
    "short_pump_active_1R",
    "short_pump_filtered_1R",
    "fast0_selective",
    "fast0_base_1R",
    "fast0_1p5R",
    "fast0_2R",
)


def _parse_strategies_env_for_report() -> set[str]:
    """Same parsing as trading/runner._get_allowed_strategies (reporting only)."""
    raw = os.getenv(
        "STRATEGIES",
        "short_pump,short_pump_filtered,short_pump_fast0,short_pump_fast0_filtered",
    ).strip()
    parts = [s.strip() for s in raw.split(",") if s.strip()]
    if not parts:
        parts = ["short_pump", "short_pump_filtered", "short_pump_fast0", "short_pump_fast0_filtered"]
    return set(parts)


def _producer_strategies_for_guard_key(guard_key: str) -> frozenset[str]:
    """Strategies that can emit signals for this risk_profile / guard bucket."""
    if guard_key in ("short_pump_active_1R", "short_pump_mid", "short_pump_deep"):
        return frozenset({"short_pump"})
    if guard_key == "short_pump_filtered_1R":
        return frozenset({"short_pump_filtered"})
    if guard_key in ("fast0_selective", "fast0_base_1R", "fast0_1p5R", "fast0_2R"):
        return frozenset({"short_pump_fast0", "short_pump_fast0_filtered"})
    if guard_key == "short_pump_fast0_filtered_1R":
        return frozenset({"short_pump_fast0_filtered"})
    if guard_key == "short_pump_premium_1R":
        return frozenset({"short_pump_premium"})
    if guard_key == "short_pump_wick_1R":
        return frozenset({"short_pump_wick"})
    return frozenset()


def _live_eligibility_parts(
    guard_key: str,
    guard_state_str: Optional[str],
    strategies_set: set[str],
) -> tuple[bool, bool, bool, bool, str]:
    """
    Returns (strategy_ok, live_whitelist_ok, guard_active_ok, final_eligible, reason).
    final_eligible is YES only if all three flags are true (runner policy).
    """
    producers = _producer_strategies_for_guard_key(guard_key)
    strat_ok = bool(producers & strategies_set) if producers else False
    wl_ok = guard_key in ALLOWED_LIVE_PROFILES_FOR_REPORT
    gst = (guard_state_str or "").strip().upper()
    guard_active_ok = gst == "ACTIVE"
    final = strat_ok and wl_ok and guard_active_ok
    if not strat_ok:
        reason = "strategy not in STRATEGIES"
    elif not wl_ok:
        reason = "not in live whitelist"
    elif not guard_active_ok:
        reason = "guard not ACTIVE"
    else:
        reason = "eligible for Bybit live"
    return strat_ok, wl_ok, guard_active_ok, final, reason


def _append_live_eligibility_submode(
    lines: list[str],
    guard_key: str,
    guard_state_str: Optional[str],
    strategies_set: set[str],
) -> None:
    """Two lines: header + YES/NO with reason (and strat/whitelist flags if not fully eligible)."""
    strat_ok, wl_ok, _act_ok, final, reason = _live_eligibility_parts(
        guard_key, guard_state_str, strategies_set
    )
    yn = "YES" if final else "NO"
    lines.append("        🔹 Live eligibility")
    if final:
        lines.append(f"        {yn} — {reason}")
    else:
        lines.append(
            f"        {yn} — {reason} (STRATEGIES: {'Y' if strat_ok else 'N'}, "
            f"whitelist: {'Y' if wl_ok else 'N'})"
        )


def _append_live_eligibility_matrix(
    lines: list[str],
    guard_state: Optional[Dict[str, Any]],
    strategies_set: set[str],
) -> None:
    lines.append("🎯 LIVE ELIGIBILITY (Bybit runner policy)")
    lines.append("")
    lines.append("    risk_profile | guard_state | live_eligible | reason")
    for gk in _LIVE_ELIGIBILITY_GUARD_KEYS:
        gst = _guard_state_str(guard_state, gk) or "N/A"
        _s, _w, _a, fin, reason = _live_eligibility_parts(gk, gst, strategies_set)
        le = "YES" if fin else "NO"
        lines.append(f"    {gk} | {gst} | {le} | {reason}")
    lines.append("")


def _guard_progress_text(
    guard_state: Optional[Dict[str, Any]],
    guard_key: str,
    *,
    trades_since_negative_start: Optional[int] = None,
    decision_window: int = DECISION_WINDOW_VERDICT,
) -> Optional[str]:
    """
    Progress-text до следующего состояния guard.
    WATCH → до DISABLED; DISABLED → до RECOVERY; RECOVERY → до ACTIVE; ACTIVE → None.
    """
    st = _guard_state_str(guard_state, guard_key)
    if not st:
        return None
    st = st.upper()
    if st == "ACTIVE":
        return None
    if st == "WATCH":
        if trades_since_negative_start is not None:
            rem = max(0, decision_window - trades_since_negative_start)
            return f"до DISABLED → ~{rem} сделок"
        return "до DISABLED → ?"
    if st == "DISABLED":
        return f"до RECOVERY → ~{RECOVERY_MIN_N}+ сделок (EV20>0, cons≥60%)"
    if st == "RECOVERY":
        return f"до ACTIVE → ~{RECOVERY_TO_ACTIVE_N} сделок (EV20>0, cons≥70%)"
    return None


def _guard_progress_watch_to_active(
    trades_since_negative_start: int,
    decision_window: int = DECISION_WINDOW_VERDICT,
) -> str:
    """Оценка: сколько сделок до возможного возврата в ACTIVE (EV20≥0, cons≥70%)."""
    rem_to_disabled = max(0, decision_window - trades_since_negative_start)
    to_active = min(rem_to_disabled, 6) if rem_to_disabled > 0 else 6
    return f"до ACTIVE → ~{to_active} сделок"


def _guard_emoji_label(state: Optional[str], n_core: int = 0) -> str:
    """Compact guard label for v2: 🟢 ACTIVE, 🟡 WATCH, 🔴 DISABLED, 🧪 ACTIVE (эксп)."""
    if not state:
        return "⚪ N/A"
    st = state.upper()
    if st == "ACTIVE":
        return "🧪 ACTIVE (эксп)" if n_core < 20 else "🟢 ACTIVE"
    if st == "WATCH":
        return "🟡 WATCH"
    if st == "DISABLED":
        return "🔴 DISABLED"
    if st == "RECOVERY":
        return "🟡 RECOVERY"
    return f"⚪ {st}"


def _guard_status_text(
    guard_state: Optional[Dict[str, Any]],
    guard_key: str,
    *,
    trades_since_negative_start: Optional[int] = None,
    decision_window: int = DECISION_WINDOW_VERDICT,
) -> Optional[str]:
    """
    Текст по фактическому guard_state (decision-layer).
    WATCH: при наличии trades_since_negative_start — «до финального вердикта ещё N сделок».
    """
    if guard_state is None:
        return None
    entry = guard_state.get(guard_key)
    if not isinstance(entry, dict):
        return None
    st = (entry.get("current_state") or "").strip().upper()
    if not st:
        return None
    if st == "ACTIVE":
        return "🟢 режим активен (торгуется)"
    if st == "WATCH":
        if trades_since_negative_start is not None:
            remaining = max(0, decision_window - trades_since_negative_start)
            return f"🟡 кандидат на отключение (до финального вердикта ещё {remaining} сделок)"
        return "🟡 режим в WATCH (под наблюдением guard'а)"
    if st == "DISABLED":
        return "🔴 режим AUTO DISABLED guard'ом (live-входы блокируются, аналитика продолжается)"
    if st == "RECOVERY":
        return "🟡 режим в RECOVERY (ожидаем подтверждение восстановления)"
    return None


def _diagnosis_v2(
    ev: float,
    ev20: float,
    n_core: int,
    consistency: Optional[float],
    trades_since_negative_start: int,
    guard_state_str: Optional[str],
    guard_status_text: Optional[str],
    decision_window: int = DECISION_WINDOW_VERDICT,
) -> list[str]:
    """
    Краткий диагноз для v2. Приоритет guard_state; иначе аналитическая эвристика.
    Возвращает список строк (1–3 строки).
    """
    if guard_status_text and guard_state_str:
        st = guard_state_str.upper()
        if st == "ACTIVE":
            if n_core < 20:
                return ["сильный edge" if ev > 0 else "edge под вопросом", "но маленькая выборка"]
            if ev > 0 and ev20 > 0:
                return ["edge стабильный"]
            if ev > 0 and ev20 <= 0:
                return ["edge нейтральный", "локальная просадка"]
            return ["edge под вопросом"]
        if st == "WATCH":
            return ["edge нейтральный", "наблюдение guard"]
        if st == "DISABLED":
            return ["edge деградировал", "режим остановлен"]
        if st == "RECOVERY":
            return ["ожидаем подтверждение восстановления"]
    if n_core < 20:
        return ["мало данных"]
    if ev <= 0:
        return ["edge отрицательный", "режим под вопросом"]
    if consistency is not None and consistency < 0.4:
        return ["edge деградировал", "можно отключать"]
    if ev20 < EV20_SOFT_DRAWDOWN and trades_since_negative_start >= decision_window:
        return ["edge деградировал", "можно отключать"]
    if ev20 >= 0 and consistency is not None and consistency >= 0.7:
        return ["edge стабильный"]
    remaining = max(0, decision_window - trades_since_negative_start)
    return ["edge нейтральный", "наблюдение", f"ждём ещё ~{remaining} сделок"]


def _submode_status_with_decision(
    ev: float,
    ev20: float,
    n_core: int,
    consistency: Optional[float] = None,
    trades_since_negative_start: int = 0,
    decision_window: int = CORE_DECISION_WINDOW,
    guard_status_text: Optional[str] = None,
) -> str:
    """
    Согласованная логика с блоком ВЫВОД.
    Если guard_status_text задан — приоритет у guard_state; иначе аналитическая эвристика.
    """
    if guard_status_text is not None:
        return guard_status_text
    if n_core < 20:
        return "🧪 мало данных"
    if ev <= 0:
        return "🔴 режим под вопросом"
    if consistency is not None and consistency < 0.4:
        return "🔴 можно отключать"
    remaining = max(0, decision_window - trades_since_negative_start)
    if ev20 < EV20_SOFT_DRAWDOWN and trades_since_negative_start >= decision_window:
        return "🔴 можно отключать"
    if ev20 >= 0 and consistency is not None and consistency >= 0.7:
        return "🟢 стабильный режим"
    suffix = ""
    if ev20 < 0 and remaining > 0:
        suffix = f" (ждём ещё ~{remaining} сделок)"
    return f"🟡 наблюдаем{suffix}"


def _edge_consistency_suffix(pnl: pd.Series, rolling_n: int) -> str:
    """Доля последних 10 окон EV(rolling_n), которые > 0. Возвращает строку для вывода."""
    if pnl.empty or len(pnl) < rolling_n:
        return "недостаточно данных"
    roll_ev = pnl.rolling(rolling_n, min_periods=rolling_n).mean()
    last_10 = roll_ev.tail(10).dropna()
    if len(last_10) < 5:
        return "недостаточно данных"
    n_positive = int((last_10 > 0).sum())
    consistency = n_positive / len(last_10)
    pct = int(round(consistency * 100))
    if consistency >= 0.7:
        return f"{pct}% 🟢 edge стабильный"
    if consistency >= 0.4:
        return f"{pct}% 🟡 edge нейтральный"
    return f"{pct}% 🔴 возможная деградация edge"


def _fast0_operational_mask(df: pd.DataFrame) -> pd.Series:
    """dist<=1.5 and liq in [0, (5k,25k], >100k]."""
    if "dist_to_peak_pct" not in df.columns:
        return pd.Series([False] * len(df), index=df.index)
    dist = pd.to_numeric(df["dist_to_peak_pct"], errors="coerce")
    valid_dist = dist.notna() & (dist <= FAST0_OP_DIST_MAX)
    liq_col = "liq_long_usd_30s" if "liq_long_usd_30s" in df.columns else (
        "liq_long_count_30s" if "liq_long_count_30s" in df.columns else None
    )
    if liq_col is None:
        return valid_dist
    liq = pd.to_numeric(df[liq_col], errors="coerce")
    op_liq = (liq.isna()) | (liq == 0) | (
        (liq > FAST0_OP_LIQ_5K) & (liq <= FAST0_OP_LIQ_25K)
    ) | (liq > FAST0_OP_LIQ_100K)
    return valid_dist & op_liq


def _fast0_infer_mask_base_1r(df: pd.DataFrame) -> pd.Series:
    """Infer base_1R from dist/liq when risk_profile is missing."""
    if "dist_to_peak_pct" not in df.columns:
        return pd.Series([False] * len(df), index=df.index)
    dist = pd.to_numeric(df["dist_to_peak_pct"], errors="coerce")
    valid = dist.notna() & (dist <= FAST0_OP_DIST_MAX)
    liq_col = "liq_long_usd_30s" if "liq_long_usd_30s" in df.columns else "liq_long_count_30s"
    if liq_col not in df.columns:
        return pd.Series([False] * len(df), index=df.index)
    liq = pd.to_numeric(df[liq_col], errors="coerce")
    return valid & (liq.isna() | (liq == 0))


def _fast0_infer_mask_1p5r(df: pd.DataFrame) -> pd.Series:
    """Infer 1p5R from dist/liq when risk_profile is missing."""
    if "dist_to_peak_pct" not in df.columns:
        return pd.Series([False] * len(df), index=df.index)
    dist = pd.to_numeric(df["dist_to_peak_pct"], errors="coerce")
    valid = dist.notna() & (dist <= FAST0_OP_DIST_MAX)
    liq_col = "liq_long_usd_30s" if "liq_long_usd_30s" in df.columns else "liq_long_count_30s"
    if liq_col not in df.columns:
        return pd.Series([False] * len(df), index=df.index)
    liq = pd.to_numeric(df[liq_col], errors="coerce")
    return valid & liq.notna() & (liq > FAST0_OP_LIQ_5K) & (liq <= FAST0_OP_LIQ_25K)


def _fast0_infer_mask_2r(df: pd.DataFrame) -> pd.Series:
    """Infer 2R from dist/liq when risk_profile is missing."""
    if "dist_to_peak_pct" not in df.columns:
        return pd.Series([False] * len(df), index=df.index)
    dist = pd.to_numeric(df["dist_to_peak_pct"], errors="coerce")
    valid = dist.notna() & (dist <= FAST0_OP_DIST_MAX)
    liq_col = "liq_long_usd_30s" if "liq_long_usd_30s" in df.columns else "liq_long_count_30s"
    if liq_col not in df.columns:
        return pd.Series([False] * len(df), index=df.index)
    liq = pd.to_numeric(df[liq_col], errors="coerce")
    return valid & liq.notna() & (liq > FAST0_OP_LIQ_100K)


def _fast0_mode_mask_base_1r(df: pd.DataFrame) -> pd.Series:
    """Prefer risk_profile when present; fallback to dist/liq inference."""
    rp = df["risk_profile"].astype(str).str.strip() if "risk_profile" in df.columns else pd.Series([""] * len(df), index=df.index)
    has_rp = rp.notna() & (rp != "") & (rp != "nan")
    by_profile = has_rp & (rp.str.lower() == "fast0_base_1r")
    fallback = ~has_rp & _fast0_infer_mask_base_1r(df)
    return by_profile | fallback


def _fast0_mode_mask_1p5r(df: pd.DataFrame) -> pd.Series:
    """Prefer risk_profile when present; fallback to dist/liq inference."""
    rp = df["risk_profile"].astype(str).str.strip() if "risk_profile" in df.columns else pd.Series([""] * len(df), index=df.index)
    has_rp = rp.notna() & (rp != "") & (rp != "nan")
    by_profile = has_rp & (rp.str.lower() == "fast0_1p5r")
    fallback = ~has_rp & _fast0_infer_mask_1p5r(df)
    return by_profile | fallback


def _fast0_mode_mask_2r(df: pd.DataFrame) -> pd.Series:
    """Prefer risk_profile when present; fallback to dist/liq inference."""
    rp = df["risk_profile"].astype(str).str.strip() if "risk_profile" in df.columns else pd.Series([""] * len(df), index=df.index)
    has_rp = rp.notna() & (rp != "") & (rp != "nan")
    by_profile = has_rp & (rp.str.lower() == "fast0_2r")
    fallback = ~has_rp & _fast0_infer_mask_2r(df)
    return by_profile | fallback


def _fast0_mode_mask_selective(df: pd.DataFrame) -> pd.Series:
    """Prefer risk_profile when present; fallback to context_score window + volume_1m gate.

    Fallback mirrors production routing in risk_profile.py:
    - context_score in [0.4, 0.6)
    - volume_1m is None (not available) OR volume_1m > 100_000
    When volume_1m column is absent from df, the volume gate is skipped (same as vol=None in prod).
    """
    rp = df["risk_profile"].astype(str).str.strip() if "risk_profile" in df.columns else pd.Series([""] * len(df), index=df.index)
    has_rp = rp.notna() & (rp != "") & (rp != "nan")
    by_profile = has_rp & (rp.str.lower() == "fast0_selective")
    if "context_score" in df.columns:
        cs = pd.to_numeric(df["context_score"], errors="coerce")
        fb_cs = (~has_rp) & cs.notna() & (cs >= 0.4) & (cs < 0.6)
        if "volume_1m" in df.columns:
            vol = pd.to_numeric(df["volume_1m"], errors="coerce")
            fb = fb_cs & (vol.isna() | (vol > 100_000.0))
        else:
            fb = fb_cs
    else:
        fb = pd.Series([False] * len(df), index=df.index)
    return by_profile | fb


def _timeout_count(df: pd.DataFrame) -> int:
    """Count TIMEOUT outcomes in df. Returns 0 if df empty or no outcome column."""
    if df is None or df.empty or "outcome" not in df.columns:
        return 0
    norm = df["outcome"].apply(_normalize_outcome_raw)
    return int((norm == "TIMEOUT").sum())


def _filter_lines_short_pump_active(tg_dist_min: float) -> list[str]:
    """Filter description for SHORT_PUMP ACTIVE."""
    d = f"{tg_dist_min}" if tg_dist_min == int(tg_dist_min) else f"{tg_dist_min}"
    return ["stage = 4", f"dist ≥ {d}%", "liq = 0"]


def _filter_lines_short_pump_filtered(max_dist: float) -> list[str]:
    d = f"{max_dist}" if max_dist == int(max_dist) else f"{max_dist}"
    return [
        "strategy = short_pump_filtered",
        f"dist ≤ {d}%",
        "same standard 1R sizing",
    ]


def _filter_lines_fast0_filtered(max_dist: float) -> list[str]:
    d = f"{max_dist}" if max_dist == int(max_dist) else f"{max_dist}"
    return [
        "strategy = short_pump_fast0_filtered",
        f"dist ≤ {d}%",
        "same fast0 sizing / liq buckets",
    ]


def _filter_lines_short_pump_mid() -> list[str]:
    return [
        "strategy = short_pump",
        "dist ∈ [3.5, 5)%",
        "context_score ∈ [0.40, 0.60)",
    ]


def _filter_lines_short_pump_deep() -> list[str]:
    return [
        "strategy = short_pump",
        "dist ∈ [7.5, 10)%",
        "context_score ∈ [0.40, 0.60)",
        "liqL30s = 0",
    ]


def _short_pump_mode_mask(df: pd.DataFrame, *, mode_name: str) -> pd.Series:
    """Prefer risk_profile when present; fallback to feature filters for known modes."""
    rp = df["risk_profile"].astype(str).str.strip().str.lower() if "risk_profile" in df.columns else pd.Series([""] * len(df), index=df.index)
    has_rp = rp.notna() & (rp != "") & (rp != "nan")
    by_profile = has_rp & (rp == mode_name.lower())
    if mode_name == "short_pump_mid":
        if "dist_to_peak_pct" in df.columns and "context_score" in df.columns:
            dist = pd.to_numeric(df["dist_to_peak_pct"], errors="coerce")
            cs = pd.to_numeric(df["context_score"], errors="coerce")
            fb = (~has_rp) & dist.notna() & cs.notna() & (dist >= 3.5) & (dist < 5.0) & (cs >= 0.4) & (cs < 0.6)
            return by_profile | fb
        return by_profile
    if mode_name == "short_pump_deep":
        if "stage" in df.columns and "dist_to_peak_pct" in df.columns and "context_score" in df.columns and "liq_long_usd_30s" in df.columns:
            st = pd.to_numeric(df["stage"], errors="coerce")
            dist = pd.to_numeric(df["dist_to_peak_pct"], errors="coerce")
            cs = pd.to_numeric(df["context_score"], errors="coerce")
            liq = pd.to_numeric(df["liq_long_usd_30s"], errors="coerce")
            fb = (~has_rp) & st.notna() & dist.notna() & cs.notna() & liq.notna() & (st == 3) & (dist >= 7.5) & (dist < 10.0) & (cs >= 0.4) & (cs < 0.6) & (liq == 0)
            return by_profile | fb
        return by_profile
    return by_profile


def _filter_lines_fast0_base() -> list[str]:
    """Filter description for FAST0 BASE (liq=0)."""
    return [
        f"dist ≈ 0.8–{FAST0_OP_DIST_MAX}%",
        "context_score ≥ 0.60",
        "liqL30s = 0",
    ]


def _filter_lines_fast0_selective() -> list[str]:
    return [
        f"dist ≈ 0.8–{FAST0_OP_DIST_MAX}%",
        "context_score ∈ [0.40, 0.60)",
        "(опц.) volume_1m > 100,000 если доступно",
    ]


def _filter_lines_fast0_1p5r() -> list[str]:
    """Filter description for FAST0 1.5R."""
    return [
        f"dist ≈ 0.8–{FAST0_OP_DIST_MAX}%",
        "risk = 1.5R",
        f"liqL30s ∈ (5k, 25k]",
    ]


def _filter_lines_fast0_2r() -> list[str]:
    """Filter description for FAST0 2R."""
    return [
        f"dist ≈ 0.8–{FAST0_OP_DIST_MAX}%",
        "risk = 2R",
        "liqL30s > 100k",
    ]


# Paper core = TP_hit, SL_hit, TIMEOUT (for N); GUARD_BLOCKED excluded from core metrics
PAPER_CORE_OUTCOMES = ("TP_hit", "SL_hit", "TIMEOUT")

# Paper FAST0 validation: risk_profile labels for monitoring block
PAPER_FAST0_PROFILES = [
    ("fast0_base_1R", "base_1R"),
    ("fast0_1p5R", "1.5R"),
    ("fast0_2R", "2R"),
]


def _paper_fast0_validation_lines(df_fast0_paper: Optional[pd.DataFrame]) -> list[str]:
    """
    Сверка paper FAST0 core outcomes по подрежимам (N/TP/SL/TIMEOUT) и флаг заполненности risk_profile.
    Источник: mode=paper, strategy=short_pump_fast0, outcome in (TP_hit, SL_hit, TIMEOUT). GUARD_BLOCKED не входит.
    """
    out: list[str] = []
    if df_fast0_paper is None or df_fast0_paper.empty:
        out.append("    paper FAST0: нет paper core outcomes")
        return out
    outcome_norm = df_fast0_paper["outcome"].apply(_normalize_outcome_raw)
    core_mask = outcome_norm.isin(PAPER_CORE_OUTCOMES)
    df_core = df_fast0_paper[core_mask]
    if df_core.empty:
        out.append("    paper FAST0: нет paper core outcomes")
        return out
    rp_col = "risk_profile" if "risk_profile" in df_core.columns else None
    rp_series = df_core["risk_profile"].astype(str).str.strip().str.lower() if rp_col else pd.Series([""] * len(df_core), index=df_core.index)
    missing_rp = (rp_series == "") | (rp_series == "nan")
    n_missing = int(missing_rp.sum())

    for profile_key, label_short in PAPER_FAST0_PROFILES:
        mask = rp_series == profile_key.lower()
        sub = df_core[mask]
        n = len(sub)
        if n == 0:
            out.append(f"    FAST0 PAPER {label_short}: N=0")
            continue
        out_norm = sub["outcome"].apply(_normalize_outcome_raw)
        tp = int((out_norm == "TP_hit").sum())
        sl = int((out_norm == "SL_hit").sum())
        to = int((out_norm == "TIMEOUT").sum())
        out.append(f"    FAST0 PAPER {label_short}: N={n}  TP={tp}  SL={sl}  TIMEOUT={to}")
    if n_missing == 0:
        out.append("    paper risk_profile filled: OK")
    else:
        out.append(f"    paper risk_profile missing: {n_missing}")
    return out


# --- Lifecycle compact report (Telegram /report): monospace, ≤48 cols ---
_LIFECYCLE_GK_ORDER: Tuple[Tuple[str, str], ...] = (
    ("short_pump_mid", "SP MID"),
    ("short_pump_deep", "SP DEP"),
    ("short_pump_active_1R", "SP ACT"),
    ("short_pump_filtered_1R", "SP FLT"),
    ("short_pump_premium_1R", "SP PRM"),
    ("short_pump_wick_1R", "SP WCK"),
    ("fast0_selective", "F0 SEL"),
    ("fast0_base_1R", "F0 B0"),
    ("fast0_1p5R", "F0 15"),
    ("fast0_2R", "F0 2R"),
)

_GK_METRIC_LABEL: Dict[str, str] = {
    "short_pump_mid": "SHORT_PUMP MID",
    "short_pump_deep": "SHORT_PUMP DEEP",
    "short_pump_active_1R": "SHORT_PUMP ACTIVE",
    "short_pump_filtered_1R": "SHORT_PUMP FILTERED",
    "short_pump_premium_1R": "SHORT_PUMP PREM",
    "short_pump_wick_1R": "SHORT_PUMP WICK",
    "fast0_selective": "FAST0 SELECTIVE",
    "fast0_base_1R": "FAST0 BASE",
    "fast0_1p5R": "FAST0 1.5R",
    "fast0_2R": "FAST0 2R",
}


def _r48(line: str) -> str:
    s = line.replace("\n", " ").strip()
    if len(s) <= 48:
        return s
    return s[:47].rstrip() + "…"


def _fmt_title_date(end_raw: str) -> str:
    s = end_raw.strip()
    if len(s) == 8 and s.isdigit():
        return f"{s[:4]}-{s[4:6]}-{s[6:8]}"
    return s


def _filter_summary_for_gk(gk: str, tg_dist_min: float) -> str:
    d = f"{tg_dist_min}" if tg_dist_min == int(tg_dist_min) else f"{tg_dist_min}"
    md = f"{SHORT_PUMP_FILTERED_DIST_TO_PEAK_MAX}"
    if gk == "short_pump_mid":
        return "dist [3.5,5)% cs [0.4,0.6)"
    if gk == "short_pump_deep":
        return "st3 dist [7.5,10)% liq0"
    if gk == "short_pump_active_1R":
        return f"st4 dist≥{d}%"
    if gk == "short_pump_filtered_1R":
        return f"dist≤{md}% 1R"
    if gk == "short_pump_premium_1R":
        return "funding×delta (premium)"
    if gk == "short_pump_wick_1R":
        return "delta×wick_body"
    if gk == "fast0_selective":
        return f"dist 0.8–{FAST0_OP_DIST_MAX}% cs mid"
    if gk == "fast0_base_1R":
        return f"dist 0.8–{FAST0_OP_DIST_MAX}% liq0"
    if gk == "fast0_1p5R":
        return "fast0 1.5R liq (5k,25k]"
    if gk == "fast0_2R":
        return "fast0 2R liq>100k"
    return gk


def _gst_bucket(gst: Optional[str]) -> str:
    if not gst:
        return "UNK"
    u = gst.strip().upper()
    if u == "RECOVERY":
        return "RECOV"
    return u


def _render_autotrading_lifecycle_report(
    *,
    report_window_days: int,
    date_range: Tuple[str, str],
    tg_dist_min: float,
    guard_state: Optional[Dict[str, Any]],
    strategies_for_live: set[str],
    mode_n_core: Dict[str, int],
    mode_ev20: Dict[str, float],
    mode_wr: Dict[str, float],
    mode_mdd: Dict[str, float],
    mode_ev_total: Dict[str, float],
    mode_guard: Dict[str, Optional[str]],
    mode_pnl_r: Dict[str, float],
    df_all_outcomes: pd.DataFrame,
    df_fast0_paper: Optional[pd.DataFrame],
    ml_cores: Dict[str, int],
) -> str:
    """Lifecycle TG report: monospace block, no SYSTEM STATE / EDGE / eligibility table."""
    from analytics.stats import LIGHTGBM_COMFORT, LIGHTGBM_MIN

    _, end = date_range
    lines: List[str] = []

    lines.append(_r48(f"📊 АВТОТОРГОВЛЯ · {report_window_days}д · {_fmt_title_date(end)}"))
    lines.append("")
    lines.append("⚡ ACTION REQUIRED")
    actions: List[str] = []
    for gk, short_name in _LIFECYCLE_GK_ORDER:
        label = _GK_METRIC_LABEL.get(gk, gk)
        gst = _guard_state_str(guard_state, gk)
        n_c = int(mode_n_core.get(gk, 0))
        ev20 = float(mode_ev20.get(label, 0.0))
        ev_t = float(mode_ev_total.get(gk, mode_ev_total.get(label, 0.0)))
        wr = float(mode_wr.get(label, 0.0))
        mdd = float(abs(mode_mdd.get(label, 0.0)))
        health = _health_score_submode(
            ev_total=ev_t,
            ev20=ev20,
            wr_pct=wr,
            n_core=n_c,
            mdd_r=mdd,
            guard_state_str=gst,
        )
        prod = _producer_strategies_for_guard_key(gk)
        strat_ok = bool(prod & strategies_for_live) if prod else False
        if (
            health >= 90
            and gk not in ALLOWED_LIVE_PROFILES_FOR_REPORT
            and strat_ok
            and gst == "ACTIVE"
        ):
            actions.append(_r48(f"{short_name}: готов к live (h{health})"))
        if ev20 < -0.3 and n_c > 0:
            actions.append(_r48(f"{short_name}: deep degr EV20 {ev20:+.2f}R"))
    for gk in ("short_pump_premium_1R", "short_pump_wick_1R"):
        gst = _guard_state_str(guard_state, gk)
        short_name = "SP PRM" if gk == "short_pump_premium_1R" else "SP WCK"
        if gst == "WATCH":
            n_c = int(mode_n_core.get(gk, 0))
            rem = max(0, CORE_DECISION_WINDOW - n_c)
            actions.append(_r48(f"{short_name} WATCH: ждём ~{rem} сделок"))
    actions = actions[:5]
    if actions:
        for a in actions:
            lines.append(_r48(a))
    else:
        lines.append(_r48("нет срочных действий"))
    lines.append("")

    sp_h = f0_h = 0
    if "SHORT_PUMP" in mode_ev20 and "SHORT_PUMP" in mode_wr and "SHORT_PUMP" in mode_mdd:
        n_sp = int(mode_n_core.get("short_pump_active_1R", 0))
        sp_h = _health_score(
            mode_ev20["SHORT_PUMP"],
            mode_wr["SHORT_PUMP"],
            abs(mode_mdd["SHORT_PUMP"]),
            mode_guard.get("SHORT_PUMP"),
            n_sp < EXP_THRESHOLD,
        )
    if "FAST0" in mode_ev20 and "FAST0" in mode_wr and "FAST0" in mode_mdd:
        n_f0 = max(
            mode_n_core.get("fast0_base_1R", 0),
            mode_n_core.get("fast0_1p5R", 0),
            mode_n_core.get("fast0_2R", 0),
        )
        f0_h = _health_score(
            mode_ev20["FAST0"],
            mode_wr["FAST0"],
            abs(mode_mdd["FAST0"]),
            mode_guard.get("FAST0"),
            n_f0 < EXP_THRESHOLD,
        )
    lines.append("🎯 AGGREGATES")
    lines.append(
        _r48(
            f"SHORT_PUMP {sp_h}/100 {_health_emoji(sp_h)}  "
            f"FAST0 {f0_h}/100 {_health_emoji(f0_h)}"
        )
    )
    lines.append("")

    def _mode_metrics(gk: str) -> Tuple[str, Optional[str], int, float, float, float, float, int]:
        label = _GK_METRIC_LABEL.get(gk, gk)
        gst = _guard_state_str(guard_state, gk)
        n_c = int(mode_n_core.get(gk, 0))
        ev20 = float(mode_ev20.get(label, 0.0))
        ev_t = float(mode_ev_total.get(gk, mode_ev_total.get(label, 0.0)))
        wr = float(mode_wr.get(label, 0.0))
        mdd = float(abs(mode_mdd.get(label, 0.0)))
        pnl = float(mode_pnl_r.get(gk, 0.0))
        health = _health_score_submode(
            ev_total=ev_t,
            ev20=ev20,
            wr_pct=wr,
            n_core=n_c,
            mdd_r=mdd,
            guard_state_str=gst,
        )
        return label, gst, n_c, ev20, wr, mdd, pnl, health

    lines.append("🟢 LIVE (on exchange)")
    live_any = False
    for gk, short_name in _LIFECYCLE_GK_ORDER:
        _lb, gst, n_c, ev20, wr, mdd, pnl, _h = _mode_metrics(gk)
        if gst != "ACTIVE" or gk not in ALLOWED_LIVE_PROFILES_FOR_REPORT:
            continue
        live_any = True
        fs = _filter_summary_for_gk(gk, tg_dist_min)
        lines.append(_r48(f"{short_name}  {wr:.0f}%  EV20:{ev20:+.2f}  N={n_c}  🟢 LIVE"))
        lines.append(_r48(f"  filter: {fs}"))
        lines.append(_r48(f"  pnl: {pnl:+.1f}R · mdd: {mdd:.1f}R"))
        lines.append("")
    if not live_any:
        lines.append(_r48("(нет)"))
        lines.append("")

    lines.append("🟡 READY FOR LIVE")
    ready_any = False
    for gk, short_name in _LIFECYCLE_GK_ORDER:
        _lb, gst, n_c, ev20, wr, mdd, pnl, health = _mode_metrics(gk)
        if gst != "ACTIVE" or gk in ALLOWED_LIVE_PROFILES_FOR_REPORT:
            continue
        ready_any = True
        fs = _filter_summary_for_gk(gk, tg_dist_min)
        lines.append(_r48(f"{short_name}  {wr:.0f}%  EV20:{ev20:+.2f}  N={n_c}  🟡"))
        lines.append(_r48(f"  filter: {fs}"))
        lines.append(_r48(f"  pnl: {pnl:+.1f}R · mdd: {mdd:.1f}R"))
        lines.append(_r48(f"  health {health}/100 · до live: в whitelist"))
        lines.append("")
    if not ready_any:
        lines.append(_r48("(нет)"))
        lines.append("")

    lines.append("🔵 PAPER (WATCH)")
    paper_any = False
    for gk, short_name in _LIFECYCLE_GK_ORDER:
        _lb, gst, n_c, ev20, wr, mdd, pnl, health = _mode_metrics(gk)
        if gst not in ("WATCH", "RECOVERY"):
            continue
        paper_any = True
        fs = _filter_summary_for_gk(gk, tg_dist_min)
        new_badge = ""
        if gk in ("short_pump_premium_1R", "short_pump_wick_1R"):
            new_badge = " NEW" if n_c < 10 else ""
        if n_c == 0 and gk in ("short_pump_premium_1R", "short_pump_wick_1R"):
            lines.append(_r48(f"{short_name}{new_badge}  [{_gst_bucket(gst)}]"))
            lines.append(_r48(f"  filter: {fs}"))
            rem = max(0, CORE_DECISION_WINDOW - n_c)
            lines.append(_r48(f"  до ACTIVE: ~{rem} сделок"))
            lines.append("")
            continue
        lines.append(_r48(f"{short_name}{new_badge}  [{_gst_bucket(gst)}]  N={n_c}"))
        lines.append(_r48(f"  filter: {fs}"))
        lines.append(_r48(f"  WR {wr:.0f}% EV20:{ev20:+.2f} health {health}/100"))
        lines.append("")
    if not paper_any:
        lines.append(_r48("(нет)"))
        lines.append("")

    lines.append("🔴 DISABLED")
    dis_any = False
    for gk, short_name in _LIFECYCLE_GK_ORDER:
        _lb, gst, n_c, ev20, wr, mdd, _pnl, health = _mode_metrics(gk)
        if gst != "DISABLED":
            continue
        dis_any = True
        lines.append(
            _r48(
                f"{short_name}  {wr:.0f}%  EV20:{ev20:+.2f}  N={n_c}  h{health}"
            )
        )
    if not dis_any:
        lines.append(_r48("(нет)"))
    lines.append("")

    lines.append("🤖 ML READINESS")
    rows_ml = [
        ("sp", ml_cores.get("short_pump", 0)),
        ("spf", ml_cores.get("short_pump_filtered", 0)),
        ("f0", ml_cores.get("fast0", 0)),
        ("prm", ml_cores.get("premium", 0)),
        ("wck", ml_cores.get("wick", 0)),
    ]
    for tag, n_core in rows_ml:
        rem = max(0, LIGHTGBM_MIN - n_core)
        pct = min(100, int(n_core * 100 / max(1, LIGHTGBM_MIN)))
        em = _health_emoji(pct)
        tail = f" +{rem}" if rem > 0 else ""
        lines.append(_r48(f"  {tag}  {n_core}/{LIGHTGBM_MIN} {em}{tail}"))
    lines.append(_r48(f"  comfort ~{LIGHTGBM_COMFORT}"))
    lines.append("")

    n_dup, n_other, n_early, n_conf, quality_ok = _dataset_quality_metrics(df_all_outcomes)
    lines.append("🧱 DATA HEALTH")
    lines.append(_r48(f"  dups {n_dup}  other {n_other}  early {n_early}"))
    lines.append(_r48(f"  pair conflicts {n_conf}  {'OK' if quality_ok else 'CHECK'}"))
    for ln in _paper_fast0_validation_lines(df_fast0_paper):
        lines.append(_r48(ln.strip()))
    lines.append("")

    lines.append("ℹ️ Легенда")
    lines.append(
        _r48("WR/EV20/N — метрики core; h — health 0–100; блоки по guard")
    )
    body = "\n".join(lines)
    return f"```\n{body}\n```"


def build_executive_compact_report(
    df_short_pump: Optional[pd.DataFrame],
    df_short_pump_filtered: Optional[pd.DataFrame],
    df_fast0: Optional[pd.DataFrame],
    date_range: Tuple[str, str],
    rolling_n: int = 20,
    tg_dist_min: float = 3.5,
    guard_state: Optional[Dict[str, Any]] = None,
    df_fast0_paper: Optional[pd.DataFrame] = None,
    df_fast0_filtered_paper: Optional[pd.DataFrame] = None,
    df_short_pump_paper: Optional[pd.DataFrame] = None,
    df_short_pump_filtered_paper: Optional[pd.DataFrame] = None,
    df_short_pump_premium: Optional[pd.DataFrame] = None,
    df_short_pump_wick: Optional[pd.DataFrame] = None,
    report_window_days: Optional[int] = None,
) -> str:
    """
    Компактный lifecycle-отчёт для TG (/report): guard-блоки, ML, data health.
    df_short_pump, df_fast0 — обогащённые outcomes (live); premium/wick — отдельные стратегии live.
    """
    start, end = date_range
    days = _calendar_days(date_range)
    strategies_for_live = _parse_strategies_env_for_report()

    # --- подготовка данных ---
    df_sp = df_short_pump
    df_spf = df_short_pump_filtered
    df_f0 = df_fast0
    if df_sp is not None:
        df_sp = df_sp.copy()
    if df_spf is not None:
        df_spf = df_spf.copy()
    if df_f0 is not None:
        df_f0 = df_f0.copy()
    df_pr = df_short_pump_premium.copy() if df_short_pump_premium is not None else None
    df_wk = df_short_pump_wick.copy() if df_short_pump_wick is not None else None

    # ACTIVE для short_pump
    df_active: Optional[pd.DataFrame] = None
    df_active_core: Optional[pd.DataFrame] = None
    if df_sp is not None and not df_sp.empty and "stage" in df_sp.columns and "dist_to_peak_pct" in df_sp.columns:
        df_active, df_active_core, _, _ = filter_active_trades(df_sp, "short_pump", tg_dist_min)
        if df_active is not None and df_active.empty:
            df_active = None
        if df_active_core is not None and df_active_core.empty:
            df_active_core = None

    df_spf_core: Optional[pd.DataFrame] = None
    if df_spf is not None and not df_spf.empty and "outcome" in df_spf.columns:
        out_spf = df_spf["outcome"].apply(_normalize_outcome_raw)
        df_spf_core = df_spf[out_spf.isin(["TP_hit", "SL_hit"])].copy()
        if df_spf_core.empty:
            df_spf_core = None

    # FAST0 operational
    df_f0_op: Optional[pd.DataFrame] = None
    df_f0_op_core: Optional[pd.DataFrame] = None
    if df_f0 is not None and not df_f0.empty:
        op_mask = _fast0_operational_mask(df_f0)
        df_f0_op = df_f0[op_mask].copy()
        core_f0 = _core_mask(df_f0_op["outcome"])
        df_f0_op_core = df_f0_op[core_f0].copy()

    df_pr_core: Optional[pd.DataFrame] = None
    df_wk_core: Optional[pd.DataFrame] = None
    if df_pr is not None and not df_pr.empty and "outcome" in df_pr.columns:
        oc_pr = df_pr["outcome"].apply(_normalize_outcome_raw)
        df_pr_core = df_pr[oc_pr.isin(["TP_hit", "SL_hit"])].copy()
        if df_pr_core.empty:
            df_pr_core = None
    if df_wk is not None and not df_wk.empty and "outcome" in df_wk.columns:
        oc_wk = df_wk["outcome"].apply(_normalize_outcome_raw)
        df_wk_core = df_wk[oc_wk.isin(["TP_hit", "SL_hit"])].copy()
        if df_wk_core.empty:
            df_wk_core = None

    # Предварительный подсчёт n_core и метрик по режимам
    mode_n_core: Dict[str, int] = {}
    mode_ev20: Dict[str, float] = {}
    mode_wr: Dict[str, float] = {}
    mode_mdd: Dict[str, float] = {}
    mode_ev_total: Dict[str, float] = {}
    mode_guard: Dict[str, Optional[str]] = {}
    mode_pnl_r: Dict[str, float] = {}

    if df_active_core is not None and not df_active_core.empty:
        mode_n_core["short_pump_active_1R"] = len(df_active_core)
        tp_ac = int((df_active_core["outcome"].apply(_normalize_outcome_raw) == "TP_hit").sum())
        sl_ac = int((df_active_core["outcome"].apply(_normalize_outcome_raw) == "SL_hit").sum())
        pnl_ac = _core_pnl_series(df_active_core)
        ev_ac = ev_core_from_tp_sl_pnl(tp_ac, sl_ac, pnl_ac)
        _, ev20_ac, _ = rolling_wr_ev_core(df_active_core, rolling_n)
        wr_ac = wr_core_from_tp_sl(tp_ac, sl_ac) * 100
        mdd_ac = _mdd(_pnl_series(df_active)) if df_active is not None else 0.0
        mode_ev_total["short_pump_active_1R"] = float(ev_ac or 0.0)
        mode_ev20["SHORT_PUMP"] = ev20_ac
        mode_wr["SHORT_PUMP"] = wr_ac
        mode_mdd["SHORT_PUMP"] = mdd_ac
        mode_guard["SHORT_PUMP"] = _guard_state_str(guard_state, "short_pump_active_1R")

        # Submode label entry for ACTIVE
        mode_ev20["SHORT_PUMP ACTIVE"] = float(ev20_ac or 0.0)
        mode_wr["SHORT_PUMP ACTIVE"] = float(wr_ac or 0.0)
        mode_mdd["SHORT_PUMP ACTIVE"] = float(mdd_ac or 0.0)
        mode_ev_total["SHORT_PUMP ACTIVE"] = float(ev_ac or 0.0)
        mode_guard["SHORT_PUMP ACTIVE"] = _guard_state_str(guard_state, "short_pump_active_1R")
        mode_pnl_r["short_pump_active_1R"] = float(pnl_ac.sum())
    if df_spf_core is not None and not df_spf_core.empty:
        mode_n_core["short_pump_filtered_1R"] = len(df_spf_core)
        tp_spf = int((df_spf_core["outcome"].apply(_normalize_outcome_raw) == "TP_hit").sum())
        sl_spf = int((df_spf_core["outcome"].apply(_normalize_outcome_raw) == "SL_hit").sum())
        pnl_spf = _core_pnl_series(df_spf_core)
        ev_spf = ev_core_from_tp_sl_pnl(tp_spf, sl_spf, pnl_spf)
        _, ev20_spf, _ = rolling_wr_ev_core(df_spf_core, rolling_n)
        wr_spf = wr_core_from_tp_sl(tp_spf, sl_spf) * 100
        mdd_spf = _mdd(_pnl_series(df_spf)) if df_spf is not None else 0.0
        mode_ev_total["short_pump_filtered_1R"] = float(ev_spf or 0.0)
        mode_ev20["SHORT_PUMP FILTERED"] = float(ev20_spf or 0.0)
        mode_wr["SHORT_PUMP FILTERED"] = float(wr_spf or 0.0)
        mode_mdd["SHORT_PUMP FILTERED"] = float(mdd_spf or 0.0)
        mode_guard["SHORT_PUMP FILTERED"] = _guard_state_str(guard_state, "short_pump_filtered_1R")
        mode_pnl_r["short_pump_filtered_1R"] = float(pnl_spf.sum())
    # SHORT_PUMP MID/DEEP counts (prefer risk_profile)
    if df_short_pump is not None and not df_short_pump.empty and "outcome" in df_short_pump.columns:
        out_sp = df_short_pump["outcome"].apply(_normalize_outcome_raw)
        df_sp_core_pre = df_short_pump[out_sp.isin(["TP_hit", "SL_hit"])].copy()
        for gk, label in (("short_pump_mid", "SHORT_PUMP MID"), ("short_pump_deep", "SHORT_PUMP DEEP")):
            sub = df_sp_core_pre[_short_pump_mode_mask(df_sp_core_pre, mode_name=gk)]
            mode_n_core[gk] = int(len(sub))
            mode_guard[label] = _guard_state_str(guard_state, gk)
            if not sub.empty:
                tp_s = int((sub["outcome"].apply(_normalize_outcome_raw) == "TP_hit").sum())
                sl_s = int((sub["outcome"].apply(_normalize_outcome_raw) == "SL_hit").sum())
                pnl_s = _core_pnl_series(sub)
                ev_s = ev_core_from_tp_sl_pnl(tp_s, sl_s, pnl_s)
                _, ev20_s, _ = rolling_wr_ev_core(sub, rolling_n)
                wr_s = wr_core_from_tp_sl(tp_s, sl_s) * 100
                sub_all = df_short_pump[_short_pump_mode_mask(df_short_pump, mode_name=gk)]
                mdd_s = _mdd(_pnl_series(sub_all))
                mode_ev_total[gk] = float(ev_s or 0.0)
                mode_ev20[label] = float(ev20_s or 0.0)
                mode_wr[label] = float(wr_s or 0.0)
                mode_mdd[label] = float(mdd_s or 0.0)
                mode_pnl_r[gk] = float(pnl_s.sum())
            else:
                mode_ev_total[gk] = 0.0
                mode_ev20[label] = 0.0
                mode_wr[label] = 0.0
                mode_mdd[label] = 0.0
                mode_pnl_r[gk] = 0.0

    if df_f0 is not None and not df_f0.empty:
        outcome_f0_pre = df_f0["outcome"].apply(_normalize_outcome_raw)
        df_f0_core_pre = df_f0[outcome_f0_pre.isin(["TP_hit", "SL_hit"])]
        for gk, mask_fn, label in [
            ("fast0_selective", _fast0_mode_mask_selective, "FAST0 SELECTIVE"),
            ("fast0_base_1R", _fast0_mode_mask_base_1r, "FAST0 BASE"),
            ("fast0_1p5R", _fast0_mode_mask_1p5r, "FAST0 1.5R"),
            ("fast0_2R", _fast0_mode_mask_2r, "FAST0 2R"),
        ]:
            sub = df_f0_core_pre[mask_fn(df_f0_core_pre)]
            mode_n_core[gk] = len(sub)
            mode_guard[label] = _guard_state_str(guard_state, gk)
            if not sub.empty:
                tp_s = int((sub["outcome"].apply(_normalize_outcome_raw) == "TP_hit").sum())
                sl_s = int((sub["outcome"].apply(_normalize_outcome_raw) == "SL_hit").sum())
                pnl_s = _core_pnl_series(sub)
                ev_s = ev_core_from_tp_sl_pnl(tp_s, sl_s, pnl_s)
                _, ev20_s, _ = rolling_wr_ev_core(sub, rolling_n)
                wr_s = wr_core_from_tp_sl(tp_s, sl_s) * 100
                sub_all = df_f0[mask_fn(df_f0)]
                mdd_s = _mdd(_pnl_series(sub_all))
                mode_ev_total[gk] = float(ev_s or 0.0)
                mode_ev20[label] = ev20_s
                mode_wr[label] = wr_s
                mode_mdd[label] = mdd_s
                mode_pnl_r[gk] = float(pnl_s.sum())
            else:
                mode_ev_total[gk] = 0.0
                mode_ev20[label] = 0.0
                mode_wr[label] = 0.0
                mode_mdd[label] = 0.0
                mode_pnl_r[gk] = 0.0

    if df_f0_op_core is not None and not df_f0_op_core.empty:
        tp_fc = int((df_f0_op_core["outcome"].apply(_normalize_outcome_raw) == "TP_hit").sum())
        sl_fc = int((df_f0_op_core["outcome"].apply(_normalize_outcome_raw) == "SL_hit").sum())
        pnl_fc = _core_pnl_series(df_f0_op_core)
        _, ev20_fc, _ = rolling_wr_ev_core(df_f0_op_core, rolling_n)
        wr_fc = wr_core_from_tp_sl(tp_fc, sl_fc) * 100
        mdd_fc = _mdd(_pnl_series(df_f0_op)) if df_f0_op is not None else 0.0
        mode_ev20["FAST0"] = ev20_fc
        mode_wr["FAST0"] = wr_fc
        mode_mdd["FAST0"] = mdd_fc
        gst_fc = "DISABLED" if all(
            _guard_state_str(guard_state, gk) == "DISABLED"
            for gk in ("fast0_base_1R", "fast0_1p5R", "fast0_2R")
        ) else ("WATCH" if any(_guard_state_str(guard_state, gk) in ("WATCH", "RECOVERY") for gk in ("fast0_base_1R", "fast0_1p5R", "fast0_2R")) else "ACTIVE")
        mode_guard["FAST0"] = gst_fc
    elif "FAST0 BASE" in mode_ev20 or "FAST0 1.5R" in mode_ev20 or "FAST0 2R" in mode_ev20:
        nz = [l for l in ("FAST0 BASE", "FAST0 1.5R", "FAST0 2R") if l in mode_ev20]
        if nz:
            mode_ev20["FAST0"] = sum(mode_ev20[l] for l in nz) / len(nz)
            mode_wr["FAST0"] = sum(mode_wr.get(l, 0) for l in nz) / len(nz)
            mode_mdd["FAST0"] = sum(mode_mdd.get(l, 0) for l in nz) / len(nz)
            mode_guard["FAST0"] = "DISABLED" if all(mode_guard.get(l) == "DISABLED" for l in nz) else ("WATCH" if any(mode_guard.get(l) in ("WATCH", "RECOVERY") for l in nz) else "ACTIVE")

    # short_pump_premium / short_pump_wick (single risk_profile each)
    if df_pr is not None and not df_pr.empty and "outcome" in df_pr.columns:
        gk_pr = "short_pump_premium_1R"
        label_pr = "SHORT_PUMP PREM"
        out_pr = df_pr["outcome"].apply(_normalize_outcome_raw)
        sub_pr = df_pr[out_pr.isin(["TP_hit", "SL_hit"])].copy()
        mode_n_core[gk_pr] = len(sub_pr)
        mode_guard[label_pr] = _guard_state_str(guard_state, gk_pr)
        if not sub_pr.empty:
            tp_s = int((sub_pr["outcome"].apply(_normalize_outcome_raw) == "TP_hit").sum())
            sl_s = int((sub_pr["outcome"].apply(_normalize_outcome_raw) == "SL_hit").sum())
            pnl_s = _core_pnl_series(sub_pr)
            ev_s = ev_core_from_tp_sl_pnl(tp_s, sl_s, pnl_s)
            _, ev20_s, _ = rolling_wr_ev_core(sub_pr, rolling_n)
            wr_s = wr_core_from_tp_sl(tp_s, sl_s) * 100
            mdd_s = _mdd(_pnl_series(df_pr))
            mode_ev_total[gk_pr] = float(ev_s or 0.0)
            mode_ev20[label_pr] = float(ev20_s or 0.0)
            mode_wr[label_pr] = float(wr_s or 0.0)
            mode_mdd[label_pr] = float(mdd_s or 0.0)
            mode_pnl_r[gk_pr] = float(pnl_s.sum())
        else:
            mode_ev_total[gk_pr] = 0.0
            mode_ev20[label_pr] = 0.0
            mode_wr[label_pr] = 0.0
            mode_mdd[label_pr] = 0.0
            mode_pnl_r[gk_pr] = 0.0
    if df_wk is not None and not df_wk.empty and "outcome" in df_wk.columns:
        gk_wk = "short_pump_wick_1R"
        label_wk = "SHORT_PUMP WICK"
        out_wk = df_wk["outcome"].apply(_normalize_outcome_raw)
        sub_wk = df_wk[out_wk.isin(["TP_hit", "SL_hit"])].copy()
        mode_n_core[gk_wk] = len(sub_wk)
        mode_guard[label_wk] = _guard_state_str(guard_state, gk_wk)
        if not sub_wk.empty:
            tp_s = int((sub_wk["outcome"].apply(_normalize_outcome_raw) == "TP_hit").sum())
            sl_s = int((sub_wk["outcome"].apply(_normalize_outcome_raw) == "SL_hit").sum())
            pnl_s = _core_pnl_series(sub_wk)
            ev_s = ev_core_from_tp_sl_pnl(tp_s, sl_s, pnl_s)
            _, ev20_s, _ = rolling_wr_ev_core(sub_wk, rolling_n)
            wr_s = wr_core_from_tp_sl(tp_s, sl_s) * 100
            mdd_s = _mdd(_pnl_series(df_wk))
            mode_ev_total[gk_wk] = float(ev_s or 0.0)
            mode_ev20[label_wk] = float(ev20_s or 0.0)
            mode_wr[label_wk] = float(wr_s or 0.0)
            mode_mdd[label_wk] = float(mdd_s or 0.0)
            mode_pnl_r[gk_wk] = float(pnl_s.sum())
        else:
            mode_ev_total[gk_wk] = 0.0
            mode_ev20[label_wk] = 0.0
            mode_wr[label_wk] = 0.0
            mode_mdd[label_wk] = 0.0
            mode_pnl_r[gk_wk] = 0.0

    parts_out = []
    for d in (df_sp, df_spf, df_f0, df_pr, df_wk):
        if d is not None and not d.empty:
            parts_out.append(d)
    df_all_outcomes = pd.concat(parts_out, ignore_index=True) if parts_out else pd.DataFrame()
    ml_cores = {
        "short_pump": len(df_active_core) if df_active_core is not None and not df_active_core.empty else 0,
        "short_pump_filtered": len(df_spf_core) if df_spf_core is not None and not df_spf_core.empty else 0,
        "fast0": len(df_f0_op_core) if df_f0_op_core is not None and not df_f0_op_core.empty else 0,
        "premium": len(df_pr_core) if df_pr_core is not None and not df_pr_core.empty else 0,
        "wick": len(df_wk_core) if df_wk_core is not None and not df_wk_core.empty else 0,
    }
    rw = report_window_days if report_window_days is not None else days
    return _render_autotrading_lifecycle_report(
        report_window_days=rw,
        date_range=date_range,
        tg_dist_min=tg_dist_min,
        guard_state=guard_state,
        strategies_for_live=strategies_for_live,
        mode_n_core=mode_n_core,
        mode_ev20=mode_ev20,
        mode_wr=mode_wr,
        mode_mdd=mode_mdd,
        mode_ev_total=mode_ev_total,
        mode_guard=mode_guard,
        mode_pnl_r=mode_pnl_r,
        df_all_outcomes=df_all_outcomes,
        df_fast0_paper=df_fast0_paper,
        ml_cores=ml_cores,
    )

