"""
Executive compact report v3 for Telegram — операционный формат.
Блоки: SYSTEM STATE, 1) Стратегии, 2) LIVE подрежимы, 3) PAPER подрежимы,
4) Мониторинг датасетов, 5) Готовность ML, 6) Легенда.
"""

from __future__ import annotations

from typing import Any, Dict, Optional, Tuple

import os
import pandas as pd

CORE_DECISION_WINDOW = 20  # мин. новых core сделок до решения по режиму при EV(20)<0

from .fast0_blocks import FAST0_OP_DIST_MAX, FAST0_OP_LIQ_5K, FAST0_OP_LIQ_25K, FAST0_OP_LIQ_100K
from .short_pump_blocks import filter_active_trades
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
    Health score for a submode (risk_profile):
    - incorporate EV_total and EV20 (EV20 has higher weight)
    - include WR and N (penalize low sample size)
    """
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
    return max(0, min(100, 50 + ev20_c + ev_c + wr_c + n_c + mdd_c + g_c))


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
    """Prefer risk_profile when present; fallback to context_score window."""
    rp = df["risk_profile"].astype(str).str.strip() if "risk_profile" in df.columns else pd.Series([""] * len(df), index=df.index)
    has_rp = rp.notna() & (rp != "") & (rp != "nan")
    by_profile = has_rp & (rp.str.lower() == "fast0_selective")
    if "context_score" in df.columns:
        cs = pd.to_numeric(df["context_score"], errors="coerce")
        fb = (~has_rp) & cs.notna() & (cs >= 0.4) & (cs < 0.6)
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


def _filter_lines_short_pump_mid() -> list[str]:
    return [
        "strategy = short_pump",
        "dist ∈ [3.5, 5)%",
        "context_score ∈ [0.40, 0.60)",
    ]


def _filter_lines_short_pump_deep() -> list[str]:
    return [
        "strategy = short_pump",
        "stage = 3",
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
        "context_score ≥ 0.40",
        "liqL30s = 0",
    ]


def _filter_lines_fast0_selective() -> list[str]:
    return [
        f"dist ≈ 0.8–{FAST0_OP_DIST_MAX}%",
        "context_score ∈ [0.40, 0.60)",
        "(опц.) volume_1m > 1,000,000 если доступно",
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


def build_executive_compact_report(
    df_short_pump: Optional[pd.DataFrame],
    df_fast0: Optional[pd.DataFrame],
    date_range: Tuple[str, str],
    rolling_n: int = 20,
    tg_dist_min: float = 3.5,
    guard_state: Optional[Dict[str, Any]] = None,
    df_fast0_paper: Optional[pd.DataFrame] = None,
    df_short_pump_paper: Optional[pd.DataFrame] = None,
) -> str:
    """
    Строит компактный управленческий отчёт (5 блоков) для TG.
    df_short_pump, df_fast0 — уже обогащённые (stage, dist_to_peak_pct, liq) outcomes (live).
    df_fast0_paper, df_short_pump_paper — outcomes mode=paper для блока «PAPER ПОДРЕЖИМЫ».
    """
    lines: list[str] = []
    start, end = date_range
    days = _calendar_days(date_range)

    # --- подготовка данных ---
    df_sp = df_short_pump
    df_f0 = df_fast0
    if df_sp is not None:
        df_sp = df_sp.copy()
    if df_f0 is not None:
        df_f0 = df_f0.copy()

    # ACTIVE для short_pump
    df_active: Optional[pd.DataFrame] = None
    df_active_core: Optional[pd.DataFrame] = None
    if df_sp is not None and not df_sp.empty and "stage" in df_sp.columns and "dist_to_peak_pct" in df_sp.columns:
        df_active, df_active_core, _, _ = filter_active_trades(df_sp, "short_pump", tg_dist_min)
        if df_active is not None and df_active.empty:
            df_active = None
        if df_active_core is not None and df_active_core.empty:
            df_active_core = None

    # FAST0 operational
    df_f0_op: Optional[pd.DataFrame] = None
    df_f0_op_core: Optional[pd.DataFrame] = None
    if df_f0 is not None and not df_f0.empty:
        op_mask = _fast0_operational_mask(df_f0)
        df_f0_op = df_f0[op_mask].copy()
        core_f0 = _core_mask(df_f0_op["outcome"])
        df_f0_op_core = df_f0_op[core_f0].copy()

    # Объединённая серия для system metrics (ACTIVE + FAST0 operational)
    pnl_system = []
    df_system_core = None
    if df_active_core is not None and not df_active_core.empty:
        pnl_active = _pnl_series(df_active_core)
        if not pnl_active.empty:
            pnl_system.append(pnl_active)
        df_system_core = df_active_core.copy()
    if df_f0_op_core is not None and not df_f0_op_core.empty:
        pnl_f0 = _pnl_series(df_f0_op_core)
        if not pnl_f0.empty:
            pnl_system.append(pnl_f0)
        if df_system_core is not None:
            df_system_core = pd.concat([df_system_core, df_f0_op_core], ignore_index=True)
        else:
            df_system_core = df_f0_op_core.copy()

    pnl_total = pd.concat(pnl_system, ignore_index=True) if pnl_system else pd.Series(dtype=float)

    total_pnl_r = float(pnl_total.sum()) if not pnl_total.empty else 0.0
    total_trades = len(pnl_total)
    trades_per_day = total_trades / days if days else 0.0
    mdd_system = _mdd(pnl_total)

    r_day_fast0 = 0.0
    r_day_active = 0.0
    if df_f0_op_core is not None and not df_f0_op_core.empty:
        r_day_fast0 = float(_core_pnl_series(df_f0_op_core).sum()) / days
    if df_active_core is not None and not df_active_core.empty:
        r_day_active = float(_core_pnl_series(df_active_core).sum()) / days
    r_day_total = total_pnl_r / days if days else 0.0

    # Rolling WR/EV по core subset (последние N core сделок по объединённой системе)
    wr20, ev20 = 0.0, 0.0
    if df_system_core is not None and not df_system_core.empty:
        wr20_frac, ev20_val, _ = rolling_wr_ev_core(df_system_core, rolling_n)
        wr20 = wr20_frac * 100.0
        ev20 = ev20_val

    # EV total (core) для метрики стабильности edge
    edge_stability = 0.0
    ev_total = 0.0
    if df_system_core is not None and not df_system_core.empty:
        norm_sys = df_system_core["outcome"].apply(_normalize_outcome_raw)
        tp_sys = int((norm_sys == "TP_hit").sum())
        sl_sys = int((norm_sys == "SL_hit").sum())
        pnl_sys_core = _core_pnl_series(df_system_core)
        ev_total = ev_core_from_tp_sl_pnl(tp_sys, sl_sys, pnl_sys_core)
        if abs(ev_total) > 1e-8:
            edge_stability = float(ev20 / ev_total)

    status = "🟢 прибыльная" if total_pnl_r > 0 else ("🟡 около нуля" if total_pnl_r >= -1.0 else "🔴 убыточная")

    # Храним метрики для v2
    ev_ac: Optional[float] = None
    ev20_ac: Optional[float] = None
    wr_ac, n_ac, pnl_ac_sum, mdd_ac = 0.0, 0, 0.0, 0.0
    ev_fc: Optional[float] = None
    ev20_fc: Optional[float] = None
    wr_fc, n_fc, pnl_fc_sum, mdd_fc = 0.0, 0, 0.0, 0.0

    # Предварительный подсчёт n_core и метрик по режимам
    mode_n_core: Dict[str, int] = {}
    mode_ev20: Dict[str, float] = {}
    mode_wr: Dict[str, float] = {}
    mode_mdd: Dict[str, float] = {}
    mode_ev_total: Dict[str, float] = {}
    mode_guard: Dict[str, Optional[str]] = {}

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
            else:
                mode_ev_total[gk] = 0.0
                mode_ev20[label] = 0.0
                mode_wr[label] = 0.0
                mode_mdd[label] = 0.0

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
            else:
                mode_ev_total[gk] = 0.0
                mode_ev20[label] = 0.0
                mode_wr[label] = 0.0
                mode_mdd[label] = 0.0

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

    # --- v2: ЗАГОЛОВОК + SYSTEM STATE ---
    lines.append("📊 АВТОТОРГОВЛЯ — КОМПАКТ ОТЧЕТ")
    lines.append("")
    lines.append("🧠 SYSTEM STATE")
    active_c, watch_c, disabled_c, exper_c = 0, 0, 0, 0
    for gkey in (
        "short_pump_active_1R",
        "short_pump_mid",
        "short_pump_deep",
        "fast0_base_1R",
        "fast0_1p5R",
        "fast0_2R",
        "fast0_selective",
    ):
        st = _guard_state_str(guard_state, gkey)
        n = mode_n_core.get(gkey, 0)
        if n < EXP_THRESHOLD:
            exper_c += 1
        if st == "ACTIVE":
            active_c += 1
        elif st == "WATCH":
            watch_c += 1
        elif st == "DISABLED":
            disabled_c += 1
        elif st == "RECOVERY":
            watch_c += 1
    lines.append(f"ACTIVE: {active_c}")
    lines.append(f"WATCH: {watch_c}")
    lines.append(f"DISABLED: {disabled_c}")
    lines.append(f"EXPERIMENT: {exper_c}")
    lines.append("")
    lines.append("🚨 EDGE ALERTS")
    # Primary: submodes (risk_profile)
    for label in (
        "SHORT_PUMP MID",
        "SHORT_PUMP DEEP",
        "SHORT_PUMP ACTIVE",
        "FAST0 SELECTIVE",
        "FAST0 BASE",
        "FAST0 1.5R",
        "FAST0 2R",
    ):
        if label in mode_ev20:
            ev20_val = mode_ev20[label]
            lines.append(f"{label}: EV20 {ev20_val:+.2f}R {_ev20_signal_emoji(ev20_val)}")
    # Secondary: aggregates
    for label in ("SHORT_PUMP", "FAST0"):
        if label in mode_ev20:
            ev20_val = mode_ev20[label]
            lines.append(f"{label} (agg): EV20 {ev20_val:+.2f}R {_ev20_signal_emoji(ev20_val)}")
    lines.append("")
    lines.append("🏥 STRATEGY HEALTH")
    # Primary: submodes
    for label, gk in (
        ("SHORT_PUMP MID", "short_pump_mid"),
        ("SHORT_PUMP DEEP", "short_pump_deep"),
        ("SHORT_PUMP ACTIVE", "short_pump_active_1R"),
        ("FAST0 SELECTIVE", "fast0_selective"),
        ("FAST0 BASE", "fast0_base_1R"),
        ("FAST0 1.5R", "fast0_1p5R"),
        ("FAST0 2R", "fast0_2R"),
    ):
        n_s = int(mode_n_core.get(gk, 0))
        ev20_s = float(mode_ev20.get(label, 0.0))
        wr_s = float(mode_wr.get(label, 0.0))
        mdd_s = float(abs(mode_mdd.get(label, 0.0)))
        ev_s = float(mode_ev_total.get(gk, mode_ev_total.get(label, 0.0)))
        gst = mode_guard.get(label) or _guard_state_str(guard_state, gk)
        health = _health_score_submode(
            ev_total=ev_s,
            ev20=ev20_s,
            wr_pct=wr_s,
            n_core=n_s,
            mdd_r=mdd_s,
            guard_state_str=gst,
        )
        lines.append(f"{label}: {health} / 100 {_health_emoji(health)} (N={n_s})")

    # Secondary: aggregates (keep existing)
    for strat in ("SHORT_PUMP", "FAST0"):
        if strat in mode_ev20 and strat in mode_wr and strat in mode_mdd:
            if strat == "SHORT_PUMP":
                n_s = mode_n_core.get("short_pump_active_1R", 0)
            else:
                n_s = max(
                    mode_n_core.get("fast0_base_1R", 0),
                    mode_n_core.get("fast0_1p5R", 0),
                    mode_n_core.get("fast0_2R", 0),
                )
            is_exp = n_s < EXP_THRESHOLD
            health = _health_score(
                mode_ev20[strat], mode_wr[strat], abs(mode_mdd[strat]),
                mode_guard.get(strat), is_exp,
            )
            lines.append(f"{strat} (agg): {health} / 100 {_health_emoji(health)}")
    lines.append("")
    lines.append("1️⃣ СТРАТЕГИИ")
    lines.append("")

    # 1.1 SHORT_PUMP
    if df_active_core is not None and not df_active_core.empty:
        tp_ac = int((df_active_core["outcome"].apply(_normalize_outcome_raw) == "TP_hit").sum())
        sl_ac = int((df_active_core["outcome"].apply(_normalize_outcome_raw) == "SL_hit").sum())
        pnl_ac = _core_pnl_series(df_active_core)
        ev_ac = ev_core_from_tp_sl_pnl(tp_ac, sl_ac, pnl_ac)
        _, ev20_ac, _ = rolling_wr_ev_core(df_active_core, rolling_n)
        n_ac = len(df_active_core)
        wr_ac = wr_core_from_tp_sl(tp_ac, sl_ac) * 100
        pnl_ac_sum = float(pnl_ac.sum())
        mdd_ac = _mdd(_pnl_series(df_active)) if df_active is not None else 0.0
        lines.append("1.1 SHORT_PUMP")
        lines.append(f"    WR: {wr_ac:.1f}%   EV: {ev_ac:+.2f}R   EV20: {ev20_ac:+.2f}R")
        lines.append(f"    N: {n_ac}      PnL: {pnl_ac_sum:+.1f}R  MDD: {mdd_ac:.1f}R")
    else:
        lines.append("1.1 SHORT_PUMP")
        lines.append("    нет данных")
    lines.append("")

    # 1.2 FAST0
    if df_f0_op_core is not None and not df_f0_op_core.empty:
        tp_fc = int((df_f0_op_core["outcome"].apply(_normalize_outcome_raw) == "TP_hit").sum())
        sl_fc = int((df_f0_op_core["outcome"].apply(_normalize_outcome_raw) == "SL_hit").sum())
        pnl_fc = _core_pnl_series(df_f0_op_core)
        ev_fc = ev_core_from_tp_sl_pnl(tp_fc, sl_fc, pnl_fc)
        _, ev20_fc, _ = rolling_wr_ev_core(df_f0_op_core, rolling_n)
        n_fc = len(df_f0_op_core)
        wr_fc = wr_core_from_tp_sl(tp_fc, sl_fc) * 100
        pnl_fc_sum = float(pnl_fc.sum())
        mdd_fc = _mdd(_pnl_series(df_f0_op)) if df_f0_op is not None else 0.0
        lines.append("1.2 FAST0")
        lines.append(f"    WR: {wr_fc:.1f}%   EV: {ev_fc:+.2f}R   EV20: {ev20_fc:+.2f}R")
        lines.append(f"    N: {n_fc}       PnL: {pnl_fc_sum:+.1f}R   MDD: {mdd_fc:.1f}R")
    else:
        lines.append("1.2 FAST0")
        lines.append("    нет данных")
    lines.append("")
    lines.append("⚡ 2️⃣ LIVE ПОДРЕЖИМЫ СТРАТЕГИЙ")
    lines.append("")

    # 2.1 SHORT_PUMP — только ACTIVE / WATCH / RECOVERY (не DISABLED)
    lines.append("2.1 SHORT_PUMP")
    lines.append("")
    # Build per-submode subsets (prefer risk_profile)
    df_sp_all = df_short_pump.copy() if df_short_pump is not None else None
    df_sp_core = None
    if df_sp_all is not None and not df_sp_all.empty and "outcome" in df_sp_all.columns:
        out_sp = df_sp_all["outcome"].apply(_normalize_outcome_raw)
        df_sp_core = df_sp_all[out_sp.isin(["TP_hit", "SL_hit"])].copy()

    short_pump_live_modes: list[tuple[str, str, callable, pd.DataFrame | None, pd.DataFrame | None]] = [
        ("SHORT_PUMP MID", "short_pump_mid", _filter_lines_short_pump_mid, df_sp_all, df_sp_core),
        ("SHORT_PUMP DEEP", "short_pump_deep", _filter_lines_short_pump_deep, df_sp_all, df_sp_core),
        ("ACTIVE (stage4 + dist≥" + (f"{int(tg_dist_min)}" if tg_dist_min == int(tg_dist_min) else f"{tg_dist_min}") + "%)", "short_pump_active_1R", lambda: _filter_lines_short_pump_active(tg_dist_min), df_active, df_active_core),
    ]

    live_sp_count = 0
    for sub_label, guard_key, filter_fn, df_all, df_core in short_pump_live_modes:
        gst_sp = _guard_state_str(guard_state, guard_key) if guard_state else None
        if not gst_sp or gst_sp.upper() == "DISABLED":
            continue
        # Build subset for metrics
        sub_core = None
        sub_all = None
        if guard_key in ("short_pump_mid", "short_pump_deep") and df_sp_all is not None and df_sp_core is not None:
            mask_all = _short_pump_mode_mask(df_sp_all, mode_name=guard_key)
            mask_core = _short_pump_mode_mask(df_sp_core, mode_name=guard_key)
            sub_all = df_sp_all[mask_all].copy()
            sub_core = df_sp_core[mask_core].copy()
        else:
            sub_all = df_all
            sub_core = df_core

        live_sp_count += 1
        lines.append(f"    2.1.{live_sp_count} {sub_label}")
        lines.append("")
        lines.append("        🔹 Фильтр")
        for ln in filter_fn():
            lines.append(f"        {ln}")
        lines.append("")
        if sub_core is None or sub_core.empty:
            lines.append("        🔹 Метрики")
            lines.append("        WR: N/A   EV: N/A   EV20: N/A")
            lines.append("        N: 0 | timeout: 0")
            lines.append("")
            lines.append("        🔹 Guard")
            lines.append(f"        {_guard_emoji_label(gst_sp, 0)}")
            prog = _guard_progress_text(guard_state, guard_key)
            if prog:
                if gst_sp == "WATCH":
                    lines.append("        live-входы пока разрешены")
                lines.append(f"        {prog}")
                if gst_sp == "WATCH":
                    lines.append(f"        {_guard_progress_watch_to_active(0, DECISION_WINDOW_VERDICT)}")
            lines.append("")
            lines.append("        🔹 Диагноз")
            lines.append("        bootstrap / нет данных за период")
            lines.append("")
            continue
        tp_sub = int((sub_core["outcome"].apply(_normalize_outcome_raw) == "TP_hit").sum())
        sl_sub = int((sub_core["outcome"].apply(_normalize_outcome_raw) == "SL_hit").sum())
        pnl_sub = _core_pnl_series(sub_core)
        ev_sub = ev_core_from_tp_sl_pnl(tp_sub, sl_sub, pnl_sub)
        _, ev20_sub, _ = rolling_wr_ev_core(sub_core, rolling_n)
        wr_sub = wr_core_from_tp_sl(tp_sub, sl_sub) * 100
        ec_sub = _edge_consistency_frac(pnl_sub, rolling_n) if len(sub_core) >= 20 else None
        trades_neg_sp = _trades_since_ev_negative(pnl_sub, rolling_n) if len(sub_core) >= 20 else 0
        timeout_sub = _timeout_count(sub_all) if sub_all is not None else 0
        guard_txt_sp = _guard_status_text(
            guard_state, guard_key,
            trades_since_negative_start=trades_neg_sp,
            decision_window=DECISION_WINDOW_VERDICT,
        )
        lines.append("        🔹 Метрики")
        lines.append(f"        WR: {wr_sub:.0f}%   EV: {ev_sub:+.2f}R   EV20: {ev20_sub:+.2f}R")
        lines.append(f"        N: {len(sub_core)} | timeout: {timeout_sub}")
        lines.append("")
        lines.append("        🔹 Guard")
        lines.append(f"        {_guard_emoji_label(gst_sp, len(sub_core))}")
        lines.append("")
        lines.append("        🔹 Диагноз")
        diag_sp = _diagnosis_v2(
            ev_sub, ev20_sub, len(sub_core), ec_sub, trades_neg_sp,
            gst_sp, guard_txt_sp, DECISION_WINDOW_VERDICT,
        )
        for d in diag_sp:
            lines.append(f"        {d}")
        prog = _guard_progress_text(
            guard_state, guard_key,
            trades_since_negative_start=trades_neg_sp,
            decision_window=DECISION_WINDOW_VERDICT,
        )
        if prog:
            if gst_sp == "WATCH":
                lines.append("        live-входы пока разрешены")
            lines.append(f"        {prog}")
            if gst_sp == "WATCH":
                lines.append(f"        {_guard_progress_watch_to_active(trades_neg_sp, DECISION_WINDOW_VERDICT)}")
        lines.append("")

    if live_sp_count == 0:
        lines.append("    (нет live-подрежимов)")
        lines.append("")

    # Preserve legacy ACTIVE-only block when data exists but guard missing
    gst_sp_legacy = _guard_state_str(guard_state, "short_pump_active_1R") if guard_state else None
    if df_active_core is not None and not df_active_core.empty and gst_sp_legacy and gst_sp_legacy.upper() not in ("DISABLED",):
        trades_neg_sp = _trades_since_ev_negative(pnl_ac, rolling_n) if n_ac >= 20 else 0
        guard_txt_sp = _guard_status_text(
            guard_state, "short_pump_active_1R",
            trades_since_negative_start=trades_neg_sp,
            decision_window=DECISION_WINDOW_VERDICT,
        )
        ec_ac = _edge_consistency_frac(pnl_ac, rolling_n) if n_ac >= 20 else None
        timeout_ac = _timeout_count(df_active) if df_active is not None else 0
        lines.append("    2.1.X ACTIVE (legacy block)")
        lines.append("")
        lines.append("        🔹 Фильтр")
        for ln in _filter_lines_short_pump_active(tg_dist_min):
            lines.append(f"        {ln}")
        lines.append("")
        lines.append("        🔹 Метрики")
        lines.append(f"        WR: {wr_ac:.0f}%   EV: {ev_ac:+.2f}R   EV20: {ev20_ac:+.2f}R")
        lines.append(f"        N: {n_ac} | timeout: {timeout_ac}")
        lines.append("")
        lines.append("        🔹 Guard")
        lines.append(f"        {_guard_emoji_label(gst_sp_legacy, n_ac)}")
        lines.append("")
        lines.append("        🔹 Диагноз")
        diag_sp = _diagnosis_v2(
            ev_ac, ev20_ac, n_ac, ec_ac, trades_neg_sp,
            gst_sp_legacy, guard_txt_sp, DECISION_WINDOW_VERDICT,
        )
        for d in diag_sp:
            lines.append(f"        {d}")
        prog = _guard_progress_text(
            guard_state, "short_pump_active_1R",
            trades_since_negative_start=trades_neg_sp,
            decision_window=DECISION_WINDOW_VERDICT,
        )
        if prog:
            if gst_sp_legacy == "WATCH":
                lines.append("        live-входы пока разрешены")
            lines.append(f"        {prog}")
            if gst_sp_legacy == "WATCH":
                lines.append(f"        {_guard_progress_watch_to_active(trades_neg_sp, DECISION_WINDOW_VERDICT)}")
    elif not (df_active_core is not None and not df_active_core.empty):
        lines.append("    2.1.1 ACTIVE: нет данных")
    lines.append("")

    # 2.2 FAST0 — только ACTIVE / WATCH / RECOVERY (не DISABLED)
    lines.append("2.2 FAST0")
    lines.append("")
    live_fast0_count = 0
    df_f0_all = df_f0.copy() if df_f0 is not None and not df_f0.empty else pd.DataFrame()
    if not df_f0_all.empty and "outcome" in df_f0_all.columns:
        outcome_f0 = df_f0_all["outcome"].apply(_normalize_outcome_raw)
        df_f0_core = df_f0_all[outcome_f0.isin(["TP_hit", "SL_hit"])].copy()
    else:
        df_f0_core = pd.DataFrame()

    fast0_modes = [
        ("FAST0 SELECTIVE", "fast0_selective", _fast0_mode_mask_selective, _filter_lines_fast0_selective),
        ("FAST0 BASE (liq=0)", "fast0_base_1R", _fast0_mode_mask_base_1r, _filter_lines_fast0_base),
        ("FAST0 1.5R", "fast0_1p5R", _fast0_mode_mask_1p5r, _filter_lines_fast0_1p5r),
        ("FAST0 2R", "fast0_2R", _fast0_mode_mask_2r, _filter_lines_fast0_2r),
    ]
    for sub_label, guard_key, mask_fn, filter_fn in fast0_modes:
        gst = _guard_state_str(guard_state, guard_key)
        if not gst or gst.upper() == "DISABLED":
            continue
        live_fast0_count += 1
        sub_core = df_f0_core[mask_fn(df_f0_core)] if not df_f0_core.empty else pd.DataFrame()
        sub_all = df_f0_all[mask_fn(df_f0_all)] if not df_f0_all.empty else pd.DataFrame()
        n_sub = len(sub_core)
        lines.append(f"    2.2.{live_fast0_count} {sub_label}")
        lines.append("")
        lines.append("        🔹 Фильтр")
        for ln in filter_fn():
            lines.append(f"        {ln}")
        lines.append("")
        if n_sub == 0:
            lines.append("        🔹 Метрики")
            lines.append("        WR: —   EV: —   EV20: —")
            lines.append("        N: 0 | timeout: 0")
            lines.append("")
            lines.append("        🔹 Guard")
            lines.append(f"        {_guard_emoji_label(gst, 0)}")
            prog = _guard_progress_text(guard_state, guard_key)
            if prog:
                if gst == "WATCH":
                    lines.append("        live-входы пока разрешены")
                lines.append(f"        {prog}")
                if gst == "WATCH":
                    lines.append(f"        {_guard_progress_watch_to_active(0, DECISION_WINDOW_VERDICT)}")
            lines.append("")
            lines.append("        🔹 Диагноз")
            lines.append("        bootstrap / нет сделок за период")
        else:
            tp_s = int((sub_core["outcome"].apply(_normalize_outcome_raw) == "TP_hit").sum())
            sl_s = int((sub_core["outcome"].apply(_normalize_outcome_raw) == "SL_hit").sum())
            pnl_s = _core_pnl_series(sub_core)
            ev_sub = ev_core_from_tp_sl_pnl(tp_s, sl_s, pnl_s)
            _, ev20_sub, _ = rolling_wr_ev_core(sub_core, rolling_n)
            wr_sub = wr_core_from_tp_sl(tp_s, sl_s) * 100
            ec_sub = _edge_consistency_frac(pnl_s, rolling_n) if n_sub >= 20 else None
            trades_neg = _trades_since_ev_negative(pnl_s, rolling_n) if n_sub >= 20 else 0
            timeout_sub = _timeout_count(sub_all)
            guard_txt = _guard_status_text(
                guard_state, guard_key,
                trades_since_negative_start=trades_neg,
                decision_window=DECISION_WINDOW_VERDICT,
            )
            lines.append("        🔹 Метрики")
            lines.append(f"        WR: {wr_sub:.0f}%   EV: {ev_sub:+.2f}R   EV20: {ev20_sub:+.2f}R")
            lines.append(f"        N: {n_sub} | timeout: {timeout_sub}")
            lines.append("")
            lines.append("        🔹 Guard")
            lines.append(f"        {_guard_emoji_label(gst, n_sub)}")
            lines.append("")
            lines.append("        🔹 Диагноз")
            diag = _diagnosis_v2(ev_sub, ev20_sub, n_sub, ec_sub, trades_neg, gst, guard_txt, DECISION_WINDOW_VERDICT)
            for d in diag:
                lines.append(f"        {d}")
            prog = _guard_progress_text(
                guard_state, guard_key,
                trades_since_negative_start=trades_neg,
                decision_window=DECISION_WINDOW_VERDICT,
            )
            if prog:
                if gst == "WATCH":
                    lines.append("        live-входы пока разрешены")
                lines.append(f"        {prog}")
                if gst == "WATCH":
                    lines.append(f"        {_guard_progress_watch_to_active(trades_neg, DECISION_WINDOW_VERDICT)}")
        lines.append("")
    if live_fast0_count == 0:
        lines.append("    (нет live-подрежимов)")
    lines.append("")

    # --- 3️⃣ PAPER ПОДРЕЖИМЫ (только DISABLED) ---
    # Source of truth: mode=paper outcomes (df_fast0_paper / df_short_pump_paper).
    # Core for N = TP_hit, SL_hit, TIMEOUT; GUARD_BLOCKED excluded. WR/EV/EV20 from TP+SL only.
    lines.append("🧪 3️⃣ PAPER ПОДРЕЖИМЫ")
    lines.append("")
    lines.append("    (режимы без live торговли)")
    lines.append("")
    disabled_modes: list[tuple[str, str, list[str], float, float, float, int, int]] = []
    # SHORT_PUMP ACTIVE when DISABLED — use paper data when provided
    if guard_state:
        gst_sp = _guard_state_str(guard_state, "short_pump_active_1R")
        if gst_sp == "DISABLED":
            if df_short_pump_paper is not None and not df_short_pump_paper.empty and "stage" in df_short_pump_paper.columns and "dist_to_peak_pct" in df_short_pump_paper.columns:
                df_active_paper, df_active_paper_core, _, _ = filter_active_trades(df_short_pump_paper, "short_pump", tg_dist_min)
                outcome_ap = df_active_paper["outcome"].apply(_normalize_outcome_raw) if not df_active_paper.empty else pd.Series(dtype=object)
                paper_core_ap = outcome_ap.isin(PAPER_CORE_OUTCOMES)
                n_ac = int(paper_core_ap.sum()) if not outcome_ap.empty else 0
                timeout_sp = int((outcome_ap == "TIMEOUT").sum()) if not outcome_ap.empty else 0
                sub_tpsl = df_active_paper_core
                if not sub_tpsl.empty:
                    tp_ac = int((sub_tpsl["outcome"].apply(_normalize_outcome_raw) == "TP_hit").sum())
                    sl_ac = int((sub_tpsl["outcome"].apply(_normalize_outcome_raw) == "SL_hit").sum())
                    pnl_ac = _core_pnl_series(sub_tpsl)
                    wr_ac = wr_core_from_tp_sl(tp_ac, sl_ac) * 100
                    ev_ac = ev_core_from_tp_sl_pnl(tp_ac, sl_ac, pnl_ac)
                    _, ev20_ac, _ = rolling_wr_ev_core(sub_tpsl, rolling_n)
                else:
                    wr_ac, ev_ac, ev20_ac = 0.0, 0.0, 0.0
                disabled_modes.append((
                    "SHORT_PUMP ACTIVE",
                    "short_pump_active_1R",
                    _filter_lines_short_pump_active(tg_dist_min),
                    wr_ac, float(ev_ac or 0), float(ev20_ac or 0), n_ac, timeout_sp,
                ))
            elif df_active_core is not None and not df_active_core.empty:
                timeout_sp = _timeout_count(df_active) if df_active is not None else 0
                disabled_modes.append((
                    "SHORT_PUMP ACTIVE",
                    "short_pump_active_1R",
                    _filter_lines_short_pump_active(tg_dist_min),
                    wr_ac, float(ev_ac or 0), float(ev20_ac or 0), n_ac, timeout_sp,
                ))
    # FAST0 modes when DISABLED — use df_fast0_paper (mode=paper) when provided
    df_f0_for_paper = (df_fast0_paper if df_fast0_paper is not None and not df_fast0_paper.empty else df_f0) if guard_state else None
    if guard_state and df_f0_for_paper is not None and not df_f0_for_paper.empty:
        outcome_f0 = df_f0_for_paper["outcome"].apply(_normalize_outcome_raw)
        paper_core_mask = outcome_f0.isin(PAPER_CORE_OUTCOMES)
        df_f0_paper_core = df_f0_for_paper[paper_core_mask].copy()
        df_f0_tpsl_only = df_f0_for_paper[outcome_f0.isin(["TP_hit", "SL_hit"])].copy()
        for label, guard_key, mask_fn, filter_fn in [
            ("FAST0 SELECTIVE", "fast0_selective", _fast0_mode_mask_selective, _filter_lines_fast0_selective),
            ("FAST0 BASE", "fast0_base_1R", _fast0_mode_mask_base_1r, _filter_lines_fast0_base),
            ("FAST0 1.5R", "fast0_1p5R", _fast0_mode_mask_1p5r, _filter_lines_fast0_1p5r),
            ("FAST0 2R", "fast0_2R", _fast0_mode_mask_2r, _filter_lines_fast0_2r),
        ]:
            gst = _guard_state_str(guard_state, guard_key)
            if gst == "DISABLED":
                sub_paper_core = df_f0_paper_core[mask_fn(df_f0_paper_core)]
                sub_tpsl = df_f0_tpsl_only[mask_fn(df_f0_tpsl_only)]
                n_s = len(sub_paper_core)
                timeout_d = int((sub_paper_core["outcome"].apply(_normalize_outcome_raw) == "TIMEOUT").sum())
                if not sub_tpsl.empty:
                    tp_s = int((sub_tpsl["outcome"].apply(_normalize_outcome_raw) == "TP_hit").sum())
                    sl_s = int((sub_tpsl["outcome"].apply(_normalize_outcome_raw) == "SL_hit").sum())
                    pnl_s = _core_pnl_series(sub_tpsl)
                    ev_s = ev_core_from_tp_sl_pnl(tp_s, sl_s, pnl_s)
                    _, ev20_s, _ = rolling_wr_ev_core(sub_tpsl, rolling_n)
                    wr_s = wr_core_from_tp_sl(tp_s, sl_s) * 100
                else:
                    wr_s, ev_s, ev20_s = 0.0, 0.0, 0.0
                disabled_modes.append((label, guard_key, filter_fn(), wr_s, ev_s, ev20_s, n_s, timeout_d))
    for idx, (label, guard_key, filter_lines, wr_s, ev_s, ev20_s, n_s, timeout_d) in enumerate(disabled_modes, 1):
        lines.append(f"    3.{idx} {label}")
        lines.append("")
        lines.append("        🔹 Фильтр")
        for ln in filter_lines:
            lines.append(f"        {ln}")
        lines.append("")
        lines.append("        🔹 Метрики")
        lines.append(f"        WR: {wr_s:.0f}%   EV: {ev_s:+.2f}R   EV20: {ev20_s:+.2f}R")
        lines.append(f"        N: {n_s} | timeout: {timeout_d}")
        lines.append("")
        lines.append("        🔹 Guard")
        lines.append("        🔴 DISABLED")
        lines.append("")
        lines.append("        🔹 Диагноз")
        lines.append("        торговля остановлена")
        lines.append("        режим в наблюдении")
        prog = _guard_progress_text(guard_state, guard_key)
        if prog:
            lines.append(f"        {prog}")
        lines.append("")
    if not disabled_modes:
        lines.append("    (нет DISABLED режимов)")
    lines.append("")

    # --- 4️⃣ МОНИТОРИНГ ДАТАСЕТОВ ---
    parts = []
    if df_sp is not None and not df_sp.empty:
        parts.append(df_sp)
    if df_f0 is not None and not df_f0.empty:
        parts.append(df_f0)
    df_all_outcomes = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()
    n_dup, n_other, n_early, n_conf, quality_ok = _dataset_quality_metrics(df_all_outcomes)
    lines.append("🧱 4️⃣ МОНИТОРИНГ ДАТАСЕТОВ")
    lines.append("")
    lines.append("    дубли outcome: " + str(n_dup))
    lines.append("    нестандартные исходы: " + str(n_other))
    lines.append(f"    EARLY_EXIT: {n_early}")
    lines.append(f"    конфликты связок: {n_conf}")
    lines.append("    качество данных: OK" if quality_ok else "    качество данных: ⚠ проверить")
    for ln in _paper_fast0_validation_lines(df_fast0_paper):
        lines.append(ln)
    lines.append("")
    lines.append("🤖 5️⃣ ГОТОВНОСТЬ ML")
    lines.append("")
    n_fast0_core = len(df_f0_op_core) if (df_f0_op_core is not None and not df_f0_op_core.empty) else 0
    n_sp_core = len(df_active_core) if (df_active_core is not None and not df_active_core.empty) else 0
    remaining_fast0 = max(0, LIGHTGBM_MIN - n_fast0_core)
    remaining_sp = max(0, LIGHTGBM_MIN - n_sp_core)
    core_per_day_fast0 = (n_fast0_core / days) if days else 0.0
    core_per_day_sp = (n_sp_core / days) if days else 0.0
    eta_fast0_days = int(remaining_fast0 / core_per_day_fast0) if (remaining_fast0 > 0 and core_per_day_fast0 > 0) else 0
    eta_sp_days = int(remaining_sp / core_per_day_sp) if (remaining_sp > 0 and core_per_day_sp > 0) else 0

    lines.append(f"    FAST0 core: {n_fast0_core}")
    lines.append(f"    SHORT_PUMP core: {n_sp_core}")
    lines.append("")
    lines.append(f"    минимум ML: {LIGHTGBM_MIN}")
    lines.append(f"    комфорт: {LIGHTGBM_COMFORT}")
    lines.append("")
    eta_f0 = f"~{eta_fast0_days}д" if remaining_fast0 > 0 and eta_fast0_days > 0 else "готово"
    eta_sp = f"~{eta_sp_days}д" if remaining_sp > 0 and eta_sp_days > 0 else "готово"
    lines.append(f"    FAST0 → осталось {remaining_fast0} ({eta_f0})")
    lines.append(f"    SHORT_PUMP → осталось {remaining_sp} ({eta_sp})")
    lines.append("")
    lines.append("ℹ️ 6️⃣ ЛЕГЕНДА")
    lines.append("")
    lines.append("    WR — winrate")
    lines.append("    EV — expectancy")
    lines.append("    EV20 — edge последних 20 сделок")
    lines.append("    N — сделки")
    lines.append("    timeout — закрытие по таймауту")
    lines.append("")
    lines.append("    🟢 ACTIVE — торгуется")
    lines.append("    🟡 WATCH — наблюдение")
    lines.append("    🔴 DISABLED — торговля остановлена")
    lines.append("    🧪 EXP — эксперимент")

    return "\n".join(lines)
