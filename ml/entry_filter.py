from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


DEFAULT_MODEL_PATH = Path("models/lgbm_entry_filter.txt")
DEFAULT_META_PATH = Path("models/lgbm_entry_filter_meta.json")


@dataclass(frozen=True)
class EntryFilterArtifacts:
    features: List[str]
    impute_median: Dict[str, float]
    model_path: Path


_CACHE: Optional[Tuple[EntryFilterArtifacts, Any]] = None  # (artifacts, lightgbm.Booster)


def _safe_float(x: Any) -> Optional[float]:
    try:
        if x is None or x == "":
            return None
        return float(x)
    except (TypeError, ValueError):
        return None


def _load_artifacts(
    *,
    model_path: Path = DEFAULT_MODEL_PATH,
    meta_path: Path = DEFAULT_META_PATH,
) -> Optional[Tuple[EntryFilterArtifacts, Any]]:
    global _CACHE
    if _CACHE is not None:
        return _CACHE
    if not model_path.is_file() or not meta_path.is_file():
        return None
    try:
        meta = json.loads(meta_path.read_text(encoding="utf-8"))
        features = list(meta.get("features") or [])
        impute = dict(meta.get("impute_median") or {})
        impute_median = {str(k): float(v) for k, v in impute.items()}
        if not features:
            return None
    except Exception:
        return None
    try:
        import lightgbm as lgb  # local import to keep runtime optional
        booster = lgb.Booster(model_file=str(model_path))
    except Exception:
        return None
    arts = EntryFilterArtifacts(features=features, impute_median=impute_median, model_path=model_path)
    _CACHE = (arts, booster)
    return _CACHE


def predict_entry_score(row: Dict[str, Any]) -> float:
    """
    Predict entry quality score in [0,1] from event-like payload dict.
    Safe: returns 0.0 when model/artifacts unavailable.
    """
    loaded = _load_artifacts()
    if loaded is None:
        return 0.0
    arts, booster = loaded
    feats = arts.features
    med = arts.impute_median

    # Build feature vector in training order.
    vec: List[float] = []
    for f in feats:
        v = _safe_float(row.get(f))
        if v is None:
            v = float(med.get(f, 0.0))
        vec.append(float(v))
    try:
        import numpy as np

        X = np.array([vec], dtype=float)
        pred = booster.predict(X)
        score = float(pred[0]) if hasattr(pred, "__len__") else float(pred)
        if score < 0:
            return 0.0
        if score > 1:
            return 1.0
        return score
    except Exception:
        return 0.0


def maybe_log_ml_score_enabled() -> bool:
    """Small helper for runtime guards."""
    if os.getenv("ML_ENTRY_FILTER_LOG", "1").strip().lower() in ("0", "false", "no", "off"):
        return False
    return DEFAULT_MODEL_PATH.is_file() and DEFAULT_META_PATH.is_file()

