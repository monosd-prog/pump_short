from __future__ import annotations

from typing import Dict, Tuple


def compute_context_score(parts: Dict[str, float]) -> Tuple[float, Dict[str, float]]:
    score = float(sum(parts.values()))
    score = max(0.0, min(1.0, score))
    return score, parts
