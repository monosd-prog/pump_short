from __future__ import annotations

import os
import time
from typing import Any, Optional

from short_pump.logging_utils import log_exception, log_info
from short_pump.telegram import TG_BOT_TOKEN, TG_CHAT_ID, send_telegram
from trading.state import add_outcome_tg_sent, load_state, make_position_id, outcome_tg_sent, save_state

# Keep names aligned with existing B41/B42 env vars.
TG_OUTCOME_TG_SEND_RETRY_MAX = int(os.getenv("TG_OUTCOME_TG_SEND_RETRY_MAX", "3").replace(",", "."))
TG_OUTCOME_TG_SEND_RETRY_BACKOFF_SEC = float(os.getenv("TG_OUTCOME_TG_SEND_RETRY_BACKOFF_SEC", "2").replace(",", "."))


def deliver_outcome_tg(
    *,
    logger,
    delivery_strategy: str,
    run_id: str,
    event_id: str,
    symbol: str,
    res: str,
    send_text: str,
    send_telegram_kwargs: dict[str, Any],
    delivery_reason: Optional[str] = None,
    delivery_mode: Optional[str] = None,
    stage: int | None = None,
    step: str = "OUTCOME_TG",
    tg_send_enabled: bool = True,
) -> bool:
    """
    Outcome Delivery Contract helper.

    Rules (important invariants):
    - build delivery_key via make_position_id(strategy, run_id, event_id, symbol)
    - skip only if outcome_tg_sent(state, delivery_key) is already marked as delivered
    - bounded retry on send failures (exceptions)
    - mark outcome_tg_sent ONLY after successful send_telegram()
    - unified OUTCOME_TG_* logging markers for reconciliation
    """

    delivery_key = make_position_id(
        delivery_strategy,
        run_id or "",
        str(event_id or ""),
        symbol or "",
    )

    # Load state once for skip decision.
    state = load_state()
    if outcome_tg_sent(state, delivery_key):
        log_info(
            logger,
            "OUTCOME_TG_SEND_SKIP_ALREADY_FINALIZED",
            symbol=symbol,
            run_id=run_id,
            stage=stage,
            step=step,
            extra={"delivery_key": delivery_key, "skip_reason": "already_finalized"},
        )
        return False

    # Emit attempt start marker for traceability.
    log_info(
        logger,
        "OUTCOME_TG_SEND_START",
        symbol=symbol,
        run_id=run_id,
        stage=stage,
        step=step,
        extra={"delivery_key": delivery_key, "res": res, "delivery_reason": delivery_reason or "unknown"},
    )

    if not tg_send_enabled:
        # Do not call send_telegram(), do not mark outcome_tg_sent.
        log_info(
            logger,
            "OUTCOME_TG_SEND_NOOP_TG_DISABLED_NO_MARK",
            symbol=symbol,
            run_id=run_id,
            stage=stage,
            step=step,
            extra={"delivery_key": delivery_key, "skip_reason": "tg_disabled"},
        )
        return False

    tg_ready = bool(TG_BOT_TOKEN and TG_CHAT_ID)
    if not tg_ready:
        # Mirrors existing marker naming from B41 implementations.
        log_info(
            logger,
            "OUTCOME_TG_SEND_NOOP_TOKEN_MISSING_NO_MARK",
            symbol=symbol,
            run_id=run_id,
            stage=stage,
            step=step,
            extra={"delivery_key": delivery_key, "skip_reason": "token_missing"},
        )
        return False

    max_attempts = max(1, TG_OUTCOME_TG_SEND_RETRY_MAX)
    base_backoff = max(0.0, TG_OUTCOME_TG_SEND_RETRY_BACKOFF_SEC)

    for attempt in range(1, max_attempts + 1):
        try:
            # send_telegram() uses env token/chat_id and raises on HTTP errors.
            send_telegram(send_text, **send_telegram_kwargs)

            # Mark ONLY after successful send.
            add_outcome_tg_sent(state, delivery_key)
            save_state(state)
            log_info(
                logger,
                "OUTCOME_TG_DELIVERED_AND_MARKED",
                symbol=symbol,
                run_id=run_id,
                stage=stage,
                step=step,
                extra={
                    "delivery_key": delivery_key,
                    "res": res,
                    "delivery_mode": delivery_mode or "unknown",
                },
            )
            return True
        except Exception as e:
            log_exception(
                logger,
                "OUTCOME_TG_SEND_ATTEMPT_FAILED",
                symbol=symbol,
                run_id=run_id,
                stage=stage,
                step=step,
                extra={
                    "delivery_key": delivery_key,
                    "attempt": attempt,
                    "max_attempts": max_attempts,
                    "error": type(e).__name__,
                },
            )
            if attempt < max_attempts:
                # Exponential backoff prevents Telegram flood.
                time.sleep(base_backoff * (2 ** (attempt - 1)))

    log_info(
        logger,
        "OUTCOME_TG_SEND_EXHAUSTED_NO_MARK",
        symbol=symbol,
        run_id=run_id,
        stage=stage,
        step=step,
        extra={"delivery_key": delivery_key, "res": res, "retry_count": max_attempts},
    )
    return False

