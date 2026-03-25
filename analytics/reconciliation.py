"""
Reconciliation module for TASK_B40_REPORTS.
Compares raw outcomes vs report metrics.
"""

import logging
import pandas as pd

logger = logging.getLogger(__name__)


def reconcile_raw_vs_report(raw_outcomes, report_metrics, strategy_name, mode):
    """
    Reconciliation logic to compare raw outcomes vs report metrics.
    
    Args:
        raw_outcomes: DataFrame with outcomes_v3 columns
        report_metrics: dict with report metrics (tp_count, sl_count, etc.)
        strategy_name: strategy name (short_pump, short_pump_fast0)
        mode: execution mode (paper, live)
    
    Returns:
        dict with reconciliation results
    """
    # Calculate raw metrics from outcomes
    raw_core_mask = raw_outcomes['outcome_label'].isin(['TP_hit', 'SL_hit'])
    raw_core = raw_outcomes[raw_core_mask]
    
    if len(raw_core) == 0:
        logger.warning("No core outcomes for %s/%s", strategy_name, mode)
        return {'match': True, 'reason': 'No core outcomes'}
    
    raw_tp = (raw_outcomes['outcome_label'] == 'TP_hit').sum()
    raw_sl = (raw_outcomes['outcome_label'] == 'SL_hit').sum()
    raw_timeout = (raw_outcomes['outcome_label'] == 'TIMEOUT').sum()
    raw_n_core = len(raw_core)
    
    # Compare with report metrics
    report_tp = report_metrics.get('tp_count', 0)
    report_sl = report_metrics.get('sl_count', 0)
    report_timeout = report_metrics.get('timeout_count', 0)
    report_n_core = report_metrics.get('n_core', 0)
    
    # Calculate differences
    tp_diff = abs(raw_tp - report_tp)
    sl_diff = abs(raw_sl - report_sl)
    timeout_diff = abs(raw_timeout - report_timeout)
    n_core_diff = abs(raw_n_core - report_n_core)
    
    match = (tp_diff == 0 and sl_diff == 0 and timeout_diff == 0 and n_core_diff == 0)
    
    reconciliation_result = {
        'match': match,
        'raw': {
            'tp': int(raw_tp),
            'sl': int(raw_sl),
            'timeout': int(raw_timeout),
            'n_core': int(raw_n_core)
        },
        'report': {
            'tp': int(report_tp),
            'sl': int(report_sl),
            'timeout': int(report_timeout),
            'n_core': int(report_n_core)
        },
        'diff': {
            'tp': int(tp_diff),
            'sl': int(sl_diff),
            'timeout': int(timeout_diff),
            'n_core': int(n_core_diff)
        },
        'strategy': strategy_name,
        'mode': mode
    }
    
    if not match:
        logger.warning("Reconciliation mismatch for %s/%s: raw_tp=%s report_tp=%s diff=%s",
                      strategy_name, mode, raw_tp, report_tp, tp_diff)
        logger.warning("Raw vs Report: tp_diff=%s, sl_diff=%s, timeout_diff=%s, n_core_diff=%s",
                      tp_diff, sl_diff, timeout_diff, n_core_diff)
    else:
        logger.info("Reconciliation passed for %s/%s", strategy_name, mode)
    
    return reconciliation_result


def check_enrich_coverage(outcomes_before, outcomes_after, threshold=0.95):
    """
    Check enrichment coverage after join with events.
    
    Args:
        outcomes_before: DataFrame before enrichment
        outcomes_after: DataFrame after enrichment  
        threshold: minimum acceptable coverage ratio
    
    Returns:
        tuple (coverage_ratio, warning_message)
    """
    rows_before = len(outcomes_before)
    rows_after = len(outcomes_after)
    
    coverage_ratio = rows_after / rows_before if rows_before > 0 else 0
    
    warning_msg = None
    if coverage_ratio < threshold:
        warning_msg = (
            f"Low enrich coverage: rows_before={rows_before}, "
            f"rows_after={rows_after}, ratio={coverage_ratio:.4f}"
        )
        logger.warning(warning_msg)
    else:
        logger.info("Good enrich coverage: rows_before=%s, rows_after=%s, ratio=%.4f",
                   rows_before, rows_after, coverage_ratio)
    
    return coverage_ratio, warning_msg


def check_ev_consistency(raw_ev, report_ev, tolerance=1e-6):
    """
    Check EV consistency between raw and report values.
    
    Args:
        raw_ev: EV calculated from raw outcomes
        report_ev: EV from report
        tolerance: maximum allowed difference
    
    Returns:
        tuple (matches, diff)
    """
    diff = abs(raw_ev - report_ev)
    matches = diff <= tolerance
    
    if not matches:
        logger.warning("EV mismatch: raw_ev=%.6f, report_ev=%.6f, diff=%.6f",
                      raw_ev, report_ev, diff)
    else:
        logger.info("EV matches within tolerance: raw_ev=%.6f, report_ev=%.6f",
                   raw_ev, report_ev)
    
    return matches, diff


def run_full_reconciliation(outcomes_df, events_df, report_metrics_dict):
    """
    Run full reconciliation pipeline.
    
    Args:
        outcomes_df: outcomes DataFrame
        events_df: events DataFrame
        report_metrics_dict: dict with all report metrics
    
    Returns:
        dict with all reconciliation results
    """
    results = {}
    
    # TODO(TASK_B40_REPORTS): enrich coverage check requires the post-join DataFrame.
    # Skipped here to avoid false-positive 100% coverage from comparing df to itself.
    # Wire up when the actual join result is passed into this function.
    results['coverage'] = {
        'ratio': None,
        'warning': 'coverage check not run (post-join df not available)'
    }
    
    # Run reconciliation for each strategy/mode
    for (strategy, mode), metrics in report_metrics_dict.items():
        # Filter outcomes for this strategy/mode
        strategy_mask = outcomes_df['strategy'] == strategy
        mode_mask = outcomes_df['mode'] == mode
        filtered_outcomes = outcomes_df[strategy_mask & mode_mask]
        
        recon_result = reconcile_raw_vs_report(
            filtered_outcomes, metrics, strategy, mode
        )
        results[f'{strategy}_{mode}'] = recon_result
        
        # Check EV consistency if available
        if 'ev' in metrics and 'raw_ev' in metrics:
            matches, diff = check_ev_consistency(
                metrics.get('raw_ev'), metrics.get('ev')
            )
            recon_result['ev_match'] = matches
            recon_result['ev_diff'] = diff
    
    return results
