#!/usr/bin/env python3
"""
Patch for TASK_B40_REPORTS reconciliation.
This script demonstrates how to add reconciliation logging to daily_tg_report.py
"""

import logging
logger = logging.getLogger(__name__)

# This would be added to scripts/daily_tg_report.py imports
from analytics.reconciliation import (
    reconcile_raw_vs_report, 
    check_enrich_coverage, 
    check_ev_consistency,
    run_full_reconciliation
)


def add_reconciliation_to_enrich(outcomes_before, outcomes_after):
    """
    Add enrichment coverage logging to _enrich_core_with_events function.
    """
    coverage_ratio, coverage_warning = check_enrich_coverage(outcomes_before, outcomes_after)
    
    if coverage_warning:
        logger.warning(f"TASK_B40_REPORTS: {coverage_warning}")
    else:
        logger.info(f"TASK_B40_REPORTS: Good enrich coverage: {coverage_ratio:.2%}")
    
    return outcomes_after, coverage_ratio


def add_reconciliation_to_report_generation(outcomes_df, report_metrics_dict):
    """
    Add reconciliation check after report generation.
    """
    reconciliation_results = {}
    
    for strategy in ['short_pump', 'short_pump_fast0']:
        for mode in ['paper', 'live']:
            strategy_mask = outcomes_df['strategy'] == strategy
            mode_mask = outcomes_df['mode'] == mode
            filtered_outcomes = outcomes_df[strategy_mask & mode_mask]
            
            if len(filtered_outcomes) == 0:
                continue
            
            report_key = f"{strategy}_{mode}"
            metrics = report_metrics_dict.get(report_key, {})
            
            if metrics:
                recon_result = reconcile_raw_vs_report(
                    filtered_outcomes, metrics, strategy, mode
                )
                reconciliation_results[report_key] = recon_result
                
                # Log EV consistency if available
                if 'ev' in metrics and 'raw_ev' in metrics:
                    ev_match, ev_diff = check_ev_consistency(
                        metrics.get('raw_ev'), metrics.get('ev')
                    )
                    recon_result['ev_match'] = ev_match
                    recon_result['ev_diff'] = ev_diff
    
    # Log summary
    if reconciliation_results:
        mismatches = {k: v for k, v in reconciliation_results.items() if not v.get('match', False)}
        
        if mismatches:
            logger.warning(f"TASK_B40_REPORTS: Reconciliation mismatches found: {len(mismatches)}")
            for key, result in mismatches.items():
                logger.warning(f"  {key}: TP_diff={result.get('diff', {}).get('tp')}, "
                             f"SL_diff={result.get('diff', {}).get('sl')}, "
                             f"N_core_diff={result.get('diff', {}).get('n_core')}")
        else:
            logger.info("TASK_B40_REPORTS: All reconciliations passed ✓")
    
    return reconciliation_results


# Example of how to integrate with existing functions
def _enrich_core_with_events_with_coverage(outcomes, events, join_key='event_id'):
    """
    Example wrapper for existing enrichment function with coverage logging.
    """
    # Original enrichment logic here...
    outcomes_before = outcomes.copy()
    
    # ... existing join logic ...
    outcomes_after = outcomes  # Placeholder for actual join result
    
    # Add coverage check
    outcomes_after, coverage_ratio = add_reconciliation_to_enrich(outcomes_before, outcomes_after)
    
    return outcomes_after


# To integrate, you would:
# 1. Add imports to scripts/daily_tg_report.py
# 2. Modify _enrich_core_with_events to call check_enrich_coverage
# 3. Add reconciliation check after report metrics are calculated
# 4. Log results for verification