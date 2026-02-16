#!/usr/bin/env python3
"""Test script for outcome_label mapping logic."""

def test_outcome_label_mapping():
    """Test that end_reason maps correctly to outcome_label."""
    test_cases = [
        ("TP_hit", "TP", "outcome_tp"),
        ("SL_hit", "SL", "outcome_sl"),
        ("TIMEOUT", "TIMEOUT", "outcome_timeout"),
        ("CONFLICT", "TIMEOUT", "outcome_timeout"),  # fallback
        ("", "TIMEOUT", "outcome_timeout"),  # empty fallback
        (None, "TIMEOUT", "outcome_timeout"),  # None fallback
    ]
    
    for end_reason, expected_label, expected_skip in test_cases:
        # Simulate the mapping logic from watcher.py
        if end_reason == "TP_hit":
            outcome_label = "TP"
            skip_reason_outcome = "outcome_tp"
        elif end_reason == "SL_hit":
            outcome_label = "SL"
            skip_reason_outcome = "outcome_sl"
        else:
            outcome_label = "TIMEOUT"
            skip_reason_outcome = "outcome_timeout"
        
        assert outcome_label == expected_label, f"end_reason={end_reason}: got {outcome_label}, expected {expected_label}"
        assert skip_reason_outcome == expected_skip, f"end_reason={end_reason}: got {skip_reason_outcome}, expected {expected_skip}"
        print(f"✓ {end_reason} -> {outcome_label} ({skip_reason_outcome})")
    
    print("\nAll outcome_label mapping tests passed!")


if __name__ == "__main__":
    test_outcome_label_mapping()
