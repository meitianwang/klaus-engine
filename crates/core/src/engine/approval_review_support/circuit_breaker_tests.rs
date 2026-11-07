// Copyright 2025 OpenAI
// SPDX-License-Identifier: Apache-2.0
// Modified for runtime component contracts and deterministic regressions.

use super::*;

#[test]
fn rejection_circuit_breaker_interrupts_after_three_consecutive_denials() {
    let mut circuit_breaker = DenialCircuitBreaker::default();
    assert_eq!(
        circuit_breaker.record_denial("turn-1", DenialCircuitBreakerPolicy::Standard),
        DenialCircuitBreakerAction::Continue
    );
    assert_eq!(
        circuit_breaker.record_denial("turn-1", DenialCircuitBreakerPolicy::Standard),
        DenialCircuitBreakerAction::Continue
    );
    assert_eq!(
        circuit_breaker.record_denial("turn-1", DenialCircuitBreakerPolicy::Standard),
        DenialCircuitBreakerAction::InterruptTurn {
            consecutive_denials: 3,
            recent_denials: 3,
        }
    );
    assert_eq!(
        circuit_breaker.record_denial("turn-1", DenialCircuitBreakerPolicy::Standard),
        DenialCircuitBreakerAction::Continue
    );
}

#[test]
fn rejection_circuit_breaker_interrupts_cyber_models_after_one_denial() {
    let mut circuit_breaker = DenialCircuitBreaker::default();
    assert_eq!(
        circuit_breaker.record_denial("turn-1", DenialCircuitBreakerPolicy::CyberModel),
        DenialCircuitBreakerAction::InterruptTurn {
            consecutive_denials: 1,
            recent_denials: 1,
        }
    );
    assert_eq!(
        circuit_breaker.record_denial("turn-1", DenialCircuitBreakerPolicy::CyberModel),
        DenialCircuitBreakerAction::Continue
    );
}

#[test]
fn rejection_circuit_breaker_resets_consecutive_denials_on_non_denial() {
    let mut circuit_breaker = DenialCircuitBreaker::default();
    assert_eq!(
        circuit_breaker.record_denial("turn-1", DenialCircuitBreakerPolicy::Standard),
        DenialCircuitBreakerAction::Continue
    );
    circuit_breaker.record_non_denial("turn-1");
    assert_eq!(
        circuit_breaker.record_denial("turn-1", DenialCircuitBreakerPolicy::Standard),
        DenialCircuitBreakerAction::Continue
    );
    assert_eq!(
        circuit_breaker.record_denial("turn-1", DenialCircuitBreakerPolicy::Standard),
        DenialCircuitBreakerAction::Continue
    );
    assert_eq!(
        circuit_breaker.record_denial("turn-1", DenialCircuitBreakerPolicy::Standard),
        DenialCircuitBreakerAction::InterruptTurn {
            consecutive_denials: 3,
            recent_denials: 4,
        }
    );
}

#[test]
fn auto_review_rejection_circuit_breaker_interrupts_after_ten_recent_denials() {
    let mut circuit_breaker = DenialCircuitBreaker::default();
    for _ in 0..9 {
        assert_eq!(
            circuit_breaker.record_denial("turn-1", DenialCircuitBreakerPolicy::Standard),
            DenialCircuitBreakerAction::Continue
        );
        circuit_breaker.record_non_denial("turn-1");
    }
    assert_eq!(
        circuit_breaker.record_denial("turn-1", DenialCircuitBreakerPolicy::Standard),
        DenialCircuitBreakerAction::InterruptTurn {
            consecutive_denials: 1,
            recent_denials: 10,
        }
    );
}

#[test]
fn auto_review_rejection_circuit_breaker_forgets_denials_outside_recent_review_window() {
    let mut circuit_breaker = DenialCircuitBreaker::default();
    for _ in 0..9 {
        assert_eq!(
            circuit_breaker.record_denial("turn-1", DenialCircuitBreakerPolicy::Standard),
            DenialCircuitBreakerAction::Continue
        );
        circuit_breaker.record_non_denial("turn-1");
    }
    for _ in 0..(DENIAL_WINDOW_SIZE - 18) {
        circuit_breaker.record_non_denial("turn-1");
    }
    assert_eq!(
        circuit_breaker.record_denial("turn-1", DenialCircuitBreakerPolicy::Standard),
        DenialCircuitBreakerAction::Continue
    );
}

#[test]
fn clearing_one_turn_preserves_other_turns_and_starts_a_fresh_denial_count() {
    let mut breaker = DenialCircuitBreaker::default();
    for turn in ["first", "second"] {
        assert_eq!(
            breaker.record_denial(turn, DenialCircuitBreakerPolicy::Standard),
            DenialCircuitBreakerAction::Continue
        );
        assert_eq!(
            breaker.record_denial(turn, DenialCircuitBreakerPolicy::Standard),
            DenialCircuitBreakerAction::Continue
        );
    }
    breaker.clear_turn("first");
    assert_eq!(
        breaker.record_denial("first", DenialCircuitBreakerPolicy::Standard),
        DenialCircuitBreakerAction::Continue
    );
    assert!(matches!(
        breaker.record_denial("second", DenialCircuitBreakerPolicy::Standard),
        DenialCircuitBreakerAction::InterruptTurn {
            consecutive_denials: 3,
            ..
        }
    ));
}
