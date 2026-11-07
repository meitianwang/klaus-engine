// Copyright 2025 OpenAI
// SPDX-License-Identifier: Apache-2.0
// Modified to bind runtime-owned errors, metadata and session contracts.

//! Counts policy denials per turn; the host carries out any requested interruption.

const MAX_CONSECUTIVE_CYBER_DENIALS_PER_TURN: u32 = 1;
const MAX_CONSECUTIVE_DENIALS_PER_TURN: u32 = 3;
const MAX_RECENT_CYBER_AUTO_REVIEW_DENIALS_PER_TURN: u32 = 1;
const MAX_RECENT_AUTO_REVIEW_DENIALS_PER_TURN: u32 = 10;
pub const DENIAL_WINDOW_SIZE: usize = 50;

#[derive(Debug, Default)]
pub struct DenialCircuitBreaker {
    turns: std::collections::HashMap<String, DenialCircuitBreakerTurn>,
}

#[derive(Debug, Default)]
struct DenialCircuitBreakerTurn {
    consecutive_denials: u32,
    recent_denials: std::collections::VecDeque<bool>,
    interrupt_triggered: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DenialCircuitBreakerPolicy {
    Standard,
    CyberModel,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DenialCircuitBreakerAction {
    Continue,
    InterruptTurn {
        consecutive_denials: u32,
        recent_denials: u32,
    },
}

impl DenialCircuitBreaker {
    pub fn clear_turn(&mut self, turn_id: &str) {
        self.turns.remove(turn_id);
    }

    pub fn record_denial(
        &mut self,
        turn_id: &str,
        policy: DenialCircuitBreakerPolicy,
    ) -> DenialCircuitBreakerAction {
        let turn = self.turns.entry(turn_id.to_string()).or_default();
        turn.consecutive_denials = turn.consecutive_denials.saturating_add(1);
        Self::record_recent_review(turn, /*denied*/ true);
        let recent_denials = turn.recent_denials.iter().filter(|denied| **denied).count() as u32;
        let (max_consecutive_denials, max_recent_denials) = match policy {
            DenialCircuitBreakerPolicy::Standard => (
                MAX_CONSECUTIVE_DENIALS_PER_TURN,
                MAX_RECENT_AUTO_REVIEW_DENIALS_PER_TURN,
            ),
            DenialCircuitBreakerPolicy::CyberModel => (
                MAX_CONSECUTIVE_CYBER_DENIALS_PER_TURN,
                MAX_RECENT_CYBER_AUTO_REVIEW_DENIALS_PER_TURN,
            ),
        };
        if !turn.interrupt_triggered
            && (turn.consecutive_denials >= max_consecutive_denials
                || recent_denials >= max_recent_denials)
        {
            turn.interrupt_triggered = true;
            DenialCircuitBreakerAction::InterruptTurn {
                consecutive_denials: turn.consecutive_denials,
                recent_denials,
            }
        } else {
            DenialCircuitBreakerAction::Continue
        }
    }

    pub fn record_non_denial(&mut self, turn_id: &str) {
        let turn = self.turns.entry(turn_id.to_string()).or_default();
        turn.consecutive_denials = 0;
        Self::record_recent_review(turn, /*denied*/ false);
    }

    fn record_recent_review(turn: &mut DenialCircuitBreakerTurn, denied: bool) {
        turn.recent_denials.push_back(denied);
        if turn.recent_denials.len() > DENIAL_WINDOW_SIZE {
            turn.recent_denials.pop_front();
        }
    }
}

#[cfg(test)]
#[path = "circuit_breaker_tests.rs"]
mod tests;
