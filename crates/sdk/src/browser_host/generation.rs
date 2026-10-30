use std::collections::{BTreeMap, BTreeSet};

use klaus_agent_core::runtime_values::RuntimeString;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BrowserGenerationTarget {
    Unix,
    Windows,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BrowserStartingSubphase {
    RootSpawn,
    BrokerAdopting,
    PackageLoading,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BrowserGenerationPhase {
    Starting(BrowserStartingSubphase),
    Open,
    Draining,
    FacadeClosed,
    Reaped,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BrowserGenerationError {
    WrongGeneration,
    InvalidTransition,
    ExternalAdmissionClosed,
    RequestUnknown,
    RequestSubgateClosed,
    RequestProcessesOutstanding,
    CounterExhausted,
    CleanupOutstanding,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BrowserRequestBoundary {
    Active,
    OrderlyCancellation,
    ImmediateCancellation,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct BrowserRequestAdmission {
    boundary: BrowserRequestBoundary,
    process_subgate_open: bool,
    next_process_token: u64,
    process_tokens: BTreeSet<u64>,
}

impl BrowserRequestAdmission {
    fn new() -> Self {
        Self {
            boundary: BrowserRequestBoundary::Active,
            process_subgate_open: true,
            next_process_token: 0,
            process_tokens: BTreeSet::new(),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SidecarTerminationPrelude {
    OrderlyChildren,
    ImmediateCutover,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SidecarTerminationActionPhase {
    StartingBoundary,
    ChildReceipts,
    Cutover,
    SidecarAction,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SidecarTerminationState {
    Armed,
    ActionStarted {
        receipt: u64,
        prelude: SidecarTerminationPrelude,
        phase: SidecarTerminationActionPhase,
    },
    Settling {
        receipt: u64,
    },
    Consumed {
        receipt: u64,
    },
}

/// One pre-reserved, single-action sidecar termination receipt. An Immediate
/// trigger may upgrade an orderly prelude before the sidecar action starts, but
/// can never allocate a second receipt or action.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SidecarTerminationMachine {
    receipt: u64,
    state: SidecarTerminationState,
}

impl SidecarTerminationMachine {
    pub fn new(receipt: u64) -> Result<Self, BrowserGenerationError> {
        if receipt == 0 {
            return Err(BrowserGenerationError::InvalidTransition);
        }
        Ok(Self {
            receipt,
            state: SidecarTerminationState::Armed,
        })
    }

    pub fn state(&self) -> SidecarTerminationState {
        self.state
    }

    pub fn trigger_orderly(&mut self) -> u64 {
        if self.state == SidecarTerminationState::Armed {
            self.state = SidecarTerminationState::ActionStarted {
                receipt: self.receipt,
                prelude: SidecarTerminationPrelude::OrderlyChildren,
                phase: SidecarTerminationActionPhase::StartingBoundary,
            };
        }
        self.receipt
    }

    pub fn trigger_immediate(&mut self) -> u64 {
        match self.state {
            SidecarTerminationState::Armed => {
                self.state = SidecarTerminationState::ActionStarted {
                    receipt: self.receipt,
                    prelude: SidecarTerminationPrelude::ImmediateCutover,
                    phase: SidecarTerminationActionPhase::Cutover,
                };
            }
            SidecarTerminationState::ActionStarted {
                phase:
                    SidecarTerminationActionPhase::StartingBoundary
                    | SidecarTerminationActionPhase::ChildReceipts,
                ..
            } => {
                self.state = SidecarTerminationState::ActionStarted {
                    receipt: self.receipt,
                    prelude: SidecarTerminationPrelude::ImmediateCutover,
                    phase: SidecarTerminationActionPhase::Cutover,
                };
            }
            SidecarTerminationState::ActionStarted {
                phase:
                    SidecarTerminationActionPhase::Cutover
                    | SidecarTerminationActionPhase::SidecarAction,
                ..
            }
            | SidecarTerminationState::Settling { .. }
            | SidecarTerminationState::Consumed { .. } => {}
        }
        self.receipt
    }

    pub fn advance_orderly_children(&mut self) -> Result<(), BrowserGenerationError> {
        match self.state {
            SidecarTerminationState::ActionStarted {
                prelude: SidecarTerminationPrelude::OrderlyChildren,
                phase: SidecarTerminationActionPhase::StartingBoundary,
                ..
            } => {
                self.state = SidecarTerminationState::ActionStarted {
                    receipt: self.receipt,
                    prelude: SidecarTerminationPrelude::OrderlyChildren,
                    phase: SidecarTerminationActionPhase::ChildReceipts,
                };
                Ok(())
            }
            _ => Err(BrowserGenerationError::InvalidTransition),
        }
    }

    pub fn start_sidecar_action(&mut self) -> Result<(), BrowserGenerationError> {
        match self.state {
            SidecarTerminationState::ActionStarted {
                prelude: SidecarTerminationPrelude::OrderlyChildren,
                phase: SidecarTerminationActionPhase::ChildReceipts,
                ..
            }
            | SidecarTerminationState::ActionStarted {
                prelude: SidecarTerminationPrelude::ImmediateCutover,
                phase: SidecarTerminationActionPhase::Cutover,
                ..
            } => {
                let prelude = match self.state {
                    SidecarTerminationState::ActionStarted { prelude, .. } => prelude,
                    _ => unreachable!("matched action-started state"),
                };
                self.state = SidecarTerminationState::ActionStarted {
                    receipt: self.receipt,
                    prelude,
                    phase: SidecarTerminationActionPhase::SidecarAction,
                };
                Ok(())
            }
            _ => Err(BrowserGenerationError::InvalidTransition),
        }
    }

    pub fn begin_settling(&mut self) -> Result<(), BrowserGenerationError> {
        match self.state {
            SidecarTerminationState::ActionStarted {
                phase: SidecarTerminationActionPhase::SidecarAction,
                ..
            } => {
                self.state = SidecarTerminationState::Settling {
                    receipt: self.receipt,
                };
                Ok(())
            }
            _ => Err(BrowserGenerationError::InvalidTransition),
        }
    }

    pub fn consume(&mut self) -> Result<(), BrowserGenerationError> {
        match self.state {
            SidecarTerminationState::Settling { .. } => {
                self.state = SidecarTerminationState::Consumed {
                    receipt: self.receipt,
                };
                Ok(())
            }
            _ => Err(BrowserGenerationError::InvalidTransition),
        }
    }
}

/// Shadow browser generation controller. It freezes target-specific readiness,
/// request/subprocess admission, ABA generation checks, and the one termination
/// receipt without performing process or callback I/O.
#[derive(Debug)]
pub struct BrowserGeneration {
    generation: u64,
    target: BrowserGenerationTarget,
    phase: BrowserGenerationPhase,
    next_request_token: u64,
    requests: BTreeMap<u64, BrowserRequestAdmission>,
    public_error: Option<RuntimeString>,
    termination: SidecarTerminationMachine,
}

impl BrowserGeneration {
    pub fn new(
        generation: u64,
        target: BrowserGenerationTarget,
        termination_receipt: u64,
    ) -> Result<Self, BrowserGenerationError> {
        if generation == 0 {
            return Err(BrowserGenerationError::InvalidTransition);
        }
        Ok(Self {
            generation,
            target,
            phase: BrowserGenerationPhase::Starting(BrowserStartingSubphase::RootSpawn),
            next_request_token: 0,
            requests: BTreeMap::new(),
            public_error: None,
            termination: SidecarTerminationMachine::new(termination_receipt)?,
        })
    }

    pub fn generation(&self) -> u64 {
        self.generation
    }

    pub fn phase(&self) -> BrowserGenerationPhase {
        self.phase
    }

    pub fn public_error(&self) -> Option<&RuntimeString> {
        self.public_error.as_ref()
    }

    pub fn termination(&self) -> &SidecarTerminationMachine {
        &self.termination
    }

    pub fn root_committed(&mut self, generation: u64) -> Result<(), BrowserGenerationError> {
        self.require_generation(generation)?;
        if self.phase != BrowserGenerationPhase::Starting(BrowserStartingSubphase::RootSpawn) {
            return Err(BrowserGenerationError::InvalidTransition);
        }
        self.phase = BrowserGenerationPhase::Starting(match self.target {
            BrowserGenerationTarget::Unix => BrowserStartingSubphase::PackageLoading,
            BrowserGenerationTarget::Windows => BrowserStartingSubphase::BrokerAdopting,
        });
        Ok(())
    }

    pub fn broker_adopted(&mut self, generation: u64) -> Result<(), BrowserGenerationError> {
        self.require_generation(generation)?;
        if self.target != BrowserGenerationTarget::Windows
            || self.phase
                != BrowserGenerationPhase::Starting(BrowserStartingSubphase::BrokerAdopting)
        {
            return Err(BrowserGenerationError::InvalidTransition);
        }
        self.phase = BrowserGenerationPhase::Starting(BrowserStartingSubphase::PackageLoading);
        Ok(())
    }

    pub fn package_ready(&mut self, generation: u64) -> Result<(), BrowserGenerationError> {
        self.require_generation(generation)?;
        if self.phase != BrowserGenerationPhase::Starting(BrowserStartingSubphase::PackageLoading) {
            return Err(BrowserGenerationError::InvalidTransition);
        }
        self.phase = BrowserGenerationPhase::Open;
        Ok(())
    }

    pub fn admit_request(&mut self, generation: u64) -> Result<u64, BrowserGenerationError> {
        self.require_generation(generation)?;
        if self.phase != BrowserGenerationPhase::Open {
            return Err(BrowserGenerationError::ExternalAdmissionClosed);
        }
        let token = self
            .next_request_token
            .checked_add(1)
            .ok_or(BrowserGenerationError::CounterExhausted)?;
        self.next_request_token = token;
        self.requests.insert(token, BrowserRequestAdmission::new());
        Ok(token)
    }

    pub fn issue_process(
        &mut self,
        generation: u64,
        request_token: u64,
    ) -> Result<u64, BrowserGenerationError> {
        self.require_generation(generation)?;
        let request = self
            .requests
            .get_mut(&request_token)
            .ok_or(BrowserGenerationError::RequestUnknown)?;
        if !request.process_subgate_open {
            return Err(BrowserGenerationError::RequestSubgateClosed);
        }
        let token = request
            .next_process_token
            .checked_add(1)
            .ok_or(BrowserGenerationError::CounterExhausted)?;
        request.next_process_token = token;
        request.process_tokens.insert(token);
        Ok(token)
    }

    pub fn settle_process(
        &mut self,
        generation: u64,
        request_token: u64,
        process_token: u64,
    ) -> Result<(), BrowserGenerationError> {
        self.require_generation(generation)?;
        let request = self
            .requests
            .get_mut(&request_token)
            .ok_or(BrowserGenerationError::RequestUnknown)?;
        if !request.process_tokens.remove(&process_token) {
            return Err(BrowserGenerationError::RequestUnknown);
        }
        Ok(())
    }

    pub fn complete_request(
        &mut self,
        generation: u64,
        request_token: u64,
    ) -> Result<BrowserRequestBoundary, BrowserGenerationError> {
        self.require_generation(generation)?;
        let request = self
            .requests
            .get(&request_token)
            .ok_or(BrowserGenerationError::RequestUnknown)?;
        if !request.process_tokens.is_empty() {
            return Err(BrowserGenerationError::RequestProcessesOutstanding);
        }
        let boundary = request.boundary;
        self.requests.remove(&request_token);
        Ok(boundary)
    }

    pub fn trigger_orderly_shutdown(
        &mut self,
        generation: u64,
    ) -> Result<u64, BrowserGenerationError> {
        self.require_generation(generation)?;
        if self.termination.state() != SidecarTerminationState::Armed {
            return Ok(self.termination.trigger_orderly());
        }
        if matches!(
            self.phase,
            BrowserGenerationPhase::FacadeClosed | BrowserGenerationPhase::Reaped
        ) {
            return Ok(self.termination.trigger_orderly());
        }
        self.phase = BrowserGenerationPhase::Draining;
        for request in self.requests.values_mut() {
            request.process_subgate_open = false;
            request.boundary = BrowserRequestBoundary::OrderlyCancellation;
        }
        Ok(self.termination.trigger_orderly())
    }

    pub fn trigger_immediate(
        &mut self,
        generation: u64,
        public_error: RuntimeString,
    ) -> Result<u64, BrowserGenerationError> {
        self.require_generation(generation)?;
        if matches!(
            self.termination.state(),
            SidecarTerminationState::ActionStarted {
                phase: SidecarTerminationActionPhase::SidecarAction,
                ..
            } | SidecarTerminationState::Settling { .. }
                | SidecarTerminationState::Consumed { .. }
        ) {
            return Ok(self.termination.trigger_immediate());
        }
        if self.public_error.is_none() {
            self.public_error = Some(public_error);
        }
        if !matches!(
            self.phase,
            BrowserGenerationPhase::FacadeClosed | BrowserGenerationPhase::Reaped
        ) {
            self.phase = BrowserGenerationPhase::Draining;
        }
        for request in self.requests.values_mut() {
            request.process_subgate_open = false;
            request.boundary = BrowserRequestBoundary::ImmediateCancellation;
        }
        Ok(self.termination.trigger_immediate())
    }

    pub fn advance_orderly_children(
        &mut self,
        generation: u64,
    ) -> Result<(), BrowserGenerationError> {
        self.require_generation(generation)?;
        self.termination.advance_orderly_children()
    }

    pub fn start_sidecar_action(&mut self, generation: u64) -> Result<(), BrowserGenerationError> {
        self.require_generation(generation)?;
        self.termination.start_sidecar_action()?;
        self.phase = BrowserGenerationPhase::FacadeClosed;
        Ok(())
    }

    pub fn begin_settling(&mut self, generation: u64) -> Result<(), BrowserGenerationError> {
        self.require_generation(generation)?;
        self.termination.begin_settling()
    }

    pub fn consume_termination(&mut self, generation: u64) -> Result<(), BrowserGenerationError> {
        self.require_generation(generation)?;
        self.termination.consume()
    }

    pub fn mark_reaped(&mut self, generation: u64) -> Result<(), BrowserGenerationError> {
        self.require_generation(generation)?;
        if self.phase != BrowserGenerationPhase::FacadeClosed
            || !self.requests.is_empty()
            || !matches!(
                self.termination.state(),
                SidecarTerminationState::Consumed { .. }
            )
        {
            return Err(BrowserGenerationError::CleanupOutstanding);
        }
        self.phase = BrowserGenerationPhase::Reaped;
        Ok(())
    }

    fn require_generation(&self, generation: u64) -> Result<(), BrowserGenerationError> {
        if generation == self.generation {
            Ok(())
        } else {
            Err(BrowserGenerationError::WrongGeneration)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unix_and_windows_readiness_have_distinct_closed_subphases() {
        let mut unix = BrowserGeneration::new(7, BrowserGenerationTarget::Unix, 100).unwrap();
        unix.root_committed(7).unwrap();
        assert_eq!(
            unix.phase(),
            BrowserGenerationPhase::Starting(BrowserStartingSubphase::PackageLoading)
        );
        assert_eq!(
            unix.broker_adopted(7),
            Err(BrowserGenerationError::InvalidTransition)
        );
        unix.package_ready(7).unwrap();
        assert_eq!(unix.phase(), BrowserGenerationPhase::Open);

        let mut windows = BrowserGeneration::new(8, BrowserGenerationTarget::Windows, 101).unwrap();
        windows.root_committed(8).unwrap();
        assert_eq!(
            windows.phase(),
            BrowserGenerationPhase::Starting(BrowserStartingSubphase::BrokerAdopting)
        );
        assert_eq!(
            windows.package_ready(8),
            Err(BrowserGenerationError::InvalidTransition)
        );
        windows.broker_adopted(8).unwrap();
        windows.package_ready(8).unwrap();
        assert_eq!(windows.phase(), BrowserGenerationPhase::Open);
    }

    #[test]
    fn request_subgate_closes_before_orderly_process_admission() {
        let mut generation = BrowserGeneration::new(3, BrowserGenerationTarget::Unix, 40).unwrap();
        generation.root_committed(3).unwrap();
        generation.package_ready(3).unwrap();
        let request = generation.admit_request(3).unwrap();
        let process = generation.issue_process(3, request).unwrap();
        assert_eq!(generation.trigger_orderly_shutdown(3), Ok(40));
        assert_eq!(
            generation.issue_process(3, request),
            Err(BrowserGenerationError::RequestSubgateClosed)
        );
        assert_eq!(
            generation.complete_request(3, request),
            Err(BrowserGenerationError::RequestProcessesOutstanding)
        );
        generation.settle_process(3, request, process).unwrap();
        assert_eq!(
            generation.complete_request(3, request),
            Ok(BrowserRequestBoundary::OrderlyCancellation)
        );
    }

    #[test]
    fn immediate_upgrade_reuses_receipt_and_first_public_error() {
        let mut generation = BrowserGeneration::new(9, BrowserGenerationTarget::Unix, 55).unwrap();
        generation.root_committed(9).unwrap();
        generation.package_ready(9).unwrap();
        generation.trigger_orderly_shutdown(9).unwrap();
        generation.advance_orderly_children(9).unwrap();
        assert_eq!(
            generation.trigger_immediate(9, RuntimeString::from("first")),
            Ok(55)
        );
        assert_eq!(
            generation.trigger_immediate(9, RuntimeString::from("second")),
            Ok(55)
        );
        assert_eq!(
            generation.public_error(),
            Some(&RuntimeString::from("first"))
        );
        assert_eq!(
            generation.termination().state(),
            SidecarTerminationState::ActionStarted {
                receipt: 55,
                prelude: SidecarTerminationPrelude::ImmediateCutover,
                phase: SidecarTerminationActionPhase::Cutover,
            }
        );
    }

    #[test]
    fn late_orderly_trigger_only_joins_an_immediate_receipt() {
        let mut generation = BrowserGeneration::new(10, BrowserGenerationTarget::Unix, 56).unwrap();
        generation.root_committed(10).unwrap();
        generation.package_ready(10).unwrap();
        let request = generation.admit_request(10).unwrap();

        assert_eq!(
            generation.trigger_immediate(10, RuntimeString::from("transport failed")),
            Ok(56)
        );
        assert_eq!(generation.trigger_orderly_shutdown(10), Ok(56));
        assert_eq!(
            generation.complete_request(10, request),
            Ok(BrowserRequestBoundary::ImmediateCancellation)
        );
        assert_eq!(
            generation.termination().state(),
            SidecarTerminationState::ActionStarted {
                receipt: 56,
                prelude: SidecarTerminationPrelude::ImmediateCutover,
                phase: SidecarTerminationActionPhase::Cutover,
            }
        );
    }

    #[test]
    fn sidecar_action_is_single_and_reap_waits_for_all_request_owners() {
        let mut generation = BrowserGeneration::new(11, BrowserGenerationTarget::Unix, 60).unwrap();
        generation.root_committed(11).unwrap();
        generation.package_ready(11).unwrap();
        let request = generation.admit_request(11).unwrap();
        generation
            .trigger_immediate(11, RuntimeString::from("transport failed"))
            .unwrap();
        generation.start_sidecar_action(11).unwrap();
        assert_eq!(generation.phase(), BrowserGenerationPhase::FacadeClosed);
        let first_error = generation.public_error().cloned();
        assert_eq!(
            generation.trigger_immediate(11, RuntimeString::from("late")),
            Ok(60)
        );
        assert_eq!(generation.public_error(), first_error.as_ref());
        generation.begin_settling(11).unwrap();
        generation.consume_termination(11).unwrap();
        assert_eq!(
            generation.mark_reaped(11),
            Err(BrowserGenerationError::CleanupOutstanding)
        );
        assert_eq!(
            generation.complete_request(11, request),
            Ok(BrowserRequestBoundary::ImmediateCancellation)
        );
        generation.mark_reaped(11).unwrap();
        assert_eq!(generation.phase(), BrowserGenerationPhase::Reaped);
    }

    #[test]
    fn stale_generation_cannot_admit_or_close_a_replacement() {
        let mut generation = BrowserGeneration::new(20, BrowserGenerationTarget::Unix, 70).unwrap();
        generation.root_committed(20).unwrap();
        generation.package_ready(20).unwrap();
        assert_eq!(
            generation.admit_request(19),
            Err(BrowserGenerationError::WrongGeneration)
        );
        assert_eq!(
            generation.trigger_immediate(21, RuntimeString::from("stale")),
            Err(BrowserGenerationError::WrongGeneration)
        );
        assert_eq!(generation.phase(), BrowserGenerationPhase::Open);
        assert_eq!(generation.public_error(), None);
    }

    #[test]
    fn trigger_after_sidecar_action_only_joins_orderly_receipt() {
        let mut generation = BrowserGeneration::new(30, BrowserGenerationTarget::Unix, 80).unwrap();
        generation.root_committed(30).unwrap();
        generation.package_ready(30).unwrap();
        let request = generation.admit_request(30).unwrap();
        generation.trigger_orderly_shutdown(30).unwrap();
        generation.advance_orderly_children(30).unwrap();
        generation.start_sidecar_action(30).unwrap();
        assert_eq!(
            generation.trigger_immediate(30, RuntimeString::from("too late")),
            Ok(80)
        );
        assert_eq!(generation.public_error(), None);
        assert_eq!(
            generation.complete_request(30, request),
            Ok(BrowserRequestBoundary::OrderlyCancellation)
        );
    }
}
