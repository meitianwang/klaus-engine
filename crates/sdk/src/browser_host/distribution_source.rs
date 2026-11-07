//! Trust configuration for binary browser assets admitted from a distribution.
//!
//! A source checkout carries no approved binary issuer: no CI workload
//! identity, no native-host manifest producer and no fixed trust anchors.
//! Every admission path that depends on one of these therefore rejects. A
//! future signed binary distribution has to supply independently reviewed
//! values here; nothing at runtime (environment variables, files beside the
//! executable, or self-reported receipt fields) can manufacture them.

/// Identity of the build workload that produced a distributed binary, plus the
/// administrator-pinned trust anchor its receipts must be bound to.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct WorkloadIdentity {
    pub repository: &'static str,
    pub reference: &'static str,
    pub job_id: &'static str,
    pub trust_anchor_sha256: [u8; 32],
}

/// The approved workload for externally verified browser runtime artifacts.
/// `None` in a source distribution: there is no issuer to trust.
pub(super) const WORKLOAD_IDENTITY: Option<WorkloadIdentity> = None;

/// The producer string a native-host distribution manifest must carry.
/// `None` in a source distribution: no manifest is admissible.
pub(super) const NATIVE_HOST_PRODUCER: Option<&str> = None;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn source_distribution_has_no_issuer_or_native_host_producer() {
        assert!(WORKLOAD_IDENTITY.is_none());
        assert!(NATIVE_HOST_PRODUCER.is_none());
    }
}
