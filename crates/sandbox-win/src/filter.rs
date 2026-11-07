//! Port-scoped network filtering, for the proxy shape.
//!
//! [`crate::capability`] already decides whether the container can touch the
//! network at all. This layer answers the narrower question the proxy mode needs:
//! *given* a container that may open client sockets, allow loopback connections to
//! a specific set of ports and refuse everything else.
//!
//! # Why the filters are session-scoped
//!
//! WFP filters live in the kernel and outlive the process that installed them.
//! A sandbox that installs filters and then dies badly leaves them behind, and the
//! next run adds more, so a machine accumulates rules for containers that no longer
//! exist. Rather than rely on a teardown path that only runs on the happy exit,
//! the engine handle is opened with `FWPM_SESSION_FLAG_DYNAMIC`: every filter added
//! through it is destroyed by the kernel when the handle closes, and the handle
//! closes on process exit no matter how the process ends.
//!
//! # Why block and permit, and why the weights matter
//!
//! Within a layer, WFP evaluates filters by weight and the highest-weight match
//! decides. So the shape is one low-weight `BLOCK` covering everything this
//! container sends, plus one high-weight `PERMIT` per allowed port. Get the weights
//! backwards and the block wins over the permits, which looks exactly like "the
//! proxy is unreachable".
//!
//! A permit is conditioned on **both** the port and the loopback address. Port
//! alone would let the container reach that port on any host it can route to,
//! which is a hole in a boundary whose whole purpose is "local proxy only".

use crate::boundary::NetworkBoundary;

/// One filter this crate will install.
///
/// Modelled as data so the plan a boundary produces can be asserted without a
/// Windows host: the ordering and the condition set are where the security lives,
/// and both are decidable here.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FilterPlan {
    /// Refuse every outbound connection from the container.
    BlockAll { weight: u64 },
    /// Allow loopback connections to one port.
    PermitLoopbackPort { port: u16, weight: u64 },
}

/// Weight of the catch-all block.
///
/// Low, so every permit outranks it. Not zero: zero is also the weight WFP gives
/// filters that do not specify one, and leaving them equal makes the outcome
/// depend on insertion order rather than on intent.
pub const BLOCK_WEIGHT: u64 = 1;

/// Weight of each port permit.
pub const PERMIT_WEIGHT: u64 = 100;

/// Which filters implement a boundary.
///
/// `Denied` and `Allowed` produce nothing: the first is enforced by withholding the
/// capability (see [`crate::capability`]) and the second has nothing to restrict.
/// Installing a block for `Denied` would be redundant, and — worse — would imply
/// that the boundary depends on a filter having been installed successfully.
pub fn plan_filters(boundary: &NetworkBoundary) -> Vec<FilterPlan> {
    match boundary {
        NetworkBoundary::Denied | NetworkBoundary::Allowed => Vec::new(),
        NetworkBoundary::LoopbackPorts { ports, .. } => {
            let mut plans = vec![FilterPlan::BlockAll {
                weight: BLOCK_WEIGHT,
            }];
            // Deduplicated: a repeated port in a policy would otherwise install the
            // same filter twice, and the second one is dead weight in the kernel.
            let mut seen = Vec::new();
            for port in ports {
                if seen.contains(port) {
                    continue;
                }
                seen.push(*port);
                plans.push(FilterPlan::PermitLoopbackPort {
                    port: *port,
                    weight: PERMIT_WEIGHT,
                });
            }
            plans
        }
    }
}

/// Loopback address the permits are scoped to, in host byte order.
///
/// WFP takes IPv4 addresses as a `u32` in host order, not network order. Passing
/// the network-order value silently produces a filter for 1.0.0.127, which permits
/// nothing the container was meant to reach and blocks the proxy.
pub const LOOPBACK_V4: u32 = 0x7F00_0001;

#[cfg(windows)]
mod imp {
    use std::ptr;

    use windows_sys::Win32::Foundation::{ERROR_SUCCESS, HANDLE};
    use windows_sys::Win32::NetworkManagement::WindowsFilteringPlatform::{
        FwpmEngineClose0, FwpmEngineOpen0, FwpmFilterAdd0, FWPM_ACTION0,
        FWPM_CONDITION_ALE_PACKAGE_ID, FWPM_CONDITION_IP_REMOTE_ADDRESS,
        FWPM_CONDITION_IP_REMOTE_PORT, FWPM_DISPLAY_DATA0, FWPM_FILTER0, FWPM_FILTER_CONDITION0,
        FWPM_LAYER_ALE_AUTH_CONNECT_V4, FWPM_SESSION0, FWPM_SESSION_FLAG_DYNAMIC, FWP_ACTION_BLOCK,
        FWP_ACTION_PERMIT, FWP_CONDITION_VALUE0, FWP_MATCH_EQUAL, FWP_SID, FWP_UINT16, FWP_UINT32,
        FWP_UINT64, FWP_VALUE0,
    };

    use super::{FilterPlan, LOOPBACK_V4};
    use crate::identity::AppContainerSid;

    /// Why network filtering could not be established.
    #[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
    pub enum FilterError {
        #[error("opening the filter engine failed: Win32 error {0}")]
        EngineOpen(u32),
        #[error("installing the {what} filter failed: Win32 error {code}")]
        FilterAdd { what: String, code: u32 },
    }

    /// An open, dynamic WFP session.
    ///
    /// Dropping it closes the engine handle, and the kernel removes every filter
    /// installed through it. That is the entire teardown story for this layer: no
    /// enumeration, no bookkeeping, nothing left behind if the process is killed.
    pub struct FilterSession {
        engine: HANDLE,
    }

    impl Drop for FilterSession {
        fn drop(&mut self) {
            if self.engine != 0 {
                unsafe { FwpmEngineClose0(self.engine) };
            }
        }
    }

    unsafe impl Send for FilterSession {}
    unsafe impl Sync for FilterSession {}

    fn wide(text: &str) -> Vec<u16> {
        text.encode_utf16().chain(std::iter::once(0)).collect()
    }

    impl FilterSession {
        /// Open a dynamic session and install a plan.
        pub fn install(
            plans: &[FilterPlan],
            sid: &AppContainerSid,
        ) -> Result<Option<Self>, FilterError> {
            if plans.is_empty() {
                // Nothing to filter, so no session and no kernel state. Returning
                // None rather than an empty session keeps the "no filters" case
                // free of anything that could fail later.
                return Ok(None);
            }
            let mut session: FWPM_SESSION0 = unsafe { std::mem::zeroed() };
            session.flags = FWPM_SESSION_FLAG_DYNAMIC;
            let mut engine: HANDLE = 0;
            let code = unsafe {
                FwpmEngineOpen0(
                    ptr::null(),
                    // RPC_C_AUTHN_WINNT: the authentication service the local
                    // filter engine expects.
                    10,
                    ptr::null(),
                    &session,
                    &mut engine,
                )
            };
            if code != ERROR_SUCCESS {
                return Err(FilterError::EngineOpen(code));
            }
            let session = Self { engine };
            for plan in plans {
                session.add(plan, sid)?;
            }
            Ok(Some(session))
        }

        fn add(&self, plan: &FilterPlan, sid: &AppContainerSid) -> Result<(), FilterError> {
            // The container condition is on every filter: without it, a block would
            // apply to the whole machine rather than to this sandbox.
            let mut container_condition: FWPM_FILTER_CONDITION0 = unsafe { std::mem::zeroed() };
            container_condition.fieldKey = FWPM_CONDITION_ALE_PACKAGE_ID;
            container_condition.matchType = FWP_MATCH_EQUAL;
            container_condition.conditionValue = FWP_CONDITION_VALUE0 {
                r#type: FWP_SID,
                Anonymous: windows_sys::Win32::NetworkManagement::WindowsFilteringPlatform::FWP_CONDITION_VALUE0_0 {
                    sid: sid.as_psid() as *mut windows_sys::Win32::Security::SID,
                },
            };

            let mut conditions = vec![container_condition];
            let (name, action, weight) = match plan {
                FilterPlan::BlockAll { weight } => {
                    ("klaus sandbox: block outbound", FWP_ACTION_BLOCK, *weight)
                }
                FilterPlan::PermitLoopbackPort { port, weight } => {
                    let mut port_condition: FWPM_FILTER_CONDITION0 = unsafe { std::mem::zeroed() };
                    port_condition.fieldKey = FWPM_CONDITION_IP_REMOTE_PORT;
                    port_condition.matchType = FWP_MATCH_EQUAL;
                    port_condition.conditionValue = FWP_CONDITION_VALUE0 {
                        r#type: FWP_UINT16,
                        Anonymous: windows_sys::Win32::NetworkManagement::WindowsFilteringPlatform::FWP_CONDITION_VALUE0_0 {
                            uint16: *port,
                        },
                    };
                    conditions.push(port_condition);

                    // Address as well as port: a port-only permit would let the
                    // container reach that port on any reachable host.
                    let mut address_condition: FWPM_FILTER_CONDITION0 =
                        unsafe { std::mem::zeroed() };
                    address_condition.fieldKey = FWPM_CONDITION_IP_REMOTE_ADDRESS;
                    address_condition.matchType = FWP_MATCH_EQUAL;
                    address_condition.conditionValue = FWP_CONDITION_VALUE0 {
                        r#type: FWP_UINT32,
                        Anonymous: windows_sys::Win32::NetworkManagement::WindowsFilteringPlatform::FWP_CONDITION_VALUE0_0 {
                            uint32: LOOPBACK_V4,
                        },
                    };
                    conditions.push(address_condition);
                    (
                        "klaus sandbox: permit loopback port",
                        FWP_ACTION_PERMIT,
                        *weight,
                    )
                }
            };

            let mut display_name = wide(name);
            let mut filter: FWPM_FILTER0 = unsafe { std::mem::zeroed() };
            filter.displayData = FWPM_DISPLAY_DATA0 {
                name: display_name.as_mut_ptr(),
                description: ptr::null_mut(),
            };
            filter.layerKey = FWPM_LAYER_ALE_AUTH_CONNECT_V4;
            filter.action = FWPM_ACTION0 {
                r#type: action,
                Anonymous: unsafe { std::mem::zeroed() },
            };
            filter.weight = FWP_VALUE0 {
                r#type: FWP_UINT64,
                Anonymous:
                    windows_sys::Win32::NetworkManagement::WindowsFilteringPlatform::FWP_VALUE0_0 {
                        uint64: &weight as *const u64 as *mut u64,
                    },
            };
            filter.numFilterConditions = conditions.len() as u32;
            filter.filterCondition = conditions.as_mut_ptr();

            let code = unsafe {
                FwpmFilterAdd0(
                    self.engine,
                    &filter,
                    // No security descriptor: the filter inherits the engine
                    // session's own, which is this process. `PSECURITY_DESCRIPTOR`
                    // is already a pointer type, so "none" is a null of that type
                    // rather than a null pointer to one.
                    ptr::null_mut(),
                    ptr::null_mut(),
                )
            };
            if code != ERROR_SUCCESS {
                return Err(FilterError::FilterAdd {
                    what: name.to_string(),
                    code,
                });
            }
            Ok(())
        }
    }
}

#[cfg(windows)]
pub use imp::{FilterError, FilterSession};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn denied_installs_no_filters_because_the_token_already_refuses() {
        // A filter here would suggest the boundary depends on it holding, which is
        // the opposite of how Denied is enforced.
        assert!(plan_filters(&NetworkBoundary::Denied).is_empty());
    }

    #[test]
    fn allowed_installs_no_filters() {
        assert!(plan_filters(&NetworkBoundary::Allowed).is_empty());
    }

    #[test]
    fn proxy_mode_blocks_everything_and_permits_each_port() {
        let boundary = NetworkBoundary::LoopbackPorts {
            ports: vec![8080, 8443],
            allow_local_binding: false,
        };
        let plans = plan_filters(&boundary);
        assert_eq!(
            plans,
            vec![
                FilterPlan::BlockAll {
                    weight: BLOCK_WEIGHT
                },
                FilterPlan::PermitLoopbackPort {
                    port: 8080,
                    weight: PERMIT_WEIGHT
                },
                FilterPlan::PermitLoopbackPort {
                    port: 8443,
                    weight: PERMIT_WEIGHT
                },
            ]
        );
    }

    #[test]
    fn every_permit_outweighs_the_block() {
        // Reversed weights make the block win and the proxy unreachable, which is
        // the failure this pins.
        let boundary = NetworkBoundary::LoopbackPorts {
            ports: vec![1, 65535],
            allow_local_binding: true,
        };
        let plans = plan_filters(&boundary);
        let block_weight = plans
            .iter()
            .find_map(|plan| match plan {
                FilterPlan::BlockAll { weight } => Some(*weight),
                _ => None,
            })
            .expect("a block is planned");
        for plan in &plans {
            if let FilterPlan::PermitLoopbackPort { weight, port } = plan {
                assert!(
                    *weight > block_weight,
                    "permit for port {port} must outrank the block"
                );
            }
        }
    }

    #[test]
    fn the_block_is_never_weightless() {
        // Weight zero is also what an unspecified weight gets, so the outcome would
        // depend on insertion order instead of intent. Checked at compile time, since
        // both sides are constants.
        const { assert!(BLOCK_WEIGHT > 0) };
    }

    #[test]
    fn a_repeated_port_is_planned_once() {
        let boundary = NetworkBoundary::LoopbackPorts {
            ports: vec![8080, 8080, 8443, 8080],
            allow_local_binding: false,
        };
        let plans = plan_filters(&boundary);
        let permits = plans
            .iter()
            .filter(|plan| matches!(plan, FilterPlan::PermitLoopbackPort { .. }))
            .count();
        assert_eq!(permits, 2);
    }

    #[test]
    fn an_empty_port_list_still_blocks() {
        // Proxy mode with no ports means "no outbound at all", and the block is what
        // enforces it. Producing an empty plan would leave the container's client
        // capability unrestricted.
        let boundary = NetworkBoundary::LoopbackPorts {
            ports: Vec::new(),
            allow_local_binding: false,
        };
        assert_eq!(
            plan_filters(&boundary),
            vec![FilterPlan::BlockAll {
                weight: BLOCK_WEIGHT
            }]
        );
    }

    #[test]
    fn loopback_is_expressed_in_the_byte_order_wfp_reads() {
        // 0x7F000001 is 127.0.0.1 in host order. The network-order value
        // (0x0100007F) is 1.0.0.127, a filter that permits nothing useful.
        assert_eq!(LOOPBACK_V4, 0x7F00_0001);
        assert_eq!(LOOPBACK_V4.to_be_bytes(), [127, 0, 0, 1]);
    }
}
