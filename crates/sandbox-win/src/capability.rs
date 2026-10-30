//! Capabilities: the network boundary's first and strongest layer.
//!
//! An AppContainer reaches the network only through a capability its token holds.
//! Withholding `internetClient` does not filter traffic, it removes the ability to
//! open a socket to a non-local address at all, enforced by the kernel before any
//! filter engine is consulted. So [`NetworkBoundary::Denied`] needs no rules and
//! nothing to clean up, and it cannot be defeated by a rule that failed to install.
//!
//! Two facts about AppContainers shape everything below.
//!
//! **Loopback is blocked by default, separately from capabilities.** Windows stops
//! an AppContainer from connecting to 127.0.0.1 even when it holds every network
//! capability, because that path is how sandboxed apps would otherwise probe each
//! other. Klaus's proxy mode is *entirely* loopback traffic, so it needs an
//! explicit exemption owned by [`crate::loopback::LoopbackLease`]. Nothing else
//! in the boundary matters if this step is skipped: the process simply has no
//! network at all and the failure looks like a broken proxy.
//!
//! **The exemption list is machine-global and it is a whole-list write.** The API
//! takes the complete set of exempt containers, not a delta, so a naive "add mine"
//! implementation that passes a one-element array silently removes every other
//! exemption on the machine, including ones other software depends on. Every write
//! belongs to an explicit lease that merges under the shared Klaus writer lock.

use crate::boundary::NetworkBoundary;
use crate::protocol::SandboxRequest;

/// Why a capability could not be established.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum CapabilityError {
    #[error("capability SID {sid} could not be parsed: Win32 error {code}")]
    ParseSid { sid: &'static str, code: u32 },
    #[error("the route capability SID is not a canonical custom capability SID")]
    InvalidRouteCapabilitySid,
    #[error("a route capability requires a nonempty loopback-port network boundary")]
    RouteCapabilityWithoutManagedNetwork,
    #[error("the route capability name is empty, contains NUL, or exceeds 1024 UTF-16 units")]
    InvalidRouteCapabilityName,
    #[error("deriving a route capability failed: Win32 error {0}")]
    DeriveRouteCapability(u32),
    #[error("deriving a route capability did not return exactly one capability SID")]
    UnexpectedRouteCapabilityCount,
    #[error("converting a route capability SID failed: Win32 error {0}")]
    ConvertRouteCapability(u32),
}

/// Validate the canonical custom-capability family returned by Windows.
///
/// Built-in capabilities, package SIDs, capability *group* SIDs, and AppSilo
/// identities are deliberately excluded. This validates shape, not provenance:
/// the trusted route owner must derive and retain the session's capability. A SID
/// is public identity data and cannot authenticate a full-trust local process.
pub fn validate_route_capability_sid(sid: &str) -> Result<(), CapabilityError> {
    let Some(hash) = sid.strip_prefix("S-1-15-3-1024-") else {
        return Err(CapabilityError::InvalidRouteCapabilitySid);
    };
    let mut components = hash.split('-');
    for _ in 0..8 {
        let Some(component) = components.next() else {
            return Err(CapabilityError::InvalidRouteCapabilitySid);
        };
        if component.is_empty()
            || !component.bytes().all(|byte| byte.is_ascii_digit())
            || component
                .parse::<u32>()
                .ok()
                .map(|value| value.to_string())
                .as_deref()
                != Some(component)
        {
            return Err(CapabilityError::InvalidRouteCapabilitySid);
        }
    }
    if components.next().is_some() {
        return Err(CapabilityError::InvalidRouteCapabilitySid);
    }
    Ok(())
}

/// Validate the trusted broker extension before any native setup is attempted.
///
/// The private request carries no proof that a broker is live. Its owner supplies
/// this field only for a registered route; an arbitrary SID or a boundary with no
/// managed loopback endpoint must not introduce another launch capability.
pub fn validate_request_capabilities(request: &SandboxRequest) -> Result<(), CapabilityError> {
    let Some(sid) = request.network_route_capability_sid.as_deref() else {
        return Ok(());
    };
    validate_route_capability_sid(sid)?;
    match &request.network {
        NetworkBoundary::LoopbackPorts { ports, .. }
            if !ports.is_empty() && !ports.contains(&0) =>
        {
            Ok(())
        }
        _ => Err(CapabilityError::RouteCapabilityWithoutManagedNetwork),
    }
}

/// A capability an AppContainer token can hold.
///
/// Only the network ones are modelled: those are the capabilities that change what
/// a sandboxed command can reach, and handing out anything else would widen the
/// sandbox for no stated reason.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Capability {
    /// Outbound connections to non-local addresses, client side only.
    InternetClient,
    /// Outbound and inbound on public networks.
    InternetClientServer,
    /// Outbound and inbound on networks the machine considers private.
    PrivateNetworkClientServer,
}

impl Capability {
    /// The well-known SID string Windows derives the capability SID from.
    pub fn sid_string(self) -> &'static str {
        // These are the documented capability SIDs, stable across Windows versions.
        match self {
            Self::InternetClient => "S-1-15-3-1",
            Self::InternetClientServer => "S-1-15-3-2",
            Self::PrivateNetworkClientServer => "S-1-15-3-3",
        }
    }
}

/// Which capabilities a boundary calls for.
///
/// Returning an empty set is the point of this function existing: it is what makes
/// "no network" a property of the token rather than a rule that has to hold.
pub fn capabilities_for(boundary: &NetworkBoundary) -> Vec<Capability> {
    match boundary {
        // Nothing. The kernel refuses the socket; no filter has to be right.
        NetworkBoundary::Denied => Vec::new(),
        NetworkBoundary::Allowed => vec![
            Capability::InternetClient,
            Capability::InternetClientServer,
            Capability::PrivateNetworkClientServer,
        ],
        // A proxy on loopback still needs a network capability: the loopback
        // exemption lifts the AppContainer-specific block, but the token must also
        // be allowed to create client sockets in the first place. Server-side and
        // private-network capabilities stay withheld unless the policy asked for
        // local binding, so a command that only talks to the proxy cannot listen.
        NetworkBoundary::LoopbackPorts {
            allow_local_binding,
            ..
        } => {
            if *allow_local_binding {
                vec![Capability::InternetClient, Capability::InternetClientServer]
            } else {
                vec![Capability::InternetClient]
            }
        }
    }
}

/// Whether a boundary needs the loopback exemption installed.
///
/// A denied network never receives an exemption. Both unrestricted networking
/// and an explicit local-proxy policy include loopback access.
pub fn needs_loopback_exemption(boundary: &NetworkBoundary) -> bool {
    !matches!(boundary, NetworkBoundary::Denied)
}

#[cfg(windows)]
mod imp {
    use std::ptr;

    use windows_sys::Win32::Foundation::{LocalFree, FALSE, HLOCAL, PSID};
    use windows_sys::Win32::Security::Authorization::{
        ConvertSidToStringSidW, ConvertStringSidToSidW,
    };
    use windows_sys::Win32::Security::DeriveCapabilitySidsFromName;

    use super::{
        capabilities_for, validate_request_capabilities, validate_route_capability_sid, Capability,
        CapabilityError,
    };
    use crate::protocol::SandboxRequest;

    /// A SID parsed from its string form, freed on drop.
    pub struct CapabilitySid {
        raw: PSID,
    }

    impl CapabilitySid {
        pub fn as_psid(&self) -> PSID {
            self.raw
        }
    }

    impl Drop for CapabilitySid {
        fn drop(&mut self) {
            if !self.raw.is_null() {
                unsafe { LocalFree(self.raw as HLOCAL) };
            }
        }
    }

    unsafe impl Send for CapabilitySid {}
    unsafe impl Sync for CapabilitySid {}

    fn wide(text: &str) -> Vec<u16> {
        text.encode_utf16().chain(std::iter::once(0)).collect()
    }

    // DeriveCapabilitySidsFromName allocates each SID and each of its two arrays
    // with LocalAlloc. FreeSid belongs to AppContainer profile APIs, not this API.
    #[derive(Default)]
    struct DerivedSids {
        groups: *mut PSID,
        group_count: u32,
        capabilities: *mut PSID,
        capability_count: u32,
    }

    impl Drop for DerivedSids {
        fn drop(&mut self) {
            for (array, count) in [
                (self.groups, self.group_count),
                (self.capabilities, self.capability_count),
            ] {
                if !array.is_null() {
                    for index in 0..count as usize {
                        let sid = unsafe { *array.add(index) };
                        if !sid.is_null() {
                            unsafe { LocalFree(sid as HLOCAL) };
                        }
                    }
                    unsafe { LocalFree(array as HLOCAL) };
                }
            }
        }
    }

    /// Derive one session-owned custom capability without creating a profile.
    ///
    /// Use CapabilitySids, never CapabilityGroupSids. Validate the OS result too:
    /// well-known names can derive built-in SIDs which are not route identities.
    pub fn derive_route_capability_sid(name: &str) -> Result<String, CapabilityError> {
        if name.is_empty() || name.contains('\0') || name.encode_utf16().count() > 1024 {
            return Err(CapabilityError::InvalidRouteCapabilityName);
        }
        let name = wide(name);
        let mut derived = DerivedSids::default();
        let ok = unsafe {
            DeriveCapabilitySidsFromName(
                name.as_ptr(),
                &mut derived.groups,
                &mut derived.group_count,
                &mut derived.capabilities,
                &mut derived.capability_count,
            )
        };
        if ok == FALSE {
            return Err(CapabilityError::DeriveRouteCapability(unsafe {
                windows_sys::Win32::Foundation::GetLastError()
            }));
        }
        if derived.capability_count != 1 || derived.capabilities.is_null() {
            return Err(CapabilityError::UnexpectedRouteCapabilityCount);
        }
        let sid = unsafe { *derived.capabilities };
        if sid.is_null() {
            return Err(CapabilityError::UnexpectedRouteCapabilityCount);
        }
        let mut text = ptr::null_mut();
        if unsafe { ConvertSidToStringSidW(sid, &mut text) } == FALSE {
            return Err(CapabilityError::ConvertRouteCapability(unsafe {
                windows_sys::Win32::Foundation::GetLastError()
            }));
        }
        let value = unsafe {
            let mut length = 0;
            while *text.add(length) != 0 {
                length += 1;
            }
            String::from_utf16_lossy(std::slice::from_raw_parts(text, length))
        };
        unsafe { LocalFree(text as HLOCAL) };
        validate_route_capability_sid(&value)?;
        Ok(value)
    }

    /// Resolve the exact capability set shared by launch and target AccessCheck.
    pub fn resolve_for_request(
        request: &SandboxRequest,
    ) -> Result<Vec<CapabilitySid>, CapabilityError> {
        validate_request_capabilities(request)?;
        let mut capabilities = resolve(&capabilities_for(&request.network))?;
        if let Some(sid) = &request.network_route_capability_sid {
            let text = wide(sid);
            let mut raw = ptr::null_mut();
            if unsafe { ConvertStringSidToSidW(text.as_ptr(), &mut raw) } == FALSE || raw.is_null()
            {
                return Err(CapabilityError::ConvertRouteCapability(unsafe {
                    windows_sys::Win32::Foundation::GetLastError()
                }));
            }
            capabilities.push(CapabilitySid { raw });
        }
        Ok(capabilities)
    }

    /// Resolve capability SIDs for a process launch.
    ///
    /// The returned SIDs must outlive the `SECURITY_CAPABILITIES` that points at
    /// them, which is why they are handed back as owned values rather than being
    /// built inside the launch call.
    pub fn resolve(capabilities: &[Capability]) -> Result<Vec<CapabilitySid>, CapabilityError> {
        capabilities
            .iter()
            .map(|capability| {
                let text = wide(capability.sid_string());
                let mut raw: PSID = ptr::null_mut();
                let ok = unsafe { ConvertStringSidToSidW(text.as_ptr(), &mut raw) };
                if ok == FALSE || raw.is_null() {
                    return Err(CapabilityError::ParseSid {
                        sid: capability.sid_string(),
                        code: unsafe { windows_sys::Win32::Foundation::GetLastError() },
                    });
                }
                Ok(CapabilitySid { raw })
            })
            .collect()
    }
}

#[cfg(windows)]
pub use imp::{derive_route_capability_sid, resolve, resolve_for_request, CapabilitySid};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn denied_network_holds_no_capability_at_all() {
        // The strongest form of "no network": the token cannot open the socket, so
        // there is no rule that could fail open.
        assert!(capabilities_for(&NetworkBoundary::Denied).is_empty());
        assert!(!needs_loopback_exemption(&NetworkBoundary::Denied));
    }

    #[test]
    fn allowed_network_holds_the_three_network_capabilities() {
        let mut capabilities = capabilities_for(&NetworkBoundary::Allowed);
        capabilities.sort();
        assert_eq!(
            capabilities,
            vec![
                Capability::InternetClient,
                Capability::InternetClientServer,
                Capability::PrivateNetworkClientServer,
            ]
        );
        assert!(needs_loopback_exemption(&NetworkBoundary::Allowed));
    }

    #[test]
    fn proxy_mode_gets_a_client_capability_and_the_loopback_exemption() {
        // Both are required: the capability lets it open a socket, the exemption
        // lets that socket reach 127.0.0.1. Missing either one means no proxy.
        let boundary = NetworkBoundary::LoopbackPorts {
            ports: vec![8080],
            allow_local_binding: false,
        };
        assert_eq!(
            capabilities_for(&boundary),
            vec![Capability::InternetClient]
        );
        assert!(needs_loopback_exemption(&boundary));
    }

    #[test]
    fn proxy_mode_without_local_binding_cannot_listen() {
        // A command that only talks to the proxy must not be able to accept
        // connections; the server-side capability is what would allow that.
        let boundary = NetworkBoundary::LoopbackPorts {
            ports: vec![8080],
            allow_local_binding: false,
        };
        let capabilities = capabilities_for(&boundary);
        assert!(!capabilities.contains(&Capability::InternetClientServer));
        assert!(!capabilities.contains(&Capability::PrivateNetworkClientServer));
    }

    #[test]
    fn local_binding_adds_the_server_capability_and_nothing_wider() {
        let boundary = NetworkBoundary::LoopbackPorts {
            ports: vec![8080],
            allow_local_binding: true,
        };
        let mut capabilities = capabilities_for(&boundary);
        capabilities.sort();
        assert_eq!(
            capabilities,
            vec![Capability::InternetClient, Capability::InternetClientServer]
        );
        // Private-network access is a different reach and is not implied by
        // "may listen on loopback".
        assert!(!capabilities.contains(&Capability::PrivateNetworkClientServer));
    }

    #[test]
    fn capability_sid_strings_are_the_documented_well_known_values() {
        assert_eq!(Capability::InternetClient.sid_string(), "S-1-15-3-1");
        assert_eq!(Capability::InternetClientServer.sid_string(), "S-1-15-3-2");
        assert_eq!(
            Capability::PrivateNetworkClientServer.sid_string(),
            "S-1-15-3-3"
        );
    }
}
