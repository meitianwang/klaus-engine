//! Defect: workspace package identity could not represent a session network route
//! independently of concurrent executions' filesystem identities.
//! Root cause: the wrapper request carried only a package moniker and built-in
//! network capabilities, while attribution combined distinct SID sources.
//! Regression for the 2026-09-09 Windows session network-capability work item.
//!
//! Exercise public request and capability APIs. Derivation and SID resolution
//! allocate caller-owned data only: no profile, ACL, exemption, filter, or process
//! is created. A SID's canonical shape does not establish trusted provenance.

#![cfg(windows)]

use klaus_sandbox_win::capability::{
    derive_route_capability_sid, resolve_for_request, validate_request_capabilities,
    validate_route_capability_sid, CapabilityError,
};
use klaus_sandbox_win::{AbsPath, FilesystemBoundary, NetworkBoundary, SandboxRequest};
use windows_sys::Win32::Foundation::LocalFree;
use windows_sys::Win32::Security::Authorization::ConvertSidToStringSidW;

const CUSTOM: &str = "S-1-15-3-1024-0-1-2-3-4-5-6-4294967295";

fn request(network: NetworkBoundary) -> SandboxRequest {
    SandboxRequest {
        command: vec!["cmd.exe".into(), "/c".into(), "exit 0".into()],
        cwd: AbsPath::new("C:\\fixture").expect("absolute fixture path"),
        env: Default::default(),
        filesystem: FilesystemBoundary::default(),
        network,
        network_route_capability_sid: None,
        container_name: "klaus-route-capability-fixture".into(),
        terminal: None,
    }
}

fn loopback() -> NetworkBoundary {
    NetworkBoundary::LoopbackPorts {
        ports: vec![29433],
        allow_local_binding: false,
    }
}

#[test]
fn private_request_preserves_route_identity_and_accepts_legacy_omission() {
    let mut original = request(loopback());
    let legacy = serde_json::to_value(&original).expect("serialize legacy request");
    assert!(legacy.get("networkRouteCapabilitySid").is_none());
    assert_eq!(
        serde_json::from_value::<SandboxRequest>(legacy).expect("decode legacy request"),
        original,
    );
    original.network_route_capability_sid = Some(CUSTOM.into());
    let encoded = serde_json::to_value(&original).expect("serialize route request");
    assert_eq!(encoded["networkRouteCapabilitySid"], CUSTOM);
    let decoded: SandboxRequest = serde_json::from_value(encoded).expect("decode route request");
    assert_eq!(decoded, original);
    validate_request_capabilities(&decoded).expect("valid private broker request");
}

#[test]
fn route_identity_rejects_builtin_package_group_and_noncanonical_sid_sources() {
    validate_route_capability_sid(CUSTOM).expect("canonical custom capability");
    for invalid in [
        "S-1-15-3-1",                      // Built-in internetClient.
        "S-1-15-3-2",                      // Built-in internetClientServer.
        "S-1-15-2-0-1-2-3-4-5-6",          // Package SID.
        "S-1-5-32-0-1-2-3-4-5-6-7",        // Capability-group authority.
        "S-1-5-18",                        // LocalSystem.
        "S-1-15-3-1025-0-1-2-3-4-5-6-7",   // Another capability family.
        "S-1-15-3-1024-0-1-2-3-4-5-6",     // Truncated hash.
        "S-1-15-3-1024-0-1-2-3-4-5-6-7-8", // Extra component.
        "S-1-15-3-1024-00-1-2-3-4-5-6-7",
        "S-1-15-3-1024-+0-1-2-3-4-5-6-7",
        "S-1-15-3-1024-0-1-2-3-4-5-6-4294967296",
        "S-1-15-3-1024-0-1-2-3-4-5-6-7\0",
        "s-1-15-3-1024-0-1-2-3-4-5-6-7",
    ] {
        assert_eq!(
            validate_route_capability_sid(invalid),
            Err(CapabilityError::InvalidRouteCapabilitySid),
            "accepted invalid route identity: {invalid:?}",
        );
        let mut request = request(loopback());
        request.network_route_capability_sid = Some(invalid.into());
        assert!(matches!(
            resolve_for_request(&request),
            Err(CapabilityError::InvalidRouteCapabilitySid),
        ));
    }
}

#[test]
fn route_capability_cannot_widen_an_unmanaged_network_boundary() {
    for boundary in [
        NetworkBoundary::Denied,
        NetworkBoundary::Allowed,
        NetworkBoundary::LoopbackPorts {
            ports: vec![],
            allow_local_binding: false,
        },
        NetworkBoundary::LoopbackPorts {
            ports: vec![29433, 0],
            allow_local_binding: true,
        },
    ] {
        let mut request = request(boundary);
        request.network_route_capability_sid = Some(CUSTOM.into());
        assert_eq!(
            validate_request_capabilities(&request),
            Err(CapabilityError::RouteCapabilityWithoutManagedNetwork),
        );
        assert!(matches!(
            resolve_for_request(&request),
            Err(CapabilityError::RouteCapabilityWithoutManagedNetwork),
        ));
    }
    let denied = request(NetworkBoundary::Denied);
    assert!(resolve_for_request(&denied)
        .expect("legacy denied boundary")
        .is_empty());
}

#[test]
fn windows_derivation_returns_the_custom_capability_not_its_service_group() {
    let first_name = format!("klaus-route-regression-{}", uuid::Uuid::new_v4());
    let first = derive_route_capability_sid(&first_name).expect("derive first capability");
    let second = derive_route_capability_sid(&format!("{first_name}-second"))
        .expect("derive independent capability");
    validate_route_capability_sid(&first).expect("custom application capability");
    validate_route_capability_sid(&second).expect("independent custom application capability");
    assert_ne!(first, second);
    assert_eq!(
        derive_route_capability_sid(&first_name).expect("repeat derivation"),
        first,
    );
    assert_eq!(
        derive_route_capability_sid("internetClient"),
        Err(CapabilityError::InvalidRouteCapabilitySid),
    );
    for invalid in ["", "route\0suffix"] {
        assert_eq!(
            derive_route_capability_sid(invalid),
            Err(CapabilityError::InvalidRouteCapabilityName),
        );
    }

    let mut request = request(loopback());
    request.network_route_capability_sid = Some(first.clone());
    let capabilities = resolve_for_request(&request).expect("resolve launch capabilities");
    let mut resolved = capabilities
        .iter()
        .map(|sid| {
            let mut text = std::ptr::null_mut();
            assert_ne!(
                unsafe { ConvertSidToStringSidW(sid.as_psid(), &mut text) },
                0
            );
            let value = unsafe {
                let mut len = 0;
                while *text.add(len) != 0 {
                    len += 1;
                }
                String::from_utf16_lossy(std::slice::from_raw_parts(text, len))
            };
            unsafe { LocalFree(text.cast()) };
            value
        })
        .collect::<Vec<_>>();
    resolved.sort();
    let mut expected = vec!["S-1-15-3-1".to_string(), first];
    expected.sort();
    assert_eq!(
        resolved, expected,
        "grant only internetClient and the route capability"
    );
}
