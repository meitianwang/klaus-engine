//! Read-only inspection of the resources used by Windows sandbox preparation.
//!
//! This is a snapshot of discretionary access, not a successful preparation or
//! launch. It derives the identity without registering a profile, creates only
//! query tokens, and never writes an ACL, exemption, or WFP object. Commands and
//! environment values are deliberately absent from the report.

use serde::{Deserialize, Serialize};

use crate::{acl, capability, identity, SandboxRequest};

const WRITE_DAC: u32 = 0x0004_0000;
const NULL_READ_WRITE: u32 = 0x0012_019f;
const NULL_OBJECT: &str = r"\\?\GLOBALROOT\Device\Null";

/// Internal entry point of the engine's bounded, read-only inspection process.
pub const INSPECTION_ARG: &str = "--windows-sandbox-inspect";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum AccessState {
    Allowed,
    Denied,
    Unavailable,
    NotRequired,
}

/// A query error is not evidence of denied access.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InspectionIssue {
    pub operation: String,
    pub code_domain: Option<String>,
    pub code: Option<i64>,
    pub message: String,
}

impl InspectionIssue {
    fn explanation(operation: &str, message: impl Into<String>) -> Self {
        Self {
            operation: operation.to_owned(),
            code_domain: None,
            code: None,
            message: message.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AccessCheck {
    pub state: AccessState,
    pub desired_access: u32,
    /// The OS result for this whole request, not a partial-rights enumeration.
    pub granted_access: Option<u32>,
    pub issue: Option<InspectionIssue>,
}

impl AccessCheck {
    fn unavailable(desired_access: u32, issue: InspectionIssue) -> Self {
        Self {
            state: AccessState::Unavailable,
            desired_access,
            granted_access: None,
            issue: Some(issue),
        }
    }

    fn not_required(desired_access: u32) -> Self {
        Self {
            state: AccessState::NotRequired,
            desired_access,
            granted_access: None,
            issue: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResourceAccess {
    pub object: String,
    pub target_access: AccessCheck,
    /// A DACL check for WRITE_DAC using the ordinary host token. No writable
    /// handle is requested, and this does not guarantee a future grant succeeds.
    pub host_can_grant: AccessCheck,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct IdentityInspection {
    pub moniker: Option<String>,
    pub sid: Option<String>,
    pub issue: Option<InspectionIssue>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LoopbackInspection {
    pub required: bool,
    /// None means unqueried/unavailable, not absent. False says nothing about
    /// the caller's authority to change the global exemption list.
    pub present: Option<bool>,
    pub issue: Option<InspectionIssue>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WfpAccess {
    pub object: String,
    /// V6 is observed but not required by the current V4-only filter installer.
    pub required: bool,
    pub access: AccessCheck,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WfpInspection {
    pub required: bool,
    pub checks: Vec<WfpAccess>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PreparationReport {
    pub schema_version: u32,
    pub read_only: bool,
    pub identity: IdentityInspection,
    pub capabilities: Vec<String>,
    pub walk: Vec<ResourceAccess>,
    pub null_device: ResourceAccess,
    pub loopback: LoopbackInspection,
    pub wfp: WfpInspection,
    pub limitations: Vec<String>,
}

/// Inspect the exact request that will be handed to the wrapper.
///
/// Async callers use an owned inspection process so cancellation can wait for
/// native reads to terminate. This consumes no launch token and starts no target
/// command. All types are available on every platform;
/// a non-Windows caller receives unavailable checks, never a synthetic pass.
pub fn inspect(request: &SandboxRequest) -> PreparationReport {
    #[cfg(windows)]
    {
        imp::inspect(request)
    }
    #[cfg(not(windows))]
    {
        initial_report(
            request,
            InspectionIssue::explanation(
                "platform",
                "Windows security APIs are unavailable on this platform",
            ),
        )
    }
}

/// Native token preparation happens in the selected wrapper before target launch.
/// AppContainer-specific DACL, package, exemption, and filter checks do not apply.
pub fn inspect_native(
    envelope: &crate::protocol::NativeSandboxRequest,
) -> Result<PreparationReport, String> {
    let Some(context) = &envelope.native else {
        return Ok(inspect(&envelope.request));
    };
    context.validate()?;
    if context.level == crate::protocol::NativeSandboxLevel::Unelevated {
        crate::unelevated::validate(&envelope.request)?;
    }
    let not_required = |object: &str, desired| ResourceAccess {
        object: object.into(),
        target_access: AccessCheck::not_required(desired),
        host_can_grant: AccessCheck::not_required(WRITE_DAC),
    };
    Ok(PreparationReport {
        schema_version: 1, read_only: true,
        identity: IdentityInspection { moniker: None, sid: None, issue: None },
        capabilities: Vec::new(), walk: Vec::new(), null_device: not_required(NULL_OBJECT, NULL_READ_WRITE),
        loopback: LoopbackInspection { required: false, present: None, issue: None },
        wfp: WfpInspection { required: false, checks: Vec::new() },
        limitations: match context.level {
            crate::protocol::NativeSandboxLevel::Unelevated => vec![
                "Native token and write ACL preparation runs in the wrapper before the target starts; this read-only inspection does not establish successful preparation.".into(),
                "The unelevated token preserves the caller's ordinary reads. Network restrictions use proxy and offline environment variables and do not provide a kernel network boundary.".into(),
            ],
            crate::protocol::NativeSandboxLevel::Elevated => vec![
                "Dedicated native identity, setup, and permission preparation runs in the elevated backend before target launch; this read-only inspection does not establish successful preparation.".into(),
            ],
        },
    })
}

/// Shared read-only exemption reader. Native allocation ownership stays in one
/// implementation even when a preparation writer later consumes this snapshot.
#[cfg(windows)]
pub(crate) fn read_loopback_sids() -> Result<Vec<String>, InspectionIssue> {
    imp::read_loopback_sids()
}

fn initial_report(request: &SandboxRequest, issue: InspectionIssue) -> PreparationReport {
    let identity = match identity::container_moniker(request) {
        Ok(moniker) => IdentityInspection {
            moniker: Some(moniker),
            sid: None,
            issue: Some(issue.clone()),
        },
        Err(error) => IdentityInspection {
            moniker: None,
            sid: None,
            issue: Some(InspectionIssue::explanation(
                "container moniker",
                error.to_string(),
            )),
        },
    };
    let resource = |object: String, mask| ResourceAccess {
        object,
        target_access: AccessCheck::unavailable(mask, issue.clone()),
        host_can_grant: AccessCheck::unavailable(WRITE_DAC, issue.clone()),
    };
    let walk = acl::plan_entries(&request.filesystem)
        .into_iter()
        .filter_map(|rule| match rule {
            acl::Rule::Allow {
                path,
                grant: acl::Grant::Walk,
                inheritable: false,
            } => Some(resource(path.as_str(), acl::access_mask(acl::Grant::Walk))),
            _ => None,
        })
        .collect();
    let network_required = capability::needs_loopback_exemption(&request.network);
    // WFP masks are specific rights, not Win32 GENERIC_* masks. The descriptor
    // reader below uses Microsoft's separate FWPM_GENERIC_* mapping.
    let checks = [
        ("engine", 0x40, true),
        ("filterContainer", 0x01, true),
        ("aleAuthConnectV4", 0x02, true),
        ("aleAuthConnectV6", 0x02, false),
        ("universalSubLayer", 0x02, true),
    ]
    .into_iter()
    .map(|(object, mask, required)| WfpAccess {
        object: object.to_owned(),
        required: network_required && required,
        access: if network_required {
            AccessCheck::unavailable(mask, issue.clone())
        } else {
            AccessCheck::not_required(mask)
        },
    })
    .collect();
    PreparationReport {
        schema_version: 1,
        read_only: true,
        identity,
        capabilities: capability::capabilities_for(&request.network)
            .into_iter()
            .map(|capability| capability.sid_string().to_owned())
            .chain(request.network_route_capability_sid.iter().cloned())
            .collect(),
        walk,
        null_device: resource(NULL_OBJECT.to_owned(), NULL_READ_WRITE),
        loopback: LoopbackInspection {
            required: network_required,
            present: None,
            issue: network_required.then_some(issue),
        },
        wfp: WfpInspection { required: network_required, checks },
        limitations: [
            "DACL snapshots do not establish mandatory-integrity, sharing, or future object-open success; paths and security may change after inspection.",
            "Only planned noninheriting ancestor Walk grants are inspected; this is not a check of every filesystem grant or withhold.",
            "The AppContainer SID is derived without creating or registering its profile.",
            "The current NUL grant still requests host WRITE_DAC even when the target already has read/write access; hostCanGrant checks WRITE_DAC only, not the additional GENERIC_WRITE requested by that open.",
            "Loopback absence or an unavailable query does not establish authority to update the exemption list.",
            "WFP access is queried, never tested by installing filters; the current installer uses V4 only, so V6 access is informational, not IPv6 enforcement.",
        ]
        .map(str::to_owned)
        .to_vec(),
    }
}

#[cfg(windows)]
mod imp {
    use std::{ptr, slice};

    use windows_sys::Win32::Foundation::{
        CloseHandle, GetLastError, LocalFree, ERROR_SUCCESS, FALSE, HANDLE, HLOCAL,
        INVALID_HANDLE_VALUE, PSID,
    };
    use windows_sys::Win32::NetworkManagement::WindowsFilteringPlatform::{
        FwpmEngineClose0, FwpmEngineGetSecurityInfo0, FwpmEngineOpen0,
        FwpmFilterGetSecurityInfoByKey0, FwpmFreeMemory0, FwpmLayerGetSecurityInfoByKey0,
        FwpmSubLayerGetSecurityInfoByKey0, FWPM_ACTRL_ADD, FWPM_ACTRL_ADD_LINK,
        FWPM_ACTRL_BEGIN_READ_TXN, FWPM_ACTRL_BEGIN_WRITE_TXN, FWPM_ACTRL_CLASSIFY,
        FWPM_ACTRL_ENUM, FWPM_ACTRL_OPEN, FWPM_ACTRL_READ, FWPM_ACTRL_READ_STATS,
        FWPM_ACTRL_SUBSCRIBE, FWPM_ACTRL_WRITE, FWPM_LAYER_ALE_AUTH_CONNECT_V4,
        FWPM_LAYER_ALE_AUTH_CONNECT_V6, FWPM_SUBLAYER_UNIVERSAL,
    };
    use windows_sys::Win32::Security::Authorization::{ConvertSidToStringSidW, SE_KERNEL_OBJECT};
    use windows_sys::Win32::Security::{
        IsValidSid, DACL_SECURITY_INFORMATION, GENERIC_MAPPING, GROUP_SECURITY_INFORMATION,
        OWNER_SECURITY_INFORMATION, PSECURITY_DESCRIPTOR,
    };
    use windows_sys::Win32::Storage::FileSystem::{
        CreateFileW, FILE_SHARE_READ, FILE_SHARE_WRITE, OPEN_EXISTING,
    };

    use super::*;
    use crate::access::{
        file_mapping, read_handle_descriptor, read_path_descriptor, AccessDecision, AccessError,
        Descriptor, HostAccess, TargetAccess,
    };

    fn native_issue(operation: &str, domain: &str, code: i64) -> InspectionIssue {
        InspectionIssue {
            operation: operation.to_owned(),
            code_domain: Some(domain.to_owned()),
            code: Some(code),
            message: format!("{operation} failed: {domain} {code:#x}"),
        }
    }

    fn access_issue(error: AccessError) -> InspectionIssue {
        match error {
            AccessError::Win32 { operation, code } => {
                native_issue(operation, "win32", i64::from(code))
            }
            AccessError::Read { path, code } => native_issue(
                &format!("reading security for {path}"),
                "win32",
                i64::from(code),
            ),
            AccessError::LowBox(code) => {
                native_issue("NtCreateLowBoxToken", "ntstatus", i64::from(code))
            }
            other => InspectionIssue::explanation("access check", other.to_string()),
        }
    }

    fn checked(mask: u32, result: Result<AccessDecision, InspectionIssue>) -> AccessCheck {
        match result {
            Ok(result) => AccessCheck {
                state: if result.allowed {
                    AccessState::Allowed
                } else {
                    AccessState::Denied
                },
                desired_access: mask,
                granted_access: Some(result.granted),
                issue: None,
            },
            Err(issue) => AccessCheck::unavailable(mask, issue),
        }
    }

    pub(super) fn inspect(request: &SandboxRequest) -> PreparationReport {
        let mut report = initial_report(
            request,
            InspectionIssue::explanation("inspection", "Query pending"),
        );
        let sid = report
            .identity
            .moniker
            .as_ref()
            .ok_or_else(|| {
                report
                    .identity
                    .issue
                    .clone()
                    .expect("invalid identity has an issue")
            })
            .and_then(|moniker| {
                identity::derive_sid(moniker).map_err(|error| {
                    InspectionIssue::explanation("derive AppContainer SID", error.to_string())
                })
            });
        match sid
            .as_ref()
            .map_err(Clone::clone)
            .and_then(|sid| sid_text(sid.as_psid()))
        {
            Ok(text) => {
                report.identity.sid = Some(text);
                report.identity.issue = None;
            }
            Err(issue) => report.identity.issue = Some(issue),
        }
        let target = sid.as_ref().map_err(Clone::clone).and_then(|sid| {
            let capabilities = capability::resolve_for_request(request).map_err(|error| {
                InspectionIssue::explanation("resolve capability SIDs", error.to_string())
            })?;
            TargetAccess::new(sid, &capabilities).map_err(access_issue)
        });
        let host = HostAccess::new().map_err(access_issue);
        for resource in &mut report.walk {
            let descriptor = read_path_descriptor(&resource.object).map_err(access_issue);
            check_resource(resource, descriptor, &target, &host);
        }
        check_resource(&mut report.null_device, null_descriptor(), &target, &host);
        if report.loopback.required {
            match super::read_loopback_sids().and_then(|entries| {
                let sid = report.identity.sid.as_ref().ok_or_else(|| {
                    report
                        .identity
                        .issue
                        .clone()
                        .expect("missing SID has an issue")
                })?;
                Ok(entries.iter().any(|entry| entry == sid))
            }) {
                Ok(present) => {
                    report.loopback.present = Some(present);
                    report.loopback.issue = None;
                }
                Err(issue) => report.loopback.issue = Some(issue),
            }
        }
        if report.wfp.required {
            inspect_wfp(&mut report.wfp.checks, &host);
        }
        report
    }

    fn check_resource(
        resource: &mut ResourceAccess,
        descriptor: Result<Descriptor, InspectionIssue>,
        target: &Result<TargetAccess, InspectionIssue>,
        host: &Result<HostAccess, InspectionIssue>,
    ) {
        let desired = resource.target_access.desired_access;
        let descriptor = match descriptor {
            Ok(descriptor) => descriptor,
            Err(issue) => {
                resource.target_access = AccessCheck::unavailable(desired, issue.clone());
                resource.host_can_grant = AccessCheck::unavailable(WRITE_DAC, issue);
                return;
            }
        };
        resource.target_access = checked(
            desired,
            target
                .as_ref()
                .map_err(Clone::clone)
                .and_then(|target| unsafe {
                    target
                        .check_descriptor(descriptor.0, desired)
                        .map_err(access_issue)
                }),
        );
        resource.host_can_grant = checked(
            WRITE_DAC,
            host.as_ref().map_err(Clone::clone).and_then(|host| unsafe {
                host.check_descriptor(descriptor.0, WRITE_DAC, file_mapping())
                    .map_err(access_issue)
            }),
        );
    }

    struct Device(HANDLE);

    impl Drop for Device {
        fn drop(&mut self) {
            unsafe { CloseHandle(self.0) };
        }
    }

    fn null_descriptor() -> Result<Descriptor, InspectionIssue> {
        let path: Vec<u16> = NULL_OBJECT.encode_utf16().chain(Some(0)).collect();
        // READ_CONTROL only: checking WRITE_DAC must not itself request it.
        let raw = unsafe {
            CreateFileW(
                path.as_ptr(),
                0x0002_0000,
                FILE_SHARE_READ | FILE_SHARE_WRITE,
                ptr::null(),
                OPEN_EXISTING,
                0,
                0,
            )
        };
        if raw == INVALID_HANDLE_VALUE {
            return Err(native_issue(
                "open fixed NUL with READ_CONTROL",
                "win32",
                i64::from(unsafe { GetLastError() }),
            ));
        }
        let device = Device(raw);
        read_handle_descriptor(device.0, SE_KERNEL_OBJECT).map_err(access_issue)
    }

    struct LocalString(*mut u16);

    impl Drop for LocalString {
        fn drop(&mut self) {
            if !self.0.is_null() {
                unsafe { LocalFree(self.0 as HLOCAL) };
            }
        }
    }

    fn sid_text(sid: PSID) -> Result<String, InspectionIssue> {
        if sid.is_null() || unsafe { IsValidSid(sid) } == FALSE {
            return Err(InspectionIssue::explanation(
                "SID reader",
                "Invalid SID returned by Windows",
            ));
        }
        let mut text = LocalString(ptr::null_mut());
        if unsafe { ConvertSidToStringSidW(sid, &mut text.0) } == FALSE {
            return Err(native_issue(
                "ConvertSidToStringSidW",
                "win32",
                i64::from(unsafe { GetLastError() }),
            ));
        }
        if text.0.is_null() {
            return Err(InspectionIssue::explanation(
                "SID reader",
                "Windows returned a null SID string",
            ));
        }
        // A valid SID has at most 15 subauthorities, each at most 10 digits.
        let length = (0..256)
            .find(|index| unsafe { *text.0.add(*index) == 0 })
            .ok_or_else(|| {
                InspectionIssue::explanation("SID reader", "SID string exceeds its limit")
            })?;
        String::from_utf16(unsafe { slice::from_raw_parts(text.0, length) })
            .map_err(|error| InspectionIssue::explanation("SID reader", error.to_string()))
    }

    pub(crate) fn read_loopback_sids() -> Result<Vec<String>, InspectionIssue> {
        crate::loopback::read_configuration()
            .map(|entries| entries.into_iter().map(|entry| entry.sid).collect())
            .map_err(|error| match error {
                crate::loopback::LoopbackError::Native { operation, code } => {
                    native_issue(operation, "win32", i64::from(code))
                }
                other => InspectionIssue::explanation("loopback reader", other.to_string()),
            })
    }

    struct WfpSession(HANDLE);

    impl Drop for WfpSession {
        fn drop(&mut self) {
            if self.0 != 0 {
                unsafe { FwpmEngineClose0(self.0) };
            }
        }
    }

    struct WfpDescriptor(PSECURITY_DESCRIPTOR);

    impl Drop for WfpDescriptor {
        fn drop(&mut self) {
            if !self.0.is_null() {
                // FwpmFreeMemory0 owns the whole descriptor; owner/group/ACL
                // pointers are interior addresses and must not be freed.
                unsafe { FwpmFreeMemory0(&mut self.0) };
            }
        }
    }

    /// The bindings omit these macro combinations. Use the published WFP
    /// mappings rather than the unrelated filesystem GENERIC_MAPPING:
    /// https://learn.microsoft.com/en-us/windows/win32/fwp/access-right-identifiers
    fn wfp_mapping() -> GENERIC_MAPPING {
        const STANDARD_RIGHTS: u32 = 0x0002_0000;
        const DELETE: u32 = 0x0001_0000;
        const STANDARD_RIGHTS_REQUIRED: u32 = 0x000f_0000;
        GENERIC_MAPPING {
            GenericRead: STANDARD_RIGHTS
                | FWPM_ACTRL_BEGIN_READ_TXN
                | FWPM_ACTRL_CLASSIFY
                | FWPM_ACTRL_OPEN
                | FWPM_ACTRL_READ
                | FWPM_ACTRL_READ_STATS,
            GenericExecute: STANDARD_RIGHTS | FWPM_ACTRL_ENUM | FWPM_ACTRL_SUBSCRIBE,
            GenericWrite: STANDARD_RIGHTS
                | DELETE
                | FWPM_ACTRL_ADD
                | FWPM_ACTRL_ADD_LINK
                | FWPM_ACTRL_BEGIN_WRITE_TXN
                | FWPM_ACTRL_WRITE,
            GenericAll: STANDARD_RIGHTS_REQUIRED
                | FWPM_ACTRL_ADD
                | FWPM_ACTRL_ADD_LINK
                | FWPM_ACTRL_BEGIN_READ_TXN
                | FWPM_ACTRL_BEGIN_WRITE_TXN
                | FWPM_ACTRL_CLASSIFY
                | FWPM_ACTRL_ENUM
                | FWPM_ACTRL_OPEN
                | FWPM_ACTRL_READ
                | FWPM_ACTRL_READ_STATS
                | FWPM_ACTRL_SUBSCRIBE
                | FWPM_ACTRL_WRITE,
        }
    }

    fn wfp_descriptor(engine: HANDLE, object: &str) -> Result<WfpDescriptor, InspectionIssue> {
        let security =
            OWNER_SECURITY_INFORMATION | GROUP_SECURITY_INFORMATION | DACL_SECURITY_INFORMATION;
        let mut descriptor = WfpDescriptor(ptr::null_mut());
        let code = unsafe {
            match object {
                "engine" => FwpmEngineGetSecurityInfo0(
                    engine,
                    security,
                    ptr::null_mut(),
                    ptr::null_mut(),
                    ptr::null_mut(),
                    ptr::null_mut(),
                    &mut descriptor.0,
                ),
                // A null filter key asks for the filter container, not a filter.
                "filterContainer" => FwpmFilterGetSecurityInfoByKey0(
                    engine,
                    ptr::null(),
                    security,
                    ptr::null_mut(),
                    ptr::null_mut(),
                    ptr::null_mut(),
                    ptr::null_mut(),
                    &mut descriptor.0,
                ),
                "aleAuthConnectV4" | "aleAuthConnectV6" => {
                    let key = if object == "aleAuthConnectV4" {
                        FWPM_LAYER_ALE_AUTH_CONNECT_V4
                    } else {
                        FWPM_LAYER_ALE_AUTH_CONNECT_V6
                    };
                    FwpmLayerGetSecurityInfoByKey0(
                        engine,
                        &key,
                        security,
                        ptr::null_mut(),
                        ptr::null_mut(),
                        ptr::null_mut(),
                        ptr::null_mut(),
                        &mut descriptor.0,
                    )
                }
                "universalSubLayer" => FwpmSubLayerGetSecurityInfoByKey0(
                    engine,
                    &FWPM_SUBLAYER_UNIVERSAL,
                    security,
                    ptr::null_mut(),
                    ptr::null_mut(),
                    ptr::null_mut(),
                    ptr::null_mut(),
                    &mut descriptor.0,
                ),
                _ => {
                    return Err(InspectionIssue::explanation(
                        "WFP descriptor",
                        "Unknown inspection object",
                    ))
                }
            }
        };
        if code != ERROR_SUCCESS {
            return Err(native_issue(
                &format!("read WFP {object} security"),
                "wfpOrWin32",
                i64::from(code),
            ));
        }
        Ok(descriptor)
    }

    fn inspect_wfp(checks: &mut [WfpAccess], host: &Result<HostAccess, InspectionIssue>) {
        let mut session = WfpSession(0);
        // No transaction or dynamic objects: this session is used only by the
        // five GetSecurityInfo calls. Opening it does not imply ADD/ADD_LINK.
        let code =
            unsafe { FwpmEngineOpen0(ptr::null(), 10, ptr::null(), ptr::null(), &mut session.0) };
        if code != ERROR_SUCCESS {
            let issue = native_issue("FwpmEngineOpen0", "wfpOrWin32", i64::from(code));
            for check in checks {
                check.access = AccessCheck::unavailable(check.access.desired_access, issue.clone());
            }
            return;
        }
        for check in checks {
            let desired = check.access.desired_access;
            let result = wfp_descriptor(session.0, &check.object).and_then(|descriptor| {
                host.as_ref().map_err(Clone::clone).and_then(|host| unsafe {
                    host.check_descriptor(descriptor.0, desired, wfp_mapping())
                        .map_err(access_issue)
                })
            });
            check.access = checked(desired, result);
        }
    }

    #[cfg(test)]
    mod tests {
        use std::{
            collections::BTreeMap,
            fs,
            path::PathBuf,
            time::{SystemTime, UNIX_EPOCH},
        };

        use windows_sys::Win32::Security::Authorization::{
            ConvertSecurityDescriptorToStringSecurityDescriptorW,
            ConvertStringSecurityDescriptorToSecurityDescriptorW,
        };

        use super::*;
        use crate::{AbsPath, FilesystemBoundary, NetworkBoundary};

        struct Directory(PathBuf);

        impl Drop for Directory {
            fn drop(&mut self) {
                let _ = fs::remove_dir_all(&self.0);
            }
        }

        fn sddl(path: &str) -> Result<String, String> {
            let descriptor = read_path_descriptor(path).map_err(|error| error.to_string())?;
            let mut text = LocalString(ptr::null_mut());
            let mut length = 0;
            let ok = unsafe {
                ConvertSecurityDescriptorToStringSecurityDescriptorW(
                    descriptor.0,
                    1,
                    OWNER_SECURITY_INFORMATION
                        | GROUP_SECURITY_INFORMATION
                        | DACL_SECURITY_INFORMATION,
                    &mut text.0,
                    &mut length,
                )
            };
            if ok == FALSE || text.0.is_null() || length == 0 {
                return Err(format!("SDDL query failed: {}", unsafe { GetLastError() }));
            }
            String::from_utf16(unsafe { slice::from_raw_parts(text.0, length as usize - 1) })
                .map_err(|error| error.to_string())
        }

        #[test]
        fn inspect_preserves_owned_files_and_acl_and_reports_missing_ancestors() {
            let name = format!(
                "klaus-inspect-{}-{}",
                std::process::id(),
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .expect("clock")
                    .as_nanos()
            );
            let directory = Directory(std::env::temp_dir().join(&name));
            fs::create_dir(&directory.0).expect("create owned directory");
            let marker = directory.0.join("marker.txt");
            fs::write(&marker, b"unchanged").expect("owned marker");
            let path = AbsPath::new(directory.0.to_string_lossy()).expect("Windows temp path");
            let absent = path.join("absent").expect("absent parent");
            let root = absent.join("root").expect("absent root");
            let request = SandboxRequest {
                command: vec!["DO_NOT_EXECUTE_INSPECTION_MARKER".to_owned()],
                cwd: root.clone(),
                env: BTreeMap::from([(
                    "DO_NOT_REPORT_SECRET".to_owned(),
                    "opaque-private-value".to_owned(),
                )]),
                filesystem: FilesystemBoundary {
                    writable: vec![root],
                    ..Default::default()
                },
                network: NetworkBoundary::LoopbackPorts {
                    ports: vec![29431],
                    allow_local_binding: false,
                },
                network_route_capability_sid: None,
                container_name: name,
                terminal: None,
            };
            let before = sddl(&path.as_str());
            let sid = identity::derive_sid(&request.container_name).expect("derive only");
            let sid = sid_text(sid.as_psid()).expect("SID text");
            let prior_loopback = read_loopback_sids().map(|entries| entries.contains(&sid));
            let report = super::super::inspect(&request);
            let after = sddl(&path.as_str());
            let after_loopback = read_loopback_sids().map(|entries| entries.contains(&sid));
            let content = fs::read(&marker);
            let absent_created = directory.0.join("absent").exists();
            let encoded = serde_json::to_string(&report).expect("serialize");
            let missing = report.walk.iter().find(|row| row.object == absent.as_str());
            let cleanup = fs::remove_dir_all(&directory.0);

            eprintln!("inspection_read_only cleanup={cleanup:?} identity={:?} missing={missing:?} wfp={:?}", report.identity, report.wfp);
            cleanup.expect("remove owned directory before assertions");
            assert_eq!(
                before.as_ref().expect("read owned DACL before"),
                after.as_ref().expect("read owned DACL after")
            );
            assert_eq!(content.expect("read owned marker"), b"unchanged");
            assert!(!absent_created, "inspection must not prepare missing paths");
            if let (Ok(before), Ok(after)) = (prior_loopback, after_loopback) {
                assert_eq!(before, after, "inspection changed its SID's exemption");
            }
            assert_eq!(report.identity.sid.as_deref(), Some(sid.as_str()));
            let missing = missing.expect("planned missing ancestor");
            assert_eq!(missing.target_access.state, AccessState::Unavailable);
            assert!(matches!(
                missing
                    .target_access
                    .issue
                    .as_ref()
                    .and_then(|issue| issue.code),
                Some(2 | 3)
            ));
            assert_eq!(missing.host_can_grant.state, AccessState::Unavailable);
            assert_eq!(report.wfp.checks.len(), 5);
            assert!(
                !report.wfp.checks[3].required,
                "V6 query must not claim V6 enforcement"
            );
            assert!(!encoded.contains("DO_NOT_EXECUTE_INSPECTION_MARKER"));
            assert!(!encoded.contains("DO_NOT_REPORT_SECRET"));
            assert!(!encoded.contains("opaque-private-value"));
        }

        #[test]
        fn wfp_specific_permissions_use_the_documented_generic_mapping() {
            let host = HostAccess::new().expect("own host query token");
            // These descriptors live only in memory. No WFP service permissions
            // or system DACL modifications are needed to check their semantics.
            // Object creation maps generic ACE rights before AccessCheck sees
            // them. An in-memory SDDL descriptor has not crossed that boundary.
            // MS-DTYP ACCESS_MASK explicitly requires concrete ACE rights for
            // access evaluation, so model the descriptor returned by WFP.
            let mapping = wfp_mapping();
            for (access, mask, expected) in [
                ("GA", mapping.GenericAll, true),
                ("GW", mapping.GenericWrite, true),
                ("GR", mapping.GenericRead, false),
            ] {
                let text: Vec<u16> = format!("O:WDG:WDD:(A;;0x{mask:08x};;;WD)\0")
                    .encode_utf16()
                    .collect();
                let mut descriptor = Descriptor(ptr::null_mut());
                assert_ne!(
                    unsafe {
                        ConvertStringSecurityDescriptorToSecurityDescriptorW(
                            text.as_ptr(),
                            1,
                            &mut descriptor.0,
                            ptr::null_mut(),
                        )
                    },
                    FALSE,
                    "construct private descriptor"
                );
                let result = unsafe {
                    host.check_descriptor(
                        descriptor.0,
                        FWPM_ACTRL_ADD | FWPM_ACTRL_ADD_LINK,
                        wfp_mapping(),
                    )
                }
                .expect("AccessCheck");
                assert_eq!(result.allowed, expected, "{access}: {result:?}");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use crate::{AbsPath, FilesystemBoundary, NetworkBoundary};

    fn request(network: NetworkBoundary) -> SandboxRequest {
        let root = AbsPath::new(r"C:\owned-inspection\root").expect("absolute path");
        SandboxRequest {
            command: vec!["not executed".to_owned()],
            cwd: root.clone(),
            env: BTreeMap::new(),
            filesystem: FilesystemBoundary {
                writable: vec![root],
                ..Default::default()
            },
            network,
            network_route_capability_sid: None,
            container_name: "klaus-inspection-contract".to_owned(),
            terminal: None,
        }
    }

    #[test]
    fn report_preserves_unknown_and_not_required_as_distinct_states() {
        let cases: [(NetworkBoundary, bool, &[&str], AccessState, &str); 3] = [
            (
                NetworkBoundary::Denied,
                false,
                &[],
                AccessState::NotRequired,
                "notRequired",
            ),
            (
                NetworkBoundary::Allowed,
                true,
                &["S-1-15-3-1", "S-1-15-3-2", "S-1-15-3-3"],
                AccessState::Unavailable,
                "unavailable",
            ),
            (
                NetworkBoundary::LoopbackPorts {
                    ports: vec![29431],
                    allow_local_binding: false,
                },
                true,
                &["S-1-15-3-1"],
                AccessState::Unavailable,
                "unavailable",
            ),
        ];
        for (network, network_required, capabilities, access_state, wire_state) in cases {
            let issue = InspectionIssue::explanation("fixture query", "unavailable");
            let report = initial_report(&request(network), issue.clone());
            assert_eq!(report.capabilities, capabilities);
            assert_eq!(report.loopback.required, network_required);
            assert_eq!(report.loopback.present, None);
            assert_eq!(report.loopback.issue, network_required.then_some(issue));
            assert_eq!(report.wfp.required, network_required);
            assert!(report.wfp.checks.iter().all(|row| {
                row.access.state == access_state
                    && row.required == (network_required && row.object != "aleAuthConnectV6")
            }));
            assert_eq!(
                report.null_device.target_access.state,
                AccessState::Unavailable
            );
            assert_eq!(
                report.null_device.host_can_grant.state,
                AccessState::Unavailable
            );
            assert_eq!(report.null_device.target_access.desired_access, 0x0012_019f);
            assert_eq!(
                report.null_device.host_can_grant.desired_access,
                0x0004_0000
            );
            let encoded = serde_json::to_value(&report).expect("serialize report");
            assert_eq!(encoded["readOnly"], true);
            assert_eq!(encoded["loopback"]["required"], network_required);
            assert_eq!(encoded["loopback"]["present"], serde_json::Value::Null);
            assert_eq!(encoded["wfp"]["checks"][0]["access"]["state"], wire_state);
            assert_eq!(
                encoded["nullDevice"]["targetAccess"]["state"],
                "unavailable"
            );
            assert!(encoded.get("ready").is_none());
            assert!(encoded.get("pass").is_none());
            assert_eq!(
                serde_json::from_value::<PreparationReport>(encoded).expect("deserialize report"),
                report
            );
        }
    }

    #[cfg(not(windows))]
    #[test]
    fn public_inspection_on_other_platforms_never_claims_windows_access() {
        let report = inspect(&request(NetworkBoundary::LoopbackPorts {
            ports: vec![29431],
            allow_local_binding: false,
        }));
        assert_eq!(report.identity.sid, None);
        assert_eq!(report.loopback.present, None);
        assert!(report
            .walk
            .iter()
            .all(|row| row.target_access.state == AccessState::Unavailable));
        assert!(report
            .wfp
            .checks
            .iter()
            .all(|row| row.access.state == AccessState::Unavailable));
        assert!(report.wfp.required);
        assert_eq!(report.capabilities, ["S-1-15-3-1"]);
    }
}
