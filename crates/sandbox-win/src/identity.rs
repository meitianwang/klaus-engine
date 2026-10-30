//! AppContainer SID allocation and per-user profile ownership.
//!
//! Windows derives a SID deterministically from a name; that mapping does not
//! establish ownership of the profile's folders and registry storage. A derived
//! `AppContainerSid` owns only its native SID allocation. The legacy
//! `ensure_container` helper may reuse a profile and therefore cannot delete it.
//!
//! `OwnedAppContainerProfile` instead requires an exact execution moniker and
//! acquires ownership only through a successful exclusive native create. The
//! caller explicitly closes it under the same Windows user after its target,
//! I/O, and permission leases have finished. Dropping an owner frees SID memory
//! without claiming that profile storage is safe to delete.

use crate::protocol::SandboxRequest;

/// Longest AppContainer moniker Windows accepts.
///
/// The API's own limit. Enforced here so an over-long name fails where the caller
/// can see why, rather than inside a Win32 call with an error code that does not
/// mention length.
pub const MAX_CONTAINER_NAME: usize = 64;

/// Namespace for profiles owned by one execution. Callers generate a fresh UUID
/// before serializing the request; validation never rewrites its identity.
pub const EXECUTION_CONTAINER_PREFIX: &str = "klaus-exec-";

/// Require an exact execution name, rather than silently sanitizing aliases.
///
/// The suffix is a UUID's 32 lowercase hexadecimal digits. Syntax alone does not
/// establish uniqueness: exclusive native creation must still reject collisions.
pub fn validate_execution_moniker(name: &str) -> Result<(), IdentityError> {
    let valid = name
        .strip_prefix(EXECUTION_CONTAINER_PREFIX)
        .is_some_and(|suffix| {
            suffix.len() == 32
                && suffix
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        });
    if !valid {
        return Err(IdentityError::InvalidExecutionName(name.to_string()));
    }
    Ok(())
}

/// Derive the AppContainer moniker for a request.
///
/// The name is what makes the SID stable, so it must depend only on things that
/// are stable for the same workspace. It is built from the caller-supplied
/// container name, sanitized: Windows accepts a limited character set here, and a
/// workspace path (which is what callers naturally reach for) contains separators
/// and colons that are not in it.
pub fn container_moniker(request: &SandboxRequest) -> Result<String, IdentityError> {
    sanitize_moniker(&request.container_name)
}

/// Why an identity could not be established.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum IdentityError {
    #[error("execution container name is not canonical: {0:?}")]
    InvalidExecutionName(String),
    #[error("container name is empty after sanitizing: {0:?}")]
    EmptyName(String),
    #[error(
        "container name is {length} characters, over the {MAX_CONTAINER_NAME} limit: {name:?}"
    )]
    NameTooLong { name: String, length: usize },
    #[error("AppContainer profile {name:?} could not be created: {detail}")]
    ProfileCreate { name: String, detail: String },
    #[error("execution AppContainer profile {name:?} already exists; ownership was not acquired")]
    ProfileAlreadyExists { name: String },
    #[error("owned AppContainer profile {name:?} could not be deleted: {detail}")]
    ProfileDelete { name: String, detail: String },
    #[error("AppContainer profile {name:?} has an unsafe owner context: {detail}")]
    ProfileOwnerContext { name: String, detail: String },
    #[error("AppContainer SID for {name:?} could not be derived: {detail}")]
    SidDerive { name: String, detail: String },
}

/// Reduce a caller-supplied name to the character set Windows accepts for a
/// moniker, without letting two different inputs collapse onto one identity.
///
/// Collision matters more than tidiness here. If `C:\a\ws` and `C:\b\ws` both
/// sanitized to `ws`, they would share one SID and therefore one set of ACEs, and
/// each workspace would be reachable from the other's sandbox. So separators
/// become a character that survives (`-`) rather than being dropped, and anything
/// outside the accepted set is encoded rather than deleted.
pub fn sanitize_moniker(raw: &str) -> Result<String, IdentityError> {
    let mut out = String::with_capacity(raw.len());
    for ch in raw.chars() {
        match ch {
            'a'..='z' | 'A'..='Z' | '0'..='9' | '.' | '-' | '_' => out.push(ch),
            // A separator is meaningful structure, not noise: keep it as a
            // distinct character so distinct paths stay distinct.
            '\\' | '/' | ':' | ' ' => out.push('-'),
            // Everything else (non-ASCII workspace names, punctuation) is encoded
            // so it still contributes to uniqueness.
            other => {
                out.push('_');
                for byte in other.to_string().bytes() {
                    out.push_str(&format!("{byte:02x}"));
                }
            }
        }
    }
    // Collapse runs of separators for readability; a run and a single separator
    // cannot come from different inputs in a way that matters, because the
    // characters around them still differ.
    while out.contains("--") {
        out = out.replace("--", "-");
    }
    let trimmed = out.trim_matches('-').to_string();
    if trimmed.is_empty() {
        return Err(IdentityError::EmptyName(raw.to_string()));
    }
    if trimmed.len() > MAX_CONTAINER_NAME {
        return Err(IdentityError::NameTooLong {
            name: trimmed.clone(),
            length: trimmed.len(),
        });
    }
    Ok(trimmed)
}

#[cfg(windows)]
mod imp {
    use std::ptr;

    use windows_sys::Win32::Foundation::{
        CloseHandle, GetLastError, LocalFree, ERROR_ALREADY_EXISTS, ERROR_INSUFFICIENT_BUFFER,
        ERROR_NO_TOKEN, HANDLE, HLOCAL, PSID, S_OK,
    };
    use windows_sys::Win32::Security::Authorization::ConvertSidToStringSidW;
    use windows_sys::Win32::Security::Isolation::{
        CreateAppContainerProfile, DeleteAppContainerProfile,
        DeriveAppContainerSidFromAppContainerName,
    };
    use windows_sys::Win32::Security::{
        FreeSid, GetTokenInformation, IsValidSid, TokenUser, TOKEN_QUERY, TOKEN_USER,
    };
    use windows_sys::Win32::System::Threading::{
        GetCurrentProcess, GetCurrentThread, OpenProcessToken, OpenThreadToken,
    };

    use super::{validate_execution_moniker, IdentityError};

    /// A SID owned by this process, freed on drop.
    ///
    /// Both AppContainer profile APIs require `FreeSid` for their output. This
    /// owner must never adopt a SID allocated by `ConvertStringSidToSid` or
    /// `DeriveCapabilitySidsFromName`, whose allocation contract is `LocalFree`.
    pub struct AppContainerSid {
        raw: PSID,
    }

    impl AppContainerSid {
        pub fn as_psid(&self) -> PSID {
            self.raw
        }
    }

    impl Drop for AppContainerSid {
        fn drop(&mut self) {
            if !self.raw.is_null() {
                unsafe { FreeSid(self.raw) };
            }
        }
    }

    // The SID is only read by Win32 calls this crate makes; it is not shared
    // mutably, so it can cross threads with the session that owns it.
    unsafe impl Send for AppContainerSid {}
    unsafe impl Sync for AppContainerSid {}

    struct TokenHandle(HANDLE);

    impl Drop for TokenHandle {
        fn drop(&mut self) {
            if self.0 != 0 {
                unsafe { CloseHandle(self.0) };
            }
        }
    }

    struct LocalString(*mut u16);

    impl Drop for LocalString {
        fn drop(&mut self) {
            if !self.0.is_null() {
                unsafe { LocalFree(self.0 as HLOCAL) };
            }
        }
    }

    /// Profile storage is per user. Do not let an owner cross an impersonation
    /// boundary, even when the impersonated token happens to name the same user.
    /// These synchronous checks never impersonate or revert the calling thread.
    fn unimpersonated_process_user_sid() -> Result<String, String> {
        let mut thread_token = TokenHandle(0);
        if unsafe { OpenThreadToken(GetCurrentThread(), TOKEN_QUERY, 1, &mut thread_token.0) } != 0
        {
            return Err("thread impersonation is not supported for owned profiles".into());
        }
        let code = unsafe { GetLastError() };
        if code != ERROR_NO_TOKEN {
            return Err(format!(
                "OpenThreadToken could not rule out impersonation: win32={code}"
            ));
        }

        let mut process_token = TokenHandle(0);
        if unsafe { OpenProcessToken(GetCurrentProcess(), TOKEN_QUERY, &mut process_token.0) } == 0
        {
            return Err(format!("OpenProcessToken failed: win32={}", unsafe {
                GetLastError()
            }));
        }
        let mut bytes = 0;
        let probe = unsafe {
            GetTokenInformation(process_token.0, TokenUser, ptr::null_mut(), 0, &mut bytes)
        };
        let code = unsafe { GetLastError() };
        if probe != 0 || code != ERROR_INSUFFICIENT_BUFFER {
            return Err(format!(
                "TokenUser size probe was unexpected: result={probe}, win32={code}"
            ));
        }
        if bytes < std::mem::size_of::<TOKEN_USER>() as u32 || bytes > 65_536 {
            return Err(format!("TokenUser size is invalid: {bytes}"));
        }
        let capacity = bytes;
        let mut storage = vec![0usize; (bytes as usize).div_ceil(std::mem::size_of::<usize>())];
        if unsafe {
            GetTokenInformation(
                process_token.0,
                TokenUser,
                storage.as_mut_ptr().cast(),
                capacity,
                &mut bytes,
            )
        } == 0
        {
            return Err(format!(
                "GetTokenInformation(TokenUser) failed: win32={}",
                unsafe { GetLastError() }
            ));
        }
        if bytes < std::mem::size_of::<TOKEN_USER>() as u32 || bytes > capacity {
            return Err(format!("TokenUser returned an invalid size: {bytes}"));
        }
        let sid = unsafe { (*storage.as_ptr().cast::<TOKEN_USER>()).User.Sid };
        if sid.is_null() || unsafe { IsValidSid(sid) } == 0 {
            return Err("TokenUser returned an invalid SID".into());
        }
        let mut text = LocalString(ptr::null_mut());
        if unsafe { ConvertSidToStringSidW(sid, &mut text.0) } == 0 {
            return Err(format!(
                "ConvertSidToStringSidW(TokenUser) failed: win32={}",
                unsafe { GetLastError() }
            ));
        }
        if text.0.is_null() {
            return Err("TokenUser SID conversion returned a null string".into());
        }
        // Windows allocated this string from a valid SID. Its maximum spelling
        // fits within 256 UTF-16 code units, including the terminator.
        let length = (0..256)
            .find(|index| unsafe { *text.0.add(*index) == 0 })
            .ok_or("TokenUser SID string is not terminated within its maximum length")?;
        String::from_utf16(unsafe { std::slice::from_raw_parts(text.0, length) })
            .map_err(|error| format!("TokenUser SID string is invalid: {error}"))
    }

    /// Exclusive ownership of a profile created for one execution.
    ///
    /// This is not a process or permission lease. The caller must confirm that its
    /// target, I/O workers, and grants have finished before calling `close`.
    /// Dropping this value only releases SID memory; it deliberately leaves the
    /// profile registered, because Drop cannot establish that the target stopped.
    /// Creation and close require an unimpersonated thread and the same process
    /// TokenUser SID. Callers must not change that context during either call.
    #[must_use = "explicitly close the owned profile after target and permission cleanup"]
    pub struct OwnedAppContainerProfile {
        name: String,
        creator_user_sid: String,
        sid: Option<AppContainerSid>,
        owned: bool,
    }

    impl std::fmt::Debug for OwnedAppContainerProfile {
        fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter
                .debug_struct("OwnedAppContainerProfile")
                .field("name", &self.name)
                .field("creator_user_sid", &self.creator_user_sid)
                .field("sid_available", &self.sid.is_some())
                .field("owned", &self.owned)
                .finish()
        }
    }

    /// A failed creation may still own a profile if the native API returned
    /// S_OK without a usable SID. `into_parts` retains that cleanup obligation;
    /// an already-existing profile never produces a recovery owner.
    #[derive(Debug)]
    #[must_use = "inspect the recovery owner before discarding a profile creation failure"]
    pub struct ProfileCreateError {
        error: IdentityError,
        recovery: Option<Box<OwnedAppContainerProfile>>,
    }

    impl ProfileCreateError {
        pub fn error(&self) -> &IdentityError {
            &self.error
        }

        pub fn into_parts(self) -> (IdentityError, Option<OwnedAppContainerProfile>) {
            (self.error, self.recovery.map(|profile| *profile))
        }
    }

    impl std::fmt::Display for ProfileCreateError {
        fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            std::fmt::Display::fmt(&self.error, formatter)
        }
    }

    impl std::error::Error for ProfileCreateError {
        fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
            Some(&self.error)
        }
    }

    impl OwnedAppContainerProfile {
        /// Create exactly this canonical profile or return a collision error.
        /// No existing profile is derived, adopted, or deleted by this operation.
        pub fn create(name: &str) -> Result<Self, ProfileCreateError> {
            validate_execution_moniker(name).map_err(|error| ProfileCreateError {
                error,
                recovery: None,
            })?;
            let creator_user_sid =
                unimpersonated_process_user_sid().map_err(|detail| ProfileCreateError {
                    error: IdentityError::ProfileOwnerContext {
                        name: name.to_string(),
                        detail,
                    },
                    recovery: None,
                })?;
            let native_name = wide(name);
            let description = wide("Klaus execution sandbox");
            // Own the output slot before the call, including any non-null output
            // on failure. Profile ownership and allocation ownership are separate.
            let mut sid = AppContainerSid {
                raw: ptr::null_mut(),
            };
            let result = unsafe {
                CreateAppContainerProfile(
                    native_name.as_ptr(),
                    native_name.as_ptr(),
                    description.as_ptr(),
                    ptr::null(),
                    0,
                    &mut sid.raw,
                )
            };
            Self::from_create_result(name.to_string(), creator_user_sid, result, sid)
        }

        fn from_create_result(
            name: String,
            creator_user_sid: String,
            result: i32,
            sid: AppContainerSid,
        ) -> Result<Self, ProfileCreateError> {
            if result == S_OK {
                // S_OK establishes that storage was created even if the output is
                // malformed. Retain its name so the caller can explicitly close it.
                let profile = Self {
                    name,
                    creator_user_sid,
                    sid: (!sid.raw.is_null()).then_some(sid),
                    owned: true,
                };
                if profile.sid.is_some() {
                    return Ok(profile);
                }
                return Err(ProfileCreateError {
                    error: IdentityError::ProfileCreate {
                        name: profile.name.clone(),
                        detail: "CreateAppContainerProfile returned S_OK without a SID; the created profile requires explicit cleanup".to_string(),
                    },
                    recovery: Some(Box::new(profile)),
                });
            }
            let error = if result == hresult_from_win32(ERROR_ALREADY_EXISTS) {
                IdentityError::ProfileAlreadyExists { name }
            } else {
                IdentityError::ProfileCreate {
                    name,
                    detail: format!("CreateAppContainerProfile returned 0x{result:08x}"),
                }
            };
            Err(ProfileCreateError {
                error,
                recovery: None,
            })
        }

        pub fn name(&self) -> &str {
            &self.name
        }

        /// The authoritative process TokenUser SID captured before creation.
        /// It remains available after close and in malformed-create recovery.
        pub fn creator_user_sid(&self) -> &str {
            &self.creator_user_sid
        }

        /// Available after successful creation; absent after close or in a
        /// recovery owner returned for S_OK with no usable native SID.
        pub fn sid(&self) -> Option<&AppContainerSid> {
            self.sid.as_ref()
        }

        pub fn is_owned(&self) -> bool {
            self.owned
        }

        /// Delete only this successfully created profile. This is idempotent
        /// after success. A native failure leaves ownership intact for retry.
        /// The caller, not this object, proves that the target has stopped.
        pub fn close(&mut self) -> Result<(), IdentityError> {
            if !self.owned {
                return Ok(());
            }
            let user_sid = unimpersonated_process_user_sid().map_err(|detail| {
                IdentityError::ProfileOwnerContext {
                    name: self.name.clone(),
                    detail,
                }
            })?;
            self.validate_creator_user(&user_sid)?;
            let result = unsafe { DeleteAppContainerProfile(wide(&self.name).as_ptr()) };
            self.finish_close(result)
        }

        fn validate_creator_user(&self, user_sid: &str) -> Result<(), IdentityError> {
            if user_sid != self.creator_user_sid {
                return Err(IdentityError::ProfileOwnerContext {
                    name: self.name.clone(),
                    detail: format!("creator TokenUser SID is {}, current TokenUser SID is {user_sid}; profile ownership was retained", self.creator_user_sid),
                });
            }
            Ok(())
        }

        fn finish_close(&mut self, result: i32) -> Result<(), IdentityError> {
            if result != S_OK {
                return Err(IdentityError::ProfileDelete {
                    name: self.name.clone(),
                    detail: format!("DeleteAppContainerProfile returned 0x{result:08x}"),
                });
            }
            self.owned = false;
            self.sid = None;
            Ok(())
        }
    }

    fn wide(text: &str) -> Vec<u16> {
        text.encode_utf16().chain(std::iter::once(0)).collect()
    }

    /// Register the profile if needed and return the container's SID.
    ///
    /// One call covers both because `CreateAppContainerProfile` hands back the SID
    /// it just registered, and on the "already exists" path the SID is derived
    /// instead. Splitting them would mean deriving twice on first run for no
    /// reason, and would invite a caller to derive without ensuring.
    ///
    /// "Already exists" is success, not an error. Profiles outlive the session
    /// that created them by design — that is what lets ACEs written for the SID
    /// stay valid — so the second and every later run take that path.
    pub fn ensure_container(moniker: &str) -> Result<AppContainerSid, IdentityError> {
        let name = wide(moniker);
        let display = wide(moniker);
        let description = wide("Klaus sandboxed command");
        // Own the output slot before the call so a non-null failure output is
        // also released, including the already-existing-profile fallback.
        let mut sid = AppContainerSid {
            raw: ptr::null_mut(),
        };
        let hr = unsafe {
            CreateAppContainerProfile(
                name.as_ptr(),
                display.as_ptr(),
                description.as_ptr(),
                ptr::null(),
                0,
                &mut sid.raw,
            )
        };
        if hr == S_OK && !sid.raw.is_null() {
            return Ok(sid);
        }
        if hr != hresult_from_win32(ERROR_ALREADY_EXISTS) {
            return Err(IdentityError::ProfileCreate {
                name: moniker.to_string(),
                detail: format!("CreateAppContainerProfile returned 0x{hr:08x}"),
            });
        }
        drop(sid);
        derive_sid(moniker)
    }

    /// Derive the container's SID from its moniker, without registering anything.
    ///
    /// Derivation does not create a profile or require one to be present. Callers
    /// that need profile storage must separately use [`ensure_container`].
    pub fn derive_sid(moniker: &str) -> Result<AppContainerSid, IdentityError> {
        let name = wide(moniker);
        let mut sid = AppContainerSid {
            raw: ptr::null_mut(),
        };
        let hr = unsafe { DeriveAppContainerSidFromAppContainerName(name.as_ptr(), &mut sid.raw) };
        if hr != S_OK || sid.raw.is_null() {
            return Err(IdentityError::SidDerive {
                name: moniker.to_string(),
                detail: format!("DeriveAppContainerSidFromAppContainerName returned 0x{hr:08x}"),
            });
        }
        Ok(sid)
    }

    /// `HRESULT_FROM_WIN32`, which the crate does not expose as a function.
    fn hresult_from_win32(code: u32) -> i32 {
        if code == 0 {
            0
        } else {
            ((code & 0x0000_FFFF) | (7 << 16) | 0x8000_0000) as i32
        }
    }

    #[cfg(test)]
    mod owned_profile_tests {
        use super::*;

        const NAME: &str = "klaus-exec-1234567890abcdef1234567890abcdef";
        const USER_SID: &str = "S-1-5-21-1-2-3-1001";

        #[test]
        fn successful_creation_without_sid_retains_a_recoverable_profile() {
            let failure = OwnedAppContainerProfile::from_create_result(
                NAME.to_string(),
                USER_SID.to_string(),
                S_OK,
                AppContainerSid {
                    raw: ptr::null_mut(),
                },
            )
            .unwrap_err();
            let (error, recovery) = failure.into_parts();
            assert!(matches!(error, IdentityError::ProfileCreate { .. }));
            let mut recovery = recovery.expect("S_OK owns the created storage");
            assert!(recovery.is_owned());
            assert!(recovery.sid().is_none());
            assert_eq!(recovery.name(), NAME);
            assert_eq!(recovery.creator_user_sid(), USER_SID);
            assert!(recovery.validate_creator_user(USER_SID).is_ok());
            assert!(recovery
                .validate_creator_user("S-1-5-21-1-2-3-1002")
                .is_err());
            assert!(recovery.is_owned(), "changed user must retain ownership");
            // Inspect only state transitions; this test calls no profile API.
            assert!(recovery.finish_close(hresult_from_win32(5)).is_err());
            assert!(recovery.is_owned(), "failed deletion must remain retryable");
            recovery.finish_close(S_OK).unwrap();
            assert!(!recovery.is_owned());
        }

        #[test]
        fn collisions_and_failed_creates_never_acquire_profile_ownership() {
            for status in [
                hresult_from_win32(ERROR_ALREADY_EXISTS),
                hresult_from_win32(5),
            ] {
                let failure = OwnedAppContainerProfile::from_create_result(
                    NAME.to_string(),
                    USER_SID.to_string(),
                    status,
                    AppContainerSid {
                        raw: ptr::null_mut(),
                    },
                )
                .unwrap_err();
                let (error, recovery) = failure.into_parts();
                assert!(recovery.is_none());
                assert_eq!(
                    matches!(error, IdentityError::ProfileAlreadyExists { .. }),
                    status == hresult_from_win32(ERROR_ALREADY_EXISTS),
                );
            }
        }
    }
}

#[cfg(windows)]
pub use imp::{
    derive_sid, ensure_container, AppContainerSid, OwnedAppContainerProfile, ProfileCreateError,
};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_ordinary_name_passes_through_unchanged() {
        assert_eq!(
            sanitize_moniker("klaus-sandbox-ws").expect("ok"),
            "klaus-sandbox-ws"
        );
        assert_eq!(
            sanitize_moniker("klaus.sandbox_1").expect("ok"),
            "klaus.sandbox_1"
        );
    }

    #[test]
    fn separators_become_a_character_that_survives() {
        // Dropping them would let two paths collapse onto one identity; see the
        // note on sanitize_moniker.
        assert_eq!(sanitize_moniker("C:\\work\\ws").expect("ok"), "C-work-ws");
        assert_eq!(sanitize_moniker("C:/work/ws").expect("ok"), "C-work-ws");
    }

    #[test]
    fn distinct_inputs_stay_distinct() {
        // The property that matters: if these collided, each workspace's sandbox
        // could reach the other's files through shared ACEs.
        let a = sanitize_moniker("C:\\a\\ws").expect("ok");
        let b = sanitize_moniker("C:\\b\\ws").expect("ok");
        assert_ne!(a, b);

        let deep = sanitize_moniker("C:\\ws\\sub").expect("ok");
        let flat = sanitize_moniker("C:\\wssub").expect("ok");
        assert_ne!(deep, flat);
    }

    #[test]
    fn non_ascii_names_are_encoded_rather_than_dropped() {
        let encoded = sanitize_moniker("ws-\u{9879}\u{76ee}").expect("ok");
        assert!(encoded.starts_with("ws-"));
        // Dropping the characters would make every non-ASCII workspace share one
        // identity.
        assert_ne!(encoded, "ws-");
        assert_ne!(
            encoded,
            sanitize_moniker("ws-\u{5de5}\u{4f5c}").expect("ok")
        );
        assert!(
            encoded
                .chars()
                .all(|ch| ch.is_ascii_alphanumeric() || ch == '.' || ch == '-' || ch == '_'),
            "encoded name must stay inside the accepted set: {encoded}"
        );
    }

    #[test]
    fn a_name_with_nothing_usable_is_refused() {
        assert!(matches!(
            sanitize_moniker(""),
            Err(IdentityError::EmptyName(_))
        ));
        assert!(matches!(
            sanitize_moniker("\\\\//::"),
            Err(IdentityError::EmptyName(_))
        ));
    }

    #[test]
    fn an_over_long_name_is_refused_where_the_reason_is_visible() {
        let long = "w".repeat(MAX_CONTAINER_NAME + 1);
        assert!(matches!(
            sanitize_moniker(&long),
            Err(IdentityError::NameTooLong { .. })
        ));
        // Exactly at the limit is fine.
        let exact = "w".repeat(MAX_CONTAINER_NAME);
        assert_eq!(
            sanitize_moniker(&exact).expect("ok").len(),
            MAX_CONTAINER_NAME
        );
    }

    #[test]
    fn the_same_name_always_sanitizes_the_same_way() {
        // Stability is the whole reason the SID can be relied on across runs.
        let name = "C:\\Users\\me\\.klaus\\worktrees\\feature-x";
        let first = sanitize_moniker(name).expect("ok");
        let second = sanitize_moniker(name).expect("ok");
        assert_eq!(first, second);
    }
}
