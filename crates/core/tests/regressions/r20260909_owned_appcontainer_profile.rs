//! 2026-09-09: workspace profile reuse did not distinguish creation ownership.
//! Pin the public execution-name contract and explicit profile lifecycle API.
//! The native case is ignored, exact-name and environment gated. It refuses an
//! elevated, noninteractive, non-medium, AppContainer, or impersonating controller
//! before creating resources. Only fresh profiles and explicit evidence files are
//! created; no target, file grant, NUL, loopback exemption, or WFP is involved.
//! Cleanup retries are bounded. Unconfirmed ownership remains recorded with the
//! native TokenUser SID; Drop must never delete a potentially live profile.

use klaus_sandbox_win::identity::{validate_execution_moniker, IdentityError};

#[test]
fn execution_profile_names_are_canonical_without_alias_rewriting() {
    for name in [
        "klaus-exec-1234567890abcdef1234567890abcdef",
        "klaus-exec-0123456789abcdef0123456789abcdef",
    ] {
        assert!(validate_execution_moniker(name).is_ok());
    }
    for name in [
        "",
        "klaus-workspace-1234567890abcdef1234567890abcdef",
        "klaus-exec-1234567890ABCDEF1234567890abcdef",
        "klaus-exec-12345678-90ab-cdef-1234-567890abcdef",
        "klaus-exec-1234567890abcdef1234567890abcde",
        "klaus-exec-1234567890abcdef1234567890abcdef0",
        "klaus-exec-1234567890abcdef1234567890abcdeg",
        " klaus-exec-1234567890abcdef1234567890abcdef",
        "klaus-exec-1234567890abcdef1234567890abcdef ",
        "klaus-exec-1234567890abcdef1234567890abcdef\0",
        "klaus-exec-1234567890abcdef1234567890abcdé",
    ] {
        assert!(
            matches!(
                validate_execution_moniker(name),
                Err(IdentityError::InvalidExecutionName(_))
            ),
            "invalid execution identity was accepted: {name:?}",
        );
    }
}

#[cfg(windows)]
mod native {
    use std::fs::OpenOptions;
    use std::io::Write;
    use std::os::windows::io::{AsRawHandle, FromRawHandle, OwnedHandle};
    use std::path::{Path, PathBuf};
    use std::ptr;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    use klaus_sandbox_win::identity::{self, OwnedAppContainerProfile, ProfileCreateError};
    use serde::Serialize;
    use serde_json::{json, Value};
    use windows_sys::Win32::Foundation::{GetLastError, LocalFree, ERROR_NO_TOKEN, HANDLE};
    use windows_sys::Win32::Security::Authorization::ConvertSidToStringSidW;
    use windows_sys::Win32::Security::{
        EqualSid, GetSidSubAuthority, GetSidSubAuthorityCount, GetTokenInformation, IsValidSid,
        TokenElevation, TokenIntegrityLevel, TokenIsAppContainer, TokenSessionId, TokenUser,
        TOKEN_ELEVATION, TOKEN_INFORMATION_CLASS, TOKEN_MANDATORY_LABEL, TOKEN_QUERY, TOKEN_USER,
    };
    use windows_sys::Win32::System::Threading::{
        GetCurrentProcess, GetCurrentThread, OpenProcessToken, OpenThreadToken,
    };

    use super::*;

    const MAX_CLOSE_ATTEMPTS: usize = 3;
    const CLOSE_RETRY_DELAY: Duration = Duration::from_millis(50);

    fn information(token: HANDLE, class: TOKEN_INFORMATION_CLASS) -> Result<Vec<usize>, String> {
        let mut bytes = 0;
        unsafe { GetTokenInformation(token, class, ptr::null_mut(), 0, &mut bytes) };
        if bytes == 0 || bytes > 65_536 {
            return Err(format!("invalid token information size: {bytes}"));
        }
        let capacity = bytes;
        let mut storage = vec![0usize; (bytes as usize).div_ceil(std::mem::size_of::<usize>())];
        if unsafe {
            GetTokenInformation(
                token,
                class,
                storage.as_mut_ptr().cast(),
                capacity,
                &mut bytes,
            )
        } == 0
        {
            return Err(std::io::Error::last_os_error().to_string());
        }
        if bytes == 0 || bytes > capacity {
            return Err(format!("invalid returned token information size: {bytes}"));
        }
        Ok(storage)
    }

    struct LocalString(*mut u16);

    impl Drop for LocalString {
        fn drop(&mut self) {
            if !self.0.is_null() {
                unsafe { LocalFree(self.0.cast()) };
            }
        }
    }

    fn token_user_sid(token: HANDLE) -> Result<String, String> {
        let user = information(token, TokenUser)?;
        if std::mem::size_of_val(user.as_slice()) < std::mem::size_of::<TOKEN_USER>() {
            return Err("TokenUser is smaller than its native header".into());
        }
        let sid = unsafe { (*user.as_ptr().cast::<TOKEN_USER>()).User.Sid };
        if sid.is_null() || unsafe { IsValidSid(sid) } == 0 {
            return Err("TokenUser SID is invalid".into());
        }
        let mut text = LocalString(ptr::null_mut());
        if unsafe { ConvertSidToStringSidW(sid, &mut text.0) } == 0 {
            return Err(std::io::Error::last_os_error().to_string());
        }
        if text.0.is_null() {
            return Err("TokenUser SID conversion returned no text".into());
        }
        let length = (0..256)
            .find(|index| unsafe { *text.0.add(*index) == 0 })
            .ok_or("TokenUser SID string exceeds its maximum length")?;
        String::from_utf16(unsafe { std::slice::from_raw_parts(text.0, length) })
            .map_err(|error| error.to_string())
    }

    #[derive(Debug, Serialize)]
    #[serde(rename_all = "camelCase")]
    struct ControllerIdentity {
        process_id: u32,
        process_user_sid: String,
        effective_user_sid: String,
        thread_impersonating: bool,
        session_id: u32,
        process_integrity_rid: u32,
        process_elevated: bool,
        process_app_container: bool,
    }

    fn controller_identity() -> Result<ControllerIdentity, String> {
        let mut raw = ptr::null_mut();
        if unsafe { OpenProcessToken(GetCurrentProcess(), TOKEN_QUERY, &mut raw) } == 0 {
            return Err(std::io::Error::last_os_error().to_string());
        }
        let token = unsafe { OwnedHandle::from_raw_handle(raw) };
        let process_user_sid = token_user_sid(token.as_raw_handle())?;
        let mut thread_raw = ptr::null_mut();
        let opened =
            unsafe { OpenThreadToken(GetCurrentThread(), TOKEN_QUERY, 1, &mut thread_raw) };
        let (effective_user_sid, thread_impersonating) = if opened != 0 {
            let thread_token = unsafe { OwnedHandle::from_raw_handle(thread_raw) };
            (token_user_sid(thread_token.as_raw_handle())?, true)
        } else {
            let code = unsafe { GetLastError() };
            if code != ERROR_NO_TOKEN {
                return Err(format!(
                    "cannot rule out controller impersonation: win32={code}"
                ));
            }
            (process_user_sid.clone(), false)
        };
        let app = information(token.as_raw_handle(), TokenIsAppContainer)?;
        let process_app_container = unsafe { *app.as_ptr().cast::<u32>() } != 0;
        let elevation = information(token.as_raw_handle(), TokenElevation)?;
        let process_elevated =
            unsafe { (*elevation.as_ptr().cast::<TOKEN_ELEVATION>()).TokenIsElevated } != 0;
        let session = information(token.as_raw_handle(), TokenSessionId)?;
        let session_id = unsafe { *session.as_ptr().cast::<u32>() };
        let label = information(token.as_raw_handle(), TokenIntegrityLevel)?;
        let sid = unsafe { (*label.as_ptr().cast::<TOKEN_MANDATORY_LABEL>()).Label.Sid };
        if sid.is_null() || unsafe { IsValidSid(sid) } == 0 {
            return Err("controller integrity SID is invalid".into());
        }
        let count = unsafe { *GetSidSubAuthorityCount(sid) };
        if count == 0 {
            return Err("controller integrity SID has no subauthority".into());
        }
        let process_integrity_rid = unsafe { *GetSidSubAuthority(sid, u32::from(count - 1)) };
        Ok(ControllerIdentity {
            process_id: std::process::id(),
            process_user_sid,
            effective_user_sid,
            thread_impersonating,
            session_id,
            process_integrity_rid,
            process_elevated,
            process_app_container,
        })
    }

    fn ordinary_controller() -> Result<ControllerIdentity, String> {
        let controller = controller_identity()?;
        eprintln!("owned_profile_controller={}", json!(&controller));
        if controller.thread_impersonating
            || controller.process_app_container
            || controller.process_elevated
            || controller.session_id == 0
            || controller.process_integrity_rid != 8192
        {
            return Err("owned profile test requires an unimpersonated ordinary interactive medium-integrity controller".into());
        }
        Ok(controller)
    }

    fn write_evidence(path: &Path, record: &Value) -> Result<(), String> {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path)
            .map_err(|error| format!("create evidence {}: {error}", path.display()))?;
        serde_json::to_writer_pretty(&mut file, record)
            .map_err(|error| format!("serialize evidence {}: {error}", path.display()))?;
        file.write_all(b"\n")
            .and_then(|_| file.flush())
            .and_then(|_| file.sync_all())
            .map_err(|error| format!("persist evidence {}: {error}", path.display()))
    }

    struct Evidence {
        directory: PathBuf,
        controller: ControllerIdentity,
        planned_path: PathBuf,
    }

    impl Evidence {
        fn prepare(name: &str, controller: ControllerIdentity) -> Result<Self, String> {
            let directory = PathBuf::from(
                std::env::var_os("KLAUS_WINDOWS_OWNED_PROFILE_EVIDENCE_DIR")
                    .ok_or("an explicit KLAUS_WINDOWS_OWNED_PROFILE_EVIDENCE_DIR is required")?,
            );
            if !directory.is_absolute() || !directory.is_dir() {
                return Err(
                    "profile evidence directory must be an existing absolute directory".into(),
                );
            }
            let directory = directory
                .canonicalize()
                .map_err(|error| error.to_string())?;
            let planned_path = directory.join(format!("owned-profile-planned-{name}.json"));
            let record = json!({
                "schemaVersion": 1,
                "status": "planned",
                "profileName": name,
                "profileCreated": false,
                "controller": &controller,
            });
            // Prove that structured evidence can be persisted before the first
            // profile call. A later write failure still has this exact identity.
            write_evidence(&planned_path, &record)?;
            eprintln!("owned_profile_planned_evidence={}", planned_path.display());
            Ok(Self {
                directory,
                controller,
                planned_path,
            })
        }
    }

    fn close_owned_profile(
        profile: &mut OwnedAppContainerProfile,
        phase: &str,
        evidence: &Evidence,
    ) -> Result<(), String> {
        let mut attempts = Vec::new();
        for attempt in 1..=MAX_CLOSE_ATTEMPTS {
            let result = profile.close();
            let entry = json!({
                "attempt": attempt,
                "error": result.as_ref().err().map(ToString::to_string),
                "ownedAfterAttempt": profile.is_owned(),
            });
            eprintln!(
                "owned_profile_close_attempt={}",
                json!({
                    "profileName": profile.name(),
                    "creatorUserSid": profile.creator_user_sid(),
                    "phase": phase,
                    "result": &entry,
                })
            );
            attempts.push(entry);
            if result.is_ok() && !profile.is_owned() {
                return Ok(());
            }
            if attempt < MAX_CLOSE_ATTEMPTS {
                std::thread::sleep(CLOSE_RETRY_DELAY);
            }
        }
        let observed_controller = match controller_identity() {
            Ok(controller) => json!({ "identity": controller }),
            Err(error) => json!({ "identityUnavailable": error }),
        };
        let record = json!({
            "schemaVersion": 1,
            "status": "cleanup-unconfirmed",
            "profileName": profile.name(),
            "creatorUserSid": profile.creator_user_sid(),
            "profileOwnershipRetained": profile.is_owned(),
            "phase": phase,
            "controllerAtStart": &evidence.controller,
            "controllerAtCleanup": observed_controller,
            "plannedEvidence": &evidence.planned_path,
            "attempts": attempts,
            "recordedUnixMillis": SystemTime::now().duration_since(UNIX_EPOCH).ok().map(|elapsed| elapsed.as_millis()),
        });
        // Preserve the same structured record in captured stderr even if the
        // evidence filesystem fails after its successful preflight write.
        eprintln!("owned_profile_cleanup_unconfirmed={record}");
        let path = evidence.directory.join(format!(
            "cleanup-unconfirmed-{}-{}.json",
            profile.name(),
            uuid::Uuid::new_v4().simple(),
        ));
        let saved = write_evidence(&path, &record);
        Err(format!(
            "profile cleanup remains unconfirmed: name={}, creator_user_sid={}, evidence={}, write_result={saved:?}",
            profile.name(), profile.creator_user_sid(), path.display(),
        ))
    }

    fn creation_failure(failure: ProfileCreateError, evidence: &Evidence) -> String {
        let (error, recovery) = failure.into_parts();
        if let Some(mut profile) = recovery {
            let cleanup = close_owned_profile(&mut profile, "create-recovery", evidence);
            format!("{error}; created-profile cleanup: {cleanup:?}")
        } else {
            error.to_string()
        }
    }

    fn expect_collision(name: &str, evidence: &Evidence) -> Result<(), String> {
        match OwnedAppContainerProfile::create(name) {
            Err(failure) => {
                let (error, recovery) = failure.into_parts();
                let is_collision = matches!(error, IdentityError::ProfileAlreadyExists { .. });
                let had_recovery = recovery.is_some();
                let cleanup = if let Some(mut unexpected) = recovery {
                    close_owned_profile(&mut unexpected, "unexpected-collision-recovery", evidence)
                } else {
                    Ok(())
                };
                if !is_collision || had_recovery {
                    return Err(format!(
                        "collision did not reject ownership: {error}; cleanup={cleanup:?}"
                    ));
                }
                Ok(())
            }
            Ok(mut unexpected) => {
                let cleanup =
                    close_owned_profile(&mut unexpected, "unexpected-collision-create", evidence);
                Err(format!(
                    "duplicate creation unexpectedly succeeded; cleanup: {cleanup:?}"
                ))
            }
        }
    }

    fn lifecycle() -> Result<(), String> {
        let controller = ordinary_controller()?;
        let name = format!("klaus-exec-{}", uuid::Uuid::new_v4().simple());
        let evidence = Evidence::prepare(&name, controller)?;
        let mut profile = OwnedAppContainerProfile::create(&name)
            .map_err(|failure| creation_failure(failure, &evidence))?;
        eprintln!(
            "owned_profile_created={}",
            json!({ "name": name, "creatorUserSid": profile.creator_user_sid() })
        );
        let check = (|| -> Result<(), String> {
            if profile.creator_user_sid() != evidence.controller.process_user_sid {
                return Err(
                    "profile creator does not match the independently queried TokenUser SID".into(),
                );
            }
            let sid = profile.sid().ok_or("created profile has no SID")?;
            let derived = identity::derive_sid(&name).map_err(|error| error.to_string())?;
            if unsafe { EqualSid(sid.as_psid(), derived.as_psid()) } == 0 {
                return Err("created and derived SIDs differ".into());
            }
            expect_collision(&name, &evidence)?;
            // Derivation cannot prove registration remains. A second collision
            // checks that the failed attempt did not delete the first profile.
            expect_collision(&name, &evidence)
        })();
        let cleanup = close_owned_profile(&mut profile, "first-owner", &evidence);
        if check.is_err() || cleanup.is_err() {
            return Err(format!(
                "profile lifecycle check={check:?}; cleanup={cleanup:?}"
            ));
        }
        if profile.is_owned() || profile.sid().is_some() {
            return Err("successful close retained the profile ownership state".into());
        }
        profile.close().map_err(|error| error.to_string())?;

        // A new exclusive create proves that the first registration was removed.
        let mut recreated = OwnedAppContainerProfile::create(&name)
            .map_err(|failure| creation_failure(failure, &evidence))?;
        close_owned_profile(&mut recreated, "recreated-owner", &evidence)?;
        let complete_path = evidence
            .directory
            .join(format!("owned-profile-complete-{name}.json"));
        write_evidence(
            &complete_path,
            &json!({
                "schemaVersion": 1,
                "status": "cleanup-confirmed",
                "profileName": name,
                "creatorUserSid": recreated.creator_user_sid(),
                "controllerAtStart": &evidence.controller,
                "plannedEvidence": &evidence.planned_path,
            }),
        )?;
        eprintln!(
            "owned_profile_complete_evidence={}",
            complete_path.display()
        );
        Ok(())
    }

    #[test]
    #[ignore = "explicit ordinary-token profile lifecycle; exact selection and opt-in environment required"]
    fn owned_profile_creation_collision_and_close_are_explicit() {
        assert_eq!(
            std::env::var("KLAUS_WINDOWS_OWNED_PROFILE_LIFECYCLE")
                .ok()
                .as_deref(),
            Some("1"),
            "explicit opt-in is required before creating resources",
        );
        let module = module_path!().split_once("::").unwrap().1;
        let name = format!("{module}::owned_profile_creation_collision_and_close_are_explicit");
        let arguments = std::env::args().collect::<Vec<_>>();
        assert!(
            arguments.iter().any(|argument| argument == "--exact")
                && arguments
                    .iter()
                    .filter(|argument| *argument == &name)
                    .count()
                    == 1,
            "exact test-name selection is required",
        );
        let result = lifecycle();
        assert!(result.is_ok(), "{result:?}");
    }
}
