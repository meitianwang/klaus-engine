//! Defect: AppContainer profile API outputs were released with LocalFree rather
//! than their required FreeSid, and error outputs had no owner before HRESULT
//! validation. Regression for the 2026-09-09 AppContainer SID ownership work item.
//!
//! Exercise public derivation, borrowing, ownership transfer, and destruction
//! without creating a profile, changing an ACL, or launching a sandbox. This is
//! native lifecycle coverage, not an allocator-call oracle: a Windows version
//! may implement both release APIs on the same heap, so the old implementation
//! is not guaranteed to fail this test. Non-null failure outputs are protected
//! structurally by owning the API output slot before the call; no OS failure
//! injection or public test-only API is introduced for that branch.

#![cfg(windows)]

use klaus_sandbox_win::identity::{derive_sid, AppContainerSid};
use windows_sys::Win32::Security::{EqualSid, GetLengthSid, IsValidSid};

fn assert_valid(sid: &AppContainerSid) {
    assert!(!sid.as_psid().is_null());
    assert_ne!(unsafe { IsValidSid(sid.as_psid()) }, 0);
    assert!(unsafe { GetLengthSid(sid.as_psid()) } > 0);
}

#[test]
fn unregistered_appcontainer_sid_survives_owner_transfer_and_repeated_drop() {
    let name = format!("klaus-sid-owner-{}", uuid::Uuid::new_v4());
    let retained = derive_sid(&name).expect("derive without creating a profile");
    assert_valid(&retained);

    // Each iteration owns a separate API allocation for the same identity. Its
    // destruction must leave the independently retained SID valid and usable.
    for _ in 0..32 {
        let temporary = derive_sid(&name).expect("derive a separate owned SID");
        assert_valid(&temporary);
        assert_ne!(
            unsafe { EqualSid(retained.as_psid(), temporary.as_psid()) },
            0,
        );
        drop(temporary);
        assert_valid(&retained);
    }

    // AppContainerSid is Send: its native allocation belongs to the value, not
    // to the thread that obtained it. Borrow and destroy it on its new owner.
    std::thread::spawn(move || {
        assert_valid(&retained);
        drop(retained);
    })
    .join()
    .expect("transferred SID owner must finish");

    let after = derive_sid(&name).expect("derive after all earlier owners ended");
    assert_valid(&after);
    drop(after);
}
