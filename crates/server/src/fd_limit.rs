//! The process's open-file budget.
//!
//! Every entry point in this workspace raises `RLIMIT_NOFILE` before it builds a
//! runtime, because the engine's steady state needs FAR more descriptors than a
//! GUI-launched process inherits. Measured on macOS, where launchd hands a
//! desktop app a soft limit of 256: one engine process reached that ceiling in
//! 23 minutes of ordinary use (per-session pinned memory directories, stdio MCP
//! servers, session lock files, provider sockets, plus the Qoder subscription's
//! in-process loopback bridge). At the ceiling `accept()`, `socketpair()` and
//! every file open start failing with EMFILE, which took DNS down with them and
//! killed the bridge's listener for the rest of the process's life.
//!
//! The kernel ceiling is not the problem: that machine's `kern.maxfilesperproc`
//! is 184320. The process simply never asked for the budget it needs, so this
//! asks once, at startup, before anything can spend a descriptor.

/// The soft limit every entry point asks for. Well under the macOS
/// `kern.maxfilesperproc` default and under the usual Linux hard limits, so the
/// request normally succeeds in one call; generous enough that fan-out across
/// sub-agents, MCP servers and provider sockets cannot exhaust it.
#[cfg(unix)]
const DESIRED_SOFT_LIMIT: u64 = 65_536;

/// The soft limit to ask for, given the pair the process starts with. `None`
/// means the current soft limit already covers the target — a raise must never
/// LOWER a limit that a parent (a shell with `ulimit -n`, a container) set
/// higher on purpose.
#[cfg(unix)]
fn desired_soft_limit(soft: u64, hard: u64) -> Option<u64> {
    let target = DESIRED_SOFT_LIMIT.min(hard);
    (target > soft).then_some(target)
}

/// Raise this process's open-file soft limit toward [`DESIRED_SOFT_LIMIT`] and
/// answer the limit in force afterwards (0 when it cannot be read).
///
/// The hard limit is left alone: raising it needs privilege this process does
/// not have, and the soft limit is the one that bites. A rejected request is
/// halved and retried, because a platform may cap the soft limit below its own
/// reported hard limit (macOS caps at `kern.maxfilesperproc`, which is not what
/// `getrlimit` reports as the hard limit — that answers `RLIM_INFINITY`).
#[cfg(unix)]
pub fn raise_open_file_limit() -> u64 {
    let mut current = libc::rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };
    if unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut current) } != 0 {
        return 0;
    }
    let soft = current.rlim_cur;
    let hard = current.rlim_max;
    let Some(mut target) = desired_soft_limit(soft, hard) else {
        return soft;
    };
    while target > soft {
        let attempt = libc::rlimit {
            rlim_cur: target as libc::rlim_t,
            rlim_max: current.rlim_max,
        };
        if unsafe { libc::setrlimit(libc::RLIMIT_NOFILE, &attempt) } == 0 {
            return target;
        }
        target /= 2;
    }
    soft
}

/// Windows has no per-process descriptor limit to raise: sockets are handles
/// bounded by memory, and the CRT's `_setmaxstdio` governs only the stdio FILE
/// table, which nothing here uses.
#[cfg(not(unix))]
pub fn raise_open_file_limit() -> u64 {
    0
}

// Everything under test here is the rlimit request itself, which only exists on
// unix: the Windows `raise_open_file_limit` has no limit to compute.
#[cfg(all(test, unix))]
mod tests {
    use super::*;

    #[test]
    fn a_gui_inherited_ceiling_is_raised_to_the_target() {
        // The measured macOS case: launchd's 256 with an effectively unbounded
        // hard limit.
        assert_eq!(
            desired_soft_limit(256, u64::MAX),
            Some(DESIRED_SOFT_LIMIT),
            "a 256 soft limit is what exhausted the engine in 23 minutes"
        );
    }

    #[test]
    fn a_hard_limit_below_the_target_bounds_the_request() {
        assert_eq!(desired_soft_limit(256, 4_096), Some(4_096));
    }

    #[test]
    fn an_already_generous_limit_is_never_lowered() {
        assert_eq!(
            desired_soft_limit(1_048_575, u64::MAX),
            None,
            "a parent that raised the limit past the target must keep it"
        );
        assert_eq!(desired_soft_limit(DESIRED_SOFT_LIMIT, u64::MAX), None);
    }

    #[test]
    fn raising_never_lowers_the_limit_in_force() {
        let mut before = libc::rlimit {
            rlim_cur: 0,
            rlim_max: 0,
        };
        assert_eq!(
            unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut before) },
            0
        );
        let after = raise_open_file_limit();
        assert!(
            after >= before.rlim_cur,
            "the limit dropped from {} to {after}",
            before.rlim_cur
        );
    }
}
