//! The command surface, pinned.
//!
//! `clap`'s derive output is only checked at runtime, so a bad `conflicts_with`
//! name or a duplicate short flag is a panic on first use rather than a compile
//! error. `debug_assert()` walks the whole tree and catches exactly that class,
//! which is why it belongs in a test and not behind a feature nobody runs.
//!
//! The rest of this file guards properties of the surface that a reader would
//! reasonably assume and that nothing else enforces: that every command says what
//! it does, that a global flag works wherever a subcommand does, and that the
//! flags offering a fixed set of values offer the engine's whole set.

use klaus_agent_cli::cli::{Cli, PermissionModeArg, QueuePriorityArg};
use klaus_agent_core::types::permission::ExternalPermissionMode;
use klaus_agent_protocol::{QueuePriority, SteerPriority};

use clap::{CommandFactory, Parser, ValueEnum};

#[test]
fn command_tree_is_well_formed() {
    Cli::command().debug_assert();
}

/// Every subcommand and every positional argument carries help text.
///
/// A blank description in `--help` is indistinguishable from a command nobody
/// finished, and the derive gives no warning for a missing doc comment. Walking
/// the built tree catches it for the whole surface at once rather than whenever
/// someone happens to run the right `--help`.
#[test]
fn every_command_and_positional_is_documented() {
    let mut undocumented = Vec::new();
    walk(Cli::command(), &mut Vec::new(), &mut undocumented);
    assert!(
        undocumented.is_empty(),
        "undocumented command surface:\n  {}",
        undocumented.join("\n  ")
    );
}

fn walk(command: clap::Command, path: &mut Vec<String>, undocumented: &mut Vec<String>) {
    let here = if path.is_empty() {
        "klaus".to_string()
    } else {
        format!("klaus {}", path.join(" "))
    };
    for argument in command.get_positionals() {
        if argument.get_help().is_none() {
            undocumented.push(format!("{here}: argument <{}>", argument.get_id()));
        }
    }
    for child in command.get_subcommands() {
        // `help` is clap's own, and clap writes its description itself.
        if child.get_name() == "help" {
            continue;
        }
        if child.get_about().is_none() {
            undocumented.push(format!("{here} {}", child.get_name()));
        }
        path.push(child.get_name().to_string());
        walk(child.clone(), path, undocumented);
        path.pop();
    }
}

/// A global flag parses in front of every subcommand.
///
/// This is a regression guard with a specific history: the root used to carry
/// `args_conflicts_with_subcommands`, which made `--json` and every other global
/// mutually exclusive with naming a subcommand at all — so the flags existed but
/// could not be used with the commands they configure. Checking one flag against
/// the whole leaf set is what catches that, because the breakage was uniform and
/// therefore invisible in any single command's help.
#[test]
fn globals_parse_alongside_every_subcommand() {
    let mut refused = Vec::new();
    for leaf in leaves(&Cli::command(), &mut Vec::new()) {
        let mut argv = vec!["klaus".to_string(), "--json".to_string()];
        argv.extend(leaf.iter().cloned());
        argv.extend(placeholders(&leaf));
        if let Err(error) = Cli::try_parse_from(&argv) {
            // A required argument this test did not guess is not the failure
            // being looked for; a rejected `--json` is.
            if error.kind() == clap::error::ErrorKind::MissingRequiredArgument {
                continue;
            }
            refused.push(format!("{}: {:?}", leaf.join(" "), error.kind()));
        }
    }
    assert!(
        refused.is_empty(),
        "`--json` refused alongside:\n  {}",
        refused.join("\n  ")
    );
}

/// Every path from the root to a command that takes no further subcommand.
fn leaves(command: &clap::Command, path: &mut Vec<String>) -> Vec<Vec<String>> {
    let children: Vec<_> = command
        .get_subcommands()
        .filter(|child| child.get_name() != "help")
        .collect();
    if children.is_empty() {
        return vec![path.clone()];
    }
    let mut found = Vec::new();
    for child in children {
        path.push(child.get_name().to_string());
        found.extend(leaves(child, path));
        path.pop();
    }
    found
}

/// Stand-ins for a leaf's required positionals, so the parse reaches the flag
/// check instead of stopping at a missing argument.
fn placeholders(leaf: &[String]) -> Vec<String> {
    let mut command = Cli::command();
    for name in leaf {
        // Cloned out of the iterator before `command` is reassigned: the borrow
        // that found the child ends with the statement, and holding it across
        // the assignment would not compile.
        let child = command
            .get_subcommands()
            .find(|child| child.get_name() == name)
            .cloned();
        match child {
            Some(child) => command = child,
            None => return Vec::new(),
        }
    }
    command
        .get_positionals()
        .filter(|argument| argument.is_required_set())
        .map(|argument| match argument.get_possible_values().first() {
            // An argument with a fixed set of values rejects anything outside it,
            // and that rejection would read as `--json` being refused. Its own
            // first value is always accepted.
            Some(value) => value.get_name().to_string(),
            None => "placeholder".to_string(),
        })
        .collect()
}

/// The hidden compatibility flag accepts each historical preset. Runtime
/// initialization translates these values to canonical policy fields.
#[test]
fn deprecated_permission_flag_accepts_historical_presets() {
    let offered: Vec<ExternalPermissionMode> = PermissionModeArg::value_variants()
        .iter()
        .map(|variant| ExternalPermissionMode::from(*variant))
        .collect();
    for mode in [
        ExternalPermissionMode::Default,
        ExternalPermissionMode::Plan,
        ExternalPermissionMode::AcceptEdits,
        ExternalPermissionMode::Auto,
        ExternalPermissionMode::BypassPermissions,
        ExternalPermissionMode::DontAsk,
    ] {
        // Keep compatibility decoding exhaustive when the historical enum changes.
        match mode {
            ExternalPermissionMode::Default
            | ExternalPermissionMode::Plan
            | ExternalPermissionMode::AcceptEdits
            | ExternalPermissionMode::Auto
            | ExternalPermissionMode::BypassPermissions
            | ExternalPermissionMode::DontAsk => (),
        }
        assert!(
            offered.contains(&mode),
            "the compatibility flag does not decode {mode:?}"
        );
    }
}

/// `--priority` covers every priority, on both enums it feeds.
///
/// Two conversions exist because the queue's own mutations and `steer` take
/// separate priority types, and one flag drives both. A variant reachable through
/// one and not the other would make `queue add --priority X` and
/// `queue edit --priority X` mean different things.
#[test]
fn queue_priority_flag_offers_every_priority() {
    let variants = QueuePriorityArg::value_variants();
    assert_eq!(variants.len(), 3, "priorities changed: {variants:?}");
    for variant in variants {
        // No wildcard arm in either: a new priority on either enum has to be
        // answered here, at compile time.
        match QueuePriority::from(*variant) {
            QueuePriority::Now | QueuePriority::Next | QueuePriority::Later => (),
        }
        match SteerPriority::from(*variant) {
            SteerPriority::Now | SteerPriority::Next | SteerPriority::Later => (),
        }
    }
}

#[test]
fn canonical_policy_flags_are_independent_cli_inputs() {
    let parsed = Cli::try_parse_from([
        "klaus",
        "--sandbox",
        "danger-full-access",
        "--ask-for-approval",
        "never",
        "--approvals-reviewer",
        "user",
        "exec",
        "Inspect the project",
    ])
    .unwrap();
    assert!(matches!(
        parsed.globals.sandbox_mode,
        Some(klaus_agent_cli::cli::SandboxModeArg::DangerFullAccess)
    ));
    assert!(matches!(
        parsed.globals.approval_policy,
        Some(klaus_agent_cli::cli::ApprovalPolicyArg::Never)
    ));
    assert!(parsed.globals.permission_mode.is_none());
}
