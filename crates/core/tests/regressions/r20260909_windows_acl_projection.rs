//! Defect: overlapping executions lost each other's cutout permissions, while
//! a parent's delete-child grant could bypass one execution's protected child.
//! Root cause: per-execution ACL edits did not project every active policy.
//! Regression for the 2026-09-09 Windows ACL policy projection work item.
//!
//! These public vectors pin policy masks and execution membership. They perform
//! no filesystem or Windows operations and do not validate a native ACL writer.

use klaus_sandbox_win::acl::{Grant, Rule};
use klaus_sandbox_win::acl_projection::{
    project, projection_paths, ExecutionGrant, ExecutionPolicy, ObjectProjection, ProjectionError,
};
use klaus_sandbox_win::AbsPath;

const A: &str = "S-1-15-2-1-2-3-4-5-6-7";
const B: &str = "S-1-15-2-11-12-13-14-15-16-17";

// Fixed policy vectors, independent of the implementation's mask constructor.
const WALK: u32 = 0x0012_00a1;
const READ: u32 = 0x0012_00a9;
const READ_WRITE: u32 = 0x0013_01ff;
const READ_WRITE_WITHOUT_DELETE_CHILD: u32 = 0x0013_01bf;

fn abs(path: &str) -> AbsPath {
    AbsPath::new(path).expect("the vector uses an absolute Windows path")
}

fn allow(path: &str, grant: Grant, inheritable: bool) -> Rule {
    Rule::Allow {
        path: abs(path),
        grant,
        inheritable,
    }
}

fn withhold(path: &str, keep: Option<Grant>) -> Rule {
    Rule::Withhold {
        path: abs(path),
        keep,
    }
}

fn policy(id: &str, sid: &str, rules: Vec<Rule>) -> ExecutionPolicy {
    ExecutionPolicy {
        execution_id: id.into(),
        package_sid: sid.into(),
        rules,
    }
}

fn grant(id: &str, sid: &str, local_mask: u32, inherited_mask: u32) -> ExecutionGrant {
    ExecutionGrant {
        execution_id: id.into(),
        package_sid: sid.into(),
        local_mask,
        inherited_mask,
    }
}

fn expect(
    policies: &[ExecutionPolicy],
    path: &str,
    grants: Vec<ExecutionGrant>,
    protection_required: bool,
) {
    assert_eq!(
        project(policies, &abs(path)).expect("the execution identities are unique"),
        ObjectProjection {
            grants,
            protection_required,
        },
        "projection at {path}"
    );
}

#[test]
fn metadata_override_keeps_b_writable_during_a_and_after_a_exits() {
    let a = policy(
        "a",
        A,
        vec![
            allow("C:\\ws", Grant::ReadWrite, true),
            withhold("C:\\ws\\.git", Some(Grant::Read)),
        ],
    );
    // A metadata override omits B's Withhold; B's parent allow remains authoritative.
    let b = policy("b", B, vec![allow("C:\\ws", Grant::ReadWrite, true)]);
    let active = [b.clone(), a];
    expect(
        &active,
        "C:\\ws\\.git",
        vec![
            grant("a", A, READ, READ),
            grant("b", B, READ_WRITE, READ_WRITE),
        ],
        true,
    );
    expect(
        &active,
        "C:\\ws\\.git\\config",
        vec![
            grant("a", A, READ, READ),
            grant("b", B, READ_WRITE, READ_WRITE),
        ],
        false,
    );
    for path in ["C:\\ws", "C:\\ws\\.git", "C:\\ws\\.git\\config"] {
        expect(
            std::slice::from_ref(&b),
            path,
            vec![grant("b", B, READ_WRITE, READ_WRITE)],
            false,
        );
    }
}

#[test]
fn deny_write_and_deny_read_cap_every_allow_regardless_of_order_or_specificity() {
    let rules = vec![
        withhold("C:\\ws\\private", Some(Grant::Read)),
        allow("C:\\ws\\private\\editable", Grant::ReadWrite, true),
        withhold("C:\\ws\\private\\secret", None),
        allow("C:\\ws", Grant::ReadWrite, true),
        allow("C:\\ws\\private\\secret\\data", Grant::ReadWrite, true),
    ];
    let mut reversed = rules.clone();
    reversed.reverse();
    for rules in [rules, reversed] {
        let active = [policy("a", A, rules)];
        for (path, mask, protected) in [
            ("C:\\ws\\private", READ, true),
            ("C:\\ws\\private\\editable", READ, false),
            ("C:\\ws\\private\\editable\\file", READ, false),
            ("C:\\ws\\private\\secret", 0, true),
            ("C:\\ws\\private\\secret\\data", 0, false),
        ] {
            expect(&active, path, vec![grant("a", A, mask, mask)], protected);
        }
    }
}

#[test]
fn noninheritable_walk_and_write_grants_apply_only_to_the_named_object() {
    let active = [policy(
        "a",
        A,
        vec![
            allow("C:\\Users", Grant::Walk, false),
            allow("C:\\scratch", Grant::ReadWrite, false),
        ],
    )];
    expect(&active, "C:\\Users", vec![grant("a", A, WALK, 0)], false);
    expect(
        &active,
        "C:\\scratch",
        vec![grant("a", A, READ_WRITE, 0)],
        false,
    );
    for path in ["C:\\Users\\alice", "C:\\scratch\\child", "C:\\Users-old"] {
        expect(&active, path, vec![grant("a", A, 0, 0)], false);
    }
}

#[test]
fn exact_local_write_does_not_upgrade_an_inheritable_read_grant() {
    let active = [policy(
        "a",
        A,
        vec![
            allow("C:\\ws", Grant::Read, true),
            allow("C:\\ws\\local", Grant::ReadWrite, false),
        ],
    )];
    expect(
        &active,
        "C:\\ws\\local",
        vec![grant("a", A, READ_WRITE, READ)],
        false,
    );
    expect(
        &active,
        "C:\\ws\\local\\child",
        vec![grant("a", A, READ, READ)],
        false,
    );
}

#[test]
fn only_the_direct_parent_loses_a_local_delete_child_bit() {
    let active = [
        policy(
            "a",
            A,
            vec![
                allow("C:\\ws", Grant::ReadWrite, true),
                withhold("C:\\ws\\nested\\.git", Some(Grant::Read)),
            ],
        ),
        policy("b", B, vec![allow("C:\\ws", Grant::ReadWrite, true)]),
    ];
    expect(
        &active,
        "C:\\ws\\nested",
        vec![
            grant("a", A, READ_WRITE_WITHOUT_DELETE_CHILD, READ_WRITE),
            grant("b", B, READ_WRITE, READ_WRITE),
        ],
        true,
    );
    for path in [
        "C:\\ws",
        "C:\\ws\\nested\\ordinary.txt",
        "C:\\ws\\nested\\ordinary-directory",
        "C:\\ws\\nested\\ordinary-directory\\file",
    ] {
        expect(
            &active,
            path,
            vec![
                grant("a", A, READ_WRITE, READ_WRITE),
                grant("b", B, READ_WRITE, READ_WRITE),
            ],
            false,
        );
    }
}

#[test]
fn retain_read_never_creates_access_without_an_applicable_read_root() {
    let active = [policy(
        "a",
        A,
        vec![
            withhold("C:\\private", Some(Grant::Read)),
            allow("C:\\walk", Grant::Walk, false),
            withhold("C:\\walk", Some(Grant::Read)),
            allow("C:\\local", Grant::Read, false),
            withhold("C:\\local\\child", Some(Grant::Read)),
        ],
    )];
    expect(&active, "C:\\private", vec![grant("a", A, 0, 0)], true);
    expect(
        &active,
        "C:\\private\\file",
        vec![grant("a", A, 0, 0)],
        false,
    );
    expect(&active, "C:\\walk", vec![grant("a", A, WALK, 0)], true);
    expect(&active, "C:\\walk\\file", vec![grant("a", A, 0, 0)], false);
    expect(&active, "C:\\local\\child", vec![grant("a", A, 0, 0)], true);
}

#[test]
fn read_only_parent_does_not_need_additional_delete_child_protection() {
    let active = [policy(
        "a",
        A,
        vec![
            allow("C:\\ws", Grant::Read, true),
            withhold("C:\\ws\\private", None),
        ],
    )];
    expect(&active, "C:\\ws", vec![grant("a", A, READ, READ)], false);
    expect(&active, "C:\\ws\\private", vec![grant("a", A, 0, 0)], true);
}

#[test]
fn read_write_ceiling_does_not_require_protection_or_reduce_parent_deletion() {
    let active = [policy(
        "a",
        A,
        vec![
            allow("C:\\ws", Grant::ReadWrite, true),
            withhold("C:\\ws\\open", Some(Grant::ReadWrite)),
        ],
    )];
    for path in ["C:\\ws", "C:\\ws\\open", "C:\\ws\\open\\file"] {
        expect(
            &active,
            path,
            vec![grant("a", A, READ_WRITE, READ_WRITE)],
            false,
        );
    }
}

#[test]
fn overlapping_withholds_intersect_instead_of_selecting_the_nearest_one() {
    let active = [policy(
        "a",
        A,
        vec![
            allow("C:\\ws", Grant::ReadWrite, true),
            withhold("C:\\ws\\private", None),
            withhold("C:\\ws\\private\\readable", Some(Grant::Read)),
            withhold("C:\\ws\\private\\readable\\open", Some(Grant::ReadWrite)),
        ],
    )];
    expect(
        &active,
        "C:\\ws\\private\\readable",
        vec![grant("a", A, 0, 0)],
        true,
    );
    expect(
        &active,
        "C:\\ws\\private\\readable\\open",
        vec![grant("a", A, 0, 0)],
        false,
    );
}

#[test]
fn containment_is_case_insensitive_and_respects_components_and_volume() {
    let active = [policy(
        "a",
        A,
        vec![allow("C:\\Workspace", Grant::Read, true)],
    )];
    expect(
        &active,
        "c:\\workspace\\file",
        vec![grant("a", A, READ, READ)],
        false,
    );
    for path in ["C:\\Workspace-old\\file", "D:\\Workspace\\file"] {
        expect(&active, path, vec![grant("a", A, 0, 0)], false);
    }
}

#[test]
fn projection_candidates_include_direct_cutout_parents_without_system_ancestors() {
    let active = [policy(
        "a",
        A,
        vec![
            allow("C:\\ws", Grant::ReadWrite, true),
            withhold("C:\\ws\\nested\\.git", Some(Grant::Read)),
            allow("c:\\WS", Grant::Read, true),
            withhold("C:\\other\\open", Some(Grant::ReadWrite)),
        ],
    )];
    assert_eq!(
        projection_paths(&active).unwrap(),
        vec![
            abs("C:\\other\\open"),
            abs("C:\\ws"),
            abs("C:\\ws\\nested"),
            abs("C:\\ws\\nested\\.git"),
        ]
    );
}

#[test]
fn repeated_execution_attachments_must_be_combined_before_projection() {
    for second_sid in [A, B] {
        let active = [
            policy("a", A, vec![allow("C:\\support", Grant::Read, true)]),
            policy(
                "a",
                second_sid,
                vec![allow("C:\\ws", Grant::ReadWrite, true)],
            ),
        ];
        let error = ProjectionError::DuplicateExecutionId {
            execution_id: "a".into(),
        };
        assert_eq!(project(&active, &abs("C:\\ws")), Err(error.clone()));
        assert_eq!(projection_paths(&active), Err(error));
    }
}

#[test]
fn one_combined_execution_policy_retains_both_support_and_workspace_rules() {
    let active = [policy(
        "a",
        A,
        vec![
            allow("C:\\support", Grant::Read, true),
            allow("C:\\ws", Grant::ReadWrite, true),
        ],
    )];
    expect(
        &active,
        "C:\\support\\tool.exe",
        vec![grant("a", A, READ, READ)],
        false,
    );
    expect(
        &active,
        "C:\\ws\\file",
        vec![grant("a", A, READ_WRITE, READ_WRITE)],
        false,
    );
}

#[test]
fn two_executions_cannot_share_one_package_sid() {
    let active = [policy("a", A, vec![]), policy("b", A, vec![])];
    let error = ProjectionError::DuplicatePackageSid {
        package_sid: A.into(),
    };
    assert_eq!(project(&active, &abs("C:\\ws")), Err(error.clone()));
    assert_eq!(projection_paths(&active), Err(error));
}

#[test]
fn no_active_execution_means_no_managed_grant_or_projection_candidate() {
    expect(&[], "C:\\ws", vec![], false);
    assert!(projection_paths(&[]).unwrap().is_empty());
}
