use std::sync::Arc;
use std::time::Duration;

use super::windows_spawn::{
    startup_info_size, CreateProcessOutcome, FakeWindowsKernel, HiddenCleanupOutcome,
    WindowsConPtySize, WindowsControlStart, WindowsJobResources, WindowsSpawnError,
    WindowsSpawnFault, WindowsSpawnPlan, WindowsSpawnStage, WindowsSpawnSupervisor,
    WindowsStdioPlan, CREATE_SUSPENDED,
};
use super::WindowsJobPolicy;

fn ordinary_plan() -> WindowsSpawnPlan {
    WindowsSpawnPlan::for_test(
        0x0400_0000,
        WindowsJobPolicy::None,
        WindowsStdioPlan::pipes(),
        None,
    )
}

fn private_job_plan(policy: WindowsJobPolicy) -> WindowsSpawnPlan {
    WindowsSpawnPlan::for_test(
        0,
        policy,
        WindowsStdioPlan::pipes(),
        (policy == WindowsJobPolicy::LibuvGlobalWithControlTracker).then_some(0xfeed),
    )
}

fn spawn(
    kernel: Arc<FakeWindowsKernel>,
    plan: WindowsSpawnPlan,
) -> Result<super::windows_spawn::WindowsSpawnBundle, WindowsSpawnError> {
    WindowsSpawnSupervisor::with_kernel_for_test(kernel).spawn_test_image(plan)
}

fn position(events: &[WindowsSpawnStage], stage: WindowsSpawnStage) -> usize {
    events
        .iter()
        .position(|candidate| *candidate == stage)
        .unwrap_or_else(|| panic!("missing Windows spawn stage {stage:?}: {events:?}"))
}

#[test]
fn wait_cell_map_and_fence_are_reserved_before_create_process() {
    let kernel = Arc::new(FakeWindowsKernel::new());
    let spawned = spawn(kernel.clone(), ordinary_plan()).expect("spawn shadow");
    let events = kernel.events();
    let create = position(&events, WindowsSpawnStage::CreateProcessW);

    for stage in [
        WindowsSpawnStage::ReserveProcessMap,
        WindowsSpawnStage::ReserveCleanupFence,
        WindowsSpawnStage::AllocateStableWaitCell,
        WindowsSpawnStage::CreateThreadpoolWait,
        WindowsSpawnStage::PrepareStdio,
    ] {
        assert!(
            position(&events, stage) < create,
            "{stage:?} must precede CreateProcessW"
        );
    }
    assert_ne!(kernel.last_creation_flags() & CREATE_SUSPENDED, 0);
    drop(spawned);
}

#[test]
fn conpty_is_an_attribute_branch_of_the_same_suspended_transaction() {
    let kernel = Arc::new(FakeWindowsKernel::new());
    let supervisor = WindowsSpawnSupervisor::with_kernel_for_test(kernel.clone());
    let spawned = supervisor
        .spawn_test_image(
            ordinary_plan()
                .with_conpty(WindowsConPtySize::new(24, 80))
                .with_target_identity_lease(true),
        )
        .expect("ConPTY spawn");
    let identity = spawned.identity();
    let events = kernel.events();
    let create = position(&events, WindowsSpawnStage::CreateProcessW);

    assert!(position(&events, WindowsSpawnStage::PrepareConPty) < create);
    assert!(position(&events, WindowsSpawnStage::BuildPseudoConsoleAttribute) < create);
    assert_eq!(kernel.count(WindowsSpawnStage::PrepareStdio), 0);
    assert_eq!(kernel.count(WindowsSpawnStage::BuildHandleListAttribute), 0);
    assert!(!kernel.last_inherit_handles());
    assert_ne!(kernel.last_creation_flags() & CREATE_SUSPENDED, 0);

    kernel.signal_process(identity, 0);
    supervisor.dispatch_wait_for_test(identity);
    supervisor.drain_cleanup_for_test();
    drop(spawned);
    assert_eq!(kernel.count(WindowsSpawnStage::ClosePseudoConsole), 1);
    assert_eq!(kernel.open_handle_count(), 0);
}

#[test]
fn threadpool_wait_allocation_failure_is_a_named_no_spawn_error() {
    let kernel = Arc::new(FakeWindowsKernel::with_fault(WindowsSpawnFault::At(
        WindowsSpawnStage::CreateThreadpoolWait,
    )));
    let result = spawn(kernel.clone(), ordinary_plan());

    assert!(matches!(
        result,
        Err(WindowsSpawnError::PreSpawn {
            stage: WindowsSpawnStage::CreateThreadpoolWait,
            ..
        })
    ));
    assert_eq!(kernel.count(WindowsSpawnStage::CreateProcessW), 0);
    assert_eq!(kernel.open_handle_count(), 0);
}

#[test]
fn private_job_completion_and_attribute_resources_are_fail_closed_before_spawn() {
    for policy in [
        WindowsJobPolicy::BrowserSidecarPrivate,
        WindowsJobPolicy::LibuvGlobalWithControlTracker,
    ] {
        let kernel = Arc::new(FakeWindowsKernel::new());
        let spawned = spawn(kernel.clone(), private_job_plan(policy)).expect("spawn shadow");
        let events = kernel.events();
        let create = position(&events, WindowsSpawnStage::CreateProcessW);

        for stage in [
            WindowsSpawnStage::CreatePrivateJob,
            WindowsSpawnStage::ConfigurePrivateJob,
            WindowsSpawnStage::AllocateJobCompletionCell,
            WindowsSpawnStage::InsertJobCompletionCell,
            WindowsSpawnStage::BuildJobListAttribute,
        ] {
            assert!(
                position(&events, stage) < create,
                "{stage:?} must precede CreateProcessW"
            );
        }
        if policy == WindowsJobPolicy::LibuvGlobalWithControlTracker {
            assert!(position(&events, WindowsSpawnStage::DuplicateTargetSentinel) < create);
        } else {
            assert_eq!(kernel.count(WindowsSpawnStage::DuplicateTargetSentinel), 0);
        }
        assert!(position(&events, WindowsSpawnStage::BuildHandleListAttribute) < create);
        assert_eq!(
            spawned.job_resources(),
            if policy == WindowsJobPolicy::BrowserSidecarPrivate {
                WindowsJobResources::PrivateAtomic
            } else {
                WindowsJobResources::GlobalAndPrivateAtomic
            }
        );
        drop(spawned);
    }

    for failed_stage in [
        WindowsSpawnStage::ConfigurePrivateJob,
        WindowsSpawnStage::InsertJobCompletionCell,
        WindowsSpawnStage::BuildJobListAttribute,
        WindowsSpawnStage::DuplicateTargetSentinel,
        WindowsSpawnStage::BuildHandleListAttribute,
    ] {
        let policy = if matches!(
            failed_stage,
            WindowsSpawnStage::DuplicateTargetSentinel
                | WindowsSpawnStage::BuildHandleListAttribute
        ) {
            WindowsJobPolicy::LibuvGlobalWithControlTracker
        } else {
            WindowsJobPolicy::BrowserSidecarPrivate
        };
        let kernel = Arc::new(FakeWindowsKernel::with_fault(WindowsSpawnFault::At(
            failed_stage,
        )));
        let result = spawn(kernel.clone(), private_job_plan(policy));
        assert!(matches!(
            result,
            Err(WindowsSpawnError::PreSpawn { stage, .. }) if stage == failed_stage
        ));
        assert_eq!(kernel.count(WindowsSpawnStage::CreateProcessW), 0);
        assert_eq!(kernel.open_handle_count(), 0);
    }
}

#[test]
fn private_job_controller_terminates_the_retained_job_handle() {
    let kernel = Arc::new(FakeWindowsKernel::new());
    let supervisor = WindowsSpawnSupervisor::with_kernel_for_test(kernel.clone());
    let spawned = supervisor
        .spawn_test_image(private_job_plan(WindowsJobPolicy::BrowserSidecarPrivate))
        .expect("private-job spawn");
    let identity = spawned.identity();
    let completion_id = spawned.completion_id().expect("private completion ID");

    assert_eq!(
        spawned.controller().terminate_private_job(),
        super::windows_spawn::WindowsTerminateResult::Delivered
    );
    assert_eq!(kernel.count(WindowsSpawnStage::TerminatePrivateJob), 1);

    supervisor.dispatch_wait_for_test(identity);
    supervisor.drain_cleanup_for_test();
    drop(spawned);
    assert!(supervisor.observe_job_active_zero_for_test(completion_id));
    assert!(!supervisor.contains_registration(identity));
    assert_eq!(kernel.open_handle_count(), 0);
}

#[test]
fn private_job_repeated_triggers_invoke_terminate_job_exactly_once() {
    let kernel = Arc::new(FakeWindowsKernel::new());
    let supervisor = WindowsSpawnSupervisor::with_kernel_for_test(kernel.clone());
    let spawned = supervisor
        .spawn_test_image(private_job_plan(WindowsJobPolicy::BrowserSidecarPrivate))
        .expect("private-job spawn");
    let controller = spawned.controller();

    let _first = controller.terminate_private_job();
    let _second = controller.terminate_private_job();

    assert_eq!(
        kernel.count(WindowsSpawnStage::TerminatePrivateJob),
        1,
        "racing/repeated private-job triggers must join one receipt and never call TerminateJobObject twice"
    );
}

#[test]
fn target_identity_lease_is_bound_at_commit_and_pins_the_generation() {
    let kernel = Arc::new(FakeWindowsKernel::new());
    let supervisor = WindowsSpawnSupervisor::with_kernel_for_test(kernel.clone());
    let spawned = supervisor
        .spawn_test_image(ordinary_plan().with_target_identity_lease(true))
        .expect("tree-target spawn");
    let identity = spawned.identity();
    let controller = spawned.controller();
    let target = controller
        .target_identity_lease()
        .expect("commit-bound target identity lease");

    assert_eq!(target.pid(), identity.pid());
    kernel.signal_process(identity, 0);
    supervisor.dispatch_wait_for_test(identity);
    supervisor.drain_cleanup_for_test();
    drop(spawned);
    assert!(supervisor.contains_registration(identity));

    drop(controller);
    assert!(supervisor.contains_registration(identity));
    drop(target);
    assert!(!supervisor.contains_registration(identity));
    assert_eq!(kernel.open_handle_count(), 0);
}

#[test]
fn control_helper_retains_target_identity_until_helper_terminal_cleanup() {
    let kernel = Arc::new(FakeWindowsKernel::new());
    let supervisor = WindowsSpawnSupervisor::with_kernel_for_test(kernel.clone());
    let target = supervisor
        .spawn_test_image(ordinary_plan().with_target_identity_lease(true))
        .expect("tree target");
    let target_identity = target.identity();
    let target_lease = target
        .controller()
        .target_identity_lease()
        .expect("tree target identity");
    let helper = supervisor
        .spawn_test_control_image(ordinary_plan(), target_lease)
        .expect("taskkill control helper");
    let helper_identity = helper.identity();
    drop(helper);

    kernel.signal_process(target_identity, 0);
    supervisor.dispatch_wait_for_test(target_identity);
    drop(target);
    supervisor.drain_cleanup_for_test();
    assert!(
        supervisor.contains_registration(target_identity),
        "helper must pin the numeric target after its facade is gone"
    );

    kernel.signal_process(helper_identity, 1);
    supervisor.dispatch_wait_for_test(helper_identity);
    supervisor.drain_cleanup_for_test();
    assert!(!supervisor.contains_registration(target_identity));
    assert!(!supervisor.contains_registration(helper_identity));
    assert_eq!(kernel.open_handle_count(), 0);
}

#[test]
fn committed_control_helper_moves_receipt_to_settling_before_return() {
    let kernel = Arc::new(FakeWindowsKernel::new());
    let supervisor = WindowsSpawnSupervisor::with_kernel_for_test(kernel);
    let target = supervisor
        .spawn_test_image(ordinary_plan().with_target_identity_lease(true))
        .expect("tree target");
    let controller = target.controller();
    let action = match controller.begin_control(false) {
        WindowsControlStart::Execute(action) => action,
        WindowsControlStart::Join { receipt } => {
            panic!("fresh tree receipt unexpectedly joined {receipt}")
        }
        WindowsControlStart::Disarmed => panic!("fresh tree receipt unexpectedly disarmed"),
    };
    let receipt = action.receipt_for_test();

    let _helper = supervisor
        .spawn_test_action_control_image(ordinary_plan(), action)
        .expect("committed control helper");

    assert_eq!(
        controller.termination_state_for_test(),
        ("Settling", Some(receipt)),
        "successfully committed helper must publish Settling(receipt, deadline) before spawn returns"
    );
}

#[test]
fn shutdown_fixed_point_starts_one_armed_tree_receipt_before_internal_gate_closes() {
    let kernel = Arc::new(FakeWindowsKernel::new());
    let supervisor = WindowsSpawnSupervisor::with_kernel_for_test(kernel);
    let _target = supervisor
        .spawn_test_image(ordinary_plan().with_target_identity_lease(true))
        .expect("tree target");

    assert_eq!(supervisor.admission_counts_for_test(), (0, 1));
    let starts = supervisor.begin_shutdown_actions_for_test();

    assert_eq!(
        starts.len(),
        1,
        "shutdown fixed-point must start/join exactly one committed armed tree receipt before closing internal admission"
    );
    assert!(matches!(
        starts.as_slice(),
        [WindowsControlStart::Execute(_)]
    ));
    assert_eq!(
        supervisor.admission_counts_for_test(),
        (0, 1),
        "the converted executing permit must keep internal admission outstanding"
    );
}

#[test]
fn every_declared_stdio_handle_is_confined_by_an_explicit_handle_list() {
    for policy in [
        WindowsJobPolicy::None,
        WindowsJobPolicy::LibuvGlobal,
        WindowsJobPolicy::BrowserSidecarPrivate,
        WindowsJobPolicy::LibuvGlobalWithControlTracker,
    ] {
        let kernel = Arc::new(FakeWindowsKernel::new());
        let plan = if policy == WindowsJobPolicy::None {
            ordinary_plan()
        } else {
            private_job_plan(policy)
        };
        let spawned = spawn(kernel.clone(), plan).expect("spawn with explicit stdio list");
        let events = kernel.events();
        assert!(
            position(&events, WindowsSpawnStage::BuildHandleListAttribute)
                < position(&events, WindowsSpawnStage::CreateProcessW),
            "{policy:?} must bind stdio handles before CreateProcessW"
        );
        assert!(kernel.last_inherit_handles());
        drop(spawned);
    }

    let kernel = Arc::new(FakeWindowsKernel::new());
    let spawned = spawn(
        kernel.clone(),
        WindowsSpawnPlan::for_test(
            0,
            WindowsJobPolicy::None,
            WindowsStdioPlan::all_inherit(),
            None,
        ),
    )
    .expect("spawn without inherited handles");
    assert_eq!(kernel.count(WindowsSpawnStage::BuildHandleListAttribute), 0);
    assert!(!kernel.last_inherit_handles());
    drop(spawned);
}

#[test]
fn startup_info_size_matches_attribute_presence() {
    assert_eq!(startup_info_size(false, 104, 112), 104);
    assert_eq!(startup_info_size(true, 104, 112), 112);
}

#[test]
fn target_identity_and_wait_are_armed_immediately_after_create_process() {
    let kernel = Arc::new(FakeWindowsKernel::new());
    let spawned = spawn(kernel.clone(), ordinary_plan()).expect("spawn shadow");
    let events = kernel.events();
    let create = position(&events, WindowsSpawnStage::CreateProcessW);

    assert_eq!(events[create + 1], WindowsSpawnStage::FillTargetIdentity);
    assert_eq!(events[create + 2], WindowsSpawnStage::SetThreadpoolWait);
    assert_eq!(spawned.identity().pid(), kernel.created_pid());
    drop(spawned);
}

#[test]
fn all_fallible_post_create_work_precedes_resume_and_resume_is_last_fallible() {
    for policy in [
        WindowsJobPolicy::None,
        WindowsJobPolicy::LibuvGlobal,
        WindowsJobPolicy::LibuvGlobalWithControlTracker,
    ] {
        let kernel = Arc::new(FakeWindowsKernel::new());
        let plan = if policy == WindowsJobPolicy::None {
            ordinary_plan()
        } else {
            private_job_plan(policy)
        };
        let spawned = spawn(kernel.clone(), plan).expect("spawn shadow");
        let events = kernel.events();
        let resume = position(&events, WindowsSpawnStage::ResumeThread);

        for stage in [
            WindowsSpawnStage::WrapParentStdio,
            WindowsSpawnStage::ValidateReturnBundle,
            WindowsSpawnStage::AcquireProcessMap,
            WindowsSpawnStage::AcquireLifecycle,
        ] {
            assert!(
                position(&events, stage) < resume,
                "{stage:?} must precede ResumeThread"
            );
        }
        if matches!(
            policy,
            WindowsJobPolicy::LibuvGlobal | WindowsJobPolicy::LibuvGlobalWithControlTracker
        ) {
            assert!(position(&events, WindowsSpawnStage::AssignLibuvGlobalJob) < resume);
        }

        assert!(position(&events, WindowsSpawnStage::PublishProcessMap) > resume);
        assert!(position(&events, WindowsSpawnStage::ClosePrimaryThread) > resume);
        assert!(position(&events, WindowsSpawnStage::ImmediateWaitQuery) > resume);
        assert_eq!(kernel.fallible_stage_after_resume(), None);
        drop(spawned);
    }
}

#[test]
fn resume_failure_publishes_nothing_and_uses_one_suspended_root_cleanup() {
    let kernel = Arc::new(FakeWindowsKernel::with_fault(WindowsSpawnFault::At(
        WindowsSpawnStage::ResumeThread,
    )));
    kernel.set_hidden_cleanup(HiddenCleanupOutcome::Terminal { exit_code: 1 });
    let result = spawn(kernel.clone(), ordinary_plan());

    assert!(matches!(
        result,
        Err(WindowsSpawnError::PostCreate {
            stage: WindowsSpawnStage::ResumeThread,
            ..
        })
    ));
    assert_eq!(kernel.count(WindowsSpawnStage::PublishProcessMap), 0);
    assert_eq!(kernel.count(WindowsSpawnStage::PublishSurface), 0);
    assert_eq!(kernel.count(WindowsSpawnStage::TerminateProcess), 1);
    assert_eq!(kernel.max_hidden_wait(), Some(Duration::from_millis(2_000)));
    assert_eq!(kernel.created_descendant_count(), 0);
    assert_eq!(kernel.open_handle_count(), 0);
}

#[test]
fn every_recoverable_post_create_failure_terminates_once_while_suspended() {
    for failed_stage in [
        WindowsSpawnStage::AssignLibuvGlobalJob,
        WindowsSpawnStage::WrapParentStdio,
        WindowsSpawnStage::ValidateReturnBundle,
        WindowsSpawnStage::ResumeThread,
    ] {
        let kernel = Arc::new(FakeWindowsKernel::with_fault(WindowsSpawnFault::At(
            failed_stage,
        )));
        kernel.set_hidden_cleanup(HiddenCleanupOutcome::Terminal { exit_code: 7 });
        let plan = if failed_stage == WindowsSpawnStage::AssignLibuvGlobalJob {
            private_job_plan(WindowsJobPolicy::LibuvGlobalWithControlTracker)
        } else {
            ordinary_plan()
        };
        let result = spawn(kernel.clone(), plan);

        assert!(matches!(
            result,
            Err(WindowsSpawnError::PostCreate { stage, .. }) if stage == failed_stage
        ));
        assert_eq!(kernel.count(WindowsSpawnStage::TerminateProcess), 1);
        assert_eq!(
            kernel.count(WindowsSpawnStage::ResumeThread),
            usize::from(failed_stage == WindowsSpawnStage::ResumeThread)
        );
        assert_eq!(kernel.created_descendant_count(), 0);
        assert_eq!(kernel.count(WindowsSpawnStage::PublishProcessMap), 0);
        assert_eq!(kernel.open_handle_count(), 0);
    }
}

#[test]
fn retained_hidden_cleanup_is_bounded_owned_and_never_reissues_terminate() {
    let kernel = Arc::new(FakeWindowsKernel::with_fault(WindowsSpawnFault::At(
        WindowsSpawnStage::WrapParentStdio,
    )));
    kernel.set_hidden_cleanup(HiddenCleanupOutcome::StillLive);
    let supervisor = WindowsSpawnSupervisor::with_kernel_for_test(kernel.clone());
    let error = supervisor
        .spawn_test_image(ordinary_plan())
        .expect_err("retained hidden cleanup");
    let identity = match error {
        WindowsSpawnError::RetainedHidden { identity, .. } => identity,
        other => panic!("unexpected error: {other:?}"),
    };

    assert_eq!(kernel.count(WindowsSpawnStage::TerminateProcess), 1);
    assert!(supervisor.contains_registration(identity));
    assert_eq!(kernel.open_handle_count_for_identity(identity), 3);

    supervisor.retry_hidden_cleanup_for_test(identity);
    assert_eq!(kernel.count(WindowsSpawnStage::TerminateProcess), 1);
    kernel.signal_process(identity, 9);
    supervisor.dispatch_wait_for_test(identity);
    supervisor.drain_cleanup_for_test();
    assert!(!supervisor.contains_registration(identity));
    assert_eq!(kernel.count(WindowsSpawnStage::TerminateProcess), 1);
    assert_eq!(kernel.open_handle_count(), 0);
}

#[test]
fn fast_exit_commit_query_and_queued_callback_publish_exactly_once() {
    let kernel = Arc::new(FakeWindowsKernel::new());
    kernel.set_create_outcome(CreateProcessOutcome::FastExitOnResume { exit_code: 23 });
    kernel.queue_callback_inside_resume();
    let supervisor = WindowsSpawnSupervisor::with_kernel_for_test(kernel.clone());
    let mut spawned = supervisor
        .spawn_test_image(ordinary_plan())
        .expect("fast-exit spawn remains success");

    kernel.join_resume_callback();
    assert_eq!(spawned.try_wait(), Some(23));
    assert_eq!(spawned.try_wait(), Some(23));
    assert_eq!(spawned.terminal_event_count(), 1);
    assert_eq!(spawned.cleanup_enqueue_count(), 1);
    assert_eq!(kernel.count(WindowsSpawnStage::GetExitCodeProcess), 1);
    assert_eq!(kernel.callback_phase_observations(), vec!["Published"]);

    supervisor.drain_cleanup_for_test();
    assert_eq!(kernel.count(WindowsSpawnStage::CloseThreadpoolWait), 1);
    drop(spawned);
    supervisor.drain_cleanup_for_test();
    assert_eq!(kernel.open_handle_count(), 0);
}

#[test]
fn timeout_then_callback_uses_the_same_terminal_observer() {
    let kernel = Arc::new(FakeWindowsKernel::new());
    kernel.set_create_outcome(CreateProcessOutcome::Running);
    let supervisor = WindowsSpawnSupervisor::with_kernel_for_test(kernel.clone());
    let mut spawned = supervisor
        .spawn_test_image(ordinary_plan())
        .expect("running spawn");
    assert_eq!(spawned.try_wait(), None);

    kernel.signal_process(spawned.identity(), 31);
    supervisor.dispatch_wait_for_test(spawned.identity());
    supervisor.dispatch_wait_for_test(spawned.identity());
    assert_eq!(spawned.try_wait(), Some(31));
    assert_eq!(spawned.terminal_event_count(), 1);
    assert_eq!(spawned.cleanup_enqueue_count(), 1);
    assert_eq!(kernel.count(WindowsSpawnStage::GetExitCodeProcess), 1);
    supervisor.drain_cleanup_for_test();
    drop(spawned);
    supervisor.drain_cleanup_for_test();
    assert_eq!(kernel.open_handle_count(), 0);
}

#[test]
fn live_registration_holds_supervisor_after_facade_and_supervisor_drop() {
    let kernel = Arc::new(FakeWindowsKernel::new());
    let supervisor = WindowsSpawnSupervisor::with_kernel_for_test(kernel.clone());
    let spawned = supervisor
        .spawn_test_image(ordinary_plan())
        .expect("running spawn");
    let identity = spawned.identity();

    drop(supervisor);
    drop(spawned);
    kernel.signal_process(identity, 41);
    kernel.dispatch_bound_wait(identity);
    assert_eq!(kernel.callback_phase_observations(), vec!["Published"]);
    assert_eq!(kernel.count(WindowsSpawnStage::GetExitCodeProcess), 1);

    kernel.drain_bound_cleanup(identity);
    assert_eq!(kernel.open_handle_count(), 0);
}

#[test]
fn cleanup_quiesces_callback_without_holding_lifecycle() {
    let kernel = Arc::new(FakeWindowsKernel::new());
    kernel.set_create_outcome(CreateProcessOutcome::FastExitOnResume { exit_code: 43 });
    kernel.callback_inside_quiesce();
    let supervisor = WindowsSpawnSupervisor::with_kernel_for_test(kernel.clone());
    let spawned = supervisor
        .spawn_test_image(ordinary_plan())
        .expect("fast terminal spawn");
    let cleanup = supervisor.clone();
    let (done_tx, done_rx) = std::sync::mpsc::sync_channel(0);
    let worker = std::thread::spawn(move || {
        cleanup.drain_cleanup_for_test();
        done_tx.send(()).expect("cleanup completion");
    });

    done_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("callback quiescence must not deadlock lifecycle");
    worker.join().expect("cleanup worker");
    assert_eq!(kernel.count(WindowsSpawnStage::CloseThreadpoolWait), 1);
    assert_eq!(kernel.callback_phase_observations(), vec!["Published"]);
    drop(spawned);
    supervisor.drain_cleanup_for_test();
    assert_eq!(kernel.open_handle_count(), 0);
}

#[test]
fn outstanding_spawns_retain_individual_capacity_reservations_until_enqueue() {
    let kernel = Arc::new(FakeWindowsKernel::new());
    let supervisor = WindowsSpawnSupervisor::with_kernel_for_test(kernel.clone());
    let first = supervisor
        .spawn_test_image(ordinary_plan())
        .expect("first running spawn");
    assert_eq!(supervisor.reservation_counts_for_test(), (0, 1, 0));
    let second = supervisor
        .spawn_test_image(ordinary_plan())
        .expect("second running spawn");
    assert_eq!(supervisor.reservation_counts_for_test(), (0, 2, 0));

    kernel.signal_process(first.identity(), 0);
    supervisor.dispatch_wait_for_test(first.identity());
    assert_eq!(supervisor.reservation_counts_for_test(), (0, 1, 0));
    kernel.signal_process(second.identity(), 0);
    supervisor.dispatch_wait_for_test(second.identity());
    assert_eq!(supervisor.reservation_counts_for_test(), (0, 0, 0));

    supervisor.drain_cleanup_for_test();
    drop(first);
    drop(second);
    supervisor.drain_cleanup_for_test();
    assert_eq!(kernel.open_handle_count(), 0);
}

#[test]
fn generation_exhaustion_is_a_no_spawn_error_before_wait_or_process_creation() {
    let kernel = Arc::new(FakeWindowsKernel::new());
    let supervisor = WindowsSpawnSupervisor::with_kernel_for_test(kernel.clone());
    supervisor.set_next_generation_for_test(u64::MAX);
    let result = supervisor.spawn_test_image(ordinary_plan());

    assert!(matches!(
        result,
        Err(WindowsSpawnError::PreSpawn {
            stage: WindowsSpawnStage::ReserveProcessMap,
            ..
        })
    ));
    assert_eq!(kernel.count(WindowsSpawnStage::CreateThreadpoolWait), 0);
    assert_eq!(kernel.count(WindowsSpawnStage::CreateProcessW), 0);
    assert_eq!(kernel.open_handle_count(), 0);
    assert_eq!(supervisor.reservation_counts_for_test(), (0, 0, 0));
}

#[test]
fn private_job_active_zero_is_a_distinct_completion_lease_with_stale_key_rejection() {
    let kernel = Arc::new(FakeWindowsKernel::new());
    let supervisor = WindowsSpawnSupervisor::with_kernel_for_test(kernel.clone());
    let spawned = supervisor
        .spawn_test_image(private_job_plan(WindowsJobPolicy::BrowserSidecarPrivate))
        .expect("private-job spawn");
    let identity = spawned.identity();
    let completion_id = spawned.completion_id().expect("private completion ID");

    kernel.signal_process(identity, 0);
    supervisor.dispatch_wait_for_test(identity);
    supervisor.drain_cleanup_for_test();
    drop(spawned);
    assert!(
        supervisor.contains_registration(identity),
        "leader terminal and facade release cannot close a job before active zero"
    );
    assert_eq!(
        kernel.open_handle_count(),
        2,
        "process and private Job remain"
    );

    assert!(supervisor.observe_job_active_zero_for_test(completion_id));
    assert!(!supervisor.contains_registration(identity));
    assert_eq!(kernel.open_handle_count(), 0);
    assert!(
        !supervisor.observe_job_active_zero_for_test(completion_id),
        "tombstoned never-reused completion key is stale"
    );
}

#[test]
fn accounting_zero_before_completion_packet_retires_then_tombstones_late_key() {
    let kernel = Arc::new(FakeWindowsKernel::new());
    kernel.set_job_active_processes(0);
    let supervisor = WindowsSpawnSupervisor::with_kernel_for_test(kernel.clone());
    let spawned = supervisor
        .spawn_test_image(private_job_plan(WindowsJobPolicy::BrowserSidecarPrivate))
        .expect("private-job spawn");
    let identity = spawned.identity();
    let completion_id = spawned.completion_id().expect("private completion ID");

    kernel.signal_process(identity, 0);
    supervisor.dispatch_wait_for_test(identity);
    supervisor.drain_cleanup_for_test();
    drop(spawned);

    assert_eq!(
        kernel.count(WindowsSpawnStage::QueryPrivateJobAccounting),
        1
    );
    assert!(!supervisor.contains_registration(identity));
    assert_eq!(kernel.open_handle_count(), 0);
    assert!(
        !supervisor.observe_job_active_zero_for_test(completion_id),
        "packet dequeued after accounting retirement is stale"
    );
}

#[test]
fn completion_packet_racing_accounting_retirement_latches_only_under_exact_lifecycle() {
    let kernel = Arc::new(FakeWindowsKernel::new());
    let supervisor = WindowsSpawnSupervisor::with_kernel_for_test(kernel.clone());
    let spawned = supervisor
        .spawn_test_image(private_job_plan(WindowsJobPolicy::BrowserSidecarPrivate))
        .expect("private-job spawn");
    let identity = spawned.identity();
    let completion_id = spawned.completion_id().expect("private completion ID");

    kernel.signal_process(identity, 0);
    supervisor.dispatch_wait_for_test(identity);
    supervisor.drain_cleanup_for_test();
    assert!(supervisor.contains_registration(identity));

    kernel.set_job_active_processes(0);
    kernel.block_job_accounting_query();
    let lease_release = std::thread::spawn(move || drop(spawned));
    assert!(kernel.wait_for_blocked_job_accounting_query(Duration::from_secs(1)));

    let packet_supervisor = supervisor.clone();
    let packet = std::thread::spawn(move || {
        packet_supervisor.observe_job_active_zero_for_test(completion_id)
    });
    assert!(kernel.wait_for_count(
        WindowsSpawnStage::LookupJobCompletionCell,
        1,
        Duration::from_secs(1)
    ));
    kernel.release_job_accounting_query();

    lease_release.join().expect("identity-lease release thread");
    assert!(packet.join().expect("completion-packet thread"));
    assert!(!supervisor.contains_registration(identity));
    assert_eq!(kernel.open_handle_count(), 0);
    assert_eq!(kernel.count(WindowsSpawnStage::ObserveJobActiveZero), 1);
    assert!(
        !supervisor.observe_job_active_zero_for_test(completion_id),
        "retired exact cell must tombstone a later packet"
    );
}

#[test]
fn automatic_cleanup_worker_drains_multiple_terminal_waits_and_stops_without_self_join() {
    let kernel = Arc::new(FakeWindowsKernel::with_automatic_cleanup_worker());
    kernel.set_create_outcome(CreateProcessOutcome::FastExitOnResume { exit_code: 0 });
    let supervisor = WindowsSpawnSupervisor::with_kernel_for_test(kernel.clone());
    let runtime = supervisor.cleanup_runtime_for_test();
    let first = supervisor
        .spawn_test_image(ordinary_plan())
        .expect("first fast spawn");
    let second = supervisor
        .spawn_test_image(ordinary_plan())
        .expect("second fast spawn");

    assert!(kernel.wait_for_count(
        WindowsSpawnStage::CloseThreadpoolWait,
        2,
        Duration::from_secs(1)
    ));
    drop(first);
    drop(second);
    drop(supervisor);
    assert!(
        runtime.wait_stopped(Duration::from_secs(1)),
        "last registration may drop its supervisor on the cleanup worker without self-join"
    );
    assert_eq!(kernel.open_handle_count(), 0);
}

#[tokio::test]
async fn explicit_shutdown_closes_admission_and_stops_the_cleanup_worker() {
    let kernel = Arc::new(FakeWindowsKernel::with_automatic_cleanup_worker());
    let supervisor = WindowsSpawnSupervisor::with_kernel_for_test(kernel.clone());
    let runtime = supervisor.cleanup_runtime_for_test();

    supervisor.shutdown().await.expect("empty shutdown");

    assert!(runtime.wait_stopped(Duration::from_secs(1)));
    assert!(supervisor.spawn_test_image(ordinary_plan()).is_err());
    assert_eq!(supervisor.admission_counts_for_test(), (0, 0));
    assert_eq!(kernel.count(WindowsSpawnStage::CloseExternalAdmission), 1);
    assert_eq!(kernel.count(WindowsSpawnStage::CloseInternalAdmission), 1);
    assert_eq!(kernel.count(WindowsSpawnStage::ShutdownCompletionWorker), 1);
    assert_eq!(kernel.count(WindowsSpawnStage::ShutdownCleanupWorker), 1);
}

#[test]
fn completion_worker_failure_marks_existing_private_registration_retained() {
    let kernel = Arc::new(FakeWindowsKernel::new());
    let supervisor = WindowsSpawnSupervisor::with_kernel_for_test(kernel.clone());
    let spawned = supervisor
        .spawn_test_image(private_job_plan(WindowsJobPolicy::BrowserSidecarPrivate))
        .expect("private-job spawn");
    let identity = spawned.identity();
    let completion_id = spawned.completion_id().expect("private completion ID");
    supervisor.fail_completion_worker_for_test();
    kernel.signal_process(identity, 0);
    supervisor.dispatch_wait_for_test(identity);
    supervisor.drain_cleanup_for_test();
    drop(spawned);

    assert!(supervisor.contains_registration(identity));
    assert_eq!(
        supervisor.worker_invariant_message_for_test().as_deref(),
        Some("completion:injected GetQueuedCompletionStatus failure")
    );
    assert_eq!(kernel.open_handle_count(), 2);

    kernel.set_job_active_processes(0);
    assert!(supervisor.observe_job_active_zero_for_test(completion_id));
    assert_eq!(kernel.open_handle_count(), 0);
}

#[test]
fn registration_and_return_storage_are_preallocated_before_create_process() {
    let kernel = Arc::new(FakeWindowsKernel::new());
    let spawned = spawn(kernel.clone(), ordinary_plan()).expect("spawn shadow");
    let events = kernel.events();
    let create = position(&events, WindowsSpawnStage::CreateProcessW);

    assert!(position(&events, WindowsSpawnStage::PreallocateRegistration) < create);
    assert!(position(&events, WindowsSpawnStage::PreallocateReturnBundle) < create);
    assert_eq!(kernel.fallible_stage_after_resume(), None);
    drop(spawned);
}

#[test]
fn tracker_sentinel_forces_handle_inheritance_even_without_stdio_handles() {
    let kernel = Arc::new(FakeWindowsKernel::new());
    let plan = WindowsSpawnPlan::for_test(
        0,
        WindowsJobPolicy::LibuvGlobalWithControlTracker,
        WindowsStdioPlan::all_inherit(),
        Some(0xfeed),
    );
    let spawned = spawn(kernel.clone(), plan).expect("tracker spawn");

    assert!(kernel.last_inherit_handles());
    assert_eq!(kernel.count(WindowsSpawnStage::BuildHandleListAttribute), 1);
    drop(spawned);
}

#[test]
fn backend_source_has_real_win32_surface_and_no_command_delegation() {
    let source = include_str!("windows_spawn.rs");
    for required in [
        "CreateProcessW",
        "CreateThreadpoolWait",
        "SetThreadpoolWait",
        "WaitForSingleObject",
        "ResumeThread",
        "TerminateProcess",
        "PROC_THREAD_ATTRIBUTE_JOB_LIST",
        "PROC_THREAD_ATTRIBUTE_HANDLE_LIST",
        "CREATE_SUSPENDED",
        "GetQueuedCompletionStatus",
        "JOB_OBJECT_MSG_ACTIVE_PROCESS_ZERO",
        "struct AttributeAllocation",
        "list_storage: Box<[usize]>",
        "job_values: Box<[usize]>",
        "handle_values: Box<[usize]>",
        "QueryInformationJobObject",
        "JobObjectBasicAccountingInformation",
        "Arc<CompletionPortRuntime>",
    ] {
        assert!(
            source.contains(required),
            "missing real Win32 surface {required}"
        );
    }
    assert!(!source.contains("let mut job_handle ="));
    assert!(!source.contains("let mut raw_handles:"));
    let kernel_drop_start = source
        .find("impl Drop for ProductionWindowsKernel")
        .expect("production kernel drop implementation");
    let kernel_drop_end = source[kernel_drop_start..]
        .find("impl WindowsSpawnKernel for ProductionWindowsKernel")
        .map(|offset| kernel_drop_start + offset)
        .expect("end of production kernel drop implementation");
    assert!(
        !source[kernel_drop_start..kernel_drop_end].contains(".join("),
        "completion-worker self-drop must not join its own thread"
    );
    let registration = source
        .find("let registration = Arc::new(WindowsRegistration")
        .expect("registration allocation");
    let return_storage = source
        .find("WindowsParentStdio::preallocated()")
        .expect("return storage allocation");
    let fixed_parent_storage = source
        .find("let fixed_parent_raw = stdio.fixed_parent_raw()")
        .expect("fixed parent raw-handle storage");
    let create = source
        .find("let created = match kernel.create_process")
        .expect("CreateProcess transaction");
    assert!(registration < create);
    assert!(return_storage < create);
    assert!(fixed_parent_storage < create);
    let fixed_parent_helper_start = source
        .find("fn fixed_parent_raw(&self)")
        .expect("fixed parent handle helper");
    let fixed_parent_helper_end = source[fixed_parent_helper_start..]
        .find("fn close_child_ends")
        .map(|offset| fixed_parent_helper_start + offset)
        .expect("end of fixed parent handle helper");
    let fixed_parent_helper = &source[fixed_parent_helper_start..fixed_parent_helper_end];
    for allocating_surface in ["Vec::", "Vec<", ".collect(", "with_capacity", "try_reserve"] {
        assert!(
            !fixed_parent_helper.contains(allocating_surface),
            "post-create raw parent-handle helper may not allocate via {allocating_surface}"
        );
    }
    for forbidden in [
        ["std", "process", "Command"].join("::"),
        ["tokio", "process", "Command"].join("::"),
    ] {
        assert!(
            !source.contains(&forbidden),
            "delegated Windows spawn to {forbidden}"
        );
    }
}
