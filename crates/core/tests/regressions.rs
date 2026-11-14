//! Defect regression suite: one pinned test per fixed defect.
//!
//! Convention (see the "Defect Regression Tests" section of AGENTS.md):
//! - One file per defect under `tests/regressions/`, mounted here via
//!   `#[path]` so the suite stays a single integration-test target.
//! - File name `r<id>_<short-slug>.rs`, where `<id>` is the tracker work-item
//!   id when one exists, else the short hash of the fixing commit.
//! - Each test exercises the PUBLIC engine surface that exposed the defect
//!   and must fail on the pre-fix engine.
//! - The file header records what broke, the one-line root cause, and the
//!   fixing commit / work item for traceability.

#[path = "regressions/r20261112_antigravity_catalog_refresh_notification.rs"]
mod r20261112_antigravity_catalog_refresh_notification;

#[path = "regressions/r20261112_antigravity_account_refusal.rs"]
mod r20261112_antigravity_account_refusal;

#[path = "regressions/r20261112_full_access_mcp_confirmation.rs"]
mod r20261112_full_access_mcp_confirmation;

#[path = "regressions/r7eab79a_native_tool_negotiation.rs"]
mod r7eab79a_native_tool_negotiation;

#[path = "regressions/r20261104_native_compaction_retry.rs"]
mod r20261104_native_compaction_retry;

#[path = "regressions/rf4a2eb9d_request_rotation_isolation.rs"]
mod rf4a2eb9d_request_rotation_isolation;

#[path = "regressions/r20261105_compaction_output_limit.rs"]
mod r20261105_compaction_output_limit;

#[path = "regressions/r20261108_compaction_tooluse_stop.rs"]
mod r20261108_compaction_tooluse_stop;

#[path = "regressions/r20261111_partial_view_memory_baseline.rs"]
mod r20261111_partial_view_memory_baseline;

#[path = "regressions/r20261102_managed_browser_approval.rs"]
mod r20261102_managed_browser_approval;

#[path = "regressions/r20261104_antigravity_catalog_local_first.rs"]
mod r20261104_antigravity_catalog_local_first;

#[cfg(unix)]
#[path = "regressions/r20261028_background_output_wait.rs"]
mod r20261028_background_output_wait;

#[path = "regressions/r2dcab957_title_query_isolation.rs"]
mod r2dcab957_title_query_isolation;

#[path = "regressions/r20261027_thinking_only_closing_retry.rs"]
mod r20261027_thinking_only_closing_retry;

#[path = "regressions/r20261027_stop_preparation.rs"]
mod r20261027_stop_preparation;

#[path = "regressions/r20261026_subscription_checkpoint_account_switch.rs"]
mod r20261026_subscription_checkpoint_account_switch;

#[path = "regressions/r20261026_cross_source_checkpoint_recovery.rs"]
mod r20261026_cross_source_checkpoint_recovery;

#[path = "regressions/r20261026_live_agent_wait_loop.rs"]
mod r20261026_live_agent_wait_loop;

#[path = "regressions/r20261026_subagent_fork_context.rs"]
mod r20261026_subagent_fork_context;

#[path = "regressions/r20261026_live_user_turn_identity.rs"]
mod r20261026_live_user_turn_identity;

#[path = "regressions/r0a2ef459_stream_text_buffer.rs"]
mod r20261026_stream_text_buffer;

#[path = "regressions/r0a2ef459_workflow_journal_append.rs"]
mod r20261026_workflow_journal_append;

#[path = "regressions/r0a2ef459_rollout_append_batches.rs"]
mod r20261026_rollout_append_batches;

#[path = "regressions/r01b91dae_mcp_close_drain.rs"]
mod r01b91dae_mcp_close_drain;

#[path = "regressions/rab6b0c00_stop_tool_cancellation.rs"]
mod rab6b0c00_stop_tool_cancellation;

#[path = "regressions/r20261024_subagent_mcp_owner.rs"]
mod r20261024_subagent_mcp_owner;

#[path = "regressions/r20261024_workflow_parent_capture.rs"]
mod r20261024_workflow_parent_capture;

#[path = "regressions/r20261024_subagent_recovery_refresh.rs"]
mod r20261024_subagent_recovery_refresh;

#[path = "regressions/r20261023_session_message_paging.rs"]
mod r20261023_session_message_paging;

#[path = "regressions/r20261024_materialized_thread_history.rs"]
mod r20261024_materialized_thread_history;

#[path = "regressions/rb7c2c984_approval_admission_replay.rs"]
mod rb7c2c984_approval_admission_replay;

#[path = "regressions/rb7c2c984_rollout_migration.rs"]
mod rb7c2c984_rollout_migration;

#[path = "regressions/re1e0ea02_native_media_retention.rs"]
mod re1e0ea02_native_media_retention;

#[path = "regressions/r20261020_null_stream_error.rs"]
mod r20261020_null_stream_error;

#[path = "regressions/r20261022_model_context_migration.rs"]
mod r20261022_model_context_migration;

#[path = "regressions/r01a1588f_context_budget_policy.rs"]
mod r01a1588f_context_budget_policy;

#[path = "regressions/r01a1588f_workspace_instruction_growth.rs"]
mod r01a1588f_workspace_instruction_growth;

#[path = "regressions/r01a15833_subagent_communication.rs"]
mod r01a15833_subagent_communication;

#[path = "regressions/r20261019_input_length_range.rs"]
mod r20261019_input_length_range;

#[path = "regressions/r20261019_small_context_tools.rs"]
mod r20261019_small_context_tools;

#[path = "regressions/r20261019_native_media_input.rs"]
mod r20261019_native_media_input;

#[path = "regressions/r20261019_signed_thought_tool_replay.rs"]
mod r20261019_signed_thought_tool_replay;

#[path = "regressions/r20261019_chat_instruction_roles.rs"]
mod r20261019_chat_instruction_roles;

#[path = "regressions/r7ec59717_profile_activity_legacy.rs"]
mod profile_activity_legacy;

#[path = "regressions/r505f930c_profile_activity_cache.rs"]
mod profile_activity_cache;

#[path = "regressions/r2a6667dc_profile_activity_partial_timing.rs"]
mod r2a6667dc_profile_activity_partial_timing;

#[path = "regressions/r20261019_adaptive_action_review.rs"]
mod r20261019_adaptive_action_review;

#[path = "regressions/r20261018_runtime_inference_settings.rs"]
mod r20261018_runtime_inference_settings;

#[path = "regressions/r20261017_goal_child_mode_accounting.rs"]
mod r20261017_goal_child_mode_accounting;

#[path = "regressions/r20261017_goal_process_restart.rs"]
mod r20261017_goal_process_restart;

#[path = "regressions/r20261017_goal_outcomes.rs"]
mod r20261017_goal_outcomes;

#[path = "regressions/r20261017_goal_completion_result.rs"]
mod r20261017_goal_completion_result;

#[path = "regressions/r20261017_goal_restart.rs"]
mod r20261017_goal_restart;

#[path = "regressions/r20261016_tool_argument_recovery.rs"]
mod r20261016_tool_argument_recovery;

#[path = "regressions/r20261016_tool_instruction_consistency.rs"]
mod r20261016_tool_instruction_consistency;

#[path = "regressions/goal_stream_fixture.rs"]
mod goal_stream_fixture;

#[path = "regressions/r20261017_goal_stream_lifetime.rs"]
mod r20261017_goal_stream_lifetime;

#[path = "regressions/r20261017_goal_owned_read.rs"]
mod r20261017_goal_owned_read;

#[path = "regressions/r20261016_goal_progress_guidance.rs"]
mod r20261016_goal_progress_guidance;

#[path = "regressions/r20261016_instruction_context_policy.rs"]
mod r20261016_instruction_context_policy;

#[path = "regressions/r20261016_instruction_authorization.rs"]
mod r20261016_instruction_authorization;

#[path = "regressions/r20261107_browser_surface_ownership.rs"]
mod r20261107_browser_surface_ownership;

#[path = "regressions/r20261015_browser_partial_batch.rs"]
mod r20261015_browser_partial_batch;

#[path = "regressions/r20261015_browser_diagnostics_and_targets.rs"]
mod r20261015_browser_diagnostics_and_targets;

#[path = "regressions/r20261010_plugin_upgrade_atomicity.rs"]
mod r20261010_plugin_upgrade_atomicity;

#[path = "regressions/r20261010_turn_error_details_history.rs"]
mod r20261010_turn_error_details_history;

#[path = "regressions/r20261010_model_image_consumption.rs"]
mod r20261010_model_image_consumption;

#[path = "regressions/r20261010_image_media_request_budget.rs"]
mod r20261010_image_media_request_budget;

#[path = "regressions/r20261010_request_size_recovery.rs"]
mod r20261010_request_size_recovery;

#[path = "regressions/r20261010_media_capability_snapshot.rs"]
mod r20261010_media_capability_snapshot;

#[path = "regressions/r20261008_tool_batch_contiguity.rs"]
mod r20261008_tool_batch_contiguity;

#[path = "regressions/r20261008_unavailable_tier_falls_back_to_main.rs"]
mod r20261008_unavailable_tier_falls_back_to_main;
#[path = "regressions/r20261010_subagent_message_delivery.rs"]
mod r20261010_subagent_message_delivery;

#[path = "regressions/r20261013_user_interrupt_guidance.rs"]
mod r20261013_user_interrupt_guidance;

#[path = "regressions/r20261013_subagent_reload_on_resume.rs"]
mod r20261013_subagent_reload_on_resume;

#[path = "regressions/r20261007_advisor_runtime.rs"]
mod r20261007_advisor_runtime;

#[path = "regressions/r20261006_task_resource_scope.rs"]
mod r20261006_task_resource_scope;

#[path = "regressions/r20261006_stored_permission_rule_isolation.rs"]
mod r20261006_stored_permission_rule_isolation;

#[path = "regressions/r20261004_side_task_complete_turn.rs"]
mod r20261004_side_task_complete_turn;

#[path = "regressions/r20261004_anthropic_root_tool_combinators.rs"]
mod r20261004_anthropic_root_tool_combinators;

#[cfg(feature = "testing")]
#[path = "regressions/r20261003_question_answer_compaction.rs"]
mod r20261003_question_answer_compaction;

#[cfg(feature = "testing")]
#[path = "regressions/r20261022_context_recovery_turn.rs"]
mod r20261022_context_recovery_turn;
#[path = "regressions/r20261022_preflight_context_management.rs"]
mod r20261022_preflight_context_management;

#[cfg(feature = "testing")]
#[path = "regressions/r20261003_opaque_reasoning_tail_budget.rs"]
mod r20261003_opaque_reasoning_tail_budget;

// Diagnostic assertions require the feature that isolates logs from the user profile.
#[cfg(feature = "testing")]
#[path = "regressions/r20261003_compaction_stream_deadlines.rs"]
mod r20261003_compaction_stream_deadlines;

#[path = "regressions/r20261002_inference_snapshot_switch.rs"]
mod r20261002_inference_snapshot_switch;

#[path = "regressions/r20261002_captured_catalog_capabilities.rs"]
mod r20261002_captured_catalog_capabilities;

#[path = "regressions/r20261002_inference_event_usage_attribution.rs"]
mod r20261002_inference_event_usage_attribution;

#[path = "regressions/rbbf26fc_stale_session_lock_recovery.rs"]
mod rbbf26fc_stale_session_lock_recovery;

#[path = "regressions/r20260928_stop_cancels_background_agent.rs"]
mod r20260928_stop_cancels_background_agent;

#[path = "regressions/r20260927_plugin_capability_refresh.rs"]
mod r20260927_plugin_capability_refresh;

#[path = "regressions/re3f8d86_remote_session_catalog.rs"]
mod re3f8d86_remote_session_catalog;

#[path = "regressions/r20260919_web_search_child_browser_dependency.rs"]
mod r20260919_web_search_child_browser_dependency;

#[path = "regressions/r20260913_complete_thread_history.rs"]
mod r20260913_complete_thread_history;

#[path = "regressions/r20260909_windows_acl_projection.rs"]
mod r20260909_windows_acl_projection;

#[path = "regressions/r20260909_owned_appcontainer_profile.rs"]
mod r20260909_owned_appcontainer_profile;

#[path = "regressions/r20260909_appcontainer_sid_ownership.rs"]
mod r20260909_appcontainer_sid_ownership;

#[path = "regressions/r20260909_windows_workspace_only_launch.rs"]
mod r20260909_windows_workspace_only_launch;

#[path = "regressions/r20260909_windows_session_broker.rs"]
mod r20260909_windows_session_broker;

#[path = "regressions/r20260909_windows_route_capabilities.rs"]
mod r20260909_windows_route_capabilities;

#[path = "regressions/r20260909_windows_sandbox_launch.rs"]
mod r20260909_windows_sandbox_launch;

#[path = "regressions/r20260909_windows_acl_revoke_noop.rs"]
mod r20260909_windows_acl_revoke_noop;

#[path = "regressions/r20260909_windows_walk_effective_access.rs"]
mod r20260909_windows_walk_effective_access;

#[path = "regressions/r7c876d49_spec_restores_latest_permission_policy.rs"]
mod r7c876d49_spec_restores_latest_permission_policy;

#[path = "regressions/r20260908_mcp_credentials_follow_config_root.rs"]
mod r20260908_mcp_credentials_follow_config_root;

#[path = "regressions/r20260908_thread_create_identity.rs"]
mod r20260908_thread_create_identity;

#[path = "regressions/r20260908_permission_rule_prefix_roundtrip.rs"]
mod r20260908_permission_rule_prefix_roundtrip;

#[path = "regressions/r01a0745e_host_recreates_completed_goal.rs"]
mod r01a0745e_host_recreates_completed_goal;

#[path = "regressions/rca43230_context_estimate_counts_the_request_prefix.rs"]
mod rca43230_context_estimate_counts_the_request_prefix;

#[path = "regressions/rb6903ac_sandbox_does_not_deny_user_credentials.rs"]
mod rb6903ac_sandbox_does_not_deny_user_credentials;

#[path = "regressions/rbad2176b_spawn_agent_inherits_parent_environment.rs"]
mod rbad2176b_spawn_agent_inherits_parent_environment;

#[path = "regressions/r20261113_explicit_plugin_preassembly.rs"]
mod r20261113_explicit_plugin_preassembly;

#[path = "regressions/rcbc19b9_file_change_counts_are_per_turn_and_exact.rs"]
mod rcbc19b9_file_change_counts_are_per_turn_and_exact;

#[path = "regressions/r20260903_write_stdin_error_categories.rs"]
mod r20260903_write_stdin_error_categories;

#[path = "regressions/r20260905_compaction_uses_full_request_estimate.rs"]
mod r20260905_compaction_uses_full_request_estimate;

#[path = "regressions/r20260905_current_plan_replacement.rs"]
mod r20260905_current_plan_replacement;

#[path = "regressions/r20260905_structured_user_questions.rs"]
mod r20260905_structured_user_questions;

#[path = "regressions/r20260905_compaction_recovery_query.rs"]
mod r20260905_compaction_recovery_query;

#[path = "regressions/r20260905_lossless_tool_result_budget.rs"]
mod r20260905_lossless_tool_result_budget;

#[path = "regressions/r20260906_provider_context_errors.rs"]
mod r20260906_provider_context_errors;

#[path = "regressions/r88483289_responses_stream_error.rs"]
mod r88483289_responses_stream_error;

#[path = "regressions/r20260906_manual_compaction_uses_selected_model.rs"]
mod r20260906_manual_compaction_uses_selected_model;

#[path = "regressions/r20260918_per_turn_effort_override.rs"]
mod r20260918_per_turn_effort_override;

#[path = "regressions/r20260905_assistant_message_phase.rs"]
mod r20260905_assistant_message_phase;

#[path = "regressions/r20260906_post_compact_hooks_match_trigger.rs"]
mod r20260906_post_compact_hooks_match_trigger;

#[path = "regressions/r8399e9e_memory_fork_approval.rs"]
mod r8399e9e_memory_fork_approval;

#[path = "regressions/r20261019_private_memory_policy.rs"]
mod r20261019_private_memory_policy;

#[path = "regressions/rbd0a6abc_empty_tool_choice.rs"]
mod rbd0a6abc_empty_tool_choice;

#[path = "regressions/rf38d372_forced_structured_thinking.rs"]
mod rf38d372_forced_structured_thinking;

#[path = "regressions/r20260906_subagent_live_stream.rs"]
mod r20260906_subagent_live_stream;

#[path = "regressions/r20260906_agent_notifications.rs"]
mod r20260906_agent_notifications;

#[path = "regressions/r20260907_plan_creation_guidance.rs"]
mod r20260907_plan_creation_guidance;

#[path = "regressions/r20260909_windows_acl_overlap.rs"]
mod r20260909_windows_acl_overlap;

#[path = "regressions/r20260907_local_model_tools_capability.rs"]
mod r20260907_local_model_tools_capability;

#[path = "regressions/r20260907_stream_total_timeout.rs"]
mod r20260907_stream_total_timeout;

#[path = "regressions/r20260927_long_prefill_stream_idle.rs"]
mod r20260927_long_prefill_stream_idle;

#[path = "regressions/r20260924_subagent_stream_timeout_recovery.rs"]
mod r20260924_subagent_stream_timeout_recovery;

#[path = "regressions/r20260926_empty_assistant_message_dropped.rs"]
mod r20260926_empty_assistant_message_dropped;

#[path = "regressions/r20260916_internal_memory_lifecycle.rs"]
mod r20260916_internal_memory_lifecycle;
#[path = "regressions/r20260917_budget_limited_subagent.rs"]
mod r20260917_budget_limited_subagent;

#[cfg(feature = "testing")]
#[path = "regressions/r20260907_parallel_child_start_contract.rs"]
mod r20260907_parallel_child_start_contract;

#[path = "regressions/r20260909_windows_loopback_lease.rs"]
mod r20260909_windows_loopback_lease;

#[path = "regressions/r20260913_pending_resume.rs"]
mod r20260913_pending_resume;

#[path = "regressions/r20260914_pending_mentions.rs"]
mod r20260914_pending_mentions;

#[path = "regressions/r20260918_plan_mode_is_a_child_floor.rs"]
mod r20260918_plan_mode_is_a_child_floor;

#[path = "regressions/r20260921_empty_final_response_fails.rs"]
mod r20260921_empty_final_response_fails;

#[path = "regressions/r20260922_empty_closing_response_fails.rs"]
mod r20260922_empty_closing_response_fails;

#[path = "regressions/r5f5c8ed5_freeform_history_replays_as_json_arguments.rs"]
mod r5f5c8ed5_freeform_history_replays_as_json_arguments;

#[path = "regressions/r20260930_user_steer_persists_as_user_message.rs"]
mod r20260930_user_steer_persists_as_user_message;

#[path = "regressions/r70130183_plan_web_fetch_network.rs"]
mod r70130183_plan_web_fetch_network;

#[path = "regressions/r20261001_prompt_image_size_gate.rs"]
mod r20261001_prompt_image_size_gate;

#[path = "regressions/r20261003_media_rejection_rescue.rs"]
mod r20261003_media_rejection_rescue;

#[path = "regressions/r20261003_budget_records_resume_projection.rs"]
mod r20261003_budget_records_resume_projection;

#[path = "regressions/r20261003_followup_lifecycle.rs"]
mod r20261003_followup_lifecycle;

#[path = "regressions/r20261004_async_user_questions.rs"]
mod r20261004_async_user_questions;

#[path = "regressions/r20261003_screenshot_delivery.rs"]
mod r20261003_screenshot_delivery;

#[path = "regressions/r20261005_cumulative_turn_diff.rs"]
mod r20261005_cumulative_turn_diff;

#[path = "regressions/r20261005_browser_tab_mentions.rs"]
mod r20261005_browser_tab_mentions;

#[path = "regressions/r20261005_browser_tab_steer_display.rs"]
mod r20261005_browser_tab_steer_display;
#[path = "regressions/r20261005_ultra_orchestration.rs"]
mod r20261005_ultra_orchestration;

#[path = "regressions/r20261011_subagent_lifecycle_single_hop.rs"]
mod r20261011_subagent_lifecycle_single_hop;

#[path = "regressions/r20261014_subagent_capacity_rejection.rs"]
mod r20261014_subagent_capacity_rejection;

#[path = "regressions/r20261013_bearer_auth_scheme_gateway.rs"]
mod r20261013_bearer_auth_scheme_gateway;

#[path = "regressions/r8fd4dc73_claude_auth_rotation.rs"]
mod claude_auth_rotation;
#[path = "regressions/rbb313dbb_claude_account_rotation.rs"]
mod rbb313dbb_claude_account_rotation;

#[path = "regressions/r20261015_tool_result_image_delivery.rs"]
mod r20261015_tool_result_image_delivery;

#[path = "regressions/r20261015_qwen_thinking_control.rs"]
mod r20261015_qwen_thinking_control;

#[path = "regressions/r20261015_subagent_result_mailbox.rs"]
mod r20261015_subagent_result_mailbox;

#[path = "regressions/r20261015_live_skill_catalog.rs"]
mod r20261015_live_skill_catalog;

#[path = "regressions/r20261014_delegation_mode_contract.rs"]
mod r20261014_delegation_mode_contract;

#[path = "regressions/r20261014_responses_incomplete.rs"]
mod r20261014_responses_incomplete;

#[path = "regressions/r20261014_subagent_cancellation_outcomes.rs"]
mod r20261014_subagent_cancellation_outcomes;

#[path = "regressions/r20261015_native_collaboration.rs"]
mod r20261015_native_collaboration;

#[path = "regressions/r20261015_native_output_verbosity.rs"]
mod r20261015_native_output_verbosity;

#[path = "regressions/r20261015_shared_instruction_policy.rs"]
mod r20261015_shared_instruction_policy;

#[path = "regressions/r20261015_dynamic_reminder_delivery.rs"]
mod r20261015_dynamic_reminder_delivery;

#[path = "regressions/r20261015_responses_function_arguments.rs"]
mod r20261015_responses_function_arguments;

#[path = "regressions/r20261016_chat_reasoning_roundtrip.rs"]
mod r20261016_chat_reasoning_roundtrip;

#[path = "regressions/r20261017_goal_idle_boundaries.rs"]
mod r20261017_goal_idle_boundaries;

#[path = "regressions/rcefd6705_goal_background_continuation.rs"]
mod rcefd6705_goal_background_continuation;

#[path = "regressions/r20261017_collaboration_planning.rs"]
mod r20261017_collaboration_planning;

#[path = "regressions/r20261017_provider_usage_snapshots.rs"]
mod r20261017_provider_usage_snapshots;

#[path = "regressions/r20261017_goal_descendant_accounting.rs"]
mod r20261017_goal_descendant_accounting;

#[path = "regressions/r20261017_instruction_request_matrix.rs"]
mod r20261017_instruction_request_matrix;

#[path = "regressions/r20261017_goal_cross_runtime_intent.rs"]
mod r20261017_goal_cross_runtime_intent;

#[path = "regressions/r20261017_goal_accounting_acknowledgement.rs"]
mod r20261017_goal_accounting_acknowledgement;

#[path = "regressions/r20261017_goal_continuation_settings.rs"]
mod r20261017_goal_continuation_settings;

#[path = "regressions/r20261017_provider_reasoning_summaries.rs"]
mod r20261017_provider_reasoning_summaries;

#[path = "regressions/r20261017_goal_completed_activity.rs"]
mod r20261017_goal_completed_activity;

#[path = "regressions/r20261017_goal_control_permissions.rs"]
mod r20261017_goal_control_permissions;

#[path = "regressions/r20261017_mode_admission_boundary.rs"]
mod r20261017_mode_admission_boundary;

#[path = "regressions/r20261017_goal_plan_safety.rs"]
mod r20261017_goal_plan_safety;

#[path = "regressions/r20261017_display_request_projection.rs"]
mod r20261017_display_request_projection;

#[path = "regressions/r20261018_plan_effect_ceiling.rs"]
mod r20261018_plan_effect_ceiling;

#[path = "regressions/r20261018_plan_sandbox_composition.rs"]
mod r20261018_plan_sandbox_composition;

#[path = "regressions/r20261018_plan_windows_network_guard.rs"]
mod r20261018_plan_windows_network_guard;

#[path = "regressions/r20261018_goal_context_rollout_meta.rs"]
mod r20261018_goal_context_rollout_meta;

#[path = "regressions/r20261018_question_ids.rs"]
mod r20261018_question_ids;

#[path = "regressions/r20261018_openai_response_contracts.rs"]
mod r20261018_openai_response_contracts;

#[path = "regressions/r20261018_independent_runtime_policy.rs"]
mod r20261018_independent_runtime_policy;

#[path = "regressions/r20261021_full_access_sensitive_edits.rs"]
mod r20261021_full_access_sensitive_edits;

#[path = "regressions/r20261018_automatic_approval_review.rs"]
mod r20261018_automatic_approval_review;

#[path = "regressions/r20261018_search_access_resolution.rs"]
mod r20261018_search_access_resolution;

#[path = "regressions/r20261018_review_model_targets.rs"]
mod r20261018_review_model_targets;

#[path = "regressions/r20261019_anthropic_review_tool_loop.rs"]
mod r20261019_anthropic_review_tool_loop;

#[path = "regressions/r20261019_explore_followup.rs"]
mod r20261019_explore_followup;

#[path = "regressions/r20261019_canonical_platform_sandbox.rs"]
mod r20261019_canonical_platform_sandbox;

#[path = "regressions/r20261019_canonical_session_sandbox.rs"]
mod r20261019_canonical_session_sandbox;

#[path = "regressions/r20261019_typed_permission_identity.rs"]
mod r20261019_typed_permission_identity;

#[path = "regressions/r20261019_antigravity_service_refusal.rs"]
mod r20261019_antigravity_service_refusal;

#[path = "regressions/r20261020_output_reservation_recovery.rs"]
mod r20261020_output_reservation_recovery;

#[path = "regressions/r01a1588f_native_media_projection.rs"]
mod r01a1588f_native_media_projection;

#[path = "regressions/r01a1588f_context_estimation.rs"]
mod r01a1588f_context_estimation;

#[path = "regressions/r01a1588f_compaction_hook_policy.rs"]
mod r01a1588f_compaction_hook_policy;

#[path = "regressions/r20261020_invoked_skill_receipts.rs"]
mod r20261020_invoked_skill_receipts;

#[path = "regressions/r20261020_child_compaction_isolation.rs"]
mod r20261020_child_compaction_isolation;

#[path = "regressions/r20261020_partial_compaction.rs"]
mod r20261020_partial_compaction;

#[path = "regressions/r01a1588f_context_hint_controller.rs"]
mod r01a1588f_context_hint_controller;

#[path = "regressions/r01a1588f_context_runtime_policy.rs"]
mod r01a1588f_context_runtime_policy;

#[path = "regressions/r20261020_selected_compaction_session.rs"]
mod r20261020_selected_compaction_session;

#[path = "regressions/r01a1588f_context_usage_resume.rs"]
mod r01a1588f_context_usage_resume;

#[path = "support/context_fixture.rs"]
mod context_fixture;

#[path = "regressions/r01a1588f_tool_result_retention.rs"]
mod r01a1588f_tool_result_retention;

#[path = "regressions/r20261020_context_snapshot_policy.rs"]
mod r20261020_context_snapshot_policy;

#[path = "regressions/r20261020_compaction_disable_policy.rs"]
mod r20261020_compaction_disable_policy;

#[path = "regressions/r20261020_ultra_mode_split.rs"]
mod r20261020_ultra_mode_split;

#[path = "regressions/r20261024_ultra_mode_lifetime.rs"]
mod r20261024_ultra_mode_lifetime;

#[path = "regressions/r20261024_ultra_effort_resolution.rs"]
mod r20261024_ultra_effort_resolution;

#[path = "regressions/r20261021_native_effort_catalog.rs"]
mod r20261021_native_effort_catalog;

#[path = "regressions/r20261024_child_inference_precedence.rs"]
mod r20261024_child_inference_precedence;

#[path = "regressions/r20261024_skill_effort_override.rs"]
mod r20261024_skill_effort_override;

#[path = "regressions/r20261021_gateway_connection_retry.rs"]
mod r20261021_gateway_connection_retry;

#[path = "regressions/r20261021_media_history_projection.rs"]
mod r20261021_media_history_projection;

#[path = "regressions/r20261021_provider_tool_budget_isolation.rs"]
mod r20261021_provider_tool_budget_isolation;

#[path = "regressions/r20261021_goal_idle_storage.rs"]
mod r20261021_goal_idle_storage;

#[path = "regressions/r20261021_task_idle_observation.rs"]
mod r20261021_task_idle_observation;

#[path = "regressions/r20261021_quota_refusal.rs"]
mod r20261021_quota_refusal;

#[path = "regressions/r5dea2d9b_compaction_turn_lifecycle.rs"]
mod r5dea2d9b_compaction_turn_lifecycle;
#[path = "regressions/re7b8ee23_local_compaction_lifecycle.rs"]
mod re7b8ee23_local_compaction_lifecycle;

#[path = "regressions/r5dea2d9b_compaction_migration_contract.rs"]
mod r5dea2d9b_compaction_migration_contract;

#[path = "regressions/r603e425d_compaction_refresh_answer.rs"]
mod r603e425d_compaction_refresh_answer;

#[path = "regressions/rb7c2c984_runtime_resume.rs"]
mod rb7c2c984_runtime_resume;

#[path = "regressions/rb7c2c984_terminal_persistence.rs"]
mod rb7c2c984_terminal_persistence;

#[path = "regressions/rb7c2c984_rollout_compression.rs"]
mod rb7c2c984_rollout_compression;

#[path = "regressions/r20261024_shared_subagent_admission.rs"]
mod r20261024_shared_subagent_admission;

#[path = "regressions/r20261024_user_presentation_records.rs"]
mod r20261024_user_presentation_records;

#[path = "regressions/r20261028_feature_model_targets.rs"]
mod r20261028_feature_model_targets;

#[path = "regressions/r20261028_scoped_policy_ladder.rs"]
mod r20261028_scoped_policy_ladder;

#[path = "regressions/r20261028_required_child_model.rs"]
mod r20261028_required_child_model;

#[path = "regressions/r20261029_title_thinking_requirement.rs"]
mod r20261029_title_thinking_requirement;

#[path = "regressions/r76224b5_required_thinking_title.rs"]
mod r76224b5_required_thinking_title;

#[path = "regressions/r20261031_display_message_live_history.rs"]
mod r20261031_display_message_live_history;

#[path = "regressions/r20261108_shell_yield_defaults.rs"]
mod r20261108_shell_yield_defaults;

#[path = "regressions/r20261113_side_task_checkpoint_ancestry.rs"]
mod r20261113_side_task_checkpoint_ancestry;

#[path = "regressions/r20261113_target_model_downshift_compaction.rs"]
mod r20261113_target_model_downshift_compaction;

#[path = "regressions/r20261113_anthropic_model_switch_prefill_and_repair_boundary.rs"]
mod r20261113_anthropic_model_switch_prefill_and_repair_boundary;

#[path = "regressions/r20261114_gemini_empty_continuation_model_turn.rs"]
mod r20261114_gemini_empty_continuation_model_turn;
