//! A replaceable engine-store layer, separate from project and session rules.

use std::sync::Mutex;

use crate::permission::permission_update::{apply_add_rules, SessionPermissionState};
use crate::permission::rule_store::DurableRules;
use crate::permission::rules::parse_rule_string;
use crate::sync_ext::MutexExt;
use crate::types::permission::{PermissionDestination, PermissionRule, PermissionUpdate};

#[derive(Debug, Default)]
struct State {
    stored: SessionPermissionState,
    // Approval takes effect before its event reaches the facade's disk writer.
    // A concurrent reload must not discard a grant that is still being saved.
    pending: Vec<(PermissionUpdate, usize)>,
}

#[derive(Debug, Default)]
pub struct DurablePermissionLayer {
    state: Mutex<State>,
}

fn parsed(rules: &DurableRules) -> SessionPermissionState {
    SessionPermissionState {
        allow_rules: rules
            .allow
            .iter()
            .filter_map(|rule| parse_rule_string(rule))
            .collect(),
        deny_rules: rules
            .deny
            .iter()
            .filter_map(|rule| parse_rule_string(rule))
            .collect(),
        ask_rules: rules
            .ask
            .iter()
            .filter_map(|rule| parse_rule_string(rule))
            .collect(),
    }
}

impl DurablePermissionLayer {
    /// A single consistent snapshot across all three rule classes.
    pub fn snapshot(&self) -> SessionPermissionState {
        let state = self.state.lock_recover();
        let mut result = state.stored.clone();
        for (update, _) in &state.pending {
            apply_add_rules(&mut result, update);
        }
        result
    }

    pub fn replace_stored(&self, rules: &DurableRules) {
        let next = parsed(rules);
        self.state.lock_recover().stored = next;
    }

    pub fn grant_pending(&self, update: PermissionUpdate) {
        if matches!(update, PermissionUpdate::AddRules { destination, .. }
            if destination != PermissionDestination::Session)
        {
            let mut state = self.state.lock_recover();
            if let Some((_, count)) = state
                .pending
                .iter_mut()
                .find(|(pending, _)| pending == &update)
            {
                *count = count.saturating_add(1);
            } else {
                state.pending.push((update, 1));
            }
        }
    }

    /// Called only after the facade saves this exact grant, under its store
    /// mutex. Remove one occurrence: a concurrent identical approval has its
    /// own later persistence receipt and must remain pending until that write.
    pub fn confirm_saved(&self, update: &PermissionUpdate, rules: &[PermissionRule]) {
        let PermissionUpdate::AddRules {
            behavior,
            destination,
            ..
        } = update
        else {
            return;
        };
        let mut state = self.state.lock_recover();
        // Publish only this save. Unrelated disk edits require explicit reload,
        // and a malformed disk row must not erase an existing live deny rule.
        apply_add_rules(
            &mut state.stored,
            &PermissionUpdate::AddRules {
                rules: rules.to_vec(),
                behavior: *behavior,
                destination: *destination,
            },
        );
        if let Some(index) = state
            .pending
            .iter()
            .position(|(pending, _)| pending == update)
        {
            if state.pending[index].1 > 1 {
                state.pending[index].1 -= 1;
            } else {
                state.pending.remove(index);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::permission::PermissionBehavior;

    fn grant() -> PermissionUpdate {
        PermissionUpdate::AddRules {
            rules: vec![parse_rule_string("Write").unwrap()],
            behavior: PermissionBehavior::Allow,
            destination: PermissionDestination::UserSettings,
        }
    }

    #[test]
    fn reload_preserves_unflushed_grants_but_removes_confirmed_grants() {
        let layer = DurablePermissionLayer::default();
        let saved = vec![parse_rule_string("Write").unwrap()];
        layer.grant_pending(grant());
        layer.replace_stored(&DurableRules::default());
        assert_eq!(layer.snapshot().allow_rules.len(), 1);
        layer.confirm_saved(&grant(), &saved);
        layer.replace_stored(&DurableRules::default());
        assert!(layer.snapshot().allow_rules.is_empty());
    }

    #[test]
    fn one_receipt_cannot_discard_a_later_identical_approval() {
        let layer = DurablePermissionLayer::default();
        let saved = vec![parse_rule_string("Write").unwrap()];
        layer.grant_pending(grant());
        layer.grant_pending(grant());
        layer.confirm_saved(&grant(), &saved);
        layer.replace_stored(&DurableRules::default());
        assert_eq!(layer.snapshot().allow_rules.len(), 1);
        layer.confirm_saved(&grant(), &saved);
        layer.replace_stored(&DurableRules::default());
        assert!(layer.snapshot().allow_rules.is_empty());
    }

    #[test]
    fn a_save_receipt_preserves_unrelated_live_deny_and_ask_rules() {
        let layer = DurablePermissionLayer::default();
        layer.replace_stored(&DurableRules {
            deny: vec!["Bash".into()],
            ask: vec!["Read".into()],
            ..Default::default()
        });
        layer.grant_pending(grant());
        layer.confirm_saved(&grant(), &[parse_rule_string("Write").unwrap()]);
        let policy = layer.snapshot();
        assert_eq!(policy.deny_rules, vec![parse_rule_string("Bash").unwrap()]);
        assert_eq!(policy.ask_rules, vec![parse_rule_string("Read").unwrap()]);
        assert_eq!(
            policy.allow_rules,
            vec![parse_rule_string("Write").unwrap()]
        );
    }
}
