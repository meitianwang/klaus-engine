//! Network domain egress policy — a right-sized allow/deny model applied at the
//! engine's own in-process HTTP egress (the model-driven `web_fetch` tool, where
//! the model picks arbitrary URLs). Shell `curl`/`wget` and raw sockets are NOT
//! gated here — the engine cannot see them without a transparent proxy; that is
//! an honest scope limit, not a bypass.
//!
//! Matching convention: a bare domain covers the apex AND every subdomain. `*`
//! is a global wildcard. Comparison is case-insensitive and a trailing FQDN dot
//! is ignored.

use crate::runtime_values::RuntimeString;

/// Normalize a host: trim, lowercase, strip surrounding brackets (IPv6 literals
/// arrive bracketed, e.g. `[2606:4700:4700::1111]`), and drop a trailing FQDN
/// dot.
fn normalize_host(host: &str) -> String {
    let lowered = host.trim().to_lowercase();
    let mut s: &str = &lowered;
    s = s.strip_prefix('[').unwrap_or(s);
    s = s.strip_suffix(']').unwrap_or(s);
    s = s.strip_suffix('.').unwrap_or(s);
    s.to_string()
}

/// Normalize a domain pattern: trim, lowercase, strip surrounding brackets, drop
/// a leading `*.`/`**.` wildcard prefix (folded into the bare = apex+subdomain
/// convention), then ignore a leading dot and a trailing FQDN dot.
fn normalize_domain(domain: &str) -> String {
    let lowered = domain.trim().to_lowercase();
    let mut s: &str = &lowered;
    s = s.strip_prefix('[').unwrap_or(s);
    s = s.strip_suffix(']').unwrap_or(s);
    s = s
        .strip_prefix("**.")
        .or_else(|| s.strip_prefix("*."))
        .unwrap_or(s);
    s = s.strip_prefix('.').unwrap_or(s);
    s = s.strip_suffix('.').unwrap_or(s);
    s.to_string()
}

/// Does `host` equal `domain` or sit under it as a subdomain?
pub fn host_matches_domain(host: &str, domain: &str) -> bool {
    let h = normalize_host(host);
    let d = normalize_domain(domain);
    if d.is_empty() {
        return false;
    }
    if d == "*" {
        return true;
    }
    h == d || h.ends_with(&format!(".{d}"))
}

/// A network domain policy: an optional allow-list and an optional block-list.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct NetworkDomainPolicy {
    /// When non-empty, an ALLOW-LIST: only these domains (+ their subdomains) are
    /// permitted.
    pub allowed_domains: Option<Vec<RuntimeString>>,
    /// Always denied (+ their subdomains). DENY WINS over an allow match.
    pub blocked_domains: Option<Vec<RuntimeString>>,
}

/// The outcome of evaluating a host against a [`NetworkDomainPolicy`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HostPolicyDecision {
    Allow,
    Deny,
}

impl HostPolicyDecision {
    /// The wire/string form (`'allow' | 'deny'`).
    pub fn as_str(&self) -> &'static str {
        match self {
            HostPolicyDecision::Allow => "allow",
            HostPolicyDecision::Deny => "deny",
        }
    }
}

/// Evaluate a host against the network domain policy. Precedence (None < Allow <
/// Deny):
///  - a `blocked_domains` match -> `Deny` (deny wins, even over an allow match);
///  - else, if `allowed_domains` is non-empty, it is an allow-list -> a match is
///    `Allow`, anything else is `Deny`;
///  - else (no allow-list, not blocked) -> `Allow`.
///
/// An empty/absent policy returns `Allow` (unrestricted — the default).
pub fn evaluate_host_policy(
    host: &str,
    policy: Option<&NetworkDomainPolicy>,
) -> HostPolicyDecision {
    let empty: Vec<RuntimeString> = Vec::new();
    let blocked = policy
        .and_then(|p| p.blocked_domains.as_ref())
        .unwrap_or(&empty);
    for domain in blocked {
        let Ok(domain) = String::from_utf16(domain.as_utf16()) else {
            return HostPolicyDecision::Deny;
        };
        if host_matches_domain(host, &domain) {
            return HostPolicyDecision::Deny;
        }
    }
    let allowed = policy
        .and_then(|p| p.allowed_domains.as_ref())
        .unwrap_or(&empty);
    if !allowed.is_empty() {
        let mut matched = false;
        for domain in allowed {
            let Ok(domain) = String::from_utf16(domain.as_utf16()) else {
                return HostPolicyDecision::Deny;
            };
            matched |= host_matches_domain(host, &domain);
        }
        return if matched {
            HostPolicyDecision::Allow
        } else {
            HostPolicyDecision::Deny
        };
    }
    HostPolicyDecision::Allow
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matches_apex_and_subdomains() {
        assert!(host_matches_domain("example.com", "example.com"));
        assert!(host_matches_domain("api.example.com", "example.com"));
        assert!(host_matches_domain("a.b.example.com", "example.com"));
    }

    #[test]
    fn does_not_match_a_sibling_or_suffix_lookalike() {
        assert!(!host_matches_domain("notexample.com", "example.com"));
        assert!(!host_matches_domain("example.com.evil.com", "example.com"));
    }

    #[test]
    fn is_case_insensitive_and_ignores_a_trailing_dot() {
        assert!(host_matches_domain("EXAMPLE.COM", "example.com"));
        assert!(host_matches_domain("example.com.", "Example.Com"));
    }

    #[test]
    fn strips_ipv6_brackets_on_both_sides() {
        assert!(host_matches_domain(
            "[2606:4700:4700::1111]",
            "2606:4700:4700::1111"
        ));
        assert!(host_matches_domain(
            "2606:4700:4700::1111",
            "[2606:4700:4700::1111]"
        ));
    }

    #[test]
    fn folds_wildcard_prefixes_into_apex_plus_subdomain() {
        assert!(host_matches_domain("sub.internal", "*.internal"));
        assert!(host_matches_domain("internal", "*.internal"));
        assert!(host_matches_domain("sub.internal", "**.internal"));
        assert!(host_matches_domain("deep.sub.internal", "*.internal"));
    }

    #[test]
    fn a_lone_star_matches_everything_and_empty_matches_nothing() {
        assert!(host_matches_domain("anything.example.org", "*"));
        assert!(!host_matches_domain("anything", ""));
        assert!(!host_matches_domain("anything", "."));
    }

    #[test]
    fn absent_or_empty_policy_allows() {
        assert_eq!(
            evaluate_host_policy("example.com", None),
            HostPolicyDecision::Allow
        );
        let empty = NetworkDomainPolicy::default();
        assert_eq!(
            evaluate_host_policy("example.com", Some(&empty)),
            HostPolicyDecision::Allow
        );
    }

    #[test]
    fn a_block_match_denies() {
        let policy = NetworkDomainPolicy {
            allowed_domains: None,
            blocked_domains: Some(vec!["internal".into()]),
        };
        assert_eq!(
            evaluate_host_policy("db.internal", Some(&policy)),
            HostPolicyDecision::Deny
        );
        assert_eq!(
            evaluate_host_policy("example.com", Some(&policy)),
            HostPolicyDecision::Allow
        );
    }

    #[test]
    fn an_allow_list_denies_everything_else() {
        let policy = NetworkDomainPolicy {
            allowed_domains: Some(vec!["example.com".into()]),
            blocked_domains: None,
        };
        assert_eq!(
            evaluate_host_policy("api.example.com", Some(&policy)),
            HostPolicyDecision::Allow
        );
        assert_eq!(
            evaluate_host_policy("evil.com", Some(&policy)),
            HostPolicyDecision::Deny
        );
    }

    #[test]
    fn deny_wins_over_an_allow_match() {
        let policy = NetworkDomainPolicy {
            allowed_domains: Some(vec!["example.com".into()]),
            blocked_domains: Some(vec!["secret.example.com".into()]),
        };
        assert_eq!(
            evaluate_host_policy("secret.example.com", Some(&policy)),
            HostPolicyDecision::Deny
        );
        assert_eq!(
            evaluate_host_policy("public.example.com", Some(&policy)),
            HostPolicyDecision::Allow
        );
    }

    #[test]
    fn non_scalar_policy_entries_fail_closed_without_aliasing_replacement_character() {
        let invalid = RuntimeString::from_utf16([0xd800]);
        let replacement = RuntimeString::from("\u{fffd}");
        assert_ne!(invalid, replacement);

        let blocked = NetworkDomainPolicy {
            allowed_domains: None,
            blocked_domains: Some(vec![invalid.clone()]),
        };
        assert_eq!(
            evaluate_host_policy("example.com", Some(&blocked)),
            HostPolicyDecision::Deny
        );

        let allowed = NetworkDomainPolicy {
            allowed_domains: Some(vec![RuntimeString::from("example.com"), invalid]),
            blocked_domains: None,
        };
        assert_eq!(
            evaluate_host_policy("example.com", Some(&allowed)),
            HostPolicyDecision::Deny
        );
    }
}
