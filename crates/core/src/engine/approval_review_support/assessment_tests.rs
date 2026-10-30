// Copyright 2025 OpenAI
// SPDX-License-Identifier: Apache-2.0
// Modified for runtime component contracts and deterministic regressions.

use super::*;

#[test]
fn parse_assessment_extracts_embedded_json() {
    let parsed = parse_assessment(
        "preface {\"risk_level\":\"medium\",\"user_authorization\":\"low\",\"outcome\":\"allow\",\"rationale\":\"ok\"}",
    )
    .expect("approval assessment");

    assert_eq!(
        parsed,
        ReviewAssessment {
            risk_level: RiskLevel::Medium,
            user_authorization: UserAuthorization::Low,
            outcome: AssessmentOutcome::Allow,
            rationale: "ok".to_string(),
        }
    );
}

#[test]
fn parse_assessment_treats_bare_allow_as_low_risk() {
    let parsed = parse_assessment(r#"{"outcome":"allow"}"#).expect("approval assessment");

    assert_eq!(
        parsed,
        ReviewAssessment {
            risk_level: RiskLevel::Low,
            user_authorization: UserAuthorization::Unknown,
            outcome: AssessmentOutcome::Allow,
            rationale: "Auto-review returned a low-risk allow decision.".to_string(),
        }
    );
}

#[test]
fn parse_assessment_treats_bare_deny_as_high_risk() {
    let parsed = parse_assessment(r#"{"outcome":"deny"}"#).expect("approval assessment");

    assert_eq!(
        parsed,
        ReviewAssessment {
            risk_level: RiskLevel::High,
            user_authorization: UserAuthorization::Unknown,
            outcome: AssessmentOutcome::Deny,
            rationale: "Auto-review returned a deny decision without a rationale.".to_string(),
        }
    );
}

#[test]
fn completed_outcome_remains_authoritative_without_risk_rejudgement() {
    let result = parse_assessment(r#"{"outcome":"allow","risk_level":"critical","user_authorization":"unknown","rationale":"Approved by the completed policy assessment","extra":"retained parser tolerance"}"#).unwrap();
    assert_eq!(result.outcome, AssessmentOutcome::Allow);
    assert_eq!(result.risk_level, RiskLevel::Critical);
    assert_eq!(result.user_authorization, UserAuthorization::Unknown);
}

#[test]
fn parser_preserves_optional_fields_and_defaults_only_missing_or_blank_rationale() {
    let result = parse_assessment(r#"prefix {"outcome":"deny","rationale":"   ","risk_level":null,"user_authorization":null} suffix"#).unwrap();
    assert_eq!(result.risk_level, RiskLevel::High);
    assert_eq!(result.user_authorization, UserAuthorization::Unknown);
    assert_eq!(
        result.rationale,
        "Auto-review returned a deny decision without a rationale."
    );
    assert!(parse_assessment(r#"{"outcome":"allow","risk_level":"unsupported"}"#).is_err());
    assert!(parse_assessment(r#"{"outcome":"ask_user"}"#).is_err());
    assert!(parse_assessment(r#"{"rationale":"missing outcome"}"#).is_err());
}

#[test]
fn schema_requires_only_outcome_and_parser_adds_no_private_payload_limit() {
    assert_eq!(output_schema()["required"], serde_json::json!(["outcome"]));
    let rationale = "x".repeat(40_000);
    let parsed =
        parse_assessment(&serde_json::json!({"outcome":"allow","rationale":rationale}).to_string())
            .unwrap();
    assert_eq!(parsed.rationale.len(), 40_000);
}
