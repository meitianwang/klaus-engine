// Copyright 2025 OpenAI
// SPDX-License-Identifier: Apache-2.0
// Modified to bind runtime-owned errors, metadata and session contracts.

//! Outcome-only structured assessment with optional risk and authorization metadata.

use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum RiskLevel {
    Low,
    Medium,
    High,
    Critical,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum UserAuthorization {
    Unknown,
    Low,
    Medium,
    High,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum AssessmentOutcome {
    Allow,
    Deny,
}

/// Structured output contract that the approval reviewer must satisfy.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct ReviewAssessment {
    pub risk_level: RiskLevel,
    pub user_authorization: UserAuthorization,
    pub outcome: AssessmentOutcome,
    pub rationale: String,
}

/// The model is asked for strict JSON, but we still accept a surrounding prose
/// wrapper so transient formatting drift fails less noisily during dogfooding.
/// Non-JSON output is still a review failure; this is only a thin recovery path
/// for cases where the model wrapped the JSON in extra prose.
pub fn parse_assessment(text: &str) -> Result<ReviewAssessment, String> {
    let parsed_payload = match serde_json::from_str::<AssessmentPayload>(text) {
        Ok(payload) => payload,
        Err(_) => match (text.find('{'), text.rfind('}')) {
            (Some(start), Some(end)) if start < end => {
                let slice = text
                    .get(start..=end)
                    .ok_or_else(|| "approval assessment was not valid JSON".to_string())?;
                serde_json::from_str::<AssessmentPayload>(slice)
                    .map_err(|error| error.to_string())?
            }
            _ => return Err("approval assessment was not valid JSON".to_string()),
        },
    };

    let outcome = parsed_payload.outcome;
    let risk_level = parsed_payload.risk_level.unwrap_or(match outcome {
        AssessmentOutcome::Allow => RiskLevel::Low,
        AssessmentOutcome::Deny => RiskLevel::High,
    });
    let rationale = parsed_payload
        .rationale
        .filter(|rationale| !rationale.trim().is_empty())
        .unwrap_or_else(|| match outcome {
            AssessmentOutcome::Allow => {
                "Auto-review returned a low-risk allow decision.".to_string()
            }
            AssessmentOutcome::Deny => {
                "Auto-review returned a deny decision without a rationale.".to_string()
            }
        });

    Ok(ReviewAssessment {
        risk_level,
        user_authorization: parsed_payload
            .user_authorization
            .unwrap_or(UserAuthorization::Unknown),
        outcome,
        rationale,
    })
}

#[derive(Deserialize)]
struct AssessmentPayload {
    risk_level: Option<RiskLevel>,
    user_authorization: Option<UserAuthorization>,
    outcome: AssessmentOutcome,
    rationale: Option<String>,
}

/// JSON schema supplied as `final_output_json_schema` to guide a structured
/// final answer from the approval review session.
///
/// Keep this next to `output_contract_prompt()` so the prompt text and
/// output schema stay aligned.
pub fn output_schema() -> Value {
    serde_json::json!({
        "type": "object",
        "additionalProperties": false,
        "properties": {
            "risk_level": {
                "type": "string",
                "enum": ["low", "medium", "high", "critical"]
            },
            "user_authorization": {
                "type": "string",
                "enum": ["unknown", "low", "medium", "high"]
            },
            "outcome": {
                "type": "string",
                "enum": ["allow", "deny"]
            },
            "rationale": {
                "type": "string"
            }
        },
        "required": ["outcome"]
    })
}

/// Prompt fragment that describes the exact JSON contract paired with
/// `output_schema()`.
pub fn output_contract_prompt() -> &'static str {
    r#"You may use read-only tool checks to gather any additional context you need before deciding. When you are ready to answer, your final message must be strict JSON.

For low-risk actions, give the final answer directly: {"outcome":"allow"}.

For anything else, use this JSON schema:
{
  "risk_level": "low" | "medium" | "high" | "critical",
  "user_authorization": "unknown" | "low" | "medium" | "high",
  "outcome": "allow" | "deny",
  "rationale": string
}"#
}

#[cfg(test)]
#[path = "assessment_tests.rs"]
mod tests;
