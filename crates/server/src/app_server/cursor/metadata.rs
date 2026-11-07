//! Metadata is a bounded view of the tools supplied for this model step.
use super::step_history::{Catalog, NAMESPACE};
use regex::RegexBuilder;
use serde_json::{json, Value};

pub(super) const SELECTOR_INVALID: &str = "cursor_metadata_selector_invalid";

fn parameter_value(value: &Value) -> Value {
    match value {
        Value::String(value) => json!({
            "value":value.chars().take(512).collect::<String>(),
            "bytes":value.len(),"utf16Units":value.encode_utf16().count(),
            "truncated":value.chars().count() > 512
        }),
        Value::Array(values) => json!({"type":"array","length":values.len()}),
        Value::Object(values) => json!({"type":"object","fieldCount":values.len()}),
        value => value.clone(),
    }
}

fn parameters(args: &Value, envelope: bool) -> Value {
    let Some(args) = args.as_object() else {
        let mut shape = super::step::safe_shape(args, &[]);
        if let Some(text) = args.as_str() {
            shape["bytes"] = json!(text.len());
            shape["utf16Units"] = json!(text.encode_utf16().count());
        }
        return shape;
    };
    let known = if envelope {
        &[
            "name",
            "providerIdentifier",
            "serverIdentifier",
            "toolName",
            "toolCallId",
            "smartModeApprovalOnly",
            "skipApproval",
            "symbolicPathArguments",
        ][..]
    } else {
        &["namespace", "server", "toolName", "pattern", "toolCallId"][..]
    };
    let mut snapshot = serde_json::Map::new();
    for key in known {
        if let Some(value) = args.get(*key) {
            snapshot.insert((*key).into(), parameter_value(value));
        }
    }
    if envelope {
        if let Some(args) = args.get("args") {
            snapshot.insert("args".into(), parameters(args, false));
        }
    }
    let unknown = args
        .iter()
        .filter(|(key, _)| !(known.contains(&key.as_str()) || envelope && key.as_str() == "args"));
    snapshot.insert(
        "unknownFields".into(),
        json!(unknown
            .clone()
            .take(16)
            .map(|(key, value)| {
                json!({"name":key.chars().take(80).collect::<String>(),
            "shape":super::step::safe_shape(value, &[])})
            })
            .collect::<Vec<_>>()),
    );
    let unknown_count = unknown.count();
    snapshot.insert("unknownFieldCount".into(), json!(unknown_count));
    snapshot.insert("unknownFieldsTruncated".into(), json!(unknown_count > 16));
    json!(snapshot)
}

fn rejected(site: &'static str, reason: &'static str, args: &Value) {
    let envelope = matches!(site, "helper_envelope" | "helper_selector");
    let shape = super::step::safe_shape(
        args,
        &[
            "namespace",
            "server",
            "toolName",
            "pattern",
            "toolCallId",
            "name",
            "providerIdentifier",
            "serverIdentifier",
            "args",
            "smartModeApprovalOnly",
            "skipApproval",
            "symbolicPathArguments",
        ],
    );
    let mut fields = json!({"site":site,"reason":reason,"shape":shape});
    if envelope {
        fields["ownership"] = json!({
            "providerMatches":args["providerIdentifier"] == NAMESPACE,
            "serverMatches":args.get("serverIdentifier").map(|owner| owner == NAMESPACE),
            "nameMatches":args["toolName"].as_str().map(|alias| args["name"] == format!("{NAMESPACE}-{alias}")),
            "smartModeDefault":args.get("smartModeApprovalOnly").map(|value| value == false),
            "skipApprovalDefault":args.get("skipApproval").map(|value| value == false),
            "symbolicPathsEmpty":args.get("symbolicPathArguments").map(|value| value.as_array().is_some_and(Vec::is_empty))
        });
    }
    let selector = if site == "helper_selector" {
        &args["args"]
    } else {
        args
    };
    let detailed = json!({
        "site":site,"reason":reason,
        "expectedNamespace":NAMESPACE,
        "parameters":parameters(args, envelope),
        "checks":{
            "namespaceMatches":selector.get("namespace").map(|value| value == NAMESPACE),
            "serverMatches":selector.get("server").map(|value| value == NAMESPACE),
            "ownerPresent":selector.get("server").is_some() || selector.get("namespace").is_some(),
            "toolNamePresent":selector.get("toolName").is_some(),
            "patternValid":selector.get("pattern").map(|value| pattern(value).is_ok()),
            "ownership":fields.get("ownership")
        }
    });
    super::diagnostics::failure("metadata_rejected", detailed);
    super::diagnostics::event("metadata_rejected", fields.clone());
    eprintln!("[Cursor] metadata rejected {fields}");
}

fn selector_error(site: &'static str, args: &Value, error: String) -> String {
    // Only model-generated discovery arguments may be resampled. This never
    // authorizes execution or changes the step's frozen tool inventory.
    let reason = match error.as_str() {
        "cursor_metadata_not_owned" => "selector_not_owned",
        "cursor_metadata_pattern_invalid" => "pattern_invalid",
        "cursor_tool_not_owned" => "tool_unknown",
        _ => "selector_shape_invalid",
    };
    rejected(site, reason, args);
    SELECTOR_INVALID.into()
}

fn short_description(description: &str) -> String {
    let units = description.encode_utf16().collect::<Vec<_>>();
    if units.len() > 200 {
        format!("{}... [truncated]", String::from_utf16_lossy(&units[..185]))
    } else {
        description.to_string()
    }
}

fn pattern(value: &Value) -> Result<regex::Regex, String> {
    let pattern = value.as_str().ok_or("cursor_metadata_call_invalid")?;
    if pattern.encode_utf16().count() > 256 {
        return Err("cursor_metadata_pattern_invalid".into());
    }
    RegexBuilder::new(pattern)
        .size_limit(1024 * 1024)
        .dfa_size_limit(1024 * 1024)
        .build()
        .map_err(|_| "cursor_metadata_pattern_invalid".into())
}

fn helper_dynamic(name: &str) -> Option<bool> {
    match name {
        "GetDynamicTools" => Some(true),
        "GetMcpTools" => Some(false),
        _ => None,
    }
}

fn helper_selector(args: &Value, helper: &str) -> Result<Value, String> {
    let dynamic = helper_dynamic(helper).ok_or("cursor_metadata_call_invalid")?;
    let owner = if dynamic { "namespace" } else { "server" };
    let mut selector = args
        .as_object()
        .filter(|args| {
            args.keys()
                .all(|key| key == owner || matches!(key.as_str(), "toolName" | "pattern"))
        })
        .cloned()
        .ok_or("cursor_metadata_call_invalid")?;
    if dynamic {
        if let Some(owner) = selector.remove("namespace") {
            selector.insert("server".into(), owner);
        }
    }
    let selector = Value::Object(selector);
    validate_selector(&selector)?;
    Ok(selector)
}

pub(super) fn mcp_result(value: &Value) -> Value {
    json!({"success":{"content":[{"text":{"text":value.to_string()}}],"isError":false}})
}

pub(super) fn mcp_result_matches(result: &Value, value: &Value) -> bool {
    result
        .as_object()
        .is_some_and(|result| result.len() == 1 && result.contains_key("success"))
        && result["success"].as_object().is_some_and(|success| {
            success
                .keys()
                .all(|key| matches!(key.as_str(), "content" | "isError"))
        })
        && result["success"]
            .get("isError")
            .is_none_or(|value| value == false)
        && result["success"]["content"] == json!([{"text":{"text":value.to_string()}}])
}

/// Every helper replay is bound to a previously generated frozen projection.
pub(super) fn history_matches(args: &Value, outer: &str, proof: &Value, raw: &str) -> bool {
    let wire = &proof["args"];
    if validate_selector(wire).is_err() || wire["toolCallId"] != raw {
        return false;
    }
    if let Some(mcp) = proof.get("mcpArgs") {
        let Some(helper) = mcp["toolName"].as_str() else {
            return false;
        };
        if helper_dynamic(helper).is_none()
            || mcp["toolCallId"] != raw
            || owned_mcp_envelope(mcp).is_err()
        {
            return false;
        }
        let Ok(mut selector) = helper_selector(&mcp["args"], helper) else {
            return false;
        };
        selector["toolCallId"] = json!(raw);
        if selector != *wire {
            return false;
        }
        let arguments = match outer {
            "CallDynamicTool" => {
                super::step_history::wrapper_arguments(args, "namespace", helper).ok()
            }
            "CallMcpTool" => super::step_history::wrapper_arguments(args, "server", helper).ok(),
            direct if direct == format!("{NAMESPACE}-{helper}") || direct == helper => Some(args),
            _ => None,
        };
        return arguments == Some(&mcp["args"]);
    }
    let mut expected = wire.clone();
    let Some(expected) = expected.as_object_mut() else {
        return false;
    };
    expected.remove("toolCallId");
    match outer {
        "GetDynamicTools" => {
            if let Some(owner) = expected.remove("server") {
                expected.insert("namespace".into(), owner);
            }
        }
        "GetMcpTools" => {}
        _ => return false,
    }
    args.as_object() == Some(expected)
}

fn owned_mcp_envelope(args: &Value) -> Result<(), String> {
    let result = validate_mcp_envelope(args);
    if result.is_err() {
        rejected("helper_envelope", "envelope_invalid", args);
    }
    result
}

pub(super) fn rejected_call(args: &Value) {
    rejected("native_call_identity", "call_id_invalid", args);
}

fn validate_mcp_envelope(args: &Value) -> Result<(), String> {
    let allowed = [
        "name",
        "args",
        "toolCallId",
        "providerIdentifier",
        "toolName",
        "serverIdentifier",
        "smartModeApprovalOnly",
        "skipApproval",
        "symbolicPathArguments",
    ];
    let alias = args["toolName"]
        .as_str()
        .ok_or("cursor_metadata_not_owned")?;
    if args
        .as_object()
        .is_none_or(|args| args.keys().any(|key| !allowed.contains(&key.as_str())))
        || args["providerIdentifier"] != NAMESPACE
        || args["name"] != format!("{NAMESPACE}-{alias}")
        || args
            .get("serverIdentifier")
            .is_some_and(|owner| owner != NAMESPACE)
        || args
            .get("smartModeApprovalOnly")
            .is_some_and(|value| value != false)
        || args.get("skipApproval").is_some_and(|value| value != false)
        || args
            .get("symbolicPathArguments")
            .is_some_and(|value| value.as_array().is_none_or(|values| !values.is_empty()))
    {
        return Err("cursor_metadata_not_owned".into());
    }
    super::step_history::safe_call_id(
        args["toolCallId"]
            .as_str()
            .ok_or("cursor_metadata_call_invalid")?,
    )?;
    Ok(())
}

pub(super) fn history_result_matches(message: &Value, part: &Value, proof: &Value) -> bool {
    let Some(result) = part["result"].as_str() else {
        return false;
    };
    if serde_json::from_str::<Value>(result).ok().as_ref() != Some(&proof["value"])
        || message["providerOptions"]["cursor"]["highLevelToolCallResult"]["isError"] == true
    {
        return false;
    }
    if let Some(parts) = part.get("experimental_content") {
        if parts.as_array().is_none_or(|parts| {
            parts.len() != 1 || parts[0] != json!({"type":"text","text":result})
        }) {
            return false;
        }
    }
    if proof.get("mcpArgs").is_some() {
        let expected_text = proof["value"].to_string();
        if result != expected_text
            || part.as_object().is_none_or(|fields| {
                fields.keys().any(|key| {
                    !matches!(
                        key.as_str(),
                        "type" | "toolCallId" | "toolName" | "result" | "experimental_content"
                    )
                })
            })
        {
            return false;
        }
        if let Some(high_level) =
            message["providerOptions"]["cursor"].get("highLevelToolCallResult")
        {
            if high_level.as_object().is_none_or(|fields| {
                fields
                    .keys()
                    .any(|key| !matches!(key.as_str(), "output" | "isError"))
            }) || high_level
                .get("isError")
                .is_some_and(|value| value != false)
                || high_level
                    .get("output")
                    .is_some_and(|value| !mcp_result_matches(value, &proof["value"]))
            {
                return false;
            }
        }
    }
    true
}

fn validate_selector(args: &Value) -> Result<(), String> {
    let map = args.as_object().ok_or("cursor_metadata_call_invalid")?;
    if map.keys().any(|key| {
        !matches!(
            key.as_str(),
            "server" | "toolName" | "pattern" | "toolCallId"
        )
    }) || args.get("server").is_some_and(|owner| owner != NAMESPACE)
    {
        return Err("cursor_metadata_not_owned".into());
    }
    if let Some(value) = args.get("pattern") {
        if value
            .as_str()
            .is_none_or(|value| value.encode_utf16().count() > 256)
        {
            return Err("cursor_metadata_pattern_invalid".into());
        }
        if args.get("toolName").is_none() {
            pattern(value)?;
        }
    }
    if let Some(tool) = args.get("toolName") {
        if args.get("server").is_none()
            || tool
                .as_str()
                .is_none_or(|name| name.is_empty() || name.len() > 96)
        {
            return Err("cursor_metadata_not_owned".into());
        }
    }
    Ok(())
}

impl Catalog {
    pub fn mcp_metadata(&self, args: &Value) -> Result<Option<Value>, String> {
        let Some(helper) = args["toolName"]
            .as_str()
            .filter(|helper| helper_dynamic(helper).is_some())
        else {
            return Ok(None);
        };
        // A caller-declared execution tool always keeps its own identity.
        if self.by_alias(helper).is_ok() {
            return Ok(None);
        }
        owned_mcp_envelope(args)?;
        let (selector, value) = helper_selector(&args["args"], helper)
            .and_then(|selector| {
                self.metadata_result(&selector, helper_dynamic(helper).expect("known helper"))
                    .map(|value| (selector, value))
            })
            .map_err(|error| selector_error("helper_selector", args, error))?;
        let mut wire = selector;
        wire["toolCallId"] = args["toolCallId"].clone();
        Ok(Some(json!({"args":wire,"value":value,"mcpArgs":args})))
    }
    pub fn validate_metadata_args(&self, args: &Value) -> Result<(), String> {
        validate_selector(args)?;
        if let Some(tool) = args.get("toolName") {
            self.by_alias(tool.as_str().ok_or("cursor_metadata_call_invalid")?)?;
        }
        Ok(())
    }

    pub fn validate_metadata_query(&self, args: &Value) -> Result<(), String> {
        self.validate_metadata_args(args)
            .map_err(|error| selector_error("native_selector", args, error))
    }

    pub fn metadata_result(&self, args: &Value, dynamic: bool) -> Result<Value, String> {
        self.validate_metadata_args(args)?;
        let owner = if dynamic { "namespace" } else { "server" };
        let status = if dynamic {
            "namespaceStatus"
        } else {
            "serverStatus"
        };
        let mut tools = self.tools.iter().collect::<Vec<_>>();
        tools.sort_by(|a, b| a.alias.cmp(&b.alias));
        if args.get("toolName").is_none() {
            if let Some(value) = args.get("pattern") {
                let matcher = pattern(value)?;
                let namespace_match = matcher.is_match(NAMESPACE);
                let mut matches = Vec::new();
                if namespace_match {
                    matches.push(json!({owner:NAMESPACE,status:"ready"}));
                }
                for tool in tools {
                    if namespace_match || matcher.is_match(&tool.alias) {
                        let mut value = json!({owner:NAMESPACE,"tool":tool.alias});
                        if !tool.description.is_empty() {
                            value["description"] = json!(short_description(&tool.description));
                        }
                        matches.push(value);
                    }
                }
                return Ok(json!({"mode":"search","pattern":value,"matches":matches}));
            }
            if args.get("server").is_none() {
                let tools = tools
                    .into_iter()
                    .map(|tool| {
                        let mut value = json!({"tool":tool.alias});
                        if !tool.description.is_empty() {
                            value["description"] = json!(short_description(&tool.description));
                        }
                        value
                    })
                    .collect::<Vec<_>>();
                let entries = if tools.is_empty() {
                    vec![]
                } else {
                    vec![json!({owner:NAMESPACE,status:"ready","tools":tools})]
                };
                let list = if dynamic { "namespaces" } else { "servers" };
                return Ok(json!({"mode":"catalog",list:entries}));
            }
        }
        self.metadata_projection(args.get("toolName").and_then(Value::as_str), dynamic)
    }

    pub fn descriptors(&self) -> Vec<Value> {
        if self.tools.is_empty() {
            return vec![];
        }
        let tools = self.definitions.iter().map(|tool| json!({
            "toolName":tool["toolName"],"description":tool["description"],
            "inputSchema":tool["inputSchema"],"inputSchemaJson":tool["inputSchema"].to_string(),
            "annotationsJson":tool["annotationsJson"]
        })).collect::<Vec<_>>();
        vec![json!({"serverName":NAMESPACE,"serverIdentifier":NAMESPACE,"tools":tools})]
    }

    pub fn frozen_state(&self, args: &Value) -> Result<Vec<Value>, String> {
        let map = args.as_object().ok_or("cursor_metadata_state_invalid")?;
        if map.keys().any(|key| {
            !matches!(
                key.as_str(),
                "serverIdentifiers" | "kickOnly" | "refreshPolicy"
            )
        }) || args
            .get("kickOnly")
            .is_some_and(|value| !value.is_boolean())
            || args.get("refreshPolicy").is_some_and(|value| {
                !matches!(
                    value.as_str(),
                    Some(
                        "MCP_REFRESH_POLICY_UNSPECIFIED"
                            | "MCP_REFRESH_POLICY_FORCE_IF_MISSING"
                            | "MCP_REFRESH_POLICY_FORCE"
                    )
                ) && !matches!(value.as_u64(), Some(0..=2))
            })
        {
            return Err("cursor_metadata_state_invalid".into());
        }
        // Refresh hints cannot expand a model step's captured tool inventory.
        if let Some(identifiers) = args.get("serverIdentifiers") {
            let identifiers = identifiers
                .as_array()
                .filter(|ids| ids.len() <= 256)
                .ok_or("cursor_metadata_state_invalid")?;
            if identifiers
                .iter()
                .any(|id| id.as_str().is_none_or(|id| id.len() > 2048))
            {
                return Err("cursor_metadata_state_invalid".into());
            }
            if !identifiers.is_empty() && !identifiers.iter().any(|id| id == NAMESPACE) {
                return Ok(vec![]);
            }
        }
        Ok(if self.tools.is_empty() {
            vec![]
        } else {
            vec![
                json!({"serverName":NAMESPACE,"serverIdentifier":NAMESPACE,"instructions":[],"tools":self.definitions,"status":"connected"}),
            ]
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn rejected_parameters_land_in_a_correlated_sanitized_failure_log() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("response-failures.jsonl");
        super::super::diagnostics::capture_failures(
            path.clone(),
            super::super::diagnostics::result("model_step", async {
                super::super::diagnostics::request_fields(
                    json!({"requestId":"fixture-request","model":"fixture-model","generation":7}),
                );
                let catalog = catalog();
                assert!(catalog
                    .validate_metadata_query(&json!({
                        "server":"foreign-server","toolName":"read","toolCallId":"query-native",
                        "pattern":"[","authorization":"Bearer fixture-credential-must-not-appear",
                        "businessPayload":{"text":"conversation-body-must-not-appear"}
                    }))
                    .is_err());
                let mut args = json!({
                    "name":"klaus_tools-GetDynamicTools","providerIdentifier":NAMESPACE,
                    "toolName":"GetDynamicTools","toolCallId":"query-helper",
                    "args":{"namespace":"foreign-namespace","pattern":"^read$"}
                });
                assert!(catalog.mcp_metadata(&args).is_err());
                args["providerIdentifier"] = json!("Bearer fixture-credential-must-not-appear");
                assert!(catalog.mcp_metadata(&args).is_err());
                Ok::<_, String>(())
            }),
        )
        .await
        .unwrap();
        let raw = std::fs::read_to_string(path).unwrap();
        let records = raw
            .lines()
            .map(|line| serde_json::from_str::<Value>(line).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(records.len(), 3);
        assert!(records.iter().all(|record| {
            record["request"]["requestId"] == "fixture-request"
                && record["request"]["model"] == "fixture-model"
                && record["request"]["generation"] == 7
                && record["operationId"] == records[0]["operationId"]
        }));
        assert_eq!(
            records[0]["fields"]["parameters"]["server"]["value"],
            "foreign-server"
        );
        assert_eq!(records[0]["fields"]["parameters"]["pattern"]["value"], "[");
        assert_eq!(records[0]["fields"]["checks"]["serverMatches"], false);
        assert_eq!(records[0]["fields"]["checks"]["patternValid"], false);
        assert_eq!(
            records[1]["fields"]["parameters"]["args"]["namespace"]["value"],
            "foreign-namespace"
        );
        assert_eq!(
            records[1]["fields"]["parameters"]["toolName"]["value"],
            "GetDynamicTools"
        );
        assert_eq!(
            records[1]["fields"]["parameters"]["toolCallId"]["value"],
            "query-helper"
        );
        assert_eq!(
            records[2]["fields"]["checks"]["ownership"]["providerMatches"],
            false
        );
        assert!(raw.contains("[redacted]"), "{raw}");
        assert!(!raw.contains("fixture-credential-must-not-appear"), "{raw}");
        assert!(!raw.contains("conversation-body-must-not-appear"), "{raw}");
    }

    #[test]
    fn parameter_snapshots_bound_invalid_values_and_report_every_omission() {
        let malformed = parameters(&json!("conversation-body-must-not-appear"), false);
        assert_eq!(malformed["type"], "string");
        assert!(!malformed.to_string().contains("conversation-body"));
        let malformed = parameters(&json!({"args":"business-body-must-not-appear"}), true);
        assert_eq!(malformed["args"]["type"], "string");
        assert!(!malformed.to_string().contains("business-body"));
        let mut args = json!({"pattern":"界".repeat(600),"toolName":["do-not-echo-array"]});
        for index in 0..20 {
            args[format!("extra-{index}")] = json!({"password":"do-not-echo-object"});
        }
        let snapshot = parameters(&args, false);
        assert_eq!(snapshot["pattern"]["utf16Units"], 600);
        assert_eq!(snapshot["pattern"]["bytes"], 1800);
        assert_eq!(snapshot["pattern"]["truncated"], true);
        assert_eq!(
            snapshot["pattern"]["value"]
                .as_str()
                .unwrap()
                .chars()
                .count(),
            512
        );
        assert_eq!(snapshot["toolName"], json!({"type":"array","length":1}));
        assert_eq!(snapshot["unknownFields"].as_array().unwrap().len(), 16);
        assert_eq!(snapshot["unknownFieldCount"], 20);
        assert_eq!(snapshot["unknownFieldsTruncated"], true);
        assert!(!snapshot.to_string().contains("do-not-echo"));
    }

    #[tokio::test]
    async fn discovery_rejections_are_scoped_diagnosable_and_content_free() {
        let (result, events) = super::super::diagnostics::capture(
            super::super::diagnostics::result("model_step", async {
                let catalog = catalog();
                let selector =
                    json!({"server":"private-owner-sentinel","toolCallId":"private-id-sentinel"});
                let native = catalog.validate_metadata_query(&selector).unwrap_err();
                let mut args = json!({
                    "name":"klaus_tools-GetDynamicTools","providerIdentifier":NAMESPACE,
                    "toolName":"GetDynamicTools","toolCallId":"metadata",
                    "args":{"namespace":"private-owner-sentinel"}
                });
                let helper = catalog.mcp_metadata(&args).unwrap_err();
                args["providerIdentifier"] = json!("private-envelope-sentinel");
                let envelope = catalog.mcp_metadata(&args).unwrap_err();
                Ok::<_, String>((native, helper, envelope))
            }),
        )
        .await;
        let (native, helper, envelope) = result.unwrap();
        assert_eq!(native, SELECTOR_INVALID);
        assert_eq!(helper, SELECTOR_INVALID);
        assert_eq!(envelope, "cursor_metadata_not_owned");
        for error in [&native, &helper] {
            assert_eq!(
                super::super::responses::public_error(error)["error"]["type"],
                "overloaded_error"
            );
        }
        assert_eq!(
            super::super::responses::public_error(&envelope)["error"]["type"],
            "invalid_request_error"
        );
        let rejected = events
            .iter()
            .filter(|event| event["phase"] == "metadata_rejected")
            .collect::<Vec<_>>();
        assert_eq!(rejected.len(), 3);
        assert_eq!(rejected[0]["fields"]["site"], "native_selector");
        assert_eq!(rejected[1]["fields"]["site"], "helper_selector");
        assert_eq!(rejected[2]["fields"]["site"], "helper_envelope");
        assert!(rejected
            .iter()
            .all(|event| event["operationId"] == events[0]["operationId"]));
        let encoded = serde_json::to_string(&events).unwrap();
        assert!(!encoded.contains("private-"), "{encoded}");
    }

    fn catalog() -> Catalog {
        Catalog::new(&json!({"tools":[
            {"function":{"name":"write","description":"Write\n a file","parameters":{"type":"object"}}},
            {"function":{"name":"read","description":"Read a file","parameters":{"type":"object","properties":{"path":{"type":"string"}}}}}
        ]})).unwrap()
    }

    #[test]
    fn discovery_selectors_search_only_owned_names_and_preserve_detail_precedence() {
        let catalog = catalog();
        assert_eq!(
            catalog
                .metadata_result(&json!({"pattern":"^read$"}), true)
                .unwrap(),
            json!({"mode":"search","pattern":"^read$","matches":[{"namespace":NAMESPACE,"tool":"read","description":"Read a file"}]})
        );
        assert_eq!(
            catalog
                .metadata_result(&json!({"pattern":"file"}), true)
                .unwrap()["matches"],
            json!([]),
            "descriptions are not searched"
        );
        assert_eq!(
            catalog
                .metadata_result(
                    &json!({"server":NAMESPACE,"pattern":"^klaus_tools$"}),
                    false
                )
                .unwrap(),
            json!({"mode":"search","pattern":"^klaus_tools$","matches":[{"server":NAMESPACE,"serverStatus":"ready"},{"server":NAMESPACE,"tool":"read","description":"Read a file"},{"server":NAMESPACE,"tool":"write","description":"Write a file"}]})
        );
        assert_eq!(
            catalog.metadata_result(&json!({}), true).unwrap(),
            json!({"mode":"catalog","namespaces":[{"namespace":NAMESPACE,"namespaceStatus":"ready","tools":[{"tool":"read","description":"Read a file"},{"tool":"write","description":"Write a file"}]}]})
        );
        assert_eq!(
            catalog
                .metadata_result(
                    &json!({"server":NAMESPACE,"toolName":"read","pattern":"unmatched"}),
                    true
                )
                .unwrap(),
            json!({"tool":"read","description":"Read a file","inputSchema":{"type":"object","properties":{"path":{"type":"string"}}}})
        );
        for args in [
            json!({"server":"foreign","pattern":".*"}),
            json!({"toolName":"read"}),
            json!({"pattern":"["}),
            json!({"pattern":"a".repeat(257)}),
            json!({"pattern":null}),
            json!({"namespace":NAMESPACE}),
            json!({"server":NAMESPACE,"toolName":"absent"}),
        ] {
            assert!(catalog.metadata_result(&args, true).is_err(), "{args}");
        }
    }

    #[test]
    fn descriptions_and_frozen_state_are_bounded_and_cannot_expand_the_catalog() {
        let catalog = catalog();
        assert_eq!(
            short_description(&"x".repeat(201)),
            format!("{}... [truncated]", "x".repeat(185))
        );
        let descriptor = &catalog.descriptors()[0];
        assert_eq!(descriptor["serverIdentifier"], NAMESPACE);
        assert_eq!(descriptor["tools"][0]["toolName"], "write");
        assert!(descriptor["tools"][0].get("name").is_none());
        assert_eq!(
            descriptor["tools"][1]["inputSchemaJson"],
            descriptor["tools"][1]["inputSchema"].to_string()
        );
        let snapshot = catalog.frozen_state(&json!({})).unwrap();
        for args in [
            json!({"serverIdentifiers":[]}),
            json!({"serverIdentifiers":[NAMESPACE],"kickOnly":true,"refreshPolicy":"MCP_REFRESH_POLICY_FORCE"}),
            json!({"refreshPolicy":2}),
        ] {
            assert_eq!(catalog.frozen_state(&args).unwrap(), snapshot);
        }
        assert_eq!(snapshot[0]["tools"], json!(catalog.definitions));
        assert!(catalog
            .frozen_state(&json!({"serverIdentifiers":["foreign"]}))
            .unwrap()
            .is_empty());
        for args in [
            json!({"serverIdentifiers":"foreign"}),
            json!({"kickOnly":null}),
            json!({"refreshPolicy":3}),
            json!({"refresh":true}),
            json!({"serverIdentifiers":[null]}),
        ] {
            assert!(catalog.frozen_state(&args).is_err());
        }
    }

    #[test]
    fn helper_history_requires_the_exact_recorded_selector() {
        let wire = json!({"pattern":"^read$","toolCallId":"metadata"});
        assert!(history_matches(
            &json!({"pattern":"^read$"}),
            "GetDynamicTools",
            &json!({"args":wire}),
            "metadata"
        ));
        for args in [
            json!({"pattern":"write"}),
            json!({"pattern":"^read$","namespace":NAMESPACE}),
            json!({"pattern":"^read$","toolName":"read"}),
        ] {
            assert!(!history_matches(
                &args,
                "GetDynamicTools",
                &json!({"args":wire}),
                "metadata"
            ));
        }
        assert!(!history_matches(
            &json!({"pattern":"^read$"}),
            "CallDynamicTool",
            &json!({"args":wire}),
            "metadata"
        ));
        for wire in [
            json!({"server":"foreign","toolCallId":"metadata"}),
            json!({"pattern":"^read$","extra":true,"toolCallId":"metadata"}),
            json!({"pattern":"^read$","toolCallId":"different"}),
        ] {
            let mut args = wire.clone();
            let args = args.as_object_mut().unwrap();
            args.remove("toolCallId");
            if let Some(owner) = args.remove("server") {
                args.insert("namespace".into(), owner);
            }
            assert!(!history_matches(
                &json!(args),
                "GetDynamicTools",
                &json!({"args":wire}),
                "metadata"
            ));
        }
    }

    #[test]
    fn mcp_history_outputs_accept_only_the_exact_generated_text_projection() {
        let args = json!({"name":"klaus_tools-GetDynamicTools","providerIdentifier":NAMESPACE,"toolName":"GetDynamicTools","toolCallId":"metadata","args":{"namespace":NAMESPACE,"toolName":"read"}});
        let proof = catalog().mcp_metadata(&args).unwrap().unwrap();
        let text = proof["value"].to_string();
        let part = json!({"type":"tool-result","toolCallId":"metadata","toolName":"CallDynamicTool","result":text,"experimental_content":[{"type":"text","text":text}]});
        let message = json!({"role":"tool","content":[part],"providerOptions":{"cursor":{"highLevelToolCallResult":{"output":mcp_result(&proof["value"]),"isError":false}}}});
        assert!(history_result_matches(
            &message,
            &message["content"][0],
            &proof
        ));
        let mut omitted_default = message.clone();
        omitted_default["providerOptions"]["cursor"]["highLevelToolCallResult"]["output"]
            ["success"]
            .as_object_mut()
            .unwrap()
            .remove("isError");
        assert!(history_result_matches(
            &omitted_default,
            &omitted_default["content"][0],
            &proof
        ));
        for mutation in [
            "image",
            "structured",
            "reminders",
            "location",
            "error",
            "errorType",
            "extra",
            "contentType",
            "text",
            "multiple",
            "experimental",
            "partExtra",
            "highLevelExtra",
        ] {
            let mut changed = message.clone();
            let output =
                &mut changed["providerOptions"]["cursor"]["highLevelToolCallResult"]["output"];
            match mutation {
                "image" => output["success"]["content"]
                    .as_array_mut()
                    .unwrap()
                    .push(json!({"image":{"data":"AA==","mimeType":"image/png"}})),
                "structured" => output["success"]["structuredContent"] = json!({}),
                "reminders" => output["success"]["systemReminders"] = json!(["execute"]),
                "location" => output["success"]["outputLocation"] = json!("remote"),
                "error" => output["success"]["isError"] = json!(true),
                "errorType" => output["success"]["isError"] = json!("false"),
                "extra" => output["error"] = json!({}),
                "contentType" => output["success"]["content"][0]["text"]["extra"] = json!(true),
                "text" => output["success"]["content"][0]["text"]["text"] = json!("{}"),
                "multiple" => output["success"]["content"]
                    .as_array_mut()
                    .unwrap()
                    .push(json!({"text":{"text":""}})),
                "experimental" => changed["content"][0]["experimental_content"]
                    .as_array_mut()
                    .unwrap()
                    .push(json!({"type":"image","data":"AA=="})),
                "partExtra" => changed["content"][0]["structuredContent"] = json!({}),
                "highLevelExtra" => {
                    changed["providerOptions"]["cursor"]["highLevelToolCallResult"]
                        ["systemReminders"] = json!(["execute"])
                }
                _ => unreachable!(),
            }
            assert!(
                !history_result_matches(&changed, &changed["content"][0], &proof),
                "{mutation}"
            );
        }
    }
}
