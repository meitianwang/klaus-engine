//! Defect: user-selected browser tab references had no executable typed host operation.
//! Root cause: the browser operation table omitted resolution and external user-tab claims.
//! Fixed for the 2026-10-05 browser tab mention work item; the JS client regression also
//! pins claim_tab instead of the previous read-only tabs:get mapping.

use klaus_agent_core::runtime_values::{runtime_codec, RuntimeJson, RuntimeString};

#[test]
fn browser_tab_mentions_have_typed_host_operations_and_exact_targets() {
    use klaus_agent_core::browser::{
        BrowserHostToolMapping, BrowserOperation, BrowserPermissionSet, BrowserRuntimeRequest,
        BrowserTarget,
    };
    let mention = RuntimeString::from("plugin://browser@klaus-bundled?mention=tab-v1&source=extension&browserId=chrome%3Aprofile-1&tabId=42&title=Orders&url=https%3A%2F%2Fshop.test%2Forders");
    let resolve = BrowserOperation::ResolveTab {
        mention: mention.clone(),
    };
    let encoded = runtime_codec::encode_value(&resolve).unwrap();
    assert_eq!(encoded.get("type"), Some(&RuntimeJson::from("resolve_tab")));
    assert_eq!(encoded.get("mention"), Some(&RuntimeJson::String(mention)));
    assert_eq!(
        runtime_codec::decode_value::<BrowserOperation>(&encoded).unwrap(),
        resolve
    );
    assert!(
        runtime_codec::decode_value::<BrowserOperation>(
            &klaus_agent_core::runtime_values::runtime_json::parse(&RuntimeString::from(
                "{\"type\":\"resolve_tab\"}"
            ))
            .unwrap()
        )
        .is_err(),
        "a mention cannot be inferred from the active tab"
    );

    let request = BrowserRuntimeRequest::new(
        BrowserOperation::ClaimTab,
        BrowserTarget {
            browser_id: Some("chrome:profile-1".into()),
            tab_id: Some("42".into()),
            session_id: None,
        },
        Some(5000),
    );
    let encoded = runtime_codec::encode_value(&request).unwrap();
    assert_eq!(
        runtime_codec::decode_value::<BrowserRuntimeRequest>(&encoded).unwrap(),
        request
    );
    assert_eq!(
        encoded.get("target").unwrap().get("browserId"),
        Some(&RuntimeJson::from("chrome:profile-1"))
    );
    assert_eq!(
        encoded.get("target").unwrap().get("tabId"),
        Some(&RuntimeJson::from("42"))
    );
    for operation in [resolve, BrowserOperation::ClaimTab] {
        let permissions = BrowserPermissionSet::for_operation(&operation);
        assert!(permissions.manage_tabs && permissions.session_state);
        assert!(
            !permissions.navigate,
            "resolving a selected tab never opens a replacement"
        );
    }
    let mapping = BrowserHostToolMapping::for_mcp_server("klaus-browser");
    assert_eq!(mapping.resolve_tab, "mcp__klaus-browser__resolve_tab");
    assert_eq!(mapping.claim_tab, "mcp__klaus-browser__claim_tab");
}
