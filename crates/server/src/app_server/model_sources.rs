//! What a subscription selection resolves to.
//!
//! `[modelProviders]` answers "which BYO key"; this answers the other way a
//! session gets a provider — a subscription the user has already signed into.
//! From the caller's side both are the same decision, one `--model` value, so
//! they are resolved at the same seam and no caller has to know which kind it
//! named.
//!
//! ## Why the vendor constants are in this crate
//!
//! `klaus-agent-core`'s provider factories are deliberately generic: they take an
//! [`OAuthConfig`](klaus_agent_core::provider::oauth::OAuthConfig) and embed no
//! vendor's client id or endpoints, which is what keeps the engine core neutral
//! about whose subscription it is speaking to. That invariant is preserved here —
//! nothing below is added to core. The app-server layer is already where product
//! policy lives (it owns which config section holds models, which credential
//! store backs them, and what the tier words mean), so "which vendor, which
//! endpoints, which credential file" belongs at this layer.
//!
//! ## Where the credentials come from
//!
//! The same records the desktop client and the Claude Code toolchain already
//! write, so one sign-in serves every client on the machine. Nothing here mints
//! or migrates a credential; it reads what a login left behind.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use serde_json::{json, Map, Value};

// ---------------------------------------------------------------- claude.ai

/// Claude Code's public OAuth client id and endpoints, the same values the
/// desktop client sends. Public by construction: this is an installed-app OAuth
/// client, so there is no secret to keep.
const CLAUDE_CLIENT_ID: &str = "9d1c250a-e61b-44d9-88ed-5944d1962f5e";
const CLAUDE_AUTHORIZE_URL: &str = "https://claude.com/cai/oauth/authorize";
const CLAUDE_TOKEN_URL: &str = "https://platform.claude.com/v1/oauth/token";
const CLAUDE_REDIRECT_URL: &str = "https://platform.claude.com/oauth/code/callback";

/// The scope set, in the order the ecosystem requests them.
const CLAUDE_SCOPES: &[&str] = &[
    "org:create_api_key",
    "user:profile",
    "user:inference",
    "user:sessions:claude_code",
    "user:mcp_servers",
    "user:file_upload",
];

/// The current-generation model behind each subscription alias.
///
/// These follow the vendor's own lineup rather than being pinned: the previous
/// generation is marked legacy on the model pages, and a legacy model keeps
/// answering right up until it is retired, so a pinned id ages into a broken
/// subscription instead of a slower one.
///
/// The ids track the vendor's own alias table, not merely "something that works":
/// the official client resolves its `opus` alias to Opus 5.5 and its `fable` alias
/// to Fable 5.2, and a subscription that keeps naming the previous point release
/// silently runs the older model for every user who never picks explicitly.
const CLAUDE_OPUS: &str = "claude-opus-5-5";
const CLAUDE_SONNET: &str = "claude-sonnet-5-5";
const CLAUDE_HAIKU: &str = "claude-haiku-5-5";

/// The lineup's top model, above Opus 5.5 on capability.
///
/// It is deliberately NOT a tier default. The tier map is what auxiliary work
/// resolves through — sub-agents, summaries, background turns — so promoting the
/// most expensive model to the `high` slot would re-price every one of them for a
/// gain the user never asked for. Fable is offered as a main model the user picks
/// on purpose, and a settings tier pick can still put it on a tier.
const CLAUDE_FABLE: &str = "claude-fable-5-2";

/// Refresh this far before expiry rather than at it, so a token cannot lapse
/// between the check and the first request of the turn.
pub(crate) const REFRESH_BUFFER_MS: i64 = 5 * 60 * 1000;

// ---------------------------------------------------------------- codex

const CODEX_CLIENT_ID: &str = "app_EMoamEEZ73f0CkXaXp7hrann";
const CODEX_BASE_URL: &str = "https://chatgpt.com/backend-api/codex";
/// The OpenAI auth host, overridable so the flow can be driven against a mock
/// (the real host is DNS-poisoned on some networks).
fn codex_auth_host() -> String {
    std::env::var("KLAUS_CODEX_AUTH_URL")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| "https://auth.openai.com".to_string())
        .trim_end_matches('/')
        .to_string()
}
const CODEX_FALLBACK_MODEL: &str = "gpt-5.6-terra";
/// The main-model default for a Codex subscription that has not picked one:
/// the current generation's mid model. The catalog head is the flagship, and
/// a DEFAULT that names it prices every unpicked conversation at five times
/// Sol — the flagship stays a deliberate pick, exactly as it does for the
/// auxiliary tiers below. Older lineups keep the previous Sol; a lineup with
/// neither falls back to its head rather than naming an unavailable model.
const CODEX_DEFAULT_MAIN_MODEL: &str = "gpt-6.1-sol";
const CODEX_PREVIOUS_MAIN_MODEL: &str = "gpt-6-sol";
/// Codex rejects a request whose client version it does not recognise, so the
/// header is always sent; the variable exists to follow the service without a
/// rebuild.
///
/// This is a GATE, not a label. The service advertises only the models whose
/// `minimal_client_version` the announced version meets, so this value decides
/// which lineup the picker can show at all. Pinned at 0.153.1 the account's own
/// `gpt-6-sol` and `gpt-6-luna` rows (both `0.155.0`) were withheld: the picker
/// offered five models where the official client offers seven, and only a
/// version bump could reveal them. Follow the official client's lineup, not
/// just the first request that starts failing.
const CODEX_CLIENT_VERSION: &str = "0.159.2";

/// The client version every Codex call announces: the override when it looks like
/// a version, else the pinned fallback. One reader, because the session's request
/// headers and the catalog fetch have to claim the same client — the service caches
/// its answer per version.
///
/// The shape check is not decoration: the backend refuses a client version it does
/// not recognise, so a typo in the variable would take the subscription down
/// wholesale rather than fall back to something that works.
pub(crate) fn codex_client_version() -> String {
    let configured = std::env::var("KLAUS_CODEX_CLIENT_VERSION").unwrap_or_default();
    let configured = configured.trim();
    if looks_like_version(configured) {
        configured.to_string()
    } else {
        CODEX_CLIENT_VERSION.to_string()
    }
}

/// `major.minor.patch` with an optional `-`/`+` suffix of alphanumerics, dots and
/// dashes — the shape the Codex backend accepts.
fn looks_like_version(value: &str) -> bool {
    let mut parts = value.splitn(3, '.');
    let major = parts.next().unwrap_or_default();
    let minor = parts.next().unwrap_or_default();
    let patch = parts.next().unwrap_or_default();
    let digits = |part: &str| !part.is_empty() && part.bytes().all(|byte| byte.is_ascii_digit());
    let patch_digits = patch.bytes().take_while(u8::is_ascii_digit).count();
    let tail = &patch[patch_digits..];
    digits(major)
        && digits(minor)
        && patch_digits > 0
        && (tail.is_empty()
            || ((tail.starts_with('-') || tail.starts_with('+'))
                && tail[1..]
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || c == '.' || c == '-')))
}

/// The signed-in Codex account record, for callers that need the credential
/// without resolving a whole provider (the model catalog).
pub(crate) fn codex_account(config_root: &Path) -> Option<Map<String, Value>> {
    let raw = std::fs::read_to_string(codex_store_path(config_root)).ok()?;
    let store: Value = serde_json::from_str(&raw).ok()?;
    default_codex_account(&store).cloned()
}

/// A session's provider and auth, as the init carries them, together with the
/// model knowledge that has to ride with them.
///
/// The capability set is NOT optional. A provider reads exactly what the init
/// declared and infers nothing from a model id, so a resolver answering
/// provider+auth alone hands the session the MINIMAL set: no vision, no PDF, no
/// thinking, the provider's default window. That is how image input on a
/// subscription session came back as "this model cannot understand images"
/// before the model was ever asked.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Resolved {
    pub provider: Value,
    pub auth: Value,
    /// `capabilities` — what the main model can do.
    pub capabilities: Value,
    /// `capabilitiesByModel` — every admitted source model other than the main
    /// model, including models available only to internal work.
    pub capabilities_by_model: Vec<Value>,
    /// `config.modelTiers` — high/medium/low in concrete model ids.
    pub model_tiers: Map<String, Value>,
    /// `pricing` — rates for a subscription wire the engine has no table for.
    /// `None` leaves the engine's own defaults in place.
    pub pricing: Option<Value>,
}

fn merge_review_catalog_metadata(
    mut catalog: klaus_agent_core::types::provider::ModelCapabilities,
    declared: klaus_agent_core::types::provider::ModelCapabilities,
) -> klaus_agent_core::types::provider::ModelCapabilities {
    if !declared.supported_reasoning_efforts.is_empty() {
        catalog.supported_reasoning_efforts = declared.supported_reasoning_efforts;
    } else if let Some(effort) = &declared.effort {
        catalog.supported_reasoning_efforts = effort
            .levels
            .iter()
            .map(|level| level.as_str().into())
            .collect();
    }
    catalog.default_reasoning_effort = declared
        .default_reasoning_effort
        .or_else(|| {
            declared
                .effort
                .as_ref()
                .and_then(|effort| effort.default)
                .map(|level| level.as_str().into())
        })
        .or(catalog.default_reasoning_effort);
    catalog.multi_agent_reasoning_effort = declared
        .multi_agent_reasoning_effort
        .or(catalog.multi_agent_reasoning_effort);
    catalog.effective_context_window_percent = declared
        .effective_context_window_percent
        .or(catalog.effective_context_window_percent);
    catalog.used_fallback_model_metadata |= declared.used_fallback_model_metadata;
    catalog.model_specialty = declared.model_specialty.or(catalog.model_specialty);
    catalog.supports_image_detail_original |= declared.supports_image_detail_original;
    catalog.tools = declared.tools.or(catalog.tools);
    catalog.forced_tool_choice_with_thinking = declared
        .forced_tool_choice_with_thinking
        .or(catalog.forced_tool_choice_with_thinking);
    catalog.effort = declared.effort.or(catalog.effort);
    catalog.output_verbosity = declared.output_verbosity.or(catalog.output_verbosity);
    catalog.thinking = declared.thinking.or(catalog.thinking);
    catalog.context_window = declared.context_window.or(catalog.context_window);
    catalog.max_output_tokens = declared.max_output_tokens.or(catalog.max_output_tokens);
    catalog.image_limits = declared.image_limits.or(catalog.image_limits);
    catalog.max_media_blocks = declared.max_media_blocks.or(catalog.max_media_blocks);
    catalog.auto_review_model_override = declared
        .auto_review_model_override
        .or(catalog.auto_review_model_override);
    catalog.requires_sandboxed_review |= declared.requires_sandboxed_review;
    catalog.node_repl_auto_review_required |= declared.node_repl_auto_review_required;
    catalog.guardian = declared.guardian.or(catalog.guardian);
    catalog.guardian_v2 = declared.guardian_v2.or(catalog.guardian_v2);
    catalog.model_messages = declared.model_messages.or(catalog.model_messages);
    catalog.comp_hash = declared.comp_hash.or(catalog.comp_hash);
    catalog.auto_compact_token_limit = declared
        .auto_compact_token_limit
        .or(catalog.auto_compact_token_limit);
    catalog.supports_reasoning_summary_parameter = declared
        .supports_reasoning_summary_parameter
        .or(catalog.supports_reasoning_summary_parameter);
    catalog.default_reasoning_summary = declared
        .default_reasoning_summary
        .or(catalog.default_reasoning_summary);
    catalog.structured_output |= declared.structured_output;
    catalog.web_search |= declared.web_search;
    catalog.prompt_cache_control |= declared.prompt_cache_control;
    catalog.native_pdf |= declared.native_pdf;
    catalog.vision |= declared.vision;
    catalog.one_million_context |= declared.one_million_context;
    catalog.task_budgets |= declared.task_budgets;
    catalog.fast_mode |= declared.fast_mode;
    catalog
}

/// Bind the complete local subscription catalog to an authenticated provider.
/// Picker filtering is applied only by the public catalog response.
pub(crate) fn provider_decorator(
    config_root: &Path,
) -> klaus_agent_protocol::server::ProviderDecorator {
    use klaus_agent_core::runtime_values::{runtime_codec, RuntimeString};
    let root = config_root.to_path_buf();
    std::sync::Arc::new(move |provider, init| {
        if init
            .model_source
            .as_ref()
            .and_then(|v| v.get("source"))
            .and_then(|v| v.as_string())
            .is_some_and(|v| v == "kiro")
        {
            super::kiro::set_config_root(&root);
            return super::kiro::decorate(provider);
        }

        if init
            .model_source
            .as_ref()
            .and_then(|value| value.get("source"))
            .and_then(|value| value.as_string())
            .is_some_and(|source| source == "trae")
        {
            super::trae::set_config_root(&root);
            return super::trae::decorate(provider);
        }
        if init
            .model_source
            .as_ref()
            .and_then(|value| value.get("source"))
            .and_then(|value| value.as_string())
            .is_some_and(|source| source == "antigravity")
        {
            super::antigravity::set_config_root(&root);
            return super::antigravity::decorate(provider, init);
        }
        if provider.uses_official_api_catalog() {
            let declared = provider.clone();
            return klaus_agent_core::provider::review_target::with_review_model_catalog(
                provider,
                std::sync::Arc::new(move || {
                    let mut catalog: Vec<(
                        RuntimeString,
                        klaus_agent_core::types::provider::ModelCapabilities,
                    )> = super::codex_catalog::bundled_api_models()
                        .into_iter()
                        .filter_map(|entry| {
                            let id = entry.get("id").and_then(Value::as_str)?;
                            let value =
                                codex_capabilities(super::codex_catalog::limits_from_entry(&entry));
                            let metadata =
                                runtime_codec::decode_text(&RuntimeString::from(value.to_string()))
                                    .ok()?;
                            Some((RuntimeString::from(id), metadata))
                        })
                        .collect();
                    for (id, metadata) in declared.offline_model_catalog() {
                        if let Some((_, prior)) = catalog.iter_mut().find(|(model, _)| model == &id)
                        {
                            *prior = merge_review_catalog_metadata(prior.clone(), metadata);
                        } else {
                            catalog.push((id, metadata));
                        }
                    }
                    catalog
                }),
            );
        }
        let source = init
            .model_source
            .as_ref()
            .and_then(|value| value.get("source"))
            .and_then(|value| value.as_string());
        if source.is_none_or(|source| source != "codex") {
            return provider;
        }
        let Some(account_id) = codex_account(&root).and_then(|account| {
            account
                .get("accountId")
                .and_then(Value::as_str)
                .map(str::to_string)
        }) else {
            return provider;
        };
        let root = root.clone();
        klaus_agent_core::provider::review_target::with_review_model_catalog(
            provider,
            std::sync::Arc::new(move || {
                super::codex_catalog::all_models_offline(&root, &account_id)
                    .into_iter()
                    .filter_map(|entry| {
                        let id = entry.get("id").and_then(Value::as_str)?;
                        let value =
                            codex_capabilities(super::codex_catalog::limits_from_entry(&entry));
                        let metadata =
                            runtime_codec::decode_text(&RuntimeString::from(value.to_string()))
                                .ok()?;
                        Some((RuntimeString::from(id), metadata))
                    })
                    .collect()
            }),
        )
    })
}

/// The tier picks a host made, in the SOURCE's own id space: Claude aliases,
/// Codex catalog ids, Qoder model keys. Empty means "this source's own lineup".
///
/// Which model backs a tier is a user PREFERENCE, so it comes from whoever holds
/// the preferences; what that model can do is model knowledge, so it is answered
/// here. Splitting it the other way is what produced two capability tables.
pub type TierSelections = Map<String, Value>;

/// The OAuth client config a subscription login runs against.
///
/// Exposed because the login command needs the same values the resolver sends:
/// one definition, so a login cannot register against endpoints the session then
/// fails to renew a token at.
pub struct OAuthEndpoints {
    pub client_id: &'static str,
    pub authorize_url: &'static str,
    pub token_url: String,
    pub scopes: &'static [&'static str],
}

/// Claude's endpoints, for the subscription login.
pub fn claude_endpoints() -> OAuthEndpoints {
    OAuthEndpoints {
        client_id: CLAUDE_CLIENT_ID,
        authorize_url: CLAUDE_AUTHORIZE_URL,
        token_url: CLAUDE_TOKEN_URL.to_string(),
        scopes: CLAUDE_SCOPES,
    }
}

/// Whether a source has a usable credential, for a status report.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SignIn {
    /// Signed in, with the account label the store knows (when it knows one).
    Active { account: Option<String> },
    /// A credential is there but past its life and cannot be renewed.
    Expired,
    /// Nothing stored.
    Absent,
    /// A credential exists but this platform cannot read it back.
    Unreadable { reason: String },
}

/// The sign-in state of both subscription sources.
///
/// Reads the same records the resolver reads, so a status line cannot disagree
/// with what a session would actually do.
pub fn sign_in_status(config_root: &Path) -> Vec<(&'static str, SignIn)> {
    vec![
        ("claude", claude_sign_in(config_root)),
        ("codex", codex_sign_in(config_root)),
    ]
}

fn claude_sign_in(config_root: &Path) -> SignIn {
    let record = match credential_record(config_root) {
        Ok(record) => record,
        Err(error) if error.contains("DPAPI") => {
            return SignIn::Unreadable { reason: error };
        }
        Err(_) => return SignIn::Absent,
    };
    let Some(tokens) = record.get("claudeAiOauth").and_then(Value::as_object) else {
        return SignIn::Absent;
    };
    if tokens
        .get("accessToken")
        .and_then(Value::as_str)
        .is_none_or(str::is_empty)
    {
        return SignIn::Absent;
    }
    let renewable = tokens
        .get("refreshToken")
        .and_then(Value::as_str)
        .is_some_and(|token| !token.is_empty());
    match tokens.get("expiresAt").and_then(Value::as_i64) {
        Some(at) if at <= now_ms() && !renewable => SignIn::Expired,
        _ => SignIn::Active {
            account: tokens
                .get("account")
                .and_then(|account| account.get("email_address"))
                .and_then(Value::as_str)
                .map(str::to_string),
        },
    }
}

fn codex_sign_in(config_root: &Path) -> SignIn {
    let Ok(raw) = std::fs::read_to_string(codex_store_path(config_root)) else {
        return SignIn::Absent;
    };
    let Ok(store) = serde_json::from_str::<Value>(&raw) else {
        return SignIn::Absent;
    };
    let Some(account) = default_codex_account(&store) else {
        return SignIn::Absent;
    };
    if account
        .get("accessToken")
        .and_then(Value::as_str)
        .is_none_or(str::is_empty)
    {
        return SignIn::Absent;
    }
    let renewable = account
        .get("refreshToken")
        .and_then(Value::as_str)
        .is_some_and(|token| !token.is_empty());
    let label = account
        .get("email")
        .and_then(Value::as_str)
        .filter(|email| !email.is_empty())
        .or_else(|| account.get("accountId").and_then(Value::as_str))
        .map(str::to_string);
    match account.get("accessTokenExpiresAt").and_then(Value::as_i64) {
        Some(at) if at <= now_ms() && !renewable => SignIn::Expired,
        _ => SignIn::Active { account: label },
    }
}

/// Store a Claude subscription login as Klaus's own private copy.
///
/// Preserves whatever else the copy holds, so a refresh that rewrites the
/// tokens cannot drop account metadata written beside them. Once the copy
/// holds the login, the legacy file's login key is pruned: it is a migration
/// source, and a login key left in it would resurrect after a later logout.
pub fn write_claude_login(config_root: &Path, tokens: Value) -> Result<(), String> {
    let mut record = credential_record(config_root).unwrap_or_default();
    record.insert("claudeAiOauth".to_string(), tokens);
    write_private_json(
        &claude_subscription_path(config_root),
        &Value::Object(record),
    )?;
    let _ = prune_login_from_file(&legacy_plaintext_path(config_root));
    Ok(())
}

/// Forget a Claude subscription login from every store Klaus owns: the copy
/// and the legacy plaintext file an older build may still hold it in.
pub fn delete_claude_login(config_root: &Path) -> Result<bool, String> {
    let copy = prune_login_from_file(&claude_subscription_path(config_root))?;
    // A corrupted legacy file must not fail a logout of the copy.
    let legacy = prune_login_from_file(&legacy_plaintext_path(config_root)).unwrap_or(false);
    Ok(copy || legacy)
}

/// Remove the login keys from one credential file, deleting the file when
/// nothing else lives in it. Returns whether anything was removed.
fn prune_login_from_file(path: &Path) -> Result<bool, String> {
    let Some(mut record) = read_record_file(path)? else {
        return Ok(false);
    };
    let had = record.remove("claudeAiOauth").is_some() || record.remove("tokens").is_some();
    if !had {
        return Ok(false);
    }
    if record.is_empty() {
        std::fs::remove_file(path).map_err(|error| format!("{}: {error}", path.display()))?;
    } else {
        write_private_json(path, &Value::Object(record))?;
    }
    Ok(true)
}

static CODEX_LOGIN_WRITE_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

/// Store a Codex login as the store's default account.
pub fn write_codex_login(config_root: &Path, account: Value) -> Result<(), String> {
    let _guard = CODEX_LOGIN_WRITE_LOCK
        .lock()
        .unwrap_or_else(|p| p.into_inner());
    let path = codex_store_path(config_root);
    let mut store = std::fs::read_to_string(&path)
        .ok()
        .and_then(|raw| serde_json::from_str::<Value>(&raw).ok())
        .filter(Value::is_object)
        .unwrap_or_else(|| json!({}));
    let account_id = account
        .get("accountId")
        .and_then(Value::as_str)
        .unwrap_or("codex-account")
        .to_string();
    if !store["accounts"].is_object() {
        store["accounts"] = json!({});
    }
    store["accounts"][&account_id] = account;
    store["defaultAccountId"] = json!(account_id);
    write_private_json(&path, &store)
}

/// Replace only the still-selected grant that initiated a refresh.
pub(super) fn write_refreshed_codex_login(
    config_root: &Path,
    expected: &Map<String, Value>,
    renewed: Value,
) -> Result<(), String> {
    let _guard = CODEX_LOGIN_WRITE_LOCK
        .lock()
        .unwrap_or_else(|p| p.into_inner());
    let path = codex_store_path(config_root);
    let raw = std::fs::read_to_string(&path).map_err(|_| "codex credential changed")?;
    let mut store: Value = serde_json::from_str(&raw).map_err(|_| "codex credential changed")?;
    let selected = default_codex_account_key(&store).ok_or("codex credential changed")?;
    if store
        .get("accounts")
        .and_then(|accounts| accounts.get(&selected))
        .and_then(Value::as_object)
        != Some(expected)
    {
        return Err("codex credential changed".into());
    }
    store["accounts"][&selected] = renewed;
    write_private_json(&path, &store)
}

/// Forget the Codex login by removing its store.
pub fn delete_codex_login(config_root: &Path) -> Result<bool, String> {
    let _guard = CODEX_LOGIN_WRITE_LOCK
        .lock()
        .unwrap_or_else(|p| p.into_inner());
    let path = codex_store_path(config_root);
    if !path.exists() {
        return Ok(false);
    }
    std::fs::remove_file(&path).map_err(|error| format!("{}: {error}", path.display()))?;
    Ok(true)
}

pub fn codex_store_path(config_root: &Path) -> PathBuf {
    config_root.join("openai-subscription-oauth.json")
}

/// The Codex auth host, for the login flow's own endpoints.
pub fn codex_auth_base() -> String {
    codex_auth_host()
}

pub fn codex_client_id() -> &'static str {
    CODEX_CLIENT_ID
}

/// Resolve a subscription selection.
///
/// `Ok(None)` means the selection is not a subscription one and the BYO path
/// should read it. `Err` means it WAS a subscription selection that cannot be
/// honoured — never a silent hand-back, because falling through to a BYO model
/// would answer with a different account than the caller named.
pub async fn resolve(
    selection: &str,
    config_root: &Path,
    tiers: &TierSelections,
) -> Result<Option<Resolved>, String> {
    if selection.starts_with("kiro:") {
        super::kiro::set_config_root(config_root);
        return super::kiro::resolve(selection, tiers).await.map(Some);
    }
    if let Some(model) = selection.strip_prefix("trae:") {
        super::trae::set_config_root(config_root);
        return super::trae::resolve(model, tiers).await.map(Some);
    }
    if let Some(model) = selection.strip_prefix("antigravity:") {
        super::antigravity::set_config_root(config_root);
        return super::antigravity::resolve(model, tiers).await.map(Some);
    }
    if let Some(alias) = selection.strip_prefix("subscription:") {
        return claude(alias, config_root, tiers).await.map(Some);
    }
    if let Some(model) = selection.strip_prefix("codex:") {
        return codex(model, config_root, tiers).await.map(Some);
    }
    // Qoder speaks its own signed wire, so the endpoint runs the translator and
    // points an ordinary OpenAI chat provider at it. This used to be refused here,
    // because the signer and the bridge only existed in the desktop client.
    if let Some(key) = selection.strip_prefix("qoder:") {
        super::qoder::set_config_root(config_root);
        // A tier-shaped selection resolves through the host's picks, like the other
        // two sources; with no pick the catalog head names the model.
        let key = qoder_main_model(key, tiers);
        let (provider, auth) = super::qoder::resolve(&key).await?;
        let model = super::qoder::wire_model(&key);
        let (model_tiers, capabilities_by_model) = qoder_tiers(&model, tiers).await;
        return Ok(Some(Resolved {
            provider,
            auth,
            capabilities: super::qoder::model_capabilities(&model).await,
            capabilities_by_model,
            model_tiers,
            pricing: None,
        }));
    }
    if let Some(key) = selection.strip_prefix("qwenwork:") {
        super::qwenwork::set_config_root(config_root);
        let key = qwenwork_main_model(key, tiers);
        let model = super::qwenwork::resolved_model(&key).await;
        let (provider, auth) = super::qwenwork::resolve(&model).await?;
        let (model_tiers, capabilities_by_model) = qwenwork_tiers(&model, tiers).await;
        return Ok(Some(Resolved {
            provider,
            auth,
            capabilities: super::qwenwork::model_capabilities(&model).await,
            capabilities_by_model,
            model_tiers,
            pricing: None,
        }));
    }
    if let Some(key) = selection.strip_prefix("qoderwork:") {
        super::qoderwork::set_config_root(config_root);
        let key = qoderwork_main_model(key, tiers);
        let model = super::qoderwork::resolved_model(&key).await;
        let (provider, auth) = super::qoderwork::resolve(&model).await?;
        let (model_tiers, capabilities_by_model) = qoderwork_tiers(&model, tiers).await;
        return Ok(Some(Resolved {
            provider,
            auth,
            capabilities: super::qoderwork::model_capabilities(&model).await,
            capabilities_by_model,
            model_tiers,
            pricing: None,
        }));
    }
    if let Some(key) = selection.strip_prefix("cursor:") {
        super::cursor::set_config_root(config_root);
        let model = cursor_main_model(key, tiers).await?;
        let (provider, auth) = super::cursor::resolve(&model).await?;
        let (model_tiers, capabilities_by_model) = cursor_tiers(&model, tiers).await;
        return Ok(Some(Resolved {
            provider,
            auth,
            capabilities: super::cursor::model_capabilities(&model).await,
            capabilities_by_model,
            model_tiers,
            pricing: None,
        }));
    }
    // WorkBuddy's gateway takes a plain bearer and speaks OpenAI chat completions,
    // so no bridge and no signer sit between the session and it — the provider is
    // pointed straight at the gateway.
    if let Some(key) = selection.strip_prefix("workbuddy:") {
        super::workbuddy::set_config_root(config_root);
        let key = workbuddy_main_model(key, tiers);
        let model = super::workbuddy::resolved_model(&key).await;
        let (provider, auth) = super::workbuddy::resolve(&model).await?;
        let (model_tiers, capabilities_by_model) = workbuddy_tiers(&model, tiers).await;
        return Ok(Some(Resolved {
            provider,
            auth,
            capabilities: super::workbuddy::model_capabilities(&model).await,
            capabilities_by_model,
            model_tiers,
            pricing: None,
        }));
    }
    // Kimi's coding gateway is the ordinary Anthropic wire with a plain
    // credential — an API key or a device-flow bearer — so like WorkBuddy there is
    // no signer and no bridge between the session and it. The provider is pointed
    // straight at the gateway, and the effort ladder comes from Kimi's own catalog.
    if let Some(key) = selection.strip_prefix("kimi:") {
        super::kimi::set_config_root(config_root);
        let key = kimi_main_model(key, tiers);
        let model = super::kimi::resolved_model(&key).await;
        let (provider, auth) = super::kimi::resolve(&model).await?;
        let (model_tiers, capabilities_by_model) = kimi_tiers(&model, tiers).await;
        return Ok(Some(Resolved {
            provider,
            auth,
            capabilities: super::kimi::model_capabilities(&model).await,
            capabilities_by_model,
            model_tiers,
            pricing: None,
        }));
    }
    // Bailian Token Plan serves the same plain-credential Anthropic wire as Kimi,
    // so the provider is likewise pointed straight at the gateway. The effort
    // ladder here is per model rather than uniform: the platform's own routing
    // model and the qwen family accept all five rungs, while glm-5.3 answers 400 to
    // medium and xhigh, so what a session may ask for is declared from the
    // measured table and not from the source.
    if let Some(key) = selection.strip_prefix("bailian:") {
        super::bailian::set_config_root(config_root);
        let key = bailian_main_model(key, tiers);
        let model = super::bailian::resolved_model(&key).await;
        let (provider, auth) = super::bailian::resolve(&model).await?;
        let (model_tiers, capabilities_by_model) = bailian_tiers(&model, tiers).await;
        return Ok(Some(Resolved {
            provider,
            auth,
            capabilities: super::bailian::model_capabilities(&model).await,
            capabilities_by_model,
            model_tiers,
            pricing: None,
        }));
    }
    // MiniMax M Plan serves the same plain-credential Anthropic wire, so the
    // provider is likewise pointed straight at the gateway. Its low tier needs the
    // same pinning Bailian's does, for a sharper reason: the plan's flagship model
    // refuses `thinking:{"type":"disabled"}` outright and the other frontier model
    // accepts it but then emits no tool call, so neither can serve a structured
    // request. Session titles are one, on the low tier.
    if let Some(key) = selection.strip_prefix("minimax:") {
        super::minimax::set_config_root(config_root);
        let key = minimax_main_model(key, tiers);
        let model = super::minimax::resolved_model(&key).await;
        let (provider, auth) = super::minimax::resolve(&model).await?;
        let (model_tiers, capabilities_by_model) = minimax_tiers(&model, tiers).await;
        return Ok(Some(Resolved {
            provider,
            auth,
            capabilities: super::minimax::model_capabilities(&model).await,
            capabilities_by_model,
            model_tiers,
            pricing: None,
        }));
    }
    // Zhipu Coding Plan serves the same plain-credential Anthropic wire as Kimi
    // and Bailian, so the provider is likewise pointed straight at the gateway.
    // Two measured facts set this branch apart from the Bailian one: the plan
    // honors all five effort rungs on every model it serves, and every model
    // accepts the disabled-thinking shape the forced-tool path sends (none of them
    // truly silence thinking — the plan maps that to its low rung — but none
    // reject it either), so there is no structured-output fallback to pin here.
    if let Some(key) = selection.strip_prefix("zhipu:") {
        super::zhipu::set_config_root(config_root);
        let key = zhipu_main_model(key, tiers);
        let model = super::zhipu::resolved_model(&key).await;
        let (provider, auth) = super::zhipu::resolve(&model).await?;
        let (model_tiers, capabilities_by_model) = zhipu_tiers(&model, tiers).await;
        return Ok(Some(Resolved {
            provider,
            auth,
            capabilities: super::zhipu::model_capabilities(&model).await,
            capabilities_by_model,
            model_tiers,
            pricing: None,
        }));
    }
    // Ark Coding Plan serves the same plain-credential Anthropic wire as the other
    // plan sources, so the provider is pointed straight at the gateway. What sets
    // it apart is measured: the gateway answers an unknown key with a real 401 and
    // an OpenAI-shaped error body (unlike a gateway that hides refusals inside a
    // 200), and the plan publishes its remaining allowance to the plan key, so the
    // usage read projects real windows.
    if let Some(key) = selection.strip_prefix("ark:") {
        super::ark::set_config_root(config_root);
        let key = ark_main_model(key, tiers);
        let model = super::ark::resolved_model(&key).await;
        let (provider, auth) = super::ark::resolve(&model).await?;
        let (model_tiers, capabilities_by_model) = ark_tiers(&model, tiers).await;
        return Ok(Some(Resolved {
            provider,
            auth,
            capabilities: super::ark::model_capabilities(&model).await,
            capabilities_by_model,
            model_tiers,
            pricing: None,
        }));
    }
    // Named, understood, and not available here: the local model server is a host
    // capability this endpoint does not carry. Saying so beats resolving to
    // something the caller did not ask for.
    if selection.starts_with("local:") {
        return Err(format!(
            "cannot run {selection}: on-device models need the local model server, which only the desktop client runs"
        ));
    }
    Ok(None)
}

pub(super) fn oauth_credential_id(source: &str, login_generation: &str) -> String {
    use sha2::{Digest, Sha256};
    // A new login gets a new namespace; subsequent target reconstruction keeps
    // the refreshed tokens instead of overwriting them with the login copy.
    format!("{source}-{:x}", Sha256::digest(login_generation.as_bytes()))
}

/// The credential namespace of one stored account row. Keyed on the row's
/// login-time refresh token, exactly like the historical single-account
/// namespace: a refresh rotation rewrites the stored copy inside that
/// namespace without changing the file, so the id stays; a re-login replaces
/// the row's tokens and therefore lands in a fresh namespace seeded by the
/// new login. Account rotation moves between accounts, each with its own id.
pub(super) fn account_credential_id(source: &str, tokens: &Map<String, Value>) -> String {
    let generation = tokens
        .get("refreshToken")
        .or_else(|| tokens.get("accessToken"))
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .unwrap_or_default();
    oauth_credential_id(source, generation)
}

pub(super) fn claude_oauth_config() -> klaus_agent_core::provider::oauth::OAuthConfig {
    klaus_agent_core::provider::oauth::OAuthConfig {
        client_id: CLAUDE_CLIENT_ID.to_string(),
        authorize_url: CLAUDE_AUTHORIZE_URL.to_string(),
        token_url: CLAUDE_TOKEN_URL.to_string(),
        redirect_uri: CLAUDE_REDIRECT_URL.to_string(),
        scopes: Some(
            CLAUDE_SCOPES
                .iter()
                .map(|scope| (*scope).to_string())
                .collect(),
        ),
        ..Default::default()
    }
}

/// The stored accounts of the Claude subscription credential file, head of the
/// rotation queue first. The file's `accounts` array is authoritative; a file
/// that still carries the historical single `claudeAiOauth` key reads as one
/// legacy account without being rewritten. Accounts without an access token
/// are skipped rather than failing the whole source.
pub(super) fn claude_accounts(config_root: &Path) -> Result<Vec<Value>, String> {
    let record = match credential_record(config_root) {
        Ok(record) => record,
        Err(error) if error == not_signed_in() => return Ok(Vec::new()),
        Err(error) => return Err(error),
    };
    if let Some(accounts) = record.get("accounts").and_then(Value::as_array) {
        let usable: Vec<Value> = accounts
            .iter()
            .filter(|entry| {
                entry
                    .get("id")
                    .and_then(Value::as_str)
                    .is_some_and(|id| !id.is_empty())
                    && entry
                        .pointer("/tokens/accessToken")
                        .and_then(Value::as_str)
                        .is_some_and(|token| !token.is_empty())
            })
            .cloned()
            .collect();
        return Ok(usable);
    }
    let tokens = record.get("claudeAiOauth").and_then(Value::as_object);
    match tokens {
        Some(tokens) if tokens.get("accessToken").and_then(Value::as_str).is_some() => Ok(vec![
            serde_json::json!({ "id": "legacy", "tokens": Value::Object(tokens.clone()) }),
        ]),
        _ => Ok(Vec::new()),
    }
}

/// The Claude subscription provider: the ordinary Anthropic wire with a bearer
/// token from the subscription login instead of an API key. When the login
/// carries more than one account, the stored account order is the rotation
/// queue and travels with the auth so the engine can advance it on quota
/// refusals.
async fn claude(
    alias: &str,
    config_root: &Path,
    tiers: &TierSelections,
) -> Result<Resolved, String> {
    let accounts = claude_accounts(config_root)?;
    let head = accounts.first().ok_or_else(|| {
        "not signed in to a Claude subscription — run `klaus login claude`".to_string()
    })?;
    let account_id = head
        .get("id")
        .and_then(Value::as_str)
        .ok_or_else(|| "a stored Claude account is missing its id".to_string())?;
    let tokens = head
        .get("tokens")
        .and_then(Value::as_object)
        .ok_or_else(|| {
            "the stored Claude credential has no tokens — run `klaus login claude`".to_string()
        })?;
    let access = tokens
        .get("accessToken")
        .and_then(Value::as_str)
        .filter(|token| !token.is_empty())
        .ok_or_else(|| {
            "the stored Claude credential has no access token — run `klaus login claude`"
                .to_string()
        })?;
    let refresh = tokens
        .get("refreshToken")
        .and_then(Value::as_str)
        .filter(|token| !token.is_empty());
    let expires_at = tokens.get("expiresAt").and_then(Value::as_i64);

    // An access token already past its life cannot be repaired here: the engine
    // refreshes with a form-encoded body and this endpoint answers JSON, so the
    // honest answer is to send the user back to a login rather than let the first
    // request of the turn fail with someone else's error message.
    if let Some(expires_at) = expires_at {
        if expires_at <= now_ms() && refresh.is_none() {
            return Err(
                "the Claude subscription credential has expired and cannot be renewed — run `klaus login claude`"
                    .to_string(),
            );
        }
    }

    let mut auth = Map::new();
    auth.insert("type".into(), json!("oauth_token"));
    auth.insert("accessToken".into(), json!(access));
    if let Some(refresh) = refresh {
        auth.insert("refreshToken".into(), json!(refresh));
    }
    if let Some(expires_at) = expires_at {
        auth.insert("expiresAt".into(), json!(expires_at));
    }
    auth.insert("accountId".into(), json!(account_id));
    auth.insert(
        "credentialId".into(),
        json!(account_credential_id("subscription", tokens)),
    );
    // Nested, not flat: the wire reads the OAuth client config from `oauth`, so
    // the same keys spread across the auth object would decode as absent and the
    // engine would hold a token it cannot renew.
    auth.insert(
        "oauth".into(),
        json!({
            "clientId": CLAUDE_CLIENT_ID,
            "authorizeUrl": CLAUDE_AUTHORIZE_URL,
            "tokenUrl": CLAUDE_TOKEN_URL,
            "redirectUri": CLAUDE_REDIRECT_URL,
            "scopes": CLAUDE_SCOPES,
        }),
    );
    if accounts.len() > 1 {
        let rotation_accounts: Vec<Value> = accounts
            .iter()
            .filter_map(|entry| {
                let id = entry.get("id").and_then(Value::as_str)?;
                let tokens = entry.get("tokens").and_then(Value::as_object)?;
                let access = tokens.get("accessToken").and_then(Value::as_str)?;
                let mut account = Map::new();
                account.insert("id".into(), json!(id));
                account.insert(
                    "credentialId".into(),
                    json!(account_credential_id("subscription", tokens)),
                );
                account.insert("accessToken".into(), json!(access));
                if let Some(refresh) = tokens.get("refreshToken").and_then(Value::as_str) {
                    account.insert("refreshToken".into(), json!(refresh));
                }
                if let Some(expires_at) = tokens.get("expiresAt").and_then(Value::as_i64) {
                    account.insert("expiresAt".into(), json!(expires_at));
                }
                Some(Value::Object(account))
            })
            .collect();
        auth.insert(
            "rotation".into(),
            json!({
                "filePath": claude_subscription_path(config_root).to_string_lossy(),
                "accounts": rotation_accounts,
            }),
        );
    }

    let alias = super::model_selection::subscription_choice(alias);
    let model = claude_model(claude_main_alias(alias, tiers));
    let (model_tiers, capabilities_by_model) = claude_tiers(&model, tiers);
    Ok(Resolved {
        provider: json!({ "type": "anthropic", "model": model.clone() }),
        auth: Value::Object(auth),
        capabilities: claude_capabilities(&model),
        capabilities_by_model,
        model_tiers,
        pricing: None,
    })
}

/// What a current-generation Claude subscription model can do.
///
/// Keyed on the CONCRETE id, not on the alias that selected it: a host may name
/// `subscription:opus`, a bare tier word, or the id itself, and all three have to
/// land on the same set. The current Fable, Opus, Sonnet and Haiku models reason
/// with adaptive thinking and carry the full effort ladder including `xhigh`.
/// Every current model supports task budgets, images, PDFs and request-side
/// cache breakpoints.
///
/// Windows and output ceilings are the published per-model limits (1M context /
/// 128K output for the current lineup). The output
/// ceiling matters because it is what a request may ask for at most — undeclared,
/// the provider default (8K) stands in and a long answer is cut short.
///
/// One documented difference this set does NOT model: Opus 5.5 and the Fable
/// models reject `thinking: {"type": "disabled"}`. The engine only ever sends that
/// shape on a non-first-party wire (see `resolve_wire_thinking`), so the
/// subscription path never trips it; a capability flag would be dead weight until
/// something actually needs to disable thinking here.
fn claude_capabilities(model: &str) -> Value {
    let base = |effort_levels: &[&str],
                one_million: bool,
                task_budgets: bool,
                context_window: i64,
                max_output_tokens: i64|
     -> Value {
        let mut caps = json!({
            "structuredOutput": true,
            "webSearch": true,
            "promptCacheControl": true,
            "nativePdf": true,
            "vision": true,
            "oneMillionContext": one_million,
            "taskBudgets": task_budgets,
            "contextWindow": context_window,
            "maxOutputTokens": max_output_tokens,
            "thinking": {
                "adaptive": !effort_levels.is_empty(),
                "interleaved": true,
                "canDisableThinking": true,
            },
        });
        if !effort_levels.is_empty() {
            caps["effort"] = json!({ "levels": effort_levels });
        }
        caps
    };
    let mut caps = match model {
        "claude-sonnet-5" => base(
            &["low", "medium", "high", "xhigh", "max"],
            true,
            false,
            1_000_000,
            128_000,
        ),
        "claude-haiku-4-5" => base(&[], false, false, 200_000, 64_000),
        // Known current models share this measured capability set.
        CLAUDE_FABLE | CLAUDE_OPUS | CLAUDE_SONNET | CLAUDE_HAIKU | "claude-fable-5-1"
        | "claude-opus-5" | "claude-opus-4-8" => base(
            &["low", "medium", "high", "xhigh", "max"],
            true,
            true,
            1_000_000,
            128_000,
        ),
        // A newly discovered id must not inherit another model's reasoning ladder.
        _ => base(&[], true, true, 1_000_000, 128_000),
    };
    caps["fastMode"] = json!(matches!(
        model,
        "claude-fable-5-2" | "claude-opus-5-5" | "claude-opus-5" | "claude-opus-4-8"
    ));
    caps["imageLimits"] = json!({
        "maxWidth": 2000,
        "maxHeight": 2000,
        "maxBase64Size": 5_242_880,
    });
    // Only the source's known extended-context models advertise the larger
    // media count. An unknown id keeps a conservative request-view budget.
    caps["maxMediaBlocks"] = json!(match model {
        CLAUDE_FABLE
        | CLAUDE_OPUS
        | CLAUDE_SONNET
        | CLAUDE_HAIKU
        | "claude-fable-5-1"
        | "claude-sonnet-5" => 600,
        _ => 100,
    });
    caps
}

/// A selection that names a TIER resolves through the host's picks: the settings
/// page decides which model backs low/medium/high, and the composer's rows for
/// this source are those same tiers — so picking one has to land on the remapped
/// model, not on the lineup's default. An explicit alias or id stands as itself.
fn claude_main_alias<'a>(alias: &'a str, selections: &'a TierSelections) -> &'a str {
    let alias = alias.strip_suffix("[1m]").unwrap_or(alias);
    let tier = match alias {
        "opus" | "high" => "high",
        "sonnet" | "medium" => "medium",
        "haiku" | "low" => "low",
        _ => return alias,
    };
    tier_pick(selections, tier).unwrap_or(alias)
}

/// The Claude tier map: the host's picks over the opus/sonnet/haiku lineup,
/// resolved to concrete ids so an engine tier slot never carries an alias.
fn claude_tiers(main_model: &str, selections: &TierSelections) -> (Map<String, Value>, Vec<Value>) {
    let mut tiers = Map::new();
    let mut by_model = Vec::new();
    let mut seen = vec![main_model.to_string()];
    for (tier, default_alias) in [("high", "opus"), ("medium", "sonnet"), ("low", "haiku")] {
        let model = claude_model(tier_pick(selections, tier).unwrap_or(default_alias));
        tiers.insert(tier.to_string(), json!(model.clone()));
        if seen.contains(&model) {
            continue;
        }
        by_model.push(json!({
            "model": model.clone(),
            "capabilities": claude_capabilities(&model),
        }));
        seen.push(model);
    }
    (tiers, by_model)
}

/// One tier's host pick; blank means "follow this source's own lineup".
fn tier_pick<'a>(selections: &'a TierSelections, tier: &str) -> Option<&'a str> {
    selections
        .get(tier)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|pick| !pick.is_empty())
}

/// The model an alias names.
///
/// The four aliases are the subscription's own vocabulary. `fable` has no tier
/// word beside it because it backs no tier by default (see [`CLAUDE_FABLE`]).
/// Anything else is passed through as a model id: a caller who names a specific
/// model gets that model, rather than being quietly moved onto the default.
fn claude_model(alias: &str) -> String {
    // Claude Code's context-window suffix is not part of the provider model ID.
    let alias = alias.strip_suffix("[1m]").unwrap_or(alias);
    match alias {
        "fable" => CLAUDE_FABLE.to_string(),
        "opus" | "high" => CLAUDE_OPUS.to_string(),
        "sonnet" | "medium" => CLAUDE_SONNET.to_string(),
        "haiku" | "low" => CLAUDE_HAIKU.to_string(),
        "" | "auto" => CLAUDE_OPUS.to_string(),
        other => other.to_string(),
    }
}

/// The Claude subscription lineup, for a model picker that has no session bound.
///
/// This exists because the host used to carry its own copy of the lineup: a
/// hardcoded three-alias table with a hardcoded effort ladder beside it. That
/// made two declarations of one fact, and they drifted exactly as such pairs do —
/// the engine declared `xhigh` for the Opus and Sonnet families while the host's
/// table stopped at `max`, so a level the session would have accepted could not be
/// picked. The Codex and Qoder sources never had that problem because their
/// catalogs already came from here.
///
/// `effortLevels` is read off [`claude_capabilities`] rather than restated, so the
/// picker's ladder and the session's declared capability set cannot disagree. An
/// empty list means a model has no effort knob; `thinking` says whether it reasons
/// regardless, which lets a host tell "no ladder" apart from "no reasoning".
///
/// `tier` is the slot this alias backs by default, or absent for a model that
/// backs none (Fable 5.2 — see [`CLAUDE_FABLE`]). No credential is read: the lineup
/// is static product knowledge, so an expired login still gets a populated picker
/// instead of a blank one.
pub fn claude_lineup() -> Value {
    json!({
        "models": [
            claude_lineup_entry("fable", "Fable 5.2", CLAUDE_FABLE, None),
            claude_lineup_entry("opus", "Opus 5.5", CLAUDE_OPUS, Some("high")),
            claude_lineup_entry("sonnet", "Sonnet 5.5", CLAUDE_SONNET, Some("medium")),
            claude_lineup_entry("haiku", "Haiku 5.5", CLAUDE_HAIKU, Some("low")),
        ]
    })
}

/// A target may differ from its provider ID only through a declared source alias.
pub(super) fn model_target_matches_provider(
    source: &str,
    model_id: &str,
    provider_model: &str,
) -> bool {
    model_id == provider_model
        || (source == "subscription"
            && claude_lineup()["models"].as_array().is_some_and(|models| {
                models.iter().any(|model| {
                    model["alias"].as_str() == Some(model_id)
                        && model["model"].as_str() == Some(provider_model)
                })
            }))
}

fn claude_lineup_entry(alias: &str, label: &str, model: &str, tier: Option<&str>) -> Value {
    let caps = claude_capabilities(model);
    let mut entry = json!({
        "alias": alias,
        "label": label,
        "model": model,
        "fastMode": caps["fastMode"],
        "fastBilling": "extraCredits",
        "effortLevels": caps
            .get("effort")
            .and_then(|effort| effort.get("levels"))
            .cloned()
            .unwrap_or_else(|| json!([])),
        "thinking": caps
            .get("thinking")
            .map(|thinking| !thinking.is_null())
            .unwrap_or(false),
    });
    if let Some(tier) = tier {
        entry["tier"] = json!(tier);
    }
    entry
}

/// Prefer the priced mid model over the catalog head: the head is the
/// flagship, and a default that names it re-prices every unpicked
/// conversation at five times Sol for a gain nobody asked for. An older
/// lineup keeps the previous Sol; one without either uses its head.
fn codex_catalog_default_model(models: &[Value]) -> Option<&str> {
    let ids = || {
        models.iter().filter_map(|model| {
            model
                .get("id")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|id| !id.is_empty())
        })
    };
    ids()
        .find(|id| id.eq_ignore_ascii_case(CODEX_DEFAULT_MAIN_MODEL))
        .or_else(|| ids().find(|id| id.eq_ignore_ascii_case(CODEX_PREVIOUS_MAIN_MODEL)))
        .or_else(|| ids().next())
}

/// Pick the catalog's preferred default model. The static fallback is only
/// for an unreachable catalog, where the account's latest lineup is unknown.
async fn codex_default_model(config_root: &Path) -> String {
    match super::codex_catalog::models(config_root).await {
        Ok(models) => codex_catalog_default_model(&models)
            .unwrap_or(CODEX_FALLBACK_MODEL)
            .to_string(),
        Err(_) => CODEX_FALLBACK_MODEL.to_string(),
    }
}

/// One default/tier decision for both persisted picker choices and providers.
pub(super) async fn codex_main_model(
    model: &str,
    config_root: &Path,
    tiers: &TierSelections,
) -> String {
    let guess = match model {
        "" | "auto" | "high" | "medium" | "low" => codex_default_model(config_root).await,
        other => other.to_string(),
    };
    match model {
        tier @ ("high" | "medium" | "low") => codex_tiers(&guess, tiers, config_root)
            .await
            .0
            .get(tier)
            .and_then(Value::as_str)
            .unwrap_or(&guess)
            .to_string(),
        _ => guess,
    }
}

/// An unmapped tier or a bare default names the catalog head, matching the
/// picker, which offers no synthetic automatic route.
pub(super) fn qoder_main_model<'a>(model: &'a str, tiers: &'a TierSelections) -> String {
    let pick = match model {
        tier @ ("high" | "medium" | "low") => tier_pick(tiers, tier).unwrap_or("auto"),
        other => other,
    };
    super::qoder::wire_model(pick)
}

pub(super) fn qwenwork_main_model<'a>(model: &'a str, tiers: &'a TierSelections) -> String {
    let pick = match model {
        tier @ ("high" | "medium" | "low") => tier_pick(tiers, tier).unwrap_or("auto"),
        other => other,
    };
    super::qwenwork::wire_model(pick)
}

pub(super) fn qoderwork_main_model<'a>(model: &'a str, tiers: &'a TierSelections) -> String {
    let pick = match model {
        tier @ ("high" | "medium" | "low") => tier_pick(tiers, tier).unwrap_or("auto"),
        other => other,
    };
    super::qoderwork::wire_model(pick)
}

pub(super) async fn cursor_main_model(
    model: &str,
    tiers: &TierSelections,
) -> Result<String, String> {
    let pick = match model {
        tier @ ("high" | "medium" | "low") => tier_pick(tiers, tier).unwrap_or(""),
        other => other,
    };
    super::cursor::resolved_model(pick.strip_prefix("cursor:").unwrap_or(pick)).await
}

pub(super) fn workbuddy_main_model<'a>(model: &'a str, tiers: &'a TierSelections) -> String {
    let pick = match model {
        tier @ ("high" | "medium" | "low") => tier_pick(tiers, tier).unwrap_or("auto"),
        other => other,
    };
    pick.to_string()
}

pub(super) fn kimi_main_model<'a>(model: &'a str, tiers: &'a TierSelections) -> String {
    let pick = match model {
        tier @ ("high" | "medium" | "low") => tier_pick(tiers, tier).unwrap_or("auto"),
        other => other,
    };
    super::kimi::wire_model(pick)
}

pub(super) fn bailian_main_model<'a>(model: &'a str, tiers: &'a TierSelections) -> String {
    let pick = match model {
        // An UNMAPPED tier must not name `auto`, unlike every other source: Bailian
        // has a real model by that name and it is excluded from the catalog because
        // it refuses the `tool_choice` parameter structured requests need. An empty
        // pick resolves to the catalog head instead, which is a model measured to
        // serve that path. This matters because the low tier is what session titles
        // run on, and a title call on `auto` fails with a provider 400.
        tier @ ("high" | "medium" | "low") => tier_pick(tiers, tier).unwrap_or(""),
        other => other,
    };
    super::bailian::wire_model(pick)
}

pub(super) fn minimax_main_model<'a>(model: &'a str, tiers: &'a TierSelections) -> String {
    let pick = match model {
        // An unmapped tier resolves to the plan's flagship rather than to a model
        // that can serve a structured request. That is deliberate and is the reason
        // `minimax_tiers` pins the LOW tier separately: the flagship is the right
        // main model and the wrong title model.
        tier @ ("high" | "medium" | "low") => tier_pick(tiers, tier).unwrap_or(""),
        other => other,
    };
    super::minimax::wire_model(pick)
}

pub(super) fn zhipu_main_model<'a>(model: &'a str, tiers: &'a TierSelections) -> String {
    let pick = match model {
        // An unmapped tier resolves to the catalog head, which is the plan's top
        // model. Session titles on an unmapped low tier therefore follow it, and
        // that is safe here: every model this plan serves accepted the forced-tool
        // structured body a title request sends, so no cheaper pin is needed.
        tier @ ("high" | "medium" | "low") => tier_pick(tiers, tier).unwrap_or(""),
        other => other,
    };
    super::zhipu::wire_model(pick)
}

pub(super) fn ark_main_model<'a>(model: &'a str, tiers: &'a TierSelections) -> String {
    let pick = match model {
        tier @ ("high" | "medium" | "low") => tier_pick(tiers, tier).unwrap_or(""),
        other => other,
    };
    super::ark::wire_model(pick)
}

/// The ChatGPT/Codex subscription provider: the OpenAI responses wire pointed at
/// the Codex backend, with the account id the service buckets requests by.
async fn codex(
    model: &str,
    config_root: &Path,
    tiers: &TierSelections,
) -> Result<Resolved, String> {
    let path = codex_store_path(config_root);
    let raw = std::fs::read_to_string(&path).map_err(|_| {
        format!(
            "not signed in to a ChatGPT/Codex subscription ({} is missing) — run `klaus login codex`",
            path.display()
        )
    })?;
    let store: Value = serde_json::from_str(&raw)
        .map_err(|error| format!("{} is not readable JSON: {error}", path.display()))?;
    let account = default_codex_account(&store).ok_or_else(|| {
        format!(
            "{} holds no signed-in account — run `klaus login codex`",
            path.display()
        )
    })?;
    let access = account
        .get("accessToken")
        .and_then(Value::as_str)
        .filter(|token| !token.is_empty())
        .ok_or_else(|| {
            "the stored Codex credential has no access token — run `klaus login codex`".to_string()
        })?;
    let expires_at = account.get("accessTokenExpiresAt").and_then(Value::as_i64);
    let refresh = account
        .get("refreshToken")
        .and_then(Value::as_str)
        .filter(|token| !token.is_empty());
    if expires_at.is_some_and(|at| at - REFRESH_BUFFER_MS <= now_ms()) && refresh.is_none() {
        return Err(
            "the Codex subscription credential has expired and cannot be renewed — run `klaus login codex`"
                .to_string(),
        );
    }

    let mut headers: BTreeMap<String, Value> = BTreeMap::new();
    if let Some(account_id) = account
        .get("accountId")
        .and_then(Value::as_str)
        .filter(|id| !id.is_empty())
    {
        headers.insert("ChatGPT-Account-Id".into(), json!(account_id));
    }
    headers.insert("version".into(), json!(codex_client_version()));

    let mut auth = Map::new();
    auth.insert("type".into(), json!("oauth_token"));
    auth.insert("accessToken".into(), json!(access));
    if let Some(id_token) = ["idToken", "id_token"].iter().find_map(|key| {
        account
            .get(*key)
            .and_then(Value::as_str)
            .filter(|token| !token.is_empty())
    }) {
        auth.insert("idToken".into(), json!(id_token));
    }
    if let Some(refresh) = refresh {
        auth.insert("refreshToken".into(), json!(refresh));
    }
    if let Some(expires_at) = expires_at {
        auth.insert("expiresAt".into(), json!(expires_at));
    }
    // All four fields or none: the wire rejects a partial OAuth client config, and
    // the engine needs `clientId` + `tokenUrl` to renew the token itself rather
    // than failing the turn on a bearer that lapsed mid-session. Codex renews with
    // a form-encoded refresh grant, which is exactly what the engine sends.
    let auth_host = codex_auth_host();
    auth.insert(
        "oauth".into(),
        json!({
            "clientId": CODEX_CLIENT_ID,
            "authorizeUrl": format!("{auth_host}/codex/device"),
            "tokenUrl": format!("{auth_host}/oauth/token"),
            "redirectUri": format!("{auth_host}/deviceauth/callback"),
        }),
    );
    auth.insert(
        "extraHeaders".into(),
        Value::Object(headers.into_iter().collect()),
    );

    // A tier-shaped selection resolves through the tier map the picks produce, so
    // remapping a tier in settings moves the main model too. The map is then
    // recomputed against the model that won, so the capability dedupe measures
    // against the real main instead of the guess.
    let model = codex_main_model(model, config_root, tiers).await;
    let model = model.as_str();
    let (model_tiers, mut capabilities_by_model) = codex_tiers(model, tiers, config_root).await;
    // Internal model selection sees the full source catalog, including targets
    // intentionally omitted from the ordinary model picker.
    if let Ok(catalog) = super::codex_catalog::all_models(config_root).await {
        for entry in catalog {
            let Some(id) = entry.get("id").and_then(Value::as_str) else {
                continue;
            };
            if id == model
                || capabilities_by_model
                    .iter()
                    .any(|entry| entry.get("model").and_then(Value::as_str) == Some(id))
            {
                continue;
            }
            capabilities_by_model.push(json!({"model":id,"capabilities":codex_capabilities(super::codex_catalog::limits_from_entry(&entry))}));
        }
    }
    Ok(Resolved {
        provider: json!({
            "type": "openai",
            "api": "responses",
            "model": model,
            "baseUrl": CODEX_BASE_URL,
        }),
        auth: Value::Object(auth),
        capabilities: codex_capabilities(super::codex_catalog::limits(config_root, model).await),
        capabilities_by_model,
        model_tiers,
        pricing: codex_pricing(model),
    })
}

/// What a Codex subscription model can do: a reasoning model on the Responses
/// surface, so the effort ladder and the OpenAI reasoning dialect are declared
/// here instead of being guessed from the id. Native PDF, hosted web search and
/// native structured output ride that surface; every current-generation Codex
/// model accepts image input.
///
/// The ladder is the one THIS model advertises, not a constant. Every rung on the
/// declaration is a rung a request may spell, so a hardcoded five-rung table
/// overstates the narrow models: `gpt-5.5` advertises low…xhigh, and declaring
/// `max` for it let a turn ask for a rung the service never offered. Understating
/// is the safer error here — an undeclared rung is clamped away at the request
/// boundary rather than rejected by the service.
///
/// Missing or empty effort metadata leaves the native effort control undeclared.
/// An unavailable catalog cannot establish that a particular model accepts a rung.
///
/// Window and output ceiling come from the catalog. Absent means the catalog could
/// not be read, and the provider default applies rather than a guess.
pub(crate) fn codex_capabilities(limits: super::codex_catalog::ModelLimits) -> Value {
    let levels: Vec<Value> = match limits.effort_levels.as_ref() {
        None => Vec::new(),
        Some(advertised) if advertised.is_empty() => Vec::new(),
        Some(advertised) => advertised.iter().map(|rung| json!(rung)).collect(),
    };
    let mut capabilities = json!({
        "structuredOutput": true,
        "webSearch": true,
        "nativePdf": true,
        "vision": true,
        "reasoningDialect": "openai",
    });
    // An empty ladder declares NO effort knob, which is a different statement
    // from "the full ladder" — omit the key entirely so the provider reads it as
    // absent capability rather than an empty list.
    if !levels.is_empty() {
        capabilities["effort"] = json!({ "levels": levels });
        if let Some(default) = limits.effort_default {
            capabilities["effort"]["default"] =
                serde_json::to_value(default).expect("effort enum serializes");
        }
    }
    if let Some(window) = limits.context_window {
        capabilities["contextWindow"] = json!(window);
    }
    if let Some(output) = limits.max_output_tokens {
        capabilities["maxOutputTokens"] = json!(output);
    }
    if let Some(verbosity) = limits.output_verbosity {
        capabilities["outputVerbosity"] = json!(verbosity.as_str());
    }
    if let Some(value) = limits.auto_review_model_override {
        capabilities["autoReviewModelOverride"] = json!(value);
    }
    if limits.requires_sandboxed_review {
        capabilities["requiresSandboxedReview"] = json!(true);
    }
    if limits.node_repl_auto_review_required {
        capabilities["nodeReplAutoReviewRequired"] = json!(true);
    }
    if let Some(value) = limits.guardian {
        capabilities["guardian"] = value;
    }
    if let Some(value) = limits.guardian_v2 {
        capabilities["guardianV2"] = value;
    }
    if let Some(value) = limits.model_messages {
        capabilities["modelMessages"] = value;
    }
    if let Some(value) = limits.comp_hash {
        capabilities["compHash"] = json!(value);
    }
    if let Some(value) = limits.supports_reasoning_summary_parameter {
        capabilities["supportsReasoningSummaryParameter"] = json!(value);
    }
    if let Some(value) = limits.default_reasoning_summary {
        capabilities["defaultReasoningSummary"] = value;
    }
    if !limits.supported_reasoning_efforts.is_empty() {
        capabilities["supportedReasoningEfforts"] = json!(limits.supported_reasoning_efforts);
    }
    if let Some(value) = limits.default_reasoning_effort {
        capabilities["defaultReasoningEffort"] = json!(value);
    }
    if let Some(value) = limits.multi_agent_reasoning_effort {
        capabilities["multiAgentReasoningEffort"] = json!(value);
    }
    if let Some(value) = limits.auto_compact_token_limit {
        capabilities["autoCompactTokenLimit"] = json!(value);
    }
    if let Some(value) = limits.effective_context_window_percent {
        capabilities["effectiveContextWindowPercent"] = json!(value);
    }
    if limits.used_fallback_model_metadata {
        capabilities["usedFallbackModelMetadata"] = json!(true);
    }
    if limits.supports_image_detail_original {
        capabilities["supportsImageDetailOriginal"] = json!(true);
    }
    if let Some(value) = limits.model_specialty {
        capabilities["modelSpecialty"] = json!(value);
    }
    capabilities
}

/// The models auxiliary work runs on when the host picked nothing, per tier.
///
/// A named constant rather than a literal in the loop because the failure mode is
/// silent: `codex_tiers` DROPS a default the catalog does not list, so a typo or a
/// retired id shows up only as "that tier has no model". The test beside this
/// constant checks every id is one the engine can price, which is what catches a
/// default that drifted off the lineup.
const CODEX_TIER_DEFAULTS: [(&str, &str); 3] = [
    ("high", "gpt-6-sol"),
    ("medium", "gpt-5.6-terra"),
    ("low", "gpt-6-luna"),
];

/// The Codex tier map: the host's picks over the lineup, defaulting to the models
/// that carry auxiliary work (sub-agents, summaries, background turns).
///
/// The defaults are the CURRENT generation's mid and light models, not the
/// flagship: a tier is a default, so putting `gpt-6-astra` in `high` would
/// re-price every auxiliary call at five times Sol for a gain the user never
/// asked for. Same reasoning as Claude's Fable, which backs no tier either —
/// the flagship stays a deliberate pick.
///
/// With the catalog loaded, a pick naming an unlisted model drops out and that
/// tier falls back to the main model; with no catalog the defaults cannot be
/// trusted, so only explicit picks pass. Each accepted tier model carries ITS OWN
/// window.
async fn codex_tiers(
    main_model: &str,
    selections: &TierSelections,
    config_root: &Path,
) -> (Map<String, Value>, Vec<Value>) {
    let catalog = super::codex_catalog::ids(config_root).await;
    let listed = |id: &str| {
        catalog
            .as_ref()
            .is_none_or(|ids| ids.iter().any(|listed| listed.eq_ignore_ascii_case(id)))
    };
    let mut tiers = Map::new();
    let mut by_model = Vec::new();
    let mut seen = vec![main_model.to_ascii_lowercase()];
    for (tier, default_id) in CODEX_TIER_DEFAULTS {
        let Some(pick) = tier_pick(selections, tier).or(catalog.as_ref().map(|_| default_id))
        else {
            continue;
        };
        if !listed(pick) {
            continue;
        }
        tiers.insert(tier.to_string(), json!(pick));
        if seen.contains(&pick.to_ascii_lowercase()) {
            continue;
        }
        seen.push(pick.to_ascii_lowercase());
        by_model.push(json!({
            "model": pick,
            "capabilities": codex_capabilities(
                super::codex_catalog::limits(config_root, pick).await,
            ),
        }));
    }
    (tiers, by_model)
}

/// The standard-API rates for the Codex subscription models.
///
/// The engine has no price table of its own, so without this every Codex turn
/// costs $0 and the usage panel and spend budget silently read zero. Lookup is
/// case-insensitive; the reported model keeps the casing it was resolved with.
/// A model the table does not know is priced by the engine's own defaults rather
/// than by a guess.
///
/// Values are the published standard short-context rates (input / output /
/// cached input / cache write). They are checked against the pricing page, not
/// carried over: the previous table still held the pre-2026-07-30 list, which
/// billed Luna at five times its real rate and Terra a quarter above it, and the
/// usage panel and spend budget report whatever is here.
fn codex_pricing(model: &str) -> Option<Value> {
    let rates = match model.trim().to_ascii_lowercase().as_str() {
        "gpt-6.1-sol" => (2.0, 10.0, 0.1, 2.5),
        "gpt-6-astra" => (10.0, 50.0, 1.0, 12.5),
        "gpt-6-sol" => (2.0, 10.0, 0.2, 2.5),
        "gpt-6-luna" => (0.1, 0.5, 0.01, 0.125),
        "gpt-5.6-sol" => (4.0, 20.0, 0.4, 5.0),
        "gpt-5.6-terra" => (2.0, 12.0, 0.2, 2.5),
        "gpt-5.6-luna" => (0.2, 1.2, 0.02, 0.25),
        _ => return None,
    };
    Some(json!([{
        "model": model.trim(),
        "inputPerMTok": rates.0,
        "outputPerMTok": rates.1,
        "cacheReadPerMTok": rates.2,
        "cacheWritePerMTok": rates.3,
    }]))
}

/// The Qoder tier map. Qoder has no lineup of its own to default to — an
/// unpicked tier means "let the service decide", which is what the main model
/// already does — so only explicit picks pass, and only keys the catalog still
/// lists: a stale pick would put an unknown key on the wire and the service
/// would refuse the turn.
async fn qoder_tiers(
    main_model: &str,
    selections: &TierSelections,
) -> (Map<String, Value>, Vec<Value>) {
    let mut tiers = Map::new();
    let mut by_model = Vec::new();
    let mut seen = vec![main_model.to_string()];
    for tier in ["high", "medium", "low"] {
        let Some(pick) = tier_pick(selections, tier) else {
            continue;
        };
        if super::qoder::lookup_model(pick).await.is_none() {
            continue;
        }
        tiers.insert(tier.to_string(), json!(pick));
        if seen.contains(&pick.to_string()) {
            continue;
        }
        seen.push(pick.to_string());
        by_model.push(json!({
            "model": pick,
            "capabilities": super::qoder::model_capabilities(pick).await,
        }));
    }
    (tiers, by_model)
}

async fn qwenwork_tiers(
    main_model: &str,
    selections: &TierSelections,
) -> (Map<String, Value>, Vec<Value>) {
    let mut tiers = Map::new();
    let mut by_model = Vec::new();
    let mut seen = vec![main_model.to_string()];
    for tier in ["high", "medium", "low"] {
        let Some(pick) = tier_pick(selections, tier) else {
            continue;
        };
        if super::qwenwork::lookup_model(pick).await.is_none() {
            continue;
        }
        tiers.insert(tier.to_string(), json!(pick));
        if seen.contains(&pick.to_string()) {
            continue;
        }
        seen.push(pick.to_string());
        by_model.push(json!({
            "model": pick,
            "capabilities": super::qwenwork::model_capabilities(pick).await,
        }));
    }
    (tiers, by_model)
}

async fn qoderwork_tiers(
    main_model: &str,
    selections: &TierSelections,
) -> (Map<String, Value>, Vec<Value>) {
    let mut tiers = Map::new();
    let mut by_model = Vec::new();
    let mut seen = vec![main_model.to_string()];
    for tier in ["high", "medium", "low"] {
        let Some(pick) = tier_pick(selections, tier) else {
            continue;
        };
        if super::qoderwork::lookup_model(pick).await.is_none() {
            continue;
        }
        tiers.insert(tier.to_string(), json!(pick));
        if seen.contains(&pick.to_string()) {
            continue;
        }
        seen.push(pick.to_string());
        by_model.push(json!({
            "model": pick,
            "capabilities": super::qoderwork::model_capabilities(pick).await,
        }));
    }
    (tiers, by_model)
}

async fn cursor_tiers(
    main_model: &str,
    selections: &TierSelections,
) -> (Map<String, Value>, Vec<Value>) {
    let mut tiers = Map::new();
    let mut by_model = Vec::new();
    let mut seen = vec![main_model.to_string()];
    for tier in ["high", "medium", "low"] {
        let Some(pick) = tier_pick(selections, tier) else {
            continue;
        };
        let pick = pick.strip_prefix("cursor:").unwrap_or(pick);
        let Ok(model) = super::cursor::resolved_model(pick).await else {
            continue;
        };
        tiers.insert(tier.into(), json!(model));
        if seen.contains(&model) {
            continue;
        }
        seen.push(model.clone());
        by_model.push(
            json!({"model":model,"capabilities":super::cursor::model_capabilities(&model).await}),
        );
    }
    (tiers, by_model)
}

async fn workbuddy_tiers(
    main_model: &str,
    selections: &TierSelections,
) -> (Map<String, Value>, Vec<Value>) {
    let mut tiers = Map::new();
    let mut by_model = Vec::new();
    let mut seen = vec![main_model.to_string()];
    for tier in ["high", "medium", "low"] {
        let Some(pick) = tier_pick(selections, tier) else {
            continue;
        };
        if super::workbuddy::lookup_model(pick).await.is_none() {
            continue;
        }
        tiers.insert(tier.to_string(), json!(pick));
        if seen.contains(&pick.to_string()) {
            continue;
        }
        seen.push(pick.to_string());
        by_model.push(json!({
            "model": pick,
            "capabilities": super::workbuddy::model_capabilities(pick).await,
        }));
    }
    (tiers, by_model)
}

/// Which model backs each tier, in Kimi's own catalog ids.
///
/// A tier pick the catalog does not list is dropped rather than sent: the service
/// refuses a model the plan does not serve, and a picker holding a stale
/// selection should fall back to the main model instead of failing the turn.
async fn kimi_tiers(
    main_model: &str,
    selections: &TierSelections,
) -> (Map<String, Value>, Vec<Value>) {
    let mut tiers = Map::new();
    let mut by_model = Vec::new();
    let mut seen = vec![main_model.to_string()];
    for tier in ["high", "medium", "low"] {
        let Some(pick) = tier_pick(selections, tier) else {
            continue;
        };
        if super::kimi::lookup_model(pick).await.is_none() {
            continue;
        }
        tiers.insert(tier.to_string(), json!(pick));
        if seen.contains(&pick.to_string()) {
            continue;
        }
        seen.push(pick.to_string());
        by_model.push(json!({
            "model": pick,
            "capabilities": super::kimi::model_capabilities(pick).await,
        }));
    }
    (tiers, by_model)
}

/// Which model backs each tier, in Bailian's own catalog ids.
///
/// Same drop-not-send rule as Kimi: a tier pick the catalog does not list would
/// reach the wire and be refused (`400 Model not exist` for an unlisted id, or
/// `403 AccessDenied` for one the plan edition does not serve), so a stale
/// selection falls back to the main model instead of failing the turn.
async fn bailian_tiers(
    main_model: &str,
    selections: &TierSelections,
) -> (Map<String, Value>, Vec<Value>) {
    let mut tiers = Map::new();
    let mut by_model = Vec::new();
    let mut seen = vec![main_model.to_string()];
    for tier in ["high", "medium", "low"] {
        // An UNMAPPED low tier is special here. The engine runs session titles on
        // the low tier as a structured request, and on this third-party wire that
        // request necessarily sends `thinking:{"type":"disabled"}` — which glm-5.3
        // refuses with 400. Left unmapped, the engine falls back to the main model
        // for the low tier, so a glm-5.3 main model would silently lose the title of
        // every conversation. Pin the unmapped low tier to a measured structured-safe
        // model instead. An EXPLICIT pick is always honoured: that is the user's
        // choice, not a gap to fill.
        let pick = match tier_pick(selections, tier) {
            Some(explicit) => explicit.to_string(),
            None if tier == "low" && !super::bailian::structured_output_safe(main_model) => {
                match super::bailian::structured_safe_fallback().await {
                    Some(safe) => safe,
                    None => continue,
                }
            }
            None => continue,
        };
        if super::bailian::lookup_model(&pick).await.is_none() {
            continue;
        }
        tiers.insert(tier.to_string(), json!(pick));
        if seen.contains(&pick) {
            continue;
        }
        seen.push(pick.clone());
        by_model.push(json!({
            "model": pick,
            "capabilities": super::bailian::model_capabilities(&pick).await,
        }));
    }
    (tiers, by_model)
}

/// Which model backs each tier, in MiniMax's own catalog ids.
///
/// The low tier carries the same drop-not-send rule as Bailian, and here it is not
/// optional: the plan's flagship model refuses `thinking:{"type":"disabled"}` with
/// 400 and the other frontier model accepts it but emits no tool call, so neither
/// can serve the forced-tool structured request a session title is. An UNMAPPED low
/// tier on a frontier main model is therefore pinned to a measured M2.x model; an
/// EXPLICIT pick is always honoured, because that is the user's choice rather than a
/// gap to fill.
async fn minimax_tiers(
    main_model: &str,
    selections: &TierSelections,
) -> (Map<String, Value>, Vec<Value>) {
    let mut tiers = Map::new();
    let mut by_model = Vec::new();
    let mut seen = vec![main_model.to_string()];
    for tier in ["high", "medium", "low"] {
        let pick = match tier_pick(selections, tier) {
            Some(explicit) => explicit.to_string(),
            None if tier == "low" && !super::minimax::structured_output_safe(main_model) => {
                match super::minimax::structured_safe_fallback().await {
                    Some(safe) => safe,
                    None => continue,
                }
            }
            None => continue,
        };
        if super::minimax::lookup_model(&pick).await.is_none() {
            continue;
        }
        tiers.insert(tier.to_string(), json!(pick));
        if seen.contains(&pick) {
            continue;
        }
        seen.push(pick.clone());
        by_model.push(json!({
            "model": pick,
            "capabilities": super::minimax::model_capabilities(&pick).await,
        }));
    }
    (tiers, by_model)
}

/// Which model backs each tier, in the plan's own catalog ids.
///
/// Same drop-not-send rule as the other plan sources: a tier pick the catalog does
/// not list would reach the wire and be refused (`400 [1211]` for an unlisted id,
/// or a `429` for one the subscription tier has not opened), so a stale selection
/// falls back to the main model instead of failing the turn.
///
/// There is no special case for an unmapped low tier here, and that is a measured
/// difference from the Bailian source rather than an oversight. Session titles run
/// on the low tier as a structured request, and on this third-party wire that
/// request necessarily sends `thinking:{"type":"disabled"}`. On the Bailian gateway
/// one model rejects that shape with a 400, so an unmapped low tier there has to be
/// pinned to a model that accepts it. Every model this plan serves accepted that
/// exact body, so leaving the tier unmapped is safe and the engine falls back to
/// the main model as it would anyway.
async fn zhipu_tiers(
    main_model: &str,
    selections: &TierSelections,
) -> (Map<String, Value>, Vec<Value>) {
    let mut tiers = Map::new();
    let mut by_model = Vec::new();
    let mut seen = vec![main_model.to_string()];
    for tier in ["high", "medium", "low"] {
        let Some(pick) = tier_pick(selections, tier) else {
            continue;
        };
        if super::zhipu::lookup_model(pick).await.is_none() {
            continue;
        }
        tiers.insert(tier.to_string(), json!(pick));
        if seen.contains(&pick.to_string()) {
            continue;
        }
        seen.push(pick.to_string());
        by_model.push(json!({
            "model": pick,
            "capabilities": super::zhipu::model_capabilities(pick).await,
        }));
    }
    (tiers, by_model)
}

/// Which model backs each tier, in the plan's own catalog ids.
///
/// Same drop-not-send rule as the other plan sources: a tier pick the catalog does
/// not list would reach the wire and be refused, so a stale selection falls back to
/// the main model instead of failing the turn.
async fn ark_tiers(
    main_model: &str,
    selections: &TierSelections,
) -> (Map<String, Value>, Vec<Value>) {
    let mut tiers = Map::new();
    let mut by_model = Vec::new();
    let mut seen = vec![main_model.to_string()];
    for tier in ["high", "medium", "low"] {
        let Some(pick) = tier_pick(selections, tier) else {
            continue;
        };
        if super::ark::lookup_model(pick).await.is_none() {
            continue;
        }
        tiers.insert(tier.to_string(), json!(pick));
        if seen.contains(&pick.to_string()) {
            continue;
        }
        seen.push(pick.to_string());
        by_model.push(json!({
            "model": pick,
            "capabilities": super::ark::model_capabilities(pick).await,
        }));
    }
    (tiers, by_model)
}

/// The account a Codex store serves.
///
/// `defaultAccountId` when it names one that is actually present, otherwise the
/// only account there is. A store holding several accounts and naming none is
/// ambiguous, and guessing one would sign the session in as a person the caller
/// did not choose — so it resolves to nothing and the caller is told to log in.
fn default_codex_account(store: &Value) -> Option<&Map<String, Value>> {
    let id = default_codex_account_key(store)?;
    store.get("accounts")?.get(&id)?.as_object()
}

fn default_codex_account_key(store: &Value) -> Option<String> {
    let accounts = store.get("accounts").and_then(Value::as_object)?;
    store
        .get("defaultAccountId")
        .and_then(Value::as_str)
        .filter(|id| accounts.contains_key(*id))
        .map(str::to_string)
        .or_else(|| {
            (accounts.len() == 1)
                .then(|| accounts.keys().next().cloned())
                .flatten()
        })
}

// ------------------------------------------------- the subscription credential

/// Where Klaus keeps its own copy of the Claude subscription credential.
///
/// A private file in the config root, the same carrier the Codex subscription
/// uses, and deliberately NOT the login keychain: the ecosystem entry is named
/// after the Claude Code client, so reading it makes macOS ask the user to
/// authorize Klaus against another product's secret — again on every refusal,
/// which turned background status polls into a prompt loop. A copy Klaus owns
/// needs no such authorization from anyone.
pub fn claude_subscription_path(config_root: &Path) -> PathBuf {
    config_root.join("claude-subscription-oauth.json")
}

/// The pre-copy carrier some profiles still hold a login in: the plaintext
/// record file older builds fell back to when the keychain refused a write.
fn legacy_plaintext_path(config_root: &Path) -> PathBuf {
    config_root.join(".credentials.json")
}

fn not_signed_in() -> String {
    "not signed in to a Claude subscription — run `klaus login claude`".to_string()
}

/// The stored Claude subscription record.
///
/// Reads Klaus's own copy first. A profile upgraded from a build that kept the
/// login in the plaintext fallback file still resolves without any ceremony:
/// that file is read as a migration source, and never written back to.
///
/// An absent copy is "not signed in"; a present-but-unreadable one is reported
/// by name, because those two need different answers from the user.
fn credential_record(config_root: &Path) -> Result<Map<String, Value>, String> {
    if let Some(record) = read_record_file(&claude_subscription_path(config_root))? {
        return Ok(record);
    }
    match read_record_file(&legacy_plaintext_path(config_root))? {
        Some(record) if record.contains_key("claudeAiOauth") || record.contains_key("tokens") => {
            return Ok(record);
        }
        Some(_) | None => {}
    }
    // A DPAPI-encrypted record is NOT "signed out": the desktop client owns the
    // Win32 call that reads it, and this engine does not. The two need different
    // answers — "sign in" versus "sign in AGAIN, here" — so the file is named.
    let encrypted = config_root.join(".credentials.enc");
    if encrypted.exists() {
        return Err(format!(
            "{} is a DPAPI-encrypted credential this engine cannot read — sign in again with `klaus login claude`",
            encrypted.display()
        ));
    }
    Err(not_signed_in())
}

/// One credential file as a record, or None when the file is simply absent.
fn read_record_file(path: &Path) -> Result<Option<Map<String, Value>>, String> {
    let raw = match std::fs::read_to_string(path) {
        Ok(raw) => raw,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(format!("{}: {error}", path.display())),
    };
    match serde_json::from_str::<Value>(&raw) {
        Ok(Value::Object(record)) => Ok(Some(record)),
        Ok(_) => Err(format!(
            "{} does not hold a credential object — sign in again with `klaus login claude`",
            path.display()
        )),
        Err(error) => Err(format!("{}: {error}", path.display())),
    }
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|since| since.as_millis() as i64)
        .unwrap_or_default()
}

/// Write JSON only the owner can read. A credential landing world-readable is
/// worse than the write failing.
fn write_private_json(path: &Path, value: &Value) -> Result<(), String> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .map_err(|error| format!("{}: {error}", parent.display()))?;
    }
    std::fs::write(path, value.to_string())
        .map_err(|error| format!("{}: {error}", path.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600));
    }
    Ok(())
}

/// The config root a credential sits in, derived from the config file the
/// endpoint was built with so both point at one profile.
pub fn config_root_of(config_path: &Path) -> PathBuf {
    config_path
        .parent()
        .map(Path::to_path_buf)
        .unwrap_or_else(|| PathBuf::from("."))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn official_api_catalog_includes_unassigned_models_and_preserves_required_metadata() {
        use klaus_agent_core::provider::openai::{OpenAIProvider, OpenAIProviderOptions};
        use klaus_agent_core::types::provider::{ModelCapabilities, ModelProvider};
        let root = std::path::Path::new("/unused-fixture-root");
        let init = klaus_agent_protocol::types::ProtocolSessionInit::default();
        let provider: std::sync::Arc<dyn ModelProvider> =
            std::sync::Arc::new(OpenAIProvider::new(OpenAIProviderOptions {
                api_key: Some("fixture-key".into()),
                default_model: Some("parent".into()),
                ..Default::default()
            }));
        let decorated = provider_decorator(root)(provider, &init);
        assert!(decorated
            .offline_model_catalog()
            .iter()
            .any(|(model, _)| model == "gpt-5.6-luna"));
        let gateway: std::sync::Arc<dyn ModelProvider> =
            std::sync::Arc::new(OpenAIProvider::new(OpenAIProviderOptions {
                api_key: Some("fixture-key".into()),
                base_url: Some("https://gateway.example/v1".into()),
                default_model: Some("parent".into()),
                ..Default::default()
            }));
        assert!(!provider_decorator(root)(gateway, &init)
            .offline_model_catalog()
            .iter()
            .any(|(model, _)| model == "gpt-5.6-luna"));
        let merged = merge_review_catalog_metadata(
            ModelCapabilities {
                requires_sandboxed_review: true,
                node_repl_auto_review_required: true,
                ..Default::default()
            },
            ModelCapabilities::default(),
        );
        assert!(merged.requires_sandboxed_review && merged.node_repl_auto_review_required);
    }

    /// No host tier picks: every tier follows the source's own lineup.
    fn no_tiers() -> TierSelections {
        TierSelections::new()
    }

    /// Host tier picks, in the source's own id space.
    fn tiers(picks: &[(&str, &str)]) -> TierSelections {
        picks
            .iter()
            .map(|(tier, pick)| ((*tier).to_string(), json!(pick)))
            .collect()
    }

    /// Point the Bailian gateway at a black hole so a tier test's background
    /// catalog refresh fails fast instead of reaching the network. The assertions
    /// read the seeded snapshot, which `fetch_models` returns before that refresh
    /// lands, so nothing here depends on connectivity.
    struct BailianGatewayGuard;
    impl Drop for BailianGatewayGuard {
        fn drop(&mut self) {
            klaus_agent_core::runtime_process::remove_process_environment_var(
                "BAILIAN_CATALOG_BASE",
            );
        }
    }
    fn blackhole_bailian_gateway() -> BailianGatewayGuard {
        klaus_agent_core::runtime_process::set_process_environment_var(
            "BAILIAN_CATALOG_BASE",
            "http://127.0.0.1:9/compatible-mode/v1",
        );
        BailianGatewayGuard
    }

    /// The plan's own catalog, as the live endpoint returns it, with the ids that
    /// matter for the tier logic: a safe head and glm-5.3, which cannot serve a
    /// structured request.
    ///
    /// The in-memory TTL cache is dropped first: it is process-global and lives
    /// across tests, so an entry left by an earlier test would win over this
    /// snapshot and make the assertions depend on test order.
    fn seed_bailian_catalog() {
        super::super::bailian::catalog::forget_cache();
        crate::app_server::catalog_test_support::store_credential(
            "bailian",
            &super::super::bailian::config_root(),
            "tier",
        );
        let entries = ["qwen3.8-flash", "qwen3.8-max", "glm-5.3"]
            .iter()
            .map(|id| {
                json!({
                    "key": id,
                    "display_name": id,
                    "contextTokens": 983_616,
                    "is_vl": false,
                    "canDisableThinking": id != &"glm-5.3",
                    "efforts": ["low", "high", "max"],
                    "defaultEffort": "max",
                })
            })
            .collect::<Vec<_>>();
        super::super::bailian::catalog::seed_owned_catalog(&entries);
    }

    /// An unmapped low tier must not inherit a main model that cannot serve a
    /// structured request.
    ///
    /// Session titles run on the LOW tier as a structured request, and on this
    /// third-party wire that request necessarily sends `thinking:{"type":"disabled"}`
    /// (the provider cannot use native structured output because
    /// `use_native_structured_output` gates on `is_first_party`). Measured, glm-5.3
    /// refuses that with HTTP 400. Left to fall back to the main model, a glm-5.3
    /// session would run fine and still silently lose its title — a failure that
    /// reads as an unrelated bug rather than as a model limitation.
    #[tokio::test]
    async fn an_unmapped_low_tier_avoids_a_main_model_that_cannot_take_a_structured_request() {
        let _lock = super::super::bailian::test_env_lock().lock().await;
        let temp = tempfile::tempdir().expect("tempdir");
        super::super::bailian::set_config_root(temp.path());
        let _gateway = blackhole_bailian_gateway();
        seed_bailian_catalog();

        // `resolved` rather than `tiers`: the test module has a `tiers()` helper,
        // and a local of that name would shadow it for the rest of the function.
        let (resolved, by_model) = bailian_tiers("glm-5.3", &no_tiers()).await;
        assert_eq!(
            resolved.get("low").and_then(Value::as_str),
            Some("qwen3.8-flash"),
            "the low tier is pinned to a measured structured-safe model, not glm-5.3"
        );
        // The pinned model needs its own capability entry, or the engine would
        // declare nothing for the model that actually runs the title call.
        assert!(
            by_model
                .iter()
                .any(|row| row["model"] == json!("qwen3.8-flash")),
            "capabilities_by_model covers the pinned low-tier model: {by_model:?}"
        );
        // Only the low tier is pinned; high and medium stay unmapped and therefore
        // follow the main model, which is what the user chose.
        assert!(
            resolved.get("high").is_none(),
            "high is left to the main model"
        );
        assert!(
            resolved.get("medium").is_none(),
            "medium is left to the main model"
        );
    }

    /// A structured-safe main model needs no pin, and an explicit host pick is
    /// always honoured — the pin fills a gap, it does not override a choice.
    #[tokio::test]
    async fn a_safe_main_model_leaves_the_low_tier_alone_and_an_explicit_pick_wins() {
        let _lock = super::super::bailian::test_env_lock().lock().await;
        let temp = tempfile::tempdir().expect("tempdir");
        super::super::bailian::set_config_root(temp.path());
        let _gateway = blackhole_bailian_gateway();
        seed_bailian_catalog();

        // qwen3.8-flash serves structured requests, so nothing is pinned.
        let (resolved, _) = bailian_tiers("qwen3.8-flash", &no_tiers()).await;
        assert!(
            resolved.is_empty(),
            "a safe main model pins no tier: {resolved:?}"
        );

        // And an explicit glm-5.3 pick on the low tier is the user's choice: it is
        // kept even though it cannot serve a structured request, because overriding
        // it would silently change the model the user asked for.
        let (explicit, _) = bailian_tiers("qwen3.8-flash", &tiers(&[("low", "glm-5.3")])).await;
        assert_eq!(
            explicit.get("low").and_then(Value::as_str),
            Some("glm-5.3"),
            "an explicit tier pick is honoured as-is"
        );
    }

    #[tokio::test]
    async fn a_non_subscription_selection_is_handed_back() {
        let root = std::env::temp_dir();
        assert_eq!(
            resolve("custom:byo", &root, &no_tiers()).await.unwrap(),
            None
        );
        assert_eq!(resolve("auto", &root, &no_tiers()).await.unwrap(), None);
        assert_eq!(resolve("high", &root, &no_tiers()).await.unwrap(), None);
    }

    /// Named but unavailable here: refused with the reason, never handed back for
    /// the BYO path to answer with a different account.
    #[tokio::test]
    async fn a_host_only_source_is_refused_with_its_reason() {
        let root = std::env::temp_dir();
        let error = resolve("local:llama3", &root, &no_tiers())
            .await
            .expect_err("local is host-only");
        assert!(error.contains("local model server"), "{error}");
    }

    /// Signed out of Qoder reads as signed out, with the command that fixes it —
    /// not as "the desktop client is required", which is what it said before the
    /// signer and the bridge moved into the engine.
    #[tokio::test]
    async fn a_signed_out_qoder_selection_says_so() {
        let root = tempfile::tempdir().expect("temp dir");
        let error = resolve("qoder:auto", root.path(), &no_tiers())
            .await
            .expect_err("no qoder login on a temp root");
        assert!(error.contains("not signed in"), "{error}");
        assert!(error.contains("klaus login qoder"), "{error}");
        assert!(
            !error.contains("desktop"),
            "qoder is no longer desktop-only: {error}"
        );
    }

    #[test]
    fn the_aliases_name_the_current_generation_and_anything_else_passes_through() {
        assert_eq!(claude_model("fable"), CLAUDE_FABLE);
        assert_eq!(claude_model("opus"), CLAUDE_OPUS);
        assert_eq!(claude_model("sonnet"), CLAUDE_SONNET);
        assert_eq!(claude_model("haiku"), CLAUDE_HAIKU);
        assert_eq!(claude_model("high"), CLAUDE_OPUS);
        assert_eq!(claude_model("auto"), CLAUDE_OPUS);
        assert_eq!(claude_model("claude-opus-4-9"), "claude-opus-4-9");
        assert_eq!(claude_model("opus[1m]"), CLAUDE_OPUS);
        assert_eq!(claude_model("claude-opus-5[1m]"), "claude-opus-5");
        assert_eq!(
            claude_model("claude-opus-5[preview]"),
            "claude-opus-5[preview]"
        );
    }

    /// Fable is a main-model pick, not a tier word: naming it must not be
    /// rewritten by whatever the host has mapped onto the `high` slot.
    #[test]
    fn the_fable_pick_survives_a_tier_map_that_remaps_high() {
        let selections = json!({ "high": "haiku" })
            .as_object()
            .cloned()
            .expect("an object");
        assert_eq!(claude_main_alias("fable", &selections), "fable");
        assert_eq!(claude_main_alias("opus", &selections), "haiku");
        assert_eq!(claude_main_alias("high[1m]", &selections), "haiku");
        assert_eq!(
            claude_model(claude_main_alias("high[1m]", &selections)),
            CLAUDE_HAIKU
        );
    }

    /// Each alias names a CURRENT point release, and this is the exact defect this
    /// file already had: `opus` resolved to `claude-opus-5` after the vendor
    /// shipped Opus 5.5, so every user who never picked a model explicitly kept
    /// running the older one — no error, no notice, just an older model.
    ///
    /// Pinned as literals rather than re-read from the constants so that editing a
    /// constant is a decision someone has to make here on purpose, against the
    /// vendor's alias table.
    #[test]
    fn the_aliases_name_the_current_point_release() {
        assert_eq!(CLAUDE_OPUS, "claude-opus-5-5");
        assert_eq!(CLAUDE_FABLE, "claude-fable-5-2");
        assert_eq!(CLAUDE_SONNET, "claude-sonnet-5-5");
        assert_eq!(CLAUDE_HAIKU, "claude-haiku-5-5");
        // And every one of them resolves through the alias table above, so the
        // constants cannot drift away from what a picker shows.
        assert_eq!(claude_model("opus"), CLAUDE_OPUS);
        assert_eq!(claude_model("fable"), CLAUDE_FABLE);
        assert_eq!(claude_model("auto"), CLAUDE_OPUS);
    }

    /// Fable shares Opus's declaration by falling through one arm, so pin it: a
    /// later edit to that arm must not silently strip Fable's ladder, window or
    /// output ceiling.
    #[test]
    fn fable_declares_the_full_ladder_a_1m_window_and_a_128k_output_ceiling() {
        let caps = claude_capabilities(CLAUDE_FABLE);
        assert_eq!(
            caps["effort"]["levels"],
            json!(["low", "medium", "high", "xhigh", "max"])
        );
        assert_eq!(caps["contextWindow"], json!(1_000_000));
        assert_eq!(caps["maxOutputTokens"], json!(128_000));
        assert_eq!(caps["oneMillionContext"], json!(true));
        assert_eq!(caps["taskBudgets"], json!(true));
        assert_eq!(caps["vision"], json!(true));
        assert_eq!(caps["nativePdf"], json!(true));
        assert_eq!(caps["thinking"]["adaptive"], json!(true));
        assert_eq!(
            claude_capabilities(CLAUDE_SONNET)["taskBudgets"],
            json!(true)
        );
    }

    /// The lineup a host's picker is built from. Its ladders are READ OFF the
    /// capability sets, so this also asserts the property that made the facade
    /// necessary: picker and session cannot disagree about a model's levels.
    #[test]
    fn the_lineup_carries_every_alias_with_the_ladder_its_capability_set_declares() {
        let lineup = claude_lineup();
        let models = lineup["models"].as_array().expect("a models array");
        assert_eq!(
            models
                .iter()
                .map(|entry| entry["alias"].as_str().unwrap())
                .collect::<Vec<_>>(),
            vec!["fable", "opus", "sonnet", "haiku"]
        );
        for entry in models {
            let model = entry["model"].as_str().unwrap();
            let declared = claude_capabilities(model);
            let levels = declared
                .get("effort")
                .and_then(|effort| effort.get("levels"))
                .cloned()
                .unwrap_or_else(|| json!([]));
            assert_eq!(
                entry["effortLevels"], levels,
                "{model} advertises a ladder the capability set does not"
            );
            // The host reads this separately from effort levels so a future
            // no-knob model does not appear to lack reasoning.
            assert_eq!(entry["thinking"], json!(true), "{model}");
        }
        // Fable backs no tier; the other three are the slot defaults.
        assert_eq!(models[0].get("tier"), None);
        assert_eq!(models[1]["tier"], json!("high"));
        assert_eq!(models[2]["tier"], json!("medium"));
        assert_eq!(models[3]["tier"], json!("low"));
        for entry in models {
            assert_eq!(
                entry["effortLevels"],
                json!(["low", "medium", "high", "xhigh", "max"]),
                "{}",
                entry["model"]
            );
        }
        assert_eq!(models[0]["label"], json!("Fable 5.2"));
        assert_eq!(models[1]["label"], json!("Opus 5.5"));
        assert_eq!(models[2]["label"], json!("Sonnet 5.5"));
        assert_eq!(models[3]["label"], json!("Haiku 5.5"));
        let haiku = claude_capabilities(CLAUDE_HAIKU);
        assert_eq!(haiku["thinking"]["adaptive"], json!(true));
        assert_eq!(haiku["contextWindow"], json!(1_000_000));
        assert_eq!(haiku["maxOutputTokens"], json!(128_000));
        assert_eq!(haiku["oneMillionContext"], json!(true));
        assert_eq!(haiku["taskBudgets"], json!(true));
    }

    #[test]
    fn previous_claude_models_keep_their_own_capabilities() {
        let fable_51 = claude_capabilities("claude-fable-5-1");
        assert_eq!(
            fable_51["effort"]["levels"],
            json!(["low", "medium", "high", "xhigh", "max"])
        );
        assert_eq!(fable_51["contextWindow"], json!(1_000_000));
        assert_eq!(fable_51["maxOutputTokens"], json!(128_000));
        assert_eq!(fable_51["taskBudgets"], json!(true));
        assert_eq!(fable_51["fastMode"], json!(false));
        let haiku = claude_capabilities("claude-haiku-4-5");
        assert!(haiku.get("effort").is_none());
        assert_eq!(haiku["contextWindow"], json!(200_000));
        assert_eq!(haiku["maxOutputTokens"], json!(64_000));
        assert_eq!(haiku["taskBudgets"], json!(false));
        assert_eq!(
            claude_capabilities("claude-sonnet-5")["taskBudgets"],
            json!(false)
        );
    }

    #[test]
    fn claude_source_declares_media_limits_from_known_model_facts() {
        for model in [
            CLAUDE_FABLE,
            CLAUDE_OPUS,
            CLAUDE_SONNET,
            CLAUDE_HAIKU,
            "claude-sonnet-5",
        ] {
            let caps = claude_capabilities(model);
            assert_eq!(caps["maxMediaBlocks"], json!(600), "{model}");
            assert_eq!(caps["imageLimits"]["maxWidth"], json!(2_000));
            assert_eq!(caps["imageLimits"]["maxHeight"], json!(2_000));
            assert_eq!(caps["imageLimits"]["maxBase64Size"], json!(5_242_880));
            assert!(caps["imageLimits"].get("targetRawSize").is_none());
        }
        let standard = claude_capabilities("claude-haiku-4-5");
        assert_eq!(standard["contextWindow"], json!(200_000));
        assert_eq!(standard["maxMediaBlocks"], json!(100));
        let unknown = claude_capabilities("unknown-future-model");
        assert_eq!(unknown["maxMediaBlocks"], json!(100));
        assert!(unknown["imageLimits"].get("targetRawSize").is_none());
        let (_, tiers) = claude_tiers(CLAUDE_OPUS, &TierSelections::new());
        for tier in tiers {
            let model = tier["model"].as_str().unwrap();
            assert_eq!(tier["capabilities"], claude_capabilities(model));
        }
    }

    /// Signed out reads as signed out, with the command that fixes it, rather
    /// than as an empty provider the engine would then half-build.
    #[tokio::test]
    async fn a_signed_out_claude_selection_says_so() {
        let root = tempfile::tempdir().expect("temp dir");
        // No copy at all is the ordinary signed-out state; an empty record is
        // the other one, and both have to read the same way.
        let error = resolve("subscription:opus", root.path(), &no_tiers())
            .await
            .expect_err("no stored credential means signed out");
        assert!(error.contains("not signed in"), "{error}");
        assert!(error.contains("klaus login claude"), "{error}");
        std::fs::write(claude_subscription_path(root.path()), "{}").unwrap();
        let error = resolve("subscription:opus", root.path(), &no_tiers())
            .await
            .expect_err("no claudeAiOauth entry means signed out");
        assert!(error.contains("not signed in"), "{error}");
    }

    /// An expired credential fails the SELECTION as expired, not as signed out.
    ///
    /// `sign_in_status` reporting it is not enough: that only feeds `auth status`,
    /// while this is the path that decides whether a turn runs at all. The three
    /// refusals a subscription selection can give — not signed in, expired,
    /// unsupported here — have to stay distinguishable, because "expired" is the
    /// only one where the user already did everything right and just has to renew.
    ///
    /// And it must REFUSE rather than resolve: falling through to a configured BYO
    /// entry would silently bill the turn to another account.
    #[tokio::test]
    async fn an_expired_subscription_selection_is_refused_as_expired() {
        let root = tempfile::tempdir().expect("temp dir");
        write_claude_login(
            root.path(),
            json!({ "accessToken": "at", "expiresAt": now_ms() - 1000 }),
        )
        .unwrap();
        let error = resolve("subscription:sonnet", root.path(), &no_tiers())
            .await
            .expect_err("an expired credential cannot resolve a provider");
        assert!(error.contains("expired"), "{error}");
        assert!(error.contains("cannot be renewed"), "{error}");
        assert!(error.contains("klaus login claude"), "{error}");
        // The three refusals stay apart: this one is not "signed out".
        assert!(!error.contains("not signed in"), "{error}");
    }

    #[tokio::test]
    async fn a_stored_claude_login_builds_the_anthropic_provider() {
        let root = tempfile::tempdir().expect("temp dir");
        std::fs::write(
            claude_subscription_path(root.path()),
            json!({
                "claudeAiOauth": {
                    "accessToken": "at-1",
                    "refreshToken": "rt-1",
                    "expiresAt": now_ms() + 3_600_000,
                }
            })
            .to_string(),
        )
        .unwrap();
        let resolved = resolve("subscription:sonnet", root.path(), &no_tiers())
            .await
            .unwrap()
            .expect("a subscription selection resolves");
        assert_eq!(resolved.provider["type"], json!("anthropic"));
        assert_eq!(resolved.provider["model"], json!(CLAUDE_SONNET));
        assert_eq!(resolved.auth["type"], json!("oauth_token"));
        assert_eq!(resolved.auth["accessToken"], json!("at-1"));
        assert_eq!(resolved.auth["refreshToken"], json!("rt-1"));
        // The client config has to be nested under `oauth` to decode at all.
        assert_eq!(resolved.auth["oauth"]["clientId"], json!(CLAUDE_CLIENT_ID));
        assert_eq!(resolved.auth["oauth"]["tokenUrl"], json!(CLAUDE_TOKEN_URL));
    }

    /// A login left in the plaintext fallback file by an older build still
    /// resolves after the upgrade: the copy is the primary store, but the old
    /// file is a migration source, and reading it must not prompt for anything.
    #[tokio::test]
    async fn a_legacy_plaintext_login_still_resolves_as_a_migration_source() {
        let root = tempfile::tempdir().expect("temp dir");
        std::fs::write(
            root.path().join(".credentials.json"),
            json!({
                "pluginSecrets": { "keep": "me" },
                "claudeAiOauth": {
                    "accessToken": "at-legacy",
                    "refreshToken": "rt-legacy",
                    "expiresAt": now_ms() + 3_600_000,
                }
            })
            .to_string(),
        )
        .unwrap();
        let resolved = resolve("subscription:sonnet", root.path(), &no_tiers())
            .await
            .unwrap()
            .expect("the legacy login resolves");
        assert_eq!(resolved.auth["accessToken"], json!("at-legacy"));
        // The legacy file is read, never written: an unrelated key beside the
        // login survives untouched.
        let raw = std::fs::read_to_string(root.path().join(".credentials.json")).unwrap();
        assert!(raw.contains("keep"), "{raw}");
    }

    #[tokio::test]
    async fn a_stored_codex_login_builds_the_responses_provider() {
        let root = tempfile::tempdir().expect("temp dir");
        std::fs::write(
            root.path().join("openai-subscription-oauth.json"),
            json!({
                "defaultAccountId": "acc-1",
                "accounts": {
                    "acc-1": {
                        "accountId": "chatgpt-acc",
                        "accessToken": "at-codex",
                        "idToken": "captured-id-token",
                        "refreshToken": "rt-codex",
                        "accessTokenExpiresAt": now_ms() + 3_600_000,
                    }
                }
            })
            .to_string(),
        )
        .unwrap();
        let resolved = resolve("codex:gpt-5.6-sol", root.path(), &no_tiers())
            .await
            .unwrap()
            .expect("a codex selection resolves");
        assert_eq!(resolved.provider["type"], json!("openai"));
        assert_eq!(resolved.provider["api"], json!("responses"));
        assert_eq!(resolved.provider["model"], json!("gpt-5.6-sol"));
        assert_eq!(resolved.provider["baseUrl"], json!(CODEX_BASE_URL));
        assert_eq!(resolved.auth["accessToken"], json!("at-codex"));
        assert_eq!(resolved.auth["idToken"], json!("captured-id-token"));
        assert_eq!(
            resolved.auth["extraHeaders"]["ChatGPT-Account-Id"],
            json!("chatgpt-acc")
        );
    }

    /// Several accounts and no `defaultAccountId` is ambiguous: resolving it would
    /// sign the session in as whichever one iterated first.
    #[tokio::test]
    async fn an_ambiguous_codex_store_does_not_guess_an_account() {
        let root = tempfile::tempdir().expect("temp dir");
        let account = json!({
            "accountId": "a",
            "accessToken": "at",
            "refreshToken": "rt",
            "accessTokenExpiresAt": now_ms() + 3_600_000,
        });
        std::fs::write(
            root.path().join("openai-subscription-oauth.json"),
            json!({ "accounts": { "one": account.clone(), "two": account } }).to_string(),
        )
        .unwrap();
        let error = resolve("codex:auto", root.path(), &no_tiers())
            .await
            .expect_err("two accounts and no default must not resolve");
        assert!(error.contains("no signed-in account"), "{error}");
    }

    #[tokio::test]
    async fn a_signed_out_codex_selection_names_the_missing_file() {
        let root = tempfile::tempdir().expect("temp dir");
        let error = resolve("codex:auto", root.path(), &no_tiers())
            .await
            .expect_err("no store means signed out");
        assert!(error.contains("not signed in"), "{error}");
        assert!(error.contains("openai-subscription-oauth.json"), "{error}");
    }

    /// A login written here is a login the resolver reads: the whole point of one
    /// credential location is that these two cannot disagree.
    #[tokio::test]
    async fn a_written_claude_login_is_what_the_resolver_then_reads() {
        let root = tempfile::tempdir().expect("temp dir");
        write_claude_login(
            root.path(),
            json!({ "accessToken": "at-written", "refreshToken": "rt-written" }),
        )
        .expect("the login is stored");

        assert!(matches!(
            sign_in_status(root.path())
                .into_iter()
                .find(|(source, _)| *source == "claude")
                .map(|(_, state)| state),
            Some(SignIn::Active { .. })
        ));
        let resolved = resolve("subscription:opus", root.path(), &no_tiers())
            .await
            .unwrap()
            .expect("the stored login resolves");
        assert_eq!(resolved.auth["accessToken"], json!("at-written"));

        assert!(delete_claude_login(root.path()).expect("delete succeeds"));
        assert_eq!(
            sign_in_status(root.path())
                .into_iter()
                .find(|(source, _)| *source == "claude")
                .map(|(_, state)| state),
            Some(SignIn::Absent)
        );
        // Deleting again is a no-op, not an error: logging out twice is not a
        // failure the user needs told about.
        assert!(!delete_claude_login(root.path()).expect("second delete succeeds"));
    }

    /// Writing a Claude login copies it into Klaus's own store and preserves
    /// whatever else that store holds — account metadata lives beside the tokens.
    #[test]
    fn writing_a_login_preserves_the_rest_of_the_record() {
        let root = tempfile::tempdir().expect("temp dir");
        std::fs::write(
            claude_subscription_path(root.path()),
            json!({ "account": { "email_address": "kept@example.test" } }).to_string(),
        )
        .unwrap();
        write_claude_login(root.path(), json!({ "accessToken": "at" })).unwrap();
        let record = credential_record(root.path()).expect("record reads back");
        assert_eq!(
            record["account"]["email_address"],
            json!("kept@example.test")
        );
        assert_eq!(record["claudeAiOauth"]["accessToken"], json!("at"));
    }

    /// The copy is Klaus's own private file: a login lands there with owner-only
    /// permissions, a logout removes the file once nothing else lives in it, and
    /// nothing in the flow touches the login keychain — which is what made macOS
    /// prompt for another product's secret on every background status poll.
    #[test]
    fn the_copy_is_a_private_file_that_logins_fill_and_logouts_empty() {
        let root = tempfile::tempdir().expect("temp dir");
        let path = claude_subscription_path(root.path());
        assert!(!path.exists());
        write_claude_login(root.path(), json!({ "accessToken": "at" })).unwrap();
        assert!(path.exists());
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
                0o600
            );
        }
        assert!(delete_claude_login(root.path()).expect("delete succeeds"));
        assert!(!path.exists(), "an emptied copy leaves no file behind");
        // A second logout changes nothing and is not an error.
        assert!(!delete_claude_login(root.path()).expect("second delete succeeds"));
    }

    /// A logout prunes only the login key from the copy: account metadata
    /// written beside the tokens survives, and the legacy plaintext file is
    /// never a logout target.
    #[test]
    fn a_logout_prunes_only_the_login_key_from_the_copy() {
        let root = tempfile::tempdir().expect("temp dir");
        std::fs::write(
            claude_subscription_path(root.path()),
            json!({ "account": { "email_address": "kept@example.test" }, "claudeAiOauth": { "accessToken": "at" } })
                .to_string(),
        )
        .unwrap();
        std::fs::write(
            root.path().join(".credentials.json"),
            json!({ "claudeAiOauth": { "accessToken": "legacy" } }).to_string(),
        )
        .unwrap();
        assert!(delete_claude_login(root.path()).expect("delete succeeds"));
        let record = credential_record(root.path()).expect("the copy survives");
        assert_eq!(
            record["account"]["email_address"],
            json!("kept@example.test")
        );
        assert!(record.get("claudeAiOauth").is_none());
        // The legacy file loses its login key too — a login left in it would
        // resurrect after the logout — and an emptied file is removed.
        assert!(!root.path().join(".credentials.json").exists());
    }

    #[tokio::test]
    async fn a_written_codex_login_becomes_the_default_account() {
        let root = tempfile::tempdir().expect("temp dir");
        write_codex_login(
            root.path(),
            json!({
                "accountId": "acct-9",
                "email": "someone@example.com",
                "accessToken": "at-9",
                "refreshToken": "rt-9",
                "accessTokenExpiresAt": now_ms() + 3_600_000,
            }),
        )
        .expect("the login is stored");
        let resolved = resolve("codex:auto", root.path(), &no_tiers())
            .await
            .unwrap()
            .expect("the stored login resolves");
        assert_eq!(resolved.auth["accessToken"], json!("at-9"));
        assert_eq!(resolved.provider["model"], json!(CODEX_DEFAULT_MAIN_MODEL));
        assert_eq!(
            sign_in_status(root.path())
                .into_iter()
                .find(|(source, _)| *source == "codex")
                .map(|(_, state)| state),
            Some(SignIn::Active {
                account: Some("someone@example.com".to_string())
            })
        );
        assert!(delete_codex_login(root.path()).expect("delete succeeds"));
        assert!(!delete_codex_login(root.path()).expect("second delete succeeds"));
    }

    /// An expired credential with no way to renew reads as expired, not as signed
    /// in: a status line that says "signed in" before a turn fails is worse than
    /// no status line.
    #[test]
    fn an_unrenewable_expired_credential_reads_as_expired() {
        let root = tempfile::tempdir().expect("temp dir");
        write_claude_login(
            root.path(),
            json!({ "accessToken": "at", "expiresAt": now_ms() - 1000 }),
        )
        .unwrap();
        assert_eq!(
            sign_in_status(root.path())
                .into_iter()
                .find(|(source, _)| *source == "claude")
                .map(|(_, state)| state),
            Some(SignIn::Expired)
        );
    }

    /// Prefer the new Sol over the flagship and previous Sol even when the
    /// service puts the flagship first; keep the previous Sol for older lineups.
    #[test]
    fn codex_auto_prefers_latest_sol_over_the_flagship_head() {
        let models = super::super::codex_catalog::parse(&json!({ "models": [
            { "slug": "gpt-6-astra", "visibility": "list", "priority": 2 },
            { "slug": "gpt-6-sol", "visibility": "list", "priority": 3 },
            { "slug": "gpt-6.1-sol", "display_name": "GPT-6.1-Sol", "visibility": "list",
              "priority": 1, "context_window": 272000,
              "supported_reasoning_levels": [{ "effort": "low" }, { "effort": "medium" }, { "effort": "ultra" }] },
            { "slug": "gpt-5.6-terra", "visibility": "list", "priority": 8 }
        ] }));
        assert_eq!(codex_catalog_default_model(&models), Some("gpt-6.1-sol"));
        assert_eq!(models[0]["contextWindow"], json!(272000));
        assert_eq!(
            models[0]["supportedReasoningLevels"],
            json!(["low", "medium", "ultra"])
        );
    }

    #[test]
    fn codex_auto_keeps_previous_sol_when_latest_is_not_listed() {
        let models = vec![json!({ "id": "gpt-6-astra" }), json!({ "id": "gpt-6-sol" })];
        assert_eq!(codex_catalog_default_model(&models), Some("gpt-6-sol"));
    }

    #[test]
    fn codex_auto_falls_back_to_the_catalog_head_without_sol() {
        let models = vec![
            json!({ "id": "gpt-6-astra" }),
            json!({ "id": "gpt-5.6-terra" }),
        ];
        assert_eq!(codex_catalog_default_model(&models), Some("gpt-6-astra"));
    }

    #[test]
    fn codex_capabilities_preserve_declared_output_detail_without_guessing() {
        use klaus_agent_core::types::provider::OutputVerbosity;

        let declared = codex_capabilities(super::super::codex_catalog::ModelLimits {
            output_verbosity: Some(OutputVerbosity::Low),
            ..Default::default()
        });
        assert_eq!(declared["outputVerbosity"], json!("low"));
        let overridden = codex_capabilities(super::super::codex_catalog::ModelLimits {
            output_verbosity: Some(OutputVerbosity::High),
            ..Default::default()
        });
        assert_eq!(overridden["outputVerbosity"], json!("high"));
        let unknown = codex_capabilities(super::super::codex_catalog::ModelLimits::default());
        assert!(unknown.get("outputVerbosity").is_none());
    }

    #[test]
    fn multi_agent_effort_metadata_survives_projection_and_declaration_merge() {
        use klaus_agent_core::runtime_values::{runtime_codec, RuntimeString};
        use klaus_agent_core::types::provider::ModelCapabilities;

        let projected = codex_capabilities(super::super::codex_catalog::ModelLimits {
            multi_agent_reasoning_effort: Some("custom/Exact".into()),
            ..Default::default()
        });
        assert_eq!(projected["multiAgentReasoningEffort"], "custom/Exact");
        let catalog: ModelCapabilities =
            runtime_codec::decode_text(&RuntimeString::from(projected.to_string()))
                .expect("projected metadata decodes");
        let inherited =
            merge_review_catalog_metadata(catalog.clone(), ModelCapabilities::default());
        assert_eq!(
            inherited.multi_agent_reasoning_effort,
            Some("custom/Exact".into())
        );
        let overridden = merge_review_catalog_metadata(
            catalog,
            ModelCapabilities {
                multi_agent_reasoning_effort: Some("high".into()),
                ..Default::default()
            },
        );
        assert_eq!(overridden.multi_agent_reasoning_effort, Some("high".into()));
        let unknown = codex_capabilities(super::super::codex_catalog::ModelLimits::default());
        assert!(unknown.get("multiAgentReasoningEffort").is_none());
        assert!(merge_review_catalog_metadata(
            ModelCapabilities::default(),
            ModelCapabilities::default()
        )
        .multi_agent_reasoning_effort
        .is_none());
    }

    /// The declared ladder follows the model, not a constant. Every rung on the
    /// declaration is a rung a request may spell, so a model that tops out below
    /// `max` must not be handed `max`: a turn asking for it would send a rung the
    /// service never offered. Measured against the live catalog, where gpt-5.5
    /// advertises low…xhigh while gpt-6-astra advertises up to `ultra`.
    #[test]
    fn codex_capabilities_declare_the_ladder_the_model_advertises() {
        let narrow = codex_capabilities(super::super::codex_catalog::ModelLimits {
            effort_levels: Some(
                ["low", "medium", "high", "xhigh"]
                    .iter()
                    .map(|rung| (*rung).to_string())
                    .collect(),
            ),
            ..Default::default()
        });
        assert_eq!(
            narrow["effort"]["levels"],
            json!(["low", "medium", "high", "xhigh"]),
            "a narrower ladder is declared as-is"
        );

        let unknown = codex_capabilities(super::super::codex_catalog::ModelLimits::default());
        assert!(
            unknown.get("effort").is_none(),
            "missing metadata declares no ladder"
        );

        // A catalog that DID answer but named only rungs the engine cannot spell
        // (`ultra` alone, say) declares no effort knob at all. Guessing the full
        // table there is the same overstatement a hardcoded ladder made.
        let unspellable = codex_capabilities(super::super::codex_catalog::ModelLimits {
            effort_levels: Some(Vec::new()),
            ..Default::default()
        });
        assert!(
            unspellable.get("effort").is_none(),
            "a ladder with no spellable rung declares no effort knob, got {unspellable}"
        );
    }

    /// The catalog's own rung spellings arrive in mixed case and mixed shapes, and
    /// a rung outside the engine's vocabulary must be dropped rather than folded
    /// onto a neighbour: folding `ultra` into `max` would declare support for a
    /// rung the model does not have while losing the distinct one it does.
    #[test]
    fn only_spellable_effort_rungs_survive_the_catalog_projection() {
        use super::super::codex_catalog::spellable_effort_levels;
        let mixed = ["LOW", "medium", "High", "xhigh", "max", "ultra", "minimal"];
        assert_eq!(
            spellable_effort_levels(
                &mixed
                    .iter()
                    .map(|rung| (*rung).to_string())
                    .collect::<Vec<_>>()
            ),
            vec!["low", "medium", "high", "xhigh", "max"],
            "unknown rungs are dropped, known ones keep ladder order"
        );
        assert_eq!(
            spellable_effort_levels(&["ultra".to_string()]),
            Vec::<String>::new(),
            "a ladder of only unspellable rungs is empty, not a guess"
        );
    }

    /// A bogus override must not make the engine announce a version the backend
    /// does not recognise: it refuses the whole request, so an unusable value has
    /// to fall back to the pinned one instead of going out on the wire.
    #[test]
    fn only_a_version_shaped_override_wins() {
        assert!(
            looks_like_version(CODEX_CLIENT_VERSION),
            "the pinned version is itself what the backend checks: {CODEX_CLIENT_VERSION}"
        );
        assert!(looks_like_version("0.153.1"));
        assert!(looks_like_version("1.2.3-alpha.1"));
        assert!(looks_like_version("1.2.3+build.5"));
        assert!(!looks_like_version("not-a-version"));
        assert!(!looks_like_version(""));
        assert!(!looks_like_version("1.2"));
        assert!(!looks_like_version("1.2.x"));
        assert!(!looks_like_version("1.2.3 rc"));
    }

    /// A resolved subscription declares what its model can do. Nothing downstream
    /// infers capability from a model id, so an undeclared set is a session that
    /// refuses images and PDFs, never thinks, and sizes its context window from a
    /// provider default.
    #[test]
    fn codex_refresh_updates_the_only_account_without_rewriting_an_absent_or_stale_default() {
        let root = tempfile::tempdir().unwrap();
        for default in [None, Some("missing")] {
            let account = json!({"accountId":"fixture","accessToken":"old"});
            let mut store = json!({"accounts":{"fixture":account}});
            if let Some(default) = default {
                store["defaultAccountId"] = json!(default);
            }
            write_private_json(&codex_store_path(root.path()), &store).unwrap();
            let mut renewed = account.clone();
            renewed["accessToken"] = json!("fresh");
            write_refreshed_codex_login(root.path(), account.as_object().unwrap(), renewed.clone())
                .unwrap();
            let saved: Value =
                serde_json::from_slice(&std::fs::read(codex_store_path(root.path())).unwrap())
                    .unwrap();
            assert_eq!(saved.get("defaultAccountId"), store.get("defaultAccountId"));
            assert_eq!(
                codex_account(root.path()).unwrap(),
                renewed.as_object().unwrap().clone()
            );
        }
    }

    #[test]
    fn codex_refresh_updates_only_the_unchanged_selected_grant() {
        let root = tempfile::tempdir().unwrap();
        let a = json!({"accountId":"fixture-a","accessToken":"old","refreshToken":"refresh-a"});
        let b = json!({"accountId":"fixture-b","accessToken":"other","refreshToken":"refresh-b"});
        write_codex_login(root.path(), a.clone()).unwrap();
        let mut renewed = a.clone();
        renewed["accessToken"] = json!("new");
        write_refreshed_codex_login(root.path(), a.as_object().unwrap(), renewed.clone()).unwrap();
        assert_eq!(
            codex_account(root.path()).unwrap(),
            renewed.as_object().unwrap().clone()
        );
        write_codex_login(root.path(), b.clone()).unwrap();
        assert!(
            write_refreshed_codex_login(root.path(), a.as_object().unwrap(), renewed.clone())
                .is_err()
        );
        assert_eq!(
            codex_account(root.path()).unwrap(),
            b.as_object().unwrap().clone()
        );
        delete_codex_login(root.path()).unwrap();
        assert!(write_refreshed_codex_login(root.path(), a.as_object().unwrap(), renewed).is_err());
        assert!(!codex_store_path(root.path()).exists());
    }

    #[tokio::test]
    async fn a_claude_subscription_declares_its_capability_set() {
        let root = tempfile::tempdir().expect("temp dir");
        write_claude_login(
            root.path(),
            json!({ "accessToken": "at", "expiresAt": now_ms() + 3_600_000 }),
        )
        .unwrap();

        let opus = resolve("subscription:opus", root.path(), &no_tiers())
            .await
            .unwrap()
            .expect("resolves");
        assert_eq!(opus.capabilities["vision"], json!(true));
        assert_eq!(opus.capabilities["nativePdf"], json!(true));
        assert_eq!(opus.capabilities["promptCacheControl"], json!(true));
        assert_eq!(opus.capabilities["taskBudgets"], json!(true));
        assert_eq!(opus.capabilities["fastMode"], json!(true));
        for model in [
            "claude-fable-5-2",
            "claude-opus-5-5",
            "claude-opus-5",
            "claude-opus-4-8",
        ] {
            assert_eq!(claude_capabilities(model)["fastMode"], json!(true));
        }
        for model in [
            "claude-fable-5-1",
            CLAUDE_SONNET,
            CLAUDE_HAIKU,
            "claude-opus-future",
        ] {
            assert_eq!(claude_capabilities(model)["fastMode"], json!(false));
        }
        assert_eq!(opus.capabilities["contextWindow"], json!(1_000_000));
        assert_eq!(
            opus.capabilities["maxOutputTokens"],
            json!(128_000),
            "the published output ceiling, so a long answer is not cut at the provider default"
        );
        assert_eq!(
            opus.capabilities["effort"]["levels"],
            json!(["low", "medium", "high", "xhigh", "max"])
        );

        // The set follows the CONCRETE id, so a tier word and an alias that name
        // the same model cannot disagree.
        let sonnet = resolve("subscription:medium", root.path(), &no_tiers())
            .await
            .unwrap()
            .expect("resolves");
        assert_eq!(sonnet.provider["model"], json!(CLAUDE_SONNET));
        assert_eq!(sonnet.capabilities["taskBudgets"], json!(true));
        assert_eq!(sonnet.capabilities["maxOutputTokens"], json!(128_000));
        assert_eq!(
            sonnet.capabilities["effort"]["levels"],
            json!(["low", "medium", "high", "xhigh", "max"])
        );

        let haiku = resolve("subscription:haiku", root.path(), &no_tiers())
            .await
            .unwrap()
            .expect("resolves");
        assert_eq!(haiku.provider["model"], json!(CLAUDE_HAIKU));
        assert_eq!(haiku.capabilities["thinking"]["adaptive"], json!(true));
        assert_eq!(haiku.capabilities["taskBudgets"], json!(true));
        assert_eq!(haiku.capabilities["contextWindow"], json!(1_000_000));
        assert_eq!(haiku.capabilities["maxOutputTokens"], json!(128_000));
        assert_eq!(
            haiku.capabilities["effort"]["levels"],
            json!(["low", "medium", "high", "xhigh", "max"])
        );
    }

    /// Tier slots carry concrete ids and their OWN sets for auxiliary work.
    /// The main model needs no entry of its own.
    #[tokio::test]
    async fn claude_tier_slots_carry_ids_with_their_own_capabilities() {
        let root = tempfile::tempdir().expect("temp dir");
        write_claude_login(
            root.path(),
            json!({ "accessToken": "at", "expiresAt": now_ms() + 3_600_000 }),
        )
        .unwrap();
        let resolved = resolve("subscription:sonnet", root.path(), &no_tiers())
            .await
            .unwrap()
            .expect("resolves");

        assert_eq!(resolved.model_tiers["high"], json!(CLAUDE_OPUS));
        assert_eq!(resolved.model_tiers["medium"], json!(CLAUDE_SONNET));
        assert_eq!(resolved.model_tiers["low"], json!(CLAUDE_HAIKU));

        // Sonnet is the main model, so only the other two declare their own sets.
        let by_model: Vec<&str> = resolved
            .capabilities_by_model
            .iter()
            .filter_map(|entry| entry["model"].as_str())
            .collect();
        assert_eq!(by_model, vec![CLAUDE_OPUS, CLAUDE_HAIKU]);
        let haiku = resolved
            .capabilities_by_model
            .iter()
            .find(|entry| entry["model"] == json!(CLAUDE_HAIKU))
            .expect("haiku declares its own set");
        assert_eq!(
            haiku["capabilities"]["effort"]["levels"],
            json!(["low", "medium", "high", "xhigh", "max"])
        );
        assert_eq!(haiku["capabilities"]["vision"], json!(true));
        assert_eq!(haiku["capabilities"]["taskBudgets"], json!(true));
        assert_eq!(haiku["capabilities"]["contextWindow"], json!(1_000_000));
        assert_eq!(haiku["capabilities"]["maxOutputTokens"], json!(128_000));
    }

    /// Remapping a tier in settings moves the MAIN model too, because the source's
    /// composer rows are those same tiers: a pick of Haiku for the top tier means
    /// the top row runs Haiku. A blank pick means "follow the lineup".
    #[tokio::test]
    async fn a_remapped_tier_moves_the_main_model_as_well_as_the_slot() {
        let root = tempfile::tempdir().expect("temp dir");
        write_claude_login(
            root.path(),
            json!({ "accessToken": "at", "expiresAt": now_ms() + 3_600_000 }),
        )
        .unwrap();
        let resolved = resolve(
            "subscription:opus",
            root.path(),
            &tiers(&[("high", "haiku"), ("medium", "")]),
        )
        .await
        .unwrap()
        .expect("resolves");

        assert_eq!(resolved.provider["model"], json!(CLAUDE_HAIKU));
        assert_eq!(
            resolved.capabilities["effort"]["levels"],
            json!(["low", "medium", "high", "xhigh", "max"]),
            "the main set follows the model that won: {}",
            resolved.capabilities
        );
        assert_eq!(resolved.model_tiers["high"], json!(CLAUDE_HAIKU));
        // A blank pick means "follow the lineup", not "no tier".
        assert_eq!(resolved.model_tiers["medium"], json!(CLAUDE_SONNET));
        assert_eq!(resolved.model_tiers["low"], json!(CLAUDE_HAIKU));
        // Haiku is now the main model, so it declares no separate entry.
        let by_model: Vec<&str> = resolved
            .capabilities_by_model
            .iter()
            .filter_map(|entry| entry["model"].as_str())
            .collect();
        assert_eq!(by_model, vec![CLAUDE_SONNET]);
    }

    /// Codex: the Responses ladder, image and PDF input, and the rates the engine
    /// has no table for — without them every Codex turn is billed at zero.
    #[tokio::test]
    async fn a_codex_subscription_declares_its_ladder_and_rates() {
        let root = tempfile::tempdir().expect("temp dir");
        write_codex_login(
            root.path(),
            json!({
                "accountId": "acct-1",
                "accessToken": "at",
                "refreshToken": "rt",
                "accessTokenExpiresAt": now_ms() + 3_600_000,
            }),
        )
        .unwrap();
        let resolved = resolve("codex:gpt-5.6-sol", root.path(), &no_tiers())
            .await
            .unwrap()
            .expect("resolves");

        assert_eq!(resolved.capabilities["vision"], json!(true));
        assert_eq!(resolved.capabilities["nativePdf"], json!(true));
        assert_eq!(resolved.capabilities["webSearch"], json!(true));
        assert_eq!(resolved.capabilities["reasoningDialect"], json!("openai"));
        assert_eq!(
            resolved.capabilities["effort"]["levels"],
            json!(["low", "medium", "high", "xhigh", "max"]),
            "the ladder reaches the top rung this generation answers to"
        );
        let pricing = resolved.pricing.expect("the codex wire carries rates");
        assert_eq!(pricing[0]["model"], json!("gpt-5.6-sol"));
        // The published standard short-context rates. A stale table bills every
        // turn wrong, and the usage panel reports whatever is here.
        assert_eq!(pricing[0]["inputPerMTok"], json!(4.0));
        assert_eq!(pricing[0]["outputPerMTok"], json!(20.0));
        assert_eq!(pricing[0]["cacheReadPerMTok"], json!(0.4));
        assert_eq!(pricing[0]["cacheWritePerMTok"], json!(5.0));
    }

    /// The GPT-6 generation's rates, checked against the pricing page rather than
    /// scaled off the 5.6 line: they are not proportional to it (Astra costs 2.5x
    /// 5.6 Sol, Sol is half of it), and a model the table misses is billed at zero
    /// by the engine's own fallback, which the usage panel then reports as free.
    #[test]
    fn every_gpt_6_model_is_priced_at_its_published_rate() {
        for (model, input, output, cache_read, cache_write) in [
            ("gpt-6.1-sol", 2.0, 10.0, 0.1, 2.5),
            ("gpt-6-astra", 10.0, 50.0, 1.0, 12.5),
            ("gpt-6-sol", 2.0, 10.0, 0.2, 2.5),
            ("gpt-6-luna", 0.1, 0.5, 0.01, 0.125),
        ] {
            let priced = codex_pricing(model).unwrap_or_else(|| panic!("{model} is unpriced"));
            assert_eq!(priced[0]["model"], json!(model));
            assert_eq!(priced[0]["inputPerMTok"], json!(input), "{model}");
            assert_eq!(priced[0]["outputPerMTok"], json!(output), "{model}");
            assert_eq!(priced[0]["cacheReadPerMTok"], json!(cache_read), "{model}");
            assert_eq!(
                priced[0]["cacheWritePerMTok"],
                json!(cache_write),
                "{model}"
            );
        }
        // Casing is the caller's; the lookup is not case-sensitive.
        assert_eq!(
            codex_pricing(" GPT-6-Sol ").unwrap()[0]["inputPerMTok"],
            json!(2.0)
        );
        // An unknown id stays unpriced instead of inheriting a neighbour's rate.
        assert!(codex_pricing("gpt-6-nova").is_none());
    }

    /// Every tier default has to be a model the engine can price.
    ///
    /// `codex_tiers` drops a default the catalog does not list, so a default that
    /// drifted off the lineup fails SILENTLY — that tier simply stops having a
    /// model, and auxiliary work quietly falls back to the main one. Pricing is the
    /// proxy for "this id is real here": a default naming a retired or misspelled
    /// model is exactly what this catches, and it fails without a network call.
    #[test]
    fn every_tier_default_names_a_priced_model() {
        for (tier, model) in CODEX_TIER_DEFAULTS {
            assert!(
                codex_pricing(model).is_some(),
                "the {tier} tier defaults to {model}, which this engine cannot price — \
                 either the id drifted off the lineup or the price table lost its row"
            );
        }
        // The flagship is deliberately absent: a tier is a DEFAULT, so promoting
        // Astra would re-price every sub-agent and summary by five times.
        assert!(
            !CODEX_TIER_DEFAULTS.iter().any(|(_, m)| *m == "gpt-6-astra"),
            "Astra must stay a deliberate pick, not an auxiliary-work default"
        );
    }

    #[tokio::test]
    async fn codex_tiers_without_a_catalog_pass_only_explicit_picks() {
        let root = tempfile::tempdir().expect("temp dir");
        let (bare, bare_capabilities) =
            codex_tiers(CODEX_FALLBACK_MODEL, &no_tiers(), root.path()).await;
        assert!(bare.is_empty(), "no catalog, no defaults: {bare:?}");
        assert!(bare_capabilities.is_empty());

        let (picked, capabilities) = codex_tiers(
            CODEX_FALLBACK_MODEL,
            &tiers(&[("low", "gpt-5.6-luna")]),
            root.path(),
        )
        .await;
        assert_eq!(picked["low"], json!("gpt-5.6-luna"));
        assert_eq!(picked.len(), 1);
        assert_eq!(capabilities.len(), 1);
        assert_eq!(capabilities[0]["model"], json!("gpt-5.6-luna"));
        assert_eq!(capabilities[0]["capabilities"]["vision"], json!(true));
    }

    /// The bundled catalog supplies defaults before a live catalog is available;
    /// an explicit pick replaces its tier without dropping the other defaults.
    #[tokio::test]
    async fn codex_tiers_use_bundled_defaults_and_preserve_explicit_picks() {
        let root = tempfile::tempdir().expect("temp dir");
        write_codex_login(
            root.path(),
            json!({
                "accountId": "acct-1",
                "accessToken": "at",
                "refreshToken": "rt",
                "accessTokenExpiresAt": now_ms() + 3_600_000,
            }),
        )
        .unwrap();

        let bare = resolve("codex:auto", root.path(), &no_tiers())
            .await
            .unwrap()
            .expect("resolves");
        let defaults = serde_json::from_value::<Map<String, Value>>(json!({
            "high": "gpt-6-sol",
            "medium": "gpt-5.6-terra",
            "low": "gpt-6-luna"
        }))
        .unwrap();
        assert_eq!(bare.provider["model"], json!(CODEX_DEFAULT_MAIN_MODEL));
        assert_eq!(bare.model_tiers, defaults);
        let full_catalog = super::super::codex_catalog::all_models(root.path())
            .await
            .unwrap();
        assert_eq!(bare.capabilities_by_model.len(), full_catalog.len() - 1);
        assert!(bare
            .capabilities_by_model
            .iter()
            .any(|entry| entry["model"] == "codex-auto-review"));
        assert_eq!(
            bare.capabilities_by_model
                .iter()
                .take(3)
                .map(|entry| entry["model"].clone())
                .collect::<Vec<_>>(),
            vec![
                json!("gpt-6-sol"),
                json!("gpt-5.6-terra"),
                json!("gpt-6-luna")
            ]
        );

        let picked = resolve(
            "codex:auto",
            root.path(),
            &tiers(&[("low", "gpt-5.6-luna")]),
        )
        .await
        .unwrap()
        .expect("resolves");
        assert_eq!(picked.model_tiers["low"], json!("gpt-5.6-luna"));
        let mut expected = defaults;
        expected.insert("low".into(), json!("gpt-5.6-luna"));
        assert_eq!(picked.model_tiers, expected);
        assert_eq!(picked.capabilities_by_model.len(), full_catalog.len() - 1);
        assert_eq!(
            picked
                .capabilities_by_model
                .iter()
                .take(3)
                .map(|entry| entry["model"].clone())
                .collect::<Vec<_>>(),
            vec![
                json!("gpt-6-sol"),
                json!("gpt-5.6-terra"),
                json!("gpt-5.6-luna")
            ]
        );
        let low = picked
            .capabilities_by_model
            .iter()
            .find(|model| model["model"] == "gpt-5.6-luna")
            .expect("the explicit low model carries its own capabilities");
        assert_eq!(low["capabilities"]["vision"], json!(true));
    }

    /// A multi-account Claude credential file resolves with a stable per-account
    /// namespace and carries the queue so the engine can rotate on quota
    /// refusals; a legacy single-key file keeps the token-hash namespace.
    #[tokio::test]
    async fn claude_multi_account_credential_carries_the_rotation_queue() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("claude-subscription-oauth.json");
        std::fs::write(
            &path,
            serde_json::to_string_pretty(&json!({
                "accounts": [
                    { "id": "acct-1", "tokens": { "accessToken": "access-1", "refreshToken": "refresh-1" } },
                    { "id": "acct-2", "tokens": { "accessToken": "access-2", "refreshToken": "refresh-2" } },
                ]
            }))
            .unwrap(),
        )
        .unwrap();
        let resolved = resolve("subscription:auto", root.path(), &no_tiers())
            .await
            .unwrap()
            .expect("resolves");
        let auth = resolved.auth.as_object().unwrap();
        assert_eq!(auth["accountId"], json!("acct-1"));
        assert_eq!(
            auth["credentialId"],
            json!(oauth_credential_id("subscription", "refresh-1"))
        );
        let rotation = auth["rotation"]
            .as_object()
            .expect("queue travels with the auth");
        assert_eq!(
            rotation["filePath"],
            json!(path.to_string_lossy().to_string())
        );
        let ids: Vec<&str> = rotation["accounts"]
            .as_array()
            .unwrap()
            .iter()
            .map(|entry| entry["id"].as_str().unwrap())
            .collect();
        assert_eq!(ids, vec!["acct-1", "acct-2"]);
        assert_eq!(
            rotation["accounts"][0]["credentialId"],
            json!(oauth_credential_id("subscription", "refresh-1"))
        );

        // The historical single-key spelling keeps its token-hash namespace so
        // refreshed tokens of an old login stay reachable.
        std::fs::write(
            &path,
            serde_json::to_string_pretty(&json!({
                "claudeAiOauth": { "accessToken": "solo-access", "refreshToken": "solo-refresh" }
            }))
            .unwrap(),
        )
        .unwrap();
        let solo = resolve("subscription:auto", root.path(), &no_tiers())
            .await
            .unwrap()
            .expect("resolves");
        let auth = solo.auth.as_object().unwrap();
        assert_eq!(auth["accountId"], json!("legacy"));
        assert_eq!(
            auth["credentialId"],
            json!(oauth_credential_id("subscription", "solo-refresh"))
        );
        assert!(auth.get("rotation").is_none(), "one account has no queue");
    }
    #[test]
    fn unknown_models_never_inherit_a_neighboring_reasoning_ladder() {
        assert!(claude_capabilities("claude-future-unmeasured")
            .get("effort")
            .is_none());
        assert!(
            codex_capabilities(super::super::codex_catalog::ModelLimits::default())
                .get("effort")
                .is_none()
        );
        assert!(claude_capabilities(CLAUDE_OPUS).get("effort").is_some());
    }
}
