//! Native Qoder COSY request signing.
//!
//! Qoder authenticates a request with three cooperating pieces:
//!
//!   * `Cosy-Key` — a 16-byte AES key, RSA-PKCS1v15-encrypted under Qoder's
//!     pinned public key and base64'd. Only the server can recover it.
//!   * the signing payload — base64 of `{version,requestId,info,cosyVersion,
//!     ideVersion}`, where `info` is the account profile encrypted with that
//!     same AES key. This is how the server learns *who* is calling.
//!   * `Authorization: Bearer COSY.<payload>.<signature>`, the signature being
//!     md5 over `payload \n key \n date \n body \n path`.
//!
//! The `(info, key)` pair is derived once per credential (login, adopt, token
//! refresh) and stored beside the profile as `encrypt_user_info` / `key`; only
//! the payload and signature are rebuilt per request. Qoder seals its access
//! token and identity fields into `info`; QwenWork seals the access token
//! alongside the account's organization and data-policy context. Both shapes are
//! pinned below rather than being treated as interchangeable just because the
//! outer COSY envelope is shared.
//!
//! This replaces executing Qoder's own `qoder_auth` wasm module. That module
//! ships *inside an installed qodercli*, which meant the Qoder provider could
//! not sign anything on a machine that has the Qoder IDE and no CLI — the
//! ordinary case. The construction below was read out of Qoder's plain-JS signer
//! rather than guessed, and the vectors at the bottom pin the exact wire bytes.

use serde_json::Value;

/// Qoder's pinned RSA public key, the one its own client carries. `Cosy-Key` is
/// this key applied to the per-credential AES key, so the server can decrypt
/// `info` without the AES key ever crossing the wire.
const PUBLIC_KEY_PEM: &str = "-----BEGIN PUBLIC KEY-----
MIGfMA0GCSqGSIb3DQEBAQUAA4GNADCBiQKBgQDA8iMH5c02LilrsERw9t6Pv5Nc
4k6Pz1EaDicBMpdpxKduSZu5OANqUq8er4GM95omAGIOPOh+Nx0spthYA2BqGz+l
6HRkPJ7S236FZz73In/KVuLnwI8JJ2CbuJap8kvheCCZpmAWpb/cPx/3Vr/J6I17
XcW+ML9FoCI6AOvOzwIDAQAB
-----END PUBLIC KEY-----";

/// The `/algo` gateway prefix every signed Qoder route carries. The signature is
/// computed over the path *without* it, because the gateway strips it before the
/// service behind it sees the request.
const GATEWAY_PREFIX: &str = "/algo";

/// The catalog route carries no `Encode=1`: it answers plain JSON and there is
/// nothing a request body could trip over. The inference and image routes both
/// carry it, for two different reasons. The image route REQUIRES the envelope
/// (400 without `Encode`). The inference route accepts plain JSON, but must not
/// send it: a WAF sits in front of the gateway and pattern-matches request
/// bodies for command injection, while an agent transcript is by nature full of
/// shell text (`ls -la /x`, `a; echo b`, `cat /etc/passwd`). A plaintext body
/// carrying any of it is refused with a 405 HTML page before the model sees the
/// request, and permanently so — the offending text stays in the transcript, so
/// every later turn of that session resends it and is refused again. The
/// `Encode=1` envelope leaves no matchable token on the wire.
pub(crate) const MODEL_LIST_PATH: &str = "/algo/api/v2/model/list";
pub(crate) const INFER_PATH: &str =
    "/algo/api/v2/service/pro/sse/agent_chat_generation?FetchKeys=llm_model_result&AgentId=agent_common&Encode=1";

/// The `Encode=1` request-body envelope. Recovered from Qoder's own auth module
/// by driving its `prepareRequest`/`decryptServerResponse` pair with chosen
/// inputs until the construction below reproduced every observed vector
/// bit-for-bit. The probe script that produced those vectors lived in the product
/// repo's retired Tauri shell (`apps/tauri/scripts/probe-qoder-image-route.mjs`,
/// readable only through that repo's git history). The construction:
///
///   1. standard base64 of the UTF-8 body (padding included),
///   2. each symbol mapped through a custom alphabet (`=` renders as `$`),
///   3. the symbol stream rotated by thirds: with N symbols and
///      T = floor(N/3), the output is `tail T + middle + head T`.
///
/// The signature then covers the ENCODED body, since that is what goes on the
/// wire.
const ENCODE_ALPHABET: &[u8; 64] =
    b"_doRTgHZBKcGVjlvpC,@aFSx#DPuNJme&i*MzLOEn)sUrthbf%Y^w.(kIQyXqWA!";

/// Encode a plaintext JSON body into the `Encode=1` wire envelope.
pub(crate) fn encode_body(body: &str) -> String {
    use base64::Engine as _;
    let standard = base64::engine::general_purpose::STANDARD.encode(body.as_bytes());
    let symbols: Vec<char> = standard
        .chars()
        .map(|c| match c {
            '=' => '$',
            'A'..='Z' => ENCODE_ALPHABET[(c as u8 - b'A') as usize] as char,
            'a'..='z' => ENCODE_ALPHABET[(c as u8 - b'a' + 26) as usize] as char,
            '0'..='9' => ENCODE_ALPHABET[(c as u8 - b'0' + 52) as usize] as char,
            '+' => ENCODE_ALPHABET[62] as char,
            '/' => ENCODE_ALPHABET[63] as char,
            other => other,
        })
        .collect();
    // Swap the head and tail thirds (the middle stays put) — a swap, not a
    // rotation: out = tail + middle + head.
    let n = symbols.len();
    let t = n / 3;
    let mut out = Vec::with_capacity(n);
    out.extend_from_slice(&symbols[n - t..]);
    out.extend_from_slice(&symbols[t..n - t]);
    out.extend_from_slice(&symbols[..t]);
    out.into_iter().collect()
}

/// Decode the response envelope used by QwenWork's `Encode=1` catalog route.
///
/// The official client applies the same head/tail-third swap in reverse, maps
/// the private alphabet back to standard base64, then decodes UTF-8. Keep this
/// beside [`encode_body`] so the two wire halves cannot drift independently.
pub(crate) fn decode_server_response(body: &str) -> Result<String, String> {
    use base64::Engine as _;

    let symbols: Vec<char> = body.chars().collect();
    let n = symbols.len();
    let t = n / 3;
    let mut unshuffled = Vec::with_capacity(n);
    unshuffled.extend_from_slice(&symbols[n - t..]);
    unshuffled.extend_from_slice(&symbols[t..n - t]);
    unshuffled.extend_from_slice(&symbols[..t]);
    let standard = unshuffled
        .into_iter()
        .map(|symbol| {
            if symbol == '$' {
                return Ok('=');
            }
            let index = ENCODE_ALPHABET
                .iter()
                .position(|candidate| *candidate as char == symbol)
                .ok_or_else(|| format!("invalid encoded response symbol {symbol:?}"))?;
            Ok(match index {
                0..=25 => (b'A' + index as u8) as char,
                26..=51 => (b'a' + (index - 26) as u8) as char,
                52..=61 => (b'0' + (index - 52) as u8) as char,
                62 => '+',
                63 => '/',
                _ => unreachable!(),
            })
        })
        .collect::<Result<String, String>>()?;
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(standard)
        .map_err(|error| error.to_string())?;
    String::from_utf8(bytes).map_err(|error| error.to_string())
}

/// The COSY client identity is part of upstream routing, not presentation. Qoder
/// and QwenWork share the signer and inference dialect, but their gateways route
/// different product/scene pairs.
#[derive(Clone, Copy)]
pub(crate) struct CosyProfile {
    version: &'static str,
    business_product: &'static str,
    scene: &'static str,
    data_policy: &'static str,
    qwenwork_wire: bool,
}

impl CosyProfile {
    pub(crate) const QODER: Self = Self {
        version: "1.1.5",
        business_product: "cli",
        scene: "assistant",
        data_policy: "agree",
        qwenwork_wire: false,
    };

    pub(crate) const QWENWORK: Self = Self {
        version: "1.1.32",
        business_product: "qoder_work",
        scene: "qwork",
        data_policy: "disagree",
        qwenwork_wire: true,
    };
}
/// `Cosy-MachineType`, the desktop-client machine class in Qoder's own signer.
const MACHINE_TYPE: &str = "5";

/// The per-credential pair the signer needs. Regenerated whenever the token
/// changes, because the token is sealed inside `encrypt_user_info`.
pub(crate) struct RuntimeAuth {
    pub encrypt_user_info: String,
    pub key: String,
}

/// A request with its signed URL, headers and (for POSTs) the exact body text
/// the signature was computed over.
#[derive(Clone)]
pub struct SignedRequest {
    pub url: String,
    pub headers: Vec<(String, String)>,
    pub body: Option<String>,
}

/// Debug is hand-written and redacting: the header list carries the bearer token
/// and the wrapped session key, so a derived Debug would put both into any log
/// line that ever formats a signed request with `{:?}`.
impl std::fmt::Debug for SignedRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        const REDACTED: &[&str] = &[
            "authorization",
            "cosy-key",
            "cosy-user",
            "cosy-clientip",
            "cosy-machineid",
            "cosy-machinetoken",
            "cosy-organization-id",
            "cosy-organization-tags",
        ];
        let headers: Vec<(&str, &str)> = self
            .headers
            .iter()
            .map(|(name, value)| {
                let shown = if REDACTED
                    .iter()
                    .any(|secret| name.eq_ignore_ascii_case(secret))
                {
                    "[redacted]"
                } else {
                    value.as_str()
                };
                (name.as_str(), shown)
            })
            .collect();
        f.debug_struct("SignedRequest")
            .field("url", &self.url)
            .field("headers", &headers)
            .field("body_len", &self.body.as_ref().map(String::len))
            .finish()
    }
}

impl SignedRequest {
    /// A case-insensitive header lookup. Only the signing vectors need it; the
    /// real callers forward the whole list untouched.
    #[cfg(test)]
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(key, _)| key.eq_ignore_ascii_case(name))
            .map(|(_, value)| value.as_str())
    }
}

/// The profile fields Qoder seals into `info`, in signing order. `aid` is always
/// empty in Qoder's own client; it is kept because the server reads a fixed shape
/// and a missing key is not the same as an empty one.
fn sealed_profile_json(user_info: &Value) -> String {
    ordered_json_object(&[
        ("uid", &string_field(user_info, "uid")),
        (
            "security_oauth_token",
            &string_field(user_info, "security_oauth_token"),
        ),
        ("name", &string_field(user_info, "name")),
        ("aid", ""),
        ("email", &string_field(user_info, "email")),
    ])
}

/// QwenWork seals the OAuth token alongside the account's organization and
/// data-policy context. Keep the boolean as a boolean and preserve the
/// organization-tag JSON value: changing either into a display string produces a
/// valid-looking blob that upstream cannot associate with the selected
/// enterprise identity, and omitting `security_oauth_token` is rejected by
/// `gateway.qwenwork.cn` with HTTP 401 code 105 (`Login required`).
fn qwenwork_sealed_profile_json(user_info: &Value) -> String {
    let mut fields = vec![("uid", Value::String(string_field(user_info, "uid")))];
    let token = string_field(user_info, "security_oauth_token");
    let token = if token.is_empty() {
        string_field(user_info, "access_token")
    } else {
        token
    };
    if !token.is_empty() {
        fields.push(("security_oauth_token", Value::String(token)));
    }
    if let Some(organization_id) = user_info
        .get("organization_id")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
    {
        fields.push((
            "organization_id",
            Value::String(organization_id.to_string()),
        ));
    }
    if let Some(organization_tags) = user_info
        .get("organization_tags")
        .filter(|value| !value.is_null())
    {
        fields.push(("organization_tags", organization_tags.clone()));
    }
    if let Some(data_policy_agreed) = user_info.get("data_policy_agreed").and_then(Value::as_bool) {
        fields.push(("data_policy_agreed", Value::Bool(data_policy_agreed)));
    }
    ordered_json_values(&fields)
}

/// A JSON object of string fields, in the ORDER GIVEN.
///
/// Both of Qoder's sealed payloads have load-bearing field order — the signature
/// is md5 over the base64 of the exact text — so the order has to be a property of
/// the call, not of a map's iteration or of whether some crate feature happens to
/// preserve insertion order. Values go through `serde_json` so escaping is
/// serde's and not hand-rolled.
///
/// A serde struct with `#[derive(Serialize)]` would pin the order too, and used
/// to. It cannot live here: this module is inside the app-server facade, where
/// every serde type is part of the exported schema bundle (a repository guard test
/// enforces it), and these two payloads belong to QODER's wire, not to the
/// facade's. Publishing a schema for a sealed credential blob would describe a
/// surface no client of this engine can call.
fn ordered_json_object(fields: &[(&str, &str)]) -> String {
    let mut out = String::from("{");
    for (index, (key, value)) in fields.iter().enumerate() {
        if index > 0 {
            out.push(',');
        }
        out.push_str(&Value::String((*key).to_string()).to_string());
        out.push(':');
        out.push_str(&Value::String((*value).to_string()).to_string());
    }
    out.push('}');
    out
}

fn ordered_json_values(fields: &[(&str, Value)]) -> String {
    let mut out = String::from("{");
    for (index, (key, value)) in fields.iter().enumerate() {
        if index > 0 {
            out.push(',');
        }
        out.push_str(&Value::String((*key).to_string()).to_string());
        out.push(':');
        out.push_str(&value.to_string());
    }
    out.push('}');
    out
}

fn string_field(user_info: &Value, key: &str) -> String {
    match user_info.get(key) {
        Some(Value::String(text)) => text.clone(),
        Some(Value::Number(number)) => number.to_string(),
        _ => String::new(),
    }
}

fn string_array_header(user_info: &Value, key: &str) -> String {
    user_info
        .get(key)
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .collect::<Vec<_>>()
        .join(",")
}

/// Seal the account profile under a fresh AES key and hand back the pair the
/// signer stores, in the form the Qoder service decrypts.
pub(crate) fn generate_runtime_auth(user_info: &Value) -> Result<RuntimeAuth, String> {
    generate_runtime_auth_with_profile(user_info, CosyProfile::QODER)
}

pub(crate) fn generate_runtime_auth_with_profile(
    user_info: &Value,
    profile: CosyProfile,
) -> Result<RuntimeAuth, String> {
    use aes::cipher::{block_padding::Pkcs7, BlockEncryptMut, KeyIvInit};
    use base64::Engine as _;
    use rsa::pkcs8::DecodePublicKey;

    // 16 hex characters, the same alphabet and width Qoder's client draws from
    // (it slices a UUID). The key doubles as the IV, which is Qoder's choice.
    let aes_key: String = super::random_bytes(8)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    let key_bytes = aes_key.as_bytes();

    let plaintext = if profile.qwenwork_wire {
        qwenwork_sealed_profile_json(user_info)
    } else {
        sealed_profile_json(user_info)
    };
    let encrypted = cbc::Encryptor::<aes::Aes128>::new(key_bytes.into(), key_bytes.into())
        .encrypt_padded_vec_mut::<Pkcs7>(plaintext.as_bytes());

    let public_key = rsa::RsaPublicKey::from_public_key_pem(PUBLIC_KEY_PEM)
        .map_err(|error| format!("Qoder public key does not parse: {error}"))?;
    // PKCS1v15 padding needs a CSPRNG and takes one directly here; the desktop
    // client had to wrap its own because the release candidate it built against
    // moved to the fallible rand_core traits.
    let wrapped = public_key
        .encrypt(&mut rand::thread_rng(), rsa::Pkcs1v15Encrypt, key_bytes)
        .map_err(|error| format!("wrapping the session key failed: {error}"))?;

    Ok(RuntimeAuth {
        encrypt_user_info: base64::engine::general_purpose::STANDARD.encode(encrypted),
        key: base64::engine::general_purpose::STANDARD.encode(wrapped),
    })
}

/// A signing context built from one stored profile. Cheap to hold: it is a few
/// strings, so it is rebuilt whenever the credential changes rather than being
/// invalidated in place.
pub(crate) struct SigningContext {
    machine_id: String,
    client_type: String,
    uid: String,
    organization_id: String,
    organization_tags: String,
    data_policy: String,
    encrypt_user_info: String,
    key: String,
    profile: CosyProfile,
}

impl SigningContext {
    /// `user_info` is Klaus's stored profile. A profile that has not been run
    /// through `generate_runtime_auth` cannot sign, and saying so here beats
    /// emitting a request the upstream will reject as unauthenticated.
    pub(crate) fn new(
        machine_id: &str,
        client_type: &str,
        user_info: &Value,
    ) -> Result<Self, String> {
        Self::new_with_profile(machine_id, client_type, user_info, CosyProfile::QODER)
    }

    pub(crate) fn new_with_profile(
        machine_id: &str,
        client_type: &str,
        user_info: &Value,
        profile: CosyProfile,
    ) -> Result<Self, String> {
        let encrypt_user_info = string_field(user_info, "encrypt_user_info");
        let key = string_field(user_info, "key");
        if encrypt_user_info.is_empty() || key.is_empty() {
            return Err("Qoder profile carries no signing material; sign in again".into());
        }
        Ok(Self {
            machine_id: machine_id.to_string(),
            client_type: client_type.to_string(),
            uid: string_field(user_info, "uid"),
            organization_id: string_field(user_info, "organization_id"),
            organization_tags: string_array_header(user_info, "organization_tags"),
            data_policy: if profile.qwenwork_wire
                && user_info.get("data_policy_agreed").and_then(Value::as_bool) == Some(true)
            {
                "agree".into()
            } else {
                profile.data_policy.into()
            },
            encrypt_user_info,
            key,
            profile,
        })
    }

    /// The signed GET the model catalog needs.
    pub(crate) fn sign_get(&self, endpoint: &str, path: &str) -> SignedRequest {
        let url = format!("{}{}", endpoint.trim_end_matches('/'), path);
        let headers = self.headers(&url, "", None);
        SignedRequest {
            url,
            headers,
            body: None,
        }
    }

    /// The signed inference POST. `INFER_PATH` asks for `Encode=1`, so the wire
    /// body is the obfuscated envelope and the signature proves those encoded
    /// bytes. Encoding here, next to the URL that demands it, is what keeps the
    /// flag and the body from ever drifting apart.
    pub(crate) fn sign_infer(
        &self,
        endpoint: &str,
        body: &str,
        model_key: &str,
        model_source: &str,
    ) -> SignedRequest {
        let url = format!("{}{}", endpoint.trim_end_matches('/'), INFER_PATH);
        let body = encode_body(body);
        let mut headers = self.headers(&url, &body, Some((model_key, model_source)));
        // SSE, and explicitly un-buffered: an intermediary that decides to
        // accumulate this response turns streaming into one late blob.
        headers.push(("Accept".into(), "text/event-stream".into()));
        headers.push(("Cache-Control".into(), "no-cache".into()));
        headers.push(("Connection".into(), "keep-alive".into()));
        SignedRequest {
            url,
            headers,
            body: Some(body),
        }
    }

    /// A signed plain-JSON POST with the full URL already assembled (the
    /// image route's URL is built by [`super::catalog`] so probes can override it
    /// whole). The signature covers the same (payload, key, date, body, url)
    /// tuple as every other route, so the path is part of the proof.
    pub(crate) fn sign_post_url(&self, url: &str, body: &str) -> SignedRequest {
        let headers = self.headers(url, body, None);
        SignedRequest {
            url: url.to_string(),
            headers,
            body: Some(body.to_string()),
        }
    }

    /// Everything common to both routes, including the signature itself.
    fn headers(&self, url: &str, body: &str, model: Option<(&str, &str)>) -> Vec<(String, String)> {
        let date = chrono::Utc::now().timestamp().to_string();
        let request_id = super::uuid_v4();
        let payload = self.auth_payload(&request_id);
        let signature = sign(&payload, &self.key, &date, body, url);

        let mut headers = vec![
            ("Content-Type".to_string(), "application/json".to_string()),
            (
                "Authorization".to_string(),
                format!("Bearer COSY.{payload}.{signature}"),
            ),
            ("Cosy-User".to_string(), self.uid.clone()),
            ("Cosy-Key".to_string(), self.key.clone()),
            ("Cosy-Date".to_string(), date),
            ("Cosy-Version".to_string(), self.profile.version.to_string()),
            ("Cosy-MachineId".to_string(), self.machine_id.clone()),
            ("Cosy-MachineToken".to_string(), self.machine_id.clone()),
            ("Cosy-MachineType".to_string(), MACHINE_TYPE.to_string()),
            ("Cosy-ClientType".to_string(), self.client_type.clone()),
            (
                "Cosy-Business-Product".to_string(),
                self.profile.business_product.to_string(),
            ),
            ("Cosy-Business-Type".to_string(), "agent".to_string()),
            ("Cosy-Scene".to_string(), self.profile.scene.to_string()),
            ("Cosy-Data-Policy".to_string(), self.data_policy.clone()),
            ("Login-Version".to_string(), "v2".to_string()),
        ];
        if self.profile.qwenwork_wire {
            headers.push(("Cosy-ClientIp".to_string(), self.machine_id.clone()));
        }
        // Only sent for an account that actually belongs to an organization; an
        // empty header here is not the same as an absent one upstream.
        if !self.organization_id.is_empty() {
            headers.push((
                "Cosy-Organization-Id".to_string(),
                self.organization_id.clone(),
            ));
        }
        if self.profile.qwenwork_wire && !self.organization_tags.is_empty() {
            headers.push((
                "Cosy-Organization-Tags".to_string(),
                self.organization_tags.clone(),
            ));
        }
        if let Some((model_key, model_source)) = model {
            headers.push(("X-Model-Key".to_string(), model_key.to_string()));
            headers.push(("X-Model-Source".to_string(), model_source.to_string()));
        }
        headers
    }

    fn auth_payload(&self, request_id: &str) -> String {
        use base64::Engine as _;
        let text = ordered_json_object(&[
            ("version", "v1"),
            ("requestId", request_id),
            ("info", &self.encrypt_user_info),
            ("cosyVersion", self.profile.version),
            // Deliberately empty: Klaus is not an IDE plugin, and Qoder's own
            // signer leaves this blank for non-plugin clients.
            ("ideVersion", ""),
        ]);
        base64::engine::general_purpose::STANDARD.encode(text)
    }
}

/// Sign a bodyless audio connection without reusing the inference wire profile.
pub(crate) fn sign_audio_get(url: &str, user_info: &Value) -> Result<SignedRequest, String> {
    use base64::Engine as _;
    let runtime = generate_runtime_auth(user_info)?;
    let request_id = uuid::Uuid::new_v4().simple().to_string();
    let date = chrono::Utc::now().timestamp().to_string();
    let version = env!("CARGO_PKG_VERSION");
    let payload = base64::engine::general_purpose::STANDARD.encode(ordered_json_object(&[
        ("version", "v1"),
        ("requestId", &request_id),
        ("info", &runtime.encrypt_user_info),
        ("cosyVersion", "1.0.0"),
        ("ideVersion", version),
    ]));
    let signature = sign(&payload, &runtime.key, &date, "", url);
    Ok(SignedRequest {
        url: url.to_string(),
        headers: vec![
            ("X-Request-Id".into(), request_id),
            ("X-IDE-Platform".into(), "Klaus".into()),
            ("X-Version".into(), version.into()),
            (
                "X-Machine-OS".into(),
                std::env::consts::OS.replace("macos", "darwin"),
            ),
            ("X-Machine-Id".into(), "unknown".into()),
            ("X-Machine-Type".into(), "desktop".into()),
            ("Cosy-User".into(), string_field(user_info, "uid")),
            ("Cosy-Key".into(), runtime.key),
            ("Cosy-Date".into(), date),
            (
                "Authorization".into(),
                format!("Bearer COSY.{payload}.{signature}"),
            ),
        ],
        body: None,
    })
}

/// The path the signature covers: no origin, no query, and no `/algo` — the
/// gateway strips that prefix, so signing the un-stripped path would sign
/// something the verifying service never sees.
fn signing_path(url: &str) -> String {
    let path = match url.find("://") {
        Some(scheme) => match url[scheme + 3..].find('/') {
            Some(offset) => &url[scheme + 3 + offset..],
            None => "/",
        },
        None => url,
    };
    let path = path.split(['?', '#']).next().unwrap_or(path);
    path.strip_prefix(GATEWAY_PREFIX)
        .unwrap_or(path)
        .to_string()
}

/// md5 over the newline-joined signing input. Qoder's construction, verbatim.
fn sign(payload: &str, key: &str, date: &str, body: &str, url: &str) -> String {
    use md5::Digest as _;

    let path = signing_path(url);
    let input = format!("{payload}\n{key}\n{date}\n{body}\n{path}");
    // Lower-case hex of the digest, which is what `md5::compute`'s `LowerHex` also
    // produced: the digest bytes are the contract, not the crate that made them.
    md5::Md5::digest(input.as_bytes())
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// Entirely synthetic: a placeholder signing pair, so no real credential
    /// ever enters a test.
    fn fake_profile() -> Value {
        json!({
            "uid": "fake-uid-0001",
            "name": "Fake Tester",
            "email": "fake@example.invalid",
            "security_oauth_token": "fake.access.token",
            "encrypt_user_info": "FAKE_ENCRYPTED_USER_INFO_BLOB",
            "key": "FAKE_SIGNING_KEY_0123456789abcdef",
            "organization_id": "",
        })
    }

    /// The signature input, pinned. Every field of it is part of the wire
    /// contract, so this vector is what catches an accidental reordering,
    /// a stray query string or a re-added `/algo`.
    #[test]
    fn signature_matches_the_pinned_vector() {
        let payload = "eyJ2ZXJzaW9uIjoidjEifQ==";
        let signature = sign(
            payload,
            "FAKE_SIGNING_KEY_0123456789abcdef",
            "1750000000",
            "",
            "https://api3.qoder.sh/algo/api/v2/model/list",
        );
        let expected: String = {
            use md5::Digest as _;
            md5::Md5::digest(
                "eyJ2ZXJzaW9uIjoidjEifQ==\nFAKE_SIGNING_KEY_0123456789abcdef\n1750000000\n\n/api/v2/model/list"
                    .as_bytes(),
            )
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect()
        };
        assert_eq!(signature, expected);
        // Pinned literal, cross-checked with `md5(1)` on the same input, so a
        // change to the joining or the digest fails here and not in production.
        assert_eq!(signature, "12c80455c77439c8c746cdfcc251815e");
    }

    /// The `Encode=1` envelope, pinned against outputs captured from Qoder's
    /// own auth module by a probe script that lived in the product repo's retired
    /// Tauri shell (`apps/tauri/scripts/probe-qoder-image-route.mjs`).
    /// Covers the custom alphabet, the `$` padding symbol, the thirds swap, and
    /// the non-multiple-of-three lengths where the pad symbols ride along.
    #[test]
    fn encode_body_matches_the_captured_vectors() {
        let vectors = [
            ("", ""),
            ("a", "$p$#"),
            ("aa", "$ST#"),
            ("aaa", "iSg#"),
            ("aaaa", "$$gi#p#S"),
            ("hello", "q$FruHPH"),
            ("abc", "MSK#"),
            ("abd", "zSK#"),
            ("xyz", "yZLm"),
            ("xyzxyz", "LyLymZmZ"),
        ];
        for (plain, expected) in vectors {
            assert_eq!(
                encode_body(plain),
                expected,
                "envelope mismatch for {plain:?}"
            );
        }
    }

    #[test]
    fn decode_server_response_matches_the_official_qwenwork_vectors() {
        for (plain, encoded) in [
            ("", ""),
            ("{}", "$kwm"),
            (
                r#"{"qwork":[{"key":"demo"}]}"#,
                "DHFtuYKWxxw$Skr*P(FQBMn*mYK%J(WYPYBy",
            ),
            ("hello", "q$FruHPH"),
            ("千问办公", "mQ#SrDmhQ#sQ#(Ry"),
            (
                r#"{"emoji":"🧪","n":1}"#,
                "KhBMn%ep$$ByBboe)yn*GomYKLuSWsP,",
            ),
        ] {
            assert_eq!(decode_server_response(encoded).as_deref(), Ok(plain));
            assert_eq!(
                decode_server_response(&encode_body(plain)).as_deref(),
                Ok(plain)
            );
        }
        assert!(decode_server_response(r#"{"already":"json"}"#).is_err());
    }

    #[test]
    fn signing_path_strips_origin_query_and_gateway_prefix() {
        assert_eq!(
            signing_path("https://api3.qoder.sh/algo/api/v2/model/list?Encode=1"),
            "/api/v2/model/list"
        );
        assert_eq!(
            signing_path(&format!("https://api3.qoder.sh{INFER_PATH}")),
            "/api/v2/service/pro/sse/agent_chat_generation"
        );
        // A route that is not behind the gateway prefix keeps its whole path.
        assert_eq!(
            signing_path("https://api3.qoder.sh/api/v1/userinfo"),
            "/api/v1/userinfo"
        );
    }

    #[test]
    fn the_payload_field_order_is_the_signed_order() {
        use base64::Engine as _;
        let context = SigningContext::new("machine-1", "6", &fake_profile()).expect("context");
        let payload = context.auth_payload("request-1");
        let decoded = base64::engine::general_purpose::STANDARD
            .decode(payload)
            .expect("base64");
        assert_eq!(
            String::from_utf8(decoded).expect("utf8"),
            r#"{"version":"v1","requestId":"request-1","info":"FAKE_ENCRYPTED_USER_INFO_BLOB","cosyVersion":"1.1.5","ideVersion":""}"#
        );
    }

    #[test]
    fn a_profile_without_signing_material_refuses_to_sign() {
        // `SigningContext` holds credential material and deliberately does not
        // derive Debug, so the error is matched rather than unwrapped.
        match SigningContext::new("machine-1", "6", &json!({ "uid": "u-1" })) {
            Ok(_) => panic!("a profile with no runtime pair must not sign"),
            Err(error) => assert!(error.contains("sign in again"), "{error}"),
        }
    }

    #[test]
    fn the_catalog_get_carries_the_cosy_header_set() {
        let context = SigningContext::new("machine-1", "6", &fake_profile()).expect("context");
        let signed = context.sign_get("https://api3.qoder.sh", MODEL_LIST_PATH);
        assert_eq!(signed.url, "https://api3.qoder.sh/algo/api/v2/model/list");
        assert_eq!(signed.body, None);
        assert_eq!(signed.header("Cosy-User"), Some("fake-uid-0001"));
        assert_eq!(
            signed.header("Cosy-Key"),
            Some("FAKE_SIGNING_KEY_0123456789abcdef")
        );
        assert_eq!(signed.header("Cosy-ClientType"), Some("6"));
        assert_eq!(signed.header("Cosy-MachineType"), Some("5"));
        assert_eq!(signed.header("Login-Version"), Some("v2"));
        assert_eq!(signed.header("Cosy-ClientIp"), None);
        assert_eq!(signed.header("Cosy-Organization-Tags"), None);
        // No org on this profile, so the header must be absent rather than empty.
        assert_eq!(signed.header("Cosy-Organization-Id"), None);
        let auth = signed.header("Authorization").expect("Authorization");
        assert!(auth.starts_with("Bearer COSY."), "{auth}");
        assert_eq!(auth.rsplit('.').next().unwrap_or_default().len(), 32);
    }

    #[test]
    fn qwenwork_profile_carries_its_official_product_and_scene() {
        use base64::Engine as _;
        let mut profile = fake_profile();
        profile["organization_id"] = json!("org-1");
        profile["organization_tags"] = json!(["tag-a", "tag-b"]);
        profile["data_policy_agreed"] = json!(true);
        let context =
            SigningContext::new_with_profile("machine-1", "6", &profile, CosyProfile::QWENWORK)
                .expect("context");
        let signed = context.sign_get(
            "https://gateway.qwenwork.cn",
            "/algo/api/v2/model/list?Encode=1",
        );
        assert_eq!(signed.header("Cosy-Version"), Some("1.1.32"));
        assert_eq!(signed.header("Cosy-Business-Product"), Some("qoder_work"));
        assert_eq!(signed.header("Cosy-Business-Type"), Some("agent"));
        assert_eq!(signed.header("Cosy-Scene"), Some("qwork"));
        assert_eq!(signed.header("Cosy-Data-Policy"), Some("agree"));
        assert_eq!(signed.header("Cosy-ClientIp"), Some("machine-1"));
        assert_eq!(signed.header("Cosy-Organization-Id"), Some("org-1"));
        assert_eq!(signed.header("Cosy-Organization-Tags"), Some("tag-a,tag-b"));
        let payload = signed
            .header("Authorization")
            .expect("Authorization")
            .trim_start_matches("Bearer COSY.")
            .split('.')
            .next()
            .expect("payload");
        let decoded = base64::engine::general_purpose::STANDARD
            .decode(payload)
            .expect("base64");
        assert!(String::from_utf8(decoded)
            .expect("utf8")
            .contains(r#""cosyVersion":"1.1.32""#));
    }

    #[test]
    fn qwenwork_runtime_profile_seals_token_and_enterprise_metadata() {
        let profile = json!({
            "uid": "user-1",
            "organization_id": "org-1",
            "organization_tags": ["tag-a", "tag-b"],
            "data_policy_agreed": true,
            "security_oauth_token": "tok-1",
            "name": "Display Name",
        });
        assert_eq!(
            qwenwork_sealed_profile_json(&profile),
            r#"{"uid":"user-1","security_oauth_token":"tok-1","organization_id":"org-1","organization_tags":["tag-a","tag-b"],"data_policy_agreed":true}"#
        );
    }

    #[test]
    fn qwenwork_omits_unknown_or_empty_organization_metadata() {
        let profile = json!({ "uid": "user-1" });
        assert_eq!(
            qwenwork_sealed_profile_json(&profile),
            r#"{"uid":"user-1"}"#
        );

        let mut with_empty_tags = profile;
        with_empty_tags["organization_tags"] = json!([]);
        let context = SigningContext::new_with_profile(
            "machine-1",
            "6",
            &json!({
                "uid": "user-1",
                "organization_tags": [],
                "encrypt_user_info": "FAKE_ENCRYPTED_USER_INFO_BLOB",
                "key": "FAKE_SIGNING_KEY_0123456789abcdef"
            }),
            CosyProfile::QWENWORK,
        )
        .expect("context");
        let signed = context.sign_get(
            "https://gateway.qwenwork.cn",
            "/algo/api/v2/model/list?Encode=1",
        );
        assert_eq!(signed.header("Cosy-Organization-Tags"), None);
        assert_eq!(
            qwenwork_sealed_profile_json(&with_empty_tags),
            r#"{"uid":"user-1","organization_tags":[]}"#
        );
    }

    #[test]
    fn the_infer_post_signs_the_encoded_body_it_sends() {
        let context = SigningContext::new("machine-1", "6", &fake_profile()).expect("context");
        let plain = r#"{"messages":[]}"#;
        let signed = context.sign_infer("https://api3.qoder.sh", plain, "auto", "system");
        let wire = encode_body(plain);
        // The `Encode=1` flag and the envelope are ONE decision: a URL asking for
        // the envelope while the body stays plaintext is refused upstream, and a
        // plaintext body is what the WAF matches on.
        assert!(signed.url.contains("Encode=1"), "{}", signed.url);
        assert_eq!(signed.body.as_deref(), Some(wire.as_str()));
        assert!(
            !wire.contains("messages"),
            "plaintext must not survive onto the wire: {wire}"
        );
        assert_eq!(signed.header("Accept"), Some("text/event-stream"));
        assert_eq!(signed.header("X-Model-Key"), Some("auto"));
        assert_eq!(signed.header("X-Model-Source"), Some("system"));

        let date = signed.header("Cosy-Date").expect("Cosy-Date");
        let auth = signed.header("Authorization").expect("Authorization");
        let payload = auth
            .trim_start_matches("Bearer COSY.")
            .split('.')
            .next()
            .unwrap_or_default();
        // The signature proves the ENCODED bytes, because those are what ships.
        assert_eq!(
            auth.rsplit('.').next().unwrap_or_default(),
            sign(
                payload,
                "FAKE_SIGNING_KEY_0123456789abcdef",
                date,
                &wire,
                &signed.url
            )
        );
    }

    #[test]
    fn an_organization_account_sends_its_org_header() {
        let mut profile = fake_profile();
        profile["organization_id"] = json!("org-42");
        let context = SigningContext::new("machine-1", "6", &profile).expect("context");
        let signed = context.sign_get("https://api3.qoder.sh", MODEL_LIST_PATH);
        assert_eq!(signed.header("Cosy-Organization-Id"), Some("org-42"));
    }

    /// Formatting a signed request must never spill the bearer token or the
    /// wrapped session key into a log line.
    #[test]
    fn debug_output_redacts_the_credential_headers() {
        let context = SigningContext::new("machine-1", "6", &fake_profile()).expect("context");
        let signed = context.sign_infer("https://api3.qoder.sh", "{}", "auto", "system");
        let rendered = format!("{signed:?}");
        assert!(!rendered.contains("Bearer COSY."), "{rendered}");
        assert!(
            !rendered.contains("FAKE_SIGNING_KEY_0123456789abcdef"),
            "{rendered}"
        );
        assert!(!rendered.contains("fake-uid-0001"), "{rendered}");
        // Still useful for diagnosis: the route and the non-secret headers stay.
        assert!(rendered.contains("agent_chat_generation"), "{rendered}");
        assert!(rendered.contains("text/event-stream"), "{rendered}");
    }

    /// The sealed pair has to follow Qoder's own scheme: `info` is AES-128-CBC
    /// over the profile JSON with the 16-byte key doubling as the IV, and `key`
    /// is that AES key wrapped for the server. The wrapped half cannot be
    /// verified without the private key, so its RSA block size is pinned; the
    /// AES half is pinned by length and by a round-trip under a known key.
    #[test]
    fn the_sealed_pair_follows_qoders_own_scheme() {
        use aes::cipher::{block_padding::Pkcs7, BlockDecryptMut, BlockEncryptMut, KeyIvInit};
        use base64::Engine as _;

        let profile = json!({
            "uid": "u-7",
            "name": "Someone",
            "email": "someone@example.invalid",
            "security_oauth_token": "token-7",
        });
        let runtime = generate_runtime_auth(&profile).expect("runtime pair");

        let wrapped = base64::engine::general_purpose::STANDARD
            .decode(&runtime.key)
            .expect("key is base64");
        assert_eq!(wrapped.len(), 128, "RSA-1024 block");

        // The sealed text, as a LITERAL rather than by re-running the builder:
        // this is the byte sequence Qoder's server decrypts, so the field set and
        // the order are pinned here and a change has to be made on purpose.
        let expected = r#"{"uid":"u-7","security_oauth_token":"token-7","name":"Someone","aid":"","email":"someone@example.invalid"}"#;
        assert_eq!(
            sealed_profile_json(&profile),
            expected,
            "sealed field order"
        );
        let sealed = base64::engine::general_purpose::STANDARD
            .decode(&runtime.encrypt_user_info)
            .expect("info is base64");
        // PKCS7 always adds a block when the plaintext is a block multiple, so
        // this pins both the padding scheme and the sealed field set.
        assert_eq!(sealed.len(), (expected.len() / 16 + 1) * 16);

        // Key and IV are the same 16 bytes — Qoder's choice, and the one detail a
        // reimplementation is most likely to get wrong.
        let key = "0123456789abcdef";
        let cipher =
            cbc::Encryptor::<aes::Aes128>::new(key.as_bytes().into(), key.as_bytes().into())
                .encrypt_padded_vec_mut::<Pkcs7>(expected.as_bytes());
        let plain =
            cbc::Decryptor::<aes::Aes128>::new(key.as_bytes().into(), key.as_bytes().into())
                .decrypt_padded_vec_mut::<Pkcs7>(&cipher)
                .expect("round-trip");
        assert_eq!(String::from_utf8(plain).expect("utf8"), expected);
        assert_eq!(
            cipher.len(),
            sealed.len(),
            "same padded width as the sealed blob"
        );
    }
}
