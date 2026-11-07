//! AWS Signature Version 4 request signing.
//!
//! A focused, dependency-light SigV4 implementation covering exactly what the
//! Bedrock runtime transport needs: signing a single `POST` with a known body
//! against a regional service endpoint. The algorithm follows the AWS reference
//! (`Authorization` header form): canonical request -> string to sign ->
//! derived signing key -> signature.

use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};

type HmacSha256 = Hmac<Sha256>;

/// AWS credentials used to sign a request.
#[derive(Debug, Clone)]
pub struct AwsCredentials {
    pub access_key_id: String,
    pub secret_access_key: String,
    /// Present for temporary credentials (STS / instance roles).
    pub session_token: Option<String>,
}

impl AwsCredentials {
    /// Resolve static credentials from the standard AWS environment variables.
    /// Returns `None` unless both the access key id and secret are present.
    pub fn from_env() -> Option<Self> {
        let access_key_id = nonempty_env("AWS_ACCESS_KEY_ID")?;
        let secret_access_key = nonempty_env("AWS_SECRET_ACCESS_KEY")?;
        Some(Self {
            access_key_id,
            secret_access_key,
            session_token: nonempty_env("AWS_SESSION_TOKEN"),
        })
    }
}

fn nonempty_env(key: &str) -> Option<String> {
    std::env::var(key)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

/// A single request to sign. Headers are the ADDITIONAL headers the caller will
/// send (beyond the signer-managed `host` / `x-amz-date` / `x-amz-security-token`).
pub struct SigV4Request<'a> {
    pub method: &'a str,
    pub service: &'a str,
    pub region: &'a str,
    /// Request host, e.g. `bedrock-runtime.us-east-1.amazonaws.com`.
    pub host: &'a str,
    /// URI path, already percent-encoded where needed, e.g. `/model/{id}/invoke`.
    pub canonical_uri: &'a str,
    /// Raw canonical query string (may be empty).
    pub canonical_query: &'a str,
    pub payload: &'a [u8],
    /// Extra signed headers as (lowercase-name, value) pairs. `content-type`
    /// belongs here when set.
    pub extra_headers: &'a [(String, String)],
    /// Include `x-amz-content-sha256` in the signed header set. AWS recommends
    /// this for services like Bedrock; the classic test vectors omit it.
    pub sign_content_sha256: bool,
}

/// The headers that must be attached to the outgoing request after signing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SignedHeaders {
    /// `Authorization` header value.
    pub authorization: String,
    /// `x-amz-date` value in `YYYYMMDDTHHMMSSZ` form.
    pub amz_date: String,
    /// SHA-256 hex of the payload (`x-amz-content-sha256`).
    pub content_sha256: String,
    /// Echoed session token, when the credentials are temporary.
    pub session_token: Option<String>,
}

fn hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push_str(&format!("{byte:02x}"));
    }
    out
}

fn sha256_hex(data: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(data);
    hex(&hasher.finalize())
}

fn hmac(key: &[u8], data: &[u8]) -> Vec<u8> {
    let mut mac = HmacSha256::new_from_slice(key).expect("HMAC accepts keys of any length");
    mac.update(data);
    mac.finalize().into_bytes().to_vec()
}

/// Trim leading/trailing spaces and collapse internal runs, per the canonical
/// header-value rule. Values here are simple ASCII, so byte handling is safe.
fn canonical_header_value(value: &str) -> String {
    value.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// Sign `request` at instant `amz_date` (`YYYYMMDDTHHMMSSZ`) with `creds`.
pub fn sign(request: &SigV4Request<'_>, creds: &AwsCredentials, amz_date: &str) -> SignedHeaders {
    let date_stamp = &amz_date[..8]; // YYYYMMDD
    let content_sha256 = sha256_hex(request.payload);

    // Assemble the signed header set: host + x-amz-* + any extra headers,
    // lowercased and sorted by name.
    let mut headers: Vec<(String, String)> = Vec::new();
    headers.push(("host".to_string(), request.host.to_string()));
    if request.sign_content_sha256 {
        headers.push(("x-amz-content-sha256".to_string(), content_sha256.clone()));
    }
    headers.push(("x-amz-date".to_string(), amz_date.to_string()));
    if let Some(token) = &creds.session_token {
        headers.push(("x-amz-security-token".to_string(), token.clone()));
    }
    for (name, value) in request.extra_headers {
        headers.push((name.to_lowercase(), value.clone()));
    }
    headers.sort_by(|a, b| a.0.cmp(&b.0));

    let canonical_headers = headers
        .iter()
        .map(|(name, value)| format!("{name}:{}\n", canonical_header_value(value)))
        .collect::<String>();
    let signed_headers = headers
        .iter()
        .map(|(name, _)| name.as_str())
        .collect::<Vec<_>>()
        .join(";");

    let canonical_request = format!(
        "{}\n{}\n{}\n{}\n{}\n{}",
        request.method,
        request.canonical_uri,
        request.canonical_query,
        canonical_headers,
        signed_headers,
        content_sha256,
    );

    let scope = format!(
        "{date_stamp}/{}/{}/aws4_request",
        request.region, request.service
    );
    let string_to_sign = format!(
        "AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n{}",
        sha256_hex(canonical_request.as_bytes())
    );

    // Derive the signing key: HMAC chain seeded with the secret.
    let k_date = hmac(
        format!("AWS4{}", creds.secret_access_key).as_bytes(),
        date_stamp.as_bytes(),
    );
    let k_region = hmac(&k_date, request.region.as_bytes());
    let k_service = hmac(&k_region, request.service.as_bytes());
    let k_signing = hmac(&k_service, b"aws4_request");
    let signature = hex(&hmac(&k_signing, string_to_sign.as_bytes()));

    let authorization = format!(
        "AWS4-HMAC-SHA256 Credential={}/{scope}, SignedHeaders={signed_headers}, Signature={signature}",
        creds.access_key_id
    );

    SignedHeaders {
        authorization,
        amz_date: amz_date.to_string(),
        content_sha256,
        session_token: creds.session_token.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Canonical vector from the AWS SigV4 test suite ("get-vanilla"): a bare
    // GET to `example.amazonaws.com` in `service`/`us-east-1` at the documented
    // instant produces this exact Authorization signature. It pins the whole
    // canonical-request -> string-to-sign -> signing-key -> signature chain.
    #[test]
    fn get_vanilla_matches_the_aws_reference_signature() {
        let creds = AwsCredentials {
            access_key_id: "AKIDEXAMPLE".to_string(),
            secret_access_key: "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY".to_string(),
            session_token: None,
        };
        let request = SigV4Request {
            method: "GET",
            service: "service",
            region: "us-east-1",
            host: "example.amazonaws.com",
            canonical_uri: "/",
            canonical_query: "",
            payload: b"",
            extra_headers: &[],
            sign_content_sha256: false,
        };
        let signed = super::sign(&request, &creds, "20150830T123600Z");
        // Empty-payload SHA-256 is the well-known constant.
        assert_eq!(
            signed.content_sha256,
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
        assert_eq!(
            signed.authorization,
            "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20150830/us-east-1/service/aws4_request, \
SignedHeaders=host;x-amz-date, \
Signature=5fa00fa31553b73ebf1942676e86291e8372ff2a2260956d9b8aae1d763fbf31"
        );
    }

    #[test]
    fn temporary_credentials_add_the_security_token_to_signed_headers() {
        let creds = AwsCredentials {
            access_key_id: "AKID".to_string(),
            secret_access_key: "secret".to_string(),
            session_token: Some("token-123".to_string()),
        };
        let request = SigV4Request {
            method: "POST",
            service: "bedrock",
            region: "us-east-1",
            host: "bedrock-runtime.us-east-1.amazonaws.com",
            canonical_uri: "/model/anthropic/invoke",
            canonical_query: "",
            payload: b"{}",
            extra_headers: &[("content-type".to_string(), "application/json".to_string())],
            sign_content_sha256: true,
        };
        let signed = super::sign(&request, &creds, "20240101T000000Z");
        assert_eq!(signed.session_token.as_deref(), Some("token-123"));
        // The security token participates in the signed header set.
        assert!(signed.authorization.contains(
            "SignedHeaders=content-type;host;x-amz-content-sha256;x-amz-date;x-amz-security-token"
        ));
    }
}
