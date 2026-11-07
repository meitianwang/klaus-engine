use klaus_agent_core::runtime_process::RuntimeEnv;
use klaus_agent_core::runtime_values::RuntimeString;

const ENV_KEY_PREFIX: &[u8] = b"KLAUS_INTERNAL_BROWSER_BROKER_";
const ARG_PREFIX: &[u8] = b"--klaus-browser-broker-env-key=";
const VALUE_PREFIX: &[u8] = b"v1.";
const PIPE_PREFIX: &[u8] = b"\\\\.\\pipe\\klaus-browser-v1-";
const PIPE_LANES: [&[u8]; 4] = [b"sync-c2h", b"sync-h2c", b"mux-c2h", b"mux-h2c"];
const MAX_KEY_ATTEMPTS: usize = 16;
const MAX_PIPE_NAME_BYTES: usize = 512;
const MAX_BOOTSTRAP_PAYLOAD_BYTES: usize = 2_096;
const MAX_BOOTSTRAP_BASE64URL_BYTES: usize = 2_795;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WindowsBrowserBootstrapError {
    InvalidGeneration,
    EntropyUnavailable,
    KeyCollision,
    InvalidKey,
    MissingArgument,
    DuplicateArgument,
    MissingEnvironmentValue,
    DuplicateEnvironmentKey,
    InvalidValue,
    NonCanonicalBase64Url,
    PayloadTooLarge,
    InvalidPipeName,
}

/// One CSPRNG attempt for the private environment key. `Unavailable` models a
/// generator failure and fails immediately rather than weakening the key.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WindowsBootstrapKeyAttempt {
    Bytes([u8; 32]),
    Unavailable,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WindowsBrowserBrokerBootstrap {
    generation: u64,
    nonce: [u8; 32],
    environment_key: RuntimeString,
    environment_value: RuntimeString,
    pipe_names: [RuntimeString; 4],
}

impl WindowsBrowserBrokerBootstrap {
    pub fn generation(&self) -> u64 {
        self.generation
    }

    pub fn nonce(&self) -> &[u8; 32] {
        &self.nonce
    }

    pub fn environment_key(&self) -> &RuntimeString {
        &self.environment_key
    }

    pub fn environment_value(&self) -> &RuntimeString {
        &self.environment_value
    }

    pub fn pipe_names(&self) -> &[RuntimeString; 4] {
        &self.pipe_names
    }

    pub fn argv_token(&self) -> RuntimeString {
        let mut bytes = ARG_PREFIX.to_vec();
        bytes.extend(runtime_ascii_owned(&self.environment_key).expect("generated key is ASCII"));
        runtime_from_ascii(&bytes)
    }
}

/// Select a collision-free private key and build the canonical bootstrap value.
/// Existing environment entries are compared with Windows ASCII-insensitive
/// semantics and are never removed or overwritten.
pub fn build_windows_browser_bootstrap(
    generation: u64,
    nonce: [u8; 32],
    environment: &RuntimeEnv,
    attempts: impl IntoIterator<Item = WindowsBootstrapKeyAttempt>,
) -> Result<WindowsBrowserBrokerBootstrap, WindowsBrowserBootstrapError> {
    if generation == 0 {
        return Err(WindowsBrowserBootstrapError::InvalidGeneration);
    }
    let pipe_name_bytes = expected_pipe_names(generation, &nonce)?;
    let payload = encode_payload(generation, &nonce, &pipe_name_bytes)?;
    let mut value = VALUE_PREFIX.to_vec();
    value.extend(encode_base64url(&payload));

    let mut saw_attempt = false;
    for attempt in attempts.into_iter().take(MAX_KEY_ATTEMPTS) {
        saw_attempt = true;
        let WindowsBootstrapKeyAttempt::Bytes(key_nonce) = attempt else {
            return Err(WindowsBrowserBootstrapError::EntropyUnavailable);
        };
        let mut key = ENV_KEY_PREFIX.to_vec();
        append_upper_hex(&mut key, &key_nonce);
        if environment.entries().any(|(existing, _)| {
            !matches!(
                windows_environment_key_equality(&existing, &key),
                WindowsEnvironmentKeyEquality::Different
            )
        }) {
            continue;
        }
        return Ok(WindowsBrowserBrokerBootstrap {
            generation,
            nonce,
            environment_key: runtime_from_ascii(&key),
            environment_value: runtime_from_ascii(&value),
            pipe_names: pipe_name_bytes.map(|name| runtime_from_ascii(&name)),
        });
    }
    if saw_attempt {
        Err(WindowsBrowserBootstrapError::KeyCollision)
    } else {
        Err(WindowsBrowserBootstrapError::EntropyUnavailable)
    }
}

/// Parse the exact one-shot sidecar bootstrap from argv and environment. This
/// redundantly verifies canonical base64url, generation, nonce, pipe order and
/// names before any named pipe may be opened.
pub fn parse_windows_browser_bootstrap(
    arguments: &[RuntimeString],
    environment: &RuntimeEnv,
) -> Result<WindowsBrowserBrokerBootstrap, WindowsBrowserBootstrapError> {
    let mut selected_key: Option<Vec<u8>> = None;
    for argument in arguments {
        let Some(bytes) = runtime_ascii_owned(argument) else {
            continue;
        };
        if let Some(key) = bytes.strip_prefix(ARG_PREFIX) {
            if selected_key.is_some() {
                return Err(WindowsBrowserBootstrapError::DuplicateArgument);
            }
            validate_environment_key(key)?;
            selected_key = Some(key.to_vec());
        }
    }
    let selected_key = selected_key.ok_or(WindowsBrowserBootstrapError::MissingArgument)?;
    let mut selected_value = None;
    for (key, value) in environment.entries() {
        match windows_environment_key_equality(&key, &selected_key) {
            WindowsEnvironmentKeyEquality::Different => {}
            WindowsEnvironmentKeyEquality::Unknown => {
                return Err(WindowsBrowserBootstrapError::InvalidKey);
            }
            WindowsEnvironmentKeyEquality::Equal => {
                if selected_value.is_some() {
                    return Err(WindowsBrowserBootstrapError::DuplicateEnvironmentKey);
                }
                if runtime_ascii_owned(&key).as_deref() != Some(selected_key.as_slice()) {
                    return Err(WindowsBrowserBootstrapError::InvalidKey);
                }
                selected_value = Some(value);
            }
        }
    }
    let selected_value =
        selected_value.ok_or(WindowsBrowserBootstrapError::MissingEnvironmentValue)?;
    let value_bytes =
        runtime_ascii_owned(selected_value).ok_or(WindowsBrowserBootstrapError::InvalidValue)?;
    let encoded = value_bytes
        .strip_prefix(VALUE_PREFIX)
        .ok_or(WindowsBrowserBootstrapError::InvalidValue)?;
    if encoded.len() > MAX_BOOTSTRAP_BASE64URL_BYTES {
        return Err(WindowsBrowserBootstrapError::PayloadTooLarge);
    }
    let payload = decode_base64url(encoded)?;
    if encode_base64url(&payload) != encoded {
        return Err(WindowsBrowserBootstrapError::NonCanonicalBase64Url);
    }
    let decoded = decode_payload(&payload)?;
    let DecodedBootstrapPayload {
        generation,
        nonce,
        names,
    } = decoded;
    let expected = expected_pipe_names(generation, &nonce)?;
    if names != expected {
        return Err(WindowsBrowserBootstrapError::InvalidPipeName);
    }
    Ok(WindowsBrowserBrokerBootstrap {
        generation,
        nonce,
        environment_key: runtime_from_ascii(&selected_key),
        environment_value: selected_value.clone(),
        pipe_names: names.map(|name| runtime_from_ascii(&name)),
    })
}

fn expected_pipe_names(
    generation: u64,
    nonce: &[u8; 32],
) -> Result<[Vec<u8>; 4], WindowsBrowserBootstrapError> {
    if generation == 0 {
        return Err(WindowsBrowserBootstrapError::InvalidGeneration);
    }
    let mut generation_hex = Vec::with_capacity(16);
    append_lower_hex(&mut generation_hex, &generation.to_be_bytes());
    let mut nonce_hex = Vec::with_capacity(64);
    append_upper_hex(&mut nonce_hex, nonce);
    let names = PIPE_LANES.map(|lane| {
        let mut name = PIPE_PREFIX.to_vec();
        name.extend_from_slice(&generation_hex);
        name.push(b'-');
        name.extend_from_slice(&nonce_hex);
        name.push(b'-');
        name.extend_from_slice(lane);
        name
    });
    if names.iter().any(|name| name.len() > MAX_PIPE_NAME_BYTES) {
        return Err(WindowsBrowserBootstrapError::InvalidPipeName);
    }
    Ok(names)
}

fn encode_payload(
    generation: u64,
    nonce: &[u8; 32],
    names: &[Vec<u8>; 4],
) -> Result<Vec<u8>, WindowsBrowserBootstrapError> {
    let mut payload = Vec::new();
    payload.extend_from_slice(&generation.to_be_bytes());
    payload.extend_from_slice(nonce);
    for name in names {
        let length =
            u16::try_from(name.len()).map_err(|_| WindowsBrowserBootstrapError::InvalidPipeName)?;
        payload.extend_from_slice(&length.to_be_bytes());
        payload.extend_from_slice(name);
    }
    if payload.len() > MAX_BOOTSTRAP_PAYLOAD_BYTES {
        return Err(WindowsBrowserBootstrapError::PayloadTooLarge);
    }
    Ok(payload)
}

struct DecodedBootstrapPayload {
    generation: u64,
    nonce: [u8; 32],
    names: [Vec<u8>; 4],
}

fn decode_payload(payload: &[u8]) -> Result<DecodedBootstrapPayload, WindowsBrowserBootstrapError> {
    if payload.len() > MAX_BOOTSTRAP_PAYLOAD_BYTES || payload.len() < 40 {
        return Err(WindowsBrowserBootstrapError::PayloadTooLarge);
    }
    let generation = u64::from_be_bytes(
        payload[..8]
            .try_into()
            .map_err(|_| WindowsBrowserBootstrapError::InvalidValue)?,
    );
    if generation == 0 {
        return Err(WindowsBrowserBootstrapError::InvalidGeneration);
    }
    let nonce = payload[8..40]
        .try_into()
        .map_err(|_| WindowsBrowserBootstrapError::InvalidValue)?;
    let mut offset = 40_usize;
    let mut names: [Vec<u8>; 4] = std::array::from_fn(|_| Vec::new());
    for name in &mut names {
        let length_end = offset
            .checked_add(2)
            .ok_or(WindowsBrowserBootstrapError::InvalidValue)?;
        if length_end > payload.len() {
            return Err(WindowsBrowserBootstrapError::InvalidValue);
        }
        let length = usize::from(u16::from_be_bytes(
            payload[offset..length_end]
                .try_into()
                .map_err(|_| WindowsBrowserBootstrapError::InvalidValue)?,
        ));
        offset = length_end;
        let end = offset
            .checked_add(length)
            .ok_or(WindowsBrowserBootstrapError::InvalidValue)?;
        if length > MAX_PIPE_NAME_BYTES || end > payload.len() {
            return Err(WindowsBrowserBootstrapError::InvalidPipeName);
        }
        if !payload[offset..end].is_ascii() {
            return Err(WindowsBrowserBootstrapError::InvalidPipeName);
        }
        *name = payload[offset..end].to_vec();
        offset = end;
    }
    if offset != payload.len() {
        return Err(WindowsBrowserBootstrapError::InvalidValue);
    }
    Ok(DecodedBootstrapPayload {
        generation,
        nonce,
        names,
    })
}

fn validate_environment_key(key: &[u8]) -> Result<(), WindowsBrowserBootstrapError> {
    let Some(suffix) = key.strip_prefix(ENV_KEY_PREFIX) else {
        return Err(WindowsBrowserBootstrapError::InvalidKey);
    };
    if suffix.len() != 64
        || !suffix
            .iter()
            .all(|byte| byte.is_ascii_digit() || (b'A'..=b'F').contains(byte))
    {
        return Err(WindowsBrowserBootstrapError::InvalidKey);
    }
    Ok(())
}

fn runtime_ascii_owned(value: &RuntimeString) -> Option<Vec<u8>> {
    if !value.as_utf16().iter().all(|unit| *unit <= 0x7f) {
        return None;
    }
    value
        .as_utf16()
        .iter()
        .copied()
        .map(u8::try_from)
        .collect::<Result<Vec<_>, _>>()
        .ok()
}

fn runtime_from_ascii(bytes: &[u8]) -> RuntimeString {
    RuntimeString::from_utf8(std::str::from_utf8(bytes).expect("validated ASCII bytes are UTF-8"))
}

#[cfg(not(windows))]
fn ascii_eq_ci(left: &[u8], right: &[u8]) -> bool {
    left.eq_ignore_ascii_case(right)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum WindowsEnvironmentKeyEquality {
    Equal,
    Different,
    Unknown,
}

/// Compare one existing logical environment key with the generated ASCII key
/// under Rust 1.93's Windows `EnvKey` law. Windows delegates to
/// `CompareStringOrdinal(..., TRUE)`. A non-Windows shadow can prove the ASCII
/// subset only; every other input is `Unknown`, which callers must reject or
/// count as a collision rather than silently accepting a possibly equal key.
fn windows_environment_key_equality(
    existing: &RuntimeString,
    generated_ascii: &[u8],
) -> WindowsEnvironmentKeyEquality {
    #[cfg(windows)]
    {
        use windows_sys::Win32::Globalization::{CompareStringOrdinal, CSTR_EQUAL};

        let Ok(existing_len) = i32::try_from(existing.as_utf16().len()) else {
            return WindowsEnvironmentKeyEquality::Unknown;
        };
        let Ok(generated_len) = i32::try_from(generated_ascii.len()) else {
            return WindowsEnvironmentKeyEquality::Unknown;
        };
        let generated = generated_ascii
            .iter()
            .copied()
            .map(u16::from)
            .collect::<Vec<_>>();
        // SAFETY: both pointers borrow allocations that remain live for the
        // counted call; lengths were checked to fit Win32's signed count.
        let result = unsafe {
            CompareStringOrdinal(
                existing.as_utf16().as_ptr(),
                existing_len,
                generated.as_ptr(),
                generated_len,
                1,
            )
        };
        match result {
            CSTR_EQUAL => WindowsEnvironmentKeyEquality::Equal,
            0 => WindowsEnvironmentKeyEquality::Unknown,
            _ => WindowsEnvironmentKeyEquality::Different,
        }
    }

    #[cfg(not(windows))]
    {
        match runtime_ascii_owned(existing) {
            Some(existing_ascii) if ascii_eq_ci(&existing_ascii, generated_ascii) => {
                WindowsEnvironmentKeyEquality::Equal
            }
            Some(_) => WindowsEnvironmentKeyEquality::Different,
            None => WindowsEnvironmentKeyEquality::Unknown,
        }
    }
}

fn append_upper_hex(output: &mut Vec<u8>, bytes: &[u8]) {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    for byte in bytes {
        output.push(HEX[usize::from(byte >> 4)]);
        output.push(HEX[usize::from(byte & 0x0f)]);
    }
}

fn append_lower_hex(output: &mut Vec<u8>, bytes: &[u8]) {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    for byte in bytes {
        output.push(HEX[usize::from(byte >> 4)]);
        output.push(HEX[usize::from(byte & 0x0f)]);
    }
}

fn encode_base64url(bytes: &[u8]) -> Vec<u8> {
    const TABLE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    let mut output = Vec::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let first = chunk[0];
        output.push(TABLE[usize::from(first >> 2)]);
        if chunk.len() == 1 {
            output.push(TABLE[usize::from((first & 0x03) << 4)]);
            continue;
        }
        let second = chunk[1];
        output.push(TABLE[usize::from(((first & 0x03) << 4) | (second >> 4))]);
        if chunk.len() == 2 {
            output.push(TABLE[usize::from((second & 0x0f) << 2)]);
            continue;
        }
        let third = chunk[2];
        output.push(TABLE[usize::from(((second & 0x0f) << 2) | (third >> 6))]);
        output.push(TABLE[usize::from(third & 0x3f)]);
    }
    output
}

fn decode_base64url(bytes: &[u8]) -> Result<Vec<u8>, WindowsBrowserBootstrapError> {
    if bytes.is_empty() || bytes.len() % 4 == 1 || bytes.contains(&b'=') {
        return Err(WindowsBrowserBootstrapError::NonCanonicalBase64Url);
    }
    let mut output = Vec::with_capacity(bytes.len() / 4 * 3 + 2);
    for chunk in bytes.chunks(4) {
        let mut values = [0_u8; 4];
        for (index, byte) in chunk.iter().copied().enumerate() {
            values[index] = decode_base64url_byte(byte)
                .ok_or(WindowsBrowserBootstrapError::NonCanonicalBase64Url)?;
        }
        output.push((values[0] << 2) | (values[1] >> 4));
        if chunk.len() >= 3 {
            output.push((values[1] << 4) | (values[2] >> 2));
        }
        if chunk.len() == 4 {
            output.push((values[2] << 6) | values[3]);
        }
    }
    Ok(output)
}

fn decode_base64url_byte(byte: u8) -> Option<u8> {
    match byte {
        b'A'..=b'Z' => Some(byte - b'A'),
        b'a'..=b'z' => Some(byte - b'a' + 26),
        b'0'..=b'9' => Some(byte - b'0' + 52),
        b'-' => Some(62),
        b'_' => Some(63),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use klaus_agent_core::runtime_values::{RuntimeDecode, RuntimeJson, RuntimeObject};

    use super::*;

    fn environment(entries: &[(&str, &str)]) -> RuntimeEnv {
        let mut object = RuntimeObject::new();
        for (key, value) in entries {
            object.insert(*key, RuntimeJson::from(*value));
        }
        RuntimeEnv::decode_runtime(&RuntimeJson::Object(object)).unwrap()
    }

    #[test]
    fn canonical_bootstrap_round_trips_generation_nonce_roles_and_argument() {
        assert_eq!(encode_base64url(&[0xff]), b"_w");
        assert_eq!(encode_base64url(&[0xff, 0xee]), b"_-4");
        assert_eq!(decode_base64url(b"_-7d").unwrap(), [0xff, 0xee, 0xdd]);
        let environment = environment(&[("KEEP", "value")]);
        let bootstrap = build_windows_browser_bootstrap(
            0x12,
            [0xab; 32],
            &environment,
            [WindowsBootstrapKeyAttempt::Bytes([0xcd; 32])],
        )
        .unwrap();
        assert_eq!(
            bootstrap.environment_key(),
            &RuntimeString::from_utf8(&format!(
                "KLAUS_INTERNAL_BROWSER_BROKER_{}",
                "CD".repeat(32)
            ))
        );
        let mut with_bootstrap = environment.clone();
        with_bootstrap.insert(
            bootstrap.environment_key().clone(),
            bootstrap.environment_value().clone(),
        );
        let parsed =
            parse_windows_browser_bootstrap(&[bootstrap.argv_token()], &with_bootstrap).unwrap();
        assert_eq!(parsed, bootstrap);
        let names = parsed
            .pipe_names()
            .iter()
            .map(|value| value.try_to_utf8().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(
            names[0],
            format!(
                "\\\\.\\pipe\\klaus-browser-v1-0000000000000012-{}-sync-c2h",
                "AB".repeat(32)
            )
        );
        assert!(names[1].ends_with("-sync-h2c"));
        assert!(names[2].ends_with("-mux-c2h"));
        assert!(names[3].ends_with("-mux-h2c"));
    }

    #[test]
    fn key_selection_preserves_case_insensitive_collisions_and_stops_at_sixteen() {
        let collision = format!("klaus_internal_browser_broker_{}", "11".repeat(32));
        let environment = environment(&[(&collision, "caller")]);
        let bootstrap = build_windows_browser_bootstrap(
            1,
            [2; 32],
            &environment,
            [
                WindowsBootstrapKeyAttempt::Bytes([0x11; 32]),
                WindowsBootstrapKeyAttempt::Bytes([0x22; 32]),
            ],
        )
        .unwrap();
        assert!(bootstrap
            .environment_key()
            .try_to_utf8()
            .unwrap()
            .ends_with(&"22".repeat(32)));
        assert_eq!(
            environment.get(&RuntimeString::from_utf8(&collision)),
            Some(&RuntimeString::from("caller"))
        );

        let attempts = [WindowsBootstrapKeyAttempt::Bytes([0x11; 32]); 17];
        assert_eq!(
            build_windows_browser_bootstrap(1, [2; 32], &environment, attempts),
            Err(WindowsBrowserBootstrapError::KeyCollision)
        );
    }

    #[test]
    fn non_ascii_target_alias_is_never_silently_accepted_or_skipped() {
        let first_key = format!("KLAUS_INTERNAL_BROWSER_BROKER_{}", "11".repeat(32));
        let mut alias_units = first_key.encode_utf16().collect::<Vec<_>>();
        alias_units[4] = 0x017f; // long s, a candidate ordinal-case alias for ASCII S.
        let alias = RuntimeString::from_utf16(alias_units);
        let mut caller_environment = environment(&[]);
        caller_environment.insert(alias.clone(), RuntimeString::from("caller"));

        // Whether this host's comparator folds the long s onto `S` is the OS's call, not
        // this engine's: `CompareStringOrdinal(…, TRUE)` upcases through the OS table, which
        // has mapped LATIN SMALL LETTER LONG S onto `S` on some Windows builds and left it
        // alone on others. Asserting one verdict pinned the OS instead of the behaviour, so
        // the invariant asserted here is what the engine does with EACH verdict: an aliasing
        // key is skipped when generating and rejected when parsing, and a non-aliasing one
        // is an unrelated variable that neither blocks generation nor gets accepted in place
        // of the byte-identical key.
        #[cfg(windows)]
        let alias_collides = !matches!(
            windows_environment_key_equality(&alias, first_key.as_bytes()),
            WindowsEnvironmentKeyEquality::Different
        );

        let selected = build_windows_browser_bootstrap(
            1,
            [2; 32],
            &caller_environment,
            [
                WindowsBootstrapKeyAttempt::Bytes([0x11; 32]),
                WindowsBootstrapKeyAttempt::Bytes([0x22; 32]),
            ],
        );
        #[cfg(windows)]
        let selected = selected.expect("Windows target comparator selects a usable key");
        #[cfg(not(windows))]
        assert_eq!(
            selected,
            Err(WindowsBrowserBootstrapError::KeyCollision),
            "a non-Windows shadow must fail closed when target equality is unknowable"
        );

        let empty = environment(&[]);
        let bootstrap = build_windows_browser_bootstrap(
            1,
            [2; 32],
            &empty,
            [WindowsBootstrapKeyAttempt::Bytes([0x11; 32])],
        )
        .unwrap();
        let mut with_alias = empty;
        with_alias.insert(alias, RuntimeString::from("caller"));
        with_alias.insert(
            bootstrap.environment_key().clone(),
            bootstrap.environment_value().clone(),
        );
        let parsed = parse_windows_browser_bootstrap(&[bootstrap.argv_token()], &with_alias);
        #[cfg(not(windows))]
        assert_eq!(parsed, Err(WindowsBrowserBootstrapError::InvalidKey));
        #[cfg(windows)]
        if alias_collides {
            assert_eq!(parsed, Err(WindowsBrowserBootstrapError::InvalidKey));
        } else {
            let parsed = parsed.expect("a comparator-different alias must not block parsing");
            assert_eq!(
                parsed.environment_key(),
                bootstrap.environment_key(),
                "a key the comparator calls different must not displace the exact one"
            );
        }

        #[cfg(windows)]
        {
            let expected_tail = if alias_collides { "22" } else { "11" };
            assert!(selected
                .environment_key()
                .try_to_utf8()
                .unwrap()
                .ends_with(&expected_tail.repeat(32)));
        }
    }

    #[test]
    fn entropy_and_argument_failures_are_distinct_and_fail_closed() {
        let empty = environment(&[]);
        assert_eq!(
            build_windows_browser_bootstrap(1, [0; 32], &empty, []),
            Err(WindowsBrowserBootstrapError::EntropyUnavailable)
        );
        assert_eq!(
            build_windows_browser_bootstrap(
                1,
                [0; 32],
                &empty,
                [WindowsBootstrapKeyAttempt::Unavailable]
            ),
            Err(WindowsBrowserBootstrapError::EntropyUnavailable)
        );
        assert_eq!(
            parse_windows_browser_bootstrap(&[], &empty),
            Err(WindowsBrowserBootstrapError::MissingArgument)
        );
        let duplicate = RuntimeString::from_utf8(&format!(
            "--klaus-browser-broker-env-key=KLAUS_INTERNAL_BROWSER_BROKER_{}",
            "AA".repeat(32)
        ));
        assert_eq!(
            parse_windows_browser_bootstrap(&[duplicate.clone(), duplicate], &empty),
            Err(WindowsBrowserBootstrapError::DuplicateArgument)
        );
    }

    #[test]
    fn parser_rejects_noncanonical_or_tampered_payloads() {
        let empty = environment(&[]);
        let bootstrap = build_windows_browser_bootstrap(
            5,
            [6; 32],
            &empty,
            [WindowsBootstrapKeyAttempt::Bytes([7; 32])],
        )
        .unwrap();
        let key = bootstrap.environment_key().clone();
        let argument = bootstrap.argv_token();
        for value in [
            RuntimeString::from("v1.A="),
            RuntimeString::from("v1.*"),
            RuntimeString::from("v2.AA"),
        ] {
            let environment =
                environment(&[(&key.try_to_utf8().unwrap(), &value.try_to_utf8().unwrap())]);
            assert!(
                parse_windows_browser_bootstrap(std::slice::from_ref(&argument), &environment)
                    .is_err()
            );
        }

        let mut encoded = runtime_ascii_owned(bootstrap.environment_value()).unwrap();
        let last = encoded.last_mut().unwrap();
        *last = if *last == b'A' { b'B' } else { b'A' };
        let tampered_environment = environment(&[(
            &key.try_to_utf8().unwrap(),
            std::str::from_utf8(&encoded).unwrap(),
        )]);
        assert!(parse_windows_browser_bootstrap(&[argument], &tampered_environment).is_err());

        let oversized = format!("v1.{}", "A".repeat(MAX_BOOTSTRAP_BASE64URL_BYTES + 1));
        let oversized_environment = environment(&[(&key.try_to_utf8().unwrap(), &oversized)]);
        assert_eq!(
            parse_windows_browser_bootstrap(&[bootstrap.argv_token()], &oversized_environment),
            Err(WindowsBrowserBootstrapError::PayloadTooLarge)
        );
    }
}
