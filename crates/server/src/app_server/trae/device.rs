//! Allocate an independent device identity for browser authorization.

use aes::cipher::{block_padding::Pkcs7, BlockEncryptMut, KeyIvInit};
use rand::RngCore;
use serde_json::json;
use sha2::{Digest, Sha512};
use std::io::Write;

const PUBLIC_SALT: [u8; 64] = [
    77, 212, 194, 230, 184, 49, 98, 9, 14, 82, 179, 199, 166, 115, 59, 164, 28, 178, 70, 43, 130,
    154, 181, 138, 25, 107, 57, 219, 87, 23, 117, 36, 244, 155, 175, 127, 8, 232, 214, 141, 38,
    167, 46, 55, 193, 169, 90, 47, 31, 5, 165, 24, 146, 174, 242, 148, 151, 50, 182, 42, 56, 170,
    221, 88,
];

fn encode(value: &serde_json::Value) -> Result<Vec<u8>, String> {
    let json = serde_json::to_vec(value).map_err(|_| "trae_device_registration_invalid")?;
    let mut compressor = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    compressor
        .write_all(&json)
        .map_err(|_| "trae_device_registration_invalid")?;
    let compressed = compressor
        .finish()
        .map_err(|_| "trae_device_registration_invalid")?;
    let mut seed = [0u8; 32];
    rand::rngs::OsRng
        .try_fill_bytes(&mut seed)
        .map_err(|_| "trae_login_random_failed")?;
    let mut material = Sha512::new();
    material.update(Sha512::digest(seed));
    material.update(PUBLIC_SALT);
    let material = material.finalize();
    let mut plain = Sha512::digest(&compressed).to_vec();
    plain.extend_from_slice(&compressed);
    let cipher = cbc::Encryptor::<aes::Aes128>::new_from_slices(&material[..16], &material[16..32])
        .map_err(|_| "trae_device_registration_invalid")?
        .encrypt_padded_vec_mut::<Pkcs7>(&plain);
    let mut body = vec![116, 99, 5, 16, 0, 0];
    body.extend_from_slice(&seed);
    body.extend_from_slice(&cipher);
    Ok(body)
}

pub(super) fn os_version() -> String {
    #[cfg(target_os = "macos")]
    {
        if let Ok(text) =
            std::fs::read_to_string("/System/Library/CoreServices/SystemVersion.plist")
        {
            if let Some((_, value)) = text.split_once("<key>ProductVersion</key>") {
                if let Some((_, value)) = value.split_once("<string>") {
                    if let Some((version, _)) = value.split_once("</string>") {
                        if !version.is_empty()
                            && version.len() <= 64
                            && version
                                .bytes()
                                .all(|byte| byte.is_ascii_digit() || byte == b'.')
                        {
                            return version.into();
                        }
                    }
                }
            }
        }
    }
    String::new()
}

struct Hardware {
    uuid: String,
    mac: String,
    product: String,
    sku: String,
}

#[cfg(any(target_os = "macos", test))]
fn registry_property(registry: &str, key: &str) -> Option<String> {
    let prefix = format!("\"{key}\" = ");
    let value = registry
        .lines()
        .find_map(|line| line.trim().strip_prefix(&prefix))?;
    if let Some(value) = value
        .strip_prefix('"')
        .and_then(|value| value.strip_suffix('"'))
        .or_else(|| {
            value
                .strip_prefix("<\"")
                .and_then(|value| value.strip_suffix("\">"))
        })
    {
        return Some(value.to_owned());
    }
    let hex = value.strip_prefix('<')?.strip_suffix('>')?;
    if hex.len() > 8192 || hex.len() % 2 != 0 || !hex.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return None;
    }
    let decoded = (0..hex.len())
        .step_by(2)
        .map(|index| u8::from_str_radix(&hex[index..index + 2], 16).ok())
        .collect::<Option<Vec<_>>>()?;
    String::from_utf8(decoded)
        .ok()
        .map(|value| value.trim_end_matches('\0').to_owned())
}

#[cfg(any(target_os = "macos", test))]
fn parse_hardware(registry: &str, interfaces: &str, product: &str) -> Result<Hardware, String> {
    let uuid =
        registry_property(registry, "IOPlatformUUID").ok_or("trae_device_identity_unavailable")?;
    if uuid.len() > 64
        || uuid.is_empty()
        || !uuid
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() || byte == b'-')
    {
        return Err("trae_device_identity_unavailable".into());
    }
    let mac = interfaces
        .lines()
        .find_map(|line| line.trim().strip_prefix("ether "))
        .and_then(|value| value.split_whitespace().next())
        .ok_or("trae_device_identity_unavailable")?;
    if mac.split(':').count() != 6
        || mac
            .split(':')
            .any(|value| value.len() != 2 || !value.bytes().all(|byte| byte.is_ascii_hexdigit()))
    {
        return Err("trae_device_identity_unavailable".into());
    }
    let product = product.trim();
    if product.is_empty() || product.len() > 256 || product.chars().any(char::is_control) {
        return Err("trae_device_identity_unavailable".into());
    }
    let sku = registry_property(registry, "board-id").unwrap_or_default();
    if sku.len() > 256 || sku.chars().any(char::is_control) {
        return Err("trae_device_identity_unavailable".into());
    }
    Ok(Hardware {
        uuid,
        mac: mac.to_owned(),
        product: product.to_owned(),
        sku,
    })
}

#[cfg(target_os = "macos")]
async fn hardware_command(program: &str, args: &[&str]) -> Result<String, String> {
    let output = tokio::time::timeout(
        std::time::Duration::from_secs(3),
        tokio::process::Command::new(program)
            .args(args)
            .kill_on_drop(true)
            .output(),
    )
    .await
    .map_err(|_| "trae_device_identity_unavailable")?
    .map_err(|_| "trae_device_identity_unavailable")?;
    if !output.status.success() || output.stdout.len() > 256 * 1024 {
        return Err("trae_device_identity_unavailable".into());
    }
    String::from_utf8(output.stdout).map_err(|_| "trae_device_identity_unavailable".into())
}

async fn hardware() -> Result<Hardware, String> {
    #[cfg(target_os = "macos")]
    {
        let registry =
            hardware_command("/usr/sbin/ioreg", &["-rd1", "-c", "IOPlatformExpertDevice"]).await?;
        let interfaces = hardware_command("/sbin/ifconfig", &["en0"]).await?;
        let product = hardware_command("/usr/sbin/sysctl", &["-n", "hw.product"]).await?;
        parse_hardware(&registry, &interfaces, &product)
    }
    #[cfg(not(target_os = "macos"))]
    {
        Err("trae_device_platform_unsupported".into())
    }
}

pub(super) async fn register() -> Result<String, String> {
    let base = super::endpoint("KLAUS_TRAE_DEVICE_URL", "https://log.snssdk.com");
    let platform = match std::env::consts::OS {
        "macos" => "MacOS",
        "windows" => "Windows",
        other => other,
    };
    let version = os_version();
    let hardware = hardware().await?;
    let local = chrono::Local::now();
    let offset = local.offset().local_minus_utc();
    let mut url = url::Url::parse(&format!("{base}/service/2/desktop/device_register/"))
        .map_err(|_| "trae_device_registration_invalid")?;
    url.query_pairs_mut().extend_pairs([
        ("aid", "787976"),
        ("channel", "stable"),
        ("os", platform),
        ("os_version", &version),
        ("device_platform", platform),
        ("version_code", "3.4.7"),
        ("macos_uuid", &hardware.uuid),
        ("macos_serial", ""),
    ]);
    let body = encode(&json!({
        "header":{"device_id":0,"install_id":0,"aid":787976,"os":platform,"device_platform":platform,
            "mc":hardware.mac,"channel":"stable","package":"com.trae.app","language":"zh-CN","app_version":"3.4.7",
            "os_version":version,"device_model":hardware.product,"time_zone":offset/3600,
            "tz_name":local.format("%Z").to_string(),"tz_offset":offset,"app_region":"CN","app_language":"zh-CN",
            "display_name":"Trae CN","macos_uuid":hardware.uuid,"macos_serial":"","sku":hardware.sku},
        "_gen_time":chrono::Utc::now().timestamp_millis(),"magic_tag":"ss_app_log",
    }))?;
    let response = super::client()
        .post(url)
        .header("content-type", "application/json")
        .body(body)
        .send()
        .await
        .map_err(|_| "trae_device_registration_failed")?;
    let response = super::bounded_json(response).await?;
    registered_id(&response).ok_or_else(|| "trae_device_registration_invalid".into())
}

fn registered_id(response: &serde_json::Value) -> Option<String> {
    let id = response
        .get("device_id_str")
        .and_then(serde_json::Value::as_str)
        .map(str::to_owned)
        .or_else(|| {
            response
                .get("device_id")
                .and_then(serde_json::Value::as_u64)
                .map(|id| id.to_string())
        })?;
    (!id.is_empty() && id != "0" && id.len() <= 32 && id.bytes().all(|byte| byte.is_ascii_digit()))
        .then_some(id)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn hardware_parsing_keeps_exact_observed_values_and_does_not_require_serial_or_board_id() {
        let fixture="    \"IOPlatformUUID\" = \"AABBCCDD-1234-5678-90AB-ABCDEFABCDEF\"\n    \"board-id\" = <4d61632d5465737400>";
        let hardware = parse_hardware(fixture, "ether 00:11:22:33:44:55\n", "MacTest\n").unwrap();
        assert_eq!(hardware.uuid, "AABBCCDD-1234-5678-90AB-ABCDEFABCDEF");
        assert_eq!(hardware.mac, "00:11:22:33:44:55");
        assert_eq!(hardware.product, "MacTest");
        assert_eq!(hardware.sku, "Mac-Test");
        assert_eq!(
            parse_hardware(
                "\"IOPlatformUUID\" = \"ABCD\"",
                "ether 00:11:22:33:44:55",
                "Model"
            )
            .unwrap()
            .sku,
            ""
        );
        assert!(parse_hardware(
            "\"IOPlatformUUID\" = \"\"",
            "ether 00:11:22:33:44:55",
            "Model"
        )
        .is_err());
        assert!(parse_hardware(fixture, "ether invalid", "Model").is_err());
    }
    #[test]
    fn registry_ids_preserve_large_integer_precision_and_reject_missing_identity() {
        let response: serde_json::Value =
            serde_json::from_str("{\"device_id\":9007199254740993123,\"install_id\":123}").unwrap();
        assert_eq!(registered_id(&response).unwrap(), "9007199254740993123");
        assert_eq!(
            registered_id(&serde_json::json!({"device_id_str":"987654321"})).unwrap(),
            "987654321"
        );
        assert!(registered_id(&serde_json::json!({"device_id":0})).is_none());
        assert!(registered_id(&serde_json::json!({"device_id":12.5})).is_none());
    }

    #[test]
    fn registry_transport_round_trips_compressed_authenticated_binary_envelopes() {
        use aes::cipher::BlockDecryptMut;
        use std::io::Read;
        let original = serde_json::json!({"header":{"device_id":0,"macos_uuid":"","macos_serial":""},"magic_tag":"ss_app_log"});
        let body = encode(&original).unwrap();
        assert_eq!(&body[..6], &[116, 99, 5, 16, 0, 0]);
        let mut material = Sha512::new();
        material.update(Sha512::digest(&body[6..38]));
        material.update(PUBLIC_SALT);
        let material = material.finalize();
        let plain =
            cbc::Decryptor::<aes::Aes128>::new_from_slices(&material[..16], &material[16..32])
                .unwrap()
                .decrypt_padded_vec_mut::<Pkcs7>(&body[38..])
                .unwrap();
        assert_eq!(&plain[..64], &Sha512::digest(&plain[64..])[..]);
        let mut json = String::new();
        flate2::read::GzDecoder::new(&plain[64..])
            .read_to_string(&mut json)
            .unwrap();
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&json).unwrap(),
            original
        );
        assert_ne!(encode(&original).unwrap(), body);
    }
}
