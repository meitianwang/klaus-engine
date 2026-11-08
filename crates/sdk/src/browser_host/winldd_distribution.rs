//! Fail-closed admission of Playwright's Windows `PrintDeps.exe` helper.
//!
//! The broker request is JavaScript-controlled, so its executable spelling is
//! evidence only.  Authority comes from the published browser runtime lock and
//! independently verified artifact/receipt identities recorded by that lock,
//! bound to the workload identity in `distribution_source`.  A source
//! distribution has neither a configured issuer nor verified artifacts, so the
//! missing evidence rejects every kind-6 spawn.

use std::fs::File;
use std::io::Read;
use std::path::{Path, PathBuf};

use klaus_agent_core::runtime_process::RuntimeBrowserWinlddExecutable;
use klaus_agent_core::runtime_values::{RuntimePath, RuntimeString};
use serde::Deserialize;
use sha2::{Digest, Sha256};

use super::distribution_source::{WorkloadIdentity, WORKLOAD_IDENTITY};

const EXACT_RUNTIME_LOCK_BYTES: &[u8] = include_bytes!(
    "../../assets/exact-browser-runtime-assets-v1/exact-browser-runtime-assets.lock.json"
);

// These four values are the browser.runtimeLock identity in
// provider-assets.lock.json.  A distribution update has to change that lock
// and this audited admission boundary together.
const PROVIDER_RUNTIME_LOCK_SIZE: usize = 12_275;
const PROVIDER_RUNTIME_LOCK_SHA256: &str =
    "f507a8c26487e3877d9d5e5898a592df75d979168474a1e69346f4e20d6759ac";
const PROVIDER_RUNTIME_CANDIDATE_SHA256: &str =
    "65997edc8a5d63348549d285a34c436c150dc218b21a568598ab5a24348782b7";
const PROVIDER_RUNTIME_PUBLISHED_SHA256: &str =
    "3586fd818dbc71d0ccd67924fe7fe8f299e4f5f595e133c9711d60c75e723438";

const WINLDD_KIND: &str = "winldd";
const WINLDD_TARGET: &str = "win-x64";
const WINLDD_REVISION: &str = "1007";
const WINLDD_ARCHIVE_FILE: &str = "browser-archives/winldd-win64.zip";
const WINLDD_EXECUTABLE: &str = "PrintDeps.exe";
const WINLDD_ARCHIVE_SIZE: u64 = 128_684;
const WINLDD_ARCHIVE_SHA256: &str =
    "0069f0d11d4ad6df068a068c003d22fe7dbec192a47bba64b2e115e9c8ce41d8";
const EXTERNAL_RECEIPT_FILE: &str = "browser-runtime-assets.receipt.json";
/// Lock descriptor for inputs only the external distribution step can supply.
/// Presence of the matching `artifact` (not this string) is what admits them.
const EXTERNAL_INPUT_AVAILABILITY: &str = "required-distribution-input";
const PINNED_ASSET_ERROR: &str = "Pinned Playwright 1.61.1 asset is missing or corrupt";

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ExactRuntimeLock {
    published: bool,
    candidate_sha256: String,
    published_sha256: String,
    external_inputs: ExternalInputs,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ExternalInputs {
    browser_archives: Vec<BrowserArchive>,
    protected_identity: ProtectedIdentity,
    receipt: ReceiptRequirement,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct BrowserArchive {
    kind: String,
    target: String,
    revision: String,
    required_file: String,
    executable_path: String,
    size: u64,
    sha256: String,
    availability: String,
    artifact: Option<WinlddArtifact>,
}

/// Identity of the extracted executable, supplied only by the externally
/// verified distribution step.  Archive size/SHA are separate fields because
/// they are not the identity of the extracted PE file.
#[derive(Clone, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct WinlddArtifact {
    absolute_path: String,
    size: u64,
    sha256: String,
    receipt_sha256: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ProtectedIdentity {
    repository: Option<String>,
    r#ref: Option<String>,
    job_id: Option<String>,
    required_bindings: Vec<String>,
    trust_anchor_source: String,
    trust_anchor_sha256: Option<String>,
    self_asserted_receipt_fields_are_proof: bool,
    availability: String,
    artifact: Option<VerifiedIdentityArtifact>,
}

#[derive(Clone, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct VerifiedIdentityArtifact {
    verified: bool,
    trust_anchor_sha256: String,
    receipt_sha256: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ReceiptRequirement {
    required_file: String,
    availability: String,
    artifact: Option<ReceiptArtifact>,
}

#[derive(Clone, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ReceiptArtifact {
    absolute_path: String,
    size: u64,
    sha256: String,
    receipt_sha256: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct DistributionReceipt {
    version: u8,
    candidate_sha256: String,
    workload: DistributionWorkload,
    browser_archives: Vec<ReceiptArchive>,
    receipt_sha256: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct DistributionWorkload {
    repository: String,
    r#ref: String,
    pipeline_id: String,
    run_id: String,
    job_id: String,
    manual_approval_id: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ReceiptArchive {
    required_file: String,
    size: u64,
    sha256: String,
}

#[derive(Clone)]
struct WinlddDistributionEvidence {
    executable: WinlddArtifact,
    receipt: ReceiptArtifact,
    identity: VerifiedIdentityArtifact,
    trust_anchor_sha256: [u8; 32],
    workload: WorkloadIdentity,
}

pub(super) fn verified_winldd_from_distribution(
    requested_executable: RuntimeString,
) -> Result<RuntimeBrowserWinlddExecutable, RuntimeString> {
    if !cfg!(all(target_os = "windows", target_arch = "x86_64")) {
        return Err(RuntimeString::from(PINNED_ASSET_ERROR));
    }
    let evidence =
        evidence_from_provider_lock().map_err(|_| RuntimeString::from(PINNED_ASSET_ERROR))?;
    verify_distribution_evidence(&requested_executable, &evidence)
        .map_err(|_| RuntimeString::from(PINNED_ASSET_ERROR))
}

/// Parse the embedded runtime lock after checking its pinned size and digests.
fn validated_provider_lock() -> Result<ExactRuntimeLock, ()> {
    if EXACT_RUNTIME_LOCK_BYTES.len() != PROVIDER_RUNTIME_LOCK_SIZE
        || sha256(EXACT_RUNTIME_LOCK_BYTES)
            != parse_sha256(PROVIDER_RUNTIME_LOCK_SHA256).ok_or(())?
    {
        return Err(());
    }
    let lock: ExactRuntimeLock =
        serde_json::from_slice(EXACT_RUNTIME_LOCK_BYTES).map_err(|_| ())?;
    if !lock.published
        || lock.candidate_sha256 != PROVIDER_RUNTIME_CANDIDATE_SHA256
        || lock.published_sha256 != PROVIDER_RUNTIME_PUBLISHED_SHA256
    {
        return Err(());
    }
    Ok(lock)
}

fn evidence_from_provider_lock() -> Result<WinlddDistributionEvidence, ()> {
    let lock = validated_provider_lock()?;
    let workload = WORKLOAD_IDENTITY.ok_or(())?;
    let mut matching = lock
        .external_inputs
        .browser_archives
        .iter()
        .filter(|entry| entry.kind == WINLDD_KIND && entry.target == WINLDD_TARGET);
    let archive = matching.next().ok_or(())?;
    if matching.next().is_some()
        || archive.revision != WINLDD_REVISION
        || archive.required_file != WINLDD_ARCHIVE_FILE
        || archive.executable_path != WINLDD_EXECUTABLE
        || archive.size != WINLDD_ARCHIVE_SIZE
        || archive.sha256 != WINLDD_ARCHIVE_SHA256
        || archive.availability != EXTERNAL_INPUT_AVAILABILITY
    {
        return Err(());
    }

    let protected = &lock.external_inputs.protected_identity;
    let required_bindings = [
        "repository",
        "ref",
        "pipelineId",
        "runId",
        "jobId",
        "manualApprovalId",
        "candidateSha256",
        "receiptSha256",
    ];
    if protected.repository.as_deref() != Some(workload.repository)
        || protected.r#ref.as_deref() != Some(workload.reference)
        || protected.job_id.as_deref() != Some(workload.job_id)
        || protected
            .required_bindings
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>()
            != required_bindings
        || protected.trust_anchor_source != "platform-administrator-pinned"
        || protected.self_asserted_receipt_fields_are_proof
        || protected.availability != "required-platform-administrator-input"
    {
        return Err(());
    }
    let trust_anchor_sha256 =
        parse_sha256(protected.trust_anchor_sha256.as_deref().ok_or(())?).ok_or(())?;
    let identity = protected.artifact.clone().ok_or(())?;
    if !identity.verified
        || trust_anchor_sha256 != workload.trust_anchor_sha256
        || parse_sha256(&identity.trust_anchor_sha256).ok_or(())? != trust_anchor_sha256
    {
        return Err(());
    }

    let receipt_requirement = &lock.external_inputs.receipt;
    if receipt_requirement.required_file != EXTERNAL_RECEIPT_FILE
        || receipt_requirement.availability != EXTERNAL_INPUT_AVAILABILITY
    {
        return Err(());
    }
    let executable = archive.artifact.clone().ok_or(())?;
    let receipt = receipt_requirement.artifact.clone().ok_or(())?;
    if executable.receipt_sha256 != receipt.receipt_sha256
        || executable.receipt_sha256 != identity.receipt_sha256
        || parse_sha256(&executable.sha256).is_none()
        || parse_sha256(&receipt.sha256).is_none()
        || parse_sha256(&executable.receipt_sha256).is_none()
        || executable.size == 0
        || receipt.size == 0
    {
        return Err(());
    }
    Ok(WinlddDistributionEvidence {
        executable,
        receipt,
        identity,
        trust_anchor_sha256,
        workload,
    })
}

fn verify_distribution_evidence(
    requested_executable: &RuntimeString,
    evidence: &WinlddDistributionEvidence,
) -> Result<RuntimeBrowserWinlddExecutable, ()> {
    let expected_runtime_path = RuntimePath::from(evidence.executable.absolute_path.clone());
    if requested_executable != expected_runtime_path.as_runtime_string()
        || evidence.trust_anchor_sha256 != evidence.workload.trust_anchor_sha256
        || !evidence.identity.verified
        || parse_sha256(&evidence.identity.trust_anchor_sha256).ok_or(())?
            != evidence.trust_anchor_sha256
        || evidence.identity.receipt_sha256 != evidence.executable.receipt_sha256
        || evidence.receipt.receipt_sha256 != evidence.executable.receipt_sha256
    {
        return Err(());
    }

    let executable_path = PathBuf::from(&evidence.executable.absolute_path);
    let receipt_path = PathBuf::from(&evidence.receipt.absolute_path);
    if !executable_path.is_absolute() || !receipt_path.is_absolute() {
        return Err(());
    }
    verify_regular_file(
        &executable_path,
        evidence.executable.size,
        parse_sha256(&evidence.executable.sha256).ok_or(())?,
    )?;
    verify_regular_file(
        &receipt_path,
        evidence.receipt.size,
        parse_sha256(&evidence.receipt.sha256).ok_or(())?,
    )?;

    let receipt_bytes = std::fs::read(&receipt_path).map_err(|_| ())?;
    if receipt_bytes.last() != Some(&b'\n') || !receipt_bytes.is_ascii() {
        return Err(());
    }
    let receipt: DistributionReceipt = serde_json::from_slice(&receipt_bytes).map_err(|_| ())?;
    if receipt.version != 2
        || receipt.candidate_sha256 != PROVIDER_RUNTIME_CANDIDATE_SHA256
        || receipt.receipt_sha256 != evidence.executable.receipt_sha256
        || receipt.workload.repository != evidence.workload.repository
        || receipt.workload.r#ref != evidence.workload.reference
        || receipt.workload.job_id != evidence.workload.job_id
        || !canonical_binding(&receipt.workload.pipeline_id)
        || !canonical_binding(&receipt.workload.run_id)
        || !canonical_binding(&receipt.workload.manual_approval_id)
    {
        return Err(());
    }
    let mut winldd_rows = receipt
        .browser_archives
        .iter()
        .filter(|row| row.required_file == WINLDD_ARCHIVE_FILE);
    let winldd_row = winldd_rows.next().ok_or(())?;
    if winldd_rows.next().is_some()
        || winldd_row.size != WINLDD_ARCHIVE_SIZE
        || winldd_row.sha256 != WINLDD_ARCHIVE_SHA256
    {
        return Err(());
    }

    // SAFETY: the immutable provider lock, the configured workload identity and
    // its trust anchor, the external identity receipt, exact
    // requested absolute path, regular-file status, size, and SHA-256 were all
    // verified above.  None of those values came from the JS request.
    unsafe {
        RuntimeBrowserWinlddExecutable::from_attested_distribution_path(expected_runtime_path)
    }
    .map_err(|_| ())
}

fn verify_regular_file(
    path: &Path,
    expected_size: u64,
    expected_sha256: [u8; 32],
) -> Result<(), ()> {
    let metadata = std::fs::symlink_metadata(path).map_err(|_| ())?;
    if !metadata.file_type().is_file()
        || metadata.file_type().is_symlink()
        || metadata.len() != expected_size
    {
        return Err(());
    }
    let mut file = File::open(path).map_err(|_| ())?;
    if sha256_reader(&mut file).map_err(|_| ())? != expected_sha256 {
        return Err(());
    }
    Ok(())
}

fn canonical_binding(value: &str) -> bool {
    !value.is_empty() && value.trim() == value && !value.contains('\0')
}

fn sha256(bytes: &[u8]) -> [u8; 32] {
    Sha256::digest(bytes).into()
}

fn sha256_reader(reader: &mut impl Read) -> std::io::Result<[u8; 32]> {
    let mut digest = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = reader.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        digest.update(&buffer[..read]);
    }
    Ok(digest.finalize().into())
}

fn parse_sha256(value: &str) -> Option<[u8; 32]> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    {
        return None;
    }
    let mut output = [0_u8; 32];
    for (index, chunk) in value.as_bytes().chunks_exact(2).enumerate() {
        output[index] = (hex_nibble(chunk[0])? << 4) | hex_nibble(chunk[1])?;
    }
    Some(output)
}

fn hex_nibble(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::*;

    fn temp_root() -> PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = std::env::temp_dir().join(format!(
            "klaus-winldd-distribution-{}-{nonce}",
            std::process::id()
        ));
        std::fs::create_dir_all(&root).unwrap();
        root
    }

    #[test]
    fn shipped_provider_lock_keeps_winldd_fail_closed_without_external_artifacts() {
        // The embedded lock itself is intact; it is the missing issuer and
        // artifacts that keep admission closed.
        assert!(validated_provider_lock().is_ok());
        assert!(WORKLOAD_IDENTITY.is_none());
        assert!(evidence_from_provider_lock().is_err());
    }

    #[test]
    fn verified_evidence_binds_exact_path_file_hash_and_receipt() {
        let root = temp_root();
        let executable_path = root.join(WINLDD_EXECUTABLE);
        let executable_bytes = b"attested PrintDeps executable";
        std::fs::write(&executable_path, executable_bytes).unwrap();

        let workload = WorkloadIdentity {
            repository: "fixture/repository",
            reference: "fixture-ref",
            job_id: "fixture-job",
            trust_anchor_sha256: [0xaa; 32],
        };
        let receipt_digest = "11".repeat(32);
        let receipt_path = root.join(EXTERNAL_RECEIPT_FILE);
        let receipt_bytes = format!(
            "{{\"version\":2,\"candidateSha256\":\"{PROVIDER_RUNTIME_CANDIDATE_SHA256}\",\"workload\":{{\"repository\":\"fixture/repository\",\"ref\":\"fixture-ref\",\"pipelineId\":\"pipeline-1\",\"runId\":\"run-1\",\"jobId\":\"fixture-job\",\"manualApprovalId\":\"approval-1\"}},\"browserArchives\":[{{\"requiredFile\":\"{WINLDD_ARCHIVE_FILE}\",\"size\":{WINLDD_ARCHIVE_SIZE},\"sha256\":\"{WINLDD_ARCHIVE_SHA256}\"}}],\"windowsAddons\":[],\"receiptSha256\":\"{receipt_digest}\"}}\n"
        );
        std::fs::write(&receipt_path, receipt_bytes.as_bytes()).unwrap();
        let trust_anchor = [0xaa_u8; 32];
        let evidence = WinlddDistributionEvidence {
            executable: WinlddArtifact {
                absolute_path: executable_path.to_string_lossy().into_owned(),
                size: executable_bytes.len() as u64,
                sha256: format!("{:x}", Sha256::digest(executable_bytes)),
                receipt_sha256: receipt_digest.clone(),
            },
            receipt: ReceiptArtifact {
                absolute_path: receipt_path.to_string_lossy().into_owned(),
                size: receipt_bytes.len() as u64,
                sha256: format!("{:x}", Sha256::digest(receipt_bytes.as_bytes())),
                receipt_sha256: receipt_digest.clone(),
            },
            identity: VerifiedIdentityArtifact {
                verified: true,
                trust_anchor_sha256: "aa".repeat(32),
                receipt_sha256: receipt_digest,
            },
            trust_anchor_sha256: trust_anchor,
            workload,
        };

        let requested = RuntimeString::from(evidence.executable.absolute_path.clone());
        assert!(verify_distribution_evidence(&requested, &evidence).is_ok());

        let same_basename_elsewhere = RuntimeString::from(
            root.join("attacker")
                .join(WINLDD_EXECUTABLE)
                .to_string_lossy()
                .into_owned(),
        );
        assert!(verify_distribution_evidence(&same_basename_elsewhere, &evidence).is_err());

        let mut invalid = evidence.clone();
        invalid.identity.verified = false;
        assert!(verify_distribution_evidence(&requested, &invalid).is_err());
        invalid = evidence.clone();
        invalid.workload.repository = "wrong/repository";
        assert!(verify_distribution_evidence(&requested, &invalid).is_err());
        invalid = evidence.clone();
        invalid.workload.job_id = "wrong-job";
        assert!(verify_distribution_evidence(&requested, &invalid).is_err());
        invalid = evidence.clone();
        invalid.identity.receipt_sha256 = "00".repeat(32);
        assert!(verify_distribution_evidence(&requested, &invalid).is_err());
        invalid = evidence.clone();
        invalid.workload.trust_anchor_sha256 = [0; 32];
        assert!(verify_distribution_evidence(&requested, &invalid).is_err());

        std::fs::write(&executable_path, b"tampered").unwrap();
        assert!(verify_distribution_evidence(&requested, &evidence).is_err());
        std::fs::remove_dir_all(root).unwrap();
    }
}
