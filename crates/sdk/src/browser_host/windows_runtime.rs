//! Production KBB1 startup for the parent-owned Windows browser broker pipes.

use std::time::Instant;

use klaus_agent_core::runtime_process::{RuntimeBrowserBrokerRead, RuntimeBrowserBrokerWrite};
use rand::RngCore;
use sha2::{Digest, Sha256};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use super::broker_wire::{BrowserBootstrapRecord, WindowsBootstrapPhase, WindowsBrokerLanePair};

const RECORD_BYTES: usize = 48;

pub(crate) struct WindowsBrowserReadyEndpoints {
    pub(crate) sync_inbound: RuntimeBrowserBrokerRead,
    pub(crate) sync_outbound: RuntimeBrowserBrokerWrite,
    pub(crate) mux_inbound: RuntimeBrowserBrokerRead,
    pub(crate) mux_outbound: RuntimeBrowserBrokerWrite,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum WindowsBrowserStartupError {
    EntropyUnavailable,
    BootstrapTimedOut,
    NativeBinding,
    PackageReadiness,
}

impl WindowsBrowserStartupError {
    pub(crate) const fn message(self) -> &'static str {
        match self {
            Self::EntropyUnavailable => "BrowserBootstrapEntropyUnavailable",
            Self::BootstrapTimedOut => "Browser broker bootstrap timed out.",
            Self::NativeBinding => "Browser broker native pipe binding failed.",
            Self::PackageReadiness => "Browser broker package readiness failed.",
        }
    }
}

pub(crate) async fn start_windows_browser_broker(
    generation: u64,
    bootstrap_nonce: [u8; 32],
    client_pid: u32,
    deadline: Instant,
    sync_io: (RuntimeBrowserBrokerRead, RuntimeBrowserBrokerWrite),
    mux_io: (RuntimeBrowserBrokerRead, RuntimeBrowserBrokerWrite),
) -> Result<WindowsBrowserReadyEndpoints, WindowsBrowserStartupError> {
    let (mut sync_inbound, mut sync_outbound) = sync_io;
    let (mut mux_inbound, mut mux_outbound) = mux_io;
    let mut sync_hello = [0_u8; 32];
    let mut mux_hello = [0_u8; 32];
    let mut adoption = [0_u8; 32];
    rand::rngs::OsRng
        .try_fill_bytes(&mut sync_hello)
        .map_err(|_| WindowsBrowserStartupError::EntropyUnavailable)?;
    rand::rngs::OsRng
        .try_fill_bytes(&mut mux_hello)
        .map_err(|_| WindowsBrowserStartupError::EntropyUnavailable)?;
    rand::rngs::OsRng
        .try_fill_bytes(&mut adoption)
        .map_err(|_| WindowsBrowserStartupError::EntropyUnavailable)?;

    let mut commit_digest = adoption_commit_digest(
        generation,
        &bootstrap_nonce,
        &sync_hello,
        &mux_hello,
        &adoption,
        client_pid,
    );
    let adoption_result =
        tokio::time::timeout_at(tokio::time::Instant::from_std(deadline), async {
            exchange_pair(
                &mut sync_inbound,
                &mut sync_outbound,
                &mut mux_inbound,
                &mut mux_outbound,
                generation,
                WindowsBootstrapPhase::ServerHello,
                WindowsBootstrapPhase::ClientHello,
                &sync_hello,
                &mux_hello,
            )
            .await?;
            exchange_pair(
                &mut sync_inbound,
                &mut sync_outbound,
                &mut mux_inbound,
                &mut mux_outbound,
                generation,
                WindowsBootstrapPhase::AdoptionChallenge,
                WindowsBootstrapPhase::AdoptionEcho,
                &adoption,
                &adoption,
            )
            .await?;
            exchange_pair(
                &mut sync_inbound,
                &mut sync_outbound,
                &mut mux_inbound,
                &mut mux_outbound,
                generation,
                WindowsBootstrapPhase::Commit,
                WindowsBootstrapPhase::CommitEcho,
                &commit_digest,
                &commit_digest,
            )
            .await?;
            write_pair(
                &mut sync_outbound,
                &mut mux_outbound,
                generation,
                WindowsBootstrapPhase::AdoptionAck,
                &commit_digest,
                &commit_digest,
            )
            .await
        })
        .await;
    sync_hello.fill(0);
    mux_hello.fill(0);
    adoption.fill(0);
    match adoption_result {
        Err(_) => {
            commit_digest.fill(0);
            return Err(WindowsBrowserStartupError::BootstrapTimedOut);
        }
        Ok(Err(())) => {
            commit_digest.fill(0);
            return Err(WindowsBrowserStartupError::NativeBinding);
        }
        Ok(Ok(())) => {}
    }

    let ready = read_pair(
        &mut sync_inbound,
        &mut mux_inbound,
        generation,
        WindowsBootstrapPhase::PackageReady,
        &commit_digest,
        &commit_digest,
    )
    .await;
    commit_digest.fill(0);
    ready.map_err(|()| WindowsBrowserStartupError::PackageReadiness)?;
    Ok(WindowsBrowserReadyEndpoints {
        sync_inbound,
        sync_outbound,
        mux_inbound,
        mux_outbound,
    })
}

#[allow(clippy::too_many_arguments)]
async fn exchange_pair(
    sync_inbound: &mut RuntimeBrowserBrokerRead,
    sync_outbound: &mut RuntimeBrowserBrokerWrite,
    mux_inbound: &mut RuntimeBrowserBrokerRead,
    mux_outbound: &mut RuntimeBrowserBrokerWrite,
    generation: u64,
    outbound_phase: WindowsBootstrapPhase,
    inbound_phase: WindowsBootstrapPhase,
    sync_token: &[u8; 32],
    mux_token: &[u8; 32],
) -> Result<(), ()> {
    write_pair(
        sync_outbound,
        mux_outbound,
        generation,
        outbound_phase,
        sync_token,
        mux_token,
    )
    .await?;
    read_pair(
        sync_inbound,
        mux_inbound,
        generation,
        inbound_phase,
        sync_token,
        mux_token,
    )
    .await
}

async fn write_pair(
    sync_outbound: &mut RuntimeBrowserBrokerWrite,
    mux_outbound: &mut RuntimeBrowserBrokerWrite,
    generation: u64,
    phase: WindowsBootstrapPhase,
    sync_token: &[u8; 32],
    mux_token: &[u8; 32],
) -> Result<(), ()> {
    let sync = BrowserBootstrapRecord::encode_windows_authenticated(
        phase,
        WindowsBrokerLanePair::Sync,
        generation,
        *sync_token,
    );
    let mux = BrowserBootstrapRecord::encode_windows_authenticated(
        phase,
        WindowsBrokerLanePair::Mux,
        generation,
        *mux_token,
    );
    tokio::try_join!(
        write_record(sync_outbound, &sync),
        write_record(mux_outbound, &mux)
    )?;
    Ok(())
}

async fn read_pair(
    sync_inbound: &mut RuntimeBrowserBrokerRead,
    mux_inbound: &mut RuntimeBrowserBrokerRead,
    generation: u64,
    phase: WindowsBootstrapPhase,
    sync_token: &[u8; 32],
    mux_token: &[u8; 32],
) -> Result<(), ()> {
    let (sync, mux) = tokio::try_join!(read_record(sync_inbound), read_record(mux_inbound))?;
    BrowserBootstrapRecord::decode_windows_authenticated(
        &sync,
        phase,
        WindowsBrokerLanePair::Sync,
        generation,
        sync_token,
    )
    .map_err(|_| ())?;
    BrowserBootstrapRecord::decode_windows_authenticated(
        &mux,
        phase,
        WindowsBrokerLanePair::Mux,
        generation,
        mux_token,
    )
    .map_err(|_| ())?;
    Ok(())
}

async fn write_record(
    output: &mut RuntimeBrowserBrokerWrite,
    record: &[u8; RECORD_BYTES],
) -> Result<(), ()> {
    output.write_all(record).await.map_err(|_| ())?;
    output.flush().await.map_err(|_| ())
}

async fn read_record(input: &mut RuntimeBrowserBrokerRead) -> Result<[u8; RECORD_BYTES], ()> {
    let mut record = [0_u8; RECORD_BYTES];
    input.read_exact(&mut record).await.map_err(|_| ())?;
    Ok(record)
}

fn adoption_commit_digest(
    generation: u64,
    bootstrap_nonce: &[u8; 32],
    sync_hello: &[u8; 32],
    mux_hello: &[u8; 32],
    adoption: &[u8; 32],
    client_pid: u32,
) -> [u8; 32] {
    let mut digest = Sha256::new();
    digest.update(b"KLAUS_BROWSER_ADOPTION_COMMIT_V1");
    digest.update([0]);
    digest.update(generation.to_be_bytes());
    digest.update(bootstrap_nonce);
    digest.update(sync_hello);
    digest.update(mux_hello);
    digest.update(adoption);
    digest.update(client_pid.to_be_bytes());
    digest.finalize().into()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn commit_digest_matches_the_sealed_addon_contract() {
        assert_eq!(
            hex(&adoption_commit_digest(
                7, &[1; 32], &[2; 32], &[3; 32], &[4; 32], 5,
            )),
            "98d2e927402ad157b620267a3cff23b2f1d7a11e2c31cb1356866d5099181f8a"
        );
    }

    fn hex(bytes: &[u8]) -> String {
        bytes.iter().map(|byte| format!("{byte:02x}")).collect()
    }
}
