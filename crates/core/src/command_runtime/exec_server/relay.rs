use super::backend::{ExecServerError, ExecServerResult};
use aes_gcm::aead::{Aead, KeyInit};
use aes_gcm::Aes256Gcm;
use sha2::{Digest, Sha256};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelayPeerIdentity {
    pub public_key: [u8; 32],
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelayConnectBundle {
    pub environment_id: String,
    pub websocket_url: String,
    pub expected_executor: RelayPeerIdentity,
    pub shared_secret: [u8; 32],
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelayCiphertextFrame {
    pub seq: u64,
    pub peer_public_key: [u8; 32],
    pub ciphertext: Vec<u8>,
}

#[derive(Clone)]
pub struct RelayFrameCodec {
    expected_peer: RelayPeerIdentity,
    cipher: Aes256Gcm,
}

impl RelayFrameCodec {
    pub fn new(bundle: &RelayConnectBundle) -> Self {
        let mut hasher = Sha256::new();
        hasher.update(bundle.shared_secret);
        hasher.update(bundle.expected_executor.public_key);
        let key = hasher.finalize();
        Self {
            expected_peer: bundle.expected_executor.clone(),
            cipher: Aes256Gcm::new_from_slice(&key).expect("sha256 output is a valid AES-256 key"),
        }
    }

    pub fn seal(&self, seq: u64, plaintext: &[u8]) -> ExecServerResult<RelayCiphertextFrame> {
        let nonce = nonce_for_seq(seq);
        let ciphertext = self
            .cipher
            .encrypt(nonce.as_ref().into(), plaintext)
            .map_err(|error| ExecServerError::internal(error.to_string()))?;
        Ok(RelayCiphertextFrame {
            seq,
            peer_public_key: self.expected_peer.public_key,
            ciphertext,
        })
    }

    pub fn open(&self, frame: &RelayCiphertextFrame) -> ExecServerResult<Vec<u8>> {
        if frame.peer_public_key != self.expected_peer.public_key {
            return Err(ExecServerError::invalid_request(
                "relay peer key does not match pinned executor",
            ));
        }
        let nonce = nonce_for_seq(frame.seq);
        self.cipher
            .decrypt(nonce.as_ref().into(), frame.ciphertext.as_ref())
            .map_err(|error| ExecServerError::invalid_request(error.to_string()))
    }
}

fn nonce_for_seq(seq: u64) -> [u8; 12] {
    let mut nonce = [0u8; 12];
    nonce[4..].copy_from_slice(&seq.to_be_bytes());
    nonce
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bundle() -> RelayConnectBundle {
        RelayConnectBundle {
            environment_id: "env-1".to_string(),
            websocket_url: "wss://relay.test/connect".to_string(),
            expected_executor: RelayPeerIdentity {
                public_key: [7; 32],
            },
            shared_secret: [42; 32],
        }
    }

    #[test]
    fn relay_codec_encrypts_and_pins_peer_key() {
        let codec = RelayFrameCodec::new(&bundle());
        let frame = codec.seal(1, b"json-rpc-frame").unwrap();
        assert_ne!(frame.ciphertext, b"json-rpc-frame");
        assert_eq!(codec.open(&frame).unwrap(), b"json-rpc-frame");

        let mut forged = frame.clone();
        forged.peer_public_key = [9; 32];
        assert!(codec.open(&forged).unwrap_err().message.contains("pinned"));
    }
}
