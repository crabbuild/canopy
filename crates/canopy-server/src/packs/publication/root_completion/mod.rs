//! Immutable native outcomes and bounded joint-root completion preparation.
//! This is conditional input for the final atomic publisher, not an ACK. Raw
//! roots cannot create native custody or authorize reading a completed response.
use super::*;
use crate::packs::{
    input_artifact::{INPUT_ROOT_BYTES, StoredInputRoot},
    metadata::MetadataLimits,
};
use crate::{directory::DirectoryCell, git_http::GitHttpResponse};
use canopy_object_storage::artifact::{ArtifactDescriptor, ArtifactStore};
use cellule_ltx::DiskBudget;
use sha2::{Digest as _, Sha256};
use std::path::Path;
use tokio::time::timeout_at;

mod codec;
mod outcome;
mod ref_free;
mod result;
mod retention;
pub use ref_free::{CompleteRootOutcome, RootOutcomeCompletion};
pub(in crate::packs::publication) use retention::closed_graph;
mod prepare;
mod publish;
pub(in crate::packs::publication) mod read;
pub use publish::CompleteRootPush;
pub use read::{CheckCompletedRootPush, RootPushReplayError, replay_root_push_response};
#[cfg(test)]
pub(super) mod tests;

pub const ROOT_COMPLETION_BYTES: u32 = 8 << 10;
const REPLAYED: &str = "Canopy signed push certificate was already used";

/// Reuses the shared metadata-root representation. The body may borrow the
/// native creator namespace; new outcome metadata/failure bodies always belong
/// to the admitted completing attempt. Decoding grants no read authority.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NativeOutcomeRoot(StoredInputRoot);
impl NativeOutcomeRoot {
    pub fn operation(self) -> [u8; 16] {
        self.0.operation
    }
    pub fn artifact(self) -> ArtifactDescriptor {
        self.0.artifact
    }
}

/// Indexed signed-certificate ownership facts. The private factory derives
/// these from the registered native result and freshly authorized signing key.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RootSignedPushFact {
    pub digest: [u8; 32],
    pub key: String,
    pub size: u64,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RootPushOutcomes {
    pub response_id: [u8; 16],
    pub ref_generation: u64,
    pub native: NativeOutcomeRoot,
    pub rejected: NativeOutcomeRoot,
    pub replayed: NativeOutcomeRoot,
    pub signed: Option<RootSignedPushFact>,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RootPushCompletion {
    pub proof: RefRootPublicationProof,
    pub outcomes: RootPushOutcomes,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CompletedRootPush {
    pub completion: CompletedCatalogPush,
    pub root: NativeOutcomeRoot,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RootCompletionReply {
    Completed(Box<CompletedRootPush>),
    Denied(PreparationDenial),
}

/// The completed traversal retains the native metadata's plan/options/signed
/// annotation, without making the original wire request a permanent audit root.
/// Active and uncertain preparation pins retain that request independently.
struct OutcomeRecord {
    native: NativeResultRoot,
    body_operation: [u8; 16],
    response: GitHttpResponse<ArtifactDescriptor>,
}

#[derive(Debug, thiserror::Error)]
pub enum RootCompletionPreparationError {
    #[error("root completion preparation is inactive")]
    Base(#[from] PreparationBaseError),
    #[error("root completion native custody failed")]
    Native(#[from] NativeResultError),
    #[error("root completion registered input custody failed")]
    Inputs(#[from] InputCheckpointError),
    #[error("root completion conditional refs failed")]
    Policy(#[source] Box<RefPolicyPreparationError>),
    #[error("root completion immutable metadata failed")]
    Root(#[from] crate::packs::InputRootError),
    #[error("root completion ref metadata failed")]
    Snapshot(#[from] crate::packs::ref_state::RefSnapshotError),
    #[error("root completion certificate failed")]
    Certificate(#[from] CatalogAttestationError),
    #[error("root completion encoding failed")]
    Codec(#[from] CodecError),
    #[error("root completion native report failed")]
    Report(#[from] crate::push::PushError),
    #[error("root completion session certificate failed")]
    Session(#[source] Box<PushCompletionProofError>),
    #[error("root completion custody, plan or metadata differs")]
    Context,
}
impl From<RefPolicyPreparationError> for RootCompletionPreparationError {
    fn from(error: RefPolicyPreparationError) -> Self {
        Self::Policy(Box::new(error))
    }
}

impl RootPushOutcomes {
    fn binding(&self) -> Result<[u8; 32], CodecError> {
        let mut e = BoundedEncoder::new(ROOT_COMPLETION_BYTES)?;
        self.encode(&mut e)?;
        let mut h = blake3::Hasher::new();
        h.update(b"canopy.root-push-completion.v1\0");
        h.update(&e.finish());
        Ok(*h.finalize().as_bytes())
    }
}
impl RootPushCompletion {
    fn shape(&self) -> Result<(), CodecError> {
        self.proof.shape()?;
        let data = self.proof.certificate.data()?;
        if data.completion_digest != Some(self.outcomes.binding()?)
            || self.outcomes.ref_generation == 0
            || data.input_checkpoint_digest.is_none()
            || [
                self.outcomes.native,
                self.outcomes.rejected,
                self.outcomes.replayed,
            ]
            .iter()
            .any(|root| root.operation() != data.token.artifact_operation)
            || data.base.generation == i64::MAX as u64
            || self.outcomes.ref_generation > data.base.generation + 1
        {
            return Err(CodecError::Invalid("invalid root completion proof"));
        }
        Ok(())
    }
}
