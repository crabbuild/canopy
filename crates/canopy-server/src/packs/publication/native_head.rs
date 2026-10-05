//! Symbolic HEAD changes share the certified ref tree and joint publication CAS.
//! Only the held preparation can certify branch existence or an unborn target.
use super::*;
use crate::packs::ref_state::{RefNameKey, RefStateIndex, RefStateSnapshot};
use cellule_runtime::InvocationError;
use std::sync::Arc;
use tokio::time::timeout_at;

pub(in crate::packs::publication) mod publish;
pub use publish::PublishNativeHead;
pub const NATIVE_HEAD_BYTES: u32 = 128 << 10;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HeadRequest {
    pub reference: String,
    pub expected_generation: i64,
}
impl WireValue for HeadRequest {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        if self.reference.len() > crate::packs::ref_state::MAX_NAME_BYTES
            || !crate::default_branch::valid_default_branch(&self.reference)
            || !(0..i64::MAX).contains(&self.expected_generation)
        {
            return Err(CodecError::Invalid("invalid symbolic HEAD request"));
        }
        e.write_text(&self.reference)?;
        e.write_i64(self.expected_generation)
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        let value = Self {
            reference: d.read_text()?.into(),
            expected_generation: d.read_i64()?,
        };
        value.encode(&mut BoundedEncoder::new(NATIVE_HEAD_BYTES)?)?;
        Ok(value)
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NativeHeadProof {
    pub certificate: CatalogCertificate,
    pub request: HeadRequest,
    /// None is a certified refusal, never an empty-ref fallback.
    pub refs: Option<RefStateSnapshotRoot>,
}
impl NativeHeadProof {
    fn binding(&self) -> Result<[u8; 32], CodecError> {
        Self::payload_binding(&self.request, &self.refs)
    }
    fn payload_binding(
        request: &HeadRequest,
        refs: &Option<RefStateSnapshotRoot>,
    ) -> Result<[u8; 32], CodecError> {
        let mut e = BoundedEncoder::new(NATIVE_HEAD_BYTES)?;
        request.encode(&mut e)?;
        refs.encode(&mut e)?;
        let mut h = blake3::Hasher::new();
        h.update(b"canopy.symbolic-head-publication.v1\0");
        h.update(&e.finish());
        Ok(*h.finalize().as_bytes())
    }
    fn shape(&self) -> Result<(), CodecError> {
        let data = self.certificate.data()?;
        self.request
            .encode(&mut BoundedEncoder::new(NATIVE_HEAD_BYTES)?)?;
        if data.compaction
            || data.base.refs.is_none()
            || data.object_count != 0
            || data.edge_count != 0
            || data.input_count != 0
            || data.input_checkpoint_digest.is_some()
            || data.completion_digest.is_some()
            || data.refs_digest != Some(self.binding()?)
            || self
                .refs
                .is_some_and(|r| r.operation() != data.token.artifact_operation)
        {
            return Err(CodecError::Invalid("invalid symbolic HEAD proof"));
        }
        Ok(())
    }
}
impl WireValue for NativeHeadProof {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        self.shape()?;
        self.certificate.encode(e)?;
        self.request.encode(e)?;
        self.refs.encode(e)
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        let value = Self {
            certificate: CatalogCertificate::decode(d)?,
            request: HeadRequest::decode(d)?,
            refs: Option::decode(d)?,
        };
        value.shape()?;
        Ok(value)
    }
}
#[derive(Debug, thiserror::Error)]
pub enum NativeHeadPreparationError {
    #[error("symbolic HEAD preparation is inactive")]
    Base(#[from] PreparationBaseError),
    #[error("symbolic HEAD snapshot failed")]
    Snapshot(#[from] crate::packs::ref_state::RefSnapshotError),
    #[error("symbolic HEAD ref lookup failed")]
    Refs(#[from] crate::packs::ref_state::RefStateError),
    #[error("symbolic HEAD range lookup failed")]
    Index(#[from] crate::packs::directory::index::IndexError),
    #[error("symbolic HEAD encoding failed")]
    Codec(#[from] CodecError),
    #[error("symbolic HEAD certificate failed")]
    Certificate(#[from] CatalogAttestationError),
    #[error("symbolic HEAD command preparation failed")]
    Command(#[source] Box<InvocationError<PublicationReply>>),
    #[error("symbolic HEAD preparation context differs")]
    Context,
}
impl PreparedCatalog {
    pub async fn native_head_proof(
        &self,
        request: HeadRequest,
    ) -> Result<NativeHeadProof, NativeHeadPreparationError> {
        request.encode(&mut BoundedEncoder::new(NATIVE_HEAD_BYTES)?)?;
        let (_, deadline) = self.base.live_lease()?;
        timeout_at(deadline, async {
            if self.object_count() != 0
                || self.edge_count() != 0
                || self.input_count() != 0
                || self.input_checkpoint_digest.is_some()
            {
                return Err(NativeHeadPreparationError::Context);
            }
            let base = self.base();
            let store = self.base.indexes().store();
            let old = base
                .refs
                .ok_or(NativeHeadPreparationError::Context)?
                .read(&store)
                .await?;
            if old.repository != self.token().repository
                || old.format != self.catalog().format
                || old.generation > base.generation
                || old.generation >= i64::MAX as u64
            {
                return Err(NativeHeadPreparationError::Context);
            }
            let mut refs = None;
            if old.generation == request.expected_generation as u64 {
                let index = RefStateIndex::new(Arc::clone(&store), old.format);
                let target = index.read(old.root.clone(), &request.reference).await?;
                let live_target = target.is_some_and(|r| r.oid.is_some());
                // The live cursor skips entire tombstoned subtrees. One seek and
                // one live record suffice; no scan proportional to repo history.
                let mut branches = index.cursor(
                    old.root.clone(),
                    Some(RefNameKey::new("refs/heads/")?),
                    true,
                )?;
                let has_branches = branches
                    .next()
                    .await?
                    .is_some_and(|r| r.name().starts_with("refs/heads/"));
                if live_target || !has_branches {
                    self.ensure_live()?;
                    refs = Some(
                        RefStateSnapshotRoot::upload(
                            &store,
                            self.token().artifact_operation,
                            RefStateSnapshot {
                                repository: old.repository,
                                format: old.format,
                                generation: old.generation + 1,
                                default_branch: request.reference.clone(),
                                root: old.root,
                            },
                        )
                        .await?,
                    );
                }
            }
            let certificate = self
                .issue_certificate(
                    Some(NativeHeadProof::payload_binding(&request, &refs)?),
                    None,
                )
                .await?;
            let proof = NativeHeadProof {
                certificate,
                request,
                refs,
            };
            proof.shape()?;
            self.ensure_live()?;
            Ok(proof)
        })
        .await
        .map_err(|_| PreparationBaseError::Inactive)?
    }
}
