//! Generated candidate publication reuses the editorial intent and exact root journal.
//! Ready never obtains authority from a client result, ref descriptor or SQL OID.
use super::*;
use crate::pulls::candidates::{CandidateResult, MergeCandidate, valid_request, valid_result};
use cellule_ltx::DiskBudget;
use cellule_runtime::{InvocationError, primitives::sql::SqlCell};
use std::path::Path;
use tokio::time::timeout_at;

pub(super) mod audit;
mod publish;
pub use publish::PublishNativeCandidate;
pub const NATIVE_CANDIDATE_BYTES: u32 = 512 << 10;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CandidatePublicationReply {
    Applied {
        id: [u8; 16],
        digest: [u8; 32],
        publication: Option<PublishedRefs>,
    },
    NotFound,
    Forbidden,
    Conflict,
    Denied(PreparationDenial),
}
impl CandidatePublicationReply {
    pub(crate) fn applied(&self) -> bool {
        matches!(self, Self::Applied { .. })
    }
}
impl WireValue for CandidatePublicationReply {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        match self {
            Self::Applied {
                id,
                digest,
                publication,
            } => {
                crate::validate_repository_id(*id)
                    .map_err(|_| CodecError::Invalid("candidate UUID"))?;
                e.write_u8(0)?;
                e.write_bytes(id)?;
                e.write_bytes(digest)?;
                publication.encode(e)
            }
            Self::NotFound => e.write_u8(1),
            Self::Forbidden => e.write_u8(2),
            Self::Conflict => e.write_u8(3),
            Self::Denied(reason) => {
                e.write_u8(4)?;
                PreparationReply::Denied(*reason).encode(e)
            }
        }
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        let value = match d.read_u8()? {
            0 => Self::Applied {
                id: crate::packs::directory::index::codec::fixed(d)?,
                digest: crate::packs::directory::index::codec::fixed(d)?,
                publication: Option::decode(d)?,
            },
            1 => Self::NotFound,
            2 => Self::Forbidden,
            3 => Self::Conflict,
            4 => match PreparationReply::decode(d)? {
                PreparationReply::Denied(reason) => Self::Denied(reason),
                _ => return Err(CodecError::Invalid("candidate denial")),
            },
            _ => return Err(CodecError::Invalid("candidate acknowledgement")),
        };
        value.encode(&mut BoundedEncoder::new(512)?)?;
        Ok(value)
    }
}
pub(super) fn candidate_bytes(candidate: &MergeCandidate) -> Result<Vec<u8>, CodecError> {
    if candidate.number <= 0
        || candidate.created_at_ms < 0
        || validate_component(&candidate.actor).is_err()
        || !valid_request(&candidate.request)
        || !valid_result(&candidate.result)
    {
        return Err(CodecError::Invalid("generated candidate result"));
    }
    let bytes =
        serde_json::to_vec(candidate).map_err(|_| CodecError::Invalid("candidate encoding"))?;
    if bytes.len() > 300 << 10 {
        return Err(CodecError::Invalid("candidate result limit"));
    }
    Ok(bytes)
}
pub(super) fn result_digest(candidate: &MergeCandidate) -> Result<[u8; 32], CodecError> {
    Ok(*blake3::hash(&candidate_bytes(candidate)?).as_bytes())
}
#[derive(Clone, Debug)]
pub struct NativeCandidateProof {
    certificate: CatalogCertificate,
    candidate: MergeCandidate,
    selection: RefSelection,
    refs: Option<RefStateSnapshotRoot>,
    ref_generation: Option<u64>,
    audit: Option<crate::packs::input_artifact::StoredInputRoot>,
}
impl NativeCandidateProof {
    fn binding(&self) -> Result<[u8; 32], CodecError> {
        Self::payload_binding(
            &self.candidate,
            &self.selection,
            self.refs,
            self.ref_generation,
            self.audit,
        )
    }
    fn payload_binding(
        candidate: &MergeCandidate,
        selection: &RefSelection,
        refs: Option<RefStateSnapshotRoot>,
        generation: Option<u64>,
        audit: Option<crate::packs::input_artifact::StoredInputRoot>,
    ) -> Result<[u8; 32], CodecError> {
        let mut e = BoundedEncoder::new(NATIVE_CANDIDATE_BYTES)?;
        e.write_bytes(&candidate_bytes(candidate)?)?;
        selection.encode(&mut e)?;
        refs.encode(&mut e)?;
        generation.encode(&mut e)?;
        audit.encode(&mut e)?;
        let mut h = blake3::Hasher::new();
        h.update(b"canopy.generated-candidate-publication.v1\0");
        h.update(&e.finish());
        Ok(*h.finalize().as_bytes())
    }
    fn shape(&self) -> Result<(), CodecError> {
        let data = self.certificate.data()?;
        candidate_bytes(&self.candidate)?;
        let ready = matches!(self.candidate.result, CandidateResult::Ready { .. });
        if data.compaction
            || data.base.refs.is_none()
            || data.actor != self.candidate.actor
            || data.completion_digest.is_some()
            || data.refs_digest != Some(self.binding()?)
            || self.selection.repository != data.token.repository
            || self.selection.actor.as_deref() != Some(&data.actor)
            || self.selection.proof.is_some()
            || self.selection.facts.len() > 2
            || ready != self.refs.is_some()
            || ready != self.ref_generation.is_some()
            || ready != self.audit.is_some()
            || self
                .refs
                .is_some_and(|r| r.operation() != data.token.artifact_operation)
            || self
                .audit
                .is_some_and(|r| r.operation != data.token.artifact_operation)
            || self
                .ref_generation
                .is_some_and(|g| g == 0 || g > i64::MAX as u64)
            || !ready && (data.input_count != 0 || data.object_count != 0 || data.edge_count != 0)
        {
            return Err(CodecError::Invalid("candidate proof scope"));
        }
        Ok(())
    }
}
impl WireValue for NativeCandidateProof {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        self.shape()?;
        self.certificate.encode(e)?;
        e.write_bytes(&candidate_bytes(&self.candidate)?)?;
        self.selection.encode(e)?;
        self.refs.encode(e)?;
        self.ref_generation.encode(e)?;
        self.audit.encode(e)
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        let value = Self {
            certificate: CatalogCertificate::decode(d)?,
            candidate: serde_json::from_slice(d.read_bytes()?)
                .map_err(|_| CodecError::Invalid("candidate result"))?,
            selection: RefSelection::decode(d)?,
            refs: Option::decode(d)?,
            ref_generation: Option::decode(d)?,
            audit: Option::decode(d)?,
        };
        value.shape()?;
        Ok(value)
    }
}
#[derive(Debug, thiserror::Error)]
pub enum NativeCandidatePublicationError {
    #[error("candidate preparation is inactive")]
    Base(#[from] PreparationBaseError),
    #[error("candidate context differs")]
    Context,
    #[error("candidate encoding failed")]
    Codec(#[from] CodecError),
    #[error("candidate native verification failed")]
    Verification(#[from] NativeCandidateVerificationError),
    #[error("candidate SQL capability failed")]
    Capability(#[from] Error),
    #[error("candidate ref snapshot failed")]
    Snapshot(#[from] crate::packs::ref_state::RefSnapshotError),
    #[error("candidate ref tree failed")]
    Refs(#[from] crate::packs::ref_state::RefStateError),
    #[error("candidate audit failed")]
    Root(#[from] crate::packs::InputRootError),
    #[error("candidate certificate failed")]
    Certificate(#[from] CatalogAttestationError),
    #[error("candidate metadata query failed")]
    Query(#[source] Box<InvocationError<Vec<SqlResultSet>>>),
    #[error("candidate command preparation failed")]
    Command(#[source] Box<InvocationError<CandidatePublicationReply>>),
}
impl PreparedCatalog {
    pub(crate) async fn native_candidate_proof(
        &self,
        produced: &crate::git_gateway::candidates::ProducedCandidate,
        directory: &Path,
        budget: DiskBudget,
        limits: crate::packs::metadata::MetadataLimits,
    ) -> Result<NativeCandidateProof, NativeCandidatePublicationError> {
        let (_, deadline) = self.base.live_lease()?;
        timeout_at(deadline, async {
            let candidate = produced.candidate().clone();
            if candidate.actor != self.base.capability().2.actor
                || produced.operation() != self.token().operation
            {
                return Err(NativeCandidatePublicationError::Context);
            }
            candidate_bytes(&candidate)?;
            let (client, target, _) = self.base.capability();
            let sql = SqlCell::<RepositoryModule>::new(client.clone(), target.clone())?;
            let selected = sql
                .query(
                    None,
                    sql::statement(
                        "SELECT source_ref,base_ref FROM pull_requests WHERE number=?1",
                        vec![SqlValue::Integer(candidate.number)],
                    ),
                )
                .await
                .map_err(|e| NativeCandidatePublicationError::Query(Box::new(e)))?;
            let store = self.base.indexes().store();
            let old = self
                .base()
                .refs
                .ok_or(NativeCandidatePublicationError::Context)?
                .read(&store)
                .await?;
            if old.repository != self.token().repository
                || old.format != self.catalog().format
                || old.generation > self.base().generation
                || old.generation >= i64::MAX as u64
            {
                return Err(NativeCandidatePublicationError::Context);
            }
            let index = crate::packs::ref_state::RefStateIndex::new(store.clone(), old.format);
            let mut selection = RefSelection {
                repository: self.token().repository,
                actor: Some(candidate.actor.clone()),
                facts: vec![],
                proof: None,
            };
            if let Some([SqlValue::Text(source), SqlValue::Text(base)]) =
                sql::rows(&selected.output)?.first().map(Vec::as_slice)
            {
                let mut names = vec![source.clone(), base.clone()];
                names.sort();
                names.dedup();
                for name in names {
                    selection.facts.push(ref_observation::RefFact {
                        state: index.read(old.root.clone(), &name).await?,
                        name,
                    });
                }
            }
            let (refs, ref_generation, audit) =
                if matches!(candidate.result, CandidateResult::Ready { .. }) {
                    self.verify_candidate_commit(&candidate, directory, budget, limits)
                        .await?;
                    let transition = index
                        .prepare_candidate(old.root, self.token().artifact_operation, &candidate)
                        .await?;
                    let generation = old.generation + 1;
                    let refs = crate::packs::ref_state::RefStateSnapshotRoot::upload(
                        &store,
                        self.token().artifact_operation,
                        crate::packs::ref_state::RefStateSnapshot {
                            repository: old.repository,
                            format: old.format,
                            generation,
                            default_branch: old.default_branch,
                            root: Some(transition.root()),
                        },
                    )
                    .await?;
                    let audit = audit::prepare(self, &candidate, refs, generation).await?;
                    (Some(refs), Some(generation), Some(audit))
                } else {
                    (None, None, None)
                };
            let binding = NativeCandidateProof::payload_binding(
                &candidate,
                &selection,
                refs,
                ref_generation,
                audit,
            )?;
            let proof = NativeCandidateProof {
                certificate: self.issue_certificate(Some(binding), None).await?,
                candidate,
                selection,
                refs,
                ref_generation,
                audit,
            };
            proof.shape()?;
            self.ensure_live()?;
            Ok(proof)
        })
        .await
        .map_err(|_| PreparationBaseError::Inactive)?
    }
}
