//! Fresh empty catalog/ref publication. No SQL-ref conversion or decoded-root
//! signing adapter exists: only a privately assembled empty catalog can mint it.
use super::*;
use crate::packs::ref_state::{RefSnapshotError, RefStateSnapshot};
use cellule_runtime::{InvocationError, primitives::sql::SqlCell};
use tokio::time::timeout_at;

mod publish;
pub use publish::{CheckInitializedCatalog, InitializeCatalogRefs};
pub const INITIALIZATION_BYTES: u32 = 2048;
const INITIAL_HEAD: &str = "refs/heads/main";

#[derive(Debug, thiserror::Error)]
pub enum InitializationPreparationError {
    #[error("initialization command preparation failed")]
    Command(#[source] Box<InvocationError<InitializationReply>>),
    #[error("initialization preparation is inactive")]
    Base(#[from] PreparationBaseError),
    #[error("initialization snapshot failed")]
    Snapshot(#[from] RefSnapshotError),
    #[error("initialization certificate failed")]
    Certificate(#[from] CatalogAttestationError),
    #[error("initialization capability failed")]
    Capability(#[from] Error),
    #[error("initialization authorization query failed")]
    Query(#[source] Box<InvocationError<Vec<SqlResultSet>>>),
    #[error("initialization encoding failed")]
    Codec(#[from] CodecError),
    #[error("initialization requires an empty generation-zero catalog")]
    Ineligible,
}

/// Public fields permit transport only. The final command authenticates the
/// purpose-separated MAC binding; raw descriptors cannot mint this proof.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InitialRefProof {
    pub certificate: CatalogCertificate,
    pub refs: RefStateSnapshotRoot,
}
fn binding(refs: RefStateSnapshotRoot) -> Result<[u8; 32], CodecError> {
    let mut e = BoundedEncoder::new(128)?;
    refs.encode(&mut e)?;
    let mut hash = blake3::Hasher::new();
    hash.update(b"canopy.ref-initialization.v1\0");
    hash.update(&e.finish());
    Ok(*hash.finalize().as_bytes())
}
impl InitialRefProof {
    fn shape(&self) -> Result<(), CodecError> {
        let data = self.certificate.data()?;
        if data.compaction
            || data.base.generation != 0
            || data.base.refs.is_some()
            || data.object_count != 0
            || data.edge_count != 0
            || data.input_count != 0
            || data.input_checkpoint_digest.is_some()
            || data.completion_digest.is_some()
            || data.refs_digest != Some(binding(self.refs)?)
            || self.refs.operation() != data.token.artifact_operation
        {
            return Err(CodecError::Invalid("invalid fresh initialization proof"));
        }
        Ok(())
    }
}
impl WireValue for InitialRefProof {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        self.shape()?;
        self.certificate.encode(e)?;
        self.refs.encode(e)
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        let value = Self {
            certificate: CatalogCertificate::decode(d)?,
            refs: RefStateSnapshotRoot::decode(d)?,
        };
        value.shape()?;
        Ok(value)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum InitializationReply {
    Initialized(Box<GenerationFact>),
    Denied(PreparationDenial),
}
fn initial_fact(fact: &GenerationFact) -> Result<(), CodecError> {
    fact.validate()?;
    if fact.generation != 1 || fact.refs.is_none() {
        return Err(CodecError::Invalid("invalid initialized generation"));
    }
    Ok(())
}
impl WireValue for InitializationReply {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        match self {
            Self::Initialized(fact) => {
                initial_fact(fact)?;
                e.write_u8(0)?;
                fact.encode(e)
            }
            Self::Denied(reason) => PreparationReply::Denied(*reason).encode(e),
        }
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        match d.read_u8()? {
            0 => {
                let fact = GenerationFact::decode(d)?;
                initial_fact(&fact)?;
                Ok(Self::Initialized(Box::new(fact)))
            }
            1 => Ok(Self::Denied(PreparationDenial::Unauthorized)),
            2 => Ok(Self::Denied(PreparationDenial::Conflict)),
            3 => Ok(Self::Denied(PreparationDenial::Stale)),
            4 => Ok(Self::Denied(PreparationDenial::Expired)),
            5 => Ok(Self::Denied(PreparationDenial::Capacity)),
            6 => Ok(Self::Denied(PreparationDenial::Missing)),
            _ => Err(CodecError::Invalid("invalid initialization reply")),
        }
    }
}

impl PreparedCatalog {
    /// Mint from the held catalog and its own store, never from caller roots.
    /// The final command independently refuses non-pristine repository state.
    pub async fn empty_ref_initialization(
        &self,
    ) -> Result<InitialRefProof, InitializationPreparationError> {
        let (_, deadline) = self.base.live_lease()?;
        timeout_at(deadline, async {
            if self.base().generation != 0
                || self.object_count() != 0
                || self.edge_count() != 0
                || self.input_count() != 0
                || self.input_checkpoint_digest.is_some()
            {
                return Err(InitializationPreparationError::Ineligible);
            }
            let (client, target, check) = self.base.capability();
            let sql = SqlCell::<RepositoryModule>::new(client.clone(), target.clone())?;
            let access = sql
                .query(
                    None,
                    SqlBatch {
                        statements: vec![access_statement(&check.actor)],
                    },
                )
                .await
                .map_err(|e| InitializationPreparationError::Query(Box::new(e)))?;
            if !decode_access(&access.output)?.is_some_and(|role| role >= TokenScope::Admin) {
                return Err(PreparationBaseError::Inactive.into());
            }
            self.ensure_live()?;
            let store = self.base.indexes().store();
            let refs = RefStateSnapshotRoot::upload(
                &store,
                self.token().artifact_operation,
                RefStateSnapshot {
                    repository: self.token().repository,
                    format: self.catalog().format,
                    generation: 0,
                    default_branch: INITIAL_HEAD.into(),
                    root: None,
                },
            )
            .await?;
            let certificate = self.issue_certificate(Some(binding(refs)?), None).await?;
            self.ensure_live()?;
            let value = InitialRefProof { certificate, refs };
            value.shape()?;
            Ok(value)
        })
        .await
        .map_err(|_| PreparationBaseError::Inactive)?
    }
}
