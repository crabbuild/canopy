//! Permanent selected merge metadata, independent of transient preparation pins.
//! The existing input root retains typed catalog/ref edges, not an SDK request
//! body. Selection from an immutable UUID row precedes all artifact traversal.
use super::*;
use crate::packs::{
    catalog::{CatalogIndexes, CatalogReader, CatalogSnapshot},
    directory::index::IndexError,
    input_artifact::{INPUT_ROOT_BYTES, StoredInputRoot},
};
use canopy_object_storage::artifact::{ArtifactKind, ArtifactStore};
use std::sync::Arc;

const DOMAIN: &[u8] = b"canopy.native-reviewed-merge-audit.v2\0";
pub(super) const SAVED: &str = "SELECT binding,id,pull_number,oid,merged_ms,pull_version,source_oid,source_version,base_oid,base_version,publication,strategy,candidate_id FROM pull_merges WHERE id=?1";

#[derive(Debug, thiserror::Error)]
pub enum NativeMergeAuditError {
    #[error("merge audit codec failed")]
    Codec(#[from] CodecError),
    #[error("merge audit root failed")]
    Root(#[from] crate::packs::InputRootError),
    #[error("merge audit catalog failed")]
    Catalog(#[from] IndexError),
    #[error("merge audit ref snapshot failed")]
    Snapshot(#[from] RefSnapshotError),
    #[error("merge audit ref lookup failed")]
    Refs(#[from] RefStateError),
    #[error("selected merge audit context differs")]
    Context,
}
struct Audit {
    input: MergeInput,
    base_ref: String,
    catalog: StoredCatalog,
    refs: RefStateSnapshotRoot,
    ref_generation: u64,
    target: crate::ObjectId,
    candidate: Option<StoredInputRoot>,
}
impl Audit {
    fn shape(&self) -> Result<(), CodecError> {
        self.input.encode(&mut BoundedEncoder::new(4096)?)?;
        if self.candidate.is_some() != (self.input.request.strategy != MergeStrategy::FastForward)
            || self.target.format() != self.catalog.format
            || (self.input.request.strategy == MergeStrategy::FastForward
                && hex::encode(self.target) != self.input.request.revision.source_oid)
            || !self.base_ref.starts_with("refs/heads/")
            || self.base_ref.len() > crate::packs::ref_state::MAX_NAME_BYTES
            || !crate::refs::valid_ref_name(&self.base_ref)
            || self.ref_generation == 0
            || self.ref_generation > i64::MAX as u64
            || [
                &self.input.request.revision.source_oid,
                &self.input.request.revision.base_oid,
            ]
            .iter()
            .any(|oid| {
                crate::pulls::merge::oid(oid).is_err()
                    || oid.len() != self.catalog.format.bytes() * 2
            })
        {
            return Err(CodecError::Invalid("native merge audit scope"));
        }
        self.catalog.encode(&mut BoundedEncoder::new(256)?)?;
        self.refs.encode(&mut BoundedEncoder::new(128)?)
    }
    fn outcome(&self) -> MergeOutcome {
        MergeOutcome::Applied {
            merge: MergeRecord {
                id: self.input.request.id.clone(),
                number: self.input.number,
                oid: hex::encode(self.target),
                merged_at_ms: self.input.issued_at_ms,
                revision: self.input.request.revision.clone(),
            },
        }
    }
}
impl WireValue for Audit {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        self.shape()?;
        e.write_bytes(DOMAIN)?;
        self.input.encode(e)?;
        e.write_text(&self.base_ref)?;
        self.catalog.encode(e)?;
        self.refs.encode(e)?;
        e.write_u64(self.ref_generation)?;
        e.write_bytes(self.target.as_ref())?;
        self.candidate.encode(e)
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        if d.read_bytes()? != DOMAIN {
            return Err(CodecError::Invalid("merge audit purpose"));
        }
        let value = Self {
            input: MergeInput::decode(d)?,
            base_ref: d.read_text()?.to_owned(),
            catalog: StoredCatalog::decode(d)?,
            refs: RefStateSnapshotRoot::decode(d)?,
            ref_generation: d.read_u64()?,
            target: crate::ObjectId::try_from(d.read_bytes()?)
                .map_err(|_| CodecError::Invalid("merge target OID"))?,
            candidate: Option::<StoredInputRoot>::decode(d)?,
        };
        value.shape()?;
        Ok(value)
    }
}

pub(super) async fn prepare(
    prepared: &PreparedCatalog,
    input: &MergeInput,
    base_ref: &str,
    refs: RefStateSnapshotRoot,
    target: crate::ObjectId,
    candidate: Option<StoredInputRoot>,
) -> Result<(StoredInputRoot, u64), NativeMergePreparationError> {
    let store = prepared.base.indexes().store();
    let record = Audit {
        input: input.clone(),
        base_ref: base_ref.to_owned(),
        catalog: prepared.catalog(),
        refs,
        ref_generation: refs.read(&store).await?.generation,
        target,
        candidate,
    };
    let generation = record.ref_generation;
    Ok((
        StoredInputRoot::upload(
            &store,
            prepared.token().artifact_operation,
            &record,
            INPUT_ROOT_BYTES,
        )
        .await?,
        generation,
    ))
}

pub(in crate::packs::publication) fn statement(outcome: &MergeOutcome) -> SqlStatement {
    let parameter = match outcome {
        MergeOutcome::Applied { merge } => uuid::Uuid::parse_str(&merge.id)
            .ok()
            .map_or(SqlValue::Null, |id| blob(id.as_bytes())),
        _ => SqlValue::Null,
    };
    SqlStatement {
        sql: SAVED.into(),
        parameters: vec![parameter],
    }
}
/// Replays may select a prior attempt's permanent root. Never substitute the
/// new attempt's proposal, generation or creating namespace for that result.
pub(in crate::packs::publication) fn selected(
    result: &[SqlResultSet],
    outcome: &MergeOutcome,
    actor: &str,
) -> Result<Option<StoredInputRoot>, Error> {
    let MergeOutcome::Applied { merge } = outcome else {
        return Ok(None);
    };
    let Some(row) = rows(result)?.first() else {
        return Ok(None);
    };
    if row.len() != 13 {
        return Err(Error::Command("invalid selected merge audit"));
    }
    let (SqlValue::Blob(binding), SqlValue::Blob(publication)) = (&row[0], &row[10]) else {
        return Err(Error::Command("invalid merge audit binding"));
    };
    let expected = MergeInput {
        actor: actor.into(),
        number: merge.number,
        issued_at_ms: merge.merged_at_ms,
        request: crate::pulls::merge::MergeRequest {
            id: merge.id.clone(),
            revision: merge.revision.clone(),
            strategy: match &row[11] {
                SqlValue::Text(value) => match value.as_str() {
                    "fast_forward" => MergeStrategy::FastForward,
                    "merge_commit" => MergeStrategy::MergeCommit,
                    "squash" => MergeStrategy::Squash,
                    "rebase" => MergeStrategy::Rebase,
                    _ => return Err(Error::Command("merge strategy")),
                },
                _ => return Err(Error::Command("merge strategy type")),
            },
            candidate_id: match &row[12] {
                SqlValue::Null => None,
                SqlValue::Blob(value) => Some(
                    uuid::Uuid::from_slice(value)
                        .map_err(|_| Error::Command("merge candidate UUID"))?
                        .to_string(),
                ),
                _ => return Err(Error::Command("merge candidate type")),
            },
        },
    };
    if *binding != request_binding(&expected) || crate::pulls::merge::record(&row[1..10])? != *merge
    {
        return Ok(None);
    }
    let mut d = BoundedDecoder::new(publication, 128)?;
    let root = StoredInputRoot::decode(&mut d)?;
    d.finish()?;
    root.validate(INPUT_ROOT_BYTES)?;
    Ok(Some(root))
}

async fn verify(
    store: &ArtifactStore,
    root: StoredInputRoot,
    outcome: &MergeOutcome,
    check: &LeaseCheck,
) -> Result<(Audit, CatalogSnapshot), NativeMergeAuditError> {
    let audit: Audit = root.read(store, INPUT_ROOT_BYTES).await?;
    if audit.input.actor != check.actor
        || audit.outcome() != *outcome
        || audit.catalog.repository != check.token.repository
        || audit.refs.operation() != root.operation
    {
        return Err(NativeMergeAuditError::Context);
    }
    let indexes = Arc::new(CatalogIndexes::new(
        Arc::new(store.clone()),
        audit.catalog.format,
    ));
    // At most 48 range roots and one source root. Descendant descriptors stay
    // reachable through the permanent audit; this performs no provider deletion
    // and does not rescan all historical objects or reprove their closure.
    let reader = CatalogReader::open(indexes, audit.catalog).await?;
    let snapshot = CatalogSnapshot::download(store, reader.stored()).await?;
    let refs = audit.refs.read(store).await?;
    if refs.format != audit.catalog.format || refs.generation != audit.ref_generation {
        return Err(NativeMergeAuditError::Context);
    }
    let state = RefStateIndex::new(Arc::new(store.clone()), refs.format)
        .read(refs.root, &audit.base_ref)
        .await?;
    if state
        != Some(crate::RefExpectation {
            oid: Some(audit.target),
            version: audit.input.request.revision.base_version + 1,
        })
    {
        return Err(NativeMergeAuditError::Context);
    }
    if let Some(candidate) = audit.candidate {
        let selected = uuid::Uuid::parse_str(
            audit
                .input
                .request
                .candidate_id
                .as_deref()
                .ok_or(NativeMergeAuditError::Context)?,
        )
        .map_err(|_| NativeMergeAuditError::Context)?;
        let ready: super::super::candidate_publication::audit::Audit =
            candidate.read(store, INPUT_ROOT_BYTES).await?;
        if ready.candidate.request.id != selected.to_string()
            || ready.candidate.number != audit.input.number
            || ready.candidate.request.strategy != audit.input.request.strategy
            || ready.candidate.request.revision != audit.input.request.revision
            || !matches!(&ready.candidate.result,crate::pulls::candidates::CandidateResult::Ready{oid,..} if *oid==hex::encode(audit.target))
            || ready.catalog.repository != store.repository()
        {
            return Err(NativeMergeAuditError::Context);
        }
    }
    Ok((audit, snapshot))
}
pub(in crate::packs::publication) async fn closed_graph(
    store: &ArtifactStore,
    root: StoredInputRoot,
    outcome: &MergeOutcome,
    check: &LeaseCheck,
    hash: &mut blake3::Hasher,
) -> Result<(), RootRecoveryError> {
    let (audit, snapshot) = verify(store, root, outcome, check).await?;
    let descriptor = super::super::recovery::archive::descriptor;
    if let Some(candidate) = audit.candidate {
        descriptor(
            hash,
            candidate.operation,
            ArtifactKind::InputRoot,
            candidate.artifact,
        )?;
    }

    descriptor(hash, root.operation, ArtifactKind::InputRoot, root.artifact)?;
    descriptor(
        hash,
        audit.catalog.operation,
        ArtifactKind::CatalogNode,
        audit.catalog.artifact,
    )?;
    descriptor(
        hash,
        snapshot.directory.operation,
        ArtifactKind::CatalogNode,
        snapshot.directory.artifact,
    )?;
    descriptor(
        hash,
        audit.refs.operation(),
        ArtifactKind::InputRoot,
        audit.refs.artifact(),
    )?;
    Ok(())
}
