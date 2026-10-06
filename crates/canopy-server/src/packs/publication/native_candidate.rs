//! Generated commit semantics are checked against the private closed catalog.
//! This is preparation, not Ready publication or current editorial authority.
use super::*;
use crate::{
    ObjectId, ObjectKind,
    packs::{
        catalog::CatalogReader,
        directory::index::IndexError,
        metadata::{MetadataError, MetadataLimits, ObjectHeader},
    },
    pulls::{
        candidates::{
            CandidateResult, MergeCandidate, commit_body,
            rebase::{Commit, MAX_COMMIT_BYTES, MAX_COMMITS},
            valid_request,
        },
        merge::MergeStrategy,
    },
};
use cellule_ltx::DiskBudget;
use std::path::Path;
use tokio::time::timeout_at;

#[derive(Debug, thiserror::Error)]
pub enum NativeCandidateVerificationError {
    #[error("native candidate preparation is inactive")]
    Base(#[from] PreparationBaseError),
    #[error("native candidate catalog failed")]
    Catalog(#[from] IndexError),
    #[error("native candidate metadata failed")]
    Metadata(#[from] MetadataError),
    #[error("native candidate object read failed")]
    Read(#[source] Box<crate::packs::catalog::NativeReadError>),
    #[error("native candidate ancestry failed")]
    Ancestry(#[source] Box<RefProofError>),
    #[error("native candidate worker failed")]
    Task(#[from] tokio::task::JoinError),
    #[error("native candidate intent or commit semantics differ")]
    Invalid,
}
impl PreparedCatalog {
    /// Verify exact generated merge/squash bytes or every linear rebase rewrite
    /// through this privately verified catalog. No SQL object/ancestry mirror or
    /// caller-provided body/closure flag is accepted. Future joint publication
    /// must bind these facts and recheck current intent, refs, access and owner.
    pub async fn verify_candidate_commit(
        &self,
        candidate: &MergeCandidate,
        directory: &Path,
        budget: DiskBudget,
        limits: MetadataLimits,
    ) -> Result<(), NativeCandidateVerificationError> {
        let (_, deadline) = self.base.live_lease()?;
        timeout_at(
            deadline,
            self.verify_candidate_commit_inner(candidate, directory, budget, limits),
        )
        .await
        .map_err(|_| PreparationBaseError::Inactive)?
    }
    async fn verify_candidate_commit_inner(
        &self,
        candidate: &MergeCandidate,
        directory: &Path,
        budget: DiskBudget,
        limits: MetadataLimits,
    ) -> Result<(), NativeCandidateVerificationError> {
        let invalid = NativeCandidateVerificationError::Invalid;
        if candidate.actor != self.base.capability().2.actor
            || validate_component(&candidate.actor).is_err()
            || candidate.number <= 0
            || candidate.created_at_ms < 0
            || !valid_request(&candidate.request)
        {
            return Err(invalid);
        }
        let CandidateResult::Ready {
            oid: tip,
            tree_oid: tree,
        } = &candidate.result
        else {
            return Err(invalid);
        };
        let parse = |s: &str| -> Result<ObjectId, NativeCandidateVerificationError> {
            let oid = crate::pulls::merge::oid(s)
                .map_err(|_| NativeCandidateVerificationError::Invalid)?;
            if oid.format() != self.catalog().format || oid.is_zero() {
                return Err(NativeCandidateVerificationError::Invalid);
            }
            Ok(oid)
        };
        let (tip, tree, source, base) = (
            parse(tip)?,
            parse(tree)?,
            parse(&candidate.request.revision.source_oid)?,
            parse(&candidate.request.revision.base_oid)?,
        );
        let reader = CatalogReader::open(self.base.indexes(), self.catalog()).await?;
        let files = self.base.files();
        let headers = reader
            .headers(&[tip, tree, source, base], &*files, &*files)
            .await?;
        if headers.len() != 4
            || headers
                .iter()
                .zip([
                    ObjectKind::Commit,
                    ObjectKind::Tree,
                    ObjectKind::Commit,
                    ObjectKind::Commit,
                ])
                .any(|(h, k)| h.is_none_or(|h| h.object.kind != k))
        {
            return Err(invalid);
        }
        if candidate.request.strategy != MergeStrategy::Rebase {
            let body = commit_body(candidate, &hex::encode(tree));
            verify_bytes(
                headers[0].ok_or(NativeCandidateVerificationError::Invalid)?,
                &body,
            )?;
        } else {
            let mut source = source;
            let mut current = tip;
            let mut originals = Vec::with_capacity(MAX_COMMITS + 1);
            let mut complete = false;
            for index in 0..MAX_COMMITS {
                self.ensure_live()?;
                if originals.contains(&source) {
                    return Err(NativeCandidateVerificationError::Invalid);
                }
                originals.push(source);
                let original = reader
                    .lookup(source, &*files, &*files)
                    .await?
                    .ok_or(NativeCandidateVerificationError::Invalid)?;
                let rewritten = reader
                    .lookup(current, &*files, &*files)
                    .await?
                    .ok_or(NativeCandidateVerificationError::Invalid)?;
                if original.entry.header.object.kind != ObjectKind::Commit
                    || rewritten.entry.header.object.kind != ObjectKind::Commit
                {
                    return Err(NativeCandidateVerificationError::Invalid);
                }
                let header = rewritten.entry.header;
                let metadata = rewritten.source.metadata.clone();
                let edges =
                    tokio::task::spawn_blocking(move || metadata.edges_after(current, None))
                        .await??;
                if edges.len() != 2 {
                    return Err(NativeCandidateVerificationError::Invalid);
                }
                let tree_edge = edges
                    .iter()
                    .find(|e| e.expected_kind == ObjectKind::Tree)
                    .ok_or(NativeCandidateVerificationError::Invalid)?;
                let parent_edge = edges
                    .iter()
                    .find(|e| e.expected_kind == ObjectKind::Commit)
                    .ok_or(NativeCandidateVerificationError::Invalid)?;
                if index == 0 && tree_edge.child != tree {
                    return Err(NativeCandidateVerificationError::Invalid);
                }
                let owner: crate::git_objects::ReadOwner = self.base.clone();
                let original = files
                    .body(original, MAX_COMMIT_BYTES, owner)
                    .await
                    .map_err(|e| NativeCandidateVerificationError::Read(Box::new(e)))?;
                let parsed =
                    Commit::parse(&original).ok_or(NativeCandidateVerificationError::Invalid)?;
                let body = parsed.rewrite(
                    candidate,
                    &hex::encode(tree_edge.child),
                    &hex::encode(parent_edge.child),
                );
                verify_bytes(header, &body)?;
                source = parse(parsed.parent)?;
                current = parent_edge.child;
                if current == base {
                    if originals.contains(&source) {
                        return Err(NativeCandidateVerificationError::Invalid);
                    }
                    originals.push(source);
                    complete = true;
                    break;
                }
            }
            if !complete {
                return Err(NativeCandidateVerificationError::Invalid);
            }
            // Walk base history once for this bounded set. Only the remaining
            // source anchor may be reachable: accepting an earlier reachable
            // source would replay commits that are already on the base branch.
            let mut walk = super::ref_proof::ancestry::Walker::new(directory, budget, limits)
                .await
                .map_err(|e| NativeCandidateVerificationError::Ancestry(Box::new(e)))?;
            let reached = walk
                .ancestors_within(&reader, &files, &originals, base, &self.base)
                .await
                .map_err(|e| NativeCandidateVerificationError::Ancestry(Box::new(e)))?;
            if reached.last() != Some(&true) || reached[..reached.len() - 1].iter().any(|v| *v) {
                return Err(NativeCandidateVerificationError::Invalid);
            }
        }
        self.ensure_live()?;
        Ok(())
    }
}
fn verify_bytes(header: ObjectHeader, body: &[u8]) -> Result<(), NativeCandidateVerificationError> {
    if body.len() > MAX_COMMIT_BYTES
        || header.object.kind != ObjectKind::Commit
        || header.object.size != body.len() as u64
        || blake3::hash(body).as_bytes() != &header.object.digest
        || crate::object_id(header.object.oid.format(), ObjectKind::Commit, body)
            != header.object.oid
    {
        return Err(NativeCandidateVerificationError::Invalid);
    }
    // Physical verification bound this OID and canonical header to actual pack
    // bytes. Matching both the Git OID and canonical BLAKE3 body digest proves equality without another
    // pack download/native subprocess for each rewritten/generated commit.
    Ok(())
}
