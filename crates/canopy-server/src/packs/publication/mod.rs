//! Fenced preparation and retained generation facts in the Repository Cell.
//! The fresh schema is selected with the final producer/reader hard cutover;
//! these commands are not registered on the legacy repository serving path.
use super::catalog::StoredCatalog;
use crate::{
    ObjectFormat, RepositoryModule,
    access::{access_statement, decode_access},
    directory::{TokenScope, validate_component},
};
use cellule_runtime::{
    CellModule, Command, Error, Query, RegistryBuilder,
    codec::{BoundedDecoder, BoundedEncoder, CodecError, WireValue},
    identity::IncarnationId,
    primitives::sql::{SqlBatch, SqlResultSet, SqlStatement, SqlValue},
    registry::{CommandContext, CommandResult, OwnerFence, QueryContext},
};
mod session;
pub use session::PreparationSession;
mod base;
pub use base::{PreparationBaseError, PreparationBaseResolver};
mod certificate;
pub(in crate::packs) mod codec;
pub use certificate::{
    AttestationOutcome, CERTIFICATE_BYTES, CatalogCertificate, RegisteredCatalog,
};
mod attestation;
pub use attestation::{CatalogAttestationError, RegisterCatalogAttestation};
mod prepare;
pub use prepare::{CatalogPreparation, CatalogPreparationError, PreparedCatalog};
pub(in crate::packs) mod ref_proof;
pub use ref_proof::{RefProofError, RefPublicationProof};
mod publish;
pub use publish::{PublicationReply, PublishCatalogRefs, PublishedRefs};
mod outcome;
pub use outcome::OutcomeCertificate;
mod completion;
mod coordinator;
pub use completion::{
    CatalogCompletionReply, CatalogPushCompletion, CatalogPushResponseError, CheckCompletedPush,
    CompleteCatalogPush, CompletedCatalogPush, CompletionCatalogProof, PushCompletionProofError,
    PushCompletionRequest, SignedPushAnnotation, replay_push_response,
};
pub use coordinator::{
    CompactionReadyError, NativeInputReadyError, PreparationCommandKind, PreparationCommandOutcome,
    PreparationReadyError, PublicationAdmissionFailure, PublicationClass, PublicationCoordinator,
    PublicationError, PublicationLimits, PublicationOutcome, PublicationScheduleError,
    PublicationState, PublicationStats, PublicationTicket, ReadyCatalogCompaction,
    ReadyCatalogPush, ReadyNativeInputs, ReadyPreparation, ReadyPublication,
    RegisteredNativeInputs,
};
mod commands;
mod compaction;
pub use compaction::{
    CheckCompletedCompaction, CompactionLimits, CompactionPlanner, CompactionPolicy,
    CompactionPressure, CompactionReply, CompactionSource, PreparedCompaction,
    PublishCatalogCompaction, PublishedCompaction,
};
mod exact;
mod staging_service;
pub use staging_service::{
    ReadyStaging, StagedInputsTicket, StagedPublicationFailure, StagedPublicationTicket,
    StagingBound, StagingContext, StagingCoordinator, StagingError, StagingLimits, StagingState,
    StagingStats, StagingTask, StagingTicket,
};
mod inputs;
mod native_result;
pub(in crate::packs) use inputs::RetainedNativeInput;
pub use native_result::{NativeResultError, NativeResultRoot, SavedNativeResult};
mod staging;
pub use inputs::{
    CheckStagedInputs, InputCheckpointError, NativeInputCertificate, RegisterStagedInputs,
};
pub use staging::{
    BeginStaging, BindStaging, CheckStaging, ClaimStaging, RenewStaging, StagingLease, StagingReply,
};
mod sql;
pub use commands::{
    AbortPreparation, BeginPreparation, CheckPreparation, CheckPreparationFrontier,
    ClaimPreparation, ReapPreparation, RenewPreparation,
};

pub const SCHEMA: &str = include_str!("schema.sql");
pub const MAX_OPERATIONS: u64 = 1024;
pub const MAX_GENERATION_LEASES: u64 = 4096;
/// Includes the reserved empty generation. Old eligible facts are reaped;
/// removing a local SQL fact never authorizes deleting remote artifacts.
pub const MAX_RETAINED_GENERATIONS: u64 = 8192;
pub const MAX_LEASE_MS: u64 = 300_000;
pub const DEFAULT_LEASE_MS: u64 = 60_000;
pub const REAP_ROWS: u64 = 512;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PreparationToken {
    pub repository: [u8; 16],
    pub operation: [u8; 16],
    /// Durable creating namespace, independent of the logical request ID.
    /// Allocated once by Begin/Claim; never supplied by a product client.
    pub artifact_operation: [u8; 16],
    pub request_digest: [u8; 32],
    pub owner: OwnerFence,
    /// Admitted execution sequence, not a counter reset by record pruning.
    pub attempt: u64,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct GenerationFact {
    pub generation: u64,
    pub catalog: Option<StoredCatalog>,
    pub certificate: Option<[u8; 32]>,
}
impl GenerationFact {
    fn validate(self) -> Result<(), CodecError> {
        if self.generation > i64::MAX as u64 {
            return Err(CodecError::Invalid("invalid catalog generation"));
        }
        match (self.generation, self.catalog, self.certificate) {
            (0, None, None) => Ok(()),
            (1.., Some(catalog), Some(_)) => catalog
                .validate()
                .map_err(|_| CodecError::Invalid("invalid generation catalog")),
            _ => Err(CodecError::Invalid("incomplete generation fact")),
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PreparationLease {
    pub token: PreparationToken,
    pub base: GenerationFact,
    pub format: ObjectFormat,
    pub observed_at_ms: i64,
    pub expires_at_ms: i64,
}
/// One authorized snapshot of the original attempt and latest catalog. The
/// attempt's immutable base is a retention floor: every subsequent generation
/// stays retained until its independent pin is reaped. This query result grants
/// neither canonical reconciliation nor publication authority.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PreparationFrontier {
    pub lease: PreparationLease,
    pub current: GenerationFact,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PreparationDenial {
    Unauthorized,
    Conflict,
    Stale,
    Expired,
    Capacity,
    Missing,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PreparationReply {
    Granted(Box<PreparationLease>),
    Denied(PreparationDenial),
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BeginRequest {
    pub repository: [u8; 16],
    pub operation: [u8; 16],
    pub request_digest: [u8; 32],
    pub actor: String,
    pub lease_ms: u64,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LeaseCheck {
    pub token: PreparationToken,
    pub actor: String,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LeaseRequest {
    pub check: LeaseCheck,
    pub lease_ms: u64,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MaintenanceRequest {
    pub repository: [u8; 16],
    pub actor: String,
    pub owner: OwnerFence,
}

/// Register on the fresh RepositoryModule only, with bounded descriptors for
/// command IDs 11..14/16..19/22/24..26/28 and query IDs 15/20/21/23/27, plus the existing trusted SQL
/// query. No separate Cell or compatibility API.
pub fn register(registry: &mut RegistryBuilder) -> cellule_runtime::Result<()> {
    registry.bind_command::<BeginStaging>()?;
    registry.bind_command::<RenewStaging>()?;
    registry.bind_command::<BindStaging>()?;
    registry.bind_command::<ClaimStaging>()?;
    registry.bind_command::<RegisterStagedInputs>()?;
    registry.bind_query::<CheckStagedInputs>()?;
    registry.bind_query::<CheckStaging>()?;
    registry.bind_command::<BeginPreparation>()?;
    registry.bind_command::<ClaimPreparation>()?;
    registry.bind_command::<RenewPreparation>()?;
    registry.bind_command::<AbortPreparation>()?;
    registry.bind_command::<ReapPreparation>()?;
    registry.bind_command::<RegisterCatalogAttestation>()?;
    registry.bind_command::<PublishCatalogRefs>()?;
    registry.bind_command::<CompleteCatalogPush>()?;
    registry.bind_command::<PublishCatalogCompaction>()?;
    registry.bind_query::<CheckCompletedCompaction>()?;
    registry.bind_query::<CheckCompletedPush>()?;
    registry.bind_query::<CheckPreparationFrontier>()?;
    registry.bind_query::<CheckPreparation>()
}
#[cfg(test)]
mod tests;
