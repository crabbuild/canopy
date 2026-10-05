//! Fenced preparation and retained generation facts in the Repository Cell.
//! Production and qualification share the same bounded packed operation registry.
//! The remaining producer/reader conversion is an unreleasable local cutover.
use super::{catalog::StoredCatalog, ref_state::RefStateSnapshotRoot};
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
mod serving;
pub use serving::{
    AcquireServingPin, AcquireServingRequest, CheckServingPin, MAX_EDGE_PARENTS,
    MAX_SERVING_GENERATIONS, MAX_SERVING_OWNERS, MAX_SERVING_PINS, NativeWorkspace,
    ReadyServingCommand, ReadyServingRelease, ReleaseServingPin, RenewServingPin,
    RenewServingRequest, ResolvedServingRef, SelectServingGeneration, ServingCheck, ServingContext,
    ServingDenial, ServingDrainObserver, ServingDrainProof, ServingEdgePage, ServingLease,
    ServingOwner, ServingOwnerError, ServingOwnerPhase, ServingOwnerStats, ServingPin, ServingPool,
    ServingPoolLimits, ServingReadBudget, ServingReadError, ServingReleaseReply, ServingReply,
    ServingSelection, ServingSnapshot, ServingToken, WorkspaceLimits, WorkspaceStats,
};
mod owner;
pub(crate) mod registry;
pub use owner::PreparationAuthority;
mod session;
pub use session::PreparationSession;
mod base;
pub use base::{PreparationBaseError, PreparationBaseResolver};
mod certificate;
mod commit_membership;
mod ref_observation;
pub(crate) use commit_membership::{CommitMembership, MembershipRequest};
pub(crate) use ref_observation::{REF_SELECTION_BYTES, RefSelection};
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
mod initialization;
pub(crate) use initialization::verify_empty as verify_initial_catalog;
pub use initialization::{
    CheckInitializedCatalog, INITIALIZATION_BYTES, InitialRefProof, InitializationPreparationError,
    InitializationReply, InitializationVerificationError, InitializeCatalogRefs,
};
mod ref_snapshot;
pub use ref_snapshot::{PreparedRefSnapshot, RefSnapshotPreparationError};
mod ref_policy;
pub use ref_policy::{
    CheckRefPolicyGuard, MAX_REF_POLICY_GUARDS, MAX_REF_POLICY_WATCHES, PreparedRefPolicyGuard,
    REF_POLICY_PAGE_BYTES, REF_POLICY_PAGE_UPDATES, ReapRefPolicyGuard, RefPolicyIntent,
    RefPolicyLookup, RefPolicyPage, RefPolicyPreparation, RefPolicyPreparationError,
    RefPolicyProgress, RefPolicyReap, RefPolicyReapReply, RefPolicyReply, RefRootPublicationProof,
    RegisterRefPolicyPage,
};
mod publish;
pub use publish::{PublicationReply, PublishCatalogRefs, PublishedRefs};
mod outcome;
pub use outcome::OutcomeCertificate;
mod completion;
mod coordinator;
mod scan;
pub use completion::{
    CatalogCompletionReply, CatalogPushCompletion, CatalogPushResponseError, CheckCompletedPush,
    CompleteCatalogPush, CompletedCatalogPush, CompletionCatalogProof, PushCompletionProofError,
    PushCompletionRequest, SignedPushAnnotation, replay_push_response,
};
pub use coordinator::{
    CompactionReadyError, NativeInputReadyError, PreparationCommandKind, PreparationCommandOutcome,
    PreparationReadyError, PublicationAdmissionFailure, PublicationBudget, PublicationBudgetStats,
    PublicationClass, PublicationCoordinator, PublicationError, PublicationLimits,
    PublicationOutcome, PublicationScheduleError, PublicationState, PublicationStats,
    PublicationTicket, ReadyBoundRecovery, ReadyCatalogCompaction, ReadyCatalogPush,
    ReadyInitialization, ReadyNativeInputs, ReadyPreparation, ReadyPublication, ReadyRefPolicyPage,
    ReadyRootPush, RecoveryBindingFailure, RefPolicyReadyError, RefPolicyRefusalFailure,
    RegisteredNativeInputs, RootPushReadyError, ServingDrainAdmission,
};
pub use scan::{RecoveryScanBudget, RecoveryScanSettings};
mod commands;
mod compaction;
pub use compaction::{
    CheckCompletedCompaction, CompactionLimits, CompactionPlanner, CompactionPolicy,
    CompactionPressure, CompactionReply, CompactionSource, PreparedCompaction,
    PublishCatalogCompaction, PublishedCompaction,
};
mod exact;
mod recovery;
pub use recovery::{
    ReadyRootRecovery, ReadyTerminalRelease, RecoveryScanLimits, RecoveryScanStats,
    RecoverySupervisor, RegisterRootRecovery, RegisteredRootRecovery, ReleaseTerminalRecovery,
    RootRecoveryCertificate, RootRecoveryError, RootRecoveryReply, TerminalReleaseCertificate,
    TerminalReleaseInput, TerminalReleaseReply,
};
mod staging_service;
pub(crate) use staging_service::StagingBudget;
pub use staging_service::{
    ReadyStaging, StagedInputsTicket, StagedPublicationFailure, StagedPublicationTicket,
    StagingBound, StagingContext, StagingCoordinator, StagingError, StagingLimits, StagingState,
    StagingStats, StagingTask, StagingTicket,
};
mod inputs;
mod native_result;
mod root_completion;
pub(in crate::packs) use inputs::RetainedNativeInput;
pub use native_result::{NativeResultError, NativeResultRoot, SavedNativeResult};
pub use root_completion::{
    CheckCompletedRootPush, CompleteRootOutcome, CompleteRootPush, CompletedRootPush,
    NativeOutcomeRoot, ROOT_COMPLETION_BYTES, RootCompletionPreparationError, RootCompletionReply,
    RootOutcomeCompletion, RootPushCompletion, RootPushOutcomes, RootPushReplayError,
    RootSignedPushFact, replay_root_push_response,
};
mod admission_receipt;
mod custody;
pub use custody::{
    CustodyAction, CustodyError, CustodyIntent, CustodyPurpose, CustodyReply, CustodyRequest,
    CustodyScanStats, CustodyStopFact, CustodyStopInput, CustodyStopOutcome, CustodyStopReply,
    CustodySupervisor, ExecuteCustody, PreparedCustody, ReadyCustodyStop, RegisterCustodyIntent,
    RegisteredCustody, StopCustodyIntent,
};
mod preparation_receipt;
pub use preparation_receipt::{PreparationAdmission, PreparationReceiptError};
mod staging_receipt;
pub use staging_receipt::{StagingAdmission, StagingReceiptError};
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

pub const SCHEMA: &str = concat!(
    include_str!("schema.sql"),
    include_str!("ref_policy/schema.sql"),
    include_str!("serving/schema.sql")
);
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
    /// Ref metadata retained under this same immutable catalog generation.
    pub refs: Option<RefStateSnapshotRoot>,
    pub certificate: Option<[u8; 32]>,
}
impl GenerationFact {
    fn validate(self) -> Result<(), CodecError> {
        if self.generation > i64::MAX as u64 {
            return Err(CodecError::Invalid("invalid catalog generation"));
        }
        match (self.generation, self.catalog, self.certificate) {
            (0, None, None) if self.refs.is_none() => Ok(()),
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

/// Bind the packed production contract. Inline publication/completion adapters
/// are deliberately excluded; qualification binds its historical fixtures itself.
pub fn register(registry: &mut RegistryBuilder) -> cellule_runtime::Result<()> {
    registry.bind_command::<crate::branch_rules::command::SetBranchRule>()?;
    registry.bind_command::<crate::checks::native::StartCommitCheck>()?;
    registry.bind_query::<crate::checks::native::ReadCommitChecks>()?;
    registry.bind_command::<crate::pulls::native::CreateNativePull>()?;
    registry.bind_command::<crate::pulls::native::ReviewNativePull>()?;
    registry.bind_query::<crate::pulls::native::ReadNativePulls>()?;
    registry.bind_command::<ReleaseServingPin>()?;
    registry.bind_query::<CheckServingPin>()?;
    registry.bind_query::<SelectServingGeneration>()?;
    registry.bind_command::<RegisterCustodyIntent>()?;
    registry.bind_command::<ExecuteCustody>()?;
    registry.bind_command::<StopCustodyIntent>()?;
    registry.bind_command::<RegisterStagedInputs>()?;
    registry.bind_query::<CheckStagedInputs>()?;
    registry.bind_query::<CheckStaging>()?;
    registry.bind_command::<AbortPreparation>()?;
    registry.bind_command::<ReapPreparation>()?;
    registry.bind_command::<RegisterCatalogAttestation>()?;
    registry.bind_command::<InitializeCatalogRefs>()?;
    registry.bind_query::<CheckInitializedCatalog>()?;
    registry.bind_command::<RegisterRefPolicyPage>()?;
    registry.bind_query::<CheckRefPolicyGuard>()?;
    registry.bind_command::<ReapRefPolicyGuard>()?;
    registry.bind_command::<CompleteRootPush>()?;
    registry.bind_command::<CompleteRootOutcome>()?;
    registry.bind_command::<RegisterRootRecovery>()?;
    registry.bind_command::<ReleaseTerminalRecovery>()?;
    registry.bind_query::<CheckCompletedRootPush>()?;
    registry.bind_command::<PublishCatalogCompaction>()?;
    registry.bind_query::<CheckCompletedCompaction>()?;
    registry.bind_query::<CheckPreparationFrontier>()?;
    registry.bind_query::<CheckPreparation>()
}
#[cfg(test)]
mod tests;
