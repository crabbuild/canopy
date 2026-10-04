//! Certified read generations retained until owned workers have actually drained.
use super::*;
use cellule_runtime::{CellClient, CellTarget, MutationIdentity, PreparedCommand};
use std::sync::Arc;
mod codec;
mod command_owner;
mod commands;
pub use command_owner::ReadyServingCommand;
mod lifecycle;
mod ownership;
mod pool;
pub use lifecycle::{
    ServingDrainObserver, ServingOwner, ServingOwnerError, ServingOwnerPhase, ServingOwnerStats,
    ServingSnapshot,
};
pub use ownership::MAX_SERVING_OWNERS;
pub use pool::{MAX_SERVING_GENERATIONS, ServingPool, ServingPoolLimits};
mod session;
pub use commands::{
    AcquireServingPin, CheckServingPin, ReleaseServingPin, RenewServingPin, SelectServingGeneration,
};
pub use session::{
    MAX_EDGE_PARENTS, ReadyServingRelease, ResolvedServingRef, ServingContext, ServingEdgePage,
    ServingPin, ServingReadBudget, ServingReadError,
};

pub const MAX_SERVING_PINS: u64 = 4096;
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ServingToken {
    pub repository: [u8; 16],
    pub reader: [u8; 16],
    pub owner: OwnerFence,
    pub admission_sequence: u64,
    pub generation: u64,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AcquireServingRequest {
    pub repository: [u8; 16],
    pub reader: [u8; 16],
    pub actor: Option<String>,
    pub lease_ms: u64,
}
/// Observe a current joint root. This neither retains it nor grants artifact I/O.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ServingSelection {
    pub repository: [u8; 16],
    pub actor: Option<String>,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ServingCheck {
    pub token: ServingToken,
    pub actor: Option<String>,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RenewServingRequest {
    pub check: ServingCheck,
    pub lease_ms: u64,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ServingLease {
    pub token: ServingToken,
    pub fact: GenerationFact,
    pub format: ObjectFormat,
    pub observed_at_ms: i64,
    pub expires_at_ms: i64,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ServingDenial {
    Unauthorized,
    Conflict,
    Uninitialized,
    Stale,
    Expired,
    Capacity,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ServingReply {
    Granted(Box<ServingLease>),
    Denied(ServingDenial),
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ServingReleaseReply {
    Released,
    Denied(ServingDenial),
}
/// Transport is untrusted until its purpose-separated MAC is checked. Only a
/// private service owner whose workers drained can issue this certificate.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ServingDrainProof(super::certificate::CertificateEnvelope);
#[derive(Clone, Debug, PartialEq, Eq)]
struct DrainData {
    tenant: [u8; 16],
    application: [u8; 16],
    token: ServingToken,
    administrator: String,
}
