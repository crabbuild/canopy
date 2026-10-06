//! Immutable native-pack metadata, outside the Repository Cell's write path.
//!
//! These structures are inputs to trusted catalog verification. Native pack
//! membership alone does not certify graph closure or authorize object reads.

pub(crate) mod backup;
pub mod catalog;
pub mod closure;
pub mod directory;
pub mod metadata;
pub mod sources;
pub mod verification;

pub(crate) mod input_artifact;
pub use input_artifact::InputRootError;
pub mod publication;
pub mod ref_state;
pub mod wire_request;
