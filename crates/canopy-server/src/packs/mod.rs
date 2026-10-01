//! Immutable native-pack metadata, outside the Repository Cell's write path.
//!
//! These structures are inputs to trusted catalog verification. Native pack
//! membership alone does not certify graph closure or authorize object reads.

pub mod catalog;
pub mod closure;
pub mod directory;
pub mod metadata;
pub mod sources;
pub mod verification;

pub mod publication;
