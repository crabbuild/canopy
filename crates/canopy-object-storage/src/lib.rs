//! Immutable object parts and content-addressed large Git blobs.

pub mod artifact;
pub mod blob;
pub mod external;

pub use blob::{LargeBlobError, LargeBlobRead, LargeBlobReference, LargeBlobStore};
