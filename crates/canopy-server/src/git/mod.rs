//! Git transport, native execution, object access, and repository browsing.

pub(crate) mod cache;
pub mod gateway;
pub mod http;
pub mod input;
pub(crate) mod native;
pub(crate) mod objects;
pub(crate) mod read;
