//! One physical drain owner per exact pin across every context in this process.
//! A weak entry survives while any worker, pin or retained release owns its Arc.
use super::*;
use std::{
    collections::HashMap,
    sync::{Mutex, OnceLock, Weak},
};

pub const MAX_SERVING_OWNERS: usize = 4096;
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
struct Key {
    tenant: [u8; 16],
    application: [u8; 16],
    repository: [u8; 16],
    reader: [u8; 16],
    incarnation: [u8; 16],
    epoch: u64,
    sequence: u64,
}
static OWNERS: OnceLock<Mutex<HashMap<Key, Weak<()>>>> = OnceLock::new();

pub(super) fn reserve(
    target: &CellTarget,
    token: ServingToken,
) -> Result<Arc<()>, ServingReadError> {
    token.validate()?;
    if crate::repository_target(target.tenant(), target.application(), token.repository)? != *target
    {
        return Err(ServingReadError::Context);
    }
    let key = Key {
        tenant: *target.tenant().as_bytes(),
        application: *target.application().as_bytes(),
        repository: token.repository,
        reader: token.reader,
        incarnation: *token.owner.incarnation.as_bytes(),
        epoch: token.owner.epoch,
        sequence: token.admission_sequence,
    };
    let mut owners = OWNERS
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .expect("serving ownership");
    owners.retain(|_, owner| owner.strong_count() != 0);
    if owners.contains_key(&key) {
        return Err(ServingReadError::AlreadyOwned);
    }
    if owners.len() >= MAX_SERVING_OWNERS {
        return Err(ServingReadError::Capability(Error::Capacity(
            "node serving owners",
        )));
    }
    let owner = Arc::new(());
    owners.insert(key, Arc::downgrade(&owner));
    Ok(owner)
}
