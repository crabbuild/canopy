//! Unknown commands retain their original bytes; closed frames retain metadata.
use super::*;
use crate::packs::{
    backup::{Inventory, decode},
    directory::index::WalkResult,
};

pub(in crate::packs::publication) async fn graph(
    bytes: &[u8],
    saved: Option<&[u8]>,
    release: Option<&[u8]>,
    seed: &[u8; 32],
    inventory: &mut Inventory<'_>,
) -> WalkResult<()> {
    let certificate: RootRecoveryCertificate = decode(bytes, 1024)?;
    if !certificate.0.authenticated(seed) {
        return Err(RootRecoveryError::Context.into());
    }
    let mut record: Record = certificate.0.data()?;
    super::super::backup::context(
        record.tenant,
        record.application,
        record.check.token.repository,
        inventory,
    )?;
    let journal = match saved {
        Some(bytes) => phase::journal(&SqlValue::Blob(bytes.to_vec()), &record)?,
        None => phase::Journal {
            primary: None,
            refusal: None,
        },
    };
    if let Some(bytes) = release {
        archive::backup_release(bytes)?;
        journal
            .terminal(&record)?
            .ok_or(RootRecoveryError::Context)?;
    }
    let check = record.check.clone();
    let mut first = true;
    loop {
        let bundle = record
            .root
            .read::<Bundle>(&inventory.store(), ROOT_BYTES)
            .await?;
        archive::validate_bundle(&bundle, &record, inventory.target())?;
        inventory
            .input(record.root.operation, record.root.artifact)
            .await?;
        if first && release.is_none() {
            if journal.primary.is_none() {
                command(&record, &bundle.primary, record.kind, seed, inventory).await?;
            }
            // A frozen future refusal remains necessary while its command is
            // unresolved, including before the primary's result is known.
            if journal.refusal.is_none()
                && (journal.primary.is_none() || journal.refused(&record)?)
                && let Some(saved) = &bundle.refusal
            {
                command(&record, saved, Kind::Outcome, seed, inventory).await?;
            }
        }
        if first && let Some(terminal) = journal.terminal(&record)? {
            match terminal {
                archive::Terminal::Push(value) => {
                    super::super::root_completion::backup_graph(value.root, inventory).await?
                }
                archive::Terminal::Initialization(InitializationReply::Initialized(fact)) => {
                    super::super::backup::fact(*fact, inventory).await?
                }
                _ => {} // Permanent selected merge/candidate/HEAD rows are scanned independently.
            }
        }
        first = false;
        let Some(previous) = record.previous else {
            break;
        };
        let frame = previous
            .read::<phase::Frame>(&inventory.store(), ROOT_BYTES)
            .await?;
        if !frame.certificate.0.authenticated(seed) {
            return Err(RootRecoveryError::Context.into());
        }
        let next: Record = frame.certificate.0.data()?;
        if next.check != check
            || next.tenant != record.tenant
            || next.application != record.application
            || next.step.checked_add(1) != Some(record.step)
        {
            return Err(RootRecoveryError::Context.into());
        }
        if !inventory
            .input(previous.operation, previous.artifact)
            .await?
        {
            break;
        }
        record = next;
    }
    Ok(())
}
async fn command(
    record: &Record,
    saved: &SavedCommand,
    kind: Kind,
    seed: &[u8; 32],
    inventory: &mut Inventory<'_>,
) -> WalkResult<()> {
    let limit = kind.body_limit();
    if saved.body.size == 0 || saved.body.size > u64::from(limit) {
        return Err(RootRecoveryError::Context.into());
    }
    let key = ArtifactKey {
        operation: record.check.token.artifact_operation,
        binding_digest: saved.body.digest,
        kind: ArtifactKind::InputBody,
    };
    let mut reader = inventory.store().read(key, saved.body).await?;
    let mut bytes = Vec::with_capacity(saved.body.size as usize);
    while let Some(part) = reader.next().await? {
        bytes.extend_from_slice(&part);
    }
    // The SDK snapshot's operation digest includes this exact input body.
    // Registration already bound the complete frozen contract to its MAC.
    inventory.artifact(key, saved.body).await?;
    super::super::backup::command(kind, &bytes, seed, inventory).await
}
