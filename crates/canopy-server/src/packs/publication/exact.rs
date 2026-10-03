//! Shared exact-command resolution for final publication and custody commands.
use super::*;
use cellule_runtime::{
    CellClient, Committed, InvocationError, PreparedCommand, Receipt, Resolution,
    cell::executor::StoredOutcome,
};

pub(super) async fn resolve<C: Command>(
    client: &CellClient,
    command: PreparedCommand<C>,
    output_limit: u32,
    before_execute: impl FnOnce() -> Result<(), Error> + Send,
) -> Result<Committed<C::Output>, InvocationError<C::Output>> {
    let evidence = command.evidence().clone();
    match client.resolve(&evidence).await {
        Ok(Resolution::Absent) => {
            before_execute().map_err(InvocationError::NotStarted)?;
            Box::pin(command.execute()).await
        }
        Ok(Resolution::Committed(outcome)) => {
            let receipt = Receipt {
                cell: evidence.target().cell_id(),
                incarnation: evidence.incarnation(),
                commit_sequence: outcome.commit_sequence(),
            };
            let decoded = (|| {
                let mut decoder = BoundedDecoder::new(outcome.result(), output_limit)?;
                let output = C::Output::decode(&mut decoder)?;
                decoder.finish()?;
                Ok::<_, CodecError>(Committed { output, receipt })
            })()
            .map_err(|source| InvocationError::InvalidPublishedResult {
                receipt,
                source: Box::new(source.into()),
            })?;
            match outcome {
                StoredOutcome::Success { .. } => Ok(decoded),
                StoredOutcome::Rejected { .. } => Err(InvocationError::Rejected(Box::new(decoded))),
            }
        }
        // Unknown, expiration and changed incarnation never prove that an
        // earlier submission failed. Keep exact evidence for logical recovery.
        Ok(Resolution::Unknown | Resolution::Expired) | Err(_) => {
            Err(InvocationError::Pending(Box::new(evidence)))
        }
    }
}

/// Execute one retained transport copy. Only authoritative absence can cause an
/// exact recovery execution; unknown/expired evidence remains charged upstream.
pub(super) async fn invoke<C: Command>(
    client: &CellClient,
    command: PreparedCommand<C>,
    recover: bool,
    output_limit: u32,
    fault: u8,
) -> Result<Committed<C::Output>, InvocationError<C::Output>> {
    invoke_guarded(client, command, recover, output_limit, fault, || Ok(())).await
}

/// A local custody guard applies only before initial submission or proven
/// absence. Resolve known outcomes first, even after local custody is fenced.
pub(super) async fn invoke_guarded<C: Command>(
    client: &CellClient,
    command: PreparedCommand<C>,
    recover: bool,
    output_limit: u32,
    fault: u8,
    before_execute: impl FnOnce() -> Result<(), Error> + Send,
) -> Result<Committed<C::Output>, InvocationError<C::Output>> {
    let evidence = command.evidence().clone();
    let outcome = if fault == 1 {
        Err(InvocationError::Pending(Box::new(evidence.clone())))
    } else if recover {
        resolve(client, command, output_limit, before_execute).await
    } else {
        before_execute().map_err(InvocationError::NotStarted)?;
        Box::pin(command.execute()).await
    };
    if fault == 2 {
        Err(InvocationError::Pending(Box::new(evidence)))
    } else {
        assert_ne!(fault, 3, "injected exact command panic after execution");
        outcome
    }
}
