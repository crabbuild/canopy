//! The receiver must never accept an unregistered or competing SDK identity.
use super::publishing::state;
use super::root_dispatch::Context;
use super::*;
use crate::packs::metadata::tests::limits;
use cellule_runtime::{Command, PreparedCommand, Resolution};

/// Native compositions share one frozen refusal across all registered pages.
pub(super) async fn register_native_pages(
    f: &Fixture,
    prepared: &PreparedCatalog,
    intent: &RefPolicyPreparation,
    store: &canopy_object_storage::artifact::ArtifactStore,
    root: &std::path::Path,
    budget: cellule_ltx::DiskBudget,
) -> Result<RegisteredRootRecovery> {
    let session = Arc::new(prepared.base.session.clone());
    let refusal =
        Box::pin(session.ready_root_refusal(identity()?, store, root, budget, None)).await?;
    let refusal_command = refusal.refusal_command().ok_or("frozen native refusal")?;
    let mut head = None;
    let mut offset = 0;
    while offset < intent.plan().updates.len() {
        let page = intent.page(prepared, offset).await?;
        offset += page.proof.plan.updates.len();
        let command = f
            .client()
            .prepare_command::<RegisterRefPolicyPage>(&f.target, identity()?, page)
            .await?;
        let registered = Box::pin(super::super::recovery::persist_full(
            &session,
            &command,
            super::super::recovery::Kind::Policy,
            Some(refusal_command),
            head.as_ref(),
            store,
            identity()?,
            0,
        ))
        .await?;
        let reply = Box::pin(command.execute()).await?;
        assert!(matches!(reply.output, RefPolicyReply::Registered(value) if value.valid));
        head = Some(registered);
    }
    head.ok_or_else(|| "native intent contains no policy page".into())
}

pub(super) async fn not_started<C: Command>(f: &Fixture, command: &PreparedCommand<C>) -> Result {
    let before = state(&f.handle).await?;
    let registration = registration_state(f).await?;
    if !matches!(
        Box::pin(command.clone().execute()).await,
        Err(InvocationError::NotStarted(_))
    ) {
        return Err(format!("unregistered or competing command {} was accepted", C::ID).into());
    }
    assert_eq!(state(&f.handle).await?, before);
    assert_eq!(registration_state(f).await?, registration);
    assert!(matches!(
        f.client().resolve(command.evidence()).await?,
        Resolution::Absent
    ));
    Ok(())
}

pub(super) async fn registration_state(f: &Fixture) -> Result<Vec<u8>> {
    Ok(f.handle
        .query(0, 64 << 10, |db| {
            let mut statement = db.prepare("SELECT incarnation,admission_sequence,recovery,recovery_phase,recovery_phase_revision FROM catalog_leases ORDER BY incarnation,admission_sequence")?;
            let values = statement
                .query_map([], |row| {
                    Ok((
                        row.get::<_, Vec<u8>>(0)?,
                        row.get::<_, u64>(1)?,
                        row.get::<_, Option<Vec<u8>>>(2)?,
                        row.get::<_, Option<Vec<u8>>>(3)?,
                        row.get::<_, u64>(4)?,
                    ))
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            serde_json::to_vec(&values).map_err(|_| Error::Command("fixture registration state"))
        })
        .await?)
}

pub(super) async fn qualify(context: Context<'_>) -> Result {
    let Context {
        fixture: f,
        prepared,
        store,
        ticket,
        root,
        budget,
        request,
        ..
    } = context;
    let session = ticket.bound_session()?;
    let intent = prepared
        .ref_policy_preparation(
            request.plan.ok_or("native policy plan")?,
            root,
            budget.clone(),
            limits(),
        )
        .await?;
    let refusal =
        Box::pin(session.ready_root_refusal(identity()?, store, root, budget.clone(), None))
            .await?;
    let refusal_command = refusal.refusal_command().ok_or("frozen refusal")?;
    not_started(f, refusal_command).await?;
    let mut head = None;
    let flag = std::sync::atomic::AtomicBool::new(false);
    let mut offset = 0;
    while offset < intent.plan().updates.len() {
        let page = intent.page(&prepared, offset).await?;
        offset += page.proof.plan.updates.len();
        let command = f
            .client()
            .prepare_command::<RegisterRefPolicyPage>(&f.target, identity()?, page.clone())
            .await?;
        // The original identity remains absent and can be submitted after its
        // exact bundle is registered. No alternate identity is needed on retry.
        not_started(f, &command).await?;
        let registered = Box::pin(super::super::recovery::persist_full(
            &session,
            &command,
            super::super::recovery::Kind::Policy,
            Some(refusal_command),
            head.as_ref(),
            store,
            identity()?,
            0,
        ))
        .await?;
        let competing = f
            .client()
            .prepare_command::<RegisterRefPolicyPage>(&f.target, identity()?, page)
            .await?;
        not_started(f, &competing).await?;
        not_started(f, refusal_command).await?;
        let original = Box::pin(command.clone().execute()).await?;
        assert!(matches!(original.output, RefPolicyReply::Registered(value) if value.valid));
        let PublicationOutcome::PolicyPage(recovered) = registered
            .dispatch_any(&f.client(), store, &f.authority(), &flag)
            .await?
        else {
            return Err("registered page lost its original result".into());
        };
        assert_eq!(original.output, recovered.output);
        assert_eq!(original.receipt, recovered.receipt);
        head = Some(registered);
    }
    let guard = intent.ready(&prepared).await?;
    let completion =
        Box::pin(prepared.root_push_completion(&guard, root, budget, limits(), None)).await?;
    let command = f
        .client()
        .prepare_command::<CompleteRootPush>(&f.target, identity()?, completion.clone())
        .await?;
    not_started(f, &command).await?;
    let registered = Box::pin(super::super::recovery::persist_full(
        &session,
        &command,
        super::super::recovery::Kind::Publish,
        None,
        head.as_ref(),
        store,
        identity()?,
        0,
    ))
    .await?;
    let competing = f
        .client()
        .prepare_command::<CompleteRootPush>(&f.target, identity()?, completion.clone())
        .await?;
    not_started(f, &competing).await?;
    let original = Box::pin(command.clone().execute()).await?;
    assert!(
        matches!(&original.output, RootCompletionReply::Completed(value)
        if !value.completion.rejected && value.completion.publication.is_some())
    );
    let PublicationOutcome::RootPush(recovered) = registered
        .dispatch_any(&f.client(), store, &f.authority(), &flag)
        .await?
    else {
        return Err("registered root lost its original result".into());
    };
    assert_eq!(original.output, recovered.output);
    assert_eq!(original.receipt, recovered.receipt);
    let replay = Box::pin(command.execute()).await?;
    assert_eq!(original.output, replay.output);
    assert_eq!(original.receipt, replay.receipt);
    // A fresh logical retry cannot manufacture the original SDK receipt even
    // after completion. Original receipt recovery remains available above.
    let late = f
        .client()
        .prepare_command::<CompleteRootPush>(&f.target, identity()?, completion)
        .await?;
    not_started(f, &late).await?;
    not_started(f, refusal_command).await?;
    Ok(())
}
