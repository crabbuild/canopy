use super::super::root_completion::tests::{audit_native, change_namespace, response};
use super::publishing::{edit, state};
use super::*;
use crate::packs::metadata::tests::limits;
use canopy_object_storage::artifact::ArtifactStore;
use cellule_ltx::DiskBudget;
use std::path::Path;

pub(super) async fn qualify(
    fixture: &Fixture,
    prepared: &PreparedCatalog,
    store: &Arc<ArtifactStore>,
    request: PushCompletionRequest,
    directory: &Path,
    budget: DiskBudget,
) -> Result {
    let plan = request.plan.unwrap();
    assert!(prepared.base().refs.is_some(), "prepared rooted base");
    let (checkpoint, _, _, _) = prepared.base.session.push_checkpoint().await?;
    let mut encoded = BoundedEncoder::new(CERTIFICATE_BYTES)?;
    checkpoint.encode(&mut encoded)?;
    assert_eq!(
        prepared.input_checkpoint_digest,
        Some(*blake3::hash(&encoded.finish()).as_bytes()),
        "prepared/checkpoint custody digest"
    );
    let pending = prepared
        .ref_policy_preparation(plan.clone(), directory, budget.clone(), limits())
        .await?;
    let mut start = 0;
    while start < pending.plan().updates.len() {
        let page = pending.page(prepared, start).await?;
        start += page.proof.plan.updates.len();
        fixture
            .client()
            .command::<RegisterRefPolicyPage>(&fixture.target, identity()?, page)
            .await?;
    }
    let guard = pending.ready(prepared).await?;
    let before = state(&fixture.handle).await?;
    let completion = prepared
        .root_push_completion(&guard, directory, budget.clone(), limits(), None)
        .await
        .map_err(|error| format!("root completion preparation: {error:?}"))?;
    assert_eq!(completion.outcomes.ref_generation, 1);
    assert_eq!(
        response(completion.outcomes.native, store).await?,
        request.response
    );
    assert_eq!(
        response(completion.outcomes.rejected, store).await?,
        crate::push::report::rejected_report(&request.response, crate::push::report::REJECTED)?
    );
    assert_eq!(
        response(completion.outcomes.replayed, store).await?,
        crate::push::report::rejected_report(
            &request.response,
            "Canopy signed push certificate was already used"
        )?
    );
    for root in [
        completion.outcomes.native,
        completion.outcomes.rejected,
        completion.outcomes.replayed,
    ] {
        assert_eq!(root.operation(), prepared.token().artifact_operation);
        assert!(root.artifact().size < 1024);
        let native = audit_native(root, store).await?;
        let (proof, _, _, _) = prepared.base.session.push_checkpoint().await?;
        assert_eq!(Some(native), proof.native_result()?);
    }
    let mut e = BoundedEncoder::new(ROOT_COMPLETION_BYTES)?;
    completion.encode(&mut e)?;
    let bytes = e.finish();
    assert!(bytes.len() < 2048);
    let mut d = BoundedDecoder::new(&bytes, ROOT_COMPLETION_BYTES)?;
    assert_eq!(RootPushCompletion::decode(&mut d)?, completion);
    d.finish()?;
    for choice in 0..8 {
        let mut changed = completion.clone();
        match choice {
            0 => changed.outcomes.response_id = *uuid::Uuid::new_v4().as_bytes(),
            1 => changed.outcomes.ref_generation += 1,
            2 => std::mem::swap(&mut changed.outcomes.native, &mut changed.outcomes.rejected),
            3 => std::mem::swap(
                &mut changed.outcomes.rejected,
                &mut changed.outcomes.replayed,
            ),
            4 => {
                changed.outcomes.signed = Some(RootSignedPushFact {
                    digest: [1; 32],
                    key: "key".into(),
                    size: 1,
                })
            }
            5 => changed.proof.guard.plan_digest[0] ^= 1,
            6 => changed.proof.snapshot = prepared.base().refs.unwrap(),
            _ => changed.outcomes.native = change_namespace(changed.outcomes.native),
        }
        assert!(
            changed
                .encode(&mut BoundedEncoder::new(ROOT_COMPLETION_BYTES)?)
                .is_err(),
            "choice {choice}"
        );
    }
    // The existing inline publisher must not accept a root-completion purpose.
    let ancestry = vec![0; plan.updates.len().div_ceil(8)];
    let denied = fixture
        .client()
        .command::<PublishCatalogRefs>(
            &fixture.target,
            identity()?,
            RefPublicationProof {
                certificate: completion.proof.certificate,
                plan,
                ancestry,
            },
        )
        .await;
    assert!(matches!(denied, Err(InvocationError::Rejected(value))
        if value.output == PublicationReply::Denied(PreparationDenial::Unauthorized)));
    assert_eq!(state(&fixture.handle).await?, before);
    edit(
        fixture,
        "INSERT INTO branch_rules VALUES('refs/heads/main',1,1,0,1,0,0)",
    )
    .await?;
    assert!(
        prepared
            .root_push_completion(&guard, directory, budget, limits(), None)
            .await
            .is_err()
    );
    Ok(())
}
