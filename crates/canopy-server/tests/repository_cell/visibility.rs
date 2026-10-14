use super::*;
use canopy_server::{ReadIdentity, Visibility};

fn identity() -> Result<MutationIdentity, Box<dyn std::error::Error>> {
    let now = i64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis())?;
    Ok(MutationIdentity {
        request_id: RequestId::from_bytes(uuid::Uuid::new_v4().into_bytes()),
        issued_at_ms: now,
        expires_at_ms: now + 60_000,
    })
}

pub async fn verify(repository: &RepositoryCell) -> Result<(), Box<dyn std::error::Error>> {
    let initial = repository.visibility().await?.output;
    assert_eq!(initial.visibility, Visibility::Private);
    assert_eq!(
        repository
            .access_level(ReadIdentity::Anonymous, None)
            .await?
            .output,
        None
    );
    repository
        .grant_member(identity()?, "canopy", "writer", TokenScope::Write)
        .await?;
    for actor in ["writer", "outsider"] {
        assert!(
            !repository
                .set_visibility(identity()?, actor, initial.generation, Visibility::Public)
                .await?
                .output
        );
    }
    for generation in [-1, i64::MAX] {
        assert!(
            repository
                .set_visibility(identity()?, "canopy", generation, Visibility::Public)
                .await
                .is_err()
        );
    }
    let request = identity()?;
    let published = repository
        .set_visibility(request, "canopy", initial.generation, Visibility::Public)
        .await?;
    assert!(published.output);
    let replayed = repository
        .set_visibility(request, "canopy", initial.generation, Visibility::Public)
        .await?;
    assert_eq!(replayed.receipt, published.receipt);
    assert!(replayed.output);
    for (reader, role) in [
        (ReadIdentity::Anonymous, TokenScope::Read),
        (ReadIdentity::Account("outsider"), TokenScope::Read),
        (ReadIdentity::Account("writer"), TokenScope::Write),
        (ReadIdentity::Account("canopy"), TokenScope::Admin),
    ] {
        assert_eq!(
            repository
                .access_level(reader, Some(published.receipt))
                .await?
                .output,
            Some(role)
        );
    }
    let public = repository.visibility().await?.output;
    assert_eq!(public.generation, initial.generation + 1);
    let head = repository.ref_state("refs/heads/main", None).await?.output;
    assert!(matches!(
        repository
            .finalize_push(
                identity()?,
                PushPlan {
                    actor: "outsider".into(),
                    updates: vec![RefUpdate {
                        name: "refs/heads/main".into(),
                        expected: head,
                        new_oid: None
                    }],
                }
            )
            .await,
        Err(InvocationError::Rejected(_))
    ));
    assert!(
        repository
            .set_visibility(
                identity()?,
                "canopy",
                public.generation,
                Visibility::Private
            )
            .await?
            .output
    );
    assert_eq!(
        repository
            .access_level(ReadIdentity::Anonymous, None)
            .await?
            .output,
        None
    );
    // Returning to private must not make an old public request valid again.
    assert!(
        !repository
            .set_visibility(
                identity()?,
                "canopy",
                initial.generation,
                Visibility::Public
            )
            .await?
            .output
    );
    Ok(())
}
