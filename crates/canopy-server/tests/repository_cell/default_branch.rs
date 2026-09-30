use super::*;
use canopy_server::RefReadError;

fn identity() -> Result<MutationIdentity, Box<dyn std::error::Error>> {
    let now = i64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis())?;
    Ok(MutationIdentity {
        request_id: RequestId::from_bytes(uuid::Uuid::new_v4().into_bytes()),
        issued_at_ms: now,
        expires_at_ms: now + 60_000,
    })
}

pub async fn empty(repository: &RepositoryCell) -> Result<(), Box<dyn std::error::Error>> {
    let initial = repository.default_branch(None).await?.output;
    assert_eq!(initial.reference, "refs/heads/main");
    let changed = repository
        .set_default_branch(
            identity()?,
            "canopy",
            initial.generation,
            "refs/heads/trunk",
        )
        .await?;
    assert!(changed.output);
    let head = repository
        .default_branch(Some(changed.receipt))
        .await?
        .output;
    assert_eq!(head.reference, "refs/heads/trunk");
    assert_eq!(head.generation, initial.generation + 1);
    let page = repository.refs_page("", None).await?.output;
    assert!(page.refs.is_empty());
    assert_eq!(page.default_branch, head.reference);
    assert!(matches!(
        repository.refs_page("zzzz", Some(initial.generation)).await,
        Err(RefReadError::Changed)
    ));
    assert!(
        repository
            .set_default_branch(identity()?, "canopy", head.generation, "refs/heads/main")
            .await?
            .output
    );
    Ok(())
}

pub async fn verify(repository: &RepositoryCell) -> Result<(), Box<dyn std::error::Error>> {
    let initial = repository.default_branch(None).await?.output;
    for name in [
        "HEAD",
        "refs/tags/release",
        "refs/heads/",
        "refs/heads/a\nref: refs/heads/b",
        "refs/heads/a.lock",
        "refs/heads/a..b",
    ] {
        assert!(
            repository
                .set_default_branch(identity()?, "canopy", initial.generation, name)
                .await
                .is_err(),
            "{name}"
        );
    }
    for generation in [-1, i64::MAX] {
        assert!(
            repository
                .set_default_branch(identity()?, "canopy", generation, "refs/heads/main")
                .await
                .is_err()
        );
    }
    assert!(
        !repository
            .set_default_branch(
                identity()?,
                "canopy",
                initial.generation,
                "refs/heads/absent"
            )
            .await?
            .output
    );
    repository
        .grant_member(identity()?, "canopy", "writer", TokenScope::Write)
        .await?;
    for actor in ["writer", "outsider"] {
        assert!(
            !repository
                .set_default_branch(identity()?, actor, initial.generation, "refs/heads/main")
                .await?
                .output
        );
    }
    assert_eq!(repository.default_branch(None).await?.output, initial);
    let oid = repository
        .ref_state("refs/heads/main", None)
        .await?
        .output
        .ok_or("missing main")?
        .oid;
    repository
        .finalize_push(
            identity()?,
            PushPlan {
                actor: "canopy".into(),
                updates: vec![RefUpdate {
                    name: "refs/heads/trunk".into(),
                    expected: None,
                    new_oid: oid,
                }],
            },
        )
        .await?;
    // A push also fences an administrative update derived from an older ref view.
    assert!(
        !repository
            .set_default_branch(
                identity()?,
                "canopy",
                initial.generation,
                "refs/heads/trunk"
            )
            .await?
            .output
    );
    let current = repository.default_branch(None).await?.output;
    let replay_id = identity()?;
    for _ in 0..2 {
        assert!(
            repository
                .set_default_branch(replay_id, "canopy", current.generation, "refs/heads/trunk")
                .await?
                .output
        );
    }
    let changed = repository.default_branch(None).await?.output;
    assert_eq!(changed.generation, current.generation + 1);
    assert_eq!(changed.reference, "refs/heads/trunk");
    assert!(matches!(
        repository.refs_page("zzzz", Some(current.generation)).await,
        Err(RefReadError::Changed)
    ));
    assert!(
        repository
            .set_default_branch(identity()?, "canopy", changed.generation, "refs/heads/main")
            .await?
            .output
    );
    // Returning HEAD to its former name must not make a stale request valid again.
    assert!(
        !repository
            .set_default_branch(
                identity()?,
                "canopy",
                current.generation,
                "refs/heads/trunk"
            )
            .await?
            .output
    );
    let generation = repository.default_branch(None).await?.output.generation;
    let (left, right) = tokio::join!(
        repository.set_default_branch(identity()?, "canopy", generation, "refs/heads/trunk"),
        repository.set_default_branch(identity()?, "canopy", generation, "refs/heads/main"),
    );
    assert_ne!(left?.output, right?.output);
    assert_eq!(
        repository.default_branch(None).await?.output.generation,
        generation + 1
    );
    Ok(())
}
