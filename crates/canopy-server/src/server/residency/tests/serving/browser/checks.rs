//! Receiver qualification uses real resident ownership and physical pack metadata.
//! Trusted generation installation isolates membership, not the writer pipeline.
use super::*;
use crate::checks::{
    CheckChange, CheckContextEdit,
    native::{CheckStart, CommitPage, CommitSelection, ReadCommitChecks, StartCommitCheck},
};
use crate::packs::publication::CommitMembership;
use cellule_runtime::codec::BoundedDecoder;

async fn select(
    repository: &RepositoryCell,
    actor: ReadIdentity<'_>,
    oid: ObjectId,
) -> Result<(crate::packs::publication::ServingSnapshot, CommitSelection)> {
    let snapshot = repository.serving_snapshot(actor).await?;
    let membership = snapshot.commit_membership(oid).await?;
    let selection = CommitSelection {
        repository: repository.repository_id(),
        actor: match actor {
            ReadIdentity::Anonymous => None,
            ReadIdentity::Account(v) => Some(v.into()),
        },
        oid,
        membership,
    };
    Ok((snapshot, selection))
}
async fn page(repository: &RepositoryCell, selection: CommitSelection) -> Result<bool> {
    Ok(repository
        .application
        .query::<ReadCommitChecks>(
            &repository.target,
            None,
            CommitPage {
                selection,
                after: None,
            },
        )
        .await?
        .output
        .is_some())
}
async fn start(
    repository: &RepositoryCell,
    selection: CommitSelection,
    version: i64,
) -> Result<CheckChange> {
    let result = repository
        .application
        .command::<StartCommitCheck>(
            &repository.target,
            crate::server::mutation_identity()?,
            CheckStart {
                selection,
                id: uuid::Uuid::new_v4().into_bytes(),
                context: "unit-tests".into(),
                context_version: version,
            },
        )
        .await;
    match result {
        Ok(value) => Ok(value.output),
        Err(cellule_runtime::InvocationError::Rejected(value)) => Ok(value.output),
        Err(error) => Err(error.into()),
    }
}
fn damaged(proof: &CommitMembership) -> Result<CommitMembership> {
    let mut e = BoundedEncoder::new(1024)?;
    proof.encode(&mut e)?;
    let mut bytes = e.finish();
    *bytes.last_mut().ok_or("empty membership")? ^= 1;
    let mut d = BoundedDecoder::new(&bytes, 1024)?;
    let proof = CommitMembership::decode(&mut d)?;
    d.finish()?;
    Ok(proof)
}

#[tokio::test]
async fn native_check_receivers_bind_commit_actor_repository_and_live_retained_pin() -> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let (server, _files) = server().await?;
        let (repository, native, _) = fixture(&server, format).await?;
        assert_eq!(
            repository
                .set_check_context(
                    crate::server::mutation_identity()?,
                    "canopy",
                    "unit-tests",
                    CheckContextEdit {
                        expected_version: 0,
                        reporter: "canopy",
                        enabled: true,
                    }
                )
                .await?
                .output,
            CheckChange::Applied
        );
        let (snapshot, selection) =
            select(&repository, ReadIdentity::Account("canopy"), native.main).await?;
        let proof = selection.membership.as_ref().ok_or("commit proof absent")?;
        assert!(page(&repository, selection.clone()).await?);
        assert_eq!(
            start(&repository, selection.clone(), 1).await?,
            CheckChange::Applied
        );
        let mut invalid = Vec::new();
        let mut forged = selection.clone();
        forged.membership = Some(damaged(proof)?);
        invalid.push(forged);
        let mut wrong_actor = selection.clone();
        wrong_actor.actor = Some("outsider".into());
        invalid.push(wrong_actor);
        let mut wrong_repository = selection.clone();
        wrong_repository.repository = uuid::Uuid::new_v4().into_bytes();
        invalid.push(wrong_repository);
        let mut other_commit = selection.clone();
        other_commit.oid = native.previous;
        invalid.push(other_commit);
        let mut no_proof = selection.clone();
        no_proof.membership = None;
        invalid.push(no_proof);
        for oid in [native.tree, native.tag, missing(format)] {
            assert!(snapshot.commit_membership(oid).await?.is_none());
            let mut substitution = selection.clone();
            substitution.oid = oid;
            invalid.push(substitution);
        }
        for selection in invalid {
            assert!(!page(&repository, selection.clone()).await?);
            assert_eq!(
                start(&repository, selection, 1).await?,
                CheckChange::NotFound
            );
        }
        let other = create(&server.repositories, "other-checks", format).await?;
        let (other, _, _) = loaded(&server.repositories, other.repository_id).await?;
        assert!(!page(&other, selection.clone()).await?);
        assert_eq!(
            start(&other, selection.clone(), 1).await?,
            CheckChange::NotFound
        );
        drop(other);
        // The exact retained fact remains valid while its original physical pin
        // is live, even though current head no longer contains that commit.
        let store = ArtifactStore::new(
            server.repositories.external_store.clone(),
            repository.repository_id(),
        );
        let directory = DirectorySnapshot::empty(repository.repository_id(), format)
            .upload(&store, operation(200))
            .await?;
        let empty = CatalogSnapshot {
            directory,
            sources: None,
        }
        .upload(&store, operation(201))
        .await?;
        install(&repository, 3, empty, native.refs).await?;
        assert!(page(&repository, selection.clone()).await?);
        assert_eq!(
            start(&repository, selection.clone(), 1).await?,
            CheckChange::Applied
        );
        assert!(
            repository
                .commit_checks(ReadIdentity::Account("canopy"), native.main, None)
                .await?
                .output
                .is_none()
        );
        // A valid kind proof must not authorize an obsolete context version.
        assert_eq!(
            repository
                .set_check_context(
                    crate::server::mutation_identity()?,
                    "canopy",
                    "unit-tests",
                    CheckContextEdit {
                        expected_version: 1,
                        reporter: "canopy",
                        enabled: true,
                    }
                )
                .await?
                .output,
            CheckChange::Applied
        );
        assert_eq!(
            start(&repository, selection.clone(), 1).await?,
            CheckChange::Conflict
        );
        assert_eq!(
            start(&repository, selection.clone(), 2).await?,
            CheckChange::Applied
        );
        // Real producer drain removes the lease; deleting SQL rows would not
        // qualify the physical lifecycle which protects the proof's metadata.
        drop(snapshot);
        let (_, _, service) = loaded(&server.repositories, repository.repository_id()).await?;
        timeout(Duration::from_secs(8), service.serving.close_and_drain()).await?;
        assert_eq!(retained(&repository).await?, 0);
        assert!(!page(&repository, selection.clone()).await?);
        assert_eq!(
            start(&repository, selection, 2).await?,
            CheckChange::NotFound
        );
        let rows = repository
            .sql
            .query(
                None,
                SqlBatch {
                    statements: vec![SqlStatement {
                        sql: "SELECT count(*) FROM check_runs".into(),
                        parameters: vec![],
                    }],
                },
            )
            .await?;
        assert_eq!(rows.output[0].rows, vec![vec![SqlValue::Integer(3)]]);
        drop((service, repository));
        timeout(Duration::from_secs(15), server.shutdown()).await??;
    }
    Ok(())
}

#[tokio::test]
async fn native_check_receivers_recheck_revoked_membership_and_public_visibility() -> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let (server, _files) = server().await?;
        let (repository, native, _) = fixture(&server, format).await?;
        repository
            .grant_member(
                crate::server::mutation_identity()?,
                "canopy",
                "ci",
                crate::server::TokenScope::Read,
            )
            .await?;
        repository
            .set_check_context(
                crate::server::mutation_identity()?,
                "canopy",
                "unit-tests",
                CheckContextEdit {
                    expected_version: 0,
                    reporter: "ci",
                    enabled: true,
                },
            )
            .await?;
        let (snapshot, selection) =
            select(&repository, ReadIdentity::Account("ci"), native.main).await?;
        assert_eq!(
            start(&repository, selection.clone(), 1).await?,
            CheckChange::Applied
        );
        repository
            .revoke_member(crate::server::mutation_identity()?, "canopy", "ci")
            .await?;
        assert!(!page(&repository, selection.clone()).await?);
        assert_eq!(
            start(&repository, selection, 1).await?,
            CheckChange::NotFound
        );
        repository
            .sql
            .batch(
                crate::server::mutation_identity()?,
                SqlBatch {
                    statements: vec![SqlStatement {
                        sql: "UPDATE ref_generation SET visibility='public' WHERE singleton=1"
                            .into(),
                        parameters: vec![],
                    }],
                },
            )
            .await?;
        let (public, selection) = select(&repository, ReadIdentity::Anonymous, native.main).await?;
        assert!(page(&repository, selection.clone()).await?);
        repository
            .sql
            .batch(
                crate::server::mutation_identity()?,
                SqlBatch {
                    statements: vec![SqlStatement {
                        sql: "UPDATE ref_generation SET visibility='private' WHERE singleton=1"
                            .into(),
                        parameters: vec![],
                    }],
                },
            )
            .await?;
        assert!(!page(&repository, selection).await?);
        drop((snapshot, public, repository));
        timeout(Duration::from_secs(15), server.shutdown()).await??;
    }
    Ok(())
}
