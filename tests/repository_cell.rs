#[path = "repository_cell/batches.rs"]
mod batches;
#[path = "repository_cell/branch_rules.rs"]
mod branch_rules;
#[path = "repository_cell/bulk_refs.rs"]
mod bulk_refs;
#[path = "repository_cell/checks.rs"]
mod checks;
#[path = "repository_cell/chunks.rs"]
mod chunks;
#[path = "repository_cell/default_branch.rs"]
mod default_branch;
#[path = "repository_cell/graph.rs"]
mod graph;
#[path = "repository_cell/issues.rs"]
mod issues;
#[path = "repository_cell/merge.rs"]
mod merge;
#[path = "support/objects.rs"]
mod objects;
#[path = "repository_cell/pages.rs"]
mod pages;
#[path = "repository_cell/pulls.rs"]
mod pulls;
#[path = "repository_cell/rebase.rs"]
mod rebase;
#[path = "repository_cell/visibility.rs"]
mod visibility;

use std::{
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use canopy_server::{
    CanopyApplication, ObjectKind, PushPlan, RefExpectation, RefUpdate, RepositoryCell,
    RepositoryModule, build_descriptor, directory::TokenScope, object_id, repository_target,
};
use cellule_app::{ApplicationHandle, CellApplication};
use cellule_ltx::{CellReplica, DiskBudget, Host, Limits};
use cellule_runtime::{
    ApplicationId, CellClient, CellModule, CellRuntime, CellTarget, Error, InvocationError,
    MutationIdentity, NamespaceId, SessionId, TenantId, cell::catalog::CatalogEntry,
    cell::catalog::CatalogRole, cell::catalog::CellCatalog, cell::worker::SqlWorkerPool,
    control::Owner, control::authority::CellAuthority, identity::IncarnationId,
    identity::RequestId, ltx::CellStorageLayout,
};
use cellule_store::Store;
use object_store::{memory::InMemory, path::Path};

#[tokio::test(flavor = "multi_thread")]
async fn repository_cell_publishes_objects_and_refs_atomically()
-> Result<(), Box<dyn std::error::Error>> {
    let application = Arc::new(CanopyApplication::compile(build_descriptor(
        include_bytes!("../Cargo.toml"),
        "repository-cell-test",
    ))?);
    let tenant = TenantId::from_bytes([2; 16]);
    let application_id = ApplicationId::from_bytes([3; 16]);
    let mut repository_id = [4; 16];
    repository_id[6] = 0x74;
    repository_id[8] = 0x84;
    let target = repository_target(tenant, application_id, repository_id)?;
    let layout = CellStorageLayout::new(
        Store::new(Arc::new(InMemory::new())),
        Path::from("canopy-test"),
        *application_id.as_bytes(),
    );
    let registry = application.registry();
    let code = registry
        .module_code(RepositoryModule::NAME)
        .ok_or(Error::Registry("repository module missing"))?;
    let proof = CellCatalog::new(layout.clone(), tenant)
        .provision(CatalogEntry::new(&target, CatalogRole::Sql, code, 1)?)
        .await?;
    let authority = CellAuthority::new(layout.clone());
    let incarnation = IncarnationId::from_bytes([5; 16]);
    let session = SessionId::from_bytes([6; 16]);
    let observed = authority
        .create_initial(
            &proof,
            incarnation,
            Owner {
                session,
                endpoint: "https://canopy.test".into(),
            },
        )
        .await?;
    let files = tempfile::TempDir::new()?;
    let runtime = CellRuntime::new_with_replica_host(
        SqlWorkerPool::new(1, 4)?,
        16 * 1024 * 1024,
        session,
        Host::default().with_local_disk_budget(DiskBudget::new(1 << 30)),
    )?;
    let outcome: Result<(), Box<dyn std::error::Error>> = async {
        let handle = runtime
            .bootstrap(
                proof,
                CellReplica::new(
                    layout,
                    *target.cell_id().as_bytes(),
                    *incarnation.as_bytes(),
                    Limits::default(),
                )?,
                authority,
                observed,
                files.path().join("repository.sqlite"),
                |transaction| {
                    transaction.execute_batch(include_str!("../src/schema.sql"))?;
                    Ok(())
                },
            )
            .await?;
        let application_handle = ApplicationHandle::<CanopyApplication>::new(
            CellClient::local(registry, handle),
            application,
            tenant,
            application_id,
        )?;
        assert!(
            application_handle
                .sql::<RepositoryModule>(CellTarget::new(
                    tenant,
                    application_id,
                    canopy_server::REPOSITORIES,
                    &[0; 16],
                )?)
                .is_err()
        );
        assert!(
            application_handle
                .sql::<RepositoryModule>(CellTarget::new(
                    tenant,
                    application_id,
                    canopy_server::REPOSITORIES,
                    &[0; 4],
                )?)
                .is_err()
        );
        assert!(
            application_handle
                .sql::<RepositoryModule>(CellTarget::new(
                    TenantId::from_bytes([99; 16]),
                    application_id,
                    canopy_server::REPOSITORIES,
                    target.partition(),
                )?)
                .is_err()
        );
        assert!(
            application_handle
                .sql::<RepositoryModule>(CellTarget::new(
                    tenant,
                    application_id,
                    NamespaceId::from_bytes([99; 16]),
                    target.partition(),
                )?)
                .is_err()
        );
        let mut other_id = repository_id;
        other_id[0] ^= 1;
        assert!(matches!(
            RepositoryCell::new(&application_handle, target.clone(), other_id, canopy_server::ObjectFormat::Sha1),
            Err(Error::Identity("repository UUID differs from Cell target"))
        ));
        let graph_sql = application_handle.sql::<RepositoryModule>(target.clone())?;
        let repository = RepositoryCell::new(&application_handle, target.clone(), repository_id, canopy_server::ObjectFormat::Sha1)?;
        let empty = repository.refs_page("", None).await?.output;
        assert_eq!(empty.generation, 0);
        assert!(empty.refs.is_empty());
        let body = b"Canopy stores ordinary Git objects in a Cell";
        let now_ms = i64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis())?;
        let identity = |byte| MutationIdentity {
            request_id: RequestId::from_bytes([byte; 16]),
            issued_at_ms: now_ms,
            expires_at_ms: now_ms + 60_000,
        };
        repository
            .ensure_owner(
                MutationIdentity {
                    request_id: RequestId::from_bytes([11; 16]),
                    issued_at_ms: now_ms,
                    expires_at_ms: now_ms + 60_000,
                },
                "canopy",
            )
            .await?;
        pages::exercise(&repository, &graph_sql).await?;
        batches::exercise(&repository, &graph_sql).await?;
        issues::exercise(&repository).await?;
        default_branch::empty(&repository).await?;
        let committed = objects::put(&repository,
                MutationIdentity {
                    request_id: RequestId::from_bytes([7; 16]),
                    issued_at_ms: now_ms,
                    expires_at_ms: now_ms + 60_000,
                },
                ObjectKind::Blob,
                body,
            )
            .await?;
        assert_eq!(committed.output, object_id(canopy_server::ObjectFormat::Sha1, ObjectKind::Blob, body));
        assert_eq!(
            repository
                .object(committed.output, Some(committed.receipt))
                .await?
                .output,
            Some((ObjectKind::Blob, body.to_vec()))
        );
        let tree = objects::put(&repository, identity(26), ObjectKind::Tree, b"")
            .await?.output;
        let first_commit = format!("tree {}\nauthor Canopy <test@example.invalid> 0 +0000\ncommitter Canopy <test@example.invalid> 0 +0000\n\nFirst\n", hex::encode(tree));
        let committed = objects::put(&repository, identity(27), ObjectKind::Commit, first_commit.as_bytes())
            .await?;
        checks::exercise(&repository, committed.output).await?;
        let second_commit = format!("tree {}\nparent {}\nauthor Canopy <test@example.invalid> 1 +0000\ncommitter Canopy <test@example.invalid> 1 +0000\n\nSecond\n", hex::encode(tree), hex::encode(committed.output));
        let next = objects::put(&repository,
                MutationIdentity {
                    request_id: RequestId::from_bytes([8; 16]),
                    issued_at_ms: now_ms,
                    expires_at_ms: now_ms + 60_000,
                },
                ObjectKind::Commit,
                second_commit.as_bytes(),
            )
            .await?;
        // The empty-ref fast path still rejects an ancestor and descendant in
        // the same atomic plan, with no generation change or partial writes.
        let before = repository.refs_page("", None).await?.output;
        assert!(before.refs.is_empty());
        assert!(matches!(
            repository
                .finalize_push(
                    identity(28),
                    PushPlan {
                        actor: "canopy".into(),
                        updates: ["refs/tags/conflict", "refs/tags/conflict/child"]
                            .into_iter()
                            .map(|name| RefUpdate {
                                name: name.into(),
                                expected: None,
                                new_oid: Some(committed.output),
                            })
                            .collect(),
                    },
                )
                .await,
            Err(InvocationError::Rejected(_))
        ));
        assert_eq!(
            repository.default_branch(None).await?.output.generation,
            before.generation
        );
        let published = repository
            .finalize_push(
                MutationIdentity {
                    request_id: RequestId::from_bytes([9; 16]),
                    issued_at_ms: now_ms,
                    expires_at_ms: now_ms + 60_000,
                },
                PushPlan {
                    actor: "canopy".into(),
                    updates: vec![
                        RefUpdate {
                            name: "refs/heads/main".into(),
                            expected: None,
                            new_oid: Some(committed.output),
                        },
                        RefUpdate {
                            name: "refs/heads/other".into(),
                            expected: None,
                            new_oid: Some(next.output),
                        },
                    ],
                },
            )
            .await?;
        assert!(published.output);
        let expected_main = RefExpectation {
            oid: Some(committed.output),
            version: 1,
        };
        assert_eq!(
            repository
                .ref_state("refs/heads/main", Some(published.receipt))
                .await?
                .output,
            Some(expected_main.clone())
        );
        let conflict = repository
            .finalize_push(
                MutationIdentity {
                    request_id: RequestId::from_bytes([10; 16]),
                    issued_at_ms: now_ms,
                    expires_at_ms: now_ms + 60_000,
                },
                PushPlan {
                    actor: "canopy".into(),
                    updates: vec![
                        RefUpdate {
                            name: "refs/heads/main".into(),
                            expected: Some(RefExpectation {
                                oid: Some(committed.output),
                                version: 2,
                            }),
                            new_oid: Some(next.output),
                        },
                        RefUpdate {
                            name: "refs/tags/rejected".into(),
                            expected: None,
                            new_oid: Some(next.output),
                        },
                    ],
                },
            )
            .await;
        assert!(matches!(conflict, Err(InvocationError::Rejected(_))));
        assert_eq!(
            repository.ref_state("refs/heads/main", None).await?.output,
            Some(expected_main)
        );
        assert_eq!(
            repository
                .ref_state("refs/tags/rejected", None)
                .await?
                .output,
            None
        );
        assert!(
            repository
                .grant_member(identity(12), "canopy", "reader", TokenScope::Read)
                .await?
                .output
        );
        assert_eq!(repository.collaborators("canopy", None).await?.output, Some(vec![canopy_server::Collaborator { account: "reader".into(), role: TokenScope::Read }]));
        for actor in ["reader", "outsider"] {
            assert!(repository.collaborators(actor, None).await?.output.is_none());
        }
        let reader_plan = PushPlan {
            actor: "reader".into(),
            updates: vec![RefUpdate {
                name: "refs/tags/reader".into(),
                expected: None,
                new_oid: Some(next.output),
            }],
        };
        assert!(matches!(
            repository
                .finalize_push(identity(13), reader_plan.clone())
                .await,
            Err(InvocationError::Rejected(_))
        ));
        assert!(
            repository
                .grant_member(identity(14), "canopy", "reader", TokenScope::Write)
                .await?
                .output
        );
        assert!(
            repository
                .finalize_push(identity(15), reader_plan.clone())
                .await?
                .output
        );
        assert!(
            repository
                .revoke_member(identity(16), "canopy", "reader")
                .await?
                .output
        );
        assert_eq!(repository.access_level("reader", None).await?.output, None);
        assert_eq!(repository.collaborators("canopy", None).await?.output, Some(Vec::new()));
        let after_revoke = PushPlan {
            actor: "reader".into(),
            updates: vec![RefUpdate {
                name: "refs/tags/after-revoke".into(),
                expected: None,
                new_oid: Some(next.output),
            }],
        };
        assert!(matches!(
            repository.finalize_push(identity(17), after_revoke).await,
            Err(InvocationError::Rejected(_))
        ));
        assert!(
            repository
                .ref_state("refs/tags/after-revoke", None)
                .await?
                .output
                .is_none()
        );
        let original_state = repository.ref_state("refs/heads/main", None).await?.output;
        let plan = |expected, new_oid| PushPlan {
            actor: "canopy".into(),
            updates: vec![RefUpdate {
                name: "refs/heads/main".into(),
                expected,
                new_oid,
            }],
        };
        repository
            .finalize_push(identity(18), plan(original_state.clone(), None))
            .await?;
        let deleted_state = repository.ref_state("refs/heads/main", None).await?.output;
        repository
            .finalize_push(
                identity(19),
                plan(deleted_state.clone(), Some(committed.output)),
            )
            .await?;
        assert!(matches!(
            repository
                .finalize_push(identity(20), plan(original_state, Some(next.output)))
                .await,
            Err(InvocationError::Rejected(_))
        ));
        let recreated = repository.ref_state("refs/heads/main", None).await?.output;
        assert_eq!(
            recreated,
            Some(RefExpectation {
                oid: Some(committed.output),
                version: 3
            })
        );
        repository
            .finalize_push(identity(21), plan(recreated, None))
            .await?;
        assert!(matches!(
            repository
                .finalize_push(identity(22), plan(deleted_state, Some(next.output)))
                .await,
            Err(InvocationError::Rejected(_))
        ));
        let deleted = repository.ref_state("refs/heads/main", None).await?.output;
        assert_eq!(
            deleted,
            Some(RefExpectation {
                oid: None,
                version: 4
            })
        );
        let child = RefUpdate {
            name: "refs/heads/main/topic".into(),
            expected: None,
            new_oid: Some(next.output),
        };
        repository
            .finalize_push(
                identity(23),
                PushPlan {
                    actor: "canopy".into(),
                    updates: vec![child],
                },
            )
            .await?;
        assert!(matches!(
            repository
                .finalize_push(identity(24), plan(deleted.clone(), Some(committed.output)))
                .await,
            Err(InvocationError::Rejected(_))
        ));
        let child = repository
            .ref_state("refs/heads/main/topic", None)
            .await?
            .output;
        let mut replacement = plan(deleted, Some(committed.output));
        replacement.updates.push(RefUpdate {
            name: "refs/heads/main/topic".into(),
            expected: child,
            new_oid: None,
        });
        repository.finalize_push(identity(25), replacement).await?;
        assert_eq!(
            repository.ref_state("refs/heads/main", None).await?.output,
            Some(RefExpectation {
                oid: Some(committed.output),
                version: 5
            })
        );
        default_branch::verify(&repository).await?;
        visibility::verify(&repository).await?;
        graph::verify(&repository, &graph_sql, &application_handle, &target).await?;
        branch_rules::verify(&repository, &graph_sql, &application_handle, &target).await?;
        pulls::verify(&repository).await?;
        merge::verify(&repository, &graph_sql, &application_handle, &target).await?;
        rebase::verify(&repository, &graph_sql).await?;
        chunks::exercise(&repository, &graph_sql).await?;
        bulk_refs::verify(&repository).await?;
        Ok(())
    }
    .await;
    let shutdown = runtime.shutdown().await;
    outcome?;
    shutdown?;
    Ok(())
}
