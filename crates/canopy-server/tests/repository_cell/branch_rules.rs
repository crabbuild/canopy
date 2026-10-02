use super::*;
use canopy_server::{
    FinalizePush,
    branch_rules::BranchRuleEdit,
    checks::{CheckContextEdit, CheckEdit, CheckState, NewCheck},
};
use cellule_runtime::{
    SqlCell, primitives::sql::SqlBatch, primitives::sql::SqlStatement, primitives::sql::SqlValue,
};

type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;
type Oid = canopy_server::ObjectId;
pub(super) fn identity() -> Result<MutationIdentity> {
    let now = i64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis())?;
    Ok(MutationIdentity {
        request_id: RequestId::from_bytes(uuid::Uuid::new_v4().into_bytes()),
        issued_at_ms: now,
        expires_at_ms: now + 60_000,
    })
}
fn rule(version: i64, checks: &[&str]) -> BranchRuleEdit {
    BranchRuleEdit {
        reference: "refs/heads/protected".into(),
        expected_version: version,
        enabled: true,
        deny_deletions: true,
        fast_forward_only: true,
        require_pull_request: false,
        required_approvals: 0,
        required_checks: checks.iter().map(|s| (*s).into()).collect(),
    }
}
pub(super) async fn plan(repo: &RepositoryCell, name: &str, oid: Option<Oid>) -> Result<PushPlan> {
    Ok(PushPlan {
        actor: "canopy".into(),
        updates: vec![RefUpdate {
            name: name.into(),
            expected: repo.ref_state(name, None).await?.output,
            new_oid: oid,
        }],
    })
}
pub(super) async fn commit(
    repo: &RepositoryCell,
    tree: Oid,
    parents: &[Oid],
    message: &str,
) -> Result<Oid> {
    let parents: String = parents
        .iter()
        .map(|oid| format!("parent {}\n", hex::encode(oid)))
        .collect();
    let body = format!(
        "tree {}\n{parents}author Test <test@example.invalid> 0 +0000\ncommitter Test <test@example.invalid> 0 +0000\n\n{message}\n",
        hex::encode(tree)
    );
    Ok(
        objects::put(repo, identity()?, ObjectKind::Commit, body.as_bytes())
            .await?
            .output,
    )
}
async fn certificate(
    sql: &SqlCell<RepositoryModule>,
    ancestor: Oid,
    descendant: Oid,
) -> Result<bool> {
    let result = sql
        .query(
            None,
            SqlBatch {
                statements: vec![SqlStatement {
                    sql: "SELECT 1 FROM commit_ancestry WHERE ancestor = ?1 AND descendant = ?2"
                        .into(),
                    parameters: vec![
                        SqlValue::Blob(ancestor.to_vec()),
                        SqlValue::Blob(descendant.to_vec()),
                    ],
                }],
            },
        )
        .await?;
    Ok(!result.output[0].rows.is_empty())
}

pub async fn verify(
    repo: &RepositoryCell,
    sql: &SqlCell<RepositoryModule>,
    app: &ApplicationHandle<CanopyApplication>,
    target: &CellTarget,
) -> Result {
    let name = "refs/heads/protected";
    let tree = objects::put(repo, identity()?, ObjectKind::Tree, b"")
        .await?
        .output;
    let root = commit(repo, tree, &[], "Root").await?;
    let child = commit(repo, tree, &[root], "Child").await?;
    let unrelated = commit(repo, tree, &[], "Unrelated").await?;
    let mut tail = child;
    for number in 0..130 {
        tail = commit(repo, tree, &[tail], &format!("Chain {number}")).await?;
    }
    let merge = commit(repo, tree, &[unrelated, tail], "Merge").await?;
    repo.finalize_push(
        identity()?,
        plan(repo, "refs/heads/branch-fixture", Some(merge)).await?,
    )
    .await?;
    repo.finalize_push(identity()?, plan(repo, name, Some(root)).await?)
        .await?;
    assert!(matches!(
        repo.set_branch_rule(identity()?, "outsider", rule(0, &[]))
            .await,
        Err(InvocationError::Rejected(rejected)) if !rejected.output
    ));
    let policy_id = identity()?;
    let first = repo
        .set_branch_rule(policy_id, "canopy", rule(0, &[]))
        .await?;
    assert_eq!(
        repo.set_branch_rule(policy_id, "canopy", rule(0, &[]))
            .await?
            .receipt,
        first.receipt
    );
    assert!(matches!(
        repo.set_branch_rule(identity()?, "canopy", rule(0, &[]))
            .await,
        Err(InvocationError::Rejected(rejected)) if !rejected.output
    ));
    let advance = plan(repo, name, Some(merge)).await?;
    let generation = repo.refs_page("", None).await?.output.generation;
    // Even valid Git ancestry needs a server-verified certificate at publication.
    assert!(matches!(
        app.command::<FinalizePush>(target, identity()?, advance.clone())
            .await,
        Err(InvocationError::Rejected(rejected)) if !rejected.output
    ));
    // A malicious certificate cannot substitute a caller-asserted link, including
    // a valid first step followed by a false one in the same transaction.
    assert!(matches!(
        app.command::<CertificateCommand>(
            target,
            identity()?,
            Proof {
                ancestor: root,
                steps: vec![(child, root), (unrelated, child)]
            }
        )
        .await,
        Err(InvocationError::Rejected(rejected)) if !rejected.output
    ));
    assert!(!certificate(sql, root, child).await?);
    assert!(!certificate(sql, root, unrelated).await?);
    assert_eq!(
        repo.refs_page("", None).await?.output.generation,
        generation
    );
    // More than one proof page, including a merge's alternate ancestry path.
    let mutation = identity()?;
    let committed = repo.finalize_push(mutation, advance.clone()).await?;
    assert!(certificate(sql, root, merge).await?);
    let delete = plan(repo, name, None).await?;
    let force = plan(repo, name, Some(unrelated)).await?;
    for rejected in [delete, force] {
        assert!(matches!(
            app.command::<FinalizePush>(target, identity()?, rejected)
                .await,
            Err(InvocationError::Rejected(rejected)) if !rejected.output
        ));
    }
    repo.set_check_context(
        identity()?,
        "canopy",
        "branch-ci",
        CheckContextEdit {
            expected_version: 0,
            reporter: "canopy",
            enabled: true,
        },
    )
    .await?;
    repo.set_branch_rule(identity()?, "canopy", rule(1, &["branch-ci"]))
        .await?;
    // Completed receipt replay returns the prior result without applying new refs.
    assert_eq!(
        app.command::<FinalizePush>(target, mutation, advance)
            .await?
            .receipt,
        committed.receipt
    );
    assert_eq!(
        repo.ref_state(name, None).await?.output.unwrap().oid,
        Some(merge)
    );
    let candidate = commit(repo, tree, &[merge], "Candidate").await?;
    repo.finalize_push(
        identity()?,
        plan(repo, "refs/heads/branch-fixture", Some(candidate)).await?,
    )
    .await?;
    app.command::<CertificateCommand>(
        target,
        identity()?,
        Proof {
            ancestor: merge,
            steps: vec![(candidate, merge)],
        },
    )
    .await?;
    let proposed = plan(repo, name, Some(candidate)).await?;
    let old = uuid::Uuid::new_v4().into_bytes();
    repo.start_check(
        identity()?,
        "canopy",
        NewCheck {
            id: old,
            oid: candidate,
            context: "branch-ci",
            context_version: 1,
        },
    )
    .await?;
    repo.update_check(
        identity()?,
        "canopy",
        old,
        CheckEdit {
            expected_version: 1,
            state: CheckState::Success,
            summary: "passed",
        },
    )
    .await?;
    // Captured push intent remains the same while a new queued attempt arrives.
    let newer = uuid::Uuid::new_v4().into_bytes();
    repo.start_check(
        identity()?,
        "canopy",
        NewCheck {
            id: newer,
            oid: candidate,
            context: "branch-ci",
            context_version: 1,
        },
    )
    .await?;
    assert!(matches!(
        app.command::<FinalizePush>(target, identity()?, proposed.clone())
            .await,
        Err(InvocationError::Rejected(rejected)) if !rejected.output
    ));
    repo.update_check(
        identity()?,
        "canopy",
        newer,
        CheckEdit {
            expected_version: 1,
            state: CheckState::Success,
            summary: "passed again",
        },
    )
    .await?;
    // Current context enablement is checked in the same transaction as refs.
    repo.set_check_context(
        identity()?,
        "canopy",
        "branch-ci",
        CheckContextEdit {
            expected_version: 1,
            reporter: "canopy",
            enabled: false,
        },
    )
    .await?;
    assert!(matches!(
        app.command::<FinalizePush>(target, identity()?, proposed.clone())
            .await,
        Err(InvocationError::Rejected(rejected)) if !rejected.output
    ));
    repo.set_branch_rule(identity()?, "canopy", rule(2, &[]))
        .await?;
    app.command::<FinalizePush>(target, identity()?, proposed)
        .await?;
    assert_eq!(
        repo.ref_state(name, None).await?.output.unwrap().oid,
        Some(candidate)
    );
    // Rule paging uses names; disabled records retain versions and requirements.
    for number in 0..33 {
        let mut edit = rule(0, &["branch-ci"]);
        edit.reference = format!("refs/heads/page-{number:02}");
        edit.enabled = false;
        repo.set_branch_rule(identity()?, "canopy", edit).await?;
    }
    let first = repo.branch_rules("canopy", None).await?.output.unwrap();
    assert_eq!(first.len(), 32);
    let after = &first.last().unwrap().reference;
    let remaining = repo
        .branch_rules("canopy", Some(after))
        .await?
        .output
        .unwrap();
    assert_eq!(remaining.len(), 2);
    assert!(repo.branch_rules("outsider", None).await?.output.is_none());
    Ok(())
}

// Invoke the registered command with independently encoded input, as an untrusted
// client can; the local fixture handler must never be selected by the server.
struct Proof {
    ancestor: Oid,
    steps: Vec<(Oid, Oid)>,
}
impl cellule_runtime::codec::WireValue for Proof {
    fn encode(
        &self,
        out: &mut cellule_runtime::codec::BoundedEncoder,
    ) -> std::result::Result<(), cellule_runtime::codec::CodecError> {
        out.write_bytes(&self.ancestor)?;
        out.write_count(self.steps.len())?;
        for (child, parent) in &self.steps {
            out.write_bytes(child)?;
            out.write_bytes(parent)?;
        }
        Ok(())
    }
    fn decode(
        _: &mut cellule_runtime::codec::BoundedDecoder<'_>,
    ) -> std::result::Result<Self, cellule_runtime::codec::CodecError> {
        Err(cellule_runtime::codec::CodecError::Invalid(
            "encode-only test client",
        ))
    }
}
struct CertificateCommand;
impl cellule_runtime::Command for CertificateCommand {
    const MODULE: &'static str = "repository";
    const ID: u32 = 7;
    const CODEC_VERSION: u32 = 2;
    type Input = Proof;
    type Output = bool;
    fn execute(
        _: &mut cellule_runtime::registry::CommandContext<'_, '_>,
        _: Proof,
    ) -> cellule_runtime::Result<cellule_runtime::registry::CommandResult<bool>> {
        Err(cellule_runtime::Error::Command(
            "client handler must not execute",
        ))
    }
}
