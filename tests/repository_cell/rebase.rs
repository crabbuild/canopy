use super::branch_rules::{commit, identity, plan};
use super::*;
use canopy_server::pulls::{
    NewPull, PullChange, PullRevision,
    candidates::{CandidateRequest, CandidateResult},
    merge::{MergeOutcome, MergeRequest, MergeStrategy},
};
use crab_cell_runtime::{
    SqlCell, primitives::sql::SqlBatch, primitives::sql::SqlStatement, primitives::sql::SqlValue,
};
type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;

pub async fn verify(repo: &RepositoryCell, sql: &SqlCell<RepositoryModule>) -> Result {
    let tree = objects::put(repo, identity()?, ObjectKind::Tree, b"")
        .await?
        .output;
    let base = commit(repo, tree, &[], "Rebase base").await?;
    let first = commit(repo, tree, &[base], "First").await?;
    let source = commit(repo, tree, &[first], "Second").await?;
    let base_ref = "refs/heads/rebase-base";
    let source_ref = "refs/heads/rebase-source";
    for (reference, commit) in [(base_ref, base), (source_ref, source)] {
        repo.finalize_push(identity()?, plan(repo, reference, Some(commit)).await?)
            .await?;
    }
    let PullChange::Applied(number) = repo
        .create_pull(
            identity()?,
            "canopy",
            NewPull {
                id: uuid::Uuid::new_v4().into_bytes(),
                title: "Certified rebase",
                body: "",
                draft: false,
                source_ref,
                source_oid: &hex::encode(source),
                base_ref,
                base_oid: &hex::encode(base),
            },
        )
        .await?
        .output
    else {
        return Err("pull not created".into());
    };
    let revision = PullRevision {
        pull_version: 1,
        source_oid: hex::encode(source),
        source_version: 1,
        base_oid: hex::encode(base),
        base_version: 1,
    };
    let rewritten = |parent, author, message| {
        format!(
            "tree {}\nparent {}\nauthor {author} <test@example.invalid> 0 +0000\ncommitter canopy <canopy@users.canopy.invalid> 1 +0000\n\n{message}\n",
            hex::encode(tree),
            hex::encode(parent)
        )
    };
    let body = rewritten(base, "Test", "First");
    let correct_first = objects::put(repo, identity()?, ObjectKind::Commit, body.as_bytes())
        .await?
        .output;
    for (name, parent, author, message, accepted) in [
        ("skip", base, "Test", "Second", false),
        ("author", correct_first, "Forged", "Second", false),
        ("message", correct_first, "Test", "Replacement", false),
        ("valid", correct_first, "Test", "Second", true),
    ] {
        let body = rewritten(parent, author, message);
        let tip = objects::put(repo, identity()?, ObjectKind::Commit, body.as_bytes())
            .await?
            .output;
        // Certify genuine Git objects first. A valid graph alone must never
        // certify a chain with a dropped commit or changed author/message.
        repo.finalize_push(
            identity()?,
            plan(repo, &format!("refs/heads/rebase-proof-{name}"), Some(tip)).await?,
        )
        .await?;
        let id = uuid::Uuid::new_v4();
        let request = CandidateRequest {
            id: id.to_string(),
            revision: revision.clone(),
            strategy: MergeStrategy::Rebase,
            message: String::new(),
        };
        let result = CandidateResult::Ready {
            oid: hex::encode(tip),
            tree_oid: hex::encode(tree),
        };
        sql.batch(identity()?, SqlBatch { statements: vec![SqlStatement {
            sql: "INSERT INTO merge_candidates (id,binding,pull_number,actor,request,created_ms,result,source_oid,base_oid,oid) VALUES (?1,?2,?3,'canopy',?4,1000,?5,?6,?7,?8)".into(),
            parameters: vec![SqlValue::Blob(id.as_bytes().to_vec()), SqlValue::Blob(vec![0;32]), SqlValue::Integer(number), SqlValue::Text(serde_json::to_string(&request)?), SqlValue::Text(serde_json::to_string(&result)?), SqlValue::Blob(source.to_vec()), SqlValue::Blob(base.to_vec()), SqlValue::Blob(tip.to_vec())],
        }] }).await?;
        let publish = MergeRequest {
            id: uuid::Uuid::new_v4().to_string(),
            revision: revision.clone(),
            strategy: MergeStrategy::Rebase,
            candidate_id: Some(id.to_string()),
        };
        let outcome = repo
            .merge_pull(identity()?, "canopy", number, publish)
            .await;
        if accepted {
            assert!(matches!(outcome?.output, MergeOutcome::Applied { .. }));
            assert_eq!(
                repo.ref_state(base_ref, None).await?.output.unwrap().oid,
                Some(tip)
            );
        } else {
            assert!(
                matches!(outcome, Err(InvocationError::Rejected(result)) if result.output == MergeOutcome::Conflict),
                "{name}"
            );
            assert_eq!(
                repo.ref_state(base_ref, None).await?.output.unwrap().oid,
                Some(base)
            );
        }
    }
    Ok(())
}
