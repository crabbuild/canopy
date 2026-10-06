//! Real stock-Git pack bytes and private closed native catalogs. These isolate
//! generated commit verification; joint Ready publication remains separate.
use super::*;
use super::{
    prepare::{opened_native, physical},
    publishing::repack,
};
use crate::{
    ObjectId, ObjectKind,
    packs::{
        catalog::{CatalogFileLimits, CatalogFiles},
        metadata::tests::limits,
        verification::physical::tests::{Prepared, independence::git_input},
    },
    pulls::{
        PullRevision,
        candidates::{
            CandidateRequest, CandidateResult, MergeCandidate, commit_body, rebase::Commit,
        },
        merge::MergeStrategy,
    },
};
use cellule_ltx::DiskBudget;

fn oid(bytes: Vec<u8>) -> Result<ObjectId> {
    let s = String::from_utf8(bytes)?;
    Ok(crate::pulls::merge::oid(s.trim())?)
}
pub(super) async fn write(native: &Prepared, body: &[u8], name: &str) -> Result<ObjectId> {
    let o = oid(git_input(
        native.fixture.root.path(),
        &["hash-object", "-t", "commit", "-w", "--stdin"],
        body,
    )
    .await?)?;
    assert_eq!(o, crate::object_id(o.format(), ObjectKind::Commit, body));
    git_input(
        native.fixture.root.path(),
        &["update-ref", name, &hex::encode(o)],
        b"",
    )
    .await?;
    Ok(o)
}
fn original(tree: ObjectId, parent: ObjectId, message: &str) -> Vec<u8> {
    format!("tree {}\nparent {}\nauthor Author <author@example.invalid> 1 +0000\ncommitter Original <original@example.invalid> 2 +0000\nencoding UTF-8\ngpgsig stale signature\n continuation\nmergetag stale tag\n continuation\n\n{message}\n",hex::encode(tree),hex::encode(parent)).into_bytes()
}
pub(super) fn candidate(
    strategy: MergeStrategy,
    base: ObjectId,
    source: ObjectId,
) -> MergeCandidate {
    MergeCandidate {
        request: CandidateRequest {
            id: uuid::Uuid::new_v4().to_string(),
            revision: PullRevision {
                pull_version: 1,
                source_oid: hex::encode(source),
                source_version: 1,
                base_oid: hex::encode(base),
                base_version: 1,
            },
            message: if strategy == MergeStrategy::Rebase {
                String::new()
            } else {
                "Exact candidate message".into()
            },
            strategy,
        },
        number: 1,
        actor: "owner".into(),
        created_at_ms: 5000,
        result: CandidateResult::Pending,
    }
}
pub(super) fn ready(c: &MergeCandidate, tip: ObjectId, tree: ObjectId) -> MergeCandidate {
    let mut c = c.clone();
    c.result = CandidateResult::Ready {
        oid: hex::encode(tip),
        tree_oid: hex::encode(tree),
    };
    c
}
pub(super) fn initial(native: &Prepared) -> Result<(ObjectId, ObjectId)> {
    let (commit, edges) = native
        .fixture
        .objects
        .values()
        .find(|(o, _)| o.kind == ObjectKind::Commit)
        .ok_or("initial commit")?;
    let tree = edges
        .iter()
        .find(|e| e.expected_kind == ObjectKind::Tree)
        .ok_or("initial tree")?
        .child;
    Ok((commit.oid, tree))
}

pub(super) async fn catalog(
    f: &Fixture,
    native: &mut Prepared,
    base: Arc<PreparationBaseResolver>,
) -> Result<(PreparedCatalog, tempfile::TempDir, DiskBudget)> {
    repack(native).await?;
    let root = tempfile::TempDir::new()?;
    let budget = DiskBudget::new(256 << 20);
    let files = Arc::new(
        CatalogFiles::new(
            f.root.path(),
            budget.clone(),
            native.store.clone(),
            f.format,
            CatalogFileLimits::default(),
        )?
        .with_native(
            crate::native_resources::NativeResources::default()
                .scope(crate::native_resources::NativeClass::Foreground),
        ),
    );
    let base = Arc::new(
        PreparationBaseResolver::from_session(base.session.clone(), base.indexes(), files).await?,
    );
    let mut builder = CatalogPreparation::new(root.path(), budget.clone(), base, limits()).await?;
    let (witness, segments) = physical(native, root.path(), budget.clone()).await?;
    builder.begin_pack(witness)?;
    for segment in segments {
        builder.add_segment(segment).await?;
    }
    builder.finish_pack().await?;
    Ok((builder.finish().await?, root, budget))
}
async fn verified(
    p: &PreparedCatalog,
    c: &MergeCandidate,
    root: &tempfile::TempDir,
    budget: &DiskBudget,
) -> Result {
    p.verify_candidate_commit(c, root.path(), budget.clone(), limits())
        .await?;
    Ok(())
}
async fn refused(
    p: &PreparedCatalog,
    c: &MergeCandidate,
    root: &tempfile::TempDir,
    budget: &DiskBudget,
) -> Result {
    assert!(matches!(
        p.verify_candidate_commit(c, root.path(), budget.clone(), limits())
            .await,
        Err(NativeCandidateVerificationError::Invalid)
    ));
    Ok(())
}
#[tokio::test]
async fn generated_merge_and_squash_require_exact_verified_bytes_without_native_body_downloads()
-> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let f = Fixture::new(format).await?;
        let (mut native, base, _, _) =
            opened_native(&f, *uuid::Uuid::new_v4().as_bytes(), 2).await?;
        let (initial, tree) = initial(&native)?;
        let source = write(
            &native,
            &original(tree, initial, "source"),
            "refs/heads/source",
        )
        .await?;
        let merge = candidate(MergeStrategy::MergeCommit, initial, source);
        let merge_oid = write(
            &native,
            &commit_body(&merge, &hex::encode(tree)),
            "refs/heads/generated-merge",
        )
        .await?;
        let squash = candidate(MergeStrategy::Squash, initial, source);
        let squash_oid = write(
            &native,
            &commit_body(&squash, &hex::encode(tree)),
            "refs/heads/generated-squash",
        )
        .await?;
        let mut swapped = merge.clone();
        swapped.request.revision.base_oid = hex::encode(source);
        swapped.request.revision.source_oid = hex::encode(initial);
        let swapped_oid = write(
            &native,
            &commit_body(&swapped, &hex::encode(tree)),
            "refs/heads/swapped",
        )
        .await?;
        let (p, root, budget) = catalog(&f, &mut native, base).await?;
        let good = ready(&merge, merge_oid, tree);
        verified(&p, &good, &root, &budget).await?;
        verified(&p, &ready(&squash, squash_oid, tree), &root, &budget).await?;
        for mut c in [
            good.clone(),
            good.clone(),
            good.clone(),
            good.clone(),
            good.clone(),
        ]
        .into_iter()
        .enumerate()
        {
            match c.0 {
                0 => c.1.request.message.push('!'),
                1 => c.1.created_at_ms += 1000,
                2 => c.1.actor = "another".into(),
                3 => c.1.result = CandidateResult::Pending,
                _ => {
                    c.1.result = CandidateResult::Ready {
                        oid: hex::encode(merge_oid),
                        tree_oid: hex::encode(source),
                    }
                }
            }
            refused(&p, &c.1, &root, &budget).await?;
        }
        refused(&p, &ready(&merge, swapped_oid, tree), &root, &budget).await?;
        refused(&p, &ready(&merge, squash_oid, tree), &root, &budget).await?;
        refused(
            &p,
            &ready(
                &merge,
                crate::object_id(format, ObjectKind::Commit, b"unpublished"),
                tree,
            ),
            &root,
            &budget,
        )
        .await?;
        assert_eq!(
            p.base
                .files()
                .native_stats()?
                .ok_or("native stats")?
                .downloaded_files,
            0
        );
        let legacy=f.handle.query(0,4096,|db|Ok(db.query_row("SELECT count(*) FROM sqlite_schema WHERE type='table' AND name IN ('objects','commit_ancestry')",[],|r|r.get::<_,u64>(0))?.to_le_bytes().to_vec())).await?;
        assert_eq!(
            u64::from_le_bytes(legacy.try_into().map_err(|_| "legacy count")?),
            0
        );
        p.base.session.fence();
        assert!(matches!(
            p.verify_candidate_commit(&good, root.path(), budget.clone(), limits())
                .await,
            Err(NativeCandidateVerificationError::Base(
                PreparationBaseError::Inactive
            ))
        ));
        drop(p);
        f.runtime.shutdown().await?;
    }
    Ok(())
}
#[tokio::test]
async fn native_rebase_binds_every_original_and_rejects_skips_and_replayed_base_history() -> Result
{
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let f = Fixture::new(format).await?;
        let (mut native, base, _, _) =
            opened_native(&f, *uuid::Uuid::new_v4().as_bytes(), 2).await?;
        let (initial, tree) = initial(&native)?;
        let common_body = original(tree, initial, "common");
        let common = write(&native, &common_body, "refs/heads/common").await?;
        let base_oid = write(&native, &original(tree, common, "base"), "refs/heads/base").await?;
        let first_body = original(tree, common, "first");
        let first = write(&native, &first_body, "refs/heads/first").await?;
        let last_body = original(tree, first, "last");
        let source = write(&native, &last_body, "refs/heads/source").await?;
        let c = candidate(MergeStrategy::Rebase, base_oid, source);
        let rewrite = |body: &[u8], parent: ObjectId| -> Result<Vec<u8>> {
            Ok(Commit::parse(body).ok_or("original parse")?.rewrite(
                &c,
                &hex::encode(tree),
                &hex::encode(parent),
            ))
        };
        let rewritten_first = write(
            &native,
            &rewrite(&first_body, base_oid)?,
            "refs/heads/rebase-first",
        )
        .await?;
        let rewritten_last = write(
            &native,
            &rewrite(&last_body, rewritten_first)?,
            "refs/heads/rebase-last",
        )
        .await?;
        let skipped = write(
            &native,
            &rewrite(&last_body, base_oid)?,
            "refs/heads/skipped",
        )
        .await?;
        let extra = write(
            &native,
            &rewrite(&common_body, base_oid)?,
            "refs/heads/extra",
        )
        .await?;
        let extra_first = write(
            &native,
            &rewrite(&first_body, extra)?,
            "refs/heads/extra-first",
        )
        .await?;
        let extra_last = write(
            &native,
            &rewrite(&last_body, extra_first)?,
            "refs/heads/extra-last",
        )
        .await?;
        let mut wrong = rewrite(&last_body, rewritten_first)?;
        wrong.extend_from_slice(b"tampered message");
        let wrong = write(&native, &wrong, "refs/heads/wrong").await?;
        let (p, root, budget) = catalog(&f, &mut native, base).await?;
        verified(&p, &ready(&c, rewritten_last, tree), &root, &budget).await?;
        for tip in [skipped, extra_last, wrong] {
            refused(&p, &ready(&c, tip, tree), &root, &budget).await?;
        }
        let stats = p.base.files().native_stats()?.ok_or("native stats")?;
        assert_eq!(stats.downloaded_files, 1);
        assert!(stats.cache_hits > 0);
        let reader =
            crate::packs::catalog::CatalogReader::open(p.base.indexes(), p.catalog()).await?;
        let files = p.base.files();
        let mut walker =
            super::super::ref_proof::ancestry::Walker::new(root.path(), budget.clone(), limits())
                .await?;
        assert_eq!(
            walker
                .ancestors_within(
                    &reader,
                    &files,
                    &[source, common, initial, common, base_oid],
                    base_oid,
                    &p.base
                )
                .await?,
            vec![false, true, true, true, true]
        );
        assert_eq!(
            walker
                .ancestors_within(&reader, &files, &[source, first, common], source, &p.base)
                .await?,
            vec![true, true, true]
        );
        drop(walker);
        drop(p);
        f.runtime.shutdown().await?;
    }
    Ok(())
}

#[tokio::test]
async fn native_rebase_accepts_128_commits_and_refuses_129_before_publication() -> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let f = Fixture::new(format).await?;
        let (mut native, base, _, _) =
            opened_native(&f, *uuid::Uuid::new_v4().as_bytes(), 1).await?;
        let (initial, tree) = initial(&native)?;
        let count = crate::pulls::candidates::rebase::MAX_COMMITS + 1;
        let mut input = Vec::new();
        for rewritten in [false, true] {
            for n in 1..=count {
                let mark = n + if rewritten { count } else { 0 };
                let parent = if n == 1 {
                    hex::encode(initial)
                } else {
                    format!(":{}", mark - 1)
                };
                let (actor, email, time, reference) = if rewritten {
                    (
                        "owner",
                        "owner@users.canopy.invalid",
                        5,
                        "refs/heads/rewritten",
                    )
                } else {
                    (
                        "Original",
                        "original@example.invalid",
                        2,
                        "refs/heads/original",
                    )
                };
                input.extend_from_slice(format!("commit {reference}\nmark :{mark}\nauthor Author <author@example.invalid> 1 +0000\ncommitter {actor} <{email}> {time} +0000\ndata 1\nx\nfrom {parent}\n\n").as_bytes());
            }
        }
        let marks_path = native.fixture.root.path().join("candidate-marks");
        let marks_arg = format!("--export-marks={}", marks_path.display());
        git_input(
            native.fixture.root.path(),
            &["fast-import", "--quiet", &marks_arg],
            &input,
        )
        .await?;
        let marks = std::fs::read_to_string(marks_path)?;
        let selected = |n: usize| -> Result<ObjectId> {
            let prefix = format!(":{n} ");
            Ok(crate::pulls::merge::oid(
                marks
                    .lines()
                    .find_map(|l| l.strip_prefix(&prefix))
                    .ok_or("mark")?,
            )?)
        };
        let short = candidate(MergeStrategy::Rebase, initial, selected(count - 1)?);
        let long = candidate(MergeStrategy::Rebase, initial, selected(count)?);
        let short_tip = selected(count * 2 - 1)?;
        let long_tip = selected(count * 2)?;
        let (p, root, budget) = catalog(&f, &mut native, base).await?;
        super::prepare::renewing(&f, &p.base, async {
            verified(&p, &ready(&short, short_tip, tree), &root, &budget).await?;
            refused(&p, &ready(&long, long_tip, tree), &root, &budget).await
        })
        .await?;
        assert_eq!(
            p.base
                .files()
                .native_stats()?
                .ok_or("native stats")?
                .downloaded_files,
            1
        );
        drop(p);
        f.runtime.shutdown().await?;
    }
    Ok(())
}
