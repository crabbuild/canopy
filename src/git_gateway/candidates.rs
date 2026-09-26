use super::*;
use crate::{
    git_http::{GitProcess, read_bounded},
    pulls::{
        candidates::{
            CandidateOutcome, CandidateRequest, CandidateResult, MergeCandidate,
            command::CandidateAction, valid_result,
        },
        merge::MergeStrategy,
    },
};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use cellule_runtime::InvocationError;
use std::process::{ExitStatus, Stdio};
use tokio::io::AsyncWriteExt;

impl GitGateway {
    pub(crate) async fn prepare_candidate(
        &self,
        actor: &str,
        number: i64,
        request: CandidateRequest,
    ) -> Result<CandidateOutcome, GatewayError> {
        // Serialize native mutations with pushes; each candidate still gets a
        // disposable cache. The Cell command rechecks refs and authority later.
        let _push = self.push.lock().await;
        let identity = new_identity()?;
        let reserved = self
            .candidate_command(CandidateAction::Reserve {
                actor: actor.into(),
                number,
                request,
                created_ms: identity.issued_at_ms,
            })
            .await?;
        let CandidateOutcome::Applied(candidate) = reserved else {
            return Ok(reserved);
        };
        if candidate.result != CandidateResult::Pending {
            return Ok(CandidateOutcome::Applied(candidate));
        }
        let policy = self
            .repository
            .pull_review_policy(actor, number)
            .await
            .map_err(|error| GatewayError::Cell(Box::new(error)))?
            .output;
        if policy.is_none_or(|policy| {
            !policy.ready || policy.revision.as_ref() != Some(&candidate.request.revision)
        }) {
            return Ok(CandidateOutcome::Conflict);
        }
        let cached = self.build_cache(self.cell_refs().await?).await?;
        let result = prepare_native(&cached.backend, &candidate).await?;
        if !valid_result(&result) {
            return Err(GitHttpError::TooLarge.into());
        }
        cached.backend.cache.reconcile().await?;
        if let CandidateResult::Ready { oid, .. } = &result {
            let plan = PushPlan {
                actor: actor.into(),
                updates: vec![RefUpdate {
                    name: candidate.fetch_ref(),
                    expected: None,
                    new_oid: Some(parse_oid(oid)?),
                }],
            };
            self.persist_objects(&cached.backend, &cached.snapshot.refs, &plan)
                .await?;
            self.repository
                .prepare_graph(&plan)
                .await
                .map_err(|error| GatewayError::Cell(Box::new(error)))?;
        }
        self.candidate_command(CandidateAction::Finish {
            actor: actor.into(),
            id: candidate.request.id,
            result,
        })
        .await
    }

    async fn candidate_command(
        &self,
        action: CandidateAction,
    ) -> Result<CandidateOutcome, GatewayError> {
        match self
            .repository
            .candidate_action(new_identity()?, action)
            .await
        {
            Ok(result) => Ok(result.output),
            Err(InvocationError::Rejected(result)) => Ok(result.output),
            Err(error) => Err(GatewayError::Cell(Box::new(error))),
        }
    }
}

async fn prepare_native(
    backend: &GitHttpBackend,
    candidate: &MergeCandidate,
) -> Result<CandidateResult, GatewayError> {
    let revision = &candidate.request.revision;
    let related = run(
        backend,
        &["merge-base", &revision.base_oid, &revision.source_oid],
        b"",
        &[],
    )
    .await?;
    match related.status.code() {
        Some(0) => {}
        Some(1) => return Ok(CandidateResult::Unrelated),
        _ => return Err(related.error()),
    }
    // Native merge-tree consolidates multiple merge bases itself. Never select
    // one merge base or infer a clean result from an empty conflict-path list.
    let merged = run(
        backend,
        &[
            "merge-tree",
            "--write-tree",
            "--name-only",
            "--no-messages",
            "-z",
            &revision.base_oid,
            &revision.source_oid,
        ],
        b"",
        &[],
    )
    .await?;
    if !matches!(merged.status.code(), Some(0 | 1)) {
        return Err(merged.error());
    }
    let (tree_oid, paths) = merge_output(&merged.stdout)?;
    if merged.status.code() == Some(1) {
        return Ok(CandidateResult::Conflicted {
            paths_base64: paths
                .into_iter()
                .map(|path| URL_SAFE_NO_PAD.encode(path))
                .collect(),
        });
    }
    if !paths.is_empty() {
        return Err(GatewayError::MalformedCache);
    }
    let mut args = vec!["commit-tree", &tree_oid, "-p", &revision.base_oid];
    if candidate.request.strategy == MergeStrategy::MergeCommit {
        args.extend(["-p", &revision.source_oid]);
    }
    args.extend(["-F", "-", "--no-gpg-sign"]);
    let email = format!("{}@users.canopy.invalid", candidate.actor);
    let date = format!("@{} +0000", candidate.created_at_ms / 1000);
    let environment = [
        ("GIT_AUTHOR_NAME", candidate.actor.as_str()),
        ("GIT_COMMITTER_NAME", candidate.actor.as_str()),
        ("GIT_AUTHOR_EMAIL", email.as_str()),
        ("GIT_COMMITTER_EMAIL", email.as_str()),
        ("GIT_AUTHOR_DATE", date.as_str()),
        ("GIT_COMMITTER_DATE", date.as_str()),
    ];
    let mut message = candidate.request.message.clone();
    if !message.ends_with('\n') {
        message.push('\n');
    }
    let commit = run(backend, &args, message.as_bytes(), &environment).await?;
    if !commit.status.success() {
        return Err(commit.error());
    }
    let text = std::str::from_utf8(&commit.stdout).map_err(|_| GatewayError::MalformedCache)?;
    let oid = text
        .strip_suffix('\n')
        .ok_or(GatewayError::MalformedCache)?;
    parse_oid(oid)?;
    Ok(CandidateResult::Ready {
        oid: oid.into(),
        tree_oid,
    })
}

fn merge_output(bytes: &[u8]) -> Result<(String, Vec<&[u8]>), GatewayError> {
    let bytes = bytes
        .strip_suffix(&[0])
        .ok_or(GatewayError::MalformedCache)?;
    let mut fields = bytes.split(|byte| *byte == 0);
    let tree = std::str::from_utf8(fields.next().ok_or(GatewayError::MalformedCache)?)
        .map_err(|_| GatewayError::MalformedCache)?;
    parse_oid(tree)?;
    let paths: Vec<_> = fields.collect();
    if paths.iter().any(|path| path.is_empty()) {
        return Err(GatewayError::MalformedCache);
    }
    Ok((tree.into(), paths))
}

struct Output {
    status: ExitStatus,
    stdout: Vec<u8>,
    stderr: Vec<u8>,
}
impl Output {
    fn error(self) -> GatewayError {
        GitHttpError::GitExit {
            status: self.status,
            stderr: String::from_utf8_lossy(&self.stderr).into_owned(),
        }
        .into()
    }
}
async fn run(
    backend: &GitHttpBackend,
    args: &[&str],
    input: &[u8],
    environment: &[(&str, &str)],
) -> Result<Output, GatewayError> {
    let mut command = crate::native_git::command(&backend.git_dir());
    command
        .envs(environment.iter().copied())
        .arg("--git-dir")
        .arg(backend.git_dir())
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut process = GitProcess::spawn(&mut command, Arc::clone(&backend.cache))?;
    let mut stdin = process
        .child
        .stdin
        .take()
        .ok_or(GitHttpError::Interrupted)?;
    let stdout = process
        .child
        .stdout
        .take()
        .ok_or(GitHttpError::Interrupted)?;
    let stderr = process
        .child
        .stderr
        .take()
        .ok_or(GitHttpError::Interrupted)?;
    // Drain bounded pipes before reaping the leader, retaining the cache and
    // process group on cancellation or overflow until all children are killed.
    let (_, stdout, stderr) = tokio::try_join!(
        async {
            stdin.write_all(input).await?;
            stdin.shutdown().await?;
            drop(stdin);
            Ok::<_, GitHttpError>(())
        },
        read_bounded(stdout, 128 * 1024),
        read_bounded(stderr, 64 * 1024)
    )?;
    let status = process.child.wait().await?;
    process.disarm();
    Ok(Output {
        status,
        stdout,
        stderr,
    })
}
