use super::*;
use canopy_server::{
    ObjectBatch, ObjectKind, PushPlan, RefExpectation, RefReadError, RefUpdate, StoredObject,
    object_id,
};
use std::collections::BTreeMap;

const FIRST: &str = "refs/tags/snapshot-000";
const LAST: &str = "refs/tags/snapshot-299";

async fn pair(
    repository: &RepositoryCell,
    version: i64,
    old: Option<[u8; 20]>,
    new: Option<[u8; 20]>,
) -> Result<(), Box<dyn std::error::Error>> {
    repository
        .finalize_push(support::identity()?, pair_plan(version, old, new))
        .await?;
    Ok(())
}

fn pair_plan(version: i64, old: Option<[u8; 20]>, new: Option<[u8; 20]>) -> PushPlan {
    PushPlan {
        actor: "canopy".into(),
        updates: [FIRST, LAST]
            .into_iter()
            .map(|name| RefUpdate {
                name: name.into(),
                expected: Some(RefExpectation { oid: old, version }),
                new_oid: new,
            })
            .collect(),
    }
}

pub async fn verify(
    repository: &RepositoryCell,
    client: &reqwest::Client,
    url: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let initial = repository.refs_page("", None).await?.output.generation;
    let mut batch = ObjectBatch::default();
    let a = object_id(ObjectKind::Blob, b"snapshot a");
    let b = object_id(ObjectKind::Blob, b"snapshot b");
    for (oid, body) in [(a, b"snapshot a"), (b, b"snapshot b")] {
        batch
            .try_push(StoredObject {
                oid,
                kind: ObjectKind::Blob,
                storage: ObjectStorage::Inline(body.to_vec()),
            })
            .map_err(|_| "fixture object exceeds batch limit")?;
    }
    repository.put_objects(support::identity()?, batch).await?;
    assert_eq!(
        repository.refs_page("", None).await?.output.generation,
        initial
    );
    for start in (0..300).step_by(64) {
        repository
            .finalize_push(
                support::identity()?,
                PushPlan {
                    actor: "canopy".into(),
                    updates: (start..(start + 64).min(300))
                        .map(|index| RefUpdate {
                            name: format!("refs/tags/snapshot-{index:03}"),
                            expected: None,
                            new_oid: Some(a),
                        })
                        .collect(),
                },
            )
            .await?;
    }
    let first = repository.refs_page("", None).await?.output;
    assert_eq!(first.generation, initial + 5);
    assert_eq!(first.refs.len(), 256);
    let cursor = first.refs.last().ok_or("missing first page")?.0.clone();
    assert!(repository.refs_page(&cursor, None).await.is_err());
    pair(repository, 1, Some(a), Some(b)).await?;
    for after in [&cursor, "zzzz"] {
        assert!(matches!(
            repository.refs_page(after, Some(first.generation)).await,
            Err(RefReadError::Changed)
        ));
    }
    let current = repository.refs_page("", None).await?.output;
    assert_eq!(current.generation, first.generation + 1);
    let mut refs: BTreeMap<_, _> = current.refs.into_iter().collect();
    let second = repository
        .refs_page(&cursor, Some(current.generation))
        .await?
        .output;
    assert_eq!(second.generation, current.generation);
    refs.extend(second.refs);
    assert_eq!(refs[FIRST].oid, Some(b));
    assert_eq!(refs[LAST].oid, Some(b));
    assert_eq!(
        refs.keys()
            .filter(|name| name.starts_with("refs/tags/snapshot-"))
            .count(),
        300
    );
    assert!(
        repository
            .refs_page("zzzz", Some(current.generation))
            .await?
            .output
            .refs
            .is_empty()
    );

    // Returning to the same OIDs, and deleting/recreating names, must both
    // invalidate old pages even when their ref tips appear unchanged.
    pair(repository, 2, Some(b), Some(a)).await?;
    pair(repository, 3, Some(a), Some(b)).await?;
    assert!(matches!(
        repository
            .refs_page(&cursor, Some(current.generation))
            .await,
        Err(RefReadError::Changed)
    ));
    pair(repository, 4, Some(b), None).await?;
    let deleted = repository.refs_page("", None).await?.output;
    assert!(
        deleted
            .refs
            .iter()
            .any(|(name, state)| name == FIRST && state.oid.is_none())
    );
    pair(repository, 5, None, Some(b)).await?;
    assert!(matches!(
        repository
            .refs_page(&cursor, Some(deleted.generation))
            .await,
        Err(RefReadError::Changed)
    ));
    let generation = repository.refs_page("", None).await?.output.generation;
    assert!(
        repository
            .finalize_push(support::identity()?, pair_plan(5, Some(b), Some(a)))
            .await
            .is_err()
    );
    assert_eq!(
        repository.refs_page("", None).await?.output.generation,
        generation
    );
    let identity = support::identity()?;
    let plan = pair_plan(6, Some(b), Some(a));
    repository.finalize_push(identity, plan.clone()).await?;
    repository.finalize_push(identity, plan).await?;
    assert_eq!(
        repository.refs_page("", None).await?.output.generation,
        generation + 1
    );

    // Both ends of the ordered scan move in one transaction while real HTTP
    // advertisements repeatedly acquire, hydrate and replace cache generations.
    let writer = async {
        let mut old = a;
        for version in 7..39 {
            let new = if old == a { b } else { a };
            pair(repository, version, Some(old), Some(new)).await?;
            old = new;
        }
        Ok::<_, Box<dyn std::error::Error>>(())
    };
    let reader = async {
        let mut successes = 0;
        let mut busy = 0;
        for _ in 0..32 {
            let response = client
                .get(format!("{url}/info/refs?service=git-upload-pack"))
                .bearer_auth("local-test-token")
                .send()
                .await?;
            if response.status() == reqwest::StatusCode::SERVICE_UNAVAILABLE {
                assert_eq!(
                    response.text().await?,
                    "Repository refs are changing; retry the request"
                );
                busy += 1;
                continue;
            }
            let bytes = response.error_for_status()?.bytes().await?;
            let advertised = advertisement_refs(&bytes)?;
            assert_eq!(advertised[FIRST], advertised[LAST]);
            assert_eq!(
                advertised
                    .keys()
                    .filter(|name| name.starts_with("refs/tags/snapshot-"))
                    .count(),
                300
            );
            successes += 1;
        }
        assert!(successes > 0);
        eprintln!("snapshot advertisements: {successes} consistent, {busy} retriable");
        Ok::<_, Box<dyn std::error::Error>>(())
    };
    tokio::try_join!(writer, reader)?;
    let listing = run_git(
        None,
        &[
            "-c",
            "http.extraHeader=Authorization: Bearer local-test-token",
            "ls-remote",
            url,
            "refs/tags/snapshot-*",
        ],
    )
    .await?;
    assert_eq!(std::str::from_utf8(&listing)?.lines().count(), 300);

    // Git compresses sufficiently large fetch requests. The CGI contract must
    // preserve their encoding while retaining the encoded bytes in the spool.
    use std::io::Write;
    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    encoder.write_all(b"0014command=ls-refs\n00010009peel\n000csymrefs\n0000")?;
    let body = encoder.finish()?;
    for encoding in ["gzip", "x-gzip", "GZIP"] {
        let response = client
            .post(format!("{url}/git-upload-pack"))
            .bearer_auth("local-test-token")
            .header("Content-Type", "application/x-git-upload-pack-request")
            .header("Content-Encoding", encoding)
            .header("Git-Protocol", "version=2")
            .body(body.clone())
            .send()
            .await?
            .error_for_status()?
            .bytes()
            .await?;
        assert_eq!(
            advertisement_refs(&response)?
                .keys()
                .filter(|name| name.starts_with("refs/tags/snapshot-"))
                .count(),
            300
        );
    }
    for encoding in ["br", "gzip, identity"] {
        let response = client
            .post(format!("{url}/git-upload-pack"))
            .bearer_auth("local-test-token")
            .header("Content-Type", "application/x-git-upload-pack-request")
            .header("Content-Encoding", encoding)
            .body(body.clone())
            .send()
            .await?;
        assert_eq!(
            response.status(),
            reqwest::StatusCode::UNSUPPORTED_MEDIA_TYPE
        );
    }
    Ok(())
}

fn advertisement_refs(
    mut bytes: &[u8],
) -> Result<BTreeMap<String, String>, Box<dyn std::error::Error>> {
    let mut refs = BTreeMap::new();
    while !bytes.is_empty() {
        let header = bytes.get(..4).ok_or("short packet header")?;
        let length = usize::from_str_radix(std::str::from_utf8(header)?, 16)?;
        if length == 0 {
            bytes = &bytes[4..];
            continue;
        }
        let packet = std::str::from_utf8(bytes.get(4..length).ok_or("short packet")?)?;
        bytes = &bytes[length..];
        if let Some((oid, name)) = packet.split_once(' ')
            && oid.len() == 40
        {
            let name = name
                .split('\0')
                .next()
                .ok_or("missing ref name")?
                .trim_end();
            refs.insert(name.into(), oid.into());
        }
    }
    Ok(refs)
}
