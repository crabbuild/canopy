use super::*;
use canopy_server::{ObjectBatch, ObjectStorage, StoredObject};

pub async fn verify(repository: &RepositoryCell) -> Result<(), Box<dyn std::error::Error>> {
    let mut updates = Vec::new();
    for start in (0..1001).step_by(128) {
        let mut objects = ObjectBatch::default();
        for index in start..(start + 128).min(1001) {
            let body = format!("bulk ref object {index}").into_bytes();
            let oid = object_id(canopy_server::ObjectFormat::Sha1, ObjectKind::Blob, &body);
            objects
                .try_push(StoredObject {
                    oid,
                    kind: ObjectKind::Blob,
                    storage: ObjectStorage::Inline(body),
                })
                .map_err(|_| "fixture exceeds object batch")?;
            updates.push(RefUpdate {
                name: format!("refs/tags/bulk/{index:04}"),
                expected: None,
                new_oid: Some(oid),
            });
        }
        repository
            .put_objects(super::graph::identity()?, objects)
            .await?;
    }
    let generation = repository.default_branch(None).await?.output.generation;
    repository
        .finalize_push(
            super::graph::identity()?,
            PushPlan {
                actor: "canopy".into(),
                updates: updates.clone(),
            },
        )
        .await?;
    assert_eq!(
        repository.default_branch(None).await?.output.generation,
        generation + 1
    );

    let oid = updates[0].new_oid;
    let mut replacement: Vec<_> = updates
        .into_iter()
        .map(|update| RefUpdate {
            name: update.name,
            expected: Some(RefExpectation {
                oid: update.new_oid,
                version: 1,
            }),
            new_oid: None,
        })
        .collect();
    let last = replacement.pop().ok_or("missing final descendant")?;
    replacement.push(RefUpdate {
        name: "refs/tags/bulk".into(),
        expected: None,
        new_oid: oid,
    });
    // The remaining descendant is beyond several SQL pages. Reject the whole
    // replacement without deleting any earlier siblings or advancing generation.
    assert!(matches!(
        repository
            .finalize_push(
                super::graph::identity()?,
                PushPlan {
                    actor: "canopy".into(),
                    updates: replacement.clone()
                }
            )
            .await,
        Err(InvocationError::Rejected(_))
    ));
    assert_eq!(
        repository
            .ref_state("refs/tags/bulk/0000", None)
            .await?
            .output
            .map(|state| state.oid),
        Some(oid)
    );
    assert_eq!(
        repository.default_branch(None).await?.output.generation,
        generation + 1
    );

    replacement.push(last.clone());
    repository
        .finalize_push(
            super::graph::identity()?,
            PushPlan {
                actor: "canopy".into(),
                updates: replacement,
            },
        )
        .await?;
    assert_eq!(
        repository
            .ref_state("refs/tags/bulk", None)
            .await?
            .output
            .map(|state| state.oid),
        Some(oid)
    );
    assert_eq!(
        repository.ref_state(&last.name, None).await?.output,
        Some(RefExpectation {
            oid: None,
            version: 2
        })
    );
    assert_eq!(
        repository.default_branch(None).await?.output.generation,
        generation + 2
    );
    let long_prefix = format!(
        "refs/tags/long-page/{}",
        vec!["r".repeat(200); 21].join("/")
    );
    let long_names: Vec<_> = (0..270)
        .map(|index| format!("{long_prefix}/{index:04}"))
        .collect();
    assert!(long_names[0].len() * 256 > 1 << 20);
    for chunk in long_names.chunks(16) {
        repository
            .finalize_push(
                super::graph::identity()?,
                PushPlan {
                    actor: "canopy".into(),
                    updates: chunk
                        .iter()
                        .map(|name| RefUpdate {
                            name: name.clone(),
                            expected: None,
                            new_oid: oid,
                        })
                        .collect(),
                },
            )
            .await?;
    }
    assert!(matches!(
        repository
            .finalize_push(
                super::graph::identity()?,
                PushPlan {
                    actor: "canopy".into(),
                    updates: vec![RefUpdate {
                        name: "refs/tags/long-page".into(),
                        expected: None,
                        new_oid: oid,
                    }],
                },
            )
            .await,
        Err(InvocationError::Rejected(_))
    ));
    let mut after = String::new();
    let mut page_generation = None;
    let mut observed = Vec::new();
    let mut byte_bounded = false;
    loop {
        let page = repository.refs_page(&after, page_generation).await?.output;
        page_generation = Some(page.generation);
        byte_bounded |= page.has_more && page.refs.len() < 256;
        if let Some((name, _)) = page.refs.last() {
            after.clone_from(name);
        }
        observed.extend(
            page.refs
                .into_iter()
                .filter(|(name, _)| name.starts_with("refs/tags/long-page/"))
                .map(|(name, _)| name),
        );
        if !page.has_more {
            break;
        }
    }
    assert!(byte_bounded);
    assert_eq!(observed, long_names);
    Ok(())
}
