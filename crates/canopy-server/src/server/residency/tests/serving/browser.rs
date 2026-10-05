//! Actual HTTP reads against native, physically verified packs. Only the joint
//! catalog fact and editorial pull records are installed by trusted test SQL;
//! this does not qualify the still-unconverted live pull/ref producers.
use super::*;
mod checks;
use crate::packs::{
    catalog::{
        CatalogSnapshot, StoredCatalog,
        serving_fixture::{BrowseFixture, operation, prepare},
    },
    directory::snapshot::DirectorySnapshot,
    ref_state::RefStateSnapshotRoot,
};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use canopy_object_storage::artifact::ArtifactStore;
use cellule_runtime::codec::{BoundedEncoder, WireValue};
use serde_json::{Value, json};

async fn install(
    repository: &RepositoryCell,
    generation: i64,
    catalog: StoredCatalog,
    refs: RefStateSnapshotRoot,
) -> Result {
    let mut e = BoundedEncoder::new(256)?;
    catalog.encode(&mut e)?;
    let catalog = e.finish();
    let mut e = BoundedEncoder::new(128)?;
    refs.encode(&mut e)?;
    repository.sql.batch(crate::server::mutation_identity()?, SqlBatch {statements: vec![
        SqlStatement {sql:"INSERT INTO catalog_generations(generation,catalog,certificate,refs) VALUES(?1,?2,?3,?4)".into(),parameters:vec![SqlValue::Integer(generation),SqlValue::Blob(catalog),SqlValue::Blob(vec![42;32]),SqlValue::Blob(e.finish())]},
        SqlStatement {sql:"UPDATE catalog_state SET generation=?1 WHERE singleton=1".into(),parameters:vec![SqlValue::Integer(generation)]},
    ]}).await?;
    Ok(())
}
async fn request(
    server: &crate::server::RunningServer,
    suffix: &str,
    body: Value,
) -> Result<reqwest::Response> {
    Ok(reqwest::Client::new()
        .post(format!(
            "http://{}/api/repositories/native-browser/{suffix}",
            server.address
        ))
        .bearer_auth("local-recovery-test")
        .json(&body)
        .send()
        .await?)
}
async fn browse(server: &crate::server::RunningServer, id: &str, query: Value) -> Result<Value> {
    let response = request(server, "browse", json!({"repository_id":id,"query":query})).await?;
    let status = response.status();
    let body = response.text().await?;
    assert_eq!(status, reqwest::StatusCode::OK, "{body}");
    Ok(serde_json::from_str::<Value>(&body)?["view"].clone())
}
fn tree(commit: ObjectId, path: &[u8], after: Option<Value>) -> Value {
    json!({"kind":"tree","commit":hex::encode(commit),"path_base64":URL_SAFE_NO_PAD.encode(path),"after":after})
}
fn file(commit: ObjectId, path: &[u8]) -> Value {
    json!({"kind":"file","commit":hex::encode(commit),"path_base64":URL_SAFE_NO_PAD.encode(path)})
}
async fn fixture(
    server: &crate::server::RunningServer,
    format: ObjectFormat,
) -> Result<(Arc<RepositoryCell>, BrowseFixture, String)> {
    let entry = create(&server.repositories, "native-browser", format).await?;
    let (repository, _, _) = loaded(&server.repositories, entry.repository_id).await?;
    let native = prepare(
        format,
        server.repositories.external_store.clone(),
        entry.repository_id,
        40,
    )
    .await?;
    install(&repository, 2, native.catalog, native.refs).await?;
    Ok((
        repository,
        native,
        uuid::Uuid::from_bytes(entry.repository_id).to_string(),
    ))
}

#[tokio::test]
async fn production_native_browser_preserves_raw_paths_modes_pages_tags_and_ordered_history()
-> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let (server, _files) = server().await?;
        let (repository, native, id) = fixture(&server, format).await?;
        let first = browse(&server, &id, tree(native.tag, b"", None)).await?;
        assert_eq!(first["tree"]["commit"]["oid"], hex::encode(native.main));
        assert_eq!(first["tree"]["tree_oid"], hex::encode(native.tree));
        let mut entries = first["tree"]["entries"]
            .as_array()
            .ok_or("entries")?
            .clone();
        assert_eq!(entries.len(), 32);
        let second = browse(
            &server,
            &id,
            tree(native.main, b"", Some(first["tree"]["next_after"].clone())),
        )
        .await?;
        assert!(second["tree"]["next_after"].is_null());
        entries.extend(
            second["tree"]["entries"]
                .as_array()
                .ok_or("entries")?
                .clone(),
        );
        assert_eq!(entries.len(), 49);
        let paths = entries
            .iter()
            .map(|e| -> Result<Vec<u8>> {
                Ok(URL_SAFE_NO_PAD.decode(e["path_base64"].as_str().ok_or("path")?)?)
            })
            .collect::<Result<Vec<_>>>()?;
        assert!(paths.windows(2).all(|p| p[0] < p[1]));
        assert!(paths.contains(&b"\xffname".to_vec()));
        let raw = entries
            .iter()
            .find(|e| e["path_base64"] == URL_SAFE_NO_PAD.encode(b"\xffname"))
            .ok_or("raw entry")?;
        assert!(raw["name"].is_null());
        for (path, mode, body) in [
            (b"link".as_slice(), "120000", b"src/lib.rs".as_slice()),
            (b"executable", "100755", b"#!/bin/sh\nexit 0\n"),
            (b"\xffname", "100644", b"raw name\n"),
            (b"literal[?]*", "100644", b"literal path\n"),
            (b"binary", "100644", b"a\0b\xff"),
            (b"src/lib.rs", "100644", b"pub fn original() {}\n"),
        ] {
            let output = browse(&server, &id, file(native.main, path)).await?;
            assert_eq!(output["file"]["mode"], mode);
            assert_eq!(output["file"]["size"], body.len());
            assert_eq!(
                output["file"]["content_base64"],
                URL_SAFE_NO_PAD.encode(body)
            );
        }
        let directory = browse(&server, &id, tree(native.main, b"src", None)).await?;
        assert_eq!(
            directory["tree"]["entries"].as_array().ok_or("src")?.len(),
            1
        );
        let large = browse(&server, &id, file(native.main, b"large")).await?;
        assert_eq!(large["file"]["content_status"], "too_large");
        assert_eq!(large["file"]["size"], native.large.len());
        assert!(large["file"]["content_base64"].is_null());
        let link = browse(&server, &id, file(native.main, b"submodule")).await?;
        assert_eq!(link["file"]["content_status"], "gitlink");
        assert!(link["file"]["size"].is_null());
        let first = browse(
            &server,
            &id,
            json!({"kind":"history","commit":hex::encode(native.tag)}),
        )
        .await?;
        let first = &first["history"];
        assert_eq!(first["commits"].as_array().ok_or("history")?.len(), 32);
        assert_eq!(
            first["commits"][0]["parents"],
            json!([hex::encode(native.previous), hex::encode(native.side)])
        );
        let second = browse(
            &server,
            &id,
            json!({"kind":"history","commit":first["next_commit"]}),
        )
        .await?;
        assert_eq!(
            second["history"]["commits"]
                .as_array()
                .ok_or("continued history")?
                .len(),
            9
        );
        assert!(second["history"]["next_commit"].is_null());
        let actual: Vec<_> = first["commits"]
            .as_array()
            .ok_or("first")?
            .iter()
            .chain(second["history"]["commits"].as_array().ok_or("second")?)
            .map(|c| c["oid"].as_str().unwrap_or_default().to_owned())
            .collect();
        assert_eq!(actual, native.history);
        // These tables do not exist: success cannot come from a fallback.
        let tables=repository.sql.query(None,SqlBatch{statements:vec![SqlStatement{sql:"SELECT name FROM sqlite_schema WHERE name IN ('objects','object_closure','object_edges','commit_parents')".into(),parameters:vec![]}]}).await?;
        assert!(tables.output[0].rows.is_empty());
        drop(repository);
        timeout(Duration::from_secs(15), server.shutdown()).await??;
    }
    Ok(())
}

#[tokio::test]
async fn production_native_browser_refuses_absent_generations_wrong_formats_and_revoked_cached_access()
-> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let (server, _files) = server().await?;
        let (repository, native, id) = fixture(&server, format).await?;
        let old = repository
            .serving_snapshot(ReadIdentity::Account("canopy"))
            .await?;
        assert!(old.body(native.main, 1 << 20).await?.is_some());
        let _ = browse(&server, &id, tree(native.main, b"", None)).await?;
        for (revision, status) in [
            (missing(format), reqwest::StatusCode::NOT_FOUND),
            (
                match format {
                    ObjectFormat::Sha1 => ObjectId::Sha1([0; 20]),
                    ObjectFormat::Sha256 => ObjectId::Sha256([0; 32]),
                },
                reqwest::StatusCode::NOT_FOUND,
            ),
            (
                match format {
                    ObjectFormat::Sha1 => ObjectId::Sha256([7; 32]),
                    ObjectFormat::Sha256 => ObjectId::Sha1([7; 20]),
                },
                reqwest::StatusCode::UNPROCESSABLE_ENTITY,
            ),
        ] {
            assert_eq!(
                request(
                    &server,
                    "browse",
                    json!({"repository_id":id,"query":tree(revision,b"",None)})
                )
                .await?
                .status(),
                status
            );
        }
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
        assert_eq!(
            request(
                &server,
                "browse",
                json!({"repository_id":id,"query":file(native.main,b"file-0000")})
            )
            .await?
            .status(),
            reqwest::StatusCode::NOT_FOUND
        );
        assert!(old.body(native.main, 1 << 20).await?.is_some());
        drop(old);
        // The cached pack must also obey current public/private authorization.
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
        let public = repository.serving_snapshot(ReadIdentity::Anonymous).await?;
        assert!(public.body(native.main, 1 << 20).await?.is_none());
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
        assert!(public.body(native.main, 1 << 20).await.is_err());
        let anonymous = reqwest::Client::new()
            .post(format!(
                "http://{}/api/repositories/native-browser/browse",
                server.address
            ))
            .json(&json!({"repository_id":id,"query":tree(native.main,b"",None)}))
            .send()
            .await?;
        assert_ne!(anonymous.status(), reqwest::StatusCode::OK);
        drop((public, repository));
        timeout(Duration::from_secs(15), server.shutdown()).await??;
    }
    Ok(())
}

async fn editorial_pull(
    repository: &RepositoryCell,
    number: i64,
    source: ObjectId,
    base: ObjectId,
) -> Result<Value> {
    // Only editorial metadata remains on legacy refs. The compared bodies and
    // ancestry must come from the certified catalog, never objects/parents SQL.
    let source_ref = format!("refs/heads/source-{number}");
    let base_ref = format!("refs/heads/base-{number}");
    repository.sql.batch(crate::server::mutation_identity()?,SqlBatch{statements:vec![
        SqlStatement{sql:"INSERT INTO refs(name,oid,version) VALUES(?1,?2,1),(?3,?4,1)".into(),parameters:vec![SqlValue::Text(source_ref.clone()),SqlValue::Blob(source.to_vec()),SqlValue::Text(base_ref.clone()),SqlValue::Blob(base.to_vec())]},
        SqlStatement{sql:"INSERT INTO pull_requests(number,id,creation_digest,author,title,body,state,draft,version,source_ref,base_ref,initial_source_oid,initial_base_oid,created_ms,updated_ms) VALUES(?1,?2,?3,'canopy','native comparison','','open',0,1,?4,?5,?6,?7,0,0)".into(),parameters:vec![SqlValue::Integer(number),SqlValue::Blob(uuid::Uuid::new_v4().into_bytes().to_vec()),SqlValue::Blob(vec![42;32]),SqlValue::Text(source_ref),SqlValue::Text(base_ref),SqlValue::Blob(source.to_vec()),SqlValue::Blob(base.to_vec())]},
    ]}).await?;
    Ok(
        json!({"kind":"current","revision":{"pull_version":1,"source_oid":hex::encode(source),"source_version":1,"base_oid":hex::encode(base),"base_version":1}}),
    )
}
#[tokio::test]
async fn production_native_comparisons_read_certified_ancestry_patches_and_previews() -> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let (server, _files) = server().await?;
        let (repository, native, id) = fixture(&server, format).await?;
        for (number, source, base, expected) in [
            (1, native.previous, native.side, native.root),
            (2, native.main, native.side, native.side),
            (3, native.main, native.main, native.main),
        ] {
            let target = editorial_pull(&repository, number, source, base).await?;
            let response = request(
                &server,
                &format!("pulls/{number}/comparison"),
                json!({"repository_id":id,"target":target,"query":{"kind":"files"}}),
            )
            .await?;
            let status = response.status();
            let body = response.text().await?;
            assert_eq!(status, reqwest::StatusCode::OK, "{body}");
            let body: Value = serde_json::from_str(&body)?;
            assert_eq!(body["comparison"]["merge_base"], hex::encode(expected));
            if number == 3 {
                assert_eq!(body["comparison"]["files"], json!([]));
                continue;
            }
            assert_eq!(
                body["comparison"]["files"]
                    .as_array()
                    .ok_or("changes")?
                    .len(),
                1
            );
            assert_eq!(body["comparison"]["files"][0]["path"], "file-0000");
            for query in [
                json!({"kind":"patch","path_base64":URL_SAFE_NO_PAD.encode(b"file-0000")}),
                json!({"kind":"file","path_base64":URL_SAFE_NO_PAD.encode(b"file-0000"),"side":"after"}),
            ] {
                let response = request(
                    &server,
                    &format!("pulls/{number}/comparison"),
                    json!({"repository_id":id,"target":target,"query":query}),
                )
                .await?;
                let status = response.status();
                let body = response.text().await?;
                assert_eq!(status, reqwest::StatusCode::OK, "{body}");
                let body: Value = serde_json::from_str(&body)?;
                if query["kind"] == "patch" {
                    assert_eq!(body["patch"]["status"], "text");
                    assert_eq!(body["patch"]["merge_base"], hex::encode(expected));
                    assert_eq!(body["patch"]["hunks"][0]["lines"][0]["kind"], "delete");
                    assert_eq!(body["patch"]["hunks"][0]["lines"][1]["text"], "version 40");
                } else {
                    assert_eq!(
                        body["file"]["content_base64"],
                        URL_SAFE_NO_PAD.encode(b"version 40\n")
                    );
                }
            }
        }
        // Equal non-commit tips must be refused even when merge_base can take
        // its equal-input fast path. A catalog membership lookup is mandatory.
        let blob = native.edges[&native.tree]
            .iter()
            .find(|edge| edge.expected_kind == crate::ObjectKind::Blob)
            .ok_or("blob edge")?
            .child;
        let target = editorial_pull(&repository, 4, blob, blob).await?;
        assert_eq!(
            request(
                &server,
                "pulls/4/comparison",
                json!({"repository_id":id,"target":target,"query":{"kind":"files"}})
            )
            .await?
            .status(),
            reqwest::StatusCode::SERVICE_UNAVAILABLE
        );
        let target = editorial_pull(&repository, 5, missing(format), missing(format)).await?;
        assert_eq!(
            request(
                &server,
                "pulls/5/comparison",
                json!({"repository_id":id,"target":target,"query":{"kind":"files"}})
            )
            .await?
            .status(),
            reqwest::StatusCode::NOT_FOUND
        );
        drop(repository);
        timeout(Duration::from_secs(15), server.shutdown()).await??;
    }
    Ok(())
}

#[tokio::test]
async fn production_certified_edge_pages_cover_wide_trees_and_parent_boundaries() -> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let (server, _files) = server().await?;
        let entry = create(&server.repositories, "native-browser", format).await?;
        let (repository, _, _) = loaded(&server.repositories, entry.repository_id).await?;
        let native = prepare(
            format,
            server.repositories.external_store.clone(),
            entry.repository_id,
            600,
        )
        .await?;
        install(&repository, 2, native.catalog, native.refs).await?;
        let snapshot = repository
            .serving_snapshot(ReadIdentity::Account("canopy"))
            .await?;
        let root_tree = native.edges[&native.root]
            .iter()
            .find(|edge| edge.expected_kind == crate::ObjectKind::Tree)
            .ok_or("root tree")?
            .child;
        let wide = native.wide.ok_or("wide merge")?;
        let mut ids = vec![
            native.main,
            native.side,
            wide,
            native.tree,
            root_tree,
            missing(format),
        ];
        ids.sort_unstable();
        let mut expected = Vec::new();
        for id in &ids {
            if let Some(edges) = native.edges.get(id) {
                let mut edges = edges.clone();
                edges.sort_by_key(|edge| edge.child);
                edges.dedup_by_key(|edge| edge.child);
                expected.extend(edges.into_iter().map(|edge| (*id, edge)));
            }
        }
        assert!(expected.len() > 1024);
        let mut cursor = None;
        let mut actual = Vec::new();
        let mut absent = false;
        let mut pages = 0;
        loop {
            let page = snapshot.edges_page(&ids, cursor).await?;
            assert!(page.edges.len() <= 512);
            assert!(page.headers.len() <= ids.len());
            absent |= page
                .headers
                .iter()
                .any(|(id, header)| *id == missing(format) && header.is_none());
            actual.extend(page.edges);
            pages += 1;
            let Some(next) = page.next_after else { break };
            assert!(cursor.is_none_or(|old| old < next));
            cursor = Some(next);
        }
        assert!(pages >= 3);
        assert!(absent);
        assert_eq!(actual, expected);
        let context = |error| {
            matches!(
                error,
                Err(crate::packs::publication::ServingReadError::Context)
            )
        };
        assert!(context(snapshot.edges_page(&[], None).await));
        assert!(context(
            snapshot.edges_page(&[native.main; 129], None).await
        ));
        assert!(context(
            snapshot.edges_page(&[native.main, native.main], None).await
        ));
        assert!(context(
            snapshot
                .edges_page(
                    &ids,
                    Some((
                        match format {
                            ObjectFormat::Sha1 => ObjectId::Sha1([0; 20]),
                            ObjectFormat::Sha256 => ObjectId::Sha256([0; 32]),
                        },
                        native.main
                    ))
                )
                .await
        ));
        assert!(context(
            snapshot
                .edges_page(
                    &ids,
                    Some((
                        native.main,
                        match format {
                            ObjectFormat::Sha1 => ObjectId::Sha1([0; 20]),
                            ObjectFormat::Sha256 => ObjectId::Sha256([0; 32]),
                        }
                    ))
                )
                .await
        ));
        let wrong = match format {
            ObjectFormat::Sha1 => ObjectId::Sha256([9; 32]),
            ObjectFormat::Sha256 => ObjectId::Sha1([9; 20]),
        };
        assert!(context(snapshot.edges_page(&[wrong], None).await));
        assert!(context(
            snapshot.edges_page(&ids, Some((native.main, wrong))).await
        ));
        let mut parents = native.edges[&wide].clone();
        parents.sort_by_key(|edge| edge.child);
        let (ordinal, last) = parents
            .iter()
            .enumerate()
            .rev()
            .find(|(_, edge)| {
                edge.expected_kind == crate::ObjectKind::Commit && edge.child != native.root
            })
            .ok_or("last parent")?;
        assert!(ordinal >= 512);
        let target = editorial_pull(&repository, 1, wide, last.child).await?;
        let response = request(&server,"pulls/1/comparison",json!({"repository_id":uuid::Uuid::from_bytes(entry.repository_id).to_string(),"target":target,"query":{"kind":"files"}})).await?;
        let status = response.status();
        let body = response.text().await?;
        assert_eq!(status, reqwest::StatusCode::OK, "{body}");
        let body: Value = serde_json::from_str(&body)?;
        assert_eq!(body["comparison"]["merge_base"], hex::encode(last.child));
        drop((snapshot, repository));
        timeout(Duration::from_secs(15), server.shutdown()).await??;
    }
    Ok(())
}

#[tokio::test]
async fn certified_http_clone_and_discovery_use_joint_refs_without_legacy_objects() -> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let (server, _files) = server().await?;
        let (repository, native, _) = fixture(&server, format).await?;
        let work = tempfile::TempDir::new()?;
        let url = format!("http://{}/canopy/native-browser.git", server.address);
        let args = [
            "-c",
            "http.extraHeader=Authorization: Bearer local-recovery-test",
            "clone",
            "--bare",
            &url,
            "clone.git",
        ];
        crate::packs::catalog::serving_fixture::run_git(work.path(), &args, None)
            .await
            .map_err(|e| e.to_string())?;
        let path = work.path().join("clone.git");
        let git = |args: Vec<String>| {
            let path = path.clone();
            async move {
                let refs: Vec<_> = args.iter().map(String::as_str).collect();
                crate::packs::catalog::serving_fixture::run_git(&path, &refs, None)
                    .await
                    .map_err(|e| e.to_string())
            }
        };
        assert_eq!(
            String::from_utf8(git(vec!["rev-parse".into(), "HEAD".into()]).await?)?.trim(),
            hex::encode(native.main)
        );
        assert_eq!(
            String::from_utf8(
                git(vec!["rev-list".into(), "--count".into(), "HEAD".into()]).await?
            )?
            .trim(),
            "42"
        );
        assert_eq!(
            git(vec!["show".into(), "HEAD:src/lib.rs".into()]).await?,
            b"pub fn original() {}\n"
        );
        git(vec!["fsck".into(), "--full".into()]).await?;
        // Guessing a physically present, certified but unreferenced tag must be
        // rejected by the HTTP gateway before native upload-pack sees the want.
        let line = format!("want {}\n", hex::encode(native.tag));
        let body = format!("{:04x}{line}00000009done\n", line.len() + 4);
        let refused = reqwest::Client::new()
            .post(format!("{url}/git-upload-pack"))
            .bearer_auth("local-recovery-test")
            .header("Content-Type", "application/x-git-upload-pack-request")
            .body(body)
            .send()
            .await?;
        assert_eq!(
            refused.status(),
            reqwest::StatusCode::BAD_REQUEST,
            "{}",
            refused.text().await?
        );
        drop(repository);
        server.shutdown().await?;
    }
    Ok(())
}
