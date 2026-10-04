//! Physically verified native inputs for serving tests. Catalog installation in
//! the caller is trusted injection, not qualification of the live publisher.
use super::*;
use crate::packs::{
    directory::DirectoryBuilder,
    metadata::{
        PAGE_OBJECTS,
        tests::{fixture_with_input, git, limits},
    },
    ref_state::{RefStateRecord, RefStateSnapshot, RefStateSnapshotRoot, RefStateTree},
    sources::{SourceIndex, SourceRecord},
    verification::{
        PhysicalVerifier,
        physical::tests::{physical_limits, upload_fixture},
    },
};
use crate::{ObjectId, RefExpectation};
use cellule_ltx::DiskBudget;
use object_store::ObjectStore;

type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
pub(crate) struct BrowseFixture {
    pub catalog: StoredCatalog,
    pub refs: RefStateSnapshotRoot,
    pub main: ObjectId,
    pub side: ObjectId,
    pub root: ObjectId,
    pub previous: ObjectId,
    pub tag: ObjectId,
    pub tree: ObjectId,
    pub wide: Option<ObjectId>,
    pub history: Vec<String>,
    pub edges: std::collections::BTreeMap<ObjectId, Vec<crate::packs::metadata::TypedEdge>>,
    pub large: Vec<u8>,
}
pub(crate) fn operation(n: u64) -> [u8; 16] {
    let mut id = *b"CANOPY0100000000";
    id[8..].copy_from_slice(&n.to_be_bytes());
    id
}
fn file(input: &mut Vec<u8>, mode: &str, name: &str, body: &[u8]) {
    input.extend_from_slice(format!("M {mode} inline {name}\ndata {}\n", body.len()).as_bytes());
    input.extend_from_slice(body);
    input.push(b'\n');
}
fn commit(input: &mut Vec<u8>, branch: &str, mark: u32, parents: &[u32]) {
    input.extend_from_slice(format!("commit refs/heads/{branch}\nmark :{mark}\ncommitter Browse Test <browse@example.invalid> {mark} +0000\ndata 7\nfixture\n").as_bytes());
    if let Some(first) = parents.first() {
        input.extend_from_slice(format!("from :{first}\n").as_bytes());
    }
    for parent in parents.iter().skip(1) {
        input.extend_from_slice(format!("merge :{parent}\n").as_bytes());
    }
}
pub(crate) async fn prepare(
    format: ObjectFormat,
    provider: Arc<dyn ObjectStore>,
    repository: [u8; 16],
    regular_files: usize,
) -> Result<BrowseFixture> {
    let mut input = Vec::new();
    commit(&mut input, "main", 1, &[]);
    for n in 0..regular_files {
        file(
            &mut input,
            "100644",
            &format!("file-{n:04}"),
            format!("original {n}\n").as_bytes(),
        );
    }
    file(
        &mut input,
        "100644",
        "src/lib.rs",
        b"pub fn original() {}\n",
    );
    file(&mut input, "120000", "link", b"src/lib.rs");
    file(&mut input, "100755", "executable", b"#!/bin/sh\nexit 0\n");
    file(&mut input, "100644", "\"\\377name\"", b"raw name\n");
    file(&mut input, "100644", "\"literal[?]*\"", b"literal path\n");
    file(&mut input, "100644", "binary", b"a\0b\xff");
    let large = vec![b'x'; 256 * 1024 + 1];
    file(&mut input, "100644", "large", &large);
    input.extend_from_slice(
        format!("M 160000 {} submodule\n\n", "8".repeat(format.bytes() * 2)).as_bytes(),
    );
    for mark in 2..=40 {
        commit(&mut input, "main", mark, &[mark - 1]);
        file(
            &mut input,
            "100644",
            "file-0000",
            format!("version {mark}\n").as_bytes(),
        );
        input.push(b'\n');
    }
    commit(&mut input, "side", 41, &[1]);
    file(&mut input, "100644", "side-file", b"side\n");
    input.push(b'\n');
    commit(&mut input, "main", 42, &[40, 41]);
    file(&mut input, "100644", "side-file", b"side\n");
    input.push(b'\n');
    if regular_files >= 600 {
        // An actual 532-parent Git merge exercises ancestry continuation beyond
        // one 512-edge page. Empty auxiliary commits share the root content.
        for mark in 50..580 {
            commit(&mut input, "aux", mark, &[1]);
            input.push(b'\n');
        }
        let mut parents = vec![1, 41];
        parents.extend(50..580);
        commit(&mut input, "wide", 600, &parents);
        input.push(b'\n');
    }
    let fixture = fixture_with_input(format, input)
        .await
        .map_err(|e| e.to_string())?;
    async fn oid(path: &std::path::Path, name: &str) -> Result<ObjectId> {
        let bytes = git(path, &["rev-parse", name], None)
            .await
            .map_err(|e| e.to_string())?;
        Ok(ObjectId::from_hex(std::str::from_utf8(&bytes)?.trim())?)
    }
    let main = oid(fixture.root.path(), "main").await?;
    let side = oid(fixture.root.path(), "side").await?;
    let root = oid(fixture.root.path(), "main~40").await?;
    let previous = oid(fixture.root.path(), "main~1").await?;
    let tag = oid(fixture.root.path(), "metadata").await?;
    let tree = oid(fixture.root.path(), "main^{tree}").await?;
    let wide = if regular_files >= 600 {
        Some(oid(fixture.root.path(), "wide").await?)
    } else {
        None
    };
    let history = String::from_utf8(
        git(
            fixture.root.path(),
            &["rev-list", "--first-parent", "main"],
            None,
        )
        .await
        .map_err(|e| e.to_string())?,
    )?
    .lines()
    .map(str::to_owned)
    .collect();
    let edges = fixture
        .objects
        .iter()
        .map(|(oid, (_, edges))| (*oid, edges.clone()))
        .collect();
    let store = Arc::new(ArtifactStore::new(provider.clone(), repository));
    let native = upload_fixture(fixture, operation(100), provider, store.clone())
        .await
        .map_err(|e| e.to_string())?;
    let work = tempfile::TempDir::new()?;
    let budget = DiskBudget::new(128 << 20);
    let mut verifier = PhysicalVerifier::download(
        work.path(),
        budget.clone(),
        &store,
        native.descriptor,
        physical_limits(),
        crate::native_resources::NativeResources::default()
            .scope(crate::native_resources::NativeClass::Foreground),
    )
    .await?;
    let mut segments = Vec::new();
    let mut remaining = native.descriptor.object_count;
    while remaining != 0 {
        let count = remaining.min(PAGE_OBJECTS as u32);
        segments.push(verifier.inspect_next_shard(count).await?);
        remaining -= count;
    }
    verifier
        .finish()
        .await?
        .verify_segments(segments.iter().map(|s| s.descriptor()))?;
    let mut directory = DirectoryBuilder::new(
        work.path(),
        budget.clone(),
        repository,
        operation(101),
        format,
        limits(),
    )?;
    let index = SourceIndex::new(store.clone(), format);
    let mut sources = None;
    for segment in &segments {
        directory.add_segment(segment)?;
        sources = Some(
            index
                .insert(
                    sources,
                    operation(102),
                    SourceRecord {
                        metadata: segment.clone().upload(&store).await?,
                        pack: native.descriptor.pack,
                        index: native.descriptor.index,
                        pack_object_count: native.descriptor.object_count,
                    },
                )
                .await?,
        );
    }
    let run = Arc::new(directory.seal()?).upload(&store).await?;
    let ranges = RangeIndex::new(store.clone(), format);
    let run_root = ranges.insert(None, operation(103), run).await?;
    let mut directory = DirectorySnapshot::empty(repository, format);
    directory.append(&ranges, run_root).await?;
    let catalog = CatalogSnapshot {
        directory: directory.upload(&store, operation(104)).await?,
        sources,
    }
    .upload(&store, operation(105))
    .await?;
    let refs = RefStateTree::new(store.clone(), format)
        .build_sorted(
            operation(106),
            [
                RefStateRecord::new(
                    "refs/heads/main",
                    RefExpectation {
                        oid: Some(main),
                        version: 1,
                    },
                    format,
                )?,
                RefStateRecord::new(
                    "refs/heads/side",
                    RefExpectation {
                        oid: Some(side),
                        version: 1,
                    },
                    format,
                )?,
            ]
            .into_iter()
            .map(Ok),
        )
        .await?;
    let refs = RefStateSnapshotRoot::upload(
        &store,
        operation(107),
        RefStateSnapshot {
            repository,
            format,
            generation: 1,
            default_branch: "refs/heads/main".into(),
            root: refs,
        },
    )
    .await?;
    drop(segments);
    tokio::time::timeout(std::time::Duration::from_secs(8), async {
        while budget.used() != 0 {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await?;
    Ok(BrowseFixture {
        catalog,
        refs,
        main,
        side,
        root,
        previous,
        tag,
        tree,
        wide,
        history,
        edges,
        large,
    })
}
