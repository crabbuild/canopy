use super::*;
use crate::{ObjectKind, git_format::ObjectHasher};
use std::io::Write;
use tokio::io::AsyncWriteExt;

// Independent native-format fixtures, checked through stock Git below. This
// creates a one-object REF_DELTA whose base is absent from the physical pack.
fn crc32(bytes: &[u8]) -> u32 {
    let mut crc = u32::MAX;
    for byte in bytes {
        crc ^= u32::from(*byte);
        for _ in 0..8 {
            crc = (crc >> 1) ^ (0xedb88320 & (0_u32.wrapping_sub(crc & 1)));
        }
    }
    !crc
}
fn thin_pack(format: ObjectFormat, base: ObjectId) -> Result<Vec<u8>> {
    let mut pack = b"PACK\0\0\0\x02\0\0\0\x01".to_vec();
    pack.push(0x78); // REF_DELTA, eight inflated delta-instruction bytes.
    pack.extend_from_slice(&base);
    let mut compressed =
        flate2::write::ZlibEncoder::new(Vec::new(), flate2::Compression::default());
    compressed.write_all(b"\x04\x05\x05base!")?;
    pack.extend_from_slice(&compressed.finish()?);
    let mut hash = ObjectHasher::raw(format);
    hash.update(&pack);
    pack.extend_from_slice(&hash.finalize());
    Ok(pack)
}
fn single_index(format: ObjectFormat, oid: ObjectId, pack: &[u8]) -> Result<Vec<u8>> {
    let width = format.bytes();
    let mut index = b"\xfftOc\0\0\0\x02".to_vec();
    for bucket in 0..256 {
        index.extend_from_slice(&u32::from(bucket >= usize::from(oid[0])).to_be_bytes());
    }
    index.extend_from_slice(&oid);
    index.extend_from_slice(&crc32(&pack[12..pack.len() - width]).to_be_bytes());
    index.extend_from_slice(&12_u32.to_be_bytes());
    index.extend_from_slice(&pack[pack.len() - width..]);
    let mut hash = ObjectHasher::raw(format);
    hash.update(&index);
    index.extend_from_slice(&hash.finalize());
    Ok(index)
}
pub(in crate::packs) async fn upload_pair(
    prepared: &Prepared,
    oid: ObjectId,
    pack: &[u8],
) -> Result<NativePackDescriptor> {
    upload_pair_for_operation(prepared, oid, pack, [9; 16]).await
}
pub(in crate::packs) async fn upload_pair_for_operation(
    prepared: &Prepared,
    oid: ObjectId,
    pack: &[u8],
    operation: [u8; 16],
) -> Result<NativePackDescriptor> {
    let format = oid.format();
    let pack_digest = *blake3::hash(pack).as_bytes();
    let key = |kind| ArtifactKey {
        operation,
        binding_digest: pack_digest,
        kind,
    };
    let index_bytes = single_index(format, oid, pack)?;
    let index = upload_bytes(&prepared.store, key(ArtifactKind::Index), &index_bytes).await?;
    let pack_artifact = upload_bytes(&prepared.store, key(ArtifactKind::Pack), pack).await?;
    Ok(NativePackDescriptor {
        repository: prepared.descriptor.repository,
        operation,
        format,
        git_checksum: ObjectId::try_from(&pack[pack.len() - format.bytes()..])?,
        object_count: 1,
        pack: pack_artifact,
        index,
    })
}
pub(in crate::packs) async fn git_input(
    root: &Path,
    args: &[&str],
    input: &[u8],
) -> Result<Vec<u8>> {
    let mut command = crate::native_git::command(root)?;
    let mut child = command
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()?;
    child.stdin.take().ok_or("stdin")?.write_all(input).await?;
    let output = child.wait_with_output().await?;
    if !output.status.success() {
        return Err(String::from_utf8_lossy(&output.stderr).into_owned().into());
    }
    Ok(output.stdout)
}

#[tokio::test]
async fn ambient_duplicate_cannot_hide_an_unresolved_delta_in_isolated_verification() -> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let prepared = prepared(format, 4).await?;
        let base = crate::object_id(format, ObjectKind::Blob, b"base");
        let target = crate::object_id(format, ObjectKind::Blob, b"base!");
        let written = git_input(
            prepared.fixture.root.path(),
            &["hash-object", "-w", "--stdin"],
            b"base",
        )
        .await?;
        assert_eq!(ObjectId::from_hex(written.trim_ascii())?, base);
        // A shared cache can answer the OID from a duplicate loose object even
        // when the physical pack cannot decode it independently.
        let written = git_input(
            prepared.fixture.root.path(),
            &["hash-object", "-w", "--stdin"],
            b"base!",
        )
        .await?;
        assert_eq!(ObjectId::from_hex(written.trim_ascii())?, target);

        let pack = thin_pack(format, base)?;
        let descriptor = upload_pair(&prepared, target, &pack).await?;
        let path = prepared.fixture.root.path().join(format!(
            "objects/pack/pack-{}.pack",
            hex::encode(descriptor.git_checksum)
        ));
        std::fs::write(&path, &pack)?;
        std::fs::write(
            path.with_extension("idx"),
            single_index(format, target, &pack)?,
        )?;
        // Exact bytes, index/native checksums and artifact bindings all pass.
        descriptor.verify_files(&path, &path.with_extension("idx"))?;
        let root = tempfile::TempDir::new()?;
        let budget = DiskBudget::new(128 << 20);
        let mut ambient = CanonicalVerifier::new(
            prepared.fixture.root.path(),
            format,
            &crate::native_resources::NativeResources::default()
                .scope(crate::native_resources::NativeClass::Foreground),
        )?;
        let decoded = ambient
            .inspect_to_disk(target, root.path(), budget.clone(), 0)
            .await?;
        assert_eq!(decoded.object().oid, target);
        assert_eq!(decoded.object().size, 5);
        assert_eq!(decoded.object().digest, *blake3::hash(b"base!").as_bytes());
        ambient.finish().await?;
        drop(decoded);
        let outcome = PhysicalVerifier::download(
            root.path(),
            budget.clone(),
            &prepared.store,
            descriptor,
            physical_limits(),
            crate::native_resources::NativeResources::default()
                .scope(crate::native_resources::NativeClass::Foreground),
        )
        .await;
        match outcome {
            Err(PhysicalError::Native(GitHttpError::GitExit { stderr, .. })) => {
                assert!(stderr.contains("unresolved"), "{stderr}")
            }
            _ => panic!("isolated native verification must reject an unresolved delta"),
        }
        drained(root.path(), &budget, 0).await?;
    }
    Ok(())
}

#[tokio::test]
async fn completed_thin_pack_verifies_every_entry_including_the_appended_external_base() -> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let prepared = prepared(format, 4).await?;
        let base = crate::object_id(format, ObjectKind::Blob, b"base");
        let target = crate::object_id(format, ObjectKind::Blob, b"base!");
        git_input(
            prepared.fixture.root.path(),
            &["hash-object", "-w", "--stdin"],
            b"base",
        )
        .await?;
        let output = git_input(
            prepared.fixture.root.path(),
            &["index-pack", "--stdin", "--fix-thin", "--index-version=2"],
            &thin_pack(format, base)?,
        )
        .await?;
        let checksum = ObjectId::from_hex(
            output
                .split(|byte| byte.is_ascii_whitespace())
                .rfind(|word| !word.is_empty())
                .ok_or("checksum")?,
        )?;
        let path = prepared
            .fixture
            .root
            .path()
            .join(format!("objects/pack/pack-{}.pack", hex::encode(checksum)));
        let pack_bytes = std::fs::read(&path)?;
        assert_eq!(u32::from_be_bytes(pack_bytes[8..12].try_into()?), 2);
        let digest = *blake3::hash(&pack_bytes).as_bytes();
        let key = |kind| ArtifactKey {
            operation: [10; 16],
            binding_digest: digest,
            kind,
        };
        let pack = upload_bytes(&prepared.store, key(ArtifactKind::Pack), &pack_bytes).await?;
        let index = upload_bytes(
            &prepared.store,
            key(ArtifactKind::Index),
            &std::fs::read(path.with_extension("idx"))?,
        )
        .await?;
        let descriptor = NativePackDescriptor {
            repository: prepared.descriptor.repository,
            operation: [10; 16],
            format,
            git_checksum: checksum,
            object_count: 2,
            pack,
            index,
        };
        let root = tempfile::TempDir::new()?;
        let budget = DiskBudget::new(128 << 20);
        let mut verifier = PhysicalVerifier::download(
            root.path(),
            budget.clone(),
            &prepared.store,
            descriptor,
            physical_limits(),
            crate::native_resources::NativeResources::default()
                .scope(crate::native_resources::NativeClass::Foreground),
        )
        .await?;
        let segment = verifier.inspect_next_shard(2).await?;
        let witness = verifier.finish().await?;
        witness.verify_segments([segment.descriptor()])?;
        for (oid, body) in [(base, b"base".as_slice()), (target, b"base!".as_slice())] {
            let header = segment.header(oid)?.ok_or("decoded header")?;
            assert_eq!(header.object.oid, oid);
            assert_eq!(header.object.kind, ObjectKind::Blob);
            assert_eq!(header.object.size, body.len() as u64);
            assert_eq!(header.object.digest, *blake3::hash(body).as_bytes());
            assert_eq!(header.edge_count, 0);
        }
        drop(segment);
        drained(root.path(), &budget, 0).await?;
    }
    Ok(())
}

#[tokio::test]
async fn isolated_pack_can_have_graph_dependencies_in_other_packs_without_external_delta_bases()
-> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let prepared = prepared(format, 16).await?;
        let (commit, edges) = prepared
            .fixture
            .objects
            .values()
            .find(|(object, _)| object.kind == ObjectKind::Commit)
            .ok_or("commit")?;
        let pack = git_input(
            prepared.fixture.root.path(),
            &["pack-objects", "--stdout", "--no-reuse-delta"],
            format!("{}\n", hex::encode(commit.oid)).as_bytes(),
        )
        .await?;
        assert_eq!(u32::from_be_bytes(pack[8..12].try_into()?), 1);
        let descriptor = upload_pair(&prepared, commit.oid, &pack).await?;
        let root = tempfile::TempDir::new()?;
        let budget = DiskBudget::new(128 << 20);
        let mut verifier = PhysicalVerifier::download(
            root.path(),
            budget.clone(),
            &prepared.store,
            descriptor,
            physical_limits(),
            crate::native_resources::NativeResources::default()
                .scope(crate::native_resources::NativeClass::Foreground),
        )
        .await?;
        let segment = verifier.inspect_next_shard(1).await?;
        let witness = verifier.finish().await?;
        witness.verify_segments([segment.descriptor()])?;
        assert_eq!(
            segment.header(commit.oid)?.ok_or("commit header")?.object,
            *commit
        );
        let dependencies = segment.edges_after(commit.oid, None)?;
        assert_eq!(dependencies, *edges);
        for edge in dependencies {
            assert!(segment.header(edge.child)?.is_none());
        }
        // This stage correctly proves physical delta independence, while global
        // graph closure still requires resolving these typed dependencies.
        drop(segment);
        drained(root.path(), &budget, 0).await?;
    }
    Ok(())
}
