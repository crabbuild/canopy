use super::*;

fn inspect(
    format: ObjectFormat,
    kind: ObjectKind,
    body: &[u8],
    chunk: usize,
) -> Option<Vec<(ObjectId, Option<ObjectKind>)>> {
    let mut parser = EdgeParser::new(format, kind);
    let mut edges = Vec::new();
    for bytes in body.chunks(chunk) {
        parser
            .feed(bytes, |edge| {
                edges.push((edge.child, Some(edge.expected_kind)))
            })
            .ok()?;
    }
    parser.finish().ok()?;
    edges.sort_unstable();
    edges.dedup();
    Some(edges)
}
#[test]
fn streamed_graph_matches_existing_semantics_across_every_field_boundary() {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let bytes = vec![1; format.bytes()];
        let oid = hex::encode(&bytes);
        let mut tree = Vec::new();
        for (mode, name) in [
            ("100644", &b"a"[..]),
            ("100755", &b"b"[..]),
            ("040000", &b"tree"[..]),
            ("120000", &b"link"[..]),
            ("160000", &b"gitlink"[..]),
            ("100644", &b"\xff\n"[..]),
        ] {
            tree.extend_from_slice(mode.as_bytes());
            tree.push(b' ');
            tree.extend_from_slice(name);
            tree.push(0);
            tree.extend_from_slice(&bytes);
        }
        let commit = format!("tree {oid}\nparent {oid}\nparent {oid}\nauthor Long Person\ngpgsig signature\n parent fake\n\nparent ignored\n").into_bytes();
        let tag = format!(
            "object {oid}\ntype commit\ntag {}\n\nmessage",
            "x".repeat(1024)
        )
        .into_bytes();
        for (kind, body) in [
            (ObjectKind::Blob, b"arbitrary\0bytes\xff".to_vec()),
            (ObjectKind::Tree, tree),
            (ObjectKind::Commit, commit),
            (ObjectKind::Tag, tag),
        ] {
            let expected = crate::graph::edges(format, kind, &body);
            for chunk in [1, 2, 7, 19, 20, 31, 32, 64, CHUNK_BYTES] {
                assert_eq!(
                    inspect(format, kind, &body, chunk),
                    expected,
                    "format={format:?} kind={kind:?} chunk={chunk}"
                );
            }
        }
        for suffix in ["", "par", "parent", "author", "\nmessage"] {
            let body = format!("tree {oid}\n{suffix}");
            assert_eq!(
                inspect(format, ObjectKind::Commit, body.as_bytes(), 1),
                crate::graph::edges(format, ObjectKind::Commit, body.as_bytes())
            );
        }
    }
}
#[test]
fn malformed_structures_and_failed_parsers_cannot_finish_or_resume() {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let raw = vec![1; format.bytes()];
        let oid = hex::encode(&raw);
        let mut cases = Vec::new();
        for (mode, name) in [
            ("", "name"),
            ("8", "name"),
            ("1000000", "name"),
            ("000000", "name"),
            ("100644", ""),
            ("100644", "."),
            ("100644", ".."),
            ("100644", "a/b"),
        ] {
            let mut body = format!("{mode} {name}\0").into_bytes();
            body.extend_from_slice(&raw);
            cases.push((ObjectKind::Tree, body));
        }
        let mut zero = b"160000 gitlink\0".to_vec();
        zero.extend(vec![0; format.bytes()]);
        cases.push((ObjectKind::Tree, zero));
        cases.push((ObjectKind::Tree, b"100644 unfinished".to_vec()));
        cases.push((
            ObjectKind::Commit,
            format!("tree {oid}\nparent ").into_bytes(),
        ));
        cases.push((ObjectKind::Commit, format!("tree {oid}").into_bytes()));
        cases.push((ObjectKind::Commit, format!("tree {oid}0\n").into_bytes()));
        cases.push((
            ObjectKind::Tag,
            format!("object {oid}\ntype unknown\ntag x\n").into_bytes(),
        ));
        cases.push((
            ObjectKind::Tag,
            format!("object {oid}\ntype commit\ntag x").into_bytes(),
        ));
        for (kind, body) in cases {
            assert!(
                inspect(format, kind, &body, 1).is_none(),
                "accepted {kind:?} {body:?}"
            );
            assert!(crate::graph::edges(format, kind, &body).is_none());
        }
        let mut parser = EdgeParser::new(format, ObjectKind::Tree);
        assert!(parser.feed(b"8", |_| {}).is_err());
        assert!(parser.feed(b"100644 a\0", |_| {}).is_err());
        assert!(parser.finish().is_err());
        let mut parser = EdgeParser::new(format, ObjectKind::Blob);
        assert!(parser.feed(&vec![0; CHUNK_BYTES + 1], |_| {}).is_err());
        assert!(parser.finish().is_err());
    }
}
#[test]
fn very_long_names_messages_and_tags_keep_constant_parser_state() -> Result<(), GraphStreamError> {
    assert!(std::mem::size_of::<EdgeParser>() <= 256);
    let mut tree = EdgeParser::new(ObjectFormat::Sha256, ObjectKind::Tree);
    tree.feed(b"100644 ", |_| {})?;
    let bytes = vec![b'x'; CHUNK_BYTES];
    for _ in 0..128 {
        tree.feed(&bytes, |_| panic!("early edge"))?;
    }
    let mut edges = 0;
    tree.feed(&[0], |_| {})?;
    tree.feed(&[1; 32], |_| edges += 1)?;
    tree.finish()?;
    assert_eq!(edges, 1);
    for kind in [ObjectKind::Commit, ObjectKind::Tag] {
        let mut parser = EdgeParser::new(ObjectFormat::Sha256, kind);
        let oid = hex::encode([1; 32]);
        let prefix = if kind == ObjectKind::Commit {
            format!("tree {oid}\nauthor ")
        } else {
            format!("object {oid}\ntype commit\ntag ")
        };
        parser.feed(prefix.as_bytes(), |_| {})?;
        for _ in 0..128 {
            parser.feed(&bytes, |_| panic!("unexpected edge"))?;
        }
        parser.feed(b"\n\nmessage", |_| {})?;
        parser.finish()?;
    }
    Ok(())
}
