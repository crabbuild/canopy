use super::*;

#[test]
fn repeated_edges_share_one_proof_but_different_types_do_not() {
    let mut body = Vec::new();
    for (mode, name) in [
        ("100644", "a"),
        ("100755", "b"),
        ("40000", "c"),
        ("160000", "d"),
    ] {
        body.extend(format!("{mode} {name}\0").as_bytes());
        body.extend([1; 20]);
    }
    assert_eq!(
        edges(crate::ObjectFormat::Sha1, ObjectKind::Tree, &body),
        Some(vec![
            (crate::ObjectId::Sha1([1; 20]), Some(ObjectKind::Blob)),
            (crate::ObjectId::Sha1([1; 20]), Some(ObjectKind::Tree))
        ])
    );
}
