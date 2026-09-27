use super::*;

#[test]
fn certificate_codec_rejects_empty_oversized_and_malformed_batches() {
    for count in [0, MAX_CERTIFICATES + 1] {
        let mut encoder = BoundedEncoder::new(1 << 20).unwrap();
        encoder.write_count(count).unwrap();
        let input = encoder.finish();
        assert!(
            CertificateBatch::decode(&mut BoundedDecoder::new(&input, 1 << 20).unwrap()).is_err()
        );
    }
    let mut encoder = BoundedEncoder::new(1 << 20).unwrap();
    encoder.write_count(1).unwrap();
    encoder.write_bytes(&[0; 19]).unwrap();
    let input = encoder.finish();
    assert!(CertificateBatch::decode(&mut BoundedDecoder::new(&input, 1 << 20).unwrap()).is_err());
    let batch = CertificateBatch(vec![crate::ObjectId::Sha1([1; 20]); MAX_CERTIFICATES]);
    let mut encoder = BoundedEncoder::new(1 << 20).unwrap();
    batch.encode(&mut encoder).unwrap();
    let input = encoder.finish();
    let mut decoder = BoundedDecoder::new(&input, 1 << 20).unwrap();
    assert_eq!(CertificateBatch::decode(&mut decoder).unwrap().0, batch.0);
    decoder.finish().unwrap();
}

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
