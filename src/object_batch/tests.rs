use super::*;

fn inline(size: usize) -> StoredObject {
    StoredObject {
        oid: crate::ObjectId::Sha1([0; 20]),
        kind: ObjectKind::Blob,
        storage: ObjectStorage::Input(vec![11; size]),
    }
}

#[test]
fn maximum_batch_round_trips_below_the_operation_wire_limit() {
    let mut batch = ObjectBatch::default();
    for _ in 0..MAX_OBJECTS - 1 {
        assert!(
            batch
                .try_push(StoredObject {
                    oid: crate::ObjectId::Sha1([1; 20]),
                    kind: ObjectKind::Blob,
                    storage: ObjectStorage::Input(Vec::new()),
                })
                .is_ok()
        );
    }
    assert!(batch.try_push(inline(INLINE_OBJECT_LIMIT)).is_ok());
    let mut encoder = BoundedEncoder::new(INPUT_LIMIT).unwrap();
    batch.encode(&mut encoder).unwrap();
    let encoded = encoder.finish();
    assert!(encoded.len() < INPUT_LIMIT as usize);
    let mut decoder = BoundedDecoder::new(&encoded, INPUT_LIMIT).unwrap();
    let decoded = ObjectBatch::decode(&mut decoder).unwrap();
    decoder.finish().unwrap();
    let mut encoder = BoundedEncoder::new(INPUT_LIMIT).unwrap();
    decoded.encode(&mut encoder).unwrap();
    assert_eq!(encoder.finish(), encoded);
}

#[test]
fn a_full_batch_returns_the_unconsumed_record() {
    let mut batch = ObjectBatch::default();
    assert!(batch.try_push(inline(INLINE_OBJECT_LIMIT + 1)).is_err());
    for _ in 0..INPUT_BYTES / INLINE_OBJECT_LIMIT {
        assert!(batch.try_push(inline(INLINE_OBJECT_LIMIT)).is_ok());
    }
    let leftover = match batch.try_push(inline(1)) {
        Err(object) => object,
        Ok(()) => panic!("byte budget must reject the record"),
    };
    let mut next = ObjectBatch::default();
    assert!(next.try_push(leftover).is_ok());
    for _ in 1..MAX_OBJECTS {
        assert!(next.try_push(inline(0)).is_ok());
    }
    assert!(next.try_push(inline(0)).is_err());
}

#[test]
fn decoding_enforces_aggregate_bytes_and_count_before_building_a_batch() {
    for count in [0, MAX_OBJECTS + 1] {
        let mut encoder = BoundedEncoder::new(INPUT_LIMIT).unwrap();
        encoder.write_count(count).unwrap();
        let bytes = encoder.finish();
        let mut decoder = BoundedDecoder::new(&bytes, INPUT_LIMIT).unwrap();
        assert!(ObjectBatch::decode(&mut decoder).is_err());
    }
    let mut encoder = BoundedEncoder::new(INPUT_LIMIT).unwrap();
    encoder.write_count(5).unwrap();
    for size in [
        INLINE_OBJECT_LIMIT,
        INLINE_OBJECT_LIMIT,
        INLINE_OBJECT_LIMIT,
        INLINE_OBJECT_LIMIT,
        1,
    ] {
        encoder.write_bytes(&[0; 20]).unwrap();
        encoder.write_u8(0).unwrap(); // Blob.
        encoder.write_u8(0).unwrap(); // Inline.
        encoder.write_bytes(&vec![5; size]).unwrap();
    }
    let bytes = encoder.finish();
    let mut decoder = BoundedDecoder::new(&bytes, INPUT_LIMIT).unwrap();
    assert!(ObjectBatch::decode(&mut decoder).is_err());
    let mut encoder = BoundedEncoder::new(INPUT_LIMIT).unwrap();
    encoder.write_count(1).unwrap();
    encoder.write_bytes(&[0; 20]).unwrap();
    encoder.write_u8(0).unwrap();
    encoder.write_u8(0).unwrap();
    encoder
        .write_bytes(&vec![5; INLINE_OBJECT_LIMIT + 1])
        .unwrap();
    let bytes = encoder.finish();
    let mut decoder = BoundedDecoder::new(&bytes, INPUT_LIMIT).unwrap();
    assert!(ObjectBatch::decode(&mut decoder).is_err());
    assert!(
        ObjectBatch::default()
            .encode(&mut BoundedEncoder::new(1 << 20).unwrap())
            .is_err()
    );
}
