use super::*;
use crate::RefExpectation;

#[test]
fn ancestry_queries_bound_exact_wire_bytes_and_row_count() -> Result<(), CodecError> {
    for long in [false, true] {
        let updates: Vec<_> = (0..256)
            .map(|i| crate::RefUpdate {
                name: if long {
                    format!("refs/tags/{}-{i:03}", "x".repeat(65_520))
                } else {
                    format!("refs/tags/{i:03}")
                },
                expected: None,
                new_oid: Some(ObjectId::Sha256([7; 32])),
            })
            .collect();
        if long {
            let original = SqlBatch {
                statements: updates[..128]
                    .iter()
                    .map(ancestry_policy_statement)
                    .collect(),
            };
            assert!(
                original
                    .encode(&mut BoundedEncoder::new(crate::operation(2).input_limit)?)
                    .is_err()
            );
        }
        let mut start = 0;
        while start < updates.len() {
            let (end, page) = ancestry_policy_page(&updates, start)?;
            assert_eq!(
                end - start,
                if long {
                    3.min(updates.len() - start)
                } else {
                    128
                }
            );
            let mut e = BoundedEncoder::new(POLICY_QUERY_BYTES)?;
            page.encode(&mut e)?;
            let bytes = e.finish();
            assert!(bytes.len() <= POLICY_QUERY_BYTES as usize);
            let mut d = BoundedDecoder::new(&bytes, POLICY_QUERY_BYTES)?;
            assert_eq!(SqlBatch::decode(&mut d)?, page);
            d.finish()?;
            start = end;
        }
    }
    Ok(())
}
#[test]
fn ref_plan_digest_reuses_exact_wire_bytes_including_long_names_and_tombstones()
-> Result<(), CodecError> {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let oid = if format == ObjectFormat::Sha1 {
            ObjectId::Sha1([7; 20])
        } else {
            ObjectId::Sha256([7; 32])
        };
        let plan = PushPlan {
            actor: "a".repeat(64),
            updates: vec![
                crate::RefUpdate {
                    name: format!("refs/heads/{}/topic", "nested/".repeat(100_000)),
                    expected: None,
                    new_oid: Some(oid),
                },
                crate::RefUpdate {
                    name: "refs/tags/é".into(),
                    expected: Some(RefExpectation {
                        oid: Some(oid),
                        version: 17,
                    }),
                    new_oid: None,
                },
                crate::RefUpdate {
                    name: "refs/heads/recreated".into(),
                    expected: Some(RefExpectation {
                        oid: None,
                        version: 91,
                    }),
                    new_oid: Some(oid),
                },
            ],
        };
        let mut e = BoundedEncoder::new(4 << 20)?;
        plan.encode(&mut e)?;
        let mut expected = blake3::Hasher::new();
        expected.update(b"canopy.ref-plan.v1\0");
        expected.update(&e.finish());
        assert_eq!(plan_digest(&plan)?, *expected.finalize().as_bytes());
        assert_ne!(binding(&plan, &[0])?, binding(&plan, &[7])?);
        assert!(binding(&plan, &[0x80]).is_err());
        assert!(binding(&plan, &[]).is_err());
    }
    Ok(())
}
