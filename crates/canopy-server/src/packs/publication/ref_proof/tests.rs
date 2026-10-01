use super::*;
use crate::RefExpectation;
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
