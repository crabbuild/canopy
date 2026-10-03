use super::*;
fn record() -> Record {
    let mut operation = *b"CANOPY01\0\0\0\0\0\0\0\0";
    operation[15] = 1;
    Record {
        check: LeaseCheck {
            actor: "owner".into(),
            token: PreparationToken {
                repository: *uuid::Uuid::new_v4().as_bytes(),
                operation: [1; 16],
                artifact_operation: operation,
                request_digest: [2; 32],
                owner: OwnerFence {
                    incarnation: IncarnationId::from_bytes([3; 16]),
                    epoch: 1,
                },
                attempt: 1,
            },
        },
        tenant: [4; 16],
        application: [5; 16],
        kind: Kind::Publish,
        root: StoredInputRoot {
            operation,
            artifact: ArtifactDescriptor {
                size: 512,
                digest: [6; 32],
                manifest_digest: [7; 32],
            },
        },
    }
}
#[test]
fn certificates_bind_exact_attempt_and_recovery_purpose_without_cycle() -> Result<(), CodecError> {
    let record = record();
    let seed = [9; 32];
    let sealed = RootRecoveryCertificate(CertificateEnvelope::seal(&record, &seed)?);
    let mut e = BoundedEncoder::new(CERTIFICATE_BYTES)?;
    sealed.encode(&mut e)?;
    let bytes = e.finish();
    assert!(bytes.len() < CERTIFICATE_BYTES as usize);
    let mut d = BoundedDecoder::new(&bytes, CERTIFICATE_BYTES)?;
    let restored = RootRecoveryCertificate::decode(&mut d)?;
    d.finish()?;
    assert!(restored.0.authenticated(&seed));
    assert_eq!(restored.0.data::<Record>()?, record);
    assert!(!restored.0.authenticated(&[10; 32]));
    let other = CertificateEnvelope::seal(&vec![1_u8, 2, 3], &seed)?;
    assert!(other.data::<Record>().is_err());
    for end in 0..bytes.len() {
        assert!(
            RootRecoveryCertificate::decode(&mut BoundedDecoder::new(
                &bytes[..end],
                CERTIFICATE_BYTES
            )?)
            .is_err()
        );
    }
    Ok(())
}
#[test]
fn root_descriptors_refuse_cross_attempt_namespace_and_oversize_before_io() -> Result<(), CodecError>
{
    let mut record = record();
    record.root.artifact.size = u64::from(ROOT_BYTES) + 1;
    assert!(CertificateEnvelope::seal(&record, &[1; 32]).is_err());
    record.root.artifact.size = u64::from(ROOT_BYTES);
    CertificateEnvelope::seal(&record, &[1; 32])?;
    record.root.operation[15] += 1;
    assert!(CertificateEnvelope::seal(&record, &[1; 32]).is_err());
    Ok(())
}
#[test]
fn registration_replies_reuse_bounded_denial_encoding() -> Result<(), CodecError> {
    for reply in [
        RootRecoveryReply::Registered,
        RootRecoveryReply::Denied(PreparationDenial::Unauthorized),
        RootRecoveryReply::Denied(PreparationDenial::Conflict),
        RootRecoveryReply::Denied(PreparationDenial::Stale),
        RootRecoveryReply::Denied(PreparationDenial::Expired),
        RootRecoveryReply::Denied(PreparationDenial::Capacity),
        RootRecoveryReply::Denied(PreparationDenial::Missing),
    ] {
        let mut e = BoundedEncoder::new(128)?;
        reply.encode(&mut e)?;
        let bytes = e.finish();
        let mut d = BoundedDecoder::new(&bytes, 128)?;
        assert_eq!(RootRecoveryReply::decode(&mut d)?, reply);
        d.finish()?;
    }
    Ok(())
}
