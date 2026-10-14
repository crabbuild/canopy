use super::*;
use crate::packs::{
    directory::{SegmentKey, index::NodeRef},
    input_artifact::StoredInputRoot,
};
use canopy_object_storage::artifact::ArtifactDescriptor;

#[test]
fn adopted_result_append_with_all_roots_and_maximum_actor_fits_checkpoint_envelope()
-> Result<(), CodecError> {
    let mut operation = *b"CANOPY01\0\0\0\0\0\0\0\x01";
    let artifact = ArtifactDescriptor {
        size: 1,
        digest: [1; 32],
        manifest_digest: [2; 32],
    };
    let stored = StoredInputRoot {
        operation,
        artifact,
    };
    let mut e = BoundedEncoder::new(256)?;
    stored.encode(&mut e)?;
    let bytes = e.finish();
    let wire_request = WireRequestRoot::decode(&mut BoundedDecoder::new(&bytes, 256)?)?;
    let native_result = NativeResultRoot::decode(&mut BoundedDecoder::new(&bytes, 256)?)?;
    let root = NativeInputRoot {
        operation,
        artifact,
        height: 0,
        first_key: SegmentKey {
            operation,
            digest: [3; 32],
        },
        last_key: SegmentKey {
            operation,
            digest: [4; 32],
        },
        record_count: i64::MAX as u64,
        object_count: i64::MAX as u64,
    };
    let mut repository = [5; 16];
    repository[6] = 0x45;
    repository[8] = 0x85;
    assert!(crate::validate_repository_id(repository).is_ok());
    let token = PreparationToken {
        repository,
        operation: [6; 16],
        artifact_operation: operation,
        request_digest: [7; 32],
        owner: OwnerFence {
            incarnation: IncarnationId::from_bytes([8; 16]),
            epoch: u64::MAX,
        },
        attempt: i64::MAX as u64,
    };
    operation[15] = 2;
    let mut current = token;
    current.artifact_operation = operation;
    current.owner.epoch -= 1;
    let inputs = Inputs {
        tenant: [9; 16],
        application: [10; 16],
        token: current,
        actor: "a".repeat(64),
        format: ObjectFormat::Sha256,
        root: Some(NodeRef { operation, ..root }),
        wire_request: Some(wire_request),
        native_result: Some(native_result),
        source: Some((token, [11; 32])),
        previous: Some([12; 32]),
    };
    let proof = NativeInputCertificate(CertificateEnvelope::seal(&inputs, &[13; 32])?);
    let bytes = proof.bytes()?;
    assert!(bytes.len() <= CERTIFICATE_BYTES as usize);
    assert_eq!(NativeInputCertificate::from_bytes(&bytes)?, proof);
    Ok(())
}
