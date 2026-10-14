use super::*;
use cellule_runtime::{ApplicationId, TenantId};
use std::io::Write;

type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;

struct Fixture {
    root: tempfile::TempDir,
    disk: DiskBudget,
    repository: [u8; 16],
    target: CellTarget,
}
impl Fixture {
    fn new() -> Result<Self> {
        let repository = *uuid::Uuid::parse_str("12345678-1234-4234-8234-123456789abc")?.as_bytes();
        Ok(Self {
            root: tempfile::TempDir::new()?,
            disk: DiskBudget::new(1 << 20),
            repository,
            target: crate::repository_target(
                TenantId::from_bytes([12; 16]),
                ApplicationId::from_bytes([13; 16]),
                repository,
            )?,
        })
    }
    async fn request(&self, bytes: Vec<u8>) -> Result<GitHttpRequest> {
        Ok(GitHttpRequest {
            method: "POST".into(),
            path_info: "/repo.git/git-receive-pack".into(),
            query: String::new(),
            content_type: Some("application/x-git-receive-pack-request".into()),
            gzip: false,
            protocol_v2: false,
            authenticated: true,
            body: GitInput::receive(Body::from(bytes), self.root.path(), &self.disk, None, None)
                .await?,
        })
    }
    async fn encoded(
        &self,
        request: GitHttpRequest,
        format: crate::ObjectFormat,
    ) -> Result<EncodedPush> {
        Ok(EncodedPush::new(
            request,
            &self.target,
            self.repository,
            format,
            "owner",
            [2; 16],
        )
        .await?)
    }
}
fn packet(bytes: &[u8]) -> Vec<u8> {
    [
        format!("{:04x}", bytes.len() + 4).into_bytes(),
        bytes.to_vec(),
    ]
    .concat()
}
fn commands(format: crate::ObjectFormat, options: Option<&str>, signed: bool) -> Vec<u8> {
    let line = format!(
        "{} {} refs/heads/main",
        "00".repeat(format.bytes()),
        "12".repeat(format.bytes())
    );
    let capabilities = if options.is_some() {
        "report-status side-band-64k push-options"
    } else {
        "report-status"
    };
    let mut bytes = if signed {
        let mut bytes = packet(format!("push-cert\0{capabilities}\n").as_bytes());
        bytes.extend(packet(b"certificate version 0.1\n"));
        if let Some(option) = options {
            bytes.extend(packet(format!("push-option {option}\n").as_bytes()));
        }
        for line in [
            "\n".to_owned(),
            format!("{line}\n"),
            "-----BEGIN SSH SIGNATURE-----\n".to_owned(),
            "unverified-signature\n".to_owned(),
            "-----END SSH SIGNATURE-----\n".to_owned(),
            "push-cert-end\n".to_owned(),
        ] {
            bytes.extend(packet(line.as_bytes()));
        }
        bytes
    } else {
        packet(format!("{line}\0{capabilities}\n").as_bytes())
    };
    bytes.extend(b"0000");
    if let Some(option) = options {
        bytes.extend(packet(option.as_bytes()));
        bytes.extend(b"0000");
    }
    bytes.extend(b"PACKnative-only");
    bytes
}
fn gzip(bytes: &[u8]) -> Result<Vec<u8>> {
    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    encoder.write_all(bytes)?;
    Ok(encoder.finish()?)
}

#[tokio::test]
async fn identity_binds_scope_actor_operation_format_metadata_and_encoded_bytes() -> Result {
    let fixture = Fixture::new()?;
    let bytes = commands(crate::ObjectFormat::Sha1, None, false);
    let original = fixture
        .encoded(
            fixture.request(bytes.clone()).await?,
            crate::ObjectFormat::Sha1,
        )
        .await?;
    let digest = original.identity().request_digest;
    let repeated = fixture
        .encoded(
            fixture.request(bytes.clone()).await?,
            crate::ObjectFormat::Sha1,
        )
        .await?;
    assert_eq!(digest, repeated.identity().request_digest);
    drop(repeated);
    for mutation in 0..10 {
        let mut request = fixture.request(bytes.clone()).await?;
        let mut target = fixture.target.clone();
        let mut repository = fixture.repository;
        let mut operation = [2; 16];
        let mut actor = "owner";
        let mut format = crate::ObjectFormat::Sha1;
        match mutation {
            0 => {
                target = crate::repository_target(
                    TenantId::from_bytes([14; 16]),
                    target.application(),
                    repository,
                )?
            }
            1 => {
                target = crate::repository_target(
                    target.tenant(),
                    ApplicationId::from_bytes([14; 16]),
                    repository,
                )?
            }
            2 => {
                repository[15] ^= 1;
                target =
                    crate::repository_target(target.tenant(), target.application(), repository)?;
            }
            3 => operation[15] ^= 1,
            4 => actor = "another",
            5 => format = crate::ObjectFormat::Sha256,
            6 => request.query = "service=git-receive-pack".into(),
            7 => request.protocol_v2 = true,
            8 => request.content_type = None,
            9 => request.gzip = true,
            _ => unreachable!(),
        }
        let changed =
            EncodedPush::new(request, &target, repository, format, actor, operation).await?;
        assert_ne!(
            digest,
            changed.identity().request_digest,
            "mutation {mutation}"
        );
    }
    let changed = fixture
        .encoded(
            fixture
                .request([bytes, b"different pack".to_vec()].concat())
                .await?,
            crate::ObjectFormat::Sha1,
        )
        .await?;
    assert_ne!(digest, changed.identity().request_digest);
    drop(changed);
    drop(original);
    assert_eq!(fixture.disk.used(), 0);
    Ok(())
}

#[tokio::test]
async fn encoded_gzip_identity_survives_normalization_and_native_spool_rewind() -> Result {
    for format in [crate::ObjectFormat::Sha1, crate::ObjectFormat::Sha256] {
        let fixture = Fixture::new()?;
        let bytes = commands(format, Some("canopy.note=normalized"), false);
        let plain = fixture
            .encoded(fixture.request(bytes.clone()).await?, format)
            .await?;
        let plain_digest = plain.identity().request_digest;
        drop(plain);
        let wire = gzip(&bytes)?;
        let mut request = fixture.request(wire.clone()).await?;
        request.gzip = true;
        let encoded = fixture.encoded(request, format).await?;
        let digest = encoded.identity().request_digest;
        assert_ne!(digest, plain_digest);
        assert_eq!(fixture.disk.used(), wire.len() as u64);
        let parts = encoded
            .decode(fixture.root.path(), &fixture.disk, Some(bytes.len() as u64))
            .await?
            .into_parts();
        assert_eq!(parts.identity.request_digest, digest);
        assert_eq!(parts.identity.operation, [2; 16]);
        assert_eq!(parts.identity.actor, "owner");
        assert!(!parts.request.gzip);
        assert_eq!(fixture.disk.used(), bytes.len() as u64);
        assert_eq!(parts.request.body.prefix(bytes.len()).await?, bytes);
        assert_eq!(parts.commands.options(), ["canopy.note=normalized"]);
        assert_eq!(parts.commands.option_error(), None);
        let branch_policy::PushCommands::Parsed { updates, .. } = &parts.commands else {
            panic!("parsed commands")
        };
        assert_eq!(updates.len(), 1);
        assert_eq!(updates[0].new_oid.unwrap().format(), format);
        assert!(updates[0].expected.is_none());
        drop(parts);
        assert_eq!(fixture.disk.used(), 0);
    }
    Ok(())
}

#[tokio::test]
async fn repository_format_applies_to_zero_ids_signed_commands_and_shallow_ids() -> Result {
    for expected in [crate::ObjectFormat::Sha1, crate::ObjectFormat::Sha256] {
        let fixture = Fixture::new()?;
        let wrong = if expected == crate::ObjectFormat::Sha1 {
            crate::ObjectFormat::Sha256
        } else {
            crate::ObjectFormat::Sha1
        };
        for signed in [false, true] {
            let bytes = commands(wrong, None, signed);
            let all_zero = String::from_utf8(bytes.clone())?
                .replace(&"12".repeat(wrong.bytes()), &"00".repeat(wrong.bytes()))
                .into_bytes();
            for bytes in [bytes, all_zero] {
                let encoded = fixture
                    .encoded(fixture.request(bytes).await?, expected)
                    .await?;
                assert!(matches!(
                    encoded
                        .decode(fixture.root.path(), &fixture.disk, None)
                        .await,
                    Err(GatewayError::Input(InputError::Commands))
                ));
            }
        }
        let mut bytes = packet(format!("shallow {}\n", "12".repeat(wrong.bytes())).as_bytes());
        bytes.extend(commands(expected, None, false));
        let encoded = fixture
            .encoded(fixture.request(bytes).await?, expected)
            .await?;
        assert!(matches!(
            encoded
                .decode(fixture.root.path(), &fixture.disk, None)
                .await,
            Err(GatewayError::Input(InputError::Commands))
        ));
        assert_eq!(fixture.disk.used(), 0);
    }
    Ok(())
}

#[tokio::test]
async fn signed_intent_preserves_certificate_and_checks_separate_option_group() -> Result {
    let fixture = Fixture::new()?;
    for mismatch in [false, true] {
        let mut bytes = commands(
            crate::ObjectFormat::Sha256,
            Some("canopy.note=signed"),
            true,
        );
        if mismatch {
            let position = bytes
                .windows(b"canopy.note=signed".len())
                .rposition(|part| part == b"canopy.note=signed")
                .unwrap();
            bytes[position + b"canopy.note=".len()] = b'x';
        }
        let encoded = fixture
            .encoded(
                fixture.request(bytes.clone()).await?,
                crate::ObjectFormat::Sha256,
            )
            .await?;
        let parts = encoded
            .decode(fixture.root.path(), &fixture.disk, None)
            .await?
            .into_parts();
        let certificate = parts.commands.certificate().expect("certificate intent");
        assert!(
            certificate
                .windows(b"unverified-signature".len())
                .any(|part| part == b"unverified-signature")
        );
        assert_eq!(parts.commands.option_error().is_some(), mismatch);
        assert_eq!(parts.request.body.prefix(bytes.len()).await?, bytes);
        drop(parts);
        assert_eq!(fixture.disk.used(), 0);
    }
    Ok(())
}

#[tokio::test]
async fn invalid_scope_authentication_and_transport_never_return_a_preflight() -> Result {
    let fixture = Fixture::new()?;
    for mutation in 0..4 {
        let mut request = fixture.request(b"invalid gzip".to_vec()).await?;
        let mut actor = "owner";
        let mut repository = fixture.repository;
        match mutation {
            0 => request.authenticated = false,
            1 => request.method = "GET".into(),
            2 => actor = "bad/actor",
            3 => repository[15] ^= 1,
            _ => unreachable!(),
        }
        let result = EncodedPush::new(
            request,
            &fixture.target,
            repository,
            crate::ObjectFormat::Sha1,
            actor,
            [2; 16],
        )
        .await;
        assert!(if mutation == 0 {
            matches!(result, Err(GatewayError::Unauthorized))
        } else {
            matches!(result, Err(GatewayError::MalformedCache))
        });
        assert_eq!(fixture.disk.used(), 0);
    }
    for wire in [b"invalid gzip".to_vec(), gzip(&vec![b'x'; 100_000])?] {
        let mut request = fixture.request(wire).await?;
        request.gzip = true;
        let encoded = fixture.encoded(request, crate::ObjectFormat::Sha1).await?;
        assert!(matches!(
            encoded
                .decode(fixture.root.path(), &fixture.disk, Some(100))
                .await,
            Err(GatewayError::Input(
                InputError::Gzip(_) | InputError::TooLarge
            ))
        ));
        assert_eq!(fixture.disk.used(), 0);
    }
    Ok(())
}
