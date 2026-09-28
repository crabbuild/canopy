use super::*;
use crate::{
    git_cache::ReceiveHook,
    refs::{MAX_UPDATES, valid_ref_name},
};

const PREFIX_LIMIT: usize = 40 * 1024 * 1024;
const OPTION_PREFIX_LIMIT: usize = 32 * 1024;

pub(super) enum PushCommands {
    OtherMedia,
    Limited,
    Parsed {
        updates: Vec<RefUpdate>,
        report_status: bool,
        sideband: bool,
        options_requested: bool,
        options: Vec<String>,
        certificate_options: Option<Vec<String>>,
        certificate_body: Option<Vec<u8>>,
        options_error: Option<&'static str>,
    },
}

impl PushCommands {
    pub(super) async fn read(request: &GitHttpRequest) -> Result<Self, InputError> {
        // Native Git owns media-type errors and command-limit hook reports.
        if request.content_type.as_deref() != Some("application/x-git-receive-pack-request") {
            return Ok(Self::OtherMedia);
        }
        match request.body.packet_prefix(PREFIX_LIMIT).await {
            Ok(prefix) => {
                let mut parsed = match commands(&prefix) {
                    Ok(parsed) => parsed,
                    // Count and byte limits share the native Git rejection path.
                    Err(InputError::TooLarge) => return Ok(Self::Limited),
                    Err(error) => return Err(error),
                };
                if let Self::Parsed {
                    options_requested: true,
                    options,
                    options_error,
                    ..
                } = &mut parsed
                {
                    match request
                        .body
                        .packet_group(prefix.len() as u64, OPTION_PREFIX_LIMIT)
                        .await
                    {
                        Ok(group) => match parse_options(&group) {
                            Ok(parsed) => *options = parsed,
                            Err(()) => *options_error = Some("Canopy push options are malformed"),
                        },
                        Err(InputError::TooLarge | InputError::Commands) => {
                            *options_error = Some("Canopy push options are malformed or too large")
                        }
                        Err(error) => return Err(error),
                    }
                }
                Ok(parsed)
            }
            Err(InputError::TooLarge) => Ok(Self::Limited),
            Err(error) => Err(error),
        }
    }

    pub(super) fn options(&self) -> &[String] {
        match self {
            Self::Parsed { options, .. } => options,
            Self::OtherMedia | Self::Limited => &[],
        }
    }

    pub(super) fn certificate(&self) -> Option<&[u8]> {
        match self {
            Self::Parsed {
                certificate_body, ..
            } => certificate_body.as_deref(),
            Self::OtherMedia | Self::Limited => None,
        }
    }

    pub(super) fn option_error(&self) -> Option<&'static str> {
        let Self::Parsed {
            options,
            certificate_options,
            options_error,
            ..
        } = self
        else {
            return None;
        };
        options_error
            .or_else(|| {
                certificate_options
                    .as_ref()
                    .filter(|signed| *signed != options)
                    .map(|_| "Canopy signed push options do not match the request")
            })
            .or_else(|| {
                (!crate::push::valid_options(options))
                    .then_some("Canopy supports only canopy.note=<text> push options")
            })
    }

    pub(super) fn rejection(&self, reason: &str) -> Result<Option<GitHttpResponse>, PushError> {
        let Self::Parsed {
            updates,
            report_status,
            sideband,
            ..
        } = self
        else {
            return Ok(None);
        };
        crate::push::report::rejected_commands(
            updates.iter().map(|update| update.name.as_str()),
            *report_status,
            *sideband,
            reason,
        )
        .map(Some)
    }
}

impl GitGateway {
    pub(super) async fn install_certificate_policy(
        &self,
        cached: &CachedRepository,
        commands: &PushCommands,
        actor: &str,
    ) -> Result<Option<std::path::PathBuf>, GatewayError> {
        let Some(directory) = &self.signer_directory else {
            return Ok(None);
        };
        if commands.certificate().is_none() {
            return Ok(None);
        }
        let signers = directory
            .push_signers(actor)
            .await
            .map_err(|error| GatewayError::Cell(Box::new(error)))?;
        let mut body = Vec::new();
        for signer in signers {
            body.extend_from_slice(format!("{actor} {}\n", signer.public_key()).as_bytes());
        }
        let path = cached.backend.cache.store_push_signers(body).await?;
        let receipt = quote(
            &cached
                .backend
                .git_dir()
                .join("hooks/canopy-push-certificate")
                .display()
                .to_string(),
        );
        let actor = quote(actor);
        let script = format!(
            "#!/bin/sh\n\
            if [ -z \"${{GIT_PUSH_CERT-}}\" ]; then exit 0; fi\n\
            if [ \"${{GIT_PUSH_CERT_STATUS-}}\" != G ] || [ \"${{GIT_PUSH_CERT_SIGNER-}}\" != {actor} ]; then\n\
              printf '%s\\n' 'Canopy signed push signature or signer is invalid' >&2; exit 1\n\
            fi\n\
            case \"${{GIT_PUSH_CERT_NONCE_STATUS-}}\" in OK|SLOP) ;; *) printf '%s\\n' 'Canopy signed push nonce is invalid' >&2; exit 1 ;; esac\n\
            if [ -z \"${{GIT_PUSH_CERT_KEY-}}\" ]; then\n\
              printf '%s\\n' 'Canopy signed push key is missing' >&2; exit 1\n\
            fi\n\
            printf '%s\\n%s\\n%s\\n' \"$GIT_PUSH_CERT\" \"$GIT_PUSH_CERT_SIGNER\" \"$GIT_PUSH_CERT_KEY\" > {receipt}\n"
        );
        cached
            .backend
            .cache
            .store_receive_hook(ReceiveHook::PreReceive, script.into_bytes())
            .await?;
        Ok(Some(path))
    }

    pub(super) async fn install_branch_policy(
        &self,
        cached: &CachedRepository,
        commands: &PushCommands,
    ) -> Result<(), GatewayError> {
        let updates = match commands {
            PushCommands::OtherMedia => return Ok(()),
            PushCommands::Parsed { updates, .. } => updates,
            PushCommands::Limited => {
                let message = format!(
                    "Canopy push command limit exceeded ({MAX_UPDATES} updates or {PREFIX_LIMIT} command bytes)"
                );
                cached
                    .backend
                    .cache
                    .store_receive_hook(
                        ReceiveHook::PreReceive,
                        format!("#!/bin/sh\nprintf '%s\\n' '{message}' >&2\nexit 1\n").into_bytes(),
                    )
                    .await?;
                return Ok(());
            }
        };
        let zero = hex::encode(self.repository.object_format().zero());
        let has_rules = self
            .repository
            .has_branch_rules()
            .await
            .map_err(|source| GatewayError::Cell(Box::new(source)))?;
        let mut restricted = false;
        let mut script = String::from(
            "#!/bin/sh\ncase \"$1\" in\nrefs/canopy|refs/canopy/*) printf '%s\\n' 'Canopy server-owned ref is immutable' >&2; exit 1 ;;\n",
        );
        for updates in updates.chunks(128) {
            let policies = if has_rules {
                self.repository
                    .branch_policies(updates)
                    .await
                    .map_err(|source| GatewayError::Cell(Box::new(source)))?
            } else {
                updates.iter().map(|_| None).collect()
            };
            for (update, policy) in updates.iter().zip(policies) {
                if crate::refs::server_owned_ref(&update.name) {
                    restricted = true;
                    continue;
                }
                if !valid_ref_name(&update.name) {
                    restricted = true;
                    script.push_str(&format!("{}) printf '%s\\n' 'Canopy ref name exceeds supported format or length' >&2; exit 1 ;;\n", quote(&update.name)));
                    continue;
                }
                let Some(policy) = policy else {
                    continue;
                };
                let old = update
                    .expected
                    .as_ref()
                    .and_then(|old| old.oid)
                    .map_or_else(|| zero.clone(), hex::encode);
                let new = update.new_oid.map_or_else(|| zero.clone(), hex::encode);
                let allowed = policy.allows(update, false);
                let ancestry = policy.fast_forward_only && old != zero && new != zero;
                if allowed && !ancestry {
                    continue;
                }
                restricted = true;
                script.push_str(&format!(
                    "{})\n[ \"$2\" = '{old}' ] && [ \"$3\" = '{new}' ] || exit 1\n",
                    quote(&update.name)
                ));
                if !allowed {
                    script.push_str(
                        "printf '%s\\n' 'Canopy branch rule rejected this update' >&2\nexit 1\n",
                    );
                } else if ancestry {
                    script.push_str("git merge-base --is-ancestor \"$2\" \"$3\" || { printf '%s\\n' 'Canopy branch requires a fast-forward update' >&2; exit 1; }\n");
                }
                script.push_str("exit 0\n;;\n");
            }
        }
        // The immutable spool contains all requested refs. When none need a
        // hook, avoid a shell process per ref; publication still checks policy.
        if !restricted {
            return Ok(());
        }
        // Only restricted refs need hook entries.
        // Native Git supplies per-ref/atomic reports, while Cell publication
        // rechecks authorization, policy, namespace conflicts and expected tips.
        script.push_str("*) exit 0 ;;\nesac\n");
        cached
            .backend
            .cache
            .store_receive_hook(ReceiveHook::Update, script.into_bytes())
            .await?;
        Ok(())
    }
}

fn quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\\''"))
}

fn commands(mut bytes: &[u8]) -> Result<PushCommands, InputError> {
    let mut report_status = false;
    let mut sideband = false;
    let mut options_requested = false;
    let mut updates = Vec::new();
    let mut names = BTreeSet::new();
    let mut certificate = None;
    let mut certificate_options: Option<Vec<String>> = None;
    let mut certificate_body: Option<Vec<u8>> = None;
    loop {
        let header = bytes.get(..4).ok_or(InputError::Commands)?;
        if !header.iter().all(u8::is_ascii_hexdigit) {
            return Err(InputError::Commands);
        }
        let length = std::str::from_utf8(header)
            .ok()
            .and_then(|s| usize::from_str_radix(s, 16).ok())
            .ok_or(InputError::Commands)?;
        if length == 0 {
            if certificate.is_some_and(|state| state != Certificate::Done) {
                return Err(InputError::Commands);
            }
            return Ok(PushCommands::Parsed {
                updates,
                report_status,
                sideband,
                options_requested,
                options: Vec::new(),
                certificate_options,
                certificate_body,
                options_error: None,
            });
        }
        if !(5..=65520).contains(&length) {
            return Err(InputError::Commands);
        }
        let payload = bytes.get(4..length).ok_or(InputError::Commands)?;
        bytes = &bytes[length..];
        if let Some(state) = &mut certificate {
            // These ref names feed policy and SSH pack detection only. This
            // parser does not verify a signature or authorize publication.
            if payload != b"push-cert-end\n" {
                certificate_body
                    .as_mut()
                    .ok_or(InputError::Commands)?
                    .extend_from_slice(payload);
            }
            match state {
                Certificate::Headers if payload == b"\n" => *state = Certificate::Updates,
                Certificate::Headers
                    if payload.starts_with(b"push-option ") && payload.ends_with(b"\n") =>
                {
                    let option =
                        std::str::from_utf8(&payload[b"push-option ".len()..payload.len() - 1])
                            .map_err(|_| InputError::Commands)?;
                    certificate_options
                        .as_mut()
                        .ok_or(InputError::Commands)?
                        .push(option.into());
                }
                Certificate::Headers if payload.ends_with(b"\n") => {}
                Certificate::Updates if payload.starts_with(b"-----BEGIN ") => {
                    *state = Certificate::Signature;
                }
                Certificate::Updates if payload.ends_with(b"\n") => {
                    parse_update(&payload[..payload.len() - 1], &mut updates, &mut names)?;
                }
                Certificate::Signature if payload == b"push-cert-end\n" => {
                    *state = Certificate::Done;
                }
                Certificate::Signature if payload.ends_with(b"\n") => {}
                _ => return Err(InputError::Commands),
            }
            continue;
        }
        let payload = payload.strip_suffix(b"\n").unwrap_or(payload);
        if let Some(shallow) = payload.strip_prefix(b"shallow ") {
            if !updates.is_empty() || parse_oid(shallow).is_none() {
                return Err(InputError::Commands);
            }
            continue;
        }
        let payload = if let Some(nul) = payload.iter().position(|byte| *byte == 0) {
            if !updates.is_empty() {
                return Err(InputError::Commands);
            }
            capabilities(
                &payload[nul + 1..],
                &mut report_status,
                &mut sideband,
                &mut options_requested,
            );
            &payload[..nul]
        } else {
            payload
        };
        if payload == b"push-cert" {
            if !updates.is_empty() {
                return Err(InputError::Commands);
            }
            certificate = Some(Certificate::Headers);
            certificate_options = Some(Vec::new());
            certificate_body = Some(Vec::new());
            continue;
        }
        parse_update(payload, &mut updates, &mut names)?;
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Certificate {
    Headers,
    Updates,
    Signature,
    Done,
}

fn capabilities(bytes: &[u8], report_status: &mut bool, sideband: &mut bool, options: &mut bool) {
    for capability in bytes.split(|byte| *byte == b' ') {
        match capability {
            b"report-status" | b"report-status-v2" => *report_status = true,
            b"side-band-64k" => *sideband = true,
            b"push-options" => *options = true,
            _ => {}
        }
    }
}

fn parse_update<'a>(
    payload: &'a [u8],
    updates: &mut Vec<RefUpdate>,
    names: &mut BTreeSet<&'a str>,
) -> Result<(), InputError> {
    if updates.len() == MAX_UPDATES {
        return Err(InputError::TooLarge);
    }
    let mut fields = payload.splitn(3, |byte| *byte == b' ');
    let old = fields
        .next()
        .and_then(parse_oid)
        .ok_or(InputError::Commands)?;
    let new = fields
        .next()
        .and_then(parse_oid)
        .ok_or(InputError::Commands)?;
    if old.format() != new.format() {
        return Err(InputError::Commands);
    }
    let name = std::str::from_utf8(fields.next().ok_or(InputError::Commands)?)
        .map_err(|_| InputError::Commands)?;
    if name.contains(['\0', '\n', '\r']) || !names.insert(name) {
        return Err(InputError::Commands);
    }
    updates.push(RefUpdate {
        name: name.into(),
        expected: (!old.is_zero()).then_some(RefExpectation {
            oid: Some(old),
            version: 1,
        }),
        new_oid: (!new.is_zero()).then_some(new),
    });
    Ok(())
}

pub(super) fn command_flags(bytes: &[u8]) -> Result<(bool, bool), InputError> {
    match commands(bytes)? {
        PushCommands::Parsed {
            options_requested,
            updates,
            ..
        } => Ok((
            options_requested,
            updates.iter().any(|update| update.new_oid.is_some()),
        )),
        PushCommands::OtherMedia | PushCommands::Limited => Ok((false, false)),
    }
}

fn parse_options(mut bytes: &[u8]) -> Result<Vec<String>, ()> {
    let mut options = Vec::new();
    loop {
        let header = bytes.get(..4).ok_or(())?;
        let length = std::str::from_utf8(header)
            .ok()
            .and_then(|value| usize::from_str_radix(value, 16).ok())
            .ok_or(())?;
        if length == 0 {
            return (bytes.len() == 4).then_some(options).ok_or(());
        }
        if !(5..=1028).contains(&length) || options.len() == 16 {
            return Err(());
        }
        let payload = bytes.get(4..length).ok_or(())?;
        if !payload.iter().all(|byte| (0x20..=0x7e).contains(byte)) {
            return Err(());
        }
        options.push(std::str::from_utf8(payload).map_err(|_| ())?.to_owned());
        bytes = bytes.get(length..).ok_or(())?;
    }
}

fn parse_oid(value: &[u8]) -> Option<crate::ObjectId> {
    crate::ObjectId::from_hex(value).ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    const ZERO: &str = "0000000000000000000000000000000000000000";
    fn packet(body: &[u8]) -> Vec<u8> {
        [
            format!("{:04x}", body.len() + 4).into_bytes(),
            body.to_vec(),
        ]
        .concat()
    }
    #[test]
    fn commands_separate_capabilities_and_ignore_pack_bytes() {
        let mut input = packet(
            format!(
                "{ZERO} {} refs/heads/a'b\0report-status side-band-64k push-options\n",
                "12".repeat(20)
            )
            .as_bytes(),
        );
        input.extend_from_slice(b"0000PACKignored");
        let PushCommands::Parsed {
            updates: plan,
            report_status,
            sideband,
            ..
        } = commands(&input).expect("valid commands")
        else {
            panic!("parsed commands");
        };
        assert!(report_status && sideband);
        assert!(command_flags(&input).expect("valid capabilities").0);
        assert_eq!(plan[0].name, "refs/heads/a'b");
        assert_eq!(quote(&plan[0].name), "'refs/heads/a'\\''b'");
        assert_eq!(plan[0].new_oid, Some(crate::ObjectId::Sha1([0x12; 20])));
    }
    #[test]
    fn signed_commands_drive_policy_and_ssh_pack_detection() -> Result<(), InputError> {
        for (old, new, needs_pack) in [
            (ZERO.to_owned(), "12".repeat(20), true),
            ("12".repeat(32), "00".repeat(32), false),
        ] {
            let mut input = packet(b"push-cert\0report-status side-band-64k push-options");
            for line in [
                "certificate version 0.1\n".to_owned(),
                "pusher test@example.invalid 123 +0000\n".to_owned(),
                "nonce 123-abcd\n".to_owned(),
                "push-option canopy.note=signed\n".to_owned(),
                "\n".to_owned(),
                format!("{old} {new} refs/heads/café\n"),
                "-----BEGIN SSH SIGNATURE-----\n".to_owned(),
                "signature-data\n".to_owned(),
                "-----END SSH SIGNATURE-----\n".to_owned(),
                "push-cert-end\n".to_owned(),
            ] {
                input.extend(packet(line.as_bytes()));
            }
            input.extend_from_slice(b"0000PACKignored");
            let PushCommands::Parsed {
                updates,
                report_status,
                sideband,
                options_requested,
                ..
            } = commands(&input)?
            else {
                panic!("signed commands did not parse");
            };
            assert_eq!(updates[0].name, "refs/heads/café");
            assert!(report_status && sideband && options_requested);
            assert_eq!(command_flags(&input)?, (true, needs_pack));
        }
        Ok(())
    }

    #[test]
    fn incomplete_or_mixed_certificate_is_rejected() {
        let mut input = packet(b"push-cert\0report-status");
        input.extend(packet(b"certificate version 0.1\n"));
        input.extend(packet(b"\n"));
        input.extend(packet(
            format!("{ZERO} {} refs/heads/main\n", "12".repeat(20)).as_bytes(),
        ));
        assert!(matches!(
            commands(&[input.as_slice(), b"0000"].concat()),
            Err(InputError::Commands)
        ));
        input.extend(packet(b"-----BEGIN SSH SIGNATURE-----\n"));
        input.extend(packet(b"-----END SSH SIGNATURE-----\n"));
        input.extend(packet(b"push-cert-end\n"));
        input.extend(packet(
            format!("{ZERO} {} refs/heads/extra\n", "12".repeat(20)).as_bytes(),
        ));
        input.extend_from_slice(b"0000");
        assert!(matches!(commands(&input), Err(InputError::Commands)));

        let mut ordinary = packet(format!("{ZERO} {} refs/heads/main", "12".repeat(20)).as_bytes());
        ordinary.extend(packet(b"push-cert"));
        ordinary.extend_from_slice(b"0000");
        assert!(matches!(commands(&ordinary), Err(InputError::Commands)));
    }

    #[test]
    fn signed_options_must_match_the_separate_push_option_group() -> Result<(), InputError> {
        let mut input = packet(b"push-cert\0report-status push-options");
        for line in [
            "certificate version 0.1\n".to_owned(),
            "push-option canopy.note=signed\n".to_owned(),
            "\n".to_owned(),
            format!("{ZERO} {} refs/heads/main\n", "12".repeat(20)),
            "-----BEGIN SSH SIGNATURE-----\n".to_owned(),
            "-----END SSH SIGNATURE-----\n".to_owned(),
            "push-cert-end\n".to_owned(),
        ] {
            input.extend(packet(line.as_bytes()));
        }
        input.extend_from_slice(b"0000");
        let mut parsed = commands(&input)?;
        assert_eq!(
            parsed.option_error(),
            Some("Canopy signed push options do not match the request")
        );
        if let PushCommands::Parsed { options, .. } = &mut parsed {
            options.push("canopy.note=signed".into());
        }
        assert_eq!(parsed.option_error(), None);
        Ok(())
    }
    #[test]
    fn malformed_and_duplicate_commands_cannot_escape_the_hook() {
        for bytes in [b"0001".as_slice(), b"ffff", b"xxxx", b"0008abc", b"0004"] {
            assert!(commands(bytes).is_err());
        }
        let mut input = packet(format!("{ZERO} {} refs/heads/main", "12".repeat(20)).as_bytes());
        input.extend(input.clone());
        input.extend_from_slice(b"0000");
        assert!(matches!(commands(&input), Err(InputError::Commands)));
    }

    #[test]
    fn push_option_packets_preserve_order_and_reject_invalid_frames() {
        let mut input = packet(b"canopy.note=first");
        input.extend(packet(b"canopy.note=second"));
        input.extend_from_slice(b"0000");
        assert_eq!(
            parse_options(&input).expect("valid options"),
            ["canopy.note=first", "canopy.note=second"]
        );
        for invalid in [
            b"0004".as_slice(),
            b"0008abc",
            b"0008a\nb!0000",
            b"0000PACK",
        ] {
            assert!(parse_options(invalid).is_err());
        }
    }
}
