use super::*;
use crate::refs::{MAX_UPDATES, valid_ref_name};

const PREFIX_LIMIT: usize = 256 * 1024;
const ZERO: &str = "0000000000000000000000000000000000000000";

impl GitGateway {
    pub(super) async fn install_branch_policy(
        &self,
        cached: &CachedRepository,
        request: &GitHttpRequest,
    ) -> Result<(), GatewayError> {
        // Let the backend report invalid media types before interpreting a Git
        // command stream; these responses are recorded for exact push replay.
        if request.content_type.as_deref() != Some("application/x-git-receive-pack-request") {
            return Ok(());
        }
        // Unprotected repositories retain native Git's complete error reports.
        // Rules enabled after this observation still gate final Cell publication.
        if !self
            .repository
            .has_branch_rules()
            .await
            .map_err(|source| GatewayError::Cell(Box::new(source)))?
        {
            return Ok(());
        }
        let prefix = request.body.prefix(PREFIX_LIMIT).await?;
        let updates = commands(&prefix)?;
        let mut protected = false;
        let mut script = String::from("#!/bin/sh\ncase \"$1\" in\n");
        for update in updates {
            let policy = self
                .repository
                .branch_policy(&update)
                .await
                .map_err(|source| GatewayError::Cell(Box::new(source)))?;
            let old = update
                .expected
                .as_ref()
                .and_then(|old| old.oid)
                .map_or_else(|| ZERO.into(), hex::encode);
            let new = update.new_oid.map_or_else(|| ZERO.into(), hex::encode);
            script.push_str(&format!(
                "{})\n[ \"$2\" = '{old}' ] && [ \"$3\" = '{new}' ] || exit 1\n",
                quote(&update.name)
            ));
            if let Some(policy) = policy {
                protected = true;
                if !policy.allows(&update, false) {
                    script.push_str(
                        "printf '%s\\n' 'Canopy branch rule rejected this update' >&2\nexit 1\n",
                    );
                } else if policy.fast_forward_only && old != ZERO && new != ZERO {
                    script.push_str("git merge-base --is-ancestor \"$2\" \"$3\" || { printf '%s\\n' 'Canopy branch requires a fast-forward update' >&2; exit 1; }\n");
                }
            }
            script.push_str("exit 0\n;;\n");
        }
        // Only commands decoded from this immutable request may use the hook.
        // Publication rechecks current Cell policy; the hook supplies Git's report.
        script.push_str("*) exit 1 ;;\nesac\n");
        if !protected {
            return Ok(());
        }
        cached
            .backend
            .cache
            .store_update_hook(script.into_bytes())
            .await?;
        Ok(())
    }
}

fn quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\\''"))
}

fn commands(mut bytes: &[u8]) -> Result<Vec<RefUpdate>, InputError> {
    let mut updates = Vec::new();
    loop {
        let header = bytes.get(..4).ok_or(InputError::Commands)?;
        let length = std::str::from_utf8(header)
            .ok()
            .and_then(|s| usize::from_str_radix(s, 16).ok())
            .ok_or(InputError::Commands)?;
        if length == 0 {
            return Ok(updates);
        }
        if !(5..=65520).contains(&length) {
            return Err(InputError::Commands);
        }
        let payload = bytes.get(4..length).ok_or(InputError::Commands)?;
        bytes = &bytes[length..];
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
            &payload[..nul]
        } else {
            payload
        };
        if updates.len() == MAX_UPDATES {
            return Err(InputError::TooLarge);
        }
        if payload.len() < 83 || payload[40] != b' ' || payload[81] != b' ' {
            return Err(InputError::Commands);
        }
        let old = parse_oid(&payload[..40]).ok_or(InputError::Commands)?;
        let new = parse_oid(&payload[41..81]).ok_or(InputError::Commands)?;
        let name = std::str::from_utf8(&payload[82..]).map_err(|_| InputError::Commands)?;
        if !valid_ref_name(name) || updates.iter().any(|update: &RefUpdate| update.name == name) {
            return Err(InputError::Commands);
        }
        updates.push(RefUpdate {
            name: name.into(),
            expected: (old != [0; 20]).then_some(RefExpectation {
                oid: Some(old),
                version: 1,
            }),
            new_oid: (new != [0; 20]).then_some(new),
        });
    }
}

fn parse_oid(value: &[u8]) -> Option<[u8; 20]> {
    if value.len() != 40 || !value.iter().all(u8::is_ascii_hexdigit) {
        return None;
    }
    let mut oid = [0; 20];
    hex::decode_to_slice(value, &mut oid).ok()?;
    Some(oid)
}

#[cfg(test)]
mod tests {
    use super::*;
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
                "{ZERO} {} refs/heads/a'b\0report-status side-band-64k\n",
                "12".repeat(20)
            )
            .as_bytes(),
        );
        input.extend_from_slice(b"0000PACKignored");
        let plan = commands(&input).expect("valid commands");
        assert_eq!(plan[0].name, "refs/heads/a'b");
        assert_eq!(quote(&plan[0].name), "'refs/heads/a'\\''b'");
        assert_eq!(plan[0].new_oid, Some([0x12; 20]));
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
}
