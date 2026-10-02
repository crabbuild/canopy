//! An owned handoff from authenticated encoded input to parsed native input.
//! Wire commands remain intent, not ref versions or a signature witness.
use super::*;
use crate::packs::publication::{BeginRequest, DEFAULT_LEASE_MS};
use cellule_runtime::CellTarget;

/// Construct only after the gateway's account/access authentication. Ownership
/// keeps hashing, gzip expansion and packet parsing on one request spool.
#[must_use]
pub(super) struct EncodedPush {
    request: GitHttpRequest,
    identity: BeginRequest,
    format: crate::ObjectFormat,
}
#[must_use]
pub(super) struct PushPreflight(PushParts);
pub(super) struct PushParts {
    pub request: GitHttpRequest,
    pub commands: branch_policy::PushCommands,
    pub identity: BeginRequest,
}
impl EncodedPush {
    pub(super) async fn new(
        request: GitHttpRequest,
        target: &CellTarget,
        repository: [u8; 16],
        format: crate::ObjectFormat,
        actor: &str,
        operation: [u8; 16],
    ) -> Result<Self, GatewayError> {
        if !request.authenticated {
            return Err(GatewayError::Unauthorized);
        }
        if request.method != "POST"
            || request.path_info != "/repo.git/git-receive-pack"
            || crate::directory::validate_component(actor).is_err()
            || crate::repository_target(target.tenant(), target.application(), repository)
                .map_err(|_| GatewayError::MalformedCache)?
                != *target
        {
            return Err(GatewayError::MalformedCache);
        }
        // Bind the exact encoded request, including compressed representation,
        // to its server context. Native Git will read expanded bytes later.
        let mut hash = blake3::Hasher::new();
        hash.update(b"canopy.git.push-request.v3\0");
        for bytes in [
            target.tenant().as_bytes().as_slice(),
            target.application().as_bytes().as_slice(),
            target.namespace().as_bytes().as_slice(),
            target.partition(),
            repository.as_slice(),
            operation.as_slice(),
            actor.as_bytes(),
        ] {
            field(&mut hash, bytes);
        }
        hash.update(&[
            format.bytes() as u8,
            u8::from(request.protocol_v2),
            u8::from(request.content_type.is_some()),
            u8::from(request.gzip),
        ]);
        for bytes in [
            request.method.as_bytes(),
            request.path_info.as_bytes(),
            request.query.as_bytes(),
            request
                .content_type
                .as_deref()
                .unwrap_or_default()
                .as_bytes(),
        ] {
            field(&mut hash, bytes);
        }
        let request_digest = request.body.digest(hash).await?;
        Ok(Self {
            request,
            format,
            identity: BeginRequest {
                repository,
                operation,
                request_digest,
                actor: actor.into(),
                lease_ms: DEFAULT_LEASE_MS,
            },
        })
    }
    pub(super) fn identity(&self) -> &BeginRequest {
        &self.identity
    }
    /// Completed-request replay uses identity before this step. Consume the
    /// encoded owner once, then keep normalized input and parsed intent together.
    pub(super) async fn decode(
        self,
        root: &Path,
        budget: &DiskBudget,
        limit: Option<u64>,
    ) -> Result<PushPreflight, GatewayError> {
        let mut request = self.request;
        if request.gzip {
            request.body = request.body.decode_gzip(root, budget, limit).await?;
            request.gzip = false;
        }
        let commands = branch_policy::PushCommands::read(&request, self.format).await?;
        Ok(PushPreflight(PushParts {
            request,
            commands,
            identity: self.identity,
        }))
    }
}
impl PushPreflight {
    pub(super) fn into_parts(self) -> PushParts {
        self.0
    }
}
fn field(hash: &mut blake3::Hasher, bytes: &[u8]) {
    hash.update(&(bytes.len() as u64).to_le_bytes());
    hash.update(bytes);
}

#[cfg(test)]
mod tests;
