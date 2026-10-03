use super::super::sql::*;
use super::*;
use canopy_object_storage::artifact::ArtifactRead;
use cellule_runtime::{CellClient, CellTarget, InvocationError, Receipt};

pub(in crate::packs::publication) const SAVED: &str = "SELECT actor,request_digest,response_id,completion_digest,rejected,publication,response_root FROM pushes WHERE id=?1";
pub(in crate::packs::publication) fn saved(
    sets: &[SqlResultSet],
    actor: &str,
    digest: [u8; 32],
) -> cellule_runtime::Result<Option<CompletedRootPush>> {
    let Some(row) = rows(sets)?.first() else {
        return Ok(None);
    };
    let [
        SqlValue::Text(original),
        request,
        response,
        completion,
        SqlValue::Integer(rejected),
        publication,
        SqlValue::Blob(root),
    ] = row.as_slice()
    else {
        return Ok(None);
    };
    if original != actor || fixed::<32>(request)? != digest {
        return Ok(None);
    }
    fixed::<32>(completion)?;
    if ![0, 1].contains(rejected) {
        return Err(Error::Command("invalid root completion rejection"));
    }
    let publication = match publication {
        SqlValue::Null => None,
        SqlValue::Blob(bytes) => {
            let mut d = BoundedDecoder::new(bytes, 128)?;
            let value = PublishedRefs::decode(&mut d)?;
            d.finish()?;
            Some(value)
        }
        _ => return Err(Error::Command("invalid root completion publication")),
    };
    let mut d = BoundedDecoder::new(root, 128)?;
    let root = NativeOutcomeRoot::decode(&mut d)?;
    d.finish()?;
    let value = CompletedRootPush {
        completion: CompletedCatalogPush {
            response_id: fixed(response)?,
            rejected: *rejected == 1,
            publication,
        },
        root,
    };
    RootCompletionReply::Completed(Box::new(value)).encode(&mut BoundedEncoder::new(512)?)?;
    Ok(Some(value))
}
pub struct CheckCompletedRootPush;
impl Query for CheckCompletedRootPush {
    const MODULE: &'static str = RepositoryModule::NAME;
    const ID: u32 = 37;
    const CODEC_VERSION: u32 = 1;
    type Input = BeginRequest;
    type Output = Option<RootCompletionReply>;
    fn execute(
        context: &mut QueryContext<'_>,
        input: Self::Input,
    ) -> cellule_runtime::Result<Self::Output> {
        validate_component(&input.actor)?;
        if !decode_access(&context.sql(&SqlBatch {
            statements: vec![access_statement(&input.actor)],
        })?)?
        .is_some_and(|role| role >= TokenScope::Read)
            || identity(
                &context.sql(&statement(IDENTITY, vec![]))?,
                input.repository,
            )?
            .is_none()
        {
            return Ok(Some(RootCompletionReply::Denied(
                PreparationDenial::Unauthorized,
            )));
        }
        let sets = context.sql(&statement(SAVED, vec![blob(input.operation)]))?;
        if rows(&sets)?.is_empty() {
            return Ok(None);
        }
        Ok(Some(
            match saved(&sets, &input.actor, input.request_digest)? {
                Some(value) => RootCompletionReply::Completed(Box::new(value)),
                None => RootCompletionReply::Denied(PreparationDenial::Conflict),
            },
        ))
    }
}
#[derive(Debug, thiserror::Error)]
pub enum RootPushReplayError {
    #[error("completed root response target or storage context differs")]
    Context,
    #[error("completed root response lookup denied: {0:?}")]
    Denied(PreparationDenial),
    #[error("completed root response target failed")]
    Target(#[from] Error),
    #[error("completed root response lookup failed")]
    Query(#[source] Box<InvocationError<Option<RootCompletionReply>>>),
    #[error("completed root response metadata failed")]
    Metadata(#[from] crate::packs::InputRootError),
    #[error("completed root response body failed")]
    Artifact(#[from] canopy_object_storage::artifact::ArtifactError),
}
/// Current authorized durable selection, followed by authenticated streaming.
/// Caller roots/receipts/decoded reply DTOs cannot grant response read access.
pub async fn replay_root_push_response(
    client: &CellClient,
    target: &CellTarget,
    request: BeginRequest,
    minimum: Option<Receipt>,
    store: &ArtifactStore,
) -> Result<Option<GitHttpResponse<ArtifactRead>>, RootPushReplayError> {
    if store.repository() != request.repository
        || target
            != &crate::repository_target(target.tenant(), target.application(), request.repository)?
    {
        return Err(RootPushReplayError::Context);
    }
    let found = client
        .query::<CheckCompletedRootPush>(target, minimum, request)
        .await
        .map_err(|error| RootPushReplayError::Query(Box::new(error)))?;
    let value = match found.output {
        None => return Ok(None),
        Some(RootCompletionReply::Denied(reason)) => {
            return Err(RootPushReplayError::Denied(reason));
        }
        Some(RootCompletionReply::Completed(value)) => value,
    };
    let record: OutcomeRecord = value.root.0.read(store, INPUT_ROOT_BYTES).await?;
    let body = store
        .read(
            native_result::body_key(record.body_operation, record.response.body),
            record.response.body,
        )
        .await?;
    Ok(Some(GitHttpResponse {
        status: record.response.status,
        headers: record.response.headers,
        body,
    }))
}
