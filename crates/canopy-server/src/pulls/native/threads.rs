//! Verified line anchors are purpose-bound to native ref facts at publication.
use super::*;
use crate::git_read::{ComparisonTarget, patch::LineAnchor};
use crate::pulls::threads::ThreadIntent;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ThreadData {
    pub(crate) number: i64,
    pub(crate) intent: ThreadIntent,
    pub(crate) anchor: LineAnchor,
}
impl ThreadData {
    pub(crate) fn digest(&self) -> Result<[u8; 32], CodecError> {
        let mut encoder = BoundedEncoder::new(INPUT_BYTES)?;
        self.encode(&mut encoder)?;
        Ok(*blake3::hash(&encoder.finish()).as_bytes())
    }
}
impl WireValue for ThreadData {
    fn encode(&self, encoder: &mut BoundedEncoder) -> Result<(), CodecError> {
        let invalid = || CodecError::Invalid("invalid verified thread");
        if !(1..=20_000).contains(&self.intent.line)
            || validate_repository_id(self.intent.id).is_err()
            || self.anchor.revision.pull_version < 1
            || self.anchor.revision.source_version < 1
            || self.anchor.revision.base_version < 1
            || parse_oid(&self.anchor.revision.source_oid).is_none()
            || parse_oid(&self.anchor.revision.base_oid).is_none()
            || match &self.intent.target {
                ComparisonTarget::Current { revision } => revision != &self.anchor.revision,
                ComparisonTarget::Review { number } => *number < 1,
                ComparisonTarget::Merged {} => false,
                ComparisonTarget::Thread { .. } => true,
            }
        {
            return Err(invalid());
        }
        // Reuse the final statement validator for anchor/intent consistency.
        crate::pulls::threads::creation_statements(
            "validated",
            self.number,
            &self.intent,
            &self.anchor,
            0,
        )
        .map_err(|_| invalid())?;
        let bytes = serde_json::to_vec(self).map_err(|_| invalid())?;
        if bytes.len() > 256 << 10 {
            return Err(invalid());
        }
        encoder.write_u8(56)?;
        encoder.write_bytes(&bytes)
    }
    fn decode(decoder: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        if decoder.read_u8()? != 56 {
            return Err(CodecError::Invalid("invalid verified thread"));
        }
        let bytes = decoder.read_bytes()?;
        if bytes.len() > 256 << 10 {
            return Err(CodecError::Invalid("oversized verified thread"));
        }
        let data: Self = serde_json::from_slice(bytes)
            .map_err(|_| CodecError::Invalid("invalid verified thread"))?;
        data.digest()?;
        Ok(data)
    }
}
pub(crate) struct ThreadRequest {
    pub(crate) selection: RefSelection,
    pub(crate) data: ThreadData,
}
impl WireValue for ThreadRequest {
    fn encode(&self, encoder: &mut BoundedEncoder) -> Result<(), CodecError> {
        if self.selection.actor.is_none() {
            return Err(CodecError::Invalid("thread actor missing"));
        }
        self.data.encode(encoder)?;
        self.selection.encode(encoder)
    }
    fn decode(decoder: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        let value = Self {
            data: ThreadData::decode(decoder)?,
            selection: RefSelection::decode(decoder)?,
        };
        value.encode(&mut BoundedEncoder::new(INPUT_BYTES)?)?;
        Ok(value)
    }
}
pub(crate) struct CreateNativeThread;
impl Command for CreateNativeThread {
    const MODULE: &'static str = RepositoryModule::NAME;
    const ID: u32 = 56;
    const CODEC_VERSION: u32 = 1;
    type Input = ThreadRequest;
    type Output = PullChange;
    fn execute(
        context: &mut CommandContext<'_, '_>,
        input: Self::Input,
    ) -> cellule_runtime::Result<CommandResult<Self::Output>> {
        input.encode(&mut BoundedEncoder::new(INPUT_BYTES)?)?;
        if !input.selection.authorized(
            context.target().cell_id(),
            Some(context.owner_fence()),
            context.now_ms(),
            input.data.digest()?,
            |q| context.sql(q),
        )? {
            return denial(context, &input.selection);
        }
        let actor = input
            .selection
            .actor
            .as_deref()
            .ok_or(Error::Command("thread actor missing"))?;
        let statements = crate::pulls::threads::creation_statements(
            actor,
            input.data.number,
            &input.data.intent,
            &input.data.anchor,
            context.now_ms(),
        )?;
        transaction(context, &input.selection, statements)
    }
}
impl RepositoryCell {
    pub(in crate::pulls) async fn native_create_thread(
        &self,
        identity: MutationIdentity,
        actor: &str,
        number: i64,
        intent: ThreadIntent,
        anchor: LineAnchor,
    ) -> Result<Committed<PullChange>, NativePullError> {
        validate_component(actor)?;
        let data = ThreadData {
            number,
            intent,
            anchor,
        };
        let digest = data.digest()?;
        let selected = self
            .sql
            .query(
                None,
                SqlBatch {
                    statements: vec![reads::selector(
                        ReadIdentity::Account(actor),
                        &ReadKind::Detail(number),
                    )],
                },
            )
            .await
            .map_err(|e| NativePullError::Metadata(Box::new(e)))?;
        let rows = &selected
            .output
            .first()
            .ok_or(Error::Command("thread selection missing"))?
            .rows;
        let names = reads::selected_names(rows)?;
        // Keep the admitted snapshot alive until the owner transaction resolves.
        let (_snapshot, selection) = self.pull_ref_selection(actor, digest, &names).await?;
        match self
            .application
            .command::<CreateNativeThread>(
                &self.target,
                identity,
                ThreadRequest { selection, data },
            )
            .await
        {
            Ok(v) => Ok(v),
            Err(InvocationError::Rejected(v)) => Ok(*v),
            Err(e) => Err(NativePullError::Command(Box::new(e))),
        }
    }
}
