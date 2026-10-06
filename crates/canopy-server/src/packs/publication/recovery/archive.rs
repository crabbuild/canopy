//! A closed attempt transfers the same recovery certificate/journal into its
//! immutable shared receipt row before releasing its independent preparation pin.
use super::*;

const DOMAIN: &[u8] = b"canopy.terminal-recovery-release.v2\0";
const RELEASE_BYTES: u32 = 4096;
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TerminalReleaseCertificate(CertificateEnvelope);
#[derive(Clone, Debug, PartialEq, Eq)]
struct Proof {
    recovery: RootRecoveryCertificate,
    phase: [u8; 32],
    graph: [u8; 32],
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TerminalReleaseInput {
    pub maintenance: MaintenanceRequest,
    pub certificate: TerminalReleaseCertificate,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TerminalReleaseReply {
    Released,
    Denied(PreparationDenial),
}

pub(super) fn encoded<T: WireValue>(value: &T, limit: u32) -> Result<Vec<u8>, CodecError> {
    let mut e = BoundedEncoder::new(limit)?;
    value.encode(&mut e)?;
    Ok(e.finish())
}
pub(in crate::packs::publication) fn descriptor(
    hash: &mut blake3::Hasher,
    operation: [u8; 16],
    kind: ArtifactKind,
    value: ArtifactDescriptor,
) -> Result<(), RootRecoveryError> {
    let mut e = BoundedEncoder::new(256)?;
    e.write_bytes(&operation)?;
    // Stable declared purpose, never a Rust enum discriminant.
    e.write_text(match kind {
        ArtifactKind::InputRoot => "input-root",
        ArtifactKind::InputBody => "input-body",
        ArtifactKind::CatalogNode => "catalog-node",
        _ => return Err(RootRecoveryError::Context),
    })?;
    artifact(&mut e, value)?;
    hash.update(&e.finish());
    Ok(())
}
impl Proof {
    fn record(&self) -> Result<Record, CodecError> {
        self.recovery.0.data()
    }
}
impl WireValue for Proof {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        self.record()?;
        if self.graph == [0; 32] {
            return Err(CodecError::Invalid("closed recovery graph"));
        }
        e.write_bytes(DOMAIN)?;
        self.recovery.encode(e)?;
        e.write_bytes(&self.phase)?;
        e.write_bytes(&self.graph)
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        if d.read_bytes()? != DOMAIN {
            return Err(CodecError::Invalid("terminal release purpose"));
        }
        let value = Self {
            recovery: RootRecoveryCertificate::decode(d)?,
            phase: wire_fixed(d)?,
            graph: wire_fixed(d)?,
        };
        value.encode(&mut BoundedEncoder::new(CERTIFICATE_BYTES)?)?;
        Ok(value)
    }
}
impl WireValue for TerminalReleaseCertificate {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        self.0.data::<Proof>()?;
        self.0.encode(e)
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        let value = Self(CertificateEnvelope::decode(d)?);
        value.0.data::<Proof>()?;
        Ok(value)
    }
}
impl WireValue for TerminalReleaseInput {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        self.maintenance.encode(e)?;
        self.certificate.encode(e)
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        Ok(Self {
            maintenance: MaintenanceRequest::decode(d)?,
            certificate: TerminalReleaseCertificate::decode(d)?,
        })
    }
}
impl WireValue for TerminalReleaseReply {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        match self {
            Self::Released => e.write_u8(0),
            Self::Denied(reason) => PreparationReply::Denied(*reason).encode(e),
        }
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        let tag = d.read_u8()?;
        if tag == 0 {
            return Ok(Self::Released);
        }
        let bytes = [tag];
        let mut denied = BoundedDecoder::new(&bytes, 1)?;
        match PreparationReply::decode(&mut denied)? {
            PreparationReply::Denied(reason) => Ok(Self::Denied(reason)),
            _ => Err(CodecError::Invalid("terminal release reply")),
        }
    }
}

pub(super) enum Terminal {
    Candidate(CandidatePublicationReply),
    Push(Box<CompletedRootPush>),
    Initialization(InitializationReply),
    Merge(crate::pulls::merge::MergeOutcome),
    Head(PublicationReply),
}
impl Terminal {
    fn selected_statement(&self, operation: [u8; 16]) -> SqlStatement {
        SqlStatement {
            sql: match self {
                Self::Push(_) => super::super::root_completion::read::SAVED,
                Self::Initialization(_) => super::super::initialization::publish::SAVED,
                Self::Candidate(reply) => {
                    return super::super::candidate_publication::audit::statement(reply);
                }
                Self::Head(_) => super::super::native_head::publish::SAVED,
                Self::Merge(outcome) => {
                    return super::super::native_merge::audit::statement(outcome);
                }
            }
            .into(),
            parameters: vec![blob(operation)],
        }
    }
    fn matches_selected(&self, result: &[SqlResultSet], check: &LeaseCheck) -> Result<bool, Error> {
        Ok(match self {
            Self::Push(terminal) => {
                super::super::root_completion::read::saved(
                    result,
                    &check.actor,
                    check.token.request_digest,
                )? == Some(**terminal)
            }
            Self::Initialization(InitializationReply::Initialized(fact)) => {
                super::super::initialization::publish::selected(result, check)? == Some(**fact)
            }
            // A known negative is the original phase knowledge. A later attempt
            // may initialize this logical operation, without rewriting that denial.
            Self::Initialization(InitializationReply::Denied(_)) => true,
            Self::Candidate(reply) => {
                !reply.applied()
                    || super::super::candidate_publication::audit::selected(
                        result,
                        reply,
                        &check.actor,
                    )?
                    .is_some()
            }
            Self::Head(reply) => {
                matches!(reply, PublicationReply::Denied(_))
                    || super::super::native_head::publish::selected(result, check, *reply)?
                        .is_some()
            }
            Self::Merge(outcome) => {
                !matches!(outcome, crate::pulls::merge::MergeOutcome::Applied { .. })
                    || super::super::native_merge::audit::selected(result, outcome, &check.actor)?
                        .is_some()
            }
        })
    }
    async fn closed_graph(
        &self,
        store: &ArtifactStore,
        selected: &[SqlResultSet],
        check: &LeaseCheck,
        hash: &mut blake3::Hasher,
    ) -> Result<(), RootRecoveryError> {
        match self {
            Self::Candidate(reply) => {
                hash.update(&encoded(reply, 512)?);
                super::super::candidate_publication::audit::closed_graph(
                    store, selected, reply, check, hash,
                )
                .await?;
            }
            Self::Head(reply) => {
                hash.update(&encoded(reply, 512)?);
                if let PublicationReply::Published(published) = reply {
                    let (request, fact) =
                        super::super::native_head::publish::selected(selected, check, *reply)?
                            .ok_or(RootRecoveryError::Context)?;
                    let catalog = fact.catalog.ok_or(RootRecoveryError::Context)?;
                    let snapshot =
                        crate::packs::catalog::CatalogSnapshot::download(store, catalog).await?;
                    crate::packs::directory::snapshot::DirectorySnapshot::download(
                        store,
                        snapshot.directory,
                    )
                    .await?;
                    let refs = fact.refs.ok_or(RootRecoveryError::Context)?;
                    let state = refs
                        .read(store)
                        .await
                        .map_err(super::super::RefSnapshotPreparationError::from)?;
                    if state.repository != check.token.repository
                        || state.format != catalog.format
                        || state.generation != published.ref_generation
                        || state.default_branch != request.reference
                    {
                        return Err(RootRecoveryError::Context);
                    }
                    descriptor(
                        hash,
                        catalog.operation,
                        ArtifactKind::CatalogNode,
                        catalog.artifact,
                    )?;
                    descriptor(
                        hash,
                        snapshot.directory.operation,
                        ArtifactKind::CatalogNode,
                        snapshot.directory.artifact,
                    )?;
                    descriptor(
                        hash,
                        refs.operation(),
                        ArtifactKind::InputRoot,
                        refs.artifact(),
                    )?;
                }
            }
            Self::Merge(outcome) => {
                hash.update(&encoded(outcome, 512)?);
                if let Some(root) =
                    super::super::native_merge::audit::selected(selected, outcome, &check.actor)?
                {
                    super::super::native_merge::audit::closed_graph(
                        store, root, outcome, check, hash,
                    )
                    .await?;
                } else if matches!(outcome, crate::pulls::merge::MergeOutcome::Applied { .. }) {
                    return Err(RootRecoveryError::Context);
                }
            }
            Self::Push(terminal) => {
                super::super::root_completion::closed_graph(store, terminal.root, hash).await?
            }
            Self::Initialization(reply) => {
                hash.update(&encoded(reply, 512)?);
                if let InitializationReply::Initialized(fact) = reply {
                    let format = fact.catalog.ok_or(RootRecoveryError::Context)?.format;
                    let (catalog, directory, refs) =
                        super::super::initialization::verify_empty(**fact, store, format).await?;
                    descriptor(
                        hash,
                        catalog.operation,
                        ArtifactKind::CatalogNode,
                        catalog.artifact,
                    )?;
                    descriptor(
                        hash,
                        directory.operation,
                        ArtifactKind::CatalogNode,
                        directory.artifact,
                    )?;
                    descriptor(
                        hash,
                        refs.operation(),
                        ArtifactKind::InputRoot,
                        refs.artifact(),
                    )?;
                }
            }
        }
        Ok(())
    }
}
impl phase::Journal {
    pub(super) fn terminal(&self, record: &Record) -> Result<Option<Terminal>, CodecError> {
        // Validation is required even when only a primary result is selected.
        self.may_advance(record)?;
        // A merge has its own typed permanent audit selection. Known denials
        // retain their original phase even if a later UUID attempt succeeds.
        if record.kind == Kind::Candidate {
            return self
                .primary
                .as_ref()
                .map(|v| {
                    v.decode_reply::<CandidatePublicationReply>()
                        .map(Terminal::Candidate)
                })
                .transpose();
        }
        if record.kind == Kind::Head {
            return self
                .primary
                .as_ref()
                .map(|v| v.decode_reply::<PublicationReply>().map(Terminal::Head))
                .transpose();
        }
        if record.kind == Kind::Merge {
            return self
                .primary
                .as_ref()
                .map(|value| {
                    value
                        .decode_reply::<crate::pulls::merge::MergeOutcome>()
                        .map(Terminal::Merge)
                })
                .transpose();
        }
        if record.kind == Kind::Initialization {
            return self
                .primary
                .as_ref()
                .map(|value| {
                    value
                        .decode_reply::<InitializationReply>()
                        .map(Terminal::Initialization)
                })
                .transpose();
        }
        let result = if record.kind == Kind::Policy {
            if !self.refused(record)? {
                return Ok(None);
            }
            self.refusal.as_ref()
        } else {
            self.primary.as_ref()
        };
        match result
            .map(|value| value.decode_reply::<RootCompletionReply>())
            .transpose()?
        {
            Some(RootCompletionReply::Completed(value)) => Ok(Some(Terminal::Push(value))),
            _ => Ok(None),
        }
    }
}
impl RegisteredRootRecovery {
    pub(super) async fn attempt_closed(
        &self,
        client: &CellClient,
    ) -> Result<bool, RootRecoveryError> {
        let sql =
            SqlCell::<RepositoryModule>::new(client.clone(), self.evidence().target().clone())?;
        let result = sql.query(None, statement(
            "SELECT 1 FROM catalog_operations WHERE incarnation=?1 AND admission_sequence=?2 LIMIT 1",
            vec![blob(self.token().owner.incarnation.as_bytes()), number(self.token().attempt)?],
        )).await.map_err(|error| RootRecoveryError::Query(Box::new(error)))?;
        Ok(rows(&result.output)?.is_empty())
    }
    /// Complete typed header/history and selected audit verification precedes
    /// minting the release proof. A historical/intermediate/unknown capability
    /// cannot release a pin. This proof authorizes no provider deletion.
    pub async fn ready_terminal_release(
        &self,
        client: CellClient,
        store: &ArtifactStore,
        maintenance: MaintenanceRequest,
        identity: MutationIdentity,
    ) -> Result<ReadyTerminalRelease, RootRecoveryError> {
        target_matches(self.evidence().target(), store, &self.record.check)?;
        if maintenance.repository != self.token().repository {
            return Err(RootRecoveryError::Context);
        }
        let journal = self.current_journal(&client, None).await?;
        let terminal = journal
            .terminal(&self.record)?
            .ok_or(RootRecoveryError::Context)?;
        if !self.attempt_closed(&client).await? {
            return Err(RootRecoveryError::Context);
        }
        let sql =
            SqlCell::<RepositoryModule>::new(client.clone(), self.evidence().target().clone())?;
        let row = sql
            .query(
                None,
                SqlBatch {
                    statements: vec![
                        terminal.selected_statement(self.token().operation),
                        SqlStatement {
                            sql: "SELECT push_cert_seed FROM repository_identity WHERE singleton=1"
                                .into(),
                            parameters: vec![],
                        },
                        SqlStatement {
                            sql: "SELECT recovery,recovery_phase FROM catalog_leases WHERE incarnation=?1 AND admission_sequence=?2".into(),
                            parameters: vec![blob(self.token().owner.incarnation.as_bytes()), number(self.token().attempt)?],
                        },
                    ],
                },
            )
            .await
            .map_err(|error| RootRecoveryError::Query(Box::new(error)))?;
        if !terminal.matches_selected(&row.output, &self.record.check)? {
            return Err(RootRecoveryError::Context);
        }
        let seed = super::super::attestation::seed(
            row.output.get(1..).ok_or(RootRecoveryError::Context)?,
        )?;
        let Some([SqlValue::Blob(certificate), SqlValue::Blob(saved)]) =
            rows(row.output.get(2..).ok_or(RootRecoveryError::Context)?)?
                .first()
                .map(Vec::as_slice)
        else {
            return Err(RootRecoveryError::Context);
        };
        if phase::certificate(certificate)? != self.certificate
            || phase::journal(&SqlValue::Blob(saved.clone()), &self.record)? != journal
        {
            return Err(RootRecoveryError::Context);
        }
        let mut hash = blake3::Hasher::new();
        hash.update(DOMAIN);
        let mut record = self.record.clone();
        loop {
            let bundle = record.root.read::<Bundle>(store, ROOT_BYTES).await?;
            validate_bundle(&bundle, &record, self.evidence().target())?;
            descriptor(
                &mut hash,
                record.root.operation,
                ArtifactKind::InputRoot,
                record.root.artifact,
            )?;
            let Some(previous) = record.previous else {
                break;
            };
            let frame = previous.read::<phase::Frame>(store, ROOT_BYTES).await?;
            if !frame.certificate.0.authenticated(&seed) {
                return Err(RootRecoveryError::Context);
            }
            let next = frame.certificate.0.data::<Record>()?;
            if next.check != self.record.check
                || next.tenant != self.record.tenant
                || next.application != self.record.application
                || next.step + 1 != record.step
            {
                return Err(RootRecoveryError::Context);
            }
            descriptor(
                &mut hash,
                previous.operation,
                ArtifactKind::InputRoot,
                previous.artifact,
            )?;
            record = next;
        }
        terminal
            .closed_graph(store, &row.output, &self.record.check, &mut hash)
            .await?;
        let proof = Proof {
            recovery: self.certificate.clone(),
            phase: *blake3::hash(&encoded(&journal, 2048)?).as_bytes(),
            graph: *hash.finalize().as_bytes(),
        };
        let input = TerminalReleaseInput {
            maintenance: maintenance.clone(),
            certificate: TerminalReleaseCertificate(CertificateEnvelope::seal(&proof, &seed)?),
        };
        input.encode(&mut BoundedEncoder::new(RELEASE_BYTES)?)?;
        let command = client
            .prepare_command::<ReleaseTerminalRecovery>(self.evidence().target(), identity, input)
            .await
            .map_err(|error| RootRecoveryError::Release(Box::new(error)))?;
        // Only the authenticated private maintenance factory derives this
        // account key. It is not a live PreparationSession/lease capability.
        let check = LeaseCheck {
            token: self.token(),
            actor: maintenance.actor,
        };
        Ok(ReadyTerminalRelease {
            command,
            client,
            target: self.evidence().target().clone(),
            check,
        })
    }
}
fn validate_bundle(
    bundle: &Bundle,
    record: &Record,
    target: &CellTarget,
) -> Result<(), RootRecoveryError> {
    if bundle.kind != record.kind
        || Stamp::of(bundle.primary.snapshot.evidence()) != record.primary
        || bundle
            .refusal
            .as_ref()
            .map(|saved| Stamp::of(saved.snapshot.evidence()))
            != record.refusal
        || std::iter::once(&bundle.primary)
            .chain(bundle.refusal.iter())
            .any(|saved| {
                saved.snapshot.evidence().target() != target
                    || saved.snapshot.evidence().incarnation()
                        != record.check.token.owner.incarnation
            })
    {
        return Err(RootRecoveryError::Context);
    }
    Ok(())
}
#[derive(Clone)]
#[must_use]
pub struct ReadyTerminalRelease {
    command: PreparedCommand<ReleaseTerminalRecovery>,
    client: CellClient,
    target: CellTarget,
    check: LeaseCheck,
}

// The release receipt uses the existing recorded-reply representation. Its
// admitted identity is independent of the original foreground command's owner.
#[derive(Clone)]
struct ReleaseRecord {
    incarnation: IncarnationId,
    stamp: Stamp,
    result: phase::Recorded,
}
impl WireValue for ReleaseRecord {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        if self.result.rejected()
            || self.result.decode_reply::<TerminalReleaseReply>()? != TerminalReleaseReply::Released
        {
            return Err(CodecError::Invalid("invalid terminal release result"));
        }
        e.write_bytes(DOMAIN)?;
        e.write_bytes(self.incarnation.as_bytes())?;
        self.stamp.encode(e)?;
        self.result.encode(e)
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        if d.read_bytes()? != DOMAIN {
            return Err(CodecError::Invalid("release receipt purpose"));
        }
        let value = Self {
            incarnation: IncarnationId::from_bytes(wire_fixed(d)?),
            stamp: Stamp::decode(d)?,
            result: phase::Recorded::decode(d)?,
        };
        value.encode(&mut BoundedEncoder::new(1024)?)?;
        Ok(value)
    }
}
pub struct ReleaseTerminalRecovery;
impl Command for ReleaseTerminalRecovery {
    const MODULE: &'static str = RepositoryModule::NAME;
    const ID: u32 = 40;
    const CODEC_VERSION: u32 = 2;
    type Input = TerminalReleaseInput;
    type Output = TerminalReleaseReply;
    fn execute(
        context: &mut CommandContext<'_, '_>,
        input: Self::Input,
    ) -> cellule_runtime::Result<CommandResult<Self::Output>> {
        let deny = |reason| {
            Ok(CommandResult::Rejected(TerminalReleaseReply::Denied(
                reason,
            )))
        };
        let proof = input.certificate.0.data::<Proof>()?;
        let record = proof.record()?;
        let seed = super::super::attestation::seed(&context.sql(&statement(
            "SELECT push_cert_seed FROM repository_identity WHERE singleton=1",
            vec![],
        ))?)?;
        if !input.certificate.0.authenticated(&seed)
            || !proof.recovery.0.authenticated(&seed)
            || record.tenant != *context.target().tenant().as_bytes()
            || record.application != *context.target().application().as_bytes()
            || record.check.token.repository != input.maintenance.repository
        {
            return deny(PreparationDenial::Unauthorized);
        }
        if context.owner_fence() != input.maintenance.owner
            || super::super::commands::authorized(
                context,
                input.maintenance.repository,
                &input.maintenance.actor,
                TokenScope::Admin,
            )?
            .is_none()
        {
            return deny(PreparationDenial::Unauthorized);
        }
        let pin=context.sql(&statement("SELECT recovery,recovery_phase FROM catalog_leases WHERE incarnation=?1 AND admission_sequence=?2 AND operation=?3 AND owner_epoch=?4 AND artifact_operation=?5",vec![blob(record.check.token.owner.incarnation.as_bytes()),number(record.check.token.attempt)?,blob(record.check.token.operation),blob(record.check.token.owner.epoch.to_be_bytes()),blob(record.check.token.artifact_operation)]))?;
        let Some([SqlValue::Blob(certificate), SqlValue::Blob(saved)]) =
            rows(&pin)?.first().map(Vec::as_slice)
        else {
            return deny(PreparationDenial::Missing);
        };
        if phase::certificate(certificate)? != proof.recovery
            || *blake3::hash(saved).as_bytes() != proof.phase
        {
            return deny(PreparationDenial::Conflict);
        }
        let terminal =
            phase::journal(&SqlValue::Blob(saved.clone()), &record)?.terminal(&record)?;
        let Some(terminal) = terminal else {
            return deny(PreparationDenial::Conflict);
        };
        let selected = context.sql(&SqlBatch {
            statements: vec![terminal.selected_statement(record.check.token.operation)],
        })?;
        if !terminal.matches_selected(&selected, &record.check)? {
            return deny(PreparationDenial::Conflict);
        }
        // Closing an older denied initialization must not delete the currently
        // claimed attempt of the same logical operation. The exact pin is the
        // authority boundary; a successor's independent namespace remains live.
        let active = context.sql(&statement(
            "SELECT 1 FROM catalog_operations WHERE incarnation=?1 AND admission_sequence=?2 LIMIT 1",
            vec![blob(record.check.token.owner.incarnation.as_bytes()), number(record.check.token.attempt)?],
        ))?;
        if !rows(&active)?.is_empty() {
            return deny(PreparationDenial::Conflict);
        }
        let evidence = context
            .mutation_evidence()
            .ok_or(Error::Command("release lacks admitted mutation evidence"))?;
        let reply = TerminalReleaseReply::Released;
        let released = ReleaseRecord {
            incarnation: evidence.incarnation(),
            stamp: Stamp::of(&evidence),
            result: phase::Recorded::new(context.sequence(), false, encoded(&reply, 128)?)?,
        };
        let changed=context.sql(&statement("INSERT INTO catalog_recovery_receipts(incarnation,admission_sequence,operation,recovery,recovery_phase,recovery_release) VALUES(?1,?2,?3,?4,?5,?6)",vec![blob(record.check.token.owner.incarnation.as_bytes()),number(record.check.token.attempt)?,blob(record.check.token.operation),blob(certificate),blob(saved),blob(encoded(&released,1024)?)]))?;
        if changed.first().is_none_or(|set| set.rows_affected != 1) {
            return Err(Error::Command("terminal archive CAS failed"));
        }
        let deleted=context.sql(&statement("DELETE FROM catalog_leases WHERE incarnation=?1 AND admission_sequence=?2 AND recovery=?3 AND recovery_phase=?4",vec![blob(record.check.token.owner.incarnation.as_bytes()),number(record.check.token.attempt)?,blob(certificate),blob(saved)]))?;
        if deleted.first().is_none_or(|set| set.rows_affected != 1) {
            return Err(Error::Command("terminal pin release CAS failed"));
        }
        Ok(CommandResult::Success(reply))
    }
}

impl RegisteredRootRecovery {
    pub(super) async fn load_archive(
        client: &CellClient,
        target: &CellTarget,
        store: &ArtifactStore,
        check: &LeaseCheck,
    ) -> Result<Option<Self>, RootRecoveryError> {
        let sql = SqlCell::<RepositoryModule>::new(client.clone(), target.clone())?;
        let result=sql.query(None,SqlBatch{statements:vec![
            SqlStatement{sql:"SELECT recovery,recovery_phase FROM catalog_recovery_receipts WHERE incarnation=?1 AND admission_sequence=?2 AND operation=?3".into(),parameters:vec![blob(check.token.owner.incarnation.as_bytes()),number(check.token.attempt)?,blob(check.token.operation)]},
            SqlStatement{sql:"SELECT push_cert_seed FROM repository_identity WHERE singleton=1 AND repository_id=?1".into(),parameters:vec![blob(check.token.repository)]},
        ]}).await.map_err(|error|RootRecoveryError::Query(Box::new(error)))?;
        let Some([SqlValue::Blob(bytes), saved]) = rows(&result.output)?.first().map(Vec::as_slice)
        else {
            return Ok(None);
        };
        let certificate = phase::certificate(bytes)?;
        let seed = super::super::attestation::seed(
            result.output.get(1..).ok_or(RootRecoveryError::Context)?,
        )?;
        if !certificate.0.authenticated(&seed) {
            return Err(RootRecoveryError::Context);
        }
        let record = certificate.0.data::<Record>()?;
        if record.check != *check
            || record.tenant != *target.tenant().as_bytes()
            || record.application != *target.application().as_bytes()
        {
            return Err(RootRecoveryError::Context);
        }
        let terminal = phase::journal(saved, &record)?
            .terminal(&record)?
            .ok_or(RootRecoveryError::Context)?;
        let selected = sql
            .query(
                None,
                SqlBatch {
                    statements: vec![terminal.selected_statement(check.token.operation)],
                },
            )
            .await
            .map_err(|error| RootRecoveryError::Query(Box::new(error)))?;
        if !terminal.matches_selected(&selected.output, check)? {
            return Err(RootRecoveryError::Context);
        }
        let bundle = record.root.read::<Bundle>(store, ROOT_BYTES).await?;
        validate_bundle(&bundle, &record, target)?;
        Ok(Some(Self {
            record,
            bundle,
            certificate,
        }))
    }
}
impl ReadyTerminalRelease {
    /// The repository's tracked cold-transition task retains this command
    /// through cancellation, just as it retains the initial publication.
    pub(crate) async fn complete(
        self,
    ) -> Result<Committed<TerminalReleaseReply>, PublicationError> {
        let evidence = Box::new(self.command.evidence().clone());
        let PublicationOutcome::TerminalRelease(result) = self.dispatch(false, 0).await? else {
            return Err(PublicationError::Recovery {
                evidence,
                source: Box::new(RootRecoveryError::Context),
            });
        };
        Ok(result)
    }
    #[cfg(test)]
    pub(in crate::packs::publication) fn with_client_for_test(
        mut self,
        client: CellClient,
    ) -> Self {
        self.client = client;
        self
    }
    #[cfg(test)]
    pub(in crate::packs::publication) fn evidence_for_test(&self) -> PendingMutation {
        self.command.evidence().clone()
    }
    pub(in crate::packs::publication) fn capability(
        &self,
    ) -> (&CellClient, &CellTarget, &LeaseCheck) {
        (&self.client, &self.target, &self.check)
    }
    pub(in crate::packs::publication) fn pending(&self) -> PublicationError {
        PublicationError::TerminalRelease(InvocationError::Pending(Box::new(
            self.command.evidence().clone(),
        )))
    }
    async fn recorded(&self) -> Result<Option<Committed<TerminalReleaseReply>>, RootRecoveryError> {
        // Owner SQL queries refuse PendingPublication until the logical head
        // has an SDK durability proof. Local transaction presence alone cannot
        // turn this application journal into an acknowledged receipt.
        let sql = SqlCell::<RepositoryModule>::new(self.client.clone(), self.target.clone())?;
        let query = sql
            .query(
                None,
                statement(
                    "SELECT recovery_release FROM catalog_recovery_receipts WHERE incarnation=?1 AND admission_sequence=?2 AND operation=?3",
                    vec![blob(self.check.token.owner.incarnation.as_bytes()), number(self.check.token.attempt)?, blob(self.check.token.operation)],
                ),
            )
            .await
            .map_err(|error| RootRecoveryError::Query(Box::new(error)))?;
        let Some([SqlValue::Blob(bytes)]) = rows(&query.output)?.first().map(Vec::as_slice) else {
            return Ok(None);
        };
        let mut d = BoundedDecoder::new(bytes, 1024)?;
        let saved = ReleaseRecord::decode(&mut d)?;
        d.finish()?;
        let evidence = self.command.evidence();
        if saved.incarnation != evidence.incarnation() || saved.stamp != Stamp::of(evidence) {
            return Ok(None);
        }
        Ok(Some(saved.result.committed(evidence)?))
    }
    pub(in crate::packs::publication) async fn dispatch(
        self,
        recover: bool,
        fault: u8,
    ) -> Result<PublicationOutcome, PublicationError> {
        if let Some(known) = self
            .recorded()
            .await
            .map_err(|source| PublicationError::Recovery {
                evidence: Box::new(self.command.evidence().clone()),
                source: Box::new(source),
            })?
        {
            return Ok(PublicationOutcome::TerminalRelease(known));
        }
        super::super::exact::invoke(&self.client, self.command, recover, 128, fault)
            .await
            .map(PublicationOutcome::TerminalRelease)
            .map_err(PublicationError::TerminalRelease)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn release_proof_binds_original_certificate_phase_graph_and_purpose() -> Result<(), CodecError>
    {
        let mut record = super::super::tests::record();
        record.check.actor = "a".repeat(64);
        let seed = [9; 32];
        let proof = Proof {
            recovery: RootRecoveryCertificate(CertificateEnvelope::seal(&record, &seed)?),
            phase: [10; 32],
            graph: [11; 32],
        };
        let certificate = TerminalReleaseCertificate(CertificateEnvelope::seal(&proof, &seed)?);
        let bytes = encoded(&certificate, CERTIFICATE_BYTES)?;
        let mut d = BoundedDecoder::new(&bytes, CERTIFICATE_BYTES)?;
        let decoded = TerminalReleaseCertificate::decode(&mut d)?;
        d.finish()?;
        assert_eq!(certificate, decoded);
        assert!(decoded.0.authenticated(&seed));
        assert!(!decoded.0.authenticated(&[8; 32]));
        assert_eq!(decoded.0.data::<Proof>()?, proof);
        for end in 0..bytes.len() {
            assert!(
                TerminalReleaseCertificate::decode(&mut BoundedDecoder::new(
                    &bytes[..end],
                    CERTIFICATE_BYTES
                )?)
                .is_err()
            );
        }
        assert!(
            encoded(
                &TerminalReleaseCertificate(proof.recovery.0.clone()),
                CERTIFICATE_BYTES
            )
            .is_err()
        );
        let mut changed = proof.clone();
        changed.graph = [0; 32];
        assert!(encoded(&changed, CERTIFICATE_BYTES).is_err());
        for tag in 7..=255 {
            assert!(TerminalReleaseReply::decode(&mut BoundedDecoder::new(&[tag], 1)?).is_err());
        }
        for reply in [
            TerminalReleaseReply::Released,
            TerminalReleaseReply::Denied(PreparationDenial::Unauthorized),
            TerminalReleaseReply::Denied(PreparationDenial::Conflict),
            TerminalReleaseReply::Denied(PreparationDenial::Stale),
            TerminalReleaseReply::Denied(PreparationDenial::Expired),
            TerminalReleaseReply::Denied(PreparationDenial::Capacity),
            TerminalReleaseReply::Denied(PreparationDenial::Missing),
        ] {
            let bytes = encoded(&reply, 128)?;
            let mut d = BoundedDecoder::new(&bytes, 128)?;
            assert_eq!(TerminalReleaseReply::decode(&mut d)?, reply);
            d.finish()?;
        }
        Ok(())
    }
    #[test]
    fn release_receipt_requires_the_original_successful_status_and_sequence()
    -> Result<(), CodecError> {
        let record = super::super::tests::record();
        let mut saved = ReleaseRecord {
            incarnation: record.check.token.owner.incarnation,
            stamp: record.primary,
            result: phase::Recorded::new(7, false, encoded(&TerminalReleaseReply::Released, 128)?)?,
        };
        let bytes = encoded(&saved, 1024)?;
        let mut d = BoundedDecoder::new(&bytes, 1024)?;
        let actual = ReleaseRecord::decode(&mut d)?;
        d.finish()?;
        assert_eq!(actual.stamp, saved.stamp);
        assert_eq!(actual.result, saved.result);
        saved.result =
            phase::Recorded::new(7, true, encoded(&TerminalReleaseReply::Released, 128)?)?;
        assert!(encoded(&saved, 1024).is_err());
        saved.result = phase::Recorded::new(
            7,
            false,
            encoded(
                &TerminalReleaseReply::Denied(PreparationDenial::Conflict),
                128,
            )?,
        )?;
        assert!(encoded(&saved, 1024).is_err());
        for sequence in [0, i64::MAX as u64 + 1] {
            assert!(
                phase::Recorded::new(
                    sequence,
                    false,
                    encoded(&TerminalReleaseReply::Released, 128)?
                )
                .is_err()
            );
        }
        Ok(())
    }
    #[test]
    fn unknown_intermediate_and_denied_roots_do_not_close_attempts() -> Result<(), CodecError> {
        let mut record = super::super::tests::record();
        assert!(phase::Journal::default().terminal(&record)?.is_none());
        let denied = phase::Journal {
            primary: Some(phase::Recorded::new(
                7,
                true,
                encoded(&RootCompletionReply::Denied(PreparationDenial::Stale), 512)?,
            )?),
            refusal: None,
        };
        assert!(denied.terminal(&record)?.is_none());
        record.kind = Kind::Policy;
        let mut fallback = record.primary;
        fallback.digest[0] ^= 1;
        record.refusal = Some(fallback);
        for valid in [false, true] {
            let journal = phase::Journal {
                primary: Some(phase::Recorded::new(
                    7,
                    false,
                    encoded(
                        &RefPolicyReply::Registered(RefPolicyProgress {
                            next: 128,
                            total: 257,
                            valid,
                        }),
                        512,
                    )?,
                )?),
                refusal: None,
            };
            assert!(journal.terminal(&record)?.is_none());
        }
        Ok(())
    }
}
