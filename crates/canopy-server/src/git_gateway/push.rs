use super::*;

impl GitGateway {
    pub(super) async fn handle_push(
        &self,
        request: GitHttpRequest,
        actor: &str,
        id: [u8; 16],
        digest: [u8; 32],
    ) -> Result<GitHttpResponse, GatewayError> {
        let commands = branch_policy::PushCommands::read(&request).await?;
        let option_error = commands.option_error().or_else(|| {
            (commands.certificate().is_some() && self.signer_directory.is_none())
                .then_some("Canopy signed pushes are unavailable on this gateway")
        });
        let prepared = async {
            if let Some(reason) = option_error {
                let response = commands
                    .rejection(reason)?
                    .ok_or(GatewayError::MalformedCache)?;
                return Ok::<_, GatewayError>((response, None, None));
            }
            let cached = self.build_cache(self.cell_refs().await?, true).await?;
            self.install_branch_policy(&cached, &commands).await?;
            let signers = self.install_certificate_policy(&cached, &commands, actor).await?;
            let before = cached.snapshot.refs.clone();
            let backend = signers.map_or_else(
                || cached.backend.clone(),
                |path| cached.backend.with_signers(path),
            );
            let mut response = backend.run(request).await?;
            let certificate = self.verified_certificate(&cached, &commands, actor).await?;
            // Git may accept some refs and reject others unless atomic was requested.
            // Publish its actual changes before returning any successful per-ref report.
            let plan = if response.status == 200 {
                let after = git_refs(&cached.backend.git_dir()).await?;
                let plan = diff_refs(&before, &after, actor);
                if plan.updates.is_empty() {
                    None
                } else {
                    match self.persist_objects(&cached.backend, &before, &plan).await {
                        Ok(()) => Some(plan),
                        Err(error) => {
                            tracing::warn!(push_id = %hex::encode(id), error = ?error, "Git object ingestion failed");
                            let reason = match error {
                                GatewayError::Objects(ObjectReadError::TooLarge) => {
                                    "Canopy object ingestion failed: object exceeds server size limit"
                                }
                                _ => "Canopy object ingestion failed; retry push after server recovery",
                            };
                            response = crate::push::report::rejected_report(&response, reason)?;
                            // Ingestion cannot publish refs. Persist this refusal through
                            // completion so a concurrent attempt with the same ID can
                            // win; only the canonical durable response reaches the client.
                            None
                        }
                    }
                }
            } else {
                None
            };
            Ok::<_, GatewayError>((response, plan, certificate))
        }
        .await;
        let (response, plan, certificate) = match prepared {
            Ok(prepared) => prepared,
            Err(error) => {
                let reason = match &error {
                    GatewayError::Cache(error) | GatewayError::Http(GitHttpError::Cache(error))
                        if error.is_admission() =>
                    {
                        "Canopy push failed before publication: cache disk budget exhausted"
                    }
                    GatewayError::Certificate(reason) => reason,
                    _ => "Canopy push failed before publication; retry after server recovery",
                };
                let Some(response) = commands.rejection(reason)? else {
                    return Err(error);
                };
                tracing::warn!(push_id = %hex::encode(id), error = ?error, "Git push failed before publication");
                (response, None, None)
            }
        };
        let options = if option_error.is_some() {
            Vec::new()
        } else {
            commands.options().to_vec()
        };
        // Release parsed command names before staging a potentially large report.
        drop(commands);
        // Preparation and native Git mutate only disposable refs. Record their
        // refusal through completion; a concurrent same-ID winner stays canonical.
        // Publication failures below may be uncertain and must never become ng.
        let response_id = self.repository.stage_push_response(id, &response).await?;
        let result = self
            .repository
            .complete_push(PushCompletion {
                id,
                actor: actor.into(),
                digest,
                response_id,
                options,
                plan,
                certificate,
            })
            .await
            .map_err(|error| GatewayError::Cell(Box::new(error)))?;
        if !result.output {
            return Err(PushError::InvalidResponse.into());
        }
        Ok(with_push_id(
            self.repository.completed_response(id).await?,
            id,
        ))
    }

    async fn verified_certificate(
        &self,
        cached: &CachedRepository,
        commands: &branch_policy::PushCommands,
        actor: &str,
    ) -> Result<Option<crate::push::VerifiedPushCertificate>, GatewayError> {
        let Some(body) = commands.certificate() else {
            return Ok(None);
        };
        let path = cached
            .backend
            .git_dir()
            .join("hooks/canopy-push-certificate");
        let receipt = tokio::fs::read_to_string(path)
            .await
            .map_err(|_| GatewayError::Certificate("Canopy signed push verification failed"))?;
        let mut lines = receipt.lines();
        let (Some(oid), Some(signer), Some(key), None) =
            (lines.next(), lines.next(), lines.next(), lines.next())
        else {
            return Err(GatewayError::Certificate(
                "Canopy signed push verification failed",
            ));
        };
        let oid = crate::ObjectId::from_hex(oid).map_err(|_| {
            GatewayError::Certificate("Canopy signed push certificate ID is invalid")
        })?;
        if oid.format() != self.repository.object_format()
            || oid != crate::object_id(oid.format(), ObjectKind::Blob, body)
            || signer != actor
        {
            return Err(GatewayError::Certificate(
                "Canopy signed push certificate does not match verified bytes",
            ));
        }
        let directory = self
            .signer_directory
            .as_ref()
            .ok_or(GatewayError::Certificate(
                "Canopy signed push signer is unavailable",
            ))?;
        let signers = directory
            .push_signers(actor)
            .await
            .map_err(|error| GatewayError::Cell(Box::new(error)))?;
        if !signers.iter().any(|signer| signer.fingerprint() == key) {
            return Err(GatewayError::Certificate(
                "Canopy signed push key is no longer authorized",
            ));
        }
        Ok(Some(crate::push::VerifiedPushCertificate {
            body: body.to_vec(),
            signer: signer.into(),
            key: key.into(),
        }))
    }
}
