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
        let prepared = async {
            let cached = self.build_cache(self.cell_refs().await?, true).await?;
            self.install_branch_policy(&cached, &commands).await?;
            let before = cached.snapshot.refs.clone();
            let mut response = cached.backend.run(request).await?;
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
            Ok::<_, GatewayError>((response, plan))
        }
        .await;
        let (response, plan) = match prepared {
            Ok(prepared) => prepared,
            Err(error) => {
                let reason = match &error {
                    GatewayError::Cache(error) | GatewayError::Http(GitHttpError::Cache(error))
                        if error.is_admission() =>
                    {
                        "Canopy push failed before publication: cache disk budget exhausted"
                    }
                    _ => "Canopy push failed before publication; retry after server recovery",
                };
                let Some(response) = commands.rejection(reason)? else {
                    return Err(error);
                };
                tracing::warn!(push_id = %hex::encode(id), error = ?error, "Git push failed before publication");
                (response, None)
            }
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
                plan,
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
}
