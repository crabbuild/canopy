use super::*;
use crate::refs::MAX_UPDATES;

const UPDATES_PER_CHUNK: usize = 128;
const CHUNK_LIMIT: u32 = 64 * 1024;

// Completion carries a small immutable binding, not the potentially large ref
// list. Count, actor and digest checks prevent incomplete or mixed attempts
// from reaching the single authoritative ref transaction.
pub(super) struct StagedPlan {
    updates: usize,
    digest: [u8; 32],
}

impl WireValue for StagedPlan {
    fn encode(&self, encoder: &mut BoundedEncoder) -> Result<(), CodecError> {
        encoder.write_count(self.updates)?;
        encoder.write_bytes(&self.digest)
    }

    fn decode(decoder: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        let updates = decoder.read_count()?;
        if !(1..=MAX_UPDATES).contains(&updates) {
            return Err(CodecError::Invalid("push update count is outside bounds"));
        }
        Ok(Self {
            updates,
            digest: fixed(decoder)?,
        })
    }
}

impl RepositoryCell {
    pub(super) async fn stage_push_plan(
        &self,
        response: [u8; 16],
        plan: &PushPlan,
    ) -> Result<StagedPlan, PushError> {
        if !(1..=MAX_UPDATES).contains(&plan.updates.len()) {
            return Err(PushError::InvalidPlan);
        }
        let mut digest = blake3::Hasher::new();
        for (part, updates) in plan.updates.chunks(UPDATES_PER_CHUNK).enumerate() {
            let mut encoder = BoundedEncoder::new(CHUNK_LIMIT).map_err(cell)?;
            PushPlan {
                actor: plan.actor.clone(),
                updates: updates.to_vec(),
            }
            .encode(&mut encoder)
            .map_err(cell)?;
            let body = encoder.finish();
            digest.update(&body);
            self.sql.batch(identity()?, SqlBatch { statements: vec![SqlStatement {
                sql: "INSERT INTO push_plan_chunks (response_id, part, body) VALUES (?1, ?2, ?3)".into(),
                parameters: vec![SqlValue::Blob(response.to_vec()), SqlValue::Integer(part as i64), SqlValue::Blob(body)],
            }] }).await.map_err(cell)?;
        }
        Ok(StagedPlan {
            updates: plan.updates.len(),
            digest: *digest.finalize().as_bytes(),
        })
    }
}

impl StagedPlan {
    pub(super) fn load(
        &self,
        context: &CommandContext<'_, '_>,
        response: [u8; 16],
        actor: &str,
    ) -> cellule_runtime::Result<PushPlan> {
        let mut digest = blake3::Hasher::new();
        let mut updates = Vec::new();
        let parts = self.updates.div_ceil(UPDATES_PER_CHUNK);
        for part in 0..parts {
            let result = context.sql(&SqlBatch {
                statements: vec![SqlStatement {
                    sql: "SELECT body FROM push_plan_chunks WHERE response_id = ?1 AND part = ?2"
                        .into(),
                    parameters: vec![
                        SqlValue::Blob(response.to_vec()),
                        SqlValue::Integer(part as i64),
                    ],
                }],
            })?;
            let Some([SqlValue::Blob(body)]) = result
                .first()
                .and_then(|set| set.rows.first())
                .map(Vec::as_slice)
            else {
                return Err(Error::Command("push ref plan is incomplete"));
            };
            digest.update(body);
            let mut decoder = BoundedDecoder::new(body, CHUNK_LIMIT)?;
            let chunk = PushPlan::decode(&mut decoder)?;
            decoder.finish()?;
            let expected = (self.updates - updates.len()).min(UPDATES_PER_CHUNK);
            if chunk.actor != actor || chunk.updates.len() != expected {
                return Err(Error::Command(
                    "push ref plan does not match its publication",
                ));
            }
            updates.extend(chunk.updates);
        }
        if digest.finalize().as_bytes() != &self.digest {
            return Err(Error::Command("push ref plan digest mismatch"));
        }
        Ok(PushPlan {
            actor: actor.into(),
            updates,
        })
    }
}
