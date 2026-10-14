//! Fresh-schema checks of the shared ref boundary. Apply is trusted fixture
//! injection only; the real publisher must also verify catalog and policy facts.
use super::*;
use crate::{PushPlan, RefExpectation, RefUpdate};

pub(super) struct FixtureRefs;
pub(super) struct FixturePlan {
    plan: PushPlan,
    apply: bool,
}
impl WireValue for FixturePlan {
    fn encode(&self, e: &mut BoundedEncoder) -> std::result::Result<(), CodecError> {
        self.plan.encode(e)?;
        e.write_bool(self.apply)
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> std::result::Result<Self, CodecError> {
        Ok(Self {
            plan: PushPlan::decode(d)?,
            apply: d.read_bool()?,
        })
    }
}
impl Command for FixtureRefs {
    const MODULE: &'static str = "repository";
    const ID: u32 = 90;
    const CODEC_VERSION: u32 = 1;
    type Input = FixturePlan;
    type Output = bool;
    fn execute(
        context: &mut CommandContext<'_, '_>,
        FixturePlan { plan, apply }: Self::Input,
    ) -> cellule_runtime::Result<CommandResult<bool>> {
        let Some(validated) = crate::refs::validate_refs(context, &plan)? else {
            return Ok(CommandResult::Rejected(false));
        };
        if apply {
            validated.apply(context)?;
        }
        Ok(CommandResult::Success(true))
    }
}
fn oid(byte: u8) -> crate::ObjectId {
    crate::ObjectId::Sha1([byte; 20])
}
fn update(name: &str, expected: Option<(Option<u8>, i64)>, new: Option<u8>) -> RefUpdate {
    RefUpdate {
        name: name.into(),
        expected: expected.map(|(id, version)| RefExpectation {
            oid: id.map(oid),
            version,
        }),
        new_oid: new.map(oid),
    }
}
fn plan(updates: Vec<RefUpdate>) -> PushPlan {
    PushPlan {
        actor: "owner".into(),
        updates,
    }
}
async fn apply(fixture: &Fixture, plan: PushPlan) -> Result<bool> {
    match fixture
        .client()
        .command::<FixtureRefs>(
            &fixture.target,
            identity()?,
            FixturePlan { plan, apply: true },
        )
        .await
    {
        Ok(committed) => Ok(committed.output),
        Err(InvocationError::Rejected(value)) => Ok(value.output),
        Err(error) => Err(error.into()),
    }
}
async fn state(handle: &CellHandle) -> Result<Vec<u8>> {
    Ok(handle
        .query(0, 64 << 10, |connection| {
            let generation: i64 = connection.query_row(
                "SELECT generation FROM ref_generation WHERE singleton=1",
                [],
                |r| r.get(0),
            )?;
            let mut stmt = connection.prepare("SELECT name,oid,version FROM refs ORDER BY name")?;
            let refs = stmt
                .query_map([], |r| {
                    Ok((
                        r.get::<_, String>(0)?,
                        r.get::<_, Option<Vec<u8>>>(1)?,
                        r.get::<_, i64>(2)?,
                    ))
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            serde_json::to_vec(&(generation, refs))
                .map_err(|_| cellule_runtime::Error::Command("fixture refs encoding"))
        })
        .await?)
}

#[tokio::test]
async fn fresh_ref_validation_reads_no_graph_tables_and_denies_before_writes() -> Result {
    let fixture = Fixture::new(ObjectFormat::Sha1).await?;
    let before = state(&fixture.handle).await?;
    assert!(
        fixture
            .client()
            .command::<FixtureRefs>(
                &fixture.target,
                identity()?,
                FixturePlan {
                    plan: plan(vec![update("refs/heads/main", None, Some(7))]),
                    apply: false
                }
            )
            .await?
            .output
    );
    assert_eq!(state(&fixture.handle).await?, before);
    for updates in [
        vec![
            update("refs/heads/team", None, Some(7)),
            update("refs/heads/team/topic", None, Some(7)),
        ],
        vec![
            update("refs/heads/main", None, Some(7)),
            update("refs/heads/main", None, Some(8)),
        ],
        vec![update("refs/canopy/candidate", None, Some(7))],
        vec![update("refs/heads/../main", None, Some(7))],
        vec![update("refs/heads/main", None, None)],
    ] {
        assert!(!apply(&fixture, plan(updates)).await?);
        assert_eq!(state(&fixture.handle).await?, before);
    }
    let mut foreign = plan(vec![update("refs/heads/main", None, Some(7))]);
    foreign.updates[0].new_oid = Some(crate::ObjectId::Sha256([7; 32]));
    assert!(!apply(&fixture, foreign).await?);
    let mut unauthorized = plan(vec![update("refs/heads/main", None, Some(7))]);
    unauthorized.actor = "outsider".into();
    assert!(!apply(&fixture, unauthorized).await?);
    assert_eq!(state(&fixture.handle).await?, before);
    fixture.runtime.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn fresh_ref_boundary_retains_tombstones_and_atomic_namespace_changes() -> Result {
    let fixture = Fixture::new(ObjectFormat::Sha1).await?;
    assert!(
        apply(
            &fixture,
            plan(vec![update("refs/heads/team", None, Some(7))])
        )
        .await?
    );
    assert!(
        apply(
            &fixture,
            plan(vec![
                update("refs/heads/team", Some((Some(7), 1)), None),
                update("refs/heads/team/topic", None, Some(8))
            ])
        )
        .await?
    );
    let before = state(&fixture.handle).await?;
    // A deleted ancestor can only be recreated with its retained version, and
    // its currently live descendant must be deleted in that same transaction.
    assert!(
        !apply(
            &fixture,
            plan(vec![update("refs/heads/team", None, Some(7))])
        )
        .await?
    );
    assert!(
        !apply(
            &fixture,
            plan(vec![update("refs/heads/team", Some((None, 2)), Some(7))])
        )
        .await?
    );
    assert_eq!(state(&fixture.handle).await?, before);
    assert!(
        apply(
            &fixture,
            plan(vec![
                update("refs/heads/team", Some((None, 2)), Some(7)),
                update("refs/heads/team/topic", Some((Some(8), 1)), None)
            ])
        )
        .await?
    );
    let after = state(&fixture.handle).await?;
    assert!(
        !apply(
            &fixture,
            plan(vec![update("refs/heads/team", Some((Some(7), 1)), Some(9))])
        )
        .await?
    );
    assert_eq!(state(&fixture.handle).await?, after);
    fixture.runtime.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn fresh_namespace_validation_checks_descendants_beyond_one_page() -> Result {
    let fixture = Fixture::new(ObjectFormat::Sha1).await?;
    let updates: Vec<_> = (0..300)
        .map(|n| update(&format!("refs/heads/team/{n:04}"), None, Some(7)))
        .collect();
    assert!(apply(&fixture, plan(updates)).await?);
    let before = state(&fixture.handle).await?;
    let mut deletes: Vec<_> = (0..299)
        .map(|n| update(&format!("refs/heads/team/{n:04}"), Some((Some(7), 1)), None))
        .collect();
    deletes.push(update("refs/heads/team", None, Some(8)));
    assert!(!apply(&fixture, plan(deletes.clone())).await?);
    assert_eq!(state(&fixture.handle).await?, before);
    deletes.push(update("refs/heads/team/0299", Some((Some(7), 1)), None));
    assert!(apply(&fixture, plan(deletes)).await?);
    fixture.runtime.shutdown().await?;
    Ok(())
}
