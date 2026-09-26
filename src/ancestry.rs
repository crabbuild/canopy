//! Bounded certificates over parent links extracted from verified commit objects.

use crate::{RepositoryCell, RepositoryModule, server::mutation_identity};
use cellule_runtime::{
    BoundedDecoder, BoundedEncoder, CellModule, CodecError, Command, CommandContext, CommandResult,
    Error, InvocationError, SqlBatch, SqlStatement, SqlValue, WireValue,
};
use std::collections::{HashMap, hash_map::Entry};

type Oid = [u8; 20];
const PAGE: usize = 128;

pub(crate) struct AncestryProof {
    ancestor: Oid,
    steps: Vec<(Oid, Oid)>,
}

impl WireValue for AncestryProof {
    fn encode(&self, out: &mut BoundedEncoder) -> Result<(), CodecError> {
        if self.steps.is_empty() || self.steps.len() > PAGE {
            return Err(CodecError::Invalid("ancestry proof size"));
        }
        out.write_bytes(&self.ancestor)?;
        out.write_count(self.steps.len())?;
        for (child, parent) in &self.steps {
            out.write_bytes(child)?;
            out.write_bytes(parent)?;
        }
        Ok(())
    }
    fn decode(input: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        let ancestor = oid(input)?;
        let count = input.read_count()?;
        if count == 0 || count > PAGE {
            return Err(CodecError::Invalid("ancestry proof size"));
        }
        let mut steps = Vec::with_capacity(count);
        for _ in 0..count {
            steps.push((oid(input)?, oid(input)?));
        }
        Ok(Self { ancestor, steps })
    }
}
fn oid(input: &mut BoundedDecoder<'_>) -> Result<Oid, CodecError> {
    input
        .read_bytes()?
        .try_into()
        .map_err(|_| CodecError::Invalid("ancestry OID width"))
}

pub(crate) struct CertifyAncestry;
impl Command for CertifyAncestry {
    const MODULE: &'static str = RepositoryModule::NAME;
    const ID: u32 = 7;
    const CODEC_VERSION: u32 = 1;
    type Input = AncestryProof;
    type Output = bool;
    fn execute(
        context: &mut CommandContext<'_, '_>,
        proof: AncestryProof,
    ) -> cellule_runtime::Result<CommandResult<bool>> {
        for (child, parent) in proof.steps {
            let result = context.sql(&SqlBatch { statements: vec![SqlStatement {
                sql: "SELECT EXISTS (SELECT 1 FROM commit_parents WHERE child = ?1 AND parent = ?2) AND (?2 = ?3 OR EXISTS (SELECT 1 FROM commit_ancestry WHERE ancestor = ?3 AND descendant = ?2))".into(),
                parameters: vec![SqlValue::Blob(child.to_vec()), SqlValue::Blob(parent.to_vec()), SqlValue::Blob(proof.ancestor.to_vec())],
            }] })?;
            if !matches!(
                result
                    .first()
                    .and_then(|set| set.rows.first())
                    .map(Vec::as_slice),
                Some([SqlValue::Integer(1)])
            ) {
                return Ok(CommandResult::Rejected(false));
            }
            context.sql(&SqlBatch { statements: vec![SqlStatement {
                sql: "INSERT INTO commit_ancestry (ancestor, descendant) VALUES (?1, ?2) ON CONFLICT DO NOTHING".into(),
                parameters: vec![SqlValue::Blob(proof.ancestor.to_vec()), SqlValue::Blob(child.to_vec())],
            }] })?;
        }
        Ok(CommandResult::Success(true))
    }
}

impl RepositoryCell {
    pub(crate) async fn prepare_ancestry(
        &self,
        ancestor: Oid,
        descendant: Oid,
    ) -> Result<(), InvocationError<bool>> {
        self.ancestry_path(ancestor, descendant)
            .await
            .map_err(|source| {
                InvocationError::NotStarted(Error::Facility {
                    name: "commit ancestry preparation",
                    source,
                })
            })
    }

    async fn ancestry_path(
        &self,
        ancestor: Oid,
        descendant: Oid,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if ancestor == descendant {
            return Ok(());
        }
        let known = self
            .sql
            .query(
                None,
                SqlBatch {
                    statements: vec![SqlStatement {
                        sql:
                            "SELECT 1 FROM commit_ancestry WHERE ancestor = ?1 AND descendant = ?2"
                                .into(),
                        parameters: vec![
                            SqlValue::Blob(ancestor.to_vec()),
                            SqlValue::Blob(descendant.to_vec()),
                        ],
                    }],
                },
            )
            .await?;
        if known.output.first().is_some_and(|set| !set.rows.is_empty()) {
            return Ok(());
        }
        let mut previous: HashMap<Oid, Option<Oid>> = HashMap::from([(descendant, None)]);
        let mut pending = vec![(descendant, None::<Oid>)];
        let mut edges = 0;
        while let Some((child, after)) = pending.pop() {
            let result = self.sql.query(None, SqlBatch { statements: vec![SqlStatement {
                sql: format!("SELECT parent, EXISTS (SELECT 1 FROM commit_ancestry WHERE ancestor = ?3 AND descendant = p.parent) FROM commit_parents p WHERE child = ?1 AND parent > ?2 ORDER BY parent LIMIT {PAGE}"),
                parameters: vec![SqlValue::Blob(child.to_vec()), SqlValue::Blob(after.map_or_else(Vec::new, |oid| oid.to_vec())), SqlValue::Blob(ancestor.to_vec())],
            }] }).await?;
            let rows = &result
                .output
                .first()
                .ok_or(Error::Command("missing commit parents"))?
                .rows;
            edges += rows.len();
            if edges > 250_000 {
                return Err(Error::Command("commit ancestry traversal limit").into());
            }
            let mut parents = Vec::with_capacity(rows.len());
            for row in rows {
                let [SqlValue::Blob(parent), SqlValue::Integer(known)] = row.as_slice() else {
                    return Err(Error::Command("invalid commit parent").into());
                };
                let parent: Oid = parent
                    .as_slice()
                    .try_into()
                    .map_err(|_| Error::Command("invalid parent OID"))?;
                parents.push(parent);
                let Entry::Vacant(entry) = previous.entry(parent) else {
                    continue;
                };
                entry.insert(Some(child));
                if previous.len() > 100_000 {
                    return Err(Error::Command("commit ancestry traversal limit").into());
                }
                if parent == ancestor || *known == 1 {
                    let mut steps = Vec::new();
                    let mut parent = parent;
                    while let Some(Some(child)) = previous.get(&parent).copied() {
                        steps.push((child, parent));
                        parent = child;
                    }
                    // Each bounded command proves its chosen links itself. Partial
                    // preparation retains only immutable facts, never ref authority.
                    for steps in steps.chunks(PAGE) {
                        self.application
                            .command::<CertifyAncestry>(
                                &self.target,
                                mutation_identity()?,
                                AncestryProof {
                                    ancestor,
                                    steps: steps.to_vec(),
                                },
                            )
                            .await?;
                    }
                    return Ok(());
                }
            }
            if parents.len() == PAGE {
                pending.push((child, parents.last().copied()));
            }
            for parent in parents.into_iter().rev() {
                if previous.get(&parent) == Some(&Some(child)) {
                    pending.push((parent, None));
                }
            }
        }
        // A non-ancestor has no positive certificate. Final publication decides
        // against the current rule; this search never certifies a negative result.
        Ok(())
    }
}
