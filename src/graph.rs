//! Bounded object certificates and constant-per-ref graph checks at publication.

use std::collections::BTreeMap;

use crate::{MAX_SQLITE_OBJECT_BYTES, ObjectKind, PushPlan, RepositoryModule, object_id};
use crab_cell_runtime::{
    CellModule, Command, Error, codec::BoundedDecoder, codec::BoundedEncoder, codec::CodecError,
    codec::WireValue, primitives::sql::SqlBatch, primitives::sql::SqlResultSet,
    primitives::sql::SqlStatement, primitives::sql::SqlValue, registry::CommandContext,
    registry::CommandResult,
};

mod preparation;

type Oid = [u8; 20];
type Edge = (Oid, Option<ObjectKind>);
const MAX_CERTIFICATES: usize = 128;

#[derive(Default)]
pub(crate) struct CertificateBatch(Vec<Oid>);

impl WireValue for CertificateBatch {
    fn encode(&self, encoder: &mut BoundedEncoder) -> Result<(), CodecError> {
        if !(1..=MAX_CERTIFICATES).contains(&self.0.len()) {
            return Err(CodecError::Invalid("certificate batch count"));
        }
        encoder.write_count(self.0.len())?;
        for oid in &self.0 {
            encoder.write_bytes(oid)?;
        }
        Ok(())
    }
    fn decode(decoder: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        let count = decoder.read_count()?;
        if !(1..=MAX_CERTIFICATES).contains(&count) {
            return Err(CodecError::Invalid("certificate batch count"));
        }
        let mut batch = Self(Vec::with_capacity(count));
        for _ in 0..count {
            batch.0.push(
                decoder
                    .read_bytes()?
                    .try_into()
                    .map_err(|_| CodecError::Invalid("certificate OID width"))?,
            );
        }
        Ok(batch)
    }
}

#[derive(Clone)]
struct Status {
    kind: ObjectKind,
    bytes: u64,
    certified: bool,
}

fn status_query(oids: &[Oid]) -> SqlBatch {
    let placeholders = vec!["?"; oids.len()].join(",");
    SqlBatch {
        statements: vec![SqlStatement {
            sql: format!(
                "SELECT o.oid, o.kind, CASE WHEN o.storage = 'external' THEN 0 ELSE o.size END, c.oid FROM objects o LEFT JOIN object_closure c ON c.oid = o.oid WHERE o.oid IN ({placeholders})"
            ),
            parameters: oids
                .iter()
                .map(|oid| SqlValue::Blob(oid.to_vec()))
                .collect(),
        }],
    }
}

fn statuses(results: &[SqlResultSet]) -> crab_cell_runtime::Result<BTreeMap<Oid, Status>> {
    let mut states = BTreeMap::new();
    for row in &results
        .first()
        .ok_or(Error::Command("missing graph result"))?
        .rows
    {
        let [
            SqlValue::Blob(oid),
            SqlValue::Text(kind),
            SqlValue::Integer(bytes),
            certificate,
        ] = row.as_slice()
        else {
            return Err(Error::Command("invalid graph status"));
        };
        let kind = parse_kind(kind.as_bytes()).ok_or(Error::Command("invalid graph kind"))?;
        let bytes = u64::try_from(*bytes).map_err(|_| Error::Command("invalid graph size"))?;
        if bytes > MAX_SQLITE_OBJECT_BYTES as u64 {
            return Err(Error::Command("graph object exceeds limit"));
        }
        states.insert(
            oid.as_slice()
                .try_into()
                .map_err(|_| Error::Command("invalid graph OID"))?,
            Status {
                kind,
                bytes,
                certified: matches!(certificate, SqlValue::Blob(_)),
            },
        );
    }
    Ok(states)
}

pub(crate) struct CertifyObjects;
impl Command for CertifyObjects {
    const MODULE: &'static str = RepositoryModule::NAME;
    const ID: u32 = 6;
    const CODEC_VERSION: u32 = 1;
    type Input = CertificateBatch;
    type Output = bool;

    fn execute(
        context: &mut CommandContext<'_, '_>,
        batch: CertificateBatch,
    ) -> crab_cell_runtime::Result<CommandResult<bool>> {
        let mut verified = 0;
        for oid in batch.0 {
            let states = statuses(&context.sql(&status_query(&[oid]))?)?;
            let Some(state) = states.get(&oid) else {
                return Ok(CommandResult::Rejected(false));
            };
            if state.certified {
                continue;
            }
            verified += state.bytes;
            if verified > MAX_SQLITE_OBJECT_BYTES as u64 {
                return Ok(CommandResult::Rejected(false));
            }
            let Some(edges) = object_edges(context, oid, state.kind)? else {
                return Ok(CommandResult::Rejected(false));
            };
            for edges in edges.chunks(MAX_CERTIFICATES) {
                let oids: Vec<_> = edges.iter().map(|(oid, _)| *oid).collect();
                let states = statuses(&context.sql(&status_query(&oids))?)?;
                if edges.iter().any(|(oid, kind)| {
                    !states.get(oid).is_some_and(|state| {
                        state.certified && kind.is_none_or(|kind| kind == state.kind)
                    })
                }) {
                    return Ok(CommandResult::Rejected(false));
                }
            }
            if state.kind == ObjectKind::Commit {
                let parents: Vec<_> = edges
                    .iter()
                    .filter(|(_, kind)| *kind == Some(ObjectKind::Commit))
                    .map(|(parent, _)| *parent)
                    .collect();
                for parents in parents.chunks(MAX_CERTIFICATES) {
                    context.sql(&SqlBatch { statements: parents.iter().map(|parent| SqlStatement {
                        sql: "INSERT INTO commit_parents (child, parent) VALUES (?1, ?2) ON CONFLICT DO NOTHING".into(),
                        parameters: vec![SqlValue::Blob(oid.to_vec()), SqlValue::Blob(parent.to_vec())],
                    }).collect() })?;
                }
            }
            // These proofs depend only on immutable objects. A future collector must
            // invalidate certificates before removing any reachable object or chunk.
            context.sql(&SqlBatch {
                statements: vec![SqlStatement {
                    sql: "INSERT INTO object_closure (oid) VALUES (?1)".into(),
                    parameters: vec![SqlValue::Blob(oid.to_vec())],
                }],
            })?;
        }
        Ok(CommandResult::Success(true))
    }
}

pub(crate) fn certified_roots(
    context: &CommandContext<'_, '_>,
    plan: &PushPlan,
) -> crab_cell_runtime::Result<bool> {
    let oids: Vec<_> = plan
        .updates
        .iter()
        .filter_map(|update| update.new_oid)
        .collect();
    if oids.is_empty() {
        return Ok(true);
    }
    let states = statuses(&context.sql(&status_query(&oids))?)?;
    Ok(plan.updates.iter().all(|update| {
        update.new_oid.is_none_or(|oid| {
            states.get(&oid).is_some_and(|state| {
                state.certified
                    && (!update.name.starts_with("refs/heads/") || state.kind == ObjectKind::Commit)
            })
        })
    }))
}

fn object_edges(
    context: &CommandContext<'_, '_>,
    oid: Oid,
    kind: ObjectKind,
) -> crab_cell_runtime::Result<Option<Vec<Edge>>> {
    let result = context.sql(&SqlBatch {
        statements: vec![SqlStatement {
            sql: "SELECT storage, body, digest, size, chunk_id FROM objects WHERE oid = ?1".into(),
            parameters: vec![SqlValue::Blob(oid.to_vec())],
        }],
    })?;
    let Some(
        [
            SqlValue::Text(storage),
            body,
            SqlValue::Blob(digest),
            SqlValue::Integer(size),
            upload,
        ],
    ) = result
        .first()
        .and_then(|set| set.rows.first())
        .map(Vec::as_slice)
    else {
        return Err(Error::Command("invalid stored graph object"));
    };
    let edges = match (storage.as_str(), body, upload) {
        ("inline", SqlValue::Blob(body), SqlValue::Null) => {
            if object_id(kind, body) != oid || blake3::hash(body).as_bytes() != digest.as_slice() {
                return Err(Error::Command("corrupt stored graph object"));
            }
            let Some(edges) = edges(kind, body) else {
                return Ok(None);
            };
            edges
        }
        // The immutable upload is verified before its SQLite record is published.
        // Blobs have no outgoing Git edges; no network I/O belongs in this transaction.
        ("external", SqlValue::Null, SqlValue::Null) if kind == ObjectKind::Blob => Vec::new(),
        ("chunked", SqlValue::Null, SqlValue::Blob(upload)) => {
            let invalid = || Error::Command("invalid stored graph object chunks");
            let body = crate::object_chunks::body(
                context,
                oid,
                kind,
                upload.as_slice().try_into().map_err(|_| invalid())?,
                u64::try_from(*size).map_err(|_| invalid())?,
                digest.as_slice().try_into().map_err(|_| invalid())?,
            )?
            .ok_or_else(invalid)?;
            let Some(edges) = edges(kind, &body) else {
                return Ok(None);
            };
            edges
        }
        _ => return Err(Error::Command("invalid stored graph object")),
    };
    Ok(Some(edges))
}

fn edges(kind: ObjectKind, mut body: &[u8]) -> Option<Vec<Edge>> {
    let mut edges = match kind {
        ObjectKind::Blob => Some(Vec::new()),
        ObjectKind::Tree => tree_edges(body),
        ObjectKind::Commit => {
            let tree = hex_oid(line(&mut body)?.strip_prefix(b"tree ")?)?;
            let mut edges = vec![(tree, Some(ObjectKind::Tree))];
            // Git recognizes parents immediately after the tree header. Later headers
            // (including multiline signatures) and the message contain no graph edges.
            while body.starts_with(b"parent ") {
                let parent = line(&mut body)?.strip_prefix(b"parent ")?;
                edges.push((hex_oid(parent)?, Some(ObjectKind::Commit)));
            }
            Some(edges)
        }
        ObjectKind::Tag => Some(vec![tag_edge(body)?]),
    }?;
    // Repeated files can share one Git object. A typed dependency needs one proof.
    edges.sort_unstable();
    edges.dedup();
    Some(edges)
}

pub(crate) fn tag_edge(mut body: &[u8]) -> Option<Edge> {
    let target = hex_oid(line(&mut body)?.strip_prefix(b"object ")?)?;
    let kind = parse_kind(line(&mut body)?.strip_prefix(b"type ")?)?;
    line(&mut body)?.strip_prefix(b"tag ")?;
    Some((target, Some(kind)))
}

fn line<'a>(body: &mut &'a [u8]) -> Option<&'a [u8]> {
    let newline = body.iter().position(|byte| *byte == b'\n')?;
    let line = &body[..newline];
    *body = &body[newline + 1..];
    Some(line)
}

fn tree_edges(mut body: &[u8]) -> Option<Vec<Edge>> {
    let mut edges = Vec::new();
    while !body.is_empty() {
        let space = body.iter().position(|byte| *byte == b' ')?;
        let mut mode = 0u32;
        if space == 0 {
            return None;
        }
        for byte in &body[..space] {
            if !(b'0'..=b'7').contains(byte) {
                return None;
            }
            mode = mode.checked_mul(8)?.checked_add(u32::from(byte - b'0'))?;
        }
        if mode > 0o177777 {
            return None;
        }
        let path = &body[space + 1..];
        let nul = path.iter().position(|byte| *byte == 0)?;
        let name = &path[..nul];
        if name.is_empty() || name.contains(&b'/') || name == b"." || name == b".." {
            return None;
        }
        let oid: Oid = path.get(nul + 1..nul + 21)?.try_into().ok()?;
        if oid == [0; 20] {
            return None;
        }
        let kind = match mode & 0o170000 {
            0o040000 => Some(ObjectKind::Tree),
            0o100000 | 0o120000 => Some(ObjectKind::Blob),
            // A gitlink names a commit in another repository, not a missing local object.
            0o160000 => None,
            _ => return None,
        };
        if let Some(kind) = kind {
            edges.push((oid, Some(kind)));
        }
        body = &path[nul + 21..];
    }
    Some(edges)
}

fn hex_oid(text: &[u8]) -> Option<Oid> {
    let mut oid = [0; 20];
    hex::decode_to_slice(text, &mut oid).ok()?;
    (oid != [0; 20]).then_some(oid)
}

fn parse_kind(kind: &[u8]) -> Option<ObjectKind> {
    match kind {
        b"blob" => Some(ObjectKind::Blob),
        b"tree" => Some(ObjectKind::Tree),
        b"commit" => Some(ObjectKind::Commit),
        b"tag" => Some(ObjectKind::Tag),
        _ => None,
    }
}

#[cfg(test)]
mod tests;
