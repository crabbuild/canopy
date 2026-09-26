//! Repository-local Git connectivity checked in the ref publication transaction.

use std::collections::HashSet;

use cellule_runtime::{CommandContext, Error, SqlBatch, SqlStatement, SqlValue};

use crate::{ObjectKind, PushPlan, object_id};

type Oid = [u8; 20];
type Edge = (Oid, Option<ObjectKind>);

enum Visit {
    Enter(Edge),
    Leave(Oid),
}

pub(crate) fn certify(
    context: &CommandContext<'_, '_>,
    plan: &PushPlan,
) -> cellule_runtime::Result<bool> {
    let mut pending: Vec<_> = plan
        .updates
        .iter()
        .filter_map(|update| {
            update.new_oid.map(|oid| {
                Visit::Enter((
                    oid,
                    update
                        .name
                        .starts_with("refs/heads/")
                        .then_some(ObjectKind::Commit),
                ))
            })
        })
        .collect();
    let mut visiting = HashSet::new();
    while let Some(visit) = pending.pop() {
        let (oid, expected) = match visit {
            Visit::Enter(edge) => edge,
            Visit::Leave(oid) => {
                // Objects are immutable and there is no collector yet. This certificate
                // remains valid across future pushes; a collector must invalidate it.
                context.sql(&SqlBatch {
                    statements: vec![SqlStatement {
                        sql: "INSERT INTO object_closure (oid) VALUES (?1)".into(),
                        parameters: vec![SqlValue::Blob(oid.to_vec())],
                    }],
                })?;
                visiting.remove(&oid);
                continue;
            }
        };
        let result = context.sql(&SqlBatch { statements: vec![SqlStatement {
            sql: "SELECT o.kind, c.oid FROM objects o LEFT JOIN object_closure c ON c.oid = o.oid WHERE o.oid = ?1".into(),
            parameters: vec![SqlValue::Blob(oid.to_vec())],
        }]})?;
        let Some([SqlValue::Text(kind), certificate]) = result
            .first()
            .and_then(|set| set.rows.first())
            .map(Vec::as_slice)
        else {
            return Ok(false);
        };
        let Some(kind) = parse_kind(kind.as_bytes()) else {
            return Ok(false);
        };
        if expected.is_some_and(|expected| expected != kind) {
            return Ok(false);
        }
        if matches!(certificate, SqlValue::Blob(_)) {
            continue;
        }
        if !visiting.insert(oid) {
            return Ok(false);
        }
        let result = context.sql(&SqlBatch {
            statements: vec![SqlStatement {
                sql: "SELECT storage, body, digest, size, chunk_id FROM objects WHERE oid = ?1"
                    .into(),
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
                if object_id(kind, body) != oid
                    || blake3::hash(body).as_bytes() != digest.as_slice()
                {
                    return Err(Error::Command("corrupt stored graph object"));
                }
                let Some(edges) = edges(kind, body) else {
                    return Ok(false);
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
                    return Ok(false);
                };
                edges
            }
            _ => return Err(Error::Command("invalid stored graph object")),
        };
        pending.push(Visit::Leave(oid));
        pending.extend(edges.into_iter().rev().map(Visit::Enter));
    }
    Ok(true)
}

fn edges(kind: ObjectKind, mut body: &[u8]) -> Option<Vec<Edge>> {
    match kind {
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
        ObjectKind::Tag => {
            let target = hex_oid(line(&mut body)?.strip_prefix(b"object ")?)?;
            let kind = parse_kind(line(&mut body)?.strip_prefix(b"type ")?)?;
            line(&mut body)?.strip_prefix(b"tag ")?;
            Some(vec![(target, Some(kind))])
        }
    }
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
