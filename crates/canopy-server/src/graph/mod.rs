//! Typed structural Git parsing for native catalog verification.
use crate::ObjectKind;

pub(crate) mod stream;
#[cfg(test)]
type Oid = crate::ObjectId;
#[cfg(test)]
type Edge = (Oid, Option<ObjectKind>);

#[cfg(test)]
pub(crate) fn edges(
    format: crate::ObjectFormat,
    kind: ObjectKind,
    mut body: &[u8],
) -> Option<Vec<Edge>> {
    let mut edges = match kind {
        ObjectKind::Blob => Some(Vec::new()),
        ObjectKind::Tree => tree_edges(format, body),
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
    if edges.iter().any(|(oid, _)| oid.format() != format) {
        return None;
    }
    // Repeated files can share one Git object. A typed dependency needs one proof.
    edges.sort_unstable();
    edges.dedup();
    Some(edges)
}

#[cfg(test)]
pub(crate) fn tag_edge(mut body: &[u8]) -> Option<Edge> {
    let target = hex_oid(line(&mut body)?.strip_prefix(b"object ")?)?;
    let kind = parse_kind(line(&mut body)?.strip_prefix(b"type ")?)?;
    line(&mut body)?.strip_prefix(b"tag ")?;
    Some((target, Some(kind)))
}

#[cfg(test)]
fn line<'a>(body: &mut &'a [u8]) -> Option<&'a [u8]> {
    let newline = body.iter().position(|byte| *byte == b'\n')?;
    let line = &body[..newline];
    *body = &body[newline + 1..];
    Some(line)
}

#[cfg(test)]
fn tree_edges(format: crate::ObjectFormat, mut body: &[u8]) -> Option<Vec<Edge>> {
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
        let oid: Oid = path
            .get(nul + 1..nul + 1 + format.bytes())?
            .try_into()
            .ok()?;
        if oid.is_zero() {
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
        body = &path[nul + 1 + format.bytes()..];
    }
    Some(edges)
}

#[cfg(test)]
fn hex_oid(text: &[u8]) -> Option<Oid> {
    let oid = Oid::from_hex(text).ok()?;
    (!oid.is_zero()).then_some(oid)
}

pub(crate) fn parse_kind(kind: &[u8]) -> Option<ObjectKind> {
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
