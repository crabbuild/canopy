//! Streaming graph projection for structural bodies; messages and names need no body buffer.
use super::{Edge, ObjectKind};
use crate::{ObjectFormat, ObjectId};
use std::collections::TryReserveError;

pub(crate) struct StreamGraph {
    format: ObjectFormat,
    kind: ObjectKind,
    edges: Vec<Edge>,
    invalid: bool,
    done: bool,
    line: Vec<u8>,
    line_number: u8,
    target: Option<ObjectId>,
    target_kind: Option<ObjectKind>,
    mode: u32,
    digits: usize,
    tree_state: u8,
    name_bytes: u64,
    dot_name: bool,
    oid: [u8; 32],
    oid_bytes: usize,
}
impl StreamGraph {
    pub(crate) fn new(format: ObjectFormat, kind: ObjectKind) -> Self {
        Self {
            format,
            kind,
            edges: Vec::new(),
            invalid: false,
            done: kind == ObjectKind::Blob,
            line: Vec::new(),
            line_number: 0,
            target: None,
            target_kind: None,
            mode: 0,
            digits: 0,
            tree_state: 0,
            name_bytes: 0,
            dot_name: true,
            oid: [0; 32],
            oid_bytes: 0,
        }
    }
    fn edge(&mut self, oid: ObjectId, kind: Option<ObjectKind>) -> Result<(), TryReserveError> {
        self.edges.try_reserve(1)?;
        self.edges.push((oid, kind));
        Ok(())
    }
    pub(crate) fn feed(&mut self, bytes: &[u8]) -> Result<(), TryReserveError> {
        if self.invalid || self.done {
            return Ok(());
        }
        for &byte in bytes {
            if self.invalid || self.done {
                break;
            }
            if self.kind == ObjectKind::Tree {
                self.tree_byte(byte)?;
                continue;
            }
            if byte == b'\n' {
                self.end_line()?;
                continue;
            }
            if self.kind == ObjectKind::Tag && self.line_number == 2 {
                if self.line.len() < 4 {
                    self.line.push(byte);
                }
                if self.line.len() == 4 && self.line != b"tag " {
                    self.invalid = true;
                }
            } else if self.line.len() < 128 {
                self.line.push(byte);
            } else if self.kind == ObjectKind::Commit
                && self.line_number != 0
                && !self.line.starts_with(b"parent ")
            {
                self.done = true;
            } else {
                self.invalid = true;
            }
        }
        Ok(())
    }
    fn parsed_oid(&self, bytes: &[u8]) -> Option<ObjectId> {
        ObjectId::from_hex(bytes)
            .ok()
            .filter(|oid| oid.format() == self.format && !oid.is_zero())
    }
    fn end_line(&mut self) -> Result<(), TryReserveError> {
        if self.kind == ObjectKind::Commit {
            if self.line_number == 0 {
                let oid = self
                    .line
                    .strip_prefix(b"tree ")
                    .and_then(|value| self.parsed_oid(value));
                if let Some(oid) = oid {
                    self.edge(oid, Some(ObjectKind::Tree))?;
                    self.line_number = 1;
                } else {
                    self.invalid = true;
                }
            } else if let Some(value) = self.line.strip_prefix(b"parent ") {
                if let Some(oid) = self.parsed_oid(value) {
                    self.edge(oid, Some(ObjectKind::Commit))?;
                } else {
                    self.invalid = true;
                }
            } else {
                self.done = true;
            }
        } else {
            match self.line_number {
                0 => {
                    self.target = self
                        .line
                        .strip_prefix(b"object ")
                        .and_then(|value| self.parsed_oid(value));
                    if self.target.is_none() {
                        self.invalid = true;
                    }
                }
                1 => {
                    self.target_kind = self.line.strip_prefix(b"type ").and_then(super::parse_kind);
                    if self.target_kind.is_none() {
                        self.invalid = true;
                    }
                }
                2 => {
                    if self.line == b"tag " {
                        if let Some(oid) = self.target {
                            self.edge(oid, self.target_kind)?;
                            self.done = true;
                        } else {
                            self.invalid = true;
                        }
                    } else {
                        self.invalid = true;
                    }
                }
                _ => self.invalid = true,
            }
            self.line_number += 1;
        }
        self.line.clear();
        Ok(())
    }
    fn tree_byte(&mut self, byte: u8) -> Result<(), TryReserveError> {
        match self.tree_state {
            0 => {
                if byte == b' ' {
                    if self.digits == 0 || self.mode > 0o177777 {
                        self.invalid = true;
                    } else {
                        self.tree_state = 1;
                    }
                } else if (b'0'..=b'7').contains(&byte) {
                    match self
                        .mode
                        .checked_mul(8)
                        .and_then(|mode| mode.checked_add(u32::from(byte - b'0')))
                    {
                        Some(mode) => {
                            self.mode = mode;
                            self.digits += 1;
                        }
                        None => self.invalid = true,
                    }
                } else {
                    self.invalid = true;
                }
            }
            1 => {
                if byte == 0 {
                    if self.name_bytes == 0 || (self.dot_name && self.name_bytes <= 2) {
                        self.invalid = true;
                    } else {
                        self.tree_state = 2;
                    }
                } else {
                    if byte == b'/' {
                        self.invalid = true;
                    }
                    self.dot_name &= byte == b'.';
                    self.name_bytes += 1;
                }
            }
            2 => {
                self.oid[self.oid_bytes] = byte;
                self.oid_bytes += 1;
                if self.oid_bytes == self.format.bytes() {
                    let Some(oid) = ObjectId::try_from(&self.oid[..self.oid_bytes])
                        .ok()
                        .filter(|oid| !oid.is_zero())
                    else {
                        self.invalid = true;
                        return Ok(());
                    };
                    match self.mode & 0o170000 {
                        0o040000 => self.edge(oid, Some(ObjectKind::Tree))?,
                        0o100000 | 0o120000 => self.edge(oid, Some(ObjectKind::Blob))?,
                        0o160000 => {}
                        _ => self.invalid = true,
                    }
                    self.mode = 0;
                    self.digits = 0;
                    self.name_bytes = 0;
                    self.dot_name = true;
                    self.oid_bytes = 0;
                    self.tree_state = 0;
                }
            }
            _ => self.invalid = true,
        }
        Ok(())
    }
    pub(crate) fn finish(mut self) -> Option<Vec<Edge>> {
        if self.invalid {
            return None;
        }
        match self.kind {
            ObjectKind::Tree if self.tree_state != 0 || self.digits != 0 => return None,
            ObjectKind::Commit
                if self.line_number == 0 || (!self.done && self.line.starts_with(b"parent ")) =>
            {
                return None;
            }
            ObjectKind::Tag if !self.done => return None,
            _ => {}
        }
        self.edges.sort_unstable();
        self.edges.dedup();
        Some(self.edges)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn streaming_matches_whole_body_graph_parser_at_every_chunk_boundary() {
        for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
            let oid = crate::object_id(format, ObjectKind::Blob, b"graph leaf");
            let mut tree = b"100644 file\0".to_vec();
            tree.extend_from_slice(&oid);
            let commit = format!(
                "tree {}\nparent {}\nauthor Person\n\nmessage",
                hex::encode(oid),
                hex::encode(oid)
            )
            .into_bytes();
            let tag = format!(
                "object {}\ntype commit\ntag release\n\nmessage",
                hex::encode(oid)
            )
            .into_bytes();
            for (kind, body) in [
                (ObjectKind::Blob, b"content".to_vec()),
                (ObjectKind::Tree, tree),
                (ObjectKind::Commit, commit),
                (ObjectKind::Tag, tag),
            ] {
                for size in 1..body.len() + 1 {
                    let mut parser = StreamGraph::new(format, kind);
                    for chunk in body.chunks(size) {
                        parser.feed(chunk).unwrap();
                    }
                    assert_eq!(parser.finish(), super::super::edges(format, kind, &body));
                }
            }
        }
    }
    #[test]
    fn malformed_graphs_fail_closed_across_fragmented_input() {
        let oid = crate::object_id(ObjectFormat::Sha1, ObjectKind::Blob, b"leaf");
        let mut malformed_tree = b"100644 ../bad\0".to_vec();
        malformed_tree.extend_from_slice(&oid);
        let cases = [
            (ObjectKind::Tree, malformed_tree),
            (ObjectKind::Tree, b"100644 name\0short".to_vec()),
            (
                ObjectKind::Commit,
                format!("tree {}\nparent missing", hex::encode(oid)).into_bytes(),
            ),
            (
                ObjectKind::Tag,
                format!("object {}\ntype wrong\ntag release\n", hex::encode(oid)).into_bytes(),
            ),
            (
                ObjectKind::Tag,
                format!("object {}\ntype blob\nwrong release\n", hex::encode(oid)).into_bytes(),
            ),
        ];
        for (kind, body) in cases {
            for size in [1, 7, 128] {
                let mut parser = StreamGraph::new(ObjectFormat::Sha1, kind);
                for chunk in body.chunks(size) {
                    parser.feed(chunk).unwrap();
                }
                assert!(parser.finish().is_none());
            }
        }
    }
}
