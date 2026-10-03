//! Constant-space structural extraction. Names, messages and signatures are
//! never retained. Native body bytes preserve commit-parent order; this stream
//! emits dependency occurrences for disk-backed typed deduplication.

use crate::{ObjectFormat, ObjectId, ObjectKind, packs::metadata::TypedEdge};

pub(crate) const CHUNK_BYTES: usize = 64 << 10;
#[derive(Debug, thiserror::Error)]
#[error("malformed structural Git object stream")]
pub(crate) struct GraphStreamError;

#[derive(Clone, Copy)]
enum Role {
    Tree,
    Parent,
    TagObject,
    TagType,
    TagName,
}
impl Role {
    fn prefix(self) -> &'static [u8] {
        match self {
            Self::Tree => b"tree ",
            Self::Parent => b"parent ",
            Self::TagObject => b"object ",
            Self::TagType => b"type ",
            Self::TagName => b"tag ",
        }
    }
}
enum State {
    Ignore,
    Prefix(Role, usize),
    Field(Role, [u8; 64], usize),
    TagName,
    TreeMode(u32, bool),
    TreeName(Option<ObjectKind>, u64, bool),
    TreeOid(Option<ObjectKind>, [u8; 32], usize),
}
pub(crate) struct EdgeParser {
    format: ObjectFormat,
    state: State,
    tag_target: Option<ObjectId>,
    poisoned: bool,
}
impl EdgeParser {
    pub(crate) fn new(format: ObjectFormat, kind: ObjectKind) -> Self {
        let state = match kind {
            ObjectKind::Blob => State::Ignore,
            ObjectKind::Tree => State::TreeMode(0, false),
            ObjectKind::Commit => State::Prefix(Role::Tree, 0),
            ObjectKind::Tag => State::Prefix(Role::TagObject, 0),
        };
        Self {
            format,
            state,
            tag_target: None,
            poisoned: false,
        }
    }
    fn oid(&self, bytes: &[u8], hexadecimal: bool) -> Result<ObjectId, GraphStreamError> {
        let oid = if hexadecimal {
            ObjectId::from_hex(bytes)
        } else {
            ObjectId::try_from(bytes)
        }
        .map_err(|_| GraphStreamError)?;
        if oid.format() != self.format || oid.is_zero() {
            return Err(GraphStreamError);
        }
        Ok(oid)
    }
    pub(crate) fn feed(
        &mut self,
        chunk: &[u8],
        mut emit: impl FnMut(TypedEdge),
    ) -> Result<(), GraphStreamError> {
        if self.poisoned || chunk.len() > CHUNK_BYTES {
            self.poisoned = true;
            return Err(GraphStreamError);
        }
        self.poisoned = true;
        if matches!(self.state, State::Ignore) {
            self.poisoned = false;
            return Ok(());
        }
        for &byte in chunk {
            let state = std::mem::replace(&mut self.state, State::Ignore);
            self.state = match state {
                State::Ignore => State::Ignore,
                State::Prefix(role, at) => {
                    if byte != role.prefix()[at] {
                        // Git stops recognizing parents at the first other
                        // header; later signatures/messages have no graph edges.
                        if matches!(role, Role::Parent) {
                            State::Ignore
                        } else {
                            return Err(GraphStreamError);
                        }
                    } else if at + 1 == role.prefix().len() {
                        if matches!(role, Role::TagName) {
                            State::TagName
                        } else {
                            State::Field(role, [0; 64], 0)
                        }
                    } else {
                        State::Prefix(role, at + 1)
                    }
                }
                State::Field(role, mut bytes, length) => {
                    if byte == b'\n' {
                        match role {
                            Role::Tree | Role::Parent => {
                                let child = self.oid(&bytes[..length], true)?;
                                emit(TypedEdge {
                                    child,
                                    expected_kind: if matches!(role, Role::Tree) {
                                        ObjectKind::Tree
                                    } else {
                                        ObjectKind::Commit
                                    },
                                });
                                State::Prefix(Role::Parent, 0)
                            }
                            Role::TagObject => {
                                self.tag_target = Some(self.oid(&bytes[..length], true)?);
                                State::Prefix(Role::TagType, 0)
                            }
                            Role::TagType => {
                                let expected_kind =
                                    super::parse_kind(&bytes[..length]).ok_or(GraphStreamError)?;
                                emit(TypedEdge {
                                    child: self.tag_target.ok_or(GraphStreamError)?,
                                    expected_kind,
                                });
                                State::Prefix(Role::TagName, 0)
                            }
                            Role::TagName => return Err(GraphStreamError),
                        }
                    } else {
                        let limit = if matches!(role, Role::TagType) {
                            6
                        } else {
                            self.format.bytes() * 2
                        };
                        if length >= limit {
                            return Err(GraphStreamError);
                        }
                        bytes[length] = byte;
                        State::Field(role, bytes, length + 1)
                    }
                }
                State::TagName => {
                    if byte == b'\n' {
                        State::Ignore
                    } else {
                        State::TagName
                    }
                }
                State::TreeMode(mode, digits) => {
                    if byte == b' ' {
                        if !digits {
                            return Err(GraphStreamError);
                        }
                        let kind = match mode & 0o170000 {
                            0o040000 => Some(ObjectKind::Tree),
                            0o100000 | 0o120000 => Some(ObjectKind::Blob),
                            0o160000 => None,
                            _ => return Err(GraphStreamError),
                        };
                        State::TreeName(kind, 0, true)
                    } else {
                        if !(b'0'..=b'7').contains(&byte) {
                            return Err(GraphStreamError);
                        }
                        let mode = mode
                            .checked_mul(8)
                            .and_then(|m| m.checked_add(u32::from(byte - b'0')))
                            .filter(|m| *m <= 0o177777)
                            .ok_or(GraphStreamError)?;
                        State::TreeMode(mode, true)
                    }
                }
                State::TreeName(kind, length, dots) => {
                    if byte == 0 {
                        if length == 0 || (dots && length <= 2) {
                            return Err(GraphStreamError);
                        }
                        State::TreeOid(kind, [0; 32], 0)
                    } else {
                        if byte == b'/' {
                            return Err(GraphStreamError);
                        }
                        State::TreeName(
                            kind,
                            length.checked_add(1).ok_or(GraphStreamError)?,
                            dots && byte == b'.',
                        )
                    }
                }
                State::TreeOid(kind, mut bytes, at) => {
                    bytes[at] = byte;
                    if at + 1 == self.format.bytes() {
                        let child = self.oid(&bytes[..at + 1], false)?;
                        if let Some(expected_kind) = kind {
                            emit(TypedEdge {
                                child,
                                expected_kind,
                            });
                        }
                        State::TreeMode(0, false)
                    } else {
                        State::TreeOid(kind, bytes, at + 1)
                    }
                }
            };
        }
        self.poisoned = false;
        Ok(())
    }
    pub(crate) fn finish(self) -> Result<(), GraphStreamError> {
        if self.poisoned
            || !matches!(
                self.state,
                State::Ignore | State::TreeMode(0, false) | State::Prefix(Role::Parent, _)
            )
        {
            return Err(GraphStreamError);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
