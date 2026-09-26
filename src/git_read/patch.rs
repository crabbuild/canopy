use super::*;

const MAX_LINES: usize = 20_000;
const MAX_TRACE: usize = 1_048_576;
const MAX_COMPARE_BYTES: usize = 128 * 1024 * 1024;
const MAX_OUTPUT_LINES: usize = 8192;
const MAX_OUTPUT_BYTES: usize = 2 * 1024 * 1024 - 64 * 1024;
const CONTEXT: usize = 3;

#[derive(Serialize)]
pub(crate) struct Patch {
    revision: PullRevision,
    merge_base: String,
    path_base64: String,
    path: Option<String>,
    before: Option<Entry>,
    after: Option<Entry>,
    status: &'static str,
    hunks: Vec<Hunk>,
}
#[derive(Debug, Serialize)]
struct Hunk {
    old_start: usize,
    old_lines: usize,
    new_start: usize,
    new_lines: usize,
    lines: Vec<Line>,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
enum Kind {
    Context,
    Delete,
    Add,
}
#[derive(Debug, Serialize)]
struct Line {
    kind: Kind,
    text: String,
    no_newline: bool,
}
#[derive(Clone, Copy)]
struct Edit {
    kind: Kind,
    index: usize,
}

impl Reader {
    pub(crate) async fn patch(
        mut self,
        actor: &str,
        number: i64,
        target: ComparisonTarget,
        encoded_path: &str,
    ) -> Result<Patch, ReadError> {
        let path = path(encoded_path)?;
        let revision = self.authorize(actor, number, &target).await?;
        let (merge_base, old_tree, new_tree) = self
            .roots(oid(&revision.base_oid)?, oid(&revision.source_oid)?)
            .await?;
        let before = self
            .resolve(old_tree, &path)
            .await?
            .filter(|node| !node.is_tree());
        let after = self
            .resolve(new_tree, &path)
            .await?
            .filter(|node| !node.is_tree());
        if before.is_none() && after.is_none() {
            return Err(ReadError::Missing);
        }
        let (status, hunks) = if [before, after]
            .into_iter()
            .flatten()
            .any(|node| node.mode & 0o170000 == 0o160000)
        {
            ("gitlink", Vec::new())
        } else {
            let old = self.patch_body(before).await?;
            let new = self.patch_body(after).await?;
            match (old, new) {
                (Some(old), Some(new)) if text(&old) && text(&new) => {
                    let admission = Arc::clone(&self.admission);
                    let hunks = tokio::task::spawn_blocking(move || {
                        // Cancellation cannot stop blocking work. Its bounded
                        // search keeps the shared transfer permit until done.
                        let _admission = admission;
                        diff(&old, &new)
                    })
                    .await??;
                    ("text", hunks)
                }
                (Some(_), Some(_)) => ("binary", Vec::new()),
                _ => ("too_large", Vec::new()),
            }
        };
        self.authorize(actor, number, &target).await?;
        Ok(Patch {
            revision,
            merge_base: hex::encode(merge_base),
            path_base64: URL_SAFE_NO_PAD.encode(&path),
            path: String::from_utf8(path).ok(),
            before: before.map(Entry::from),
            after: after.map(Entry::from),
            status,
            hunks,
        })
    }
    async fn patch_body(&mut self, node: Option<Node>) -> Result<Option<Vec<u8>>, ReadError> {
        let Some(node) = node else {
            return Ok(Some(Vec::new()));
        };
        if self.size(node.oid, ObjectKind::Blob).await? > PREVIEW_BYTES {
            return Ok(None);
        }
        self.body(node.oid, ObjectKind::Blob).await.map(Some)
    }
}
fn text(bytes: &[u8]) -> bool {
    !bytes.contains(&0) && std::str::from_utf8(bytes).is_ok()
}
fn diff(old: &[u8], new: &[u8]) -> Result<Vec<Hunk>, ReadError> {
    let old: Vec<_> = std::str::from_utf8(old)
        .map_err(|_| ReadError::Malformed)?
        .split_inclusive('\n')
        .collect();
    let new: Vec<_> = std::str::from_utf8(new)
        .map_err(|_| ReadError::Malformed)?
        .split_inclusive('\n')
        .collect();
    if old.len() > MAX_LINES || new.len() > MAX_LINES {
        return Err(ReadError::TooLarge);
    }
    let edits = shortest_edits(&old, &new)?;
    hunks(&old, &new, &edits)
}

fn shortest_edits(old: &[&str], new: &[&str]) -> Result<Vec<Edit>, ReadError> {
    let prefix = old.iter().zip(new).take_while(|(a, b)| a == b).count();
    let (mut old_end, mut new_end) = (old.len(), new.len());
    while old_end > prefix && new_end > prefix && old[old_end - 1] == new[new_end - 1] {
        old_end -= 1;
        new_end -= 1;
    }
    let mut edits: Vec<_> = (0..prefix)
        .map(|index| Edit {
            kind: Kind::Context,
            index,
        })
        .collect();
    let middle = search(&old[prefix..old_end], &new[prefix..new_end])?;
    edits.extend(middle.into_iter().map(|edit| Edit {
        kind: edit.kind,
        index: edit.index + prefix,
    }));
    edits.extend((old_end..old.len()).map(|index| Edit {
        kind: Kind::Context,
        index,
    }));
    Ok(edits)
}
fn search(old: &[&str], new: &[&str]) -> Result<Vec<Edit>, ReadError> {
    if old.is_empty() || new.is_empty() {
        let (kind, count) = if old.is_empty() {
            (Kind::Add, new.len())
        } else {
            (Kind::Delete, old.len())
        };
        return Ok((0..count).map(|index| Edit { kind, index }).collect());
    }
    // Myers frontiers store only reachable diagonals (-d, -d+2, ..., d).
    // Bound both the retained trace and bytes compared on matching runs.
    let mut trace: Vec<Vec<usize>> = Vec::new();
    let mut retained = 0;
    let mut compared = 0;
    for depth in 0..=old.len() + new.len() {
        retained += depth + 1;
        if retained > MAX_TRACE {
            return Err(ReadError::TooLarge);
        }
        let mut frontier = Vec::with_capacity(depth + 1);
        for index in 0..=depth {
            let diagonal = 2 * index as isize - depth as isize;
            let mut x = if depth == 0 {
                0
            } else {
                let previous = &trace[depth - 1];
                if index == 0 || (index != depth && previous[index - 1] < previous[index]) {
                    previous[index]
                } else {
                    previous[index - 1] + 1
                }
            };
            let mut y = x as isize - diagonal;
            while x < old.len() && y >= 0 && (y as usize) < new.len() {
                compared += old[x].len().min(new[y as usize].len()) + 1;
                if compared > MAX_COMPARE_BYTES {
                    return Err(ReadError::TooLarge);
                }
                if old[x] != new[y as usize] {
                    break;
                }
                x += 1;
                y += 1;
            }
            frontier.push(x);
            if x == old.len() && y == new.len() as isize {
                trace.push(frontier);
                return Ok(backtrack(&trace, old.len(), new.len()));
            }
        }
        trace.push(frontier);
    }
    Err(ReadError::Malformed)
}
fn backtrack(trace: &[Vec<usize>], mut x: usize, mut y: usize) -> Vec<Edit> {
    let mut edits = Vec::with_capacity(x + y);
    for depth in (1..trace.len()).rev() {
        let index = ((x as isize - y as isize + depth as isize) / 2) as usize;
        let previous = &trace[depth - 1];
        let addition = index == 0 || (index != depth && previous[index - 1] < previous[index]);
        let old_index = if addition { index } else { index - 1 };
        let old_x = previous[old_index];
        let old_y = (old_x as isize - (2 * old_index as isize - (depth - 1) as isize)) as usize;
        while x > old_x && y > old_y {
            x -= 1;
            y -= 1;
            edits.push(Edit {
                kind: Kind::Context,
                index: x,
            });
        }
        if addition {
            y -= 1;
            edits.push(Edit {
                kind: Kind::Add,
                index: y,
            });
        } else {
            x -= 1;
            edits.push(Edit {
                kind: Kind::Delete,
                index: x,
            });
        }
    }
    while x > 0 && y > 0 {
        x -= 1;
        y -= 1;
        edits.push(Edit {
            kind: Kind::Context,
            index: x,
        });
    }
    edits.reverse();
    edits
}
fn hunks(old: &[&str], new: &[&str], edits: &[Edit]) -> Result<Vec<Hunk>, ReadError> {
    let mut ranges: Vec<(usize, usize)> = Vec::new();
    for (index, _) in edits
        .iter()
        .enumerate()
        .filter(|(_, edit)| edit.kind != Kind::Context)
    {
        let start = index.saturating_sub(CONTEXT);
        let end = (index + CONTEXT + 1).min(edits.len());
        if let Some(last) = ranges.last_mut().filter(|last| start <= last.1) {
            last.1 = end;
        } else {
            ranges.push((start, end));
        }
    }
    let mut result = Vec::new();
    let (mut cursor, mut old_line, mut new_line) = (0, 1, 1);
    let (mut output_lines, mut output_bytes) = (0, 0);
    for (start, end) in ranges {
        for edit in &edits[cursor..start] {
            old_line += usize::from(edit.kind != Kind::Add);
            new_line += usize::from(edit.kind != Kind::Delete);
        }
        let mut hunk = Hunk {
            old_start: old_line,
            old_lines: 0,
            new_start: new_line,
            new_lines: 0,
            lines: Vec::new(),
        };
        output_bytes += 128;
        for edit in &edits[start..end] {
            let line = if edit.kind == Kind::Add {
                new[edit.index]
            } else {
                old[edit.index]
            };
            output_lines += 1;
            // Reserve worst-case JSON escaping plus field/hunk overhead. No
            // partial patch is returned when output exceeds the response budget.
            output_bytes += line.len() * 6 + 96;
            if output_lines > MAX_OUTPUT_LINES || output_bytes > MAX_OUTPUT_BYTES {
                return Err(ReadError::TooLarge);
            }
            hunk.lines.push(Line {
                kind: edit.kind,
                text: line.strip_suffix('\n').unwrap_or(line).into(),
                no_newline: !line.ends_with('\n'),
            });
            hunk.old_lines += usize::from(edit.kind != Kind::Add);
            hunk.new_lines += usize::from(edit.kind != Kind::Delete);
        }
        old_line += hunk.old_lines;
        new_line += hunk.new_lines;
        if hunk.old_lines == 0 {
            hunk.old_start -= 1;
        }
        if hunk.new_lines == 0 {
            hunk.new_start -= 1;
        }
        result.push(hunk);
        cursor = end;
    }
    Ok(result)
}

#[cfg(test)]
mod tests;
