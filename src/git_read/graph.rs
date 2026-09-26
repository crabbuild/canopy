use super::*;
use std::collections::{HashMap, HashSet};

const MAX_COMMITS: usize = 100_000;
const MAX_EDGES: usize = 250_000;
const GROUP: usize = 128;
const PAGE: usize = 512;
type Graph = HashMap<Oid, Vec<Oid>>;

impl Reader {
    pub(super) async fn merge_base(&self, base: Oid, source: Oid) -> Result<Oid, ReadError> {
        if base == source {
            return Ok(base);
        }
        let mut graph = Graph::new();
        let mut discovered = HashSet::from([base, source]);
        let mut pending = vec![base, source];
        let mut edges = 0;
        while !pending.is_empty() {
            let group: Vec<_> = pending
                .drain(pending.len().saturating_sub(GROUP)..)
                .collect();
            for oid in &group {
                graph.insert(*oid, Vec::new());
            }
            let mut cursor: Option<(Oid, Oid)> = None;
            loop {
                let placeholders = (3..group.len() + 3)
                    .map(|n| format!("?{n}"))
                    .collect::<Vec<_>>()
                    .join(",");
                let mut parameters = vec![
                    SqlValue::Blob(cursor.map_or_else(Vec::new, |(child, _)| child.to_vec())),
                    SqlValue::Blob(cursor.map_or_else(Vec::new, |(_, parent)| parent.to_vec())),
                ];
                parameters.extend(group.iter().map(|oid| SqlValue::Blob(oid.to_vec())));
                // Parent rows come only from verified commit certificates. Keyset
                // paging includes every parent even for unusually wide merges.
                let result = self.repository.sql.query(None,SqlBatch { statements:vec![SqlStatement {
                    sql:format!("SELECT child, parent FROM commit_parents WHERE child IN ({placeholders}) AND (child > ?1 OR (child = ?1 AND parent > ?2)) ORDER BY child, parent LIMIT {PAGE}"),parameters,
                }] }).await?;
                let rows = &result.output.first().ok_or(ReadError::Malformed)?.rows;
                for row in rows {
                    let [SqlValue::Blob(child), SqlValue::Blob(parent)] = row.as_slice() else {
                        return Err(ReadError::Malformed);
                    };
                    let child: Oid = child
                        .as_slice()
                        .try_into()
                        .map_err(|_| ReadError::Malformed)?;
                    let parent: Oid = parent
                        .as_slice()
                        .try_into()
                        .map_err(|_| ReadError::Malformed)?;
                    edges += 1;
                    if edges > MAX_EDGES {
                        return Err(ReadError::TooLarge);
                    }
                    graph
                        .get_mut(&child)
                        .ok_or(ReadError::Malformed)?
                        .push(parent);
                    cursor = Some((child, parent));
                    if discovered.insert(parent) {
                        if discovered.len() > MAX_COMMITS {
                            return Err(ReadError::TooLarge);
                        }
                        pending.push(parent);
                    }
                }
                if rows.len() < PAGE {
                    break;
                }
            }
        }
        let admission = Arc::clone(&self.admission);
        tokio::task::spawn_blocking(move || {
            let _admission = admission;
            best_common(graph, base, source)
        })
        .await?
    }
}
fn best_common(graph: Graph, base: Oid, source: Oid) -> Result<Oid, ReadError> {
    let mut flags = HashMap::<Oid, u8>::new();
    let mut pending = vec![(base, 1), (source, 2)];
    while let Some((oid, flag)) = pending.pop() {
        let current = flags.entry(oid).or_default();
        if *current & flag == flag {
            continue;
        }
        *current |= flag;
        for parent in graph.get(&oid).ok_or(ReadError::Malformed)? {
            pending.push((*parent, flag));
        }
    }
    // Every proper ancestor of any common ancestor is inferior. Mark that
    // closure once; the remaining common nodes are exactly Git's best bases.
    let common: Vec<_> = flags
        .iter()
        .filter_map(|(oid, flag)| (*flag == 3).then_some(*oid))
        .collect();
    let mut inferior = HashSet::new();
    let mut pending = Vec::new();
    for oid in &common {
        pending.extend(graph.get(oid).ok_or(ReadError::Malformed)?.iter().copied());
    }
    while let Some(oid) = pending.pop() {
        if !inferior.insert(oid) {
            continue;
        }
        pending.extend(graph.get(&oid).ok_or(ReadError::Malformed)?.iter().copied());
    }
    let mut best = common.into_iter().filter(|oid| !inferior.contains(oid));
    let first = best.next().ok_or(ReadError::Unrelated)?;
    if best.next().is_some() {
        return Err(ReadError::Ambiguous);
    }
    Ok(first)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn common_ancestor_selection_handles_merges_and_criss_cross() {
        let (root, a, b, c, d) = ([1; 20], [2; 20], [3; 20], [4; 20], [5; 20]);
        let graph = Graph::from([
            (root, vec![]),
            (a, vec![root]),
            (b, vec![root]),
            (c, vec![a, b]),
            (d, vec![a]),
        ]);
        assert_eq!(best_common(graph.clone(), c, d).unwrap(), a);
        assert_eq!(best_common(graph.clone(), c, b).unwrap(), b);
        let mut cross = graph;
        cross.insert(d, vec![b, a]);
        assert!(matches!(
            best_common(cross, c, d),
            Err(ReadError::Ambiguous)
        ));
        assert!(matches!(
            best_common(Graph::from([(a, vec![]), (b, vec![])]), a, b),
            Err(ReadError::Unrelated)
        ));
    }
}
