//! Bounded typed graph pages from preferred certified metadata, never legacy SQL.
use super::*;
use crate::packs::metadata::TypedEdge;

pub const MAX_EDGE_PARENTS: usize = 128;
#[derive(Debug, PartialEq, Eq)]
pub struct ServingEdgePage {
    pub headers: Vec<(crate::ObjectId, Option<ObjectHeader>)>,
    pub edges: Vec<(crate::ObjectId, TypedEdge)>,
    /// Conservative continuation: an exact-full page may need one empty read.
    pub next_after: Option<(crate::ObjectId, crate::ObjectId)>,
}
impl ServingPin {
    pub async fn edges_page(
        &self,
        actor: Option<String>,
        ids: &[crate::ObjectId],
        after: Option<(crate::ObjectId, crate::ObjectId)>,
    ) -> Result<ServingEdgePage, ServingReadError> {
        if ids.is_empty()
            || ids.len() > MAX_EDGE_PARENTS
            || ids
                .iter()
                .any(|oid| oid.is_zero() || oid.format() != self.inner.lease.format)
            || ids.windows(2).any(|pair| pair[0] >= pair[1])
            || after.is_some_and(|(parent, child)| {
                ids.binary_search(&parent).is_err()
                    || child.is_zero()
                    || child.format() != self.inner.lease.format
            })
        {
            return Err(ServingReadError::Context);
        }
        let ids = ids.to_vec();
        self.read_owned(actor, move |inner, deadline, permit| async move {
            let reader = inner.catalog().await?;
            let mut output = ServingEdgePage {
                headers: Vec::new(),
                edges: Vec::new(),
                next_after: None,
            };
            for parent in ids {
                if after.is_some_and(|(cursor, _)| parent < cursor) {
                    continue;
                }
                if Instant::now() >= deadline {
                    return Err(ServingReadError::Inactive);
                }
                let Some(object) = reader
                    .lookup(parent, &*inner.context.files, &*inner.context.files)
                    .await?
                else {
                    output.headers.push((parent, None));
                    continue;
                };
                output.headers.push((parent, Some(object.entry.header)));
                let metadata = object.source.metadata;
                let cursor = after
                    .filter(|(cursor, _)| *cursor == parent)
                    .map(|(_, child)| child);
                let owner = (inner.child(), permit.clone());
                let mut edges = tokio::task::spawn_blocking(move || {
                    let _owner = owner;
                    metadata.edges_after(parent, cursor)
                })
                .await?
                .map_err(crate::packs::directory::index::IndexError::from)?;
                let keep = edges.len().min(PAGE_OBJECTS - output.edges.len());
                edges.truncate(keep);
                output
                    .edges
                    .extend(edges.into_iter().map(|edge| (parent, edge)));
                if output.edges.len() == PAGE_OBJECTS {
                    output.next_after = output
                        .edges
                        .last()
                        .map(|(parent, edge)| (*parent, edge.child));
                    break;
                }
            }
            Ok(output)
        })
        .await
    }
}
