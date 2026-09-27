//! Bounded immutable object pages for cold cache hydration.

use crab_cell_runtime::{
    Error, InvocationError, Observed, Receipt, primitives::sql::SqlBatch,
    primitives::sql::SqlResultSet, primitives::sql::SqlStatement, primitives::sql::SqlValue,
};

use crate::{
    INLINE_OBJECT_LIMIT, MAX_SQLITE_OBJECT_BYTES, ObjectKind, ObjectStorage, RepositoryCell,
    StoredObject, large_blob::MAX_EXTERNAL_BLOB_BYTES, object_batch::MAX_OBJECTS, object_id,
};

pub(crate) struct ObjectHeaders {
    pub(crate) objects: Observed<Vec<[u8; 20]>>,
    pub(crate) through: i64,
}

const CHANGED_HEADERS: &str = "SELECT sequence, oid, CASE WHEN storage = 'inline' THEN size ELSE 0 END FROM objects WHERE sequence > ?1 AND sequence <= ?2 ORDER BY sequence LIMIT ?3";

impl RepositoryCell {
    /// Reads at most 128 objects and 768 KiB of inline bodies, in OID order.
    ///
    /// Continue after the last OID until an empty page, including after short
    /// pages. Objects are immutable; a future collector must fence this read.
    pub async fn object_page(
        &self,
        after: Option<[u8; 20]>,
    ) -> Result<Observed<Vec<StoredObject>>, InvocationError<Vec<SqlResultSet>>> {
        let headers = self.read_object_headers(None, SqlStatement {
            sql: "SELECT sequence, oid, CASE WHEN storage = 'inline' THEN size ELSE 0 END FROM objects WHERE oid > ?1 ORDER BY oid LIMIT ?2".into(),
            parameters: vec![SqlValue::Blob(after.map_or_else(Vec::new, |oid| oid.to_vec())), SqlValue::Integer(MAX_OBJECTS as i64)],
        }).await?;
        self.object_records(headers.objects).await
    }

    pub(crate) async fn selected_objects(
        &self,
        ids: &[[u8; 20]],
    ) -> Result<Vec<StoredObject>, InvocationError<Vec<SqlResultSet>>> {
        if ids.is_empty() || ids.len() > MAX_OBJECTS {
            return Err(InvocationError::NotStarted(Error::Command(
                "invalid object selection",
            )));
        }
        let placeholders = vec!["?"; ids.len()].join(",");
        let headers = self.read_object_headers(None, SqlStatement {
            sql: format!("SELECT sequence, oid, CASE WHEN storage = 'inline' THEN size ELSE 0 END FROM objects WHERE oid IN ({placeholders}) ORDER BY oid"),
            parameters: ids.iter().map(|oid| SqlValue::Blob(oid.to_vec())).collect(),
        }).await?;
        Ok(self.object_records(headers.objects).await?.output)
    }

    pub(crate) async fn object_high_water(
        &self,
    ) -> Result<Observed<i64>, InvocationError<Vec<SqlResultSet>>> {
        let result = self
            .sql
            .query(
                None,
                SqlBatch {
                    statements: vec![SqlStatement {
                        sql: "SELECT COALESCE(MAX(sequence), 0) FROM objects".into(),
                        parameters: vec![],
                    }],
                },
            )
            .await?;
        let row = result.output.first().and_then(|set| set.rows.first());
        let Some([SqlValue::Integer(sequence)]) = row.map(Vec::as_slice) else {
            return Err(InvocationError::NotStarted(Error::Command(
                "invalid object high water",
            )));
        };
        Ok(Observed {
            output: *sequence,
            receipt: result.receipt,
        })
    }

    pub(crate) async fn object_headers(
        &self,
        after: i64,
        high_water: &Observed<i64>,
    ) -> Result<ObjectHeaders, InvocationError<Vec<SqlResultSet>>> {
        self.read_object_headers(
            Some(high_water.receipt),
            SqlStatement {
                sql: CHANGED_HEADERS.into(),
                parameters: vec![
                    SqlValue::Integer(after),
                    SqlValue::Integer(high_water.output),
                    SqlValue::Integer(MAX_OBJECTS as i64),
                ],
            },
        )
        .await
    }

    async fn read_object_headers(
        &self,
        minimum: Option<Receipt>,
        statement: SqlStatement,
    ) -> Result<ObjectHeaders, InvocationError<Vec<SqlResultSet>>> {
        let headers = self
            .sql
            .query(
                minimum,
                SqlBatch {
                    statements: vec![statement],
                },
            )
            .await?;
        let rows = headers
            .output
            .first()
            .ok_or_else(|| InvocationError::NotStarted(Error::Command("missing object headers")))?;
        let (ids, through) = decode_headers(&rows.rows).map_err(InvocationError::NotStarted)?;
        Ok(ObjectHeaders {
            objects: Observed {
                output: ids,
                receipt: headers.receipt,
            },
            through,
        })
    }

    pub(crate) async fn object_records(
        &self,
        headers: Observed<Vec<[u8; 20]>>,
    ) -> Result<Observed<Vec<StoredObject>>, InvocationError<Vec<SqlResultSet>>> {
        // Callers may remove already cached IDs, but never add IDs: the header
        // query's size bound and receipt protect this body read's wire ceiling.
        let mut ids = headers.output;
        ids.sort_unstable();
        if ids.is_empty() {
            return Ok(Observed {
                output: Vec::new(),
                receipt: headers.receipt,
            });
        }
        let placeholders = vec!["?"; ids.len()].join(",");
        // Immutable records bind this second read to the selected headers. The
        // payload bound leaves room for record metadata under Crab's 1 MiB cap.
        let result = self.sql.query(Some(headers.receipt), SqlBatch {
            statements: vec![SqlStatement {
                sql: format!("SELECT oid, kind, size, digest, storage, body, external_sha256, chunk_id FROM objects WHERE oid IN ({placeholders}) ORDER BY oid"),
                parameters: ids.iter().map(|oid| SqlValue::Blob(oid.to_vec())).collect(),
            }],
        }).await?;
        let rows = result
            .output
            .into_iter()
            .next()
            .ok_or_else(|| InvocationError::NotStarted(Error::Command("missing object page")))?
            .rows;
        // Hash a whole bounded page off the async executor. Move SQL bodies into
        // the result so page verification does not duplicate their payloads.
        let output = tokio::task::spawn_blocking(move || {
            let objects = rows
                .into_iter()
                .map(decode_object)
                .collect::<crab_cell_runtime::Result<Vec<_>>>()?;
            if objects.iter().map(|object| object.oid).ne(ids) {
                return Err(Error::Command("objects changed during page read"));
            }
            Ok(objects)
        })
        .await
        .map_err(|error| {
            InvocationError::NotStarted(Error::Facility {
                name: "object page verification",
                source: Box::new(error),
            })
        })?
        .map_err(InvocationError::NotStarted)?;
        Ok(Observed {
            output,
            receipt: result.receipt,
        })
    }
}

fn decode_headers(rows: &[Vec<SqlValue>]) -> crab_cell_runtime::Result<(Vec<[u8; 20]>, i64)> {
    let mut ids = Vec::new();
    let mut bytes = 0;
    let mut through = 0;
    for row in rows {
        let [
            SqlValue::Integer(sequence),
            SqlValue::Blob(oid),
            SqlValue::Integer(size),
        ] = row.as_slice()
        else {
            return Err(Error::Command("invalid object header"));
        };
        if *sequence <= 0 {
            return Err(Error::Command("invalid object sequence"));
        }
        let oid = oid
            .as_slice()
            .try_into()
            .map_err(|_| Error::Command("invalid stored object ID"))?;
        let size = usize::try_from(*size)
            .ok()
            .filter(|size| *size <= INLINE_OBJECT_LIMIT)
            .ok_or(Error::Command("invalid inline object size"))?;
        if size > INLINE_OBJECT_LIMIT - bytes {
            break;
        }
        bytes += size;
        ids.push(oid);
        through = *sequence;
    }
    Ok((ids, through))
}

fn decode_object(row: Vec<SqlValue>) -> crab_cell_runtime::Result<StoredObject> {
    let row: [SqlValue; 8] = row
        .try_into()
        .map_err(|_| Error::Command("invalid stored object row"))?;
    let [
        SqlValue::Blob(oid),
        SqlValue::Text(kind),
        SqlValue::Integer(size),
        SqlValue::Blob(digest),
        SqlValue::Text(storage),
        body,
        external_sha256,
        chunk_id,
    ] = row
    else {
        return Err(Error::Command("invalid stored object row"));
    };
    let oid: [u8; 20] = oid
        .try_into()
        .map_err(|_| Error::Command("invalid stored object ID"))?;
    let digest: [u8; 32] = digest
        .try_into()
        .map_err(|_| Error::Command("invalid object digest"))?;
    let kind = match kind.as_str() {
        "blob" => ObjectKind::Blob,
        "tree" => ObjectKind::Tree,
        "commit" => ObjectKind::Commit,
        "tag" => ObjectKind::Tag,
        _ => return Err(Error::Command("invalid stored object kind")),
    };
    let storage = match (storage.as_str(), body, external_sha256, chunk_id) {
        ("inline", SqlValue::Blob(body), SqlValue::Null, SqlValue::Null)
            if usize::try_from(size).ok() == Some(body.len())
                && body.len() <= INLINE_OBJECT_LIMIT
                && object_id(kind, &body) == oid
                && blake3::hash(&body).as_bytes() == &digest =>
        {
            ObjectStorage::Inline(body)
        }
        ("external", SqlValue::Null, SqlValue::Blob(sha256), SqlValue::Null)
            if kind == ObjectKind::Blob && (0..=MAX_EXTERNAL_BLOB_BYTES as i64).contains(&size) =>
        {
            ObjectStorage::External {
                size: size as u64,
                blake3: digest,
                sha256: sha256
                    .try_into()
                    .map_err(|_| Error::Command("invalid SHA-256 digest"))?,
            }
        }
        ("chunked", SqlValue::Null, SqlValue::Null, SqlValue::Blob(upload))
            if kind != ObjectKind::Blob
                && size > INLINE_OBJECT_LIMIT as i64
                && size <= MAX_SQLITE_OBJECT_BYTES as i64 =>
        {
            ObjectStorage::Chunked {
                upload: upload
                    .try_into()
                    .map_err(|_| Error::Command("invalid object chunk reference"))?,
                size: size as u64,
                blake3: digest,
            }
        }
        _ => return Err(Error::Command("corrupt stored object")),
    };
    Ok(StoredObject { oid, kind, storage })
}

#[cfg(test)]
mod tests;
