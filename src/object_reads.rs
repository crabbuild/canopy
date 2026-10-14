//! Bounded immutable object pages for cold cache hydration.

use cellule_runtime::{
    Error, InvocationError, Observed, Receipt, primitives::sql::SqlBatch,
    primitives::sql::SqlResultSet, primitives::sql::SqlStatement, primitives::sql::SqlValue,
};

use crate::{
    ObjectKind, ObjectStorage, PackedObject, RepositoryCell, StoredObject,
    object_batch::MAX_OBJECTS,
};

pub(crate) struct ObjectHeaders {
    pub(crate) objects: Observed<Vec<crate::ObjectId>>,
    pub(crate) through: i64,
}

const CHANGED_HEADERS: &str = "SELECT sequence, oid, 0 FROM objects WHERE sequence > ?1 AND sequence <= ?2 ORDER BY sequence LIMIT ?3";

impl RepositoryCell {
    /// Reads at most 512 immutable native-pack metadata records, in OID order.
    ///
    /// Continue after the last OID until an empty page, including after short
    /// pages. Objects are immutable; a future collector must fence this read.
    pub async fn object_page(
        &self,
        after: Option<crate::ObjectId>,
    ) -> Result<Observed<Vec<StoredObject>>, InvocationError<Vec<SqlResultSet>>> {
        let headers = self
            .read_object_headers(
                None,
                SqlStatement {
                    sql:
                        "SELECT sequence, oid, 0 FROM objects WHERE oid > ?1 ORDER BY oid LIMIT ?2"
                            .into(),
                    parameters: vec![
                        SqlValue::Blob(after.map_or_else(Vec::new, |oid| oid.to_vec())),
                        SqlValue::Integer(MAX_OBJECTS as i64),
                    ],
                },
            )
            .await?;
        self.object_records(headers.objects).await
    }

    pub(crate) async fn selected_objects(
        &self,
        ids: &[crate::ObjectId],
    ) -> Result<Vec<StoredObject>, InvocationError<Vec<SqlResultSet>>> {
        if ids.is_empty() || ids.len() > MAX_OBJECTS {
            return Err(InvocationError::NotStarted(Error::Command(
                "invalid object selection",
            )));
        }
        let placeholders = vec!["?"; ids.len()].join(",");
        let headers = self.read_object_headers(None, SqlStatement {
            sql: format!("SELECT sequence, oid, 0 FROM objects WHERE oid IN ({placeholders}) ORDER BY oid"),
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
        headers: Observed<Vec<crate::ObjectId>>,
    ) -> Result<Observed<Vec<StoredObject>>, InvocationError<Vec<SqlResultSet>>> {
        // Callers may remove already cached IDs, but never add IDs: the header
        // query's count bound and receipt protect this metadata read.
        let mut ids = headers.output;
        ids.sort_unstable();
        if ids.is_empty() {
            return Ok(Observed {
                output: Vec::new(),
                receipt: headers.receipt,
            });
        }
        let placeholders = vec!["?"; ids.len()].join(",");
        // Immutable records bind this second read to the selected headers.
        // Fixed metadata widths fit comfortably under Cellule's 1 MiB cap.
        let result = self.sql.query(Some(headers.receipt), SqlBatch {
            statements: vec![SqlStatement {
                sql: format!("SELECT o.oid, o.kind, o.size, o.digest, o.graph_valid, o.graph_count, o.graph_digest, p.format, p.digest, p.bytes, p.index_digest, p.index_bytes, o.rebase_metadata FROM objects o JOIN git_packs p ON p.digest = o.pack WHERE o.oid IN ({placeholders}) ORDER BY o.oid"),
                parameters: ids.iter().map(|oid| SqlValue::Blob(oid.to_vec())).collect(),
            }],
        }).await?;
        let rows = result
            .output
            .into_iter()
            .next()
            .ok_or_else(|| InvocationError::NotStarted(Error::Command("missing object page")))?
            .rows;
        // Decode the bounded metadata page off the async executor.
        let repository = self.id;
        let output = tokio::task::spawn_blocking(move || {
            let objects = rows
                .into_iter()
                .map(|row| decode_object(row, repository))
                .collect::<cellule_runtime::Result<Vec<_>>>()?;
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

fn decode_headers(rows: &[Vec<SqlValue>]) -> cellule_runtime::Result<(Vec<crate::ObjectId>, i64)> {
    let mut ids = Vec::new();
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
        if *size != 0 {
            return Err(Error::Command("unexpected metadata header payload"));
        }
        ids.push(oid);
        through = *sequence;
    }
    Ok((ids, through))
}

fn decode_object(
    row: Vec<SqlValue>,
    repository: [u8; 16],
) -> cellule_runtime::Result<StoredObject> {
    let [
        SqlValue::Blob(oid),
        SqlValue::Text(kind),
        SqlValue::Integer(size),
        SqlValue::Blob(digest),
        SqlValue::Integer(valid),
        SqlValue::Integer(count),
        SqlValue::Blob(graph_digest),
        SqlValue::Text(format),
        SqlValue::Blob(pack_digest),
        SqlValue::Integer(pack_bytes),
        SqlValue::Blob(index_digest),
        SqlValue::Integer(index_bytes),
        rebase_metadata,
    ] = row.as_slice()
    else {
        return Err(Error::Command("invalid packed object row"));
    };
    let invalid = || Error::Command("invalid packed object metadata");
    let oid: crate::ObjectId = oid.as_slice().try_into().map_err(|_| invalid())?;
    let kind = match kind.as_str() {
        "blob" => ObjectKind::Blob,
        "tree" => ObjectKind::Tree,
        "commit" => ObjectKind::Commit,
        "tag" => ObjectKind::Tag,
        _ => return Err(invalid()),
    };
    let format = crate::ObjectFormat::parse(format).ok_or_else(invalid)?;
    if oid.format() != format || ![0, 1].contains(valid) || *pack_bytes <= 0 || *index_bytes <= 0 {
        return Err(invalid());
    }
    let reference = PackedObject {
        repository,
        oid,
        kind,
        size: u64::try_from(*size).map_err(|_| invalid())?,
        digest: digest.as_slice().try_into().map_err(|_| invalid())?,
        pack: crate::git_pack::PackIdentity {
            format,
            pack_digest: pack_digest.as_slice().try_into().map_err(|_| invalid())?,
            pack_bytes: *pack_bytes as u64,
            index_digest: index_digest.as_slice().try_into().map_err(|_| invalid())?,
            index_bytes: *index_bytes as u64,
        },
        graph_valid: *valid == 1,
        graph_count: u64::try_from(*count).map_err(|_| invalid())?,
        graph_digest: graph_digest.as_slice().try_into().map_err(|_| invalid())?,
        dependencies: None,
        rebase_metadata: match rebase_metadata {
            SqlValue::Null => None,
            SqlValue::Blob(bytes) if bytes.len() == 65 => Some(bytes.clone()),
            _ => return Err(invalid()),
        },
    };
    Ok(StoredObject {
        oid,
        kind,
        storage: ObjectStorage::Packed(Box::new(reference)),
    })
}

#[cfg(test)]
mod tests;
