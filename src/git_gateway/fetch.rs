use super::*;
use crab_cell_runtime::primitives::sql::{SqlBatch, SqlStatement, SqlValue};

// Correlate each ancestor lookup so SQLite can stop at the first live ref,
// without materializing the entire reverse graph or scanning all refs.
const REACHABLE_WANT: &str = "WITH RECURSIVE ancestors(oid) AS (VALUES (?1) UNION SELECT e.parent FROM object_edges e JOIN ancestors a ON e.child = a.oid) SELECT g.generation, EXISTS (SELECT 1 FROM ancestors a WHERE EXISTS (SELECT 1 FROM refs r WHERE r.oid = a.oid)) FROM ref_generation g WHERE g.singleton = 1";

pub(super) struct FetchRequest {
    pub(super) wants: BTreeSet<[u8; 20]>,
    pub(super) filter: Option<String>,
}

impl FetchRequest {
    pub(super) async fn read(request: &GitHttpRequest) -> Result<Self, InputError> {
        if request.method != "POST"
            || request.path_info != "/repo.git/git-upload-pack"
            || request.content_type.as_deref() != Some("application/x-git-upload-pack-request")
        {
            return Ok(Self {
                wants: BTreeSet::new(),
                filter: None,
            });
        }
        Self::parse(
            &request
                .body
                .prefix(MAX_FETCH_REQUEST_BYTES as usize)
                .await?,
        )
    }

    pub(super) fn parse(mut bytes: &[u8]) -> Result<Self, InputError> {
        let mut wants = BTreeSet::new();
        let mut filter = None;
        while !bytes.is_empty() {
            let header = bytes.get(..4).ok_or(InputError::Fetch)?;
            if !header.iter().all(u8::is_ascii_hexdigit) {
                return Err(InputError::Fetch);
            }
            let length = std::str::from_utf8(header)
                .ok()
                .and_then(|value| usize::from_str_radix(value, 16).ok())
                .ok_or(InputError::Fetch)?;
            if length <= 2 {
                bytes = &bytes[4..];
                continue;
            }
            if !(5..=65520).contains(&length) {
                return Err(InputError::Fetch);
            }
            let payload = bytes.get(4..length).ok_or(InputError::Fetch)?;
            let payload = payload.strip_suffix(b"\n").unwrap_or(payload);
            if let Some(want) = payload.strip_prefix(b"want ") {
                let oid = want
                    .split(|byte| *byte == b' ')
                    .next()
                    .ok_or(InputError::Fetch)?;
                if oid.len() != 40 || !oid.iter().all(u8::is_ascii_hexdigit) {
                    return Err(InputError::Fetch);
                }
                let mut id = [0; 20];
                hex::decode_to_slice(oid, &mut id).map_err(|_| InputError::Fetch)?;
                wants.insert(id);
            }
            if let Some(value) = payload.strip_prefix(b"filter ")
                && filter.replace(value).is_some()
            {
                return Err(InputError::Fetch);
            }
            bytes = &bytes[length..];
        }
        let filter = filter
            .map(|value| {
                let value = std::str::from_utf8(value).map_err(|_| InputError::Fetch)?;
                check_filter_policy(value)?;
                Ok::<_, InputError>(value.to_owned())
            })
            .transpose()?;
        Ok(Self { wants, filter })
    }
}

fn check_filter_policy(value: &str) -> Result<(), InputError> {
    // rev-list does not enforce uploadpackfilter.*. Match the transport policy
    // before traversal, including escaped subfilters, so sparse filters cannot
    // inspect pattern blobs outside the validated wants.
    let mut pending = vec![std::borrow::Cow::Borrowed(value)];
    while let Some(value) = pending.pop() {
        if value.contains('\0') {
            return Err(InputError::Fetch);
        }
        if let Some(combined) = value.strip_prefix("combine:") {
            for part in combined.split('+') {
                let decoded = percent_encoding::percent_decode_str(part)
                    .decode_utf8()
                    .map_err(|_| InputError::Fetch)?;
                pending.push(std::borrow::Cow::Owned(decoded.into_owned()));
            }
        } else if value != "blob:none"
            && !value.starts_with("blob:limit=")
            && !value.starts_with("tree:")
            && !value.starts_with("object:type=")
        {
            return Err(InputError::Fetch);
        }
    }
    Ok(())
}

impl GitGateway {
    pub(super) async fn prepare_fetch(
        &self,
        cached: &CachedRepository,
        request: FetchRequest,
    ) -> Result<(), GatewayError> {
        if request.wants.is_empty() {
            return Ok(());
        }
        let started = std::time::Instant::now();
        let objects = self.objects.lock().await;
        let shared = objects.as_ref().ok_or(GatewayError::MalformedCache)?;
        let roots = request.wants.iter().copied().collect();
        self.hydrate_selected(&shared.cache, request.wants).await?;
        if request.filter.as_deref() == Some("blob:none") {
            return Ok(());
        }
        // Use the same native filter as upload-pack. Structure is present, so
        // tree/type filters can omit missing blobs without reading their bodies.
        // Git conservatively includes missing blobs under size filters.
        let mut walk = crate::git_objects::GitObjectWalk::missing(
            &cached.backend.git_dir(),
            roots,
            request.filter.as_deref(),
        )?;
        let mut stats = Hydration::default();
        loop {
            let mut ids = Vec::with_capacity(MAX_OBJECTS);
            for _ in 0..MAX_OBJECTS {
                let Some(oid) = walk.next().await? else {
                    break;
                };
                ids.push(oid);
            }
            if ids.is_empty() {
                break;
            }
            self.hydrate_objects(&shared.cache, ids, &mut stats).await?;
        }
        walk.finish().await?;
        tracing::debug!(
            repository = %hex::encode(self.repository.repository_id()),
            objects = stats.objects,
            bytes = stats.bytes,
            elapsed_seconds = started.elapsed().as_secs_f64(),
            "prepared reachable Git blobs"
        );
        Ok(())
    }

    pub(super) async fn fetch_cache(
        &self,
        wants: &BTreeSet<[u8; 20]>,
    ) -> Result<Arc<CachedRepository>, GatewayError> {
        if let Some(cached) = self.current_cache().await? {
            self.validate_wants(&cached.snapshot, wants).await?;
            return Ok(cached);
        }
        let live_refs = self.cell_refs().await?;
        self.validate_wants(&live_refs, wants).await?;
        let mut cache = self.cache.lock().await;
        if cache
            .as_ref()
            .is_none_or(|cached| cached.snapshot != live_refs)
        {
            *cache = None;
            *cache = Some(Arc::new(self.build_cache(live_refs, false).await?));
        }
        Ok(Arc::clone(
            cache.as_ref().ok_or(GatewayError::MalformedCache)?,
        ))
    }

    pub(super) async fn validate_wants(
        &self,
        snapshot: &RefSnapshot,
        wants: &BTreeSet<[u8; 20]>,
    ) -> Result<(), GatewayError> {
        // Native reachable-want validation walks commits only. Reverse edges
        // certified by the Cell also fence trees and blobs, including cached
        // objects retained after a ref deletion. Generation binds every batch.
        let ids: Vec<_> = wants.iter().collect();
        for ids in ids.chunks(MAX_OBJECTS) {
            let result = self
                .repository
                .sql
                .query(
                    None,
                    SqlBatch {
                        statements: ids
                            .iter()
                            .map(|oid| SqlStatement {
                                sql: REACHABLE_WANT.into(),
                                parameters: vec![SqlValue::Blob(oid.to_vec())],
                            })
                            .collect(),
                    },
                )
                .await
                .map_err(|error| GatewayError::Cell(Box::new(error)))?;
            for set in result.output {
                let Some([SqlValue::Integer(generation), SqlValue::Integer(reachable)]) =
                    set.rows.first().map(Vec::as_slice)
                else {
                    return Err(GatewayError::MalformedCache);
                };
                if *generation != snapshot.generation {
                    return Err(GatewayError::RefSnapshotBusy);
                }
                if *reachable != 1 {
                    return Err(GatewayError::UnreachableWant);
                }
            }
        }
        Ok(())
    }

    pub(super) async fn hydrate_selected(
        &self,
        cache: &Arc<GitCache>,
        mut pending: BTreeSet<[u8; 20]>,
    ) -> Result<(), GatewayError> {
        let mut visited = BTreeSet::new();
        let mut stats = Hydration::default();
        while !pending.is_empty() {
            let ids: Vec<_> = pending.iter().take(MAX_OBJECTS).copied().collect();
            // Advertisements peel tags even with blob filtering. Follow only
            // tag edges here; ordinary tree descendants stay omitted.
            let placeholders = vec!["?"; ids.len()].join(",");
            let result = self.repository.sql.query(None, SqlBatch { statements: vec![SqlStatement {
                sql: format!("SELECT e.child FROM object_edges e JOIN objects o ON o.oid = e.parent WHERE o.kind = 'tag' AND e.parent IN ({placeholders})"),
                parameters: ids.iter().map(|oid| SqlValue::Blob(oid.to_vec())).collect(),
            }] }).await.map_err(|error| GatewayError::Cell(Box::new(error)))?;
            for oid in &ids {
                pending.remove(oid);
                visited.insert(*oid);
            }
            for row in &result
                .output
                .first()
                .ok_or(GatewayError::MalformedCache)?
                .rows
            {
                let [SqlValue::Blob(oid)] = row.as_slice() else {
                    return Err(GatewayError::MalformedCache);
                };
                let oid = oid
                    .as_slice()
                    .try_into()
                    .map_err(|_| GatewayError::MalformedCache)?;
                if !visited.contains(&oid) {
                    pending.insert(oid);
                }
            }
            self.hydrate_objects(cache, ids, &mut stats).await?;
        }
        tracing::debug!(
            objects = stats.objects,
            bytes = stats.bytes,
            "hydrated explicit Git objects"
        );
        Ok(())
    }

    async fn hydrate_objects(
        &self,
        cache: &Arc<GitCache>,
        ids: Vec<[u8; 20]>,
        stats: &mut Hydration,
    ) -> Result<(), GatewayError> {
        let mut missing: BTreeSet<_> = cache.missing_objects(ids).await?.into_iter().collect();
        while !missing.is_empty() {
            let selected: Vec<_> = missing.iter().copied().collect();
            let page = self
                .repository
                .selected_objects(&selected)
                .await
                .map_err(|error| GatewayError::Cell(Box::new(error)))?;
            if page.is_empty() {
                return Err(GatewayError::MalformedCache);
            }
            for object in page {
                missing.remove(&object.oid);
                self.cache_object(cache, object, stats).await?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reachability_stops_at_live_refs_without_scanning_other_history()
    -> Result<(), Box<dyn std::error::Error>> {
        use crab_ltx::rusqlite::{Connection, StatementStatus, params};
        let db = Connection::open_in_memory()?;
        db.execute_batch(crate::SCHEMA)?;
        let oid = |n: u32| {
            let mut oid = [0; 20];
            oid[..4].copy_from_slice(&n.to_be_bytes());
            oid
        };
        db.execute_batch("BEGIN")?;
        for n in 0..10_000 {
            db.execute("INSERT INTO objects (oid, kind, size, digest, storage, body) VALUES (?1, 'blob', 0, zeroblob(32), 'inline', X'')", [oid(n).as_slice()])?;
            db.execute(
                "INSERT INTO refs (name, oid, version) VALUES (?1, ?2, 1)",
                params![format!("refs/tags/{n}"), oid(n).as_slice()],
            )?;
            if n > 0 {
                db.execute(
                    "INSERT INTO object_edges (parent, child) VALUES (?1, ?2)",
                    params![oid(n).as_slice(), oid(n - 1).as_slice()],
                )?;
            }
        }
        db.execute_batch("COMMIT")?;
        // A directly referenced object has thousands of reverse ancestors;
        // removing its ref makes the next ancestor the nearest live root.
        for direct_ref in [true, false] {
            if !direct_ref {
                db.execute("DELETE FROM refs WHERE name = 'refs/tags/0'", [])?;
            }
            let mut query = db.prepare(REACHABLE_WANT)?;
            assert_eq!(
                query.query_row([oid(0).as_slice()], |row| row.get::<_, i64>(1))?,
                1
            );
            assert!(query.get_status(StatementStatus::VmStep) < 500);
            assert_eq!(query.get_status(StatementStatus::FullscanStep), 0);
        }
        db.execute("DELETE FROM refs", [])?;
        let mut query = db.prepare(REACHABLE_WANT)?;
        assert_eq!(
            query.query_row([oid(0).as_slice()], |row| row.get::<_, i64>(1))?,
            0
        );
        assert_eq!(
            query.query_row([oid(10_000).as_slice()], |row| row.get::<_, i64>(1))?,
            0
        );
        Ok(())
    }

    fn packet(line: &str) -> Vec<u8> {
        format!("{:04x}{line}", line.len() + 4).into_bytes()
    }

    #[test]
    fn fetch_filters_cannot_read_unvalidated_sparse_patterns() {
        for filter in [
            "sparse:oid=HEAD:private-pattern",
            "combine:tree:0+sparse%3Aoid%3DHEAD%3Aprivate-pattern",
            "combine:combine%3Atree%253A0%2Bsparse%253Aoid%253DHEAD",
            "combine:blob:none+%00sparse:oid=HEAD",
            "combine:blob:none+%ff",
        ] {
            let request = packet(&format!("filter {filter}\n"));
            assert!(FetchRequest::parse(&request).is_err(), "{filter}");
        }
    }

    #[test]
    fn fetch_selection_covers_v0_and_v2_without_ignoring_late_wants() {
        for prefix in [b"".as_slice(), b"0012command=fetch\n0001"] {
            let mut request = prefix.to_vec();
            request.extend(packet(&format!("want {} filter\n", "12".repeat(20))));
            request.extend(packet("filter blob:none\n"));
            request.extend_from_slice(b"0000");
            request.extend(packet(&format!("want {}\n", "34".repeat(20))));
            request.extend(packet("done\n"));
            let parsed = FetchRequest::parse(&request).unwrap();
            assert_eq!(parsed.wants, BTreeSet::from([[0x12; 20], [0x34; 20]]));
            assert_eq!(parsed.filter.as_deref(), Some("blob:none"));
        }
        for input in [
            b"0003".as_slice(),
            b"+005x",
            b"0032want bad",
            b"0015filter blob:none\n0015filter blob:none\n",
        ] {
            assert!(FetchRequest::parse(input).is_err());
        }
    }
}
