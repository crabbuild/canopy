use super::*;
use cellule_runtime::primitives::sql::{SqlBatch, SqlStatement, SqlValue};

// Correlate each ancestor lookup so SQLite can stop at the first live ref,
// without materializing the entire reverse graph or scanning all refs.
const REACHABLE_WANT: &str = "WITH RECURSIVE ancestors(oid) AS (VALUES (?1) UNION SELECT e.parent FROM object_edges e JOIN ancestors a ON e.child = a.oid) SELECT g.generation, EXISTS (SELECT 1 FROM ancestors a WHERE EXISTS (SELECT 1 FROM refs r WHERE r.oid = a.oid)) FROM ref_generation g WHERE g.singleton = 1";

pub(super) struct FetchRequest {
    pub(super) wants: BTreeSet<crate::ObjectId>,
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
                let id = crate::ObjectId::from_hex(oid).map_err(|_| InputError::Fetch)?;
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
        // The shared object cache publishes loose objects atomically and
        // coordinates duplicate OID writes. Hold the gateway lock only long
        // enough to borrow it; slow fetches must not queue behind each other.
        let shared = cached.backend.cache.object_cache();
        let _hydrating = shared.hydration_guard();
        let through = {
            let objects = self.objects.lock().await;
            objects
                .as_ref()
                .filter(|objects| Arc::ptr_eq(&objects.cache, &shared))
                .map_or(0, |objects| objects.through)
        };
        if self
            .repository
            .object_high_water()
            .await
            .map_err(|error| GatewayError::Cell(Box::new(error)))?
            .output
            <= through
        {
            shared
                .prepared
                .lock()
                .await
                .extend(request.wants.iter().map(|oid| (*oid, true)));
            return Ok(());
        }
        let roots: Vec<_> = request.wants.iter().copied().collect();
        let unfiltered = request.filter.is_none();
        if roots.iter().all(|oid| {
            shared.prepared.try_lock().ok().is_some_and(|prepared| {
                prepared.contains(&(*oid, true))
                    || (request.filter.as_deref() == Some("blob:none")
                        && prepared.contains(&(*oid, false)))
            })
        }) {
            return Ok(());
        }
        let _selection = shared.selection.lock().await;
        // A concurrent cold request may have completed while we waited.
        if (unfiltered || request.filter.as_deref() == Some("blob:none"))
            && roots.iter().all(|oid| {
                shared.prepared.try_lock().ok().is_some_and(|prepared| {
                    prepared.contains(&(*oid, true))
                        || (!unfiltered && prepared.contains(&(*oid, false)))
                })
            })
        {
            return Ok(());
        }
        self.hydrate_selected(&shared, request.wants).await?;
        // The certified Cell graph already names every reachable blob. A full
        // fetch can hydrate those bodies during the structural walk and avoid
        // a second native traversal over the same cold history.
        self.hydrate_structure(&shared, &roots, unfiltered, through)
            .await?;
        if unfiltered || request.filter.as_deref() == Some("blob:none") {
            shared
                .prepared
                .lock()
                .await
                .extend(roots.iter().map(|oid| (*oid, unfiltered)));
            // Per-root preparation above is a coverage certificate. Physical
            // index counts include cross-pack duplicates and cannot certify a
            // whole-repository watermark. Durable covering packs are handled by
            // hydrate_selected using their committed covered_through metadata.
            return Ok(());
        }
        // Use the same native filter as upload-pack. Structure is present, so
        // tree/type filters can omit missing blobs without reading their bodies.
        // Git conservatively includes missing blobs under size filters.
        let mut walk = crate::git_objects::GitObjectWalk::missing(
            &cached.backend.git_dir(),
            roots,
            request.filter.as_deref(),
            &cached.backend.cache.native,
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
            self.hydrate_objects(&shared, ids, &mut stats).await?;
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
        wants: &BTreeSet<crate::ObjectId>,
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
        wants: &BTreeSet<crate::ObjectId>,
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
        mut pending: BTreeSet<crate::ObjectId>,
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

    async fn hydrate_structure(
        &self,
        cache: &Arc<GitCache>,
        roots: &[crate::ObjectId],
        include_blobs: bool,
        through: i64,
    ) -> Result<(), GatewayError> {
        let mut pending: BTreeSet<_> = roots.iter().copied().collect();
        let mut visited = BTreeSet::new();
        let mut stats = Hydration::default();
        while !pending.is_empty() {
            let ids: Vec<_> = pending.iter().take(MAX_OBJECTS).copied().collect();
            for oid in &ids {
                pending.remove(oid);
                visited.insert(*oid);
            }
            let placeholders = vec!["?"; ids.len()].join(",");
            let kind_filter = if include_blobs {
                ""
            } else {
                "AND o.kind != 'blob'"
            };
            let mut after_parent = Vec::new();
            let mut after_child = Vec::new();
            loop {
                // Certified edges supply only reachable structure. Page both
                // parents and children so a large tree stays within SQL wire bounds.
                let mut parameters: Vec<_> =
                    ids.iter().map(|oid| SqlValue::Blob(oid.to_vec())).collect();
                parameters.extend([
                    SqlValue::Integer(through),
                    SqlValue::Blob(after_parent.clone()),
                    SqlValue::Blob(after_parent.clone()),
                    SqlValue::Blob(after_child.clone()),
                    SqlValue::Integer(MAX_OBJECTS as i64),
                ]);
                let result = self.repository.sql.query(None, SqlBatch { statements: vec![SqlStatement {
                    sql: format!("SELECT e.parent, e.child, o.kind FROM object_edges e JOIN objects o ON o.oid = e.child JOIN objects p ON p.oid = e.parent WHERE e.parent IN ({placeholders}) AND p.sequence > ? {kind_filter} AND (e.parent > ? OR (e.parent = ? AND e.child > ?)) ORDER BY e.parent, e.child LIMIT ?"),
                    parameters,
                }] }).await.map_err(|error| GatewayError::Cell(Box::new(error)))?;
                let rows = &result
                    .output
                    .first()
                    .ok_or(GatewayError::MalformedCache)?
                    .rows;
                let mut blobs = Vec::new();
                for row in rows {
                    let [
                        SqlValue::Blob(parent),
                        SqlValue::Blob(child),
                        SqlValue::Text(kind),
                    ] = row.as_slice()
                    else {
                        return Err(GatewayError::MalformedCache);
                    };
                    after_parent.clone_from(parent);
                    after_child.clone_from(child);
                    let oid = child
                        .as_slice()
                        .try_into()
                        .map_err(|_| GatewayError::MalformedCache)?;
                    if kind == "blob" {
                        if include_blobs && visited.insert(oid) {
                            blobs.push(oid);
                        }
                    } else if !visited.contains(&oid) {
                        pending.insert(oid);
                    }
                }
                if !blobs.is_empty() {
                    self.hydrate_objects(cache, blobs, &mut stats).await?;
                }
                if rows.len() < MAX_OBJECTS {
                    break;
                }
            }
            self.hydrate_objects(cache, ids, &mut stats).await?;
        }
        tracing::debug!(
            repository = %hex::encode(self.repository.repository_id()),
            objects = stats.objects,
            bytes = stats.bytes,
            include_blobs,
            "prepared reachable Git structure"
        );
        Ok(())
    }

    async fn hydrate_objects(
        &self,
        cache: &Arc<GitCache>,
        ids: Vec<crate::ObjectId>,
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
        use cellule_ltx::rusqlite::{Connection, StatementStatus, params};
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
            assert_eq!(
                parsed.wants,
                BTreeSet::from([
                    crate::ObjectId::Sha1([0x12; 20]),
                    crate::ObjectId::Sha1([0x34; 20])
                ])
            );
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
