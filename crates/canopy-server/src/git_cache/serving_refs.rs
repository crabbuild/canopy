//! Count-bounded ref pages streamed into one unpublished, admitted native cache.
use super::*;
use crate::{ObjectId, git_objects::ReadOwner};

pub(crate) struct ServingRefsWriter {
    output: BufWriter<CacheWriter>,
    last: String,
    replace: bool,
    _owner: ReadOwner,
}
impl GitCache {
    pub(crate) async fn serving_refs(
        self: &Arc<Self>,
        owner: ReadOwner,
        fully_peeled: bool,
    ) -> Result<ServingRefsWriter, CacheError> {
        let cache = self.clone();
        tokio::task::spawn_blocking(move || {
            let mut output = BufWriter::new(cache.writer(Path::new(if fully_peeled {
                "packed-refs.lock"
            } else {
                "packed-refs"
            }))?);
            output.write_all(if fully_peeled {
                b"# pack-refs with: peeled fully-peeled sorted\n"
            } else {
                b"# pack-refs with: sorted\n"
            })?;
            Ok(ServingRefsWriter {
                output,
                last: String::new(),
                replace: fully_peeled,
                _owner: owner,
            })
        })
        .await?
    }
}
impl ServingRefsWriter {
    pub(crate) async fn append(
        self,
        page: Vec<(String, RefExpectation)>,
    ) -> Result<Self, CacheError> {
        self.append_peeled(
            page.into_iter()
                .map(|(name, state)| (name, state, None))
                .collect(),
        )
        .await
    }
    pub(crate) async fn append_peeled(
        mut self,
        page: Vec<(String, RefExpectation, Option<ObjectId>)>,
    ) -> Result<Self, CacheError> {
        if page.len() > crate::refs::REF_PAGE_SIZE {
            return Err(CacheError::InvalidHead);
        }
        tokio::task::spawn_blocking(move || {
            for (name, state, peeled) in page {
                let Some(oid) = state.oid else {
                    return Err(CacheError::InvalidHead);
                };
                if name <= self.last
                    || !valid_ref_name(&name)
                    || oid.is_zero()
                    || oid.format() != self.output.get_ref().cache.object_format
                {
                    return Err(CacheError::InvalidHead);
                }
                writeln!(self.output, "{} {name}", hex::encode(oid))?;
                if let Some(peeled) = peeled {
                    if peeled.is_zero() || peeled.format() != oid.format() {
                        return Err(CacheError::InvalidHead);
                    }
                    writeln!(self.output, "^{}", hex::encode(peeled))?;
                }
                self.last = name;
            }
            Ok(self)
        })
        .await?
    }
    pub(crate) async fn finish(mut self) -> Result<(), CacheError> {
        tokio::task::spawn_blocking(move || {
            self.output.flush()?;
            self.output.get_ref().file.sync_all()?;
            if self.replace {
                let path = self.output.get_ref().cache.git_dir();
                std::fs::rename(path.join("packed-refs.lock"), path.join("packed-refs"))?;
            }
            Ok(())
        })
        .await?
    }
}
