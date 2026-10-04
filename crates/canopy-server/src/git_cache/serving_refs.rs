//! Count-bounded ref pages streamed into one unpublished, admitted native cache.
use super::*;
use crate::git_objects::ReadOwner;

pub(crate) struct ServingRefsWriter {
    output: BufWriter<CacheWriter>,
    last: String,
    _owner: ReadOwner,
}
impl GitCache {
    pub(crate) async fn serving_refs(
        self: &Arc<Self>,
        owner: ReadOwner,
    ) -> Result<ServingRefsWriter, CacheError> {
        let cache = self.clone();
        tokio::task::spawn_blocking(move || {
            let mut output = BufWriter::new(cache.writer(Path::new("packed-refs"))?);
            output.write_all(b"# pack-refs with: sorted\n")?;
            Ok(ServingRefsWriter {
                output,
                last: String::new(),
                _owner: owner,
            })
        })
        .await?
    }
}
impl ServingRefsWriter {
    pub(crate) async fn append(
        mut self,
        page: Vec<(String, RefExpectation)>,
    ) -> Result<Self, CacheError> {
        if page.len() > crate::refs::REF_PAGE_SIZE {
            return Err(CacheError::InvalidHead);
        }
        tokio::task::spawn_blocking(move || {
            for (name, state) in page {
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
            Ok(())
        })
        .await?
    }
}
