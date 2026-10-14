use super::*;
use crate::git_input::GitInput;
use axum::body::Body;
use bytes::Bytes;
use futures_core::Stream;
use std::{
    fs::File,
    io::{self, Read},
    pin::Pin,
    task::{Context, Poll},
};

pub(super) const MAX_PLAN_BYTES: u64 = 128 << 20;
const FRAME_BYTES: u32 = 64 << 10;
const FRAME_UPDATES: usize = 32;
const DOMAIN: &[u8] = b"canopy.retained-push-plan.v1\0";
#[cfg(test)]
mod tests;

struct Frames {
    plan: crate::PushPlan,
    offset: usize,
    started: bool,
    failed: bool,
}
impl Stream for Frames {
    type Item = Result<Bytes, CodecError>;
    fn poll_next(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.failed || self.started && self.offset == self.plan.updates.len() {
            return Poll::Ready(None);
        }
        let encoded = (|| {
            let mut e = BoundedEncoder::new(FRAME_BYTES)?;
            if !self.started {
                e.write_bytes(DOMAIN)?;
                e.write_text(&self.plan.actor)?;
                e.write_count(self.plan.updates.len())?;
                self.started = true;
            } else {
                let end = (self.offset + FRAME_UPDATES).min(self.plan.updates.len());
                self.plan.encode_range(self.offset..end, &mut e)?;
                self.offset = end;
            }
            let bytes = e.finish();
            let mut frame = Vec::with_capacity(bytes.len() + 4);
            frame.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
            frame.extend_from_slice(&bytes);
            Ok(Bytes::from(frame))
        })();
        self.failed = encoded.is_err();
        Poll::Ready(Some(encoded))
    }
}
pub(super) async fn retain(
    plan: crate::PushPlan,
    store: &ArtifactStore,
    operation: [u8; 16],
    directory: &Path,
    budget: &DiskBudget,
) -> Result<ArtifactDescriptor, NativeResultError> {
    let body = GitInput::receive(
        Body::from_stream(Frames {
            plan,
            offset: 0,
            started: false,
            failed: false,
        }),
        directory,
        budget,
        Some(MAX_PLAN_BYTES),
        None,
    )
    .await?;
    let digest = body.content_digest().await?;
    let (_, artifact) = body.retain(store, operation, digest).await?;
    Ok(artifact)
}
fn frame(file: &mut File) -> io::Result<Vec<u8>> {
    let mut length = [0; 4];
    file.read_exact(&mut length)?;
    let length = u32::from_be_bytes(length);
    if length == 0 || length > FRAME_BYTES {
        return Err(io::ErrorKind::InvalidData.into());
    }
    let mut bytes = vec![0; length as usize];
    file.read_exact(&mut bytes)?;
    Ok(bytes)
}
fn invalid(error: CodecError) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, error)
}
fn read(file: &mut File) -> io::Result<crate::PushPlan> {
    let header = frame(file)?;
    let mut d = BoundedDecoder::new(&header, FRAME_BYTES).map_err(invalid)?;
    if d.read_bytes().map_err(invalid)? != DOMAIN {
        return Err(io::ErrorKind::InvalidData.into());
    }
    let actor = d.read_text().map_err(invalid)?.to_owned();
    let count = d.read_count().map_err(invalid)?;
    d.finish().map_err(invalid)?;
    if crate::directory::validate_component(&actor).is_err()
        || !(1..=crate::refs::MAX_UPDATES).contains(&count)
    {
        return Err(io::ErrorKind::InvalidData.into());
    }
    let mut updates = Vec::with_capacity(count);
    while updates.len() < count {
        let bytes = frame(file)?;
        let mut d = BoundedDecoder::new(&bytes, FRAME_BYTES).map_err(invalid)?;
        let chunk = crate::PushPlan::decode(&mut d).map_err(invalid)?;
        d.finish().map_err(invalid)?;
        if chunk.actor != actor || chunk.updates.len() != (count - updates.len()).min(FRAME_UPDATES)
        {
            return Err(io::ErrorKind::InvalidData.into());
        }
        updates.extend(chunk.updates);
    }
    let mut extra = [0];
    if file.read(&mut extra)? != 0 {
        return Err(io::ErrorKind::InvalidData.into());
    }
    Ok(crate::PushPlan { actor, updates })
}
pub(super) async fn reopen(
    store: &ArtifactStore,
    operation: [u8; 16],
    artifact: ArtifactDescriptor,
    directory: &Path,
    budget: &DiskBudget,
) -> Result<crate::PushPlan, NativeResultError> {
    let reader = store.read(body_key(operation, artifact), artifact).await?;
    let body = GitInput::reopen(reader, directory, budget, Some(MAX_PLAN_BYTES), None).await?;
    Ok(body.read_owned(read).await?)
}
