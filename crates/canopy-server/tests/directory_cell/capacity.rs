//! Deterministic feedback loop at the actual authentication query call site.
//!
//! The held SQL worker controls overlap only. This is not a RustFS throughput
//! benchmark or proof that every observed peer capacity error has this cause.

use super::*;
use std::{future::Future, task::Poll, time::Duration};

struct ReleaseOnDrop(Option<std::sync::mpsc::Sender<()>>);

impl ReleaseOnDrop {
    fn release(&mut self) {
        if let Some(sender) = self.0.take() {
            let _ = sender.send(());
        }
    }
}

impl Drop for ReleaseOnDrop {
    fn drop(&mut self) {
        self.release();
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn sixteen_overlapping_authentication_queries_fit_the_directory_contract()
-> Result<(), Box<dyn std::error::Error>> {
    let application = Arc::new(CanopyApplication::compile(build_descriptor(
        include_bytes!("../../../../Cargo.lock"),
        "directory-authentication-capacity-test",
    ))?);
    let tenant = TenantId::from_bytes([71; 16]);
    let application_id = ApplicationId::from_bytes([72; 16]);
    let target = directory::directory_target(tenant, application_id)?;
    let session = SessionId::from_bytes([73; 16]);
    let runtime = runtime(session)?;
    let layout = CellStorageLayout::new(
        Store::new(Arc::new(InMemory::new())),
        StorePath::from("authentication-capacity-test"),
        *application_id.as_bytes(),
    );
    let files = tempfile::TempDir::new()?;
    let handle = bootstrap(
        &runtime,
        &application.registry(),
        &layout,
        &target,
        (DirectoryModule::NAME, directory::SCHEMA),
        session,
        &files.path().join("directory.sqlite"),
    )
    .await?;
    let directory = DirectoryCell::new(
        &app_handle(&application, tenant, application_id, handle.clone())?,
        target,
    )?;
    directory
        .create_account(random_identity()?, "owner", [1; 32], TokenScope::Admin)
        .await?;
    assert!(
        directory
            .authenticate([1; 32], None)
            .await?
            .output
            .is_some()
    );

    let (entered, observed) = tokio::sync::oneshot::channel();
    let (release, released) = std::sync::mpsc::channel();
    let mut release = ReleaseOnDrop(Some(release));
    let blocker = tokio::spawn(async move {
        handle
            .query(0, 1, move |_| {
                let _ = entered.send(());
                released
                    .recv_timeout(Duration::from_secs(4))
                    .map_err(|source| cellule_runtime::Error::Facility {
                        name: "capacity test release",
                        source: Box::new(source),
                    })?;
                Ok(Vec::new())
            })
            .await
    });
    tokio::time::timeout(Duration::from_secs(3), observed).await??;

    // Poll each actual typed authentication query while the worker is held.
    // Immediate capacity refusals stay in the result; there is no retry,
    // lowered concurrency, synthetic oversized input or changed runtime limit.
    let mut queries = (0..16)
        .map(|_| Box::pin(directory.authenticate([1; 32], None)))
        .collect::<Vec<_>>();
    let mut pending = Vec::new();
    let mut refused = Vec::new();
    let mut premature = 0;
    for query in &mut queries {
        let polled = std::future::poll_fn(|cx| Poll::Ready(query.as_mut().poll(cx))).await;
        match polled {
            Poll::Pending => pending.push(query),
            Poll::Ready(Err(error)) => refused.push(error),
            Poll::Ready(Ok(_)) => premature += 1,
        }
    }
    let admitted = pending.len();
    let retained = runtime.stats().retained_bytes();
    println!(
        "authentication overlap: pending={admitted}, premature={premature}, \
         refused={refused:?}, retained_bytes={retained}"
    );
    release.release();
    tokio::time::timeout(Duration::from_secs(5), blocker).await???;
    for query in pending {
        let principal = tokio::time::timeout(Duration::from_secs(5), query)
            .await??
            .output
            .ok_or("queued authentication lost its valid principal")?;
        assert_eq!(principal.account, "owner");
        assert_eq!(principal.scope, TokenScope::Admin);
    }
    runtime.shutdown().await?;
    assert_eq!(premature, 0, "a query bypassed the held FIFO worker");
    assert_eq!(
        admitted, 16,
        "authentication used generic SQL result credit"
    );
    assert!(
        refused.is_empty(),
        "valid authentication queries were refused"
    );
    Ok(())
}
