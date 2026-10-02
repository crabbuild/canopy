use super::*;
#[test]
fn vector_denials_do_not_leak_other_dimensions() -> io::Result<()> {
    let pool = NativeResources::default();
    let scope = pool.scope(NativeClass::Foreground);
    let mut permits = Vec::new();
    while let Ok(permit) = scope.try_admit(NativeWork::Pack) {
        permits.push(permit);
    }
    assert_eq!(permits.len(), 10); // CPU, before process/memory/descriptor limits.
    let prior = pool.usage()?;
    for _ in 0..100 {
        assert_eq!(
            scope.try_admit(NativeWork::Read).err().unwrap().kind(),
            io::ErrorKind::WouldBlock
        );
        assert_eq!(pool.usage()?, prior);
    }
    drop(permits);
    assert_eq!(pool.usage()?, NativeUsage::default());
    Ok(())
}
#[test]
fn maintenance_and_foreground_shares_survive_each_others_saturation() -> io::Result<()> {
    let pool = NativeResources::default();
    let foreground = pool.scope(NativeClass::Foreground);
    let maintenance = foreground.for_class(NativeClass::Maintenance);
    let mut active = Vec::new();
    while let Ok(permit) = foreground.try_admit(NativeWork::Pack) {
        active.push(permit);
    }
    let listing = maintenance.try_admit(NativeWork::Read)?;
    let pack = maintenance.try_admit(NativeWork::Pack)?;
    assert!(maintenance.try_admit(NativeWork::Read).is_err());
    drop(active);
    let foreground_work = foreground.try_admit(NativeWork::Pack)?;
    drop((listing, pack, foreground_work));
    assert_eq!(pool.usage()?, NativeUsage::default());
    Ok(())
}
#[test]
fn invalid_and_overflowing_shares_reject_before_pool_creation() {
    let mut limits = NativeLimits::default();
    limits.maintenance_reserved.memory_bytes = u64::MAX;
    assert!(NativeResources::new(limits).is_err());
    limits = NativeLimits::default();
    limits.total.cpu_units = limits.maintenance_reserved.cpu_units;
    assert!(NativeResources::new(limits).is_err());
    limits = NativeLimits::default();
    limits.maintenance_reserved.processes = 1;
    assert!(NativeResources::new(limits).is_err());
    limits = NativeLimits::default();
    limits.pack.memory_bytes = u64::MAX;
    assert!(NativeResources::new(limits).is_err());
    limits = NativeLimits::default();
    limits.pack.cpu_units = u32::MAX;
    assert!(NativeResources::new(limits).is_err());
}

#[test]
fn every_dimension_can_be_the_limiting_resource() -> io::Result<()> {
    for dimension in 0..4 {
        let mut limits = NativeLimits::default();
        let mut foreground = NativeCapacity {
            processes: 100,
            cpu_units: 100,
            memory_bytes: 100 << 30,
            descriptors: 10000,
        };
        let exact = limits.read.add(limits.pack).unwrap();
        match dimension {
            0 => foreground.processes = exact.processes,
            1 => foreground.cpu_units = exact.cpu_units,
            2 => foreground.memory_bytes = exact.memory_bytes,
            _ => foreground.descriptors = exact.descriptors,
        }
        limits.total = foreground.add(limits.maintenance_reserved).unwrap();
        let pool = NativeResources::new(limits)?;
        let scope = pool.scope(NativeClass::Foreground);
        let pack = scope.try_admit(NativeWork::Pack)?;
        let read = scope.try_admit(NativeWork::Read)?;
        let used = pool.usage()?;
        let denied = scope.try_admit(NativeWork::Read).err().unwrap();
        assert!(is_exhausted(&denied));
        assert_eq!(pool.usage()?, used);
        drop((pack, read));
        assert_eq!(pool.usage()?, NativeUsage::default());
    }
    Ok(())
}

#[test]
fn simultaneous_admission_is_atomic_across_shared_scopes() -> io::Result<()> {
    let mut limits = NativeLimits::default();
    limits.total.processes = limits.maintenance_reserved.processes + 4;
    let pool = NativeResources::new(limits)?;
    let admitted = Arc::new(std::sync::Barrier::new(9));
    let release = Arc::new(std::sync::Barrier::new(9));
    let mut threads = Vec::new();
    for _ in 0..8 {
        let scope = pool.scope(NativeClass::Foreground);
        let admitted = Arc::clone(&admitted);
        let release = Arc::clone(&release);
        threads.push(std::thread::spawn(move || {
            let permit = scope.try_admit(NativeWork::Read);
            admitted.wait();
            release.wait();
            permit.is_ok()
        }));
    }
    admitted.wait();
    assert_eq!(pool.usage()?.foreground.processes, 4);
    assert_eq!(
        pool.usage()?.foreground.memory_bytes,
        4 * limits.read.memory_bytes
    );
    release.wait();
    assert_eq!(
        threads
            .into_iter()
            .map(|thread| thread.join().unwrap())
            .filter(|admitted| *admitted)
            .count(),
        4
    );
    assert_eq!(pool.usage()?, NativeUsage::default());
    Ok(())
}

#[test]
fn poisoned_admission_never_returns_a_live_claim() -> io::Result<()> {
    let pool = NativeResources::default();
    let scope = pool.scope(NativeClass::Foreground);
    let permit = scope.try_admit(NativeWork::Read)?;
    let prior = pool.usage()?;
    let _panic = std::panic::catch_unwind(|| {
        let _lock = pool.0.used.lock().unwrap();
        panic!("fault injection");
    });
    assert!(scope.try_admit(NativeWork::Read).is_err());
    drop(permit);
    assert!(pool.usage().is_err());
    assert_eq!(*pool.0.used.lock().unwrap_err().into_inner(), prior);
    Ok(())
}
