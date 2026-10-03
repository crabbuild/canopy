//! Node-wide claims for native Git work. Claims are admission, not OS limits.
use serde::Deserialize;
use std::{
    io,
    sync::{Arc, Mutex},
};
use tokio::sync::Notify;

/// Capacity charged atomically as one vector; no partially held reservations.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeCapacity {
    pub processes: u32,
    pub cpu_units: u32,
    pub memory_bytes: u64,
    pub descriptors: u32,
}
impl NativeCapacity {
    fn add(self, other: Self) -> Option<Self> {
        Some(Self {
            processes: self.processes.checked_add(other.processes)?,
            cpu_units: self.cpu_units.checked_add(other.cpu_units)?,
            memory_bytes: self.memory_bytes.checked_add(other.memory_bytes)?,
            descriptors: self.descriptors.checked_add(other.descriptors)?,
        })
    }
    fn sub(self, other: Self) -> Option<Self> {
        Some(Self {
            processes: self.processes.checked_sub(other.processes)?,
            cpu_units: self.cpu_units.checked_sub(other.cpu_units)?,
            memory_bytes: self.memory_bytes.checked_sub(other.memory_bytes)?,
            descriptors: self.descriptors.checked_sub(other.descriptors)?,
        })
    }
    fn fits(self, limit: Self) -> bool {
        self.processes <= limit.processes
            && self.cpu_units <= limit.cpu_units
            && self.memory_bytes <= limit.memory_bytes
            && self.descriptors <= limit.descriptors
    }
}

/// Maintenance has a disjoint reserved share. Foreground cannot consume it.
#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeLimits {
    pub total: NativeCapacity,
    pub maintenance_reserved: NativeCapacity,
    pub read: NativeCapacity,
    pub pack: NativeCapacity,
}
impl Default for NativeLimits {
    fn default() -> Self {
        Self {
            total: NativeCapacity {
                processes: 32,
                cpu_units: 48,
                memory_bytes: 16 << 30,
                descriptors: 2048,
            },
            maintenance_reserved: NativeCapacity {
                processes: 2,
                cpu_units: 8,
                memory_bytes: 2 << 30,
                descriptors: 128,
            },
            read: NativeWork::Read.claim(),
            pack: NativeWork::Pack.claim(),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NativeClass {
    Foreground,
    Maintenance,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NativeWork {
    Read,
    Pack,
}
impl NativeWork {
    /// Default admission estimates for the sanitized two-thread Git command policy.
    /// Parent/helpers, heaps and mappings require empirical/OS qualification.
    pub fn claim(self) -> NativeCapacity {
        match self {
            Self::Read => NativeCapacity {
                processes: 1,
                cpu_units: 1,
                memory_bytes: 256 << 20,
                descriptors: 32,
            },
            Self::Pack => NativeCapacity {
                processes: 1,
                cpu_units: 4,
                memory_bytes: 1 << 30,
                descriptors: 64,
            },
        }
    }
}
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct NativeUsage {
    pub foreground: NativeCapacity,
    pub maintenance: NativeCapacity,
}
struct Pool {
    limits: NativeLimits,
    foreground: NativeCapacity,
    state: Mutex<PoolState>,
    changed: Notify,
}
#[derive(Debug, Default)]
struct PoolState {
    used: NativeUsage,
    closed: bool,
    faulted: bool,
}

/// Create once per node and clone into all gateways and preparation services.
#[derive(Clone)]
pub struct NativeResources(Arc<Pool>);
impl NativeResources {
    pub fn new(limits: NativeLimits) -> io::Result<Self> {
        let read_minimum = NativeCapacity {
            processes: 1,
            cpu_units: 1,
            memory_bytes: 128 << 20,
            descriptors: 32,
        };
        let pack_minimum = NativeCapacity {
            processes: 1,
            cpu_units: 4,
            memory_bytes: 512 << 20,
            descriptors: 64,
        };
        if limits.read.processes != 1
            || limits.pack.processes != 1
            || !read_minimum.fits(limits.read)
            || !pack_minimum.fits(limits.pack)
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "native profiles are below their minimum claims",
            ));
        }
        let minimum = limits.read.add(limits.pack).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "native profile claim overflow")
        })?;
        let foreground = limits
            .total
            .sub(limits.maintenance_reserved)
            .filter(|foreground| {
                minimum.fits(*foreground) && minimum.fits(limits.maintenance_reserved)
            })
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "native shares must each fit a read/pack pipeline",
                )
            })?;
        Ok(Self(Arc::new(Pool {
            limits,
            foreground,
            state: Mutex::new(PoolState::default()),
            changed: Notify::new(),
        })))
    }
    pub fn scope(&self, class: NativeClass) -> NativeScope {
        NativeScope {
            resources: self.clone(),
            class,
        }
    }
    pub fn usage(&self) -> io::Result<NativeUsage> {
        self.0
            .state
            .lock()
            .map(|state| state.used)
            .map_err(|_| io::Error::other("native admission poisoned"))
    }

    /// Permanently reject launches through every cloned scope. Serialized with
    /// admission: an already admitted claim remains owned until actual drain.
    pub fn close(&self) {
        match self.0.state.lock() {
            Ok(mut state) => state.closed = true,
            Err(poisoned) => {
                poisoned.into_inner().closed = true;
                tracing::error!("native admission poisoned; shutdown ownership retained");
            }
        }
        self.0.changed.notify_waiters();
    }

    /// Close admission and wait for all foreground and maintenance owners.
    /// Cancellation only abandons this observer, never claims or closure.
    /// Poison, underflow or quarantined owners cannot prove drain; callers
    /// must retain workspace and lease ownership while this remains pending.
    pub async fn drain(&self) {
        self.close();
        loop {
            let changed = self.0.changed.notified();
            tokio::pin!(changed);
            // Register before checking state so a final release cannot be lost,
            // including when several independent observers wait for drain.
            changed.as_mut().enable();
            if self.0.state.lock().is_ok_and(|state| {
                state.closed && !state.faulted && state.used == NativeUsage::default()
            }) {
                return;
            }
            changed.await;
        }
    }
}
impl Default for NativeResources {
    fn default() -> Self {
        Self::new(NativeLimits::default()).expect("valid native defaults")
    }
}

/// Scope retains the node pool, not an active-work reservation.
#[derive(Clone)]
pub struct NativeScope {
    resources: NativeResources,
    class: NativeClass,
}
impl NativeScope {
    pub fn for_class(&self, class: NativeClass) -> Self {
        self.resources.scope(class)
    }
    pub fn try_admit(&self, work: NativeWork) -> io::Result<NativePermit> {
        let claim = match work {
            NativeWork::Read => self.resources.0.limits.read,
            NativeWork::Pack => self.resources.0.limits.pack,
        };
        let mut state = self
            .resources
            .0
            .state
            .lock()
            .map_err(|_| io::Error::other("native admission poisoned"))?;
        if state.closed {
            return Err(io::Error::new(io::ErrorKind::WouldBlock, NativeClosed));
        }
        if state.faulted {
            return Err(io::Error::other("native admission faulted"));
        }
        let (current, limit) = match self.class {
            NativeClass::Foreground => (&mut state.used.foreground, self.resources.0.foreground),
            NativeClass::Maintenance => (
                &mut state.used.maintenance,
                self.resources.0.limits.maintenance_reserved,
            ),
        };
        let next = current
            .add(claim)
            .filter(|next| next.fits(limit))
            .ok_or_else(|| io::Error::new(io::ErrorKind::WouldBlock, NativeExhausted))?;
        *current = next;
        Ok(NativePermit {
            scope: self.clone(),
            claim,
        })
    }
}

/// Unforgeable single-owner claim. Native process drain owns its release.
pub struct NativePermit {
    scope: NativeScope,
    claim: NativeCapacity,
}
impl Drop for NativePermit {
    fn drop(&mut self) {
        let Ok(mut state) = self.scope.resources.0.state.lock() else {
            tracing::error!("native admission poisoned; claim quarantined");
            return;
        };
        let current = match self.scope.class {
            NativeClass::Foreground => &mut state.used.foreground,
            NativeClass::Maintenance => &mut state.used.maintenance,
        };
        if let Some(next) = current.sub(self.claim) {
            *current = next;
        } else {
            state.faulted = true;
            tracing::error!("native admission underflow; claim quarantined");
        }
        drop(state);
        self.scope.resources.0.changed.notify_waiters();
    }
}

#[cfg(test)]
mod tests;

#[derive(Debug, thiserror::Error)]
#[error("native resource admission exhausted")]
struct NativeExhausted;

#[derive(Debug, thiserror::Error)]
#[error("native resource admission closed")]
struct NativeClosed;

pub(crate) fn is_exhausted(error: &io::Error) -> bool {
    error
        .get_ref()
        .is_some_and(|source| source.is::<NativeExhausted>() || source.is::<NativeClosed>())
}
