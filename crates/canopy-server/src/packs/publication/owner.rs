//! Fresh durable owner observations, separate from historical lease replies.
use super::*;
use cellule_runtime::CellTarget;

/// Server-owned authority source for one repository Cell. A decoded lease or
/// caller-supplied fence cannot construct this capability.
#[derive(Clone)]
pub struct PreparationAuthority {
    target: CellTarget,
    source: Source,
}
#[derive(Clone)]
enum Source {
    Node(crate::server::peer::NodePeer),
    // The local runtime fixture has real durable Control ownership but no
    // network node advertisement. This path is absent in production builds.
    #[cfg(test)]
    Local(std::sync::Arc<cellule_runtime::control::authority::CellAuthority>),
}
impl PreparationAuthority {
    pub(crate) fn node(peer: crate::server::peer::NodePeer, target: CellTarget) -> Self {
        Self {
            target,
            source: Source::Node(peer),
        }
    }
    #[cfg(test)]
    pub(crate) fn local(layout: cellule_ltx::CellStorageLayout, target: CellTarget) -> Self {
        Self {
            target,
            source: Source::Local(std::sync::Arc::new(
                cellule_runtime::control::authority::CellAuthority::new(layout),
            )),
        }
    }
    pub(super) fn matches(&self, target: &CellTarget) -> bool {
        self.target == *target
    }
    pub(super) async fn check(
        &self,
        target: &CellTarget,
        expected: OwnerFence,
    ) -> Result<(), PreparationBaseError> {
        if self.observe(target).await? != expected {
            return Err(PreparationBaseError::Inactive);
        }
        Ok(())
    }
    pub(super) async fn observe(
        &self,
        target: &CellTarget,
    ) -> Result<OwnerFence, PreparationBaseError> {
        if !self.matches(target) {
            return Err(PreparationBaseError::Context);
        }
        let actual = match &self.source {
            Source::Node(peer) => peer
                .current_owner_fence(target)
                .await
                .map_err(|error| PreparationBaseError::Owner(Box::new(error)))?,
            #[cfg(test)]
            Source::Local(authority) => {
                let control = authority
                    .load(target.cell_id())
                    .await
                    .map_err(|error| PreparationBaseError::Owner(Box::new(error)))?
                    .ok_or(PreparationBaseError::Inactive)?;
                if control.value().owner.is_none() {
                    return Err(PreparationBaseError::Inactive);
                }
                control.value().owner_fence()
            }
        };
        Ok(actual)
    }
}
