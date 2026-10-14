pub fn new(
    layout: &cellule_ltx::CellStorageLayout,
    root: &std::path::Path,
    id: [u8; 16],
) -> cellule_runtime::Result<canopy_server::git_pack::PackStore> {
    canopy_server::git_pack::PackStore::new(
        layout.store().inner().clone(),
        id,
        root.into(),
        cellule_ltx::DiskBudget::new(1 << 30),
        256 * 1024 * 1024,
        64 * 1024 * 1024,
    )
}
