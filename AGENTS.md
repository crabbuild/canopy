# Cellule integration

- Keep Canopy on the latest published Cellule release. Update all direct
  `cellule-*` exact version pins and `Cargo.lock` together, then adapt callers
  to the current APIs and run the verification gates.
- Support only the compiled release and its current storage formats. Breaking
  Cellule or Canopy schema changes use a fresh storage prefix. Do not add old
  readers, retained module code, migration chains, compatibility adapters, or
  mixed-version operation to preserve an earlier deployment.
- Keep release, identity, format, and fencing validation. Rejection of older
  state is intentional. Same-release crash recovery, retries, backup/restore,
  and standard Git/Git LFS protocol support remain required.
- Cellule's single schema-initialization `MigrationDescriptor` is required for
  fresh Cells; it is not an upgrade path. Keep command codec identities aligned
  with the current handlers; do not reset version numbers to hide a change.
- Run `python3 scripts/check_cellule.py`, formatting, Clippy, ordinary tests,
  and the isolated provider/process qualification for dependency changes. Record
  any qualification that was not run; retain historical evidence with its
  original dependency version.
