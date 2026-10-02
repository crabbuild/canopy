//! Owned fixture for a Directory-only predecessor release. Other module
//! identities stay current: the packed Repository module has a hard cutover.

use cellule_runtime::Registry;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

pub fn predecessor(registry: &Registry, historical: &[u8]) -> Result<Vec<u8>> {
    let historical: serde_json::Value = serde_json::from_slice(historical)?;
    let old_directory = historical["modules"]
        .as_array()
        .ok_or("historical modules missing")?
        .iter()
        .find(|module| module["name"] == "directory")
        .ok_or("historical Directory missing")?;
    let mut selected: serde_json::Value = serde_json::from_slice(registry.release_bytes())?;
    let modules = selected["modules"]
        .as_array_mut()
        .ok_or("current modules missing")?;
    let directory = modules
        .iter_mut()
        .find(|module| module["name"] == "directory")
        .ok_or("current Directory missing")?;
    assert_ne!(directory["code"], old_directory["code"]);
    *directory = old_directory.clone();
    // Preserve the exact historical Directory contract, including its code,
    // migration and operation bounds. No old Repository code is selected.
    selected["build"]["source_revision"] = "owned-directory-predecessor-fixture".into();
    let encoded = serde_json::to_vec(&selected)?;
    registry.verify_rolling_from(&encoded)?;
    Ok(encoded)
}
