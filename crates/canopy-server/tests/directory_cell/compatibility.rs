use super::*;
use cellule_runtime::Digest;

// Captured read-only from the unchanged RustFS deployment. The canonical
// descriptor's SHA-256 is 8b85c842995e4a2b0bcc6d1f3d361ccdbd2c33a1c52592241b50ad8cfe9aeb88.
// This is contract compatibility, not proof that old Cells have been restored.
#[test]
fn bounded_authentication_retains_the_exact_selected_predecessor()
-> Result<(), Box<dyn std::error::Error>> {
    let previous = include_bytes!("fixtures/c51-selected-release.json").trim_ascii_end();
    assert_eq!(
        blake3::hash(previous).to_hex().as_str(),
        "e31bf1a951e2fa19d91e9f964b2ddeade1a81b05a20ad628362819a1487c16b1"
    );
    let application = CanopyApplication::compile(build_descriptor(
        include_bytes!("../../../../Cargo.lock"),
        "directory-compatibility-test",
    ))?;
    let registry = application.registry();
    // Directory compatibility remains independent of the repository pack
    // schema. Whole-release rolling compatibility is fenced separately below.
    let predecessor: serde_json::Value = serde_json::from_slice(previous)?;
    let old_directory = predecessor["modules"]
        .as_array()
        .ok_or("modules missing")?
        .iter()
        .find(|module| module["name"] == "directory")
        .ok_or("directory missing")?;
    let bytes: [u8; 32] = hex::decode(old_directory["code"].as_str().ok_or("code missing")?)?
        .try_into()
        .map_err(|_| "invalid predecessor code length")?;
    let old_code = Digest::from_bytes(bytes);
    assert_ne!(Some(old_code), registry.module_code("directory"));
    assert!(registry.supports_cell(directory::DIRECTORY, CatalogRole::Sql, old_code, 1));
    for schema in [0, 2] {
        assert!(!registry.supports_cell(directory::DIRECTORY, CatalogRole::Sql, old_code, schema));
    }
    assert!(!registry.supports_cell(
        directory::DIRECTORY,
        CatalogRole::Sql,
        Digest::from_bytes([1; 32]),
        1
    ));
    let current: serde_json::Value = serde_json::from_slice(registry.release_bytes())?;
    let module = current["modules"]
        .as_array()
        .ok_or("modules missing")?
        .iter()
        .find(|module| module["name"] == "directory")
        .ok_or("directory missing")?;
    let queries = module["queries"].as_array().ok_or("queries missing")?;
    assert_eq!(
        queries
            .iter()
            .map(|operation| operation["id"].as_u64().unwrap())
            .collect::<Vec<_>>(),
        vec![2, 4, 5]
    );
    assert_eq!(queries[2]["input_limit"], 36);
    assert_eq!(queries[2]["output_limit"], 256);
    Ok(())
}

#[test]
fn packed_repository_rejects_an_unqualified_predecessor_upgrade()
-> Result<(), Box<dyn std::error::Error>> {
    let previous = include_bytes!("fixtures/c51-selected-release.json").trim_ascii_end();
    let application = CanopyApplication::compile(build_descriptor(
        include_bytes!("../../../../Cargo.lock"),
        "repository-upgrade-fence-test",
    ))?;
    let registry = application.registry();
    let predecessor: serde_json::Value = serde_json::from_slice(previous)?;
    let old_repository = predecessor["modules"]
        .as_array()
        .ok_or("modules missing")?
        .iter()
        .find(|module| module["name"] == "repository")
        .ok_or("repository missing")?;
    let bytes: [u8; 32] = hex::decode(old_repository["code"].as_str().ok_or("code missing")?)?
        .try_into()
        .map_err(|_| "invalid predecessor code length")?;
    assert!(!registry.supports_cell(
        canopy_server::REPOSITORIES,
        CatalogRole::Sql,
        Digest::from_bytes(bytes),
        1,
    ));
    assert!(
        registry.verify_rolling_from(previous).is_err(),
        "pack storage requires a qualified repository migration before rolling upgrade"
    );
    Ok(())
}
