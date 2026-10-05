//! One bounded operation contract shared by production and qualification.
use super::*;
use cellule_runtime::registry::OperationDescriptor;

const fn command<C: Command>(input_limit: u32, output_limit: u32) -> OperationDescriptor {
    OperationDescriptor {
        id: C::ID,
        codec_version: C::CODEC_VERSION,
        schema_min: 1,
        schema_max: 1,
        input_limit,
        output_limit,
    }
}
const fn query<Q: Query>(input_limit: u32, output_limit: u32) -> OperationDescriptor {
    OperationDescriptor {
        id: Q::ID,
        codec_version: Q::CODEC_VERSION,
        schema_min: 1,
        schema_max: 1,
        input_limit,
        output_limit,
    }
}

pub(crate) const COMMANDS: [OperationDescriptor; 21] = [
    crate::operation(1),
    command::<crate::branch_rules::command::SetBranchRule>(64 << 10, 64),
    command::<AbortPreparation>(4096, 4096),
    command::<ReapPreparation>(4096, 4096),
    command::<RegisterCatalogAttestation>(4096, 4096),
    command::<PublishCatalogCompaction>(4096, 4096),
    command::<RegisterStagedInputs>(4096, 4096),
    command::<InitializeCatalogRefs>(INITIALIZATION_BYTES, 512),
    command::<RegisterRefPolicyPage>(REF_POLICY_PAGE_BYTES, 128),
    command::<ReapRefPolicyGuard>(4096, 128),
    command::<CompleteRootPush>(ROOT_COMPLETION_BYTES, 512),
    command::<CompleteRootOutcome>(ROOT_COMPLETION_BYTES, 512),
    command::<RegisterRootRecovery>(4096, 4096),
    command::<ReleaseTerminalRecovery>(4096, 128),
    command::<RegisterCustodyIntent>(4096, 4096),
    command::<ExecuteCustody>(1024, 512),
    command::<StopCustodyIntent>(1024, 128),
    command::<ReleaseServingPin>(1024, 128),
    command::<crate::checks::native::StartCommitCheck>(4096, 16),
    command::<crate::pulls::native::CreateNativePull>(crate::pulls::native::INPUT_BYTES, 16),
    command::<crate::pulls::native::ReviewNativePull>(crate::pulls::native::INPUT_BYTES, 16),
];
pub(crate) const QUERIES: [OperationDescriptor; 13] = [
    crate::operation(2),
    query::<CheckPreparation>(4096, 4096),
    query::<CheckPreparationFrontier>(4096, 4096),
    query::<CheckCompletedCompaction>(4096, 4096),
    query::<CheckStaging>(4096, 4096),
    query::<CheckStagedInputs>(4096, 4096),
    query::<CheckInitializedCatalog>(4096, 512),
    query::<CheckRefPolicyGuard>(4096, 128),
    query::<CheckCompletedRootPush>(4096, 512),
    query::<CheckServingPin>(1024, 1024),
    query::<SelectServingGeneration>(1024, 512),
    query::<crate::checks::native::ReadCommitChecks>(4096, 256 << 10),
    query::<crate::pulls::native::ReadNativePulls>(
        crate::pulls::native::INPUT_BYTES,
        crate::pulls::native::OUTPUT_BYTES,
    ),
];

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{CanopyApplication, RepositoryModule, build_descriptor};
    use cellule_app::CellApplication;
    use cellule_runtime::CellModule;

    #[test]
    fn production_registers_packed_and_policy_metadata_contracts() -> cellule_runtime::Result<()> {
        let application = CanopyApplication::compile(build_descriptor(
            include_bytes!("../../../../../Cargo.lock"),
            env!("CARGO_PKG_VERSION"),
        ))?;
        assert!(
            application
                .registry()
                .module_code(RepositoryModule::NAME)
                .is_some()
        );
        let descriptor = RepositoryModule.descriptor();
        let ids: Vec<_> = descriptor
            .commands
            .iter()
            .map(|operation| operation.id)
            .collect();
        assert_eq!(
            ids,
            vec![
                1, 8, 14, 16, 17, 22, 29, 31, 33, 35, 36, 38, 39, 40, 41, 42, 43, 46, 49, 51, 53
            ]
        );
        assert_eq!(
            descriptor
                .queries
                .iter()
                .map(|operation| operation.id)
                .collect::<Vec<_>>(),
            vec![2, 15, 21, 23, 27, 30, 32, 34, 37, 47, 48, 50, 52]
        );
        for (id, codec, input, output) in [
            (
                33,
                RegisterRefPolicyPage::CODEC_VERSION,
                REF_POLICY_PAGE_BYTES,
                128,
            ),
            (
                36,
                CompleteRootPush::CODEC_VERSION,
                ROOT_COMPLETION_BYTES,
                512,
            ),
            (
                38,
                CompleteRootOutcome::CODEC_VERSION,
                ROOT_COMPLETION_BYTES,
                512,
            ),
            (39, RegisterRootRecovery::CODEC_VERSION, 4096, 4096),
            (40, ReleaseTerminalRecovery::CODEC_VERSION, 4096, 128),
            (41, RegisterCustodyIntent::CODEC_VERSION, 4096, 4096),
            (42, ExecuteCustody::CODEC_VERSION, 1024, 512),
            (43, StopCustodyIntent::CODEC_VERSION, 1024, 128),
            (46, ReleaseServingPin::CODEC_VERSION, 1024, 128),
            (
                49,
                crate::checks::native::StartCommitCheck::CODEC_VERSION,
                4096,
                16,
            ),
            (
                51,
                crate::pulls::native::CreateNativePull::CODEC_VERSION,
                crate::pulls::native::INPUT_BYTES,
                16,
            ),
            (
                53,
                crate::pulls::native::ReviewNativePull::CODEC_VERSION,
                crate::pulls::native::INPUT_BYTES,
                16,
            ),
        ] {
            let operation = descriptor
                .commands
                .iter()
                .find(|value| value.id == id)
                .unwrap();
            assert_eq!(
                (
                    operation.codec_version,
                    operation.input_limit,
                    operation.output_limit
                ),
                (codec, input, output)
            );
        }
        Ok(())
    }
}
