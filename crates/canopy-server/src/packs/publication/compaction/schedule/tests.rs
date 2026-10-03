use super::*;
use canopy_object_storage::artifact::ArtifactDescriptor;

fn root(format: ObjectFormat, operation: u8, objects: u64) -> NodeRef {
    let mut first = vec![0; format.bytes()];
    first[0] = 1;
    let mut last = first.clone();
    last[0] = 2;
    NodeRef {
        operation: [operation; 16],
        artifact: ArtifactDescriptor {
            size: 1024,
            digest: [operation; 32],
            manifest_digest: [3; 32],
        },
        height: 0,
        first_key: first.try_into().unwrap(),
        last_key: last.try_into().unwrap(),
        record_count: 1,
        object_count: objects,
    }
}

#[test]
fn geometric_targets_reject_invalid_profiles_and_final_level_overflow() {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let policy = CompactionPolicy {
            base_objects: 2,
            level_ratio: 2,
            ..CompactionPolicy::default()
        };
        let mut directory = DirectorySnapshot::empty([1; 16], format);
        directory.levels = vec![Some(root(format, 1, 2)), Some(root(format, 2, 4))];
        let pressure = policy.pressure(&directory).unwrap();
        assert_eq!(&pressure.level_targets[..4], &[2, 4, 8, 16]);
        assert!(
            CompactionPlanner::new(policy)
                .unwrap()
                .choose(&directory)
                .unwrap()
                .is_none()
        );
        directory.levels.resize(MAX_LEVELS, None);
        directory.levels[MAX_LEVELS - 1] = Some(root(format, 3, (2 << (MAX_LEVELS - 1)) + 1));
        assert!(matches!(
            policy.pressure(&directory),
            Err(IndexError::Limit)
        ));
        for bad in [
            CompactionPolicy {
                base_objects: 0,
                ..policy
            },
            CompactionPolicy {
                base_objects: u64::MAX,
                ..policy
            },
            CompactionPolicy {
                level_ratio: 1,
                ..policy
            },
            CompactionPolicy {
                level_ratio: 17,
                ..policy
            },
            CompactionPolicy {
                ingress_high_water: 0,
                ..policy
            },
            CompactionPolicy {
                ingress_high_water: LEVEL_ZERO_ROOTS + 1,
                ..policy
            },
            CompactionPolicy {
                urgent_burst: 0,
                ..policy
            },
            CompactionPolicy {
                urgent_burst: 33,
                ..policy
            },
        ] {
            assert!(CompactionPlanner::new(bad).is_err());
        }
        let saturated = CompactionPolicy {
            base_objects: i64::MAX as u64 / 2,
            level_ratio: 16,
            ..policy
        }
        .targets()
        .unwrap();
        assert!(
            saturated[1..]
                .iter()
                .all(|target| *target == i64::MAX as u64)
        );
    }
}

#[test]
fn urgent_ingress_cannot_starve_any_pressured_level_and_roots_rotate() {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let policy = CompactionPolicy {
            base_objects: 1,
            level_ratio: 2,
            ingress_high_water: 2,
            urgent_burst: 3,
        };
        let mut planner = CompactionPlanner::new(policy).unwrap();
        let mut directory = DirectorySnapshot::empty([1; 16], format);
        directory.level_zero = (1..=5)
            .map(|operation| root(format, operation, 10))
            .collect();
        directory.levels = (0..MAX_LEVELS - 1)
            .map(|at| Some(root(format, at as u8 + 10, (1 << at) + 1)))
            .collect();
        let through = directory.level_zero[0].first_key;
        let mut levels = Vec::new();
        let mut ingress = Vec::new();
        let mut streak = 0;
        for _ in 0..4 * (MAX_LEVELS - 1) {
            let choice = planner.choose(&directory).unwrap().unwrap();
            match choice.source {
                CompactionSource::Ingress(at) => {
                    ingress.push(at);
                    streak += 1;
                    assert!(streak <= 3);
                }
                CompactionSource::Level(at) => {
                    levels.push(at);
                    streak = 0;
                }
            }
            planner.advance(&directory, choice, through);
        }
        assert_eq!(levels, (0..MAX_LEVELS - 1).collect::<Vec<_>>());
        assert!(ingress.iter().enumerate().all(|(n, at)| *at == n % 5));
        let mut foreign = directory.clone();
        foreign.repository = [2; 16];
        assert!(matches!(
            planner.choose(&foreign),
            Err(IndexError::Integrity)
        ));
        foreign.repository = directory.repository;
        foreign.format = if format == ObjectFormat::Sha1 {
            ObjectFormat::Sha256
        } else {
            ObjectFormat::Sha1
        };
        // Use a structurally valid foreign-format snapshot, so this assertion
        // tests planner context binding rather than malformed OID widths.
        foreign.level_zero = vec![root(foreign.format, 1, 10)];
        foreign.levels.clear();
        foreign.validate().unwrap();
        assert!(planner.choose(&foreign).is_err());
        // Tail ingress is not stranded below the urgent watermark.
        directory.level_zero.truncate(1);
        directory.levels.clear();
        assert_eq!(
            planner.choose(&directory).unwrap().unwrap().source,
            CompactionSource::Ingress(0)
        );
    }
}
