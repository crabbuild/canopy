use super::*;

fn apply(old: &str, hunks: &[Hunk]) -> String {
    let old: Vec<_> = old.split_inclusive('\n').collect();
    let mut result = String::new();
    let mut cursor = 0;
    for hunk in hunks {
        let start = hunk.old_start - usize::from(hunk.old_lines != 0);
        for line in &old[cursor..start] {
            result.push_str(line);
        }
        cursor = start;
        let mut new_lines = 0;
        for line in &hunk.lines {
            let text = format!("{}{}", line.text, if line.no_newline { "" } else { "\n" });
            if line.kind != Kind::Add {
                assert_eq!(old[cursor], text);
                cursor += 1;
            }
            if line.kind != Kind::Delete {
                result.push_str(&text);
                new_lines += 1;
            }
        }
        assert_eq!(cursor - start, hunk.old_lines);
        assert_eq!(new_lines, hunk.new_lines);
    }
    for line in &old[cursor..] {
        result.push_str(line);
    }
    result
}
fn minimum_edits(old: &[&str], new: &[&str]) -> usize {
    let mut row: Vec<usize> = (0..=new.len()).collect();
    for (i, before) in old.iter().enumerate() {
        let mut next = vec![i + 1; new.len() + 1];
        for (j, after) in new.iter().enumerate() {
            next[j + 1] = if before == after {
                row[j]
            } else {
                1 + row[j + 1].min(next[j])
            };
        }
        row = next;
    }
    row[new.len()]
}
#[test]
fn shortest_scripts_reconstruct_every_small_repeated_line_sequence() {
    let sequences: Vec<String> = (0..=5)
        .flat_map(|len| {
            (0..(1 << len)).map(move |bits| {
                (0..len)
                    .map(|i| if bits & (1 << i) == 0 { "a\n" } else { "b\n" })
                    .collect()
            })
        })
        .collect();
    for old in &sequences {
        for new in &sequences {
            let old_lines: Vec<_> = old.split_inclusive('\n').collect();
            let new_lines: Vec<_> = new.split_inclusive('\n').collect();
            let edits = shortest_edits(&old_lines, &new_lines).unwrap();
            assert_eq!(
                edits.iter().filter(|e| e.kind != Kind::Context).count(),
                minimum_edits(&old_lines, &new_lines)
            );
            assert_eq!(
                apply(old, &diff(old.as_bytes(), new.as_bytes()).unwrap()),
                *new
            );
        }
    }
}
#[test]
fn hunks_preserve_crlf_unicode_and_missing_final_newlines() {
    for (old, new) in [
        ("one\r\ntwo\r\n", "one\r\n二\r\n"),
        ("same", "same\n"),
        ("", "new"),
        ("gone", ""),
        ("a\nb\nend", "a\nchanged\nend"),
    ] {
        assert_eq!(
            apply(old, &diff(old.as_bytes(), new.as_bytes()).unwrap()),
            new
        );
    }
}
#[test]
fn separated_changes_keep_three_context_lines_and_correct_offsets() {
    let old: String = (0..30).map(|n| format!("line {n}\n")).collect();
    let new = old
        .replace("line 5\n", "first\ninserted\n")
        .replace("line 24\n", "last\n");
    let hunks = diff(old.as_bytes(), new.as_bytes()).unwrap();
    assert_eq!(hunks.len(), 2);
    assert_eq!(
        (
            hunks[0].old_start,
            hunks[0].old_lines,
            hunks[0].new_start,
            hunks[0].new_lines
        ),
        (3, 7, 3, 8)
    );
    assert_eq!(
        (
            hunks[1].old_start,
            hunks[1].old_lines,
            hunks[1].new_start,
            hunks[1].new_lines
        ),
        (22, 7, 23, 7)
    );
    assert_eq!(apply(&old, &hunks), new);
}
#[test]
fn append_and_delete_large_contiguous_blocks_avoid_quadratic_trace() {
    let common = "common\n".repeat(5000);
    let new = format!("{common}{}", "added\n".repeat(2000));
    for (old, new) in [(&common, &new), (&new, &common)] {
        assert_eq!(
            apply(old, &diff(old.as_bytes(), new.as_bytes()).unwrap()),
            *new
        );
    }
}
#[test]
fn input_work_and_output_limits_return_no_partial_patch() {
    for (old, new) in [
        ("same\n".repeat(MAX_LINES + 1), "new\n".into()),
        ("a\n".repeat(2000), "b\n".repeat(2000)),
        (String::new(), "new\n".repeat(MAX_OUTPUT_LINES + 1)),
        ("x".repeat(200_000), "y".repeat(200_000)),
        (
            format!("{}\n", "a".repeat(399)).repeat(600),
            format!("{}\n", "b".repeat(399)).repeat(600),
        ),
    ] {
        assert!(matches!(
            diff(old.as_bytes(), new.as_bytes()),
            Err(ReadError::TooLarge)
        ));
    }
}
