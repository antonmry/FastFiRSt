use std::fs;
use std::path::PathBuf;

use perf_lib::RepeatIndex;
use perf_lib::motif::build_rep_set;
use perf_lib::scanner::scan_fasta;
use perf_lib::{MotifConfig, ScanConfig};
use tempfile::tempdir;

#[test]
fn reproduces_expected_fasta_output() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let input = root.join("tests/data/test_input.fa");
    let expected = root.join("tests/data/test_input_perf.tsv");

    let temp = tempdir().expect("failed to create temp dir");
    let output = temp.path().join("output.tsv");

    let repeats = build_rep_set(&MotifConfig::default()).expect("build repeat set");
    let index = RepeatIndex::new(repeats);
    scan_fasta(&input, &output, &index, &ScanConfig::default()).expect("scan fasta");

    let produced = fs::read_to_string(&output).expect("read produced output");
    let mut expected_text = fs::read_to_string(&expected).expect("read expected output");
    expected_text = expected_text.replace("INPUT_PLACEHOLDER", &input.display().to_string());

    assert_eq!(produced, expected_text);
}
