use std::fs;
use std::path::PathBuf;

use flash_lib::{CombineParams, merge_fastq_files};
use tempfile::tempdir;

#[test]
fn reproduces_flash_outputs_for_sample_pair() {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let data_dir = manifest_dir.join("../../FLASH-lowercase-overhang");
    assert!(
        data_dir.exists(),
        "reference data directory missing: {:?}",
        data_dir
    );

    let input1 = data_dir.join("input1.fq");
    let input2 = data_dir.join("input2.fq");

    let expected_extended = data_dir.join("out.extendedFrags.fastq");
    let expected_not1 = data_dir.join("out.notCombined_1.fastq");
    let expected_not2 = data_dir.join("out.notCombined_2.fastq");

    let tmp_dir = tempdir().expect("failed to create temp dir");
    let output_dir = tmp_dir.path();

    let params = CombineParams::default();
    merge_fastq_files(&input1, &input2, output_dir, "out", &params)
        .expect("merge_fastq_files should succeed");

    let produced_extended = output_dir.join("out.extendedFrags.fastq");
    let produced_not1 = output_dir.join("out.notCombined_1.fastq");
    let produced_not2 = output_dir.join("out.notCombined_2.fastq");

    assert_eq!(
        fs::read(&produced_extended).unwrap(),
        fs::read(&expected_extended).unwrap()
    );
    assert_eq!(
        fs::read(&produced_not1).unwrap(),
        fs::read(&expected_not1).unwrap()
    );
    assert_eq!(
        fs::read(&produced_not2).unwrap(),
        fs::read(&expected_not2).unwrap()
    );
}
