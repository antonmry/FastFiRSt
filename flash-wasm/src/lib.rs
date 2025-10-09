#[cfg(target_arch = "wasm32")]
use std::str;

use anyhow::{Context, Result, anyhow, ensure};
use flash_lib::{CombineParams, FastqRecord, combine_pair_from_strs};
use serde::Serialize;
#[cfg(target_arch = "wasm32")]
use serde_json::json;
#[cfg(target_arch = "wasm32")]
use std::slice;

#[cfg(target_arch = "wasm32")]
static mut LAST_RESULT_LEN: usize = 0;

#[cfg_attr(not(target_arch = "wasm32"), allow(dead_code))]
#[derive(Serialize)]
struct FlashOutputs {
    combined: String,
    not_combined_1: String,
    not_combined_2: String,
}

#[cfg_attr(not(target_arch = "wasm32"), allow(dead_code))]
fn run_flash_internal(forward: &str, reverse: &str) -> Result<FlashOutputs> {
    let params = CombineParams::default();
    params.validate()?;

    let forward_reads = parse_fastq_records(forward, params.phred_offset)
        .context("failed to parse forward FASTQ contents")?;
    let reverse_reads = parse_fastq_records(reverse, params.phred_offset)
        .context("failed to parse reverse FASTQ contents")?;

    ensure!(
        forward_reads.len() == reverse_reads.len(),
        "FASTQ inputs have different number of records ({} vs {})",
        forward_reads.len(),
        reverse_reads.len()
    );

    let mut combined_out = String::new();
    let mut not_combined_left = String::new();
    let mut not_combined_right = String::new();

    for (read1, read2) in forward_reads.iter().zip(reverse_reads.iter()) {
        let outcome = combine_pair_from_strs(
            read1.tag(),
            &read1.seq_string(),
            &read1.qual_string(params.phred_offset),
            read2.tag(),
            &read2.seq_string(),
            &read2.qual_string(params.phred_offset),
            &params,
        )?;

        if outcome.is_combined {
            let tag = outcome
                .combined_tag
                .as_deref()
                .unwrap_or_else(|| read1.tag());
            let seq = outcome.combined_seq.as_deref().unwrap_or_default();
            let qual = outcome.combined_qual.as_deref().unwrap_or_default();

            append_fastq_record(&mut combined_out, tag, seq, qual);
        } else {
            let seq1 = read1.seq_string();
            let qual1 = read1.qual_string(params.phred_offset);
            append_fastq_record(&mut not_combined_left, read1.tag(), &seq1, &qual1);
            let seq2 = read2.seq_string();
            let qual2 = read2.qual_string(params.phred_offset);
            append_fastq_record(&mut not_combined_right, read2.tag(), &seq2, &qual2);
        }
    }

    Ok(FlashOutputs {
        combined: combined_out,
        not_combined_1: not_combined_left,
        not_combined_2: not_combined_right,
    })
}

fn parse_fastq_records(input: &str, phred_offset: u8) -> Result<Vec<FastqRecord>> {
    let mut records = Vec::new();
    let mut lines = input.lines();

    loop {
        let tag_line = match next_nonempty_line(&mut lines) {
            Some(line) => line,
            None => break,
        };

        ensure!(
            tag_line.starts_with('@'),
            "invalid FASTQ tag line: {}",
            tag_line
        );

        let seq_line = lines
            .next()
            .ok_or_else(|| anyhow!("unexpected EOF reading sequence for {}", tag_line))?;
        let plus_line = lines
            .next()
            .ok_or_else(|| anyhow!("unexpected EOF reading '+' line for {}", tag_line))?;
        let qual_line = lines
            .next()
            .ok_or_else(|| anyhow!("unexpected EOF reading quality for {}", tag_line))?;

        let seq = trim_cr(seq_line);
        let plus = trim_cr(plus_line);
        let qual = trim_cr(qual_line);

        ensure!(
            plus.starts_with('+'),
            "invalid FASTQ '+' separator: {}",
            plus
        );
        ensure!(
            seq.len() == qual.len(),
            "sequence and quality lengths differ for {}: {} vs {}",
            tag_line,
            seq.len(),
            qual.len()
        );

        let record = FastqRecord::from_strs(tag_line, seq, qual, phred_offset)?;
        records.push(record);
    }

    Ok(records)
}

fn next_nonempty_line<'a, I>(lines: &mut I) -> Option<&'a str>
where
    I: Iterator<Item = &'a str>,
{
    for line in lines {
        let trimmed = line.trim_end_matches(['\r', '\n']);
        if trimmed.is_empty() {
            continue;
        }
        return Some(trimmed);
    }
    None
}

fn trim_cr(line: &str) -> &str {
    line.trim_end_matches(['\r', '\n'])
}

fn append_fastq_record(target: &mut String, tag: &str, seq: &str, qual: &str) {
    push_line(target, tag);
    push_line(target, seq);
    target.push_str("+\n");
    push_line(target, qual);
}

fn push_line(target: &mut String, line: &str) {
    target.push_str(line);
    if !line.ends_with('\n') {
        target.push('\n');
    }
}

#[cfg(target_arch = "wasm32")]
#[no_mangle]
pub extern "C" fn flash_alloc(len: usize) -> *mut u8 {
    let mut buf = Vec::<u8>::with_capacity(len);
    let ptr = buf.as_mut_ptr();
    std::mem::forget(buf);
    ptr
}

#[cfg(target_arch = "wasm32")]
#[no_mangle]
pub extern "C" fn flash_dealloc(ptr: *mut u8, len: usize) {
    if ptr.is_null() {
        return;
    }
    unsafe {
        let _ = Vec::from_raw_parts(ptr, 0, len);
    }
}

#[cfg(target_arch = "wasm32")]
#[no_mangle]
pub extern "C" fn flash_run(
    forward_ptr: *const u8,
    forward_len: usize,
    reverse_ptr: *const u8,
    reverse_len: usize,
) -> *mut u8 {
    let forward = unsafe { slice::from_raw_parts(forward_ptr, forward_len) };
    let reverse = unsafe { slice::from_raw_parts(reverse_ptr, reverse_len) };

    let response = match (str::from_utf8(forward), str::from_utf8(reverse)) {
        (Ok(f), Ok(r)) => match run_flash_internal(f, r) {
            Ok(outputs) => json!({
                "status": "ok",
                "combined": outputs.combined,
                "not_combined_1": outputs.not_combined_1,
                "not_combined_2": outputs.not_combined_2,
            })
            .to_string(),
            Err(err) => json!({
                "status": "error",
                "error": err.to_string(),
            })
            .to_string(),
        },
        (Err(_), _) | (_, Err(_)) => json!({
            "status": "error",
            "error": "FASTQ inputs must be valid UTF-8",
        })
        .to_string(),
    };

    let mut bytes = response.into_bytes().into_boxed_slice();
    let ptr = bytes.as_mut_ptr();
    unsafe {
        LAST_RESULT_LEN = bytes.len();
    }
    std::mem::forget(bytes);
    ptr
}

#[cfg(target_arch = "wasm32")]
#[no_mangle]
pub extern "C" fn flash_result_len() -> usize {
    unsafe { LAST_RESULT_LEN }
}

#[cfg(target_arch = "wasm32")]
#[no_mangle]
pub extern "C" fn flash_free_result(ptr: *mut u8, len: usize) {
    if ptr.is_null() {
        return;
    }
    unsafe {
        let _ = Vec::from_raw_parts(ptr, len, len);
        LAST_RESULT_LEN = 0;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use flash_lib::{CombineParams, merge_fastq_files};
    use std::fs;
    use std::path::PathBuf;
    use tempfile::tempdir;

    #[test]
    fn wasm_flash_matches_reference_outputs() -> anyhow::Result<()> {
        let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let forward = manifest_dir.join("../input1.fq");
        let reverse = manifest_dir.join("../input2.fq");
        let forward_text = fs::read_to_string(&forward)?;
        let reverse_text = fs::read_to_string(&reverse)?;

        let tmp = tempdir()?;
        merge_fastq_files(
            &forward,
            &reverse,
            tmp.path(),
            "ref",
            &CombineParams::default(),
        )?;

        let outputs = run_flash_internal(&forward_text, &reverse_text)?;

        let expected_combined = fs::read_to_string(tmp.path().join("ref.extendedFrags.fastq"))?;
        let expected_not1 = fs::read_to_string(tmp.path().join("ref.notCombined_1.fastq"))?;
        let expected_not2 = fs::read_to_string(tmp.path().join("ref.notCombined_2.fastq"))?;

        assert_eq!(outputs.combined, expected_combined);
        assert_eq!(outputs.not_combined_1, expected_not1);
        assert_eq!(outputs.not_combined_2, expected_not2);

        Ok(())
    }
}
