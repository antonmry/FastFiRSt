use std::fs::{self, File};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::Path;

use anyhow::{Context, Result, bail, ensure};

#[derive(Debug, Clone)]
pub struct CombineParams {
    pub min_overlap: usize,
    pub max_overlap: usize,
    pub max_mismatch_density: f32,
    pub cap_mismatch_quals: bool,
    pub allow_outies: bool,
    pub lowercase_overhang: bool,
    pub phred_offset: u8,
}

impl Default for CombineParams {
    fn default() -> Self {
        Self {
            min_overlap: 10,
            max_overlap: 65,
            max_mismatch_density: 0.25,
            cap_mismatch_quals: false,
            allow_outies: false,
            lowercase_overhang: false,
            phred_offset: 33,
        }
    }
}

impl CombineParams {
    pub fn validate(&self) -> Result<()> {
        ensure!(self.min_overlap >= 1, "min-overlap must be >= 1");
        ensure!(self.max_overlap >= 1, "max-overlap must be >= 1");
        ensure!(
            self.max_overlap >= self.min_overlap,
            "max-overlap ({}) cannot be less than min-overlap ({})",
            self.max_overlap,
            self.min_overlap
        );
        ensure!(
            self.max_mismatch_density >= 0.0,
            "max-mismatch-density must be non-negative"
        );
        ensure!(
            self.phred_offset <= 127,
            "phred-offset must be in the range [0, 127]"
        );
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CombineStatus {
    CombinedInnie,
    CombinedOutie,
}

#[derive(Debug, Clone)]
struct Read {
    tag: String,
    seq: Vec<u8>,
    qual: Vec<u8>,
}

impl Read {
    fn len(&self) -> usize {
        self.seq.len()
    }
}

struct AlignmentCandidate {
    position: usize,
    mismatch_density: f32,
    qual_score: f32,
}

struct MismatchStats {
    effective_len: usize,
    mismatches: u32,
    mismatch_qual_total: u32,
}

pub fn merge_fastq_files(
    forward: impl AsRef<Path>,
    reverse: impl AsRef<Path>,
    output_dir: impl AsRef<Path>,
    output_prefix: &str,
    params: &CombineParams,
) -> Result<()> {
    params.validate()?;

    let forward = forward.as_ref();
    let reverse = reverse.as_ref();
    let output_dir = output_dir.as_ref();

    fs::create_dir_all(output_dir)
        .with_context(|| format!("failed to create output directory {:?}", output_dir))?;

    let mut reader1 = BufReader::new(
        File::open(forward)
            .with_context(|| format!("failed to open forward FASTQ {:?}", forward))?,
    );
    let mut reader2 = BufReader::new(
        File::open(reverse)
            .with_context(|| format!("failed to open reverse FASTQ {:?}", reverse))?,
    );

    let ext_path = output_dir.join(format!("{}.extendedFrags.fastq", output_prefix));
    let not1_path = output_dir.join(format!("{}.notCombined_1.fastq", output_prefix));
    let not2_path = output_dir.join(format!("{}.notCombined_2.fastq", output_prefix));

    let mut out_extended = BufWriter::new(
        File::create(&ext_path).with_context(|| format!("failed to create {:?}", ext_path))?,
    );
    let mut out_not1 = BufWriter::new(
        File::create(&not1_path).with_context(|| format!("failed to create {:?}", not1_path))?,
    );
    let mut out_not2 = BufWriter::new(
        File::create(&not2_path).with_context(|| format!("failed to create {:?}", not2_path))?,
    );

    loop {
        let read1 = read_fastq_record(&mut reader1, forward, params.phred_offset)?;
        let read2 = read_fastq_record(&mut reader2, reverse, params.phred_offset)?;

        match (read1, read2) {
            (Some(r1), Some(r2)) => {
                process_pair(
                    &r1,
                    &r2,
                    params,
                    &mut out_extended,
                    &mut out_not1,
                    &mut out_not2,
                )?;
            }
            (None, None) => break,
            (Some(_), None) | (None, Some(_)) => {
                bail!("FASTQ inputs have different number of records")
            }
        }
    }

    out_extended
        .flush()
        .context("failed to flush extendedFrags writer")?;
    out_not1
        .flush()
        .context("failed to flush notCombined_1 writer")?;
    out_not2
        .flush()
        .context("failed to flush notCombined_2 writer")?;

    Ok(())
}

fn process_pair<W: Write>(
    read1: &Read,
    read2: &Read,
    params: &CombineParams,
    out_extended: &mut W,
    out_not1: &mut W,
    out_not2: &mut W,
) -> Result<()> {
    let mut read2_rev = read2.clone();
    reverse_complement(&mut read2_rev);

    if let Some(mut combined) = combine_pair(read1, &read2_rev, params) {
        combined.tag = combined_tag(read1);
        write_fastq(out_extended, &combined, params.phred_offset)?;
    } else {
        write_fastq(out_not1, read1, params.phred_offset)?;
        write_fastq(out_not2, read2, params.phred_offset)?;
    }

    Ok(())
}

fn read_fastq_record<R: BufRead>(
    reader: &mut R,
    source: &Path,
    phred_offset: u8,
) -> Result<Option<Read>> {
    let mut tag_line = String::new();
    if reader.read_line(&mut tag_line)? == 0 {
        return Ok(None);
    }

    let mut seq_line = String::new();
    if reader.read_line(&mut seq_line)? == 0 {
        bail!("unexpected EOF reading sequence in {:?}", source);
    }

    let mut plus_line = String::new();
    if reader.read_line(&mut plus_line)? == 0 {
        bail!("unexpected EOF reading '+' separator in {:?}", source);
    }

    let mut qual_line = String::new();
    if reader.read_line(&mut qual_line)? == 0 {
        bail!("unexpected EOF reading quality in {:?}", source);
    }

    trim_newline(&mut tag_line);
    trim_newline(&mut seq_line);
    trim_newline(&mut plus_line);
    trim_newline(&mut qual_line);

    ensure!(
        !tag_line.is_empty() && tag_line.starts_with('@'),
        "invalid FASTQ tag line in {:?}: {}",
        source,
        tag_line
    );
    ensure!(
        !plus_line.is_empty() && plus_line.starts_with('+'),
        "invalid FASTQ '+' separator in {:?}: {}",
        source,
        plus_line
    );
    ensure!(
        seq_line.len() == qual_line.len(),
        "sequence and quality lengths differ in {:?}: {} vs {}",
        source,
        seq_line.len(),
        qual_line.len()
    );

    if seq_line
        .bytes()
        .any(|b| matches!(b, b' ' | b'\t' | b'\r' | b'\n'))
    {
        bail!("sequence line contains whitespace in {:?}", source);
    }

    let seq: Vec<u8> = seq_line.bytes().map(canonical_base).collect();
    let mut qual = Vec::with_capacity(qual_line.len());
    for (idx, byte) in qual_line.bytes().enumerate() {
        if phred_offset > 0 && byte < phred_offset {
            bail!(
                "quality char below PHRED offset ({}) at position {} in {:?}",
                phred_offset,
                idx,
                source
            );
        }
        qual.push(byte.saturating_sub(phred_offset));
    }

    Ok(Some(Read {
        tag: tag_line,
        seq,
        qual,
    }))
}

fn trim_newline(line: &mut String) {
    while matches!(line.chars().last(), Some('\n') | Some('\r')) {
        line.pop();
    }
}

fn canonical_base(b: u8) -> u8 {
    match b {
        b'A' | b'a' => b'A',
        b'C' | b'c' => b'C',
        b'G' | b'g' => b'G',
        b'T' | b't' => b'T',
        b'N' | b'n' => b'N',
        _ => b'N',
    }
}

fn complement(base: u8) -> u8 {
    match base {
        b'A' => b'T',
        b'T' => b'A',
        b'C' => b'G',
        b'G' => b'C',
        _ => b'N',
    }
}

fn to_lower(base: u8) -> u8 {
    if (b'A'..=b'Z').contains(&base) {
        base + 32
    } else {
        base
    }
}

fn reverse_complement(read: &mut Read) {
    let len = read.seq.len();
    for i in 0..(len / 2) {
        let j = len - 1 - i;
        let base_i = read.seq[i];
        let base_j = read.seq[j];
        read.seq[i] = complement(base_j);
        read.seq[j] = complement(base_i);
        let qual_i = read.qual[i];
        read.qual[i] = read.qual[j];
        read.qual[j] = qual_i;
    }
    if len % 2 == 1 {
        let mid = len / 2;
        read.seq[mid] = complement(read.seq[mid]);
    }
}

fn combine_pair(read1: &Read, read2_rev: &Read, params: &CombineParams) -> Option<Read> {
    let mut best: Option<(AlignmentCandidate, CombineStatus)> =
        evaluate_alignment(read1, read2_rev, params)
            .map(|candidate| (candidate, CombineStatus::CombinedInnie));

    if params.allow_outies {
        if let Some(candidate) = evaluate_alignment(read2_rev, read1, params) {
            match &best {
                None => best = Some((candidate, CombineStatus::CombinedOutie)),
                Some((best_cand, _)) => {
                    if candidate.mismatch_density < best_cand.mismatch_density
                        || (candidate.mismatch_density == best_cand.mismatch_density
                            && candidate.qual_score < best_cand.qual_score)
                    {
                        best = Some((candidate, CombineStatus::CombinedOutie));
                    }
                }
            }
        }
    }

    let (candidate, status) = best?;
    let (first, second) = match status {
        CombineStatus::CombinedInnie => (read1, read2_rev),
        CombineStatus::CombinedOutie => (read2_rev, read1),
    };
    Some(generate_combined_read(
        first,
        second,
        candidate.position,
        params,
    ))
}

fn evaluate_alignment(
    first: &Read,
    second: &Read,
    params: &CombineParams,
) -> Option<AlignmentCandidate> {
    if first.len() < params.min_overlap {
        return None;
    }

    let have_n = first.seq.contains(&b'N') || second.seq.contains(&b'N');
    let mut best_density = params.max_mismatch_density + 1.0;
    let mut best_qual_score = 0.0f32;
    let mut best_position: Option<usize> = None;

    let start = if first.len() > second.len() {
        first.len() - second.len()
    } else {
        0
    };
    let end = first
        .len()
        .saturating_sub(params.min_overlap)
        .saturating_add(1);

    for i in start..end {
        let overlap_len = first.len().saturating_sub(i);
        if overlap_len > second.len() {
            continue;
        }

        let stats = compute_mismatch_stats(
            &first.seq[i..first.len()],
            &second.seq[..overlap_len],
            &first.qual[i..first.len()],
            &second.qual[..overlap_len],
            have_n,
        );

        if stats.effective_len >= params.min_overlap {
            let score_len = (stats.effective_len.min(params.max_overlap)).max(1) as f32;
            let mismatch_density = stats.mismatches as f32 / score_len;
            let qual_score = stats.mismatch_qual_total as f32 / score_len;

            if mismatch_density <= best_density
                && (mismatch_density < best_density || qual_score < best_qual_score)
            {
                best_density = mismatch_density;
                best_qual_score = qual_score;
                best_position = Some(i);
            }
        }
    }

    let position = best_position?;
    if best_density > params.max_mismatch_density {
        return None;
    }

    Some(AlignmentCandidate {
        position,
        mismatch_density: best_density,
        qual_score: best_qual_score,
    })
}

fn compute_mismatch_stats(
    seq1: &[u8],
    seq2: &[u8],
    qual1: &[u8],
    qual2: &[u8],
    have_n: bool,
) -> MismatchStats {
    let mut effective_len = seq1.len();
    let mut mismatches: u32 = 0;
    let mut mismatch_qual_total: u32 = 0;

    if have_n {
        for i in 0..seq1.len() {
            if seq1[i] == b'N' || seq2[i] == b'N' {
                effective_len -= 1;
            } else if seq1[i] != seq2[i] {
                mismatches += 1;
                mismatch_qual_total += qual1[i].min(qual2[i]) as u32;
            }
        }
    } else {
        for i in 0..seq1.len() {
            if seq1[i] != seq2[i] {
                mismatches += 1;
                mismatch_qual_total += qual1[i].min(qual2[i]) as u32;
            }
        }
    }

    MismatchStats {
        effective_len,
        mismatches,
        mismatch_qual_total,
    }
}

fn generate_combined_read(
    read1: &Read,
    read2: &Read,
    overlap_begin: usize,
    params: &CombineParams,
) -> Read {
    let overlap_len = read1.len() - overlap_begin;
    let remaining_len = read2.len().saturating_sub(overlap_len);
    let combined_len = read1.len() + remaining_len;

    let mut seq = Vec::with_capacity(combined_len);
    let mut qual = Vec::with_capacity(combined_len);

    for idx in 0..overlap_begin {
        let base = read1.seq[idx];
        let base = if params.lowercase_overhang {
            to_lower(base)
        } else {
            base
        };
        seq.push(base);
        qual.push(read1.qual[idx]);
    }

    for offset in 0..overlap_len {
        let base1 = read1.seq[overlap_begin + offset];
        let base2 = read2.seq[offset];
        let q1 = read1.qual[overlap_begin + offset];
        let q2 = read2.qual[offset];

        if base1 == base2 {
            seq.push(base1);
            qual.push(q1.max(q2));
        } else {
            let q = if params.cap_mismatch_quals {
                q1.min(q2).min(2)
            } else {
                q1.abs_diff(q2).max(2)
            };
            let base = if q1 > q2 {
                base1
            } else if q2 > q1 {
                base2
            } else if base2 == b'N' {
                base1
            } else {
                base2
            };
            seq.push(base);
            qual.push(q);
        }
    }

    for idx in overlap_len..read2.len() {
        let base = read2.seq[idx];
        let base = if params.lowercase_overhang {
            to_lower(base)
        } else {
            base
        };
        seq.push(base);
        qual.push(read2.qual[idx]);
    }

    Read {
        tag: String::new(),
        seq,
        qual,
    }
}

fn combined_tag(read1: &Read) -> String {
    let tag = &read1.tag;
    if let Some(pos) = tag.rfind('/') {
        if pos + 2 < tag.len() && tag.as_bytes()[pos + 2] == b'#' {
            format!("{}{}", &tag[..pos], &tag[pos + 2..])
        } else {
            tag[..pos].to_string()
        }
    } else {
        tag.clone()
    }
}

fn write_fastq<W: Write>(writer: &mut W, read: &Read, phred_offset: u8) -> Result<()> {
    writer
        .write_all(read.tag.as_bytes())
        .context("failed to write FASTQ tag")?;
    writer.write_all(b"\n").context("failed to write newline")?;
    writer
        .write_all(&read.seq)
        .context("failed to write FASTQ sequence")?;
    writer
        .write_all(b"\n+\n")
        .context("failed to write FASTQ separator")?;

    let mut qual_buf = Vec::with_capacity(read.qual.len());
    for &q in &read.qual {
        qual_buf.push(q + phred_offset);
    }
    writer
        .write_all(&qual_buf)
        .context("failed to write FASTQ quality")?;
    writer.write_all(b"\n").context("failed to write newline")?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn trim_newline_removes_crlf() {
        let mut s = String::from("TAG\r\n");
        trim_newline(&mut s);
        assert_eq!(s, "TAG");
    }

    #[test]
    fn canonical_base_normalises_cases_and_unknowns() {
        assert_eq!(canonical_base(b'a'), b'A');
        assert_eq!(canonical_base(b'T'), b'T');
        assert_eq!(canonical_base(b'!'), b'N');
    }
}
