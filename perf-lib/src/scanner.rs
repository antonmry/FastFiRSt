use std::collections::{BTreeSet, HashMap, HashSet};
use std::io::{BufRead, Write};
use std::path::Path;

use anyhow::{Context, Result};

use crate::ScanConfig;
use crate::io::{create_writer, open_maybe_gzip, write_fasta_footer};
use crate::motif::{RepeatInfo, Strand};
use crate::stats::FastqStats;

/// Fast lookup table: true for A, C, G, T (upper-case only).
/// Sequences are uppercased at read time, so we only need upper-case checks.
static IS_ACGT: [bool; 256] = {
    let mut t = [false; 256];
    t[b'A' as usize] = true;
    t[b'C' as usize] = true;
    t[b'G' as usize] = true;
    t[b'T' as usize] = true;
    t
};

#[derive(Default)]
struct FastaTotals {
    sequences: usize,
    bases: u64,
    gc: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SsrMatch {
    pub seq_name: String,
    pub start: usize,
    pub end: usize,
    pub motif: String,
    pub length: usize,
    pub strand: Strand,
    pub num_units: usize,
    pub canonical: String,
}

/// Pre-computed index of repeat motifs for efficient scanning.
/// Wraps the HashMap and caches the sorted unique key lengths so they
/// are computed once instead of once per sequence.
pub struct RepeatIndex {
    pub repeats: HashMap<String, RepeatInfo>,
    /// Distinct key lengths, sorted descending (longest first).
    pub lengths: Vec<usize>,
}

impl RepeatIndex {
    pub fn new(repeats: HashMap<String, RepeatInfo>) -> Self {
        let lengths: Vec<usize> = repeats
            .keys()
            .map(|k| k.len())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .rev()
            .collect();
        Self { repeats, lengths }
    }
}

pub fn get_ssrs(seq: &[u8], index: &RepeatIndex, min_length: usize) -> Vec<SsrMatch> {
    if seq.len() < min_length || index.repeats.is_empty() {
        return Vec::new();
    }

    let lengths = &index.lengths;
    let repeats = &index.repeats;

    let mut raw_matches: Vec<SsrMatch> = Vec::new();
    let mut idx = 0usize;

    while idx + min_length <= seq.len() {
        for &key_len in lengths {
            if idx + key_len > seq.len() {
                continue;
            }

            let window = &seq[idx..idx + key_len];
            if !window.iter().all(|&b| IS_ACGT[b as usize]) {
                continue;
            }

            // SAFETY: we just verified every byte is A/C/G/T which are valid ASCII/UTF-8.
            let key = unsafe { std::str::from_utf8_unchecked(window) };

            let Some(info) = repeats.get(key) else {
                continue;
            };

            let motif = key[0..info.motif_len].to_string();
            let motif_bytes = motif.as_bytes();
            let mut end = idx;

            while end < seq.len() {
                let observed = seq[end];
                if !IS_ACGT[observed as usize] {
                    break;
                }
                let expected = motif_bytes[(end - idx) % motif_bytes.len()];
                if observed != expected {
                    break;
                }
                end += 1;
            }

            let length = end - idx;
            if length < min_length {
                continue;
            }

            raw_matches.push(SsrMatch {
                seq_name: String::new(),
                start: idx,
                end,
                motif,
                length,
                strand: info.strand,
                num_units: length / info.motif_len,
                canonical: info.canonical.clone(),
            });
        }

        idx += 1;
    }

    // Keep only the longest match per canonical class in each region.
    merge_overlapping(&mut raw_matches)
}

pub fn scan_fasta(
    input: &Path,
    output: &Path,
    index: &RepeatIndex,
    config: &ScanConfig,
) -> Result<()> {
    let mut reader = open_maybe_gzip(input)?;
    let mut writer = create_writer(output)?;

    let mut line = String::new();
    let mut current_name = String::new();
    let mut current_seq: Vec<u8> = Vec::new();

    let mut totals = FastaTotals::default();

    loop {
        line.clear();
        let read = reader.read_line(&mut line)?;
        if read == 0 {
            if !current_name.is_empty() {
                process_fasta_record(
                    &current_name,
                    &current_seq,
                    index,
                    config,
                    &mut writer,
                    &mut totals,
                )?;
            }
            break;
        }

        let trimmed = line.trim_end_matches(['\n', '\r']);
        if let Some(rest) = trimmed.strip_prefix('>') {
            if !current_name.is_empty() {
                process_fasta_record(
                    &current_name,
                    &current_seq,
                    index,
                    config,
                    &mut writer,
                    &mut totals,
                )?;
            }

            current_name = rest.trim().to_string();
            current_seq.clear();
        } else if !trimmed.is_empty() {
            current_seq.extend(trimmed.as_bytes().iter().map(|b| b.to_ascii_uppercase()));
        }
    }

    let gc_pct = if totals.bases > 0 {
        (totals.gc as f64 / totals.bases as f64) * 100.0
    } else {
        0.0
    };
    write_fasta_footer(
        &mut writer,
        &input.display().to_string(),
        totals.sequences,
        totals.bases,
        gc_pct,
    )?;
    writer.flush()?;

    Ok(())
}

pub fn scan_fastq(
    input: &Path,
    output: &Path,
    index: &RepeatIndex,
    config: &ScanConfig,
) -> Result<FastqStats> {
    let mut reader = open_maybe_gzip(input)?;
    let mut writer = create_writer(output)?;

    let mut stats = FastqStats::default();
    let mut tag = String::new();
    let mut seq = String::new();
    let mut plus = String::new();
    let mut qual = String::new();

    loop {
        tag.clear();
        if reader.read_line(&mut tag)? == 0 {
            break;
        }
        seq.clear();
        plus.clear();
        qual.clear();

        reader
            .read_line(&mut seq)
            .context("missing FASTQ sequence line")?;
        reader
            .read_line(&mut plus)
            .context("missing FASTQ plus line")?;
        reader
            .read_line(&mut qual)
            .context("missing FASTQ quality line")?;

        let tag = tag.trim_end_matches(['\n', '\r']);
        let seq = seq.trim_end_matches(['\n', '\r']).to_ascii_uppercase();

        if seq.len() < config.min_seq_len || seq.len() > config.max_seq_len {
            continue;
        }

        let mut matches = get_ssrs(seq.as_bytes(), index, config.min_repeat_length)
            .into_iter()
            .filter(|m| {
                m.canonical.len() >= config.min_motif_size
                    && m.canonical.len() <= config.max_motif_size
                    && m.num_units >= min_units_for(config, m.canonical.len())
            })
            .collect::<Vec<_>>();

        for m in &mut matches {
            m.seq_name = tag.to_string();
        }

        stats.observe_read(seq.len(), &matches);
    }

    stats.write_tsv(&mut writer)?;
    writer.flush()?;

    Ok(stats)
}

/// Parallel FASTA scanning using rayon + double-buffered reader thread.
/// Output order: batches are sequential; within each batch, records are
/// processed in parallel but collected in order.
#[cfg(feature = "parallel")]
pub fn scan_fasta_parallel(
    input: &Path,
    output: &Path,
    index: &RepeatIndex,
    config: &ScanConfig,
    num_threads: usize,
) -> Result<()> {
    use rayon::prelude::*;

    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(num_threads)
        .build()
        .map_err(|e| anyhow::anyhow!("failed to build rayon pool: {e}"))?;

    // --- reader thread ---
    let input_path = input.to_path_buf();
    let (tx, rx) = std::sync::mpsc::sync_channel::<Vec<(String, Vec<u8>)>>(1);

    let reader_handle = std::thread::spawn(move || -> Result<()> {
        let mut reader = open_maybe_gzip(&input_path)?;
        let mut line = String::new();
        let mut current_name = String::new();
        let mut current_seq: Vec<u8> = Vec::new();
        let mut batch: Vec<(String, Vec<u8>)> = Vec::with_capacity(256);

        loop {
            line.clear();
            let read = reader.read_line(&mut line)?;
            if read == 0 {
                if !current_name.is_empty() {
                    batch.push((
                        std::mem::take(&mut current_name),
                        std::mem::take(&mut current_seq),
                    ));
                }
                if !batch.is_empty() {
                    let _ = tx.send(batch);
                }
                break;
            }

            let trimmed = line.trim_end_matches(['\n', '\r']);
            if let Some(rest) = trimmed.strip_prefix('>') {
                if !current_name.is_empty() {
                    batch.push((
                        std::mem::take(&mut current_name),
                        std::mem::take(&mut current_seq),
                    ));
                    if batch.len() >= 256 {
                        if tx.send(batch).is_err() {
                            return Ok(());
                        }
                        batch = Vec::with_capacity(256);
                    }
                }
                current_name = rest.trim().to_string();
                current_seq.clear();
            } else if !trimmed.is_empty() {
                current_seq.extend(trimmed.as_bytes().iter().map(|b| b.to_ascii_uppercase()));
            }
        }

        Ok(())
    });

    // --- main thread: process batches ---
    let mut writer = create_writer(output)?;
    let mut totals = FastaTotals::default();

    for batch in rx {
        // Parallel process: compute SSR matches for each record.
        let results: Vec<_> = pool.install(|| {
            batch
                .into_par_iter()
                .map(|(name, seq)| {
                    let seq_len = seq.len() as u64;
                    let gc = seq.iter().filter(|&&b| matches!(b, b'G' | b'C')).count() as u64;

                    let matches =
                        if seq.len() >= config.min_seq_len && seq.len() <= config.max_seq_len {
                            let mut matches = get_ssrs(&seq, index, config.min_repeat_length)
                                .into_iter()
                                .filter(|m| {
                                    m.canonical.len() >= config.min_motif_size
                                        && m.canonical.len() <= config.max_motif_size
                                        && m.num_units >= min_units_for(config, m.canonical.len())
                                })
                                .collect::<Vec<_>>();
                            dedup_matches(&mut matches);
                            matches
                        } else {
                            Vec::new()
                        };

                    (name, seq_len, gc, matches)
                })
                .collect()
        });

        // Sequential write to preserve output order.
        for (name, seq_len, gc, matches) in results {
            totals.sequences += 1;
            totals.bases += seq_len;
            totals.gc += gc;

            for mut m in matches {
                m.seq_name.clone_from(&name);
                writeln!(
                    writer,
                    "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
                    m.seq_name,
                    m.start,
                    m.end,
                    m.canonical,
                    m.length,
                    m.strand.as_char(),
                    m.num_units,
                    m.motif
                )?;
            }
        }
    }

    reader_handle
        .join()
        .map_err(|_| anyhow::anyhow!("reader thread panicked"))??;

    let gc_pct = if totals.bases > 0 {
        (totals.gc as f64 / totals.bases as f64) * 100.0
    } else {
        0.0
    };
    write_fasta_footer(
        &mut writer,
        &input.display().to_string(),
        totals.sequences,
        totals.bases,
        gc_pct,
    )?;
    writer.flush()?;

    Ok(())
}

fn process_fasta_record(
    seq_name: &str,
    seq: &[u8],
    index: &RepeatIndex,
    config: &ScanConfig,
    writer: &mut impl Write,
    totals: &mut FastaTotals,
) -> Result<()> {
    totals.sequences += 1;
    totals.bases += seq.len() as u64;
    totals.gc += seq.iter().filter(|&&b| matches!(b, b'G' | b'C')).count() as u64;

    if seq.len() < config.min_seq_len || seq.len() > config.max_seq_len {
        return Ok(());
    }

    let mut matches = get_ssrs(seq, index, config.min_repeat_length)
        .into_iter()
        .filter(|m| {
            m.canonical.len() >= config.min_motif_size
                && m.canonical.len() <= config.max_motif_size
                && m.num_units >= min_units_for(config, m.canonical.len())
        })
        .collect::<Vec<_>>();

    dedup_matches(&mut matches);

    for mut m in matches {
        m.seq_name = seq_name.to_string();
        writeln!(
            writer,
            "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
            m.seq_name,
            m.start,
            m.end,
            m.canonical,
            m.length,
            m.strand.as_char(),
            m.num_units,
            m.motif
        )?;
    }

    Ok(())
}

/// For each canonical class, keep only the longest non-overlapping match.
/// Different canonical classes may overlap (matching Python PERF behaviour).
fn merge_overlapping(raw: &mut Vec<SsrMatch>) -> Vec<SsrMatch> {
    if raw.is_empty() {
        return Vec::new();
    }

    // Group by canonical class.
    let mut by_class: HashMap<String, Vec<SsrMatch>> = HashMap::new();
    for m in raw.drain(..) {
        by_class.entry(m.canonical.clone()).or_default().push(m);
    }

    let mut result = Vec::new();
    for (_canonical, mut group) in by_class {
        // Sort by start, then longest first.
        group.sort_by(|a, b| a.start.cmp(&b.start).then(b.length.cmp(&a.length)));

        let mut prev_end = 0usize;
        for m in group {
            if m.start >= prev_end {
                // Non-overlapping: keep it.
                prev_end = m.end;
                result.push(m);
            } else if m.end > prev_end {
                // Overlapping but extends further: keep both (matches Python PERF).
                prev_end = m.end;
                result.push(m);
            }
            // Otherwise: fully contained in previous match, skip.
        }
    }

    // Restore positional order.
    result.sort_by(|a, b| a.start.cmp(&b.start).then(b.length.cmp(&a.length)));
    result
}

#[inline]
fn dedup_matches(matches: &mut Vec<SsrMatch>) {
    let mut seen = HashSet::new();
    matches.retain(|m| {
        seen.insert((
            m.start,
            m.end,
            m.motif.clone(),
            m.canonical.clone(),
            m.strand,
        ))
    });
}

#[inline]
fn min_units_for(config: &ScanConfig, motif_len: usize) -> usize {
    config.min_units.get(&motif_len).copied().unwrap_or(1)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::MotifConfig;
    use crate::motif::build_rep_set;

    #[test]
    fn scanner_finds_simple_repeat() {
        let reps = build_rep_set(&MotifConfig::default()).unwrap();
        let index = RepeatIndex::new(reps);
        let seq = b"ACACACACACACGT";
        let matches = get_ssrs(seq, &index, 12);
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].canonical, "AC");
        assert_eq!(matches[0].length, 12);
    }
}
