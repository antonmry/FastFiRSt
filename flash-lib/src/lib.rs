use std::fs::{self, File};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail, ensure};
#[cfg(feature = "parallel")]
use rayon::prelude::*;

const IO_BUF_SIZE: usize = 1024 * 1024;
const IO_WRITE_BUF_SIZE: usize = 1024 * 1024;

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
pub struct FastqRecord {
    tag: String,
    seq: Vec<u8>,
    qual: Vec<u8>,
}

impl FastqRecord {
    pub fn from_strs(tag: &str, seq: &str, qual: &str, phred_offset: u8) -> Result<Self> {
        ensure!(
            seq.len() == qual.len(),
            "sequence and quality lengths differ"
        );

        let mut seq_bytes = Vec::with_capacity(seq.len());
        for &base in seq.as_bytes() {
            seq_bytes.push(canonical_base(base));
        }

        let mut qual_bytes = Vec::with_capacity(qual.len());
        for (idx, &byte) in qual.as_bytes().iter().enumerate() {
            if phred_offset > 0 && byte < phred_offset {
                bail!(
                    "quality char below PHRED offset ({}) at position {}",
                    phred_offset,
                    idx
                );
            }
            qual_bytes.push(byte.saturating_sub(phred_offset));
        }

        Ok(Self {
            tag: tag.to_string(),
            seq: seq_bytes,
            qual: qual_bytes,
        })
    }

    pub fn len(&self) -> usize {
        self.seq.len()
    }

    pub fn is_empty(&self) -> bool {
        self.seq.is_empty()
    }

    pub fn tag(&self) -> &str {
        &self.tag
    }

    pub fn set_tag<S: Into<String>>(&mut self, tag: S) {
        self.tag = tag.into();
    }

    pub fn seq_bytes(&self) -> &[u8] {
        &self.seq
    }

    pub fn qual_bytes(&self) -> &[u8] {
        &self.qual
    }

    pub fn seq_string(&self) -> String {
        String::from_utf8_lossy(&self.seq).into_owned()
    }

    pub fn qual_string(&self, phred_offset: u8) -> String {
        let mut buf = Vec::with_capacity(self.qual.len());
        for &q in &self.qual {
            buf.push(q + phred_offset);
        }
        String::from_utf8_lossy(&buf).into_owned()
    }
}

pub struct FastqPairReader {
    forward: BufReader<File>,
    reverse: BufReader<File>,
    forward_path: PathBuf,
    reverse_path: PathBuf,
    phred_offset: u8,
    // Reusable line buffers to avoid allocations per record
    line_buf: Vec<u8>,
    seq_buf: Vec<u8>,
    qual_buf: Vec<u8>,
}

impl FastqPairReader {
    pub fn from_paths(
        forward: impl AsRef<Path>,
        reverse: impl AsRef<Path>,
        phred_offset: u8,
    ) -> Result<Self> {
        let forward_path = forward.as_ref().to_path_buf();
        let reverse_path = reverse.as_ref().to_path_buf();
        let forward_file = File::open(&forward_path)
            .with_context(|| format!("failed to open forward FASTQ {:?}", forward_path))?;
        let reverse_file = File::open(&reverse_path)
            .with_context(|| format!("failed to open reverse FASTQ {:?}", reverse_path))?;

        #[cfg(target_os = "linux")]
        {
            use std::os::unix::io::AsRawFd;
            unsafe {
                libc::posix_fadvise(forward_file.as_raw_fd(), 0, 0, libc::POSIX_FADV_SEQUENTIAL);
                libc::posix_fadvise(reverse_file.as_raw_fd(), 0, 0, libc::POSIX_FADV_SEQUENTIAL);
            }
        }

        Ok(Self {
            forward: BufReader::with_capacity(IO_BUF_SIZE, forward_file),
            reverse: BufReader::with_capacity(IO_BUF_SIZE, reverse_file),
            forward_path,
            reverse_path,
            phred_offset,
            line_buf: Vec::with_capacity(1024),
            seq_buf: Vec::with_capacity(512),
            qual_buf: Vec::with_capacity(512),
        })
    }

    pub fn next_pair(&mut self) -> Result<Option<(FastqRecord, FastqRecord)>> {
        let read1 = read_fastq_record(
            &mut self.forward,
            &self.forward_path,
            self.phred_offset,
            &mut self.line_buf,
            &mut self.seq_buf,
            &mut self.qual_buf,
        )?;
        let read2 = read_fastq_record(
            &mut self.reverse,
            &self.reverse_path,
            self.phred_offset,
            &mut self.line_buf,
            &mut self.seq_buf,
            &mut self.qual_buf,
        )?;

        match (read1, read2) {
            (Some(r1), Some(r2)) => Ok(Some((r1, r2))),
            (None, None) => Ok(None),
            (Some(_), None) | (None, Some(_)) => {
                bail!("FASTQ inputs have different number of records")
            }
        }
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

#[derive(Debug, Clone)]
pub struct CombineOutcome {
    pub is_combined: bool,
    pub combined_tag: Option<String>,
    pub combined_seq: Option<String>,
    pub combined_qual: Option<String>,
}

pub const DEFAULT_BATCH_SIZE: usize = 4096;
pub const DEFAULT_NUM_THREADS: usize = 2;
#[cfg(feature = "parallel")]
const PARALLEL_PIPELINE_DEPTH: usize = 4;

#[derive(Debug, Clone)]
pub enum PairOutcome {
    Combined(FastqRecord),
    NotCombined(FastqRecord, FastqRecord),
}

#[cfg(feature = "parallel")]
enum ParallelPairDecision {
    Combined(FastqRecord),
    NotCombined,
}

#[cfg(feature = "parallel")]
struct RecordBatch {
    records: Vec<FastqRecord>,
    len: usize,
}

#[cfg(feature = "parallel")]
#[derive(Default)]
struct ReaderProfile {
    wait_batch_ns: u64,
    read_ns: u64,
    send_filled_ns: u64,
    batches: u64,
    records: u64,
}

#[cfg(feature = "parallel")]
#[derive(Default)]
struct WriterProfile {
    wait_result_ns: u64,
    write_ns: u64,
    return_batch_ns: u64,
    batches: u64,
    records: u64,
}

#[cfg(feature = "parallel")]
#[derive(Default)]
struct MainProfile {
    wait_filled_ns: u64,
    compute_ns: u64,
    send_result_ns: u64,
    batches: u64,
    records: u64,
}

#[cfg(feature = "parallel")]
#[inline]
fn elapsed_ns(start: std::time::Instant) -> u64 {
    start.elapsed().as_nanos().min(u64::MAX as u128) as u64
}

#[cfg(feature = "parallel")]
fn new_record_batch(batch_size: usize) -> RecordBatch {
    let mut records = Vec::with_capacity(batch_size);
    for _ in 0..batch_size {
        records.push(FastqRecord {
            tag: String::with_capacity(64),
            seq: Vec::with_capacity(256),
            qual: Vec::with_capacity(256),
        });
    }
    RecordBatch { records, len: 0 }
}

pub fn combine_pair_from_strs(
    tag1: &str,
    seq1: &str,
    qual1: &str,
    tag2: &str,
    seq2: &str,
    qual2: &str,
    params: &CombineParams,
) -> Result<CombineOutcome> {
    let read1 = FastqRecord::from_strs(tag1, seq1, qual1, params.phred_offset)?;
    let read2 = FastqRecord::from_strs(tag2, seq2, qual2, params.phred_offset)?;

    let mut read2_rev = FastqRecord {
        tag: String::new(),
        seq: Vec::with_capacity(read2.seq.len()),
        qual: Vec::with_capacity(read2.qual.len()),
    };
    reverse_complement_into(&read2, &mut read2_rev);

    if let Some(mut combined_record) = combine_pair(&read1, &read2_rev, params) {
        let combined_tag = combined_tag(&read1);
        combined_record.set_tag(combined_tag.clone());
        Ok(CombineOutcome {
            is_combined: true,
            combined_tag: Some(combined_tag),
            combined_seq: Some(combined_record.seq_string()),
            combined_qual: Some(combined_record.qual_string(params.phred_offset)),
        })
    } else {
        Ok(CombineOutcome {
            is_combined: false,
            combined_tag: None,
            combined_seq: None,
            combined_qual: None,
        })
    }
}

pub fn compute_pair(
    read1: &FastqRecord,
    read2: &FastqRecord,
    params: &CombineParams,
) -> PairOutcome {
    let mut read2_rev = FastqRecord {
        tag: String::new(),
        seq: Vec::with_capacity(read2.seq.len()),
        qual: Vec::with_capacity(read2.qual.len()),
    };
    reverse_complement_into(read2, &mut read2_rev);

    if let Some(mut combined) = combine_pair(read1, &read2_rev, params) {
        combined.tag = combined_tag(read1);
        PairOutcome::Combined(combined)
    } else {
        PairOutcome::NotCombined(read1.clone(), read2.clone())
    }
}

#[cfg(feature = "parallel")]
fn compute_pair_decision_with_scratch(
    read1: &FastqRecord,
    read2: &FastqRecord,
    params: &CombineParams,
    scratch: &mut FastqRecord,
) -> ParallelPairDecision {
    reverse_complement_into(read2, scratch);

    if let Some(combined) = combine_pair(read1, scratch, params) {
        ParallelPairDecision::Combined(combined)
    } else {
        ParallelPairDecision::NotCombined
    }
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

    let mut pair_reader = FastqPairReader::from_paths(forward, reverse, params.phred_offset)?;

    let ext_path = output_dir.join(format!("{}.extendedFrags.fastq", output_prefix));
    let not1_path = output_dir.join(format!("{}.notCombined_1.fastq", output_prefix));
    let not2_path = output_dir.join(format!("{}.notCombined_2.fastq", output_prefix));

    let mut out_extended = BufWriter::with_capacity(
        IO_WRITE_BUF_SIZE,
        File::create(&ext_path).with_context(|| format!("failed to create {:?}", ext_path))?,
    );
    let mut out_not1 = BufWriter::with_capacity(
        IO_WRITE_BUF_SIZE,
        File::create(&not1_path).with_context(|| format!("failed to create {:?}", not1_path))?,
    );
    let mut out_not2 = BufWriter::with_capacity(
        IO_WRITE_BUF_SIZE,
        File::create(&not2_path).with_context(|| format!("failed to create {:?}", not2_path))?,
    );

    let mut scratch = FastqRecord {
        tag: String::new(),
        seq: Vec::new(),
        qual: Vec::new(),
    };
    let mut record_buf = Vec::new();

    while let Some((r1, r2)) = pair_reader.next_pair()? {
        process_pair_with_scratch(
            &r1,
            &r2,
            params,
            &mut out_extended,
            &mut out_not1,
            &mut out_not2,
            &mut scratch,
            &mut record_buf,
        )?;
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

#[cfg(feature = "parallel")]
pub fn merge_fastq_files_parallel(
    forward: impl AsRef<Path>,
    reverse: impl AsRef<Path>,
    output_dir: impl AsRef<Path>,
    output_prefix: &str,
    params: &CombineParams,
    batch_size: usize,
    num_threads: usize,
) -> Result<()> {
    params.validate()?;
    ensure!(batch_size > 0, "batch-size must be >= 1");
    ensure!(num_threads > 0, "num-threads must be >= 1");
    let profile_enabled = std::env::var_os("FLASH_PROFILE_PARALLEL").is_some();

    let forward = forward.as_ref();
    let reverse = reverse.as_ref();
    let output_dir = output_dir.as_ref();

    fs::create_dir_all(output_dir)
        .with_context(|| format!("failed to create output directory {:?}", output_dir))?;

    let forward_path = forward.to_path_buf();
    let reverse_path = reverse.to_path_buf();
    let phred_offset = params.phred_offset;

    let ext_path = output_dir.join(format!("{}.extendedFrags.fastq", output_prefix));
    let not1_path = output_dir.join(format!("{}.notCombined_1.fastq", output_prefix));
    let not2_path = output_dir.join(format!("{}.notCombined_2.fastq", output_prefix));

    let out_extended = BufWriter::with_capacity(
        IO_WRITE_BUF_SIZE,
        File::create(&ext_path).with_context(|| format!("failed to create {:?}", ext_path))?,
    );
    let out_not1 = BufWriter::with_capacity(
        IO_WRITE_BUF_SIZE,
        File::create(&not1_path).with_context(|| format!("failed to create {:?}", not1_path))?,
    );
    let out_not2 = BufWriter::with_capacity(
        IO_WRITE_BUF_SIZE,
        File::create(&not2_path).with_context(|| format!("failed to create {:?}", not2_path))?,
    );

    // Build a custom Rayon pool with limited threads to reduce synchronization overhead.
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(num_threads)
        .build()
        .context("failed to build Rayon thread pool")?;

    // Reader threads: parse each FASTQ in parallel and stream reusable batches.
    let (r1_filled_tx, r1_filled_rx) =
        std::sync::mpsc::sync_channel::<RecordBatch>(PARALLEL_PIPELINE_DEPTH);
    let (r2_filled_tx, r2_filled_rx) =
        std::sync::mpsc::sync_channel::<RecordBatch>(PARALLEL_PIPELINE_DEPTH);
    let (r1_free_tx, r1_free_rx) =
        std::sync::mpsc::sync_channel::<RecordBatch>(PARALLEL_PIPELINE_DEPTH);
    let (r2_free_tx, r2_free_rx) =
        std::sync::mpsc::sync_channel::<RecordBatch>(PARALLEL_PIPELINE_DEPTH);

    for _ in 0..PARALLEL_PIPELINE_DEPTH {
        // Seed batch pools for each reader.
        r1_free_tx
            .send(new_record_batch(batch_size))
            .context("failed to initialize reader 1 batch pool")?;
        r2_free_tx
            .send(new_record_batch(batch_size))
            .context("failed to initialize reader 2 batch pool")?;
    }

    let spawn_reader = |path: PathBuf,
                        filled_tx: std::sync::mpsc::SyncSender<RecordBatch>,
                        free_rx: std::sync::mpsc::Receiver<RecordBatch>| {
        std::thread::spawn(move || -> Result<ReaderProfile> {
            let file =
                File::open(&path).with_context(|| format!("failed to open FASTQ {:?}", path))?;
            #[cfg(target_os = "linux")]
            {
                use std::os::unix::io::AsRawFd;
                unsafe {
                    libc::posix_fadvise(file.as_raw_fd(), 0, 0, libc::POSIX_FADV_SEQUENTIAL);
                }
            }

            let mut reader = BufReader::with_capacity(IO_BUF_SIZE, file);
            let mut line_buf = Vec::with_capacity(1024);
            let mut profile = ReaderProfile::default();

            loop {
                let wait_start = profile_enabled.then(std::time::Instant::now);
                let mut batch = match free_rx.recv() {
                    Ok(batch) => batch,
                    Err(_) => break,
                };
                if let Some(start) = wait_start {
                    profile.wait_batch_ns += elapsed_ns(start);
                }

                let read_start = profile_enabled.then(std::time::Instant::now);
                let mut filled = 0usize;
                for record in batch.records.iter_mut() {
                    if read_fastq_record_into(
                        &mut reader,
                        &path,
                        phred_offset,
                        &mut line_buf,
                        record,
                    )? {
                        filled += 1;
                    } else {
                        break;
                    }
                }
                if let Some(start) = read_start {
                    profile.read_ns += elapsed_ns(start);
                }

                if filled == 0 {
                    break;
                }

                let reached_eof = filled < batch.records.len();
                batch.len = filled;
                profile.batches += 1;
                profile.records += filled as u64;

                let send_start = profile_enabled.then(std::time::Instant::now);
                if filled_tx.send(batch).is_err() {
                    break;
                }
                if let Some(start) = send_start {
                    profile.send_filled_ns += elapsed_ns(start);
                }
                if reached_eof {
                    break;
                }
            }

            Ok(profile)
        })
    };
    let reader1_handle = spawn_reader(forward_path, r1_filled_tx, r1_free_rx);
    let reader2_handle = spawn_reader(reverse_path, r2_filled_tx, r2_free_rx);

    // Writer thread: write current batch while main thread computes next batch.
    let (result_tx, result_rx) =
        std::sync::mpsc::sync_channel::<(RecordBatch, RecordBatch, Vec<ParallelPairDecision>)>(2);
    let phred_offset = params.phred_offset;

    let writer_handle = std::thread::spawn(move || -> Result<WriterProfile> {
        let mut out_extended = out_extended;
        let mut out_not1 = out_not1;
        let mut out_not2 = out_not2;
        let mut record_buf = Vec::new();
        let mut profile = WriterProfile::default();

        loop {
            let wait_start = profile_enabled.then(std::time::Instant::now);
            let (mut batch1, mut batch2, decisions) = match result_rx.recv() {
                Ok(item) => item,
                Err(_) => break,
            };
            if let Some(start) = wait_start {
                profile.wait_result_ns += elapsed_ns(start);
            }

            let write_start = profile_enabled.then(std::time::Instant::now);
            for (idx, decision) in decisions.into_iter().enumerate() {
                let read1 = &batch1.records[idx];
                let read2 = &batch2.records[idx];
                match decision {
                    ParallelPairDecision::Combined(combined) => {
                        write_combined_fastq(
                            &mut out_extended,
                            read1.tag(),
                            &combined,
                            phred_offset,
                            &mut record_buf,
                        )?;
                    }
                    ParallelPairDecision::NotCombined => {
                        write_fastq(&mut out_not1, read1, phred_offset, &mut record_buf)?;
                        write_fastq(&mut out_not2, read2, phred_offset, &mut record_buf)?;
                    }
                }
            }
            if let Some(start) = write_start {
                profile.write_ns += elapsed_ns(start);
            }
            profile.batches += 1;
            profile.records += batch1.len as u64;

            batch1.len = 0;
            batch2.len = 0;
            let return_start = profile_enabled.then(std::time::Instant::now);
            // Readers may already have exited after reaching EOF; returning batches is best-effort.
            let _ = r1_free_tx.send(batch1);
            let _ = r2_free_tx.send(batch2);
            if let Some(start) = return_start {
                profile.return_batch_ns += elapsed_ns(start);
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
        Ok(profile)
    });

    // Main thread: pair same-index records from both readers, compute decisions in parallel,
    // then hand off to writer thread.
    let mut pairing_error: Option<anyhow::Error> = None;
    let mut main_profile = MainProfile::default();
    loop {
        let wait_start = profile_enabled.then(std::time::Instant::now);
        let batch1 = r1_filled_rx.recv();
        let batch2 = r2_filled_rx.recv();
        if let Some(start) = wait_start {
            main_profile.wait_filled_ns += elapsed_ns(start);
        }
        match (batch1, batch2) {
            (Ok(batch1), Ok(batch2)) => {
                if batch1.len != batch2.len {
                    pairing_error = Some(anyhow::anyhow!(
                        "FASTQ inputs have different number of records"
                    ));
                    break;
                }

                let len = batch1.len;
                let compute_start = profile_enabled.then(std::time::Instant::now);
                let decisions: Vec<ParallelPairDecision> = pool.install(|| {
                    batch1.records[..len]
                        .par_iter()
                        .zip(batch2.records[..len].par_iter())
                        .map_init(
                            || FastqRecord {
                                tag: String::new(),
                                seq: Vec::new(),
                                qual: Vec::new(),
                            },
                            |scratch, (read1, read2)| {
                                compute_pair_decision_with_scratch(read1, read2, params, scratch)
                            },
                        )
                        .collect()
                });
                if let Some(start) = compute_start {
                    main_profile.compute_ns += elapsed_ns(start);
                }
                main_profile.batches += 1;
                main_profile.records += len as u64;

                let send_start = profile_enabled.then(std::time::Instant::now);
                if result_tx.send((batch1, batch2, decisions)).is_err() {
                    break;
                }
                if let Some(start) = send_start {
                    main_profile.send_result_ns += elapsed_ns(start);
                }
            }
            (Err(_), Err(_)) => break,
            (Ok(_), Err(_)) | (Err(_), Ok(_)) => {
                pairing_error = Some(anyhow::anyhow!(
                    "FASTQ inputs have different number of records"
                ));
                break;
            }
        }
    }
    drop(result_tx);
    drop(r1_filled_rx);
    drop(r2_filled_rx);

    let reader1_result = reader1_handle
        .join()
        .map_err(|_| anyhow::anyhow!("reader_1 thread panicked"))?;
    let reader2_result = reader2_handle
        .join()
        .map_err(|_| anyhow::anyhow!("reader_2 thread panicked"))?;
    let writer_result = writer_handle
        .join()
        .map_err(|_| anyhow::anyhow!("writer thread panicked"))?;

    let reader1_profile = reader1_result?;
    let reader2_profile = reader2_result?;
    let writer_profile = writer_result?;
    if let Some(err) = pairing_error {
        return Err(err);
    }

    if profile_enabled {
        let to_ms = |ns: u64| ns as f64 / 1_000_000.0;
        eprintln!(
            concat!(
                "[flash profile] batches={} records={}\n",
                "  main:   wait_filled={:.3}ms compute={:.3}ms send_result={:.3}ms\n",
                "  read1:  wait_batch={:.3}ms read={:.3}ms send_filled={:.3}ms\n",
                "  read2:  wait_batch={:.3}ms read={:.3}ms send_filled={:.3}ms\n",
                "  writer: wait_result={:.3}ms write={:.3}ms return_batch={:.3}ms"
            ),
            main_profile.batches,
            main_profile.records,
            to_ms(main_profile.wait_filled_ns),
            to_ms(main_profile.compute_ns),
            to_ms(main_profile.send_result_ns),
            to_ms(reader1_profile.wait_batch_ns),
            to_ms(reader1_profile.read_ns),
            to_ms(reader1_profile.send_filled_ns),
            to_ms(reader2_profile.wait_batch_ns),
            to_ms(reader2_profile.read_ns),
            to_ms(reader2_profile.send_filled_ns),
            to_ms(writer_profile.wait_result_ns),
            to_ms(writer_profile.write_ns),
            to_ms(writer_profile.return_batch_ns),
        );
    }

    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn process_pair_with_scratch<W: Write>(
    read1: &FastqRecord,
    read2: &FastqRecord,
    params: &CombineParams,
    out_extended: &mut W,
    out_not1: &mut W,
    out_not2: &mut W,
    scratch: &mut FastqRecord,
    record_buf: &mut Vec<u8>,
) -> Result<()> {
    reverse_complement_into(read2, scratch);

    if let Some(combined) = combine_pair(read1, scratch, params) {
        write_combined_fastq(
            out_extended,
            read1.tag(),
            &combined,
            params.phred_offset,
            record_buf,
        )?;
    } else {
        write_fastq(out_not1, read1, params.phred_offset, record_buf)?;
        write_fastq(out_not2, read2, params.phred_offset, record_buf)?;
    }

    Ok(())
}

/// Read one line into `buf` (cleared first), stripping trailing \n and \r.
/// Returns Ok(0) on EOF.
#[inline]
fn read_line_bytes<R: BufRead>(reader: &mut R, buf: &mut Vec<u8>) -> std::io::Result<usize> {
    buf.clear();
    let n = reader.read_until(b'\n', buf)?;
    // Strip trailing \n and \r
    while matches!(buf.last(), Some(b'\n') | Some(b'\r')) {
        buf.pop();
    }
    Ok(n)
}

fn read_fastq_record<R: BufRead>(
    reader: &mut R,
    source: &Path,
    phred_offset: u8,
    line_buf: &mut Vec<u8>,
    seq_buf: &mut Vec<u8>,
    qual_buf: &mut Vec<u8>,
) -> Result<Option<FastqRecord>> {
    // Read tag line
    if read_line_bytes(reader, line_buf)? == 0 {
        return Ok(None);
    }
    ensure!(
        !line_buf.is_empty() && line_buf[0] == b'@',
        "invalid FASTQ tag line in {:?}",
        source,
    );
    // SAFETY: FASTQ tags are ASCII; we accept lossy conversion for non-ASCII edge cases
    let tag = String::from_utf8_lossy(line_buf).into_owned();

    // Read sequence line into reusable buffer
    if read_line_bytes(reader, seq_buf)? == 0 {
        bail!("unexpected EOF reading sequence in {:?}", source);
    }

    // Read '+' separator
    if read_line_bytes(reader, line_buf)? == 0 {
        bail!("unexpected EOF reading '+' separator in {:?}", source);
    }
    ensure!(
        !line_buf.is_empty() && line_buf[0] == b'+',
        "invalid FASTQ '+' separator in {:?}",
        source,
    );

    // Read quality line into reusable buffer
    if read_line_bytes(reader, qual_buf)? == 0 {
        bail!("unexpected EOF reading quality in {:?}", source);
    }

    ensure!(
        seq_buf.len() == qual_buf.len(),
        "sequence and quality lengths differ in {:?}: {} vs {}",
        source,
        seq_buf.len(),
        qual_buf.len()
    );

    // Canonicalize sequence in-place
    for b in seq_buf.iter_mut() {
        *b = canonical_base(*b);
    }

    // Convert quality in-place.
    if phred_offset == 0 {
        // No conversion required.
    } else {
        for (idx, q) in qual_buf.iter_mut().enumerate() {
            if *q < phred_offset {
                bail!(
                    "quality char below PHRED offset ({}) at position {} in {:?}",
                    phred_offset,
                    idx,
                    source
                );
            }
            *q -= phred_offset;
        }
    }

    // Take ownership by swapping with empty vecs, avoiding clone
    let seq = std::mem::take(seq_buf);
    let qual = std::mem::take(qual_buf);

    Ok(Some(FastqRecord { tag, seq, qual }))
}

#[cfg(feature = "parallel")]
fn read_fastq_record_into<R: BufRead>(
    reader: &mut R,
    source: &Path,
    phred_offset: u8,
    line_buf: &mut Vec<u8>,
    out: &mut FastqRecord,
) -> Result<bool> {
    if read_line_bytes(reader, line_buf)? == 0 {
        return Ok(false);
    }
    ensure!(
        !line_buf.is_empty() && line_buf[0] == b'@',
        "invalid FASTQ tag line in {:?}",
        source,
    );
    out.tag.clear();
    out.tag.push_str(&String::from_utf8_lossy(line_buf));

    if read_line_bytes(reader, &mut out.seq)? == 0 {
        bail!("unexpected EOF reading sequence in {:?}", source);
    }

    if read_line_bytes(reader, line_buf)? == 0 {
        bail!("unexpected EOF reading '+' separator in {:?}", source);
    }
    ensure!(
        !line_buf.is_empty() && line_buf[0] == b'+',
        "invalid FASTQ '+' separator in {:?}",
        source,
    );

    if read_line_bytes(reader, &mut out.qual)? == 0 {
        bail!("unexpected EOF reading quality in {:?}", source);
    }

    ensure!(
        out.seq.len() == out.qual.len(),
        "sequence and quality lengths differ in {:?}: {} vs {}",
        source,
        out.seq.len(),
        out.qual.len()
    );

    for b in out.seq.iter_mut() {
        *b = canonical_base(*b);
    }

    if phred_offset != 0 {
        for (idx, q) in out.qual.iter_mut().enumerate() {
            if *q < phred_offset {
                bail!(
                    "quality char below PHRED offset ({}) at position {} in {:?}",
                    phred_offset,
                    idx,
                    source
                );
            }
            *q -= phred_offset;
        }
    }
    Ok(true)
}

/// Lookup table: maps any byte to its canonical uppercase base (A/C/G/T/N).
/// All unrecognised bytes map to N.
static CANONICAL_BASE_LUT: [u8; 256] = {
    let mut lut = [b'N'; 256];
    lut[b'A' as usize] = b'A';
    lut[b'a' as usize] = b'A';
    lut[b'C' as usize] = b'C';
    lut[b'c' as usize] = b'C';
    lut[b'G' as usize] = b'G';
    lut[b'g' as usize] = b'G';
    lut[b'T' as usize] = b'T';
    lut[b't' as usize] = b'T';
    lut[b'N' as usize] = b'N';
    lut[b'n' as usize] = b'N';
    lut
};

/// Lookup table: maps a base to its complement (A<->T, C<->G, else N).
static COMPLEMENT_LUT: [u8; 256] = {
    let mut lut = [b'N'; 256];
    lut[b'A' as usize] = b'T';
    lut[b'T' as usize] = b'A';
    lut[b'C' as usize] = b'G';
    lut[b'G' as usize] = b'C';
    lut
};

#[inline(always)]
fn canonical_base(b: u8) -> u8 {
    CANONICAL_BASE_LUT[b as usize]
}

#[inline(always)]
fn complement(base: u8) -> u8 {
    COMPLEMENT_LUT[base as usize]
}

fn to_lower(base: u8) -> u8 {
    if base.is_ascii_uppercase() {
        base + 32
    } else {
        base
    }
}

/// Write the reverse complement of `src` into `dst`, reusing dst's allocations.
/// The tag is intentionally NOT copied — callers always overwrite it.
fn reverse_complement_into(src: &FastqRecord, dst: &mut FastqRecord) {
    let len = src.seq.len();
    dst.seq.clear();
    dst.seq.reserve(len);
    dst.qual.clear();
    dst.qual.reserve(len);
    for i in (0..len).rev() {
        dst.seq.push(complement(src.seq[i]));
        dst.qual.push(src.qual[i]);
    }
}

fn combine_pair(
    read1: &FastqRecord,
    read2_rev: &FastqRecord,
    params: &CombineParams,
) -> Option<FastqRecord> {
    let (candidate, status) = best_alignment(read1, read2_rev, params)?;
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

fn best_alignment(
    read1: &FastqRecord,
    read2_rev: &FastqRecord,
    params: &CombineParams,
) -> Option<(AlignmentCandidate, CombineStatus)> {
    let mut best: Option<(AlignmentCandidate, CombineStatus)> =
        evaluate_alignment(read1, read2_rev, params)
            .map(|candidate| (candidate, CombineStatus::CombinedInnie));

    if params.allow_outies
        && let Some(candidate) = evaluate_alignment(read2_rev, read1, params)
    {
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

    best
}

fn evaluate_alignment(
    first: &FastqRecord,
    second: &FastqRecord,
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

        let score_len = overlap_len.min(params.max_overlap).max(1);
        let stats = if have_n {
            compute_mismatch_stats(
                &first.seq[i..first.len()],
                &second.seq[..overlap_len],
                &first.qual[i..first.len()],
                &second.qual[..overlap_len],
                true,
            )
        } else {
            // Early-reject impossible candidates: once mismatch count exceeds the
            // tighter of (current best density) and configured max density, this
            // alignment cannot win and we stop scanning it.
            let density_cap = best_density.min(params.max_mismatch_density);
            let max_mismatches = (density_cap * score_len as f32).ceil() as u32;
            let stats = compute_mismatch_stats_no_n_limited(
                &first.seq[i..first.len()],
                &second.seq[..overlap_len],
                &first.qual[i..first.len()],
                &second.qual[..overlap_len],
                max_mismatches,
            );
            if stats.mismatches > max_mismatches {
                continue;
            }
            stats
        };

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

/// SSE2 fast path: process 16 bytes at a time using SIMD intrinsics.
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "sse2")]
unsafe fn compute_mismatch_stats_no_n_sse2(
    seq1: &[u8],
    seq2: &[u8],
    qual1: &[u8],
    qual2: &[u8],
) -> MismatchStats {
    use std::arch::x86_64::*;

    let len = seq1.len();
    let mut mismatches: u32 = 0;
    let mut mismatch_qual_total: u32 = 0;

    let chunks = len / 16;
    let remainder = len % 16;

    for chunk in 0..chunks {
        let base = chunk * 16;
        let s1 = _mm_loadu_si128(seq1.as_ptr().add(base) as *const __m128i);
        let s2 = _mm_loadu_si128(seq2.as_ptr().add(base) as *const __m128i);

        // Compare: 0xFF where equal, 0x00 where different
        let eq = _mm_cmpeq_epi8(s1, s2);
        // Bitmask: bit set where bytes are EQUAL
        let eq_mask = _mm_movemask_epi8(eq) as u32;
        // Invert: bit set where bytes are DIFFERENT
        let diff_mask = (!eq_mask) & 0xFFFF;
        let chunk_mismatches = diff_mask.count_ones();
        mismatches += chunk_mismatches;

        if chunk_mismatches > 0 {
            let q1 = _mm_loadu_si128(qual1.as_ptr().add(base) as *const __m128i);
            let q2 = _mm_loadu_si128(qual2.as_ptr().add(base) as *const __m128i);
            // min(q1, q2) for unsigned bytes
            let qmin = _mm_min_epu8(q1, q2);
            // Zero out positions where bases matched (keep only mismatches)
            let qmin_masked = _mm_andnot_si128(eq, qmin);

            // Horizontal sum of the 16 bytes in qmin_masked.
            // SAD (sum of absolute differences) against zero gives us
            // two 16-bit partial sums in a 128-bit register.
            let zero = _mm_setzero_si128();
            let sad = _mm_sad_epu8(qmin_masked, zero);
            // Extract the two 64-bit lanes which each contain a 16-bit sum
            let lo = _mm_extract_epi16::<0>(sad) as u32;
            let hi = _mm_extract_epi16::<4>(sad) as u32;
            mismatch_qual_total += lo + hi;
        }
    }

    // Scalar remainder
    let tail = chunks * 16;
    for j in 0..remainder {
        let i = tail + j;
        let s1 = *seq1.get_unchecked(i);
        let s2 = *seq2.get_unchecked(i);
        if s1 != s2 {
            mismatches += 1;
            let q1 = *qual1.get_unchecked(i);
            let q2 = *qual2.get_unchecked(i);
            mismatch_qual_total += q1.min(q2) as u32;
        }
    }

    MismatchStats {
        effective_len: len,
        mismatches,
        mismatch_qual_total,
    }
}

/// NEON fast path for aarch64: process 16 bytes at a time.
#[cfg(target_arch = "aarch64")]
#[target_feature(enable = "neon")]
unsafe fn compute_mismatch_stats_no_n_neon(
    seq1: &[u8],
    seq2: &[u8],
    qual1: &[u8],
    qual2: &[u8],
) -> MismatchStats {
    use std::arch::aarch64::*;

    let len = seq1.len();
    let mut mismatches: u32 = 0;
    let mut mismatch_qual_total: u32 = 0;

    let chunks = len / 16;
    let remainder = len % 16;

    for chunk in 0..chunks {
        let base = chunk * 16;
        // SAFETY: base + 16 <= len is guaranteed by chunks calculation
        unsafe {
            let s1 = vld1q_u8(seq1.as_ptr().add(base));
            let s2 = vld1q_u8(seq2.as_ptr().add(base));

            // Compare: 0xFF where equal, 0x00 where different
            let eq = vceqq_u8(s1, s2);
            // Invert: 0xFF where different
            let neq = vmvnq_u8(eq);

            // Count mismatches via popcount: each 0xFF byte has 8 set bits
            let bit_counts = vcntq_u8(neq);
            let sum16 = vpaddlq_u8(bit_counts);
            let sum32 = vpaddlq_u16(sum16);
            let sum64 = vpaddlq_u32(sum32);
            let total_bits = vgetq_lane_u64(sum64, 0) + vgetq_lane_u64(sum64, 1);
            let chunk_mismatches = (total_bits / 8) as u32;
            mismatches += chunk_mismatches;

            if chunk_mismatches > 0 {
                let q1 = vld1q_u8(qual1.as_ptr().add(base));
                let q2 = vld1q_u8(qual2.as_ptr().add(base));
                let qmin = vminq_u8(q1, q2);
                // Zero out matched positions, keep only mismatch qualities
                let qmin_masked = vandq_u8(neq, qmin);

                // Horizontal sum of masked quality values
                let qsum16 = vpaddlq_u8(qmin_masked);
                let qsum32 = vpaddlq_u16(qsum16);
                let qsum64 = vpaddlq_u32(qsum32);
                let qual_sum = vgetq_lane_u64(qsum64, 0) + vgetq_lane_u64(qsum64, 1);
                mismatch_qual_total += qual_sum as u32;
            }
        }
    }

    // Scalar remainder
    let tail = chunks * 16;
    for j in 0..remainder {
        let i = tail + j;
        // SAFETY: i < len is guaranteed by remainder calculation
        unsafe {
            let s1 = *seq1.get_unchecked(i);
            let s2 = *seq2.get_unchecked(i);
            if s1 != s2 {
                mismatches += 1;
                let q1 = *qual1.get_unchecked(i);
                let q2 = *qual2.get_unchecked(i);
                mismatch_qual_total += q1.min(q2) as u32;
            }
        }
    }

    MismatchStats {
        effective_len: len,
        mismatches,
        mismatch_qual_total,
    }
}

/// Scalar fallback.
#[inline(always)]
fn compute_mismatch_stats_no_n_scalar(
    seq1: &[u8],
    seq2: &[u8],
    qual1: &[u8],
    qual2: &[u8],
) -> MismatchStats {
    let len = seq1.len();
    let mut mismatches: u32 = 0;
    let mut mismatch_qual_total: u32 = 0;

    for i in 0..len {
        let s1 = unsafe { *seq1.get_unchecked(i) };
        let s2 = unsafe { *seq2.get_unchecked(i) };
        if s1 != s2 {
            mismatches += 1;
            let q1 = unsafe { *qual1.get_unchecked(i) };
            let q2 = unsafe { *qual2.get_unchecked(i) };
            mismatch_qual_total += q1.min(q2) as u32;
        }
    }

    MismatchStats {
        effective_len: len,
        mismatches,
        mismatch_qual_total,
    }
}

#[inline(always)]
fn compute_mismatch_stats_no_n(
    seq1: &[u8],
    seq2: &[u8],
    qual1: &[u8],
    qual2: &[u8],
) -> MismatchStats {
    #[cfg(target_arch = "x86_64")]
    {
        if is_x86_feature_detected!("sse2") {
            // SAFETY: we just checked that SSE2 is available
            return unsafe { compute_mismatch_stats_no_n_sse2(seq1, seq2, qual1, qual2) };
        }
    }
    #[cfg(target_arch = "aarch64")]
    {
        if std::arch::is_aarch64_feature_detected!("neon") {
            // SAFETY: we just checked that NEON is available
            return unsafe { compute_mismatch_stats_no_n_neon(seq1, seq2, qual1, qual2) };
        }
    }
    compute_mismatch_stats_no_n_scalar(seq1, seq2, qual1, qual2)
}

/// Same as `compute_mismatch_stats_no_n`, but stops as soon as mismatches
/// exceed `max_mismatches`.
#[inline(always)]
fn compute_mismatch_stats_no_n_limited(
    seq1: &[u8],
    seq2: &[u8],
    qual1: &[u8],
    qual2: &[u8],
    max_mismatches: u32,
) -> MismatchStats {
    #[cfg(target_arch = "x86_64")]
    {
        if is_x86_feature_detected!("sse2") {
            // SAFETY: we just checked that SSE2 is available
            return unsafe {
                compute_mismatch_stats_no_n_sse2_limited(seq1, seq2, qual1, qual2, max_mismatches)
            };
        }
    }
    #[cfg(target_arch = "aarch64")]
    {
        if std::arch::is_aarch64_feature_detected!("neon") {
            // SAFETY: we just checked that NEON is available
            return unsafe {
                compute_mismatch_stats_no_n_neon_limited(seq1, seq2, qual1, qual2, max_mismatches)
            };
        }
    }
    compute_mismatch_stats_no_n_scalar_limited(seq1, seq2, qual1, qual2, max_mismatches)
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "sse2")]
unsafe fn compute_mismatch_stats_no_n_sse2_limited(
    seq1: &[u8],
    seq2: &[u8],
    qual1: &[u8],
    qual2: &[u8],
    max_mismatches: u32,
) -> MismatchStats {
    use std::arch::x86_64::*;

    let len = seq1.len();
    let mut mismatches: u32 = 0;
    let mut mismatch_qual_total: u32 = 0;

    let chunks = len / 16;
    let remainder = len % 16;

    for chunk in 0..chunks {
        let base = chunk * 16;
        let s1 = _mm_loadu_si128(seq1.as_ptr().add(base) as *const __m128i);
        let s2 = _mm_loadu_si128(seq2.as_ptr().add(base) as *const __m128i);
        let eq = _mm_cmpeq_epi8(s1, s2);
        let eq_mask = _mm_movemask_epi8(eq) as u32;
        let diff_mask = (!eq_mask) & 0xFFFF;
        let chunk_mismatches = diff_mask.count_ones();
        mismatches += chunk_mismatches;
        if mismatches > max_mismatches {
            return MismatchStats {
                effective_len: len,
                mismatches,
                mismatch_qual_total,
            };
        }

        if chunk_mismatches > 0 {
            let q1 = _mm_loadu_si128(qual1.as_ptr().add(base) as *const __m128i);
            let q2 = _mm_loadu_si128(qual2.as_ptr().add(base) as *const __m128i);
            let qmin = _mm_min_epu8(q1, q2);
            let qmin_masked = _mm_andnot_si128(eq, qmin);
            let zero = _mm_setzero_si128();
            let sad = _mm_sad_epu8(qmin_masked, zero);
            let lo = _mm_extract_epi16::<0>(sad) as u32;
            let hi = _mm_extract_epi16::<4>(sad) as u32;
            mismatch_qual_total += lo + hi;
        }
    }

    let tail = chunks * 16;
    for j in 0..remainder {
        let i = tail + j;
        let s1 = *seq1.get_unchecked(i);
        let s2 = *seq2.get_unchecked(i);
        if s1 != s2 {
            mismatches += 1;
            if mismatches > max_mismatches {
                return MismatchStats {
                    effective_len: len,
                    mismatches,
                    mismatch_qual_total,
                };
            }
            let q1 = *qual1.get_unchecked(i);
            let q2 = *qual2.get_unchecked(i);
            mismatch_qual_total += q1.min(q2) as u32;
        }
    }

    MismatchStats {
        effective_len: len,
        mismatches,
        mismatch_qual_total,
    }
}

#[cfg(target_arch = "aarch64")]
#[target_feature(enable = "neon")]
unsafe fn compute_mismatch_stats_no_n_neon_limited(
    seq1: &[u8],
    seq2: &[u8],
    qual1: &[u8],
    qual2: &[u8],
    max_mismatches: u32,
) -> MismatchStats {
    use std::arch::aarch64::*;

    let len = seq1.len();
    let mut mismatches: u32 = 0;
    let mut mismatch_qual_total: u32 = 0;

    let chunks = len / 16;
    let remainder = len % 16;

    for chunk in 0..chunks {
        let base = chunk * 16;
        let s1 = vld1q_u8(seq1.as_ptr().add(base));
        let s2 = vld1q_u8(seq2.as_ptr().add(base));
        let eq = vceqq_u8(s1, s2);
        let neq = vmvnq_u8(eq);
        let bit_counts = vcntq_u8(neq);
        let sum16 = vpaddlq_u8(bit_counts);
        let sum32 = vpaddlq_u16(sum16);
        let sum64 = vpaddlq_u32(sum32);
        let total_bits = vgetq_lane_u64(sum64, 0) + vgetq_lane_u64(sum64, 1);
        let chunk_mismatches = (total_bits / 8) as u32;
        mismatches += chunk_mismatches;
        if mismatches > max_mismatches {
            return MismatchStats {
                effective_len: len,
                mismatches,
                mismatch_qual_total,
            };
        }

        if chunk_mismatches > 0 {
            let q1 = vld1q_u8(qual1.as_ptr().add(base));
            let q2 = vld1q_u8(qual2.as_ptr().add(base));
            let qmin = vminq_u8(q1, q2);
            let qmin_masked = vandq_u8(neq, qmin);
            let qsum16 = vpaddlq_u8(qmin_masked);
            let qsum32 = vpaddlq_u16(qsum16);
            let qsum64 = vpaddlq_u32(qsum32);
            let qual_sum = vgetq_lane_u64(qsum64, 0) + vgetq_lane_u64(qsum64, 1);
            mismatch_qual_total += qual_sum as u32;
        }
    }

    let tail = chunks * 16;
    for j in 0..remainder {
        let i = tail + j;
        let s1 = *seq1.get_unchecked(i);
        let s2 = *seq2.get_unchecked(i);
        if s1 != s2 {
            mismatches += 1;
            if mismatches > max_mismatches {
                return MismatchStats {
                    effective_len: len,
                    mismatches,
                    mismatch_qual_total,
                };
            }
            let q1 = *qual1.get_unchecked(i);
            let q2 = *qual2.get_unchecked(i);
            mismatch_qual_total += q1.min(q2) as u32;
        }
    }

    MismatchStats {
        effective_len: len,
        mismatches,
        mismatch_qual_total,
    }
}

#[inline(always)]
fn compute_mismatch_stats_no_n_scalar_limited(
    seq1: &[u8],
    seq2: &[u8],
    qual1: &[u8],
    qual2: &[u8],
    max_mismatches: u32,
) -> MismatchStats {
    let len = seq1.len();
    let mut mismatches: u32 = 0;
    let mut mismatch_qual_total: u32 = 0;

    for i in 0..len {
        let s1 = unsafe { *seq1.get_unchecked(i) };
        let s2 = unsafe { *seq2.get_unchecked(i) };
        if s1 != s2 {
            mismatches += 1;
            if mismatches > max_mismatches {
                return MismatchStats {
                    effective_len: len,
                    mismatches,
                    mismatch_qual_total,
                };
            }
            let q1 = unsafe { *qual1.get_unchecked(i) };
            let q2 = unsafe { *qual2.get_unchecked(i) };
            mismatch_qual_total += q1.min(q2) as u32;
        }
    }

    MismatchStats {
        effective_len: len,
        mismatches,
        mismatch_qual_total,
    }
}

fn compute_mismatch_stats(
    seq1: &[u8],
    seq2: &[u8],
    qual1: &[u8],
    qual2: &[u8],
    have_n: bool,
) -> MismatchStats {
    if !have_n {
        return compute_mismatch_stats_no_n(seq1, seq2, qual1, qual2);
    }

    let mut effective_len = seq1.len();
    let mut mismatches: u32 = 0;
    let mut mismatch_qual_total: u32 = 0;

    for i in 0..seq1.len() {
        if seq1[i] == b'N' || seq2[i] == b'N' {
            effective_len -= 1;
        } else if seq1[i] != seq2[i] {
            mismatches += 1;
            mismatch_qual_total += qual1[i].min(qual2[i]) as u32;
        }
    }

    MismatchStats {
        effective_len,
        mismatches,
        mismatch_qual_total,
    }
}

fn generate_combined_read(
    read1: &FastqRecord,
    read2: &FastqRecord,
    overlap_begin: usize,
    params: &CombineParams,
) -> FastqRecord {
    let mut out = FastqRecord {
        tag: String::new(),
        seq: Vec::new(),
        qual: Vec::new(),
    };
    generate_combined_read_into(read1, read2, overlap_begin, params, &mut out);
    out
}

fn generate_combined_read_into(
    read1: &FastqRecord,
    read2: &FastqRecord,
    overlap_begin: usize,
    params: &CombineParams,
    out: &mut FastqRecord,
) {
    let overlap_len = read1.len() - overlap_begin;
    let remaining_len = read2.len().saturating_sub(overlap_len);
    let combined_len = read1.len() + remaining_len;

    out.seq.clear();
    out.qual.clear();
    out.seq.reserve(combined_len);
    out.qual.reserve(combined_len);

    for idx in 0..overlap_begin {
        let base = read1.seq[idx];
        let base = if params.lowercase_overhang {
            to_lower(base)
        } else {
            base
        };
        out.seq.push(base);
        out.qual.push(read1.qual[idx]);
    }

    for offset in 0..overlap_len {
        let base1 = read1.seq[overlap_begin + offset];
        let base2 = read2.seq[offset];
        let q1 = read1.qual[overlap_begin + offset];
        let q2 = read2.qual[offset];

        if base1 == base2 {
            out.seq.push(base1);
            out.qual.push(q1.max(q2));
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
            out.seq.push(base);
            out.qual.push(q);
        }
    }

    for idx in overlap_len..read2.len() {
        let base = read2.seq[idx];
        let base = if params.lowercase_overhang {
            to_lower(base)
        } else {
            base
        };
        out.seq.push(base);
        out.qual.push(read2.qual[idx]);
    }
}

fn combined_tag(read1: &FastqRecord) -> String {
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

fn write_fastq<W: Write>(
    writer: &mut W,
    read: &FastqRecord,
    phred_offset: u8,
    record_buf: &mut Vec<u8>,
) -> Result<()> {
    record_buf.clear();
    record_buf.reserve(read.tag.len() + read.seq.len() + read.qual.len() + 4);
    record_buf.extend_from_slice(read.tag.as_bytes());
    record_buf.push(b'\n');
    record_buf.extend_from_slice(&read.seq);
    record_buf.extend_from_slice(b"\n+\n");
    append_phred_qualities(record_buf, &read.qual, phred_offset);
    record_buf.push(b'\n');
    writer
        .write_all(record_buf)
        .context("failed to write FASTQ record")?;
    Ok(())
}

fn append_combined_tag(out: &mut Vec<u8>, read1_tag: &str) {
    let tag = read1_tag.as_bytes();
    if let Some(pos) = tag.iter().rposition(|&b| b == b'/') {
        if pos + 2 < tag.len() && tag[pos + 2] == b'#' {
            out.extend_from_slice(&tag[..pos]);
            out.extend_from_slice(&tag[pos + 2..]);
        } else {
            out.extend_from_slice(&tag[..pos]);
        }
    } else {
        out.extend_from_slice(tag);
    }
}

#[inline]
fn append_phred_qualities(out: &mut Vec<u8>, quals: &[u8], phred_offset: u8) {
    let start = out.len();
    out.resize(start + quals.len(), 0);
    for (dst, &q) in out[start..].iter_mut().zip(quals.iter()) {
        *dst = q + phred_offset;
    }
}

fn write_combined_fastq<W: Write>(
    writer: &mut W,
    read1_tag: &str,
    combined: &FastqRecord,
    phred_offset: u8,
    record_buf: &mut Vec<u8>,
) -> Result<()> {
    record_buf.clear();
    record_buf.reserve(read1_tag.len() + combined.seq.len() + combined.qual.len() + 4);
    append_combined_tag(record_buf, read1_tag);
    record_buf.push(b'\n');
    record_buf.extend_from_slice(&combined.seq);
    record_buf.extend_from_slice(b"\n+\n");
    append_phred_qualities(record_buf, &combined.qual, phred_offset);
    record_buf.push(b'\n');
    writer
        .write_all(record_buf)
        .context("failed to write FASTQ record")?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn read_line_bytes_strips_crlf() {
        let data = b"TAG\r\n";
        let mut cursor = std::io::Cursor::new(&data[..]);
        let mut buf = Vec::new();
        let n = read_line_bytes(&mut cursor, &mut buf).unwrap();
        assert!(n > 0);
        assert_eq!(&buf, b"TAG");
    }

    #[test]
    fn canonical_base_normalises_cases_and_unknowns() {
        assert_eq!(canonical_base(b'a'), b'A');
        assert_eq!(canonical_base(b'T'), b'T');
        assert_eq!(canonical_base(b'!'), b'N');
    }
}
