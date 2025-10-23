use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use clap::Parser;

/// Small FASTQ generator for producing paired-end reads at scale.
#[derive(Parser, Debug)]
#[command(author, version, about = "Generate synthetic paired FASTQ files", long_about = None)]
struct Args {
    /// Number of paired sequences to generate
    #[arg(short = 'n', long = "num-sequences", value_name = "COUNT")]
    num_sequences: u64,

    /// Output FASTQ file for read 1
    #[arg(
        long = "output-r1",
        value_name = "PATH",
        default_value = "generated_R1.fastq"
    )]
    output_r1: PathBuf,

    /// Output FASTQ file for read 2
    #[arg(
        long = "output-r2",
        value_name = "PATH",
        default_value = "generated_R2.fastq"
    )]
    output_r2: PathBuf,

    /// Length of each read in bases
    #[arg(long = "read-length", value_name = "BASES", default_value_t = 150)]
    read_length: usize,
}

fn main() -> Result<()> {
    let args = Args::parse();

    run(&args)
}

fn run(args: &Args) -> Result<()> {
    if args.read_length == 0 {
        anyhow::bail!("read-length must be greater than zero");
    }

    generate_fastqs(
        args.num_sequences,
        args.read_length,
        &args.output_r1,
        &args.output_r2,
    )
}

fn generate_fastqs(
    num_sequences: u64,
    read_length: usize,
    output_r1: &Path,
    output_r2: &Path,
) -> Result<()> {
    let mut writer_r1 = BufWriter::new(
        File::create(output_r1)
            .with_context(|| format!("failed to create {}", output_r1.display()))?,
    );
    let mut writer_r2 = BufWriter::new(
        File::create(output_r2)
            .with_context(|| format!("failed to create {}", output_r2.display()))?,
    );

    let quality = vec![b'I'; read_length];
    let mut seq_r1 = vec![0u8; read_length];
    let mut seq_r2 = vec![0u8; read_length];

    for index in 0..num_sequences {
        populate_sequence_pair(&mut seq_r1, &mut seq_r2, index);
        write_record(&mut writer_r1, index, 1, &seq_r1, &quality)?;
        write_record(&mut writer_r2, index, 2, &seq_r2, &quality)?;
    }

    writer_r1.flush()?;
    writer_r2.flush()?;

    Ok(())
}

fn populate_sequence_pair(read1: &mut [u8], read2: &mut [u8], seed: u64) {
    const BASES: [u8; 4] = *b"ACGT";

    let mut state = seed;
    for slot in read1.iter_mut() {
        *slot = BASES[(state & 3) as usize];
        state = state.wrapping_add(1);
    }

    for (idx, slot) in read2.iter_mut().enumerate() {
        let base = read1[read1.len() - 1 - idx];
        *slot = complement(base);
    }
}

fn complement(base: u8) -> u8 {
    match base {
        b'A' => b'T',
        b'C' => b'G',
        b'G' => b'C',
        b'T' => b'A',
        _ => b'N',
    }
}

fn write_record<W: Write>(
    writer: &mut W,
    index: u64,
    mate: u8,
    sequence: &[u8],
    quality: &[u8],
) -> Result<()> {
    writeln!(writer, "@SEQ{:010}.{}", index + 1, mate)?;
    writer.write_all(sequence)?;
    writer.write_all(b"\n+\n")?;
    writer.write_all(quality)?;
    writer.write_all(b"\n")?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn populate_sequence_pair_generates_reverse_complements() {
        let mut r1 = vec![0u8; 8];
        let mut r2 = vec![0u8; 8];
        populate_sequence_pair(&mut r1, &mut r2, 1);
        assert_eq!(&r1, b"CGTACGTA");
        assert_eq!(&r2, b"TACGTACG");
    }

    #[test]
    fn write_record_emits_fastq_block() {
        let mut buffer = Vec::new();
        let sequence = b"ACGT";
        let quality = b"IIII";

        write_record(&mut buffer, 0, 1, sequence, quality).unwrap();

        let expected = format!("@SEQ{:010}.{}\nACGT\n+\nIIII\n", 1, 1);
        assert_eq!(String::from_utf8(buffer).unwrap(), expected);
    }

    #[test]
    fn generate_fastqs_writes_requested_sequences() {
        let dir = tempfile::tempdir().unwrap();
        let r1 = dir.path().join("r1.fastq");
        let r2 = dir.path().join("r2.fastq");

        generate_fastqs(3, 5, &r1, &r2).unwrap();

        let contents_r1 = fs::read_to_string(&r1).unwrap();
        let contents_r2 = fs::read_to_string(&r2).unwrap();

        assert_eq!(contents_r1.lines().count(), 12); // 3 records * 4 lines
        assert_eq!(contents_r2.lines().count(), 12);

        let expected_tag_r1 = format!("@SEQ{:010}.{}", 2, 1);
        let expected_tag_r2 = format!("@SEQ{:010}.{}", 2, 2);
        assert!(contents_r1.contains(&expected_tag_r1));
        assert!(contents_r2.contains(&expected_tag_r2));
    }
}
