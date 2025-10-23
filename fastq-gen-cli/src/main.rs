use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::PathBuf;

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

    if args.read_length == 0 {
        anyhow::bail!("read-length must be greater than zero");
    }

    let mut writer_r1 = BufWriter::new(
        File::create(&args.output_r1)
            .with_context(|| format!("failed to create {}", args.output_r1.display()))?,
    );
    let mut writer_r2 = BufWriter::new(
        File::create(&args.output_r2)
            .with_context(|| format!("failed to create {}", args.output_r2.display()))?,
    );

    let quality = vec![b'I'; args.read_length];
    let mut seq_r1 = vec![0u8; args.read_length];
    let mut seq_r2 = vec![0u8; args.read_length];

    for index in 0..args.num_sequences {
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
