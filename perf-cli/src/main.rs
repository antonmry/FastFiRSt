use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail, ensure};
use clap::{Parser, ValueEnum};
use perf_lib::RepeatIndex;
use perf_lib::motif::build_rep_set;
use perf_lib::scanner::{scan_fasta, scan_fasta_parallel, scan_fastq};
use perf_lib::{MotifConfig, ScanConfig};

#[derive(Debug, Clone, ValueEnum)]
enum InputFormat {
    Fasta,
    Fastq,
}

#[derive(Debug, Parser)]
#[command(author, version, about = "Rust port of PERF (SSR detector)")]
struct Cli {
    #[arg(short = 'i', value_name = "INPUT")]
    input: PathBuf,

    #[arg(short = 'o', value_name = "OUTPUT")]
    output: Option<PathBuf>,

    #[arg(long = "format", value_enum, default_value_t = InputFormat::Fasta)]
    format: InputFormat,

    #[arg(short = 'm', default_value_t = 1)]
    min_motif_size: usize,

    #[arg(short = 'M', default_value_t = 6)]
    max_motif_size: usize,

    #[arg(short = 'l', default_value_t = 12)]
    min_repeat_length: usize,

    #[arg(short = 'u')]
    min_units: Option<String>,

    #[arg(short = 's', default_value_t = 0)]
    min_seq_len: usize,

    #[arg(short = 'S', default_value_t = usize::MAX)]
    max_seq_len: usize,

    #[arg(long = "rep", value_name = "PATH")]
    custom_repeat_file: Option<PathBuf>,

    /// Number of threads for parallel FASTA scanning (0 = all available cores).
    #[arg(short = 't', long = "threads", default_value_t = 0)]
    threads: usize,
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    run(cli)
}

fn run(cli: Cli) -> Result<()> {
    ensure!(cli.max_motif_size >= cli.min_motif_size, "-M must be >= -m");
    ensure!(cli.max_seq_len >= cli.min_seq_len, "-S must be >= -s");

    let output = cli
        .output
        .clone()
        .unwrap_or_else(|| default_output_path(&cli.input));

    let custom_motifs = if let Some(path) = &cli.custom_repeat_file {
        Some(load_custom_motifs(path)?)
    } else {
        None
    };

    let motif_cfg = MotifConfig {
        min_size: cli.min_motif_size,
        max_size: cli.max_motif_size,
        min_repeat_length: cli.min_repeat_length,
        custom_motifs,
    };
    let repeats = build_rep_set(&motif_cfg)?;
    let index = RepeatIndex::new(repeats);

    let scan_cfg = ScanConfig {
        min_motif_size: cli.min_motif_size,
        max_motif_size: cli.max_motif_size,
        min_repeat_length: cli.min_repeat_length,
        min_seq_len: cli.min_seq_len,
        max_seq_len: cli.max_seq_len,
        min_units: parse_min_units(
            cli.min_units.as_deref(),
            cli.min_motif_size,
            cli.max_motif_size,
            cli.min_repeat_length,
        )?,
    };

    match cli.format {
        InputFormat::Fasta => {
            let num_threads = if cli.threads == 0 {
                std::thread::available_parallelism()
                    .map(|n| n.get())
                    .unwrap_or(1)
            } else {
                cli.threads
            };

            if num_threads > 1 {
                scan_fasta_parallel(&cli.input, &output, &index, &scan_cfg, num_threads)
            } else {
                scan_fasta(&cli.input, &output, &index, &scan_cfg)
            }
        }
        InputFormat::Fastq => {
            scan_fastq(&cli.input, &output, &index, &scan_cfg)?;
            Ok(())
        }
    }
}

fn default_output_path(input: &Path) -> PathBuf {
    let name = input
        .file_name()
        .and_then(|n| n.to_str())
        .map(|n| format!("{n}_perf.tsv"))
        .unwrap_or_else(|| "output_perf.tsv".to_string());

    input.with_file_name(name)
}

fn load_custom_motifs(path: &Path) -> Result<Vec<String>> {
    let content = fs::read_to_string(path)
        .with_context(|| format!("failed to read custom repeat file {}", path.display()))?;

    let motifs = content
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
        .map(ToOwned::to_owned)
        .collect::<Vec<_>>();

    if motifs.is_empty() {
        bail!(
            "custom repeat file {} did not contain any motifs",
            path.display()
        );
    }

    Ok(motifs)
}

fn parse_min_units(
    value: Option<&str>,
    min_motif_size: usize,
    max_motif_size: usize,
    _min_repeat_length: usize,
) -> Result<HashMap<usize, usize>> {
    let mut out = HashMap::new();

    if let Some(raw) = value {
        if let Ok(units) = raw.parse::<usize>() {
            ensure!(units >= 1, "-u integer value must be >= 1");
            for size in min_motif_size..=max_motif_size {
                out.insert(size, units);
            }
            return Ok(out);
        }

        let path = Path::new(raw);
        let content = fs::read_to_string(path)
            .with_context(|| format!("failed to read min-units file {}", path.display()))?;
        for (idx, line) in content.lines().enumerate() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }

            let mut parts = line.split_whitespace();
            let motif_len = parts
                .next()
                .context("missing motif length column")?
                .parse::<usize>()
                .with_context(|| format!("invalid motif length on line {}", idx + 1))?;
            let units = parts
                .next()
                .context("missing min units column")?
                .parse::<usize>()
                .with_context(|| format!("invalid min units on line {}", idx + 1))?;

            out.insert(motif_len, units);
        }
        return Ok(out);
    }

    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_output_is_derived_from_input_name() {
        let out = default_output_path(Path::new("/tmp/input.fa"));
        assert_eq!(out, PathBuf::from("/tmp/input.fa_perf.tsv"));
    }
}
